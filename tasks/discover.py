# tasks/discover.py
import logging
import handler.tmdb as tmdb
from database import media_db, settings_db, user_db
import constants
from utils import DAILY_THEME
logger = logging.getLogger(__name__)

def task_update_daily_theme(processor):
    """
    每天从预设的主题列表中选择一个，推荐该主题下的热门电影。
    如果第一页不满足条件，会自动扫描后续页面。
    """
    logger.info("  ➜ 开始执行【每日推荐池-主题轮换】全量更新任务...")
    try:
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        # ★ 1. 定义勘探目标和安全限制
        MIN_POOL_SIZE = 10  # 至少找到 10 部电影
        MAX_PAGES_TO_SCAN = 5 # 最多扫描 5 页

        # ★ 2. 引入每日主题轮换逻辑
        #   - 我们只用 DAILY_THEME 的键（中文名）和值（ID）
        theme_list = list(DAILY_THEME.items())
        if not theme_list:
            logger.error("  ➜ 每日推荐失败：主题列表 (DAILY_THEME) 为空，请检查 utils.py。")
            return

        #   - 从数据库获取上次推荐到哪个主题的索引
        last_theme_index = settings_db.get_setting('recommendation_theme_index')
        if last_theme_index is None:
            last_theme_index = -1 # 如果是第一次运行，从-1开始，这样下一个就是0

        #   - 计算今天的主题
        today_theme_index = (last_theme_index + 1) % len(theme_list)
        today_theme_name, today_theme_id = theme_list[today_theme_index]
        
        logger.info(f"  ➜ 今日推荐主题: 【{today_theme_name}】 (ID: {today_theme_id})")

        # ★ 3. 启动循环，勘探今日主题的电影
        recommendation_pool = []
        page_to_fetch = 1
        
        while len(recommendation_pool) < MIN_POOL_SIZE and page_to_fetch <= MAX_PAGES_TO_SCAN:
            logger.debug(f"  ➜ 正在扫描主题【{today_theme_name}】的第 {page_to_fetch}/{MAX_PAGES_TO_SCAN} 页...")
            
            # 调用“发现电影”，并传入主题ID
            discover_params = {
                'with_keywords': today_theme_id,
                'sort_by': 'popularity.desc', # 按热度排序
                'page': page_to_fetch,
                'include_adult': True
            }
            # 你的 tmdb.py 中已经有 discover_movie_tmdb 这个函数了
            movies_data = tmdb.discover_movie_tmdb(api_key, discover_params)
            
            if not movies_data or not movies_data.get("results"):
                logger.warning(f"  ➜ 从主题【{today_theme_name}】第 {page_to_fetch} 页获取电影失败，勘探提前结束。")
                break

            popular_movies = movies_data["results"]
            tmdb_ids = [str(movie["id"]) for movie in popular_movies]

            library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type='Movie')
            subscription_statuses = user_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids)

            candidate_movies = [
                movie for movie in popular_movies
                if str(movie["id"]) not in library_items_map
                and str(movie["id"]) not in subscription_statuses
            ]
            
            movies_with_overview = [movie for movie in candidate_movies if movie.get("overview", "").strip()]

            if not movies_with_overview:
                logger.debug(f"  ➜ 第 {page_to_fetch} 页没有符合条件的电影，继续扫描下一页。")
                page_to_fetch += 1
                continue

            logger.debug(f"  ➜ 在第 {page_to_fetch} 页发现 {len(movies_with_overview)} 部符合条件的电影，开始获取详情...")
            for movie in movies_with_overview:
                try:
                    # 获取详情
                    movie_details = tmdb.get_movie_details(movie["id"], api_key)
                    if not movie_details: continue

                    cast = [
                        {"id": actor.get("id"), "name": actor.get("name"), "profile_path": actor.get("profile_path"), "character": actor.get("character")}
                        for actor in movie_details.get("credits", {}).get("cast", [])[:10]
                    ]
                    
                    recommendation_pool.append({
                        "id": movie["id"], "title": movie.get("title"),
                        "overview": movie.get("overview"), "poster_path": movie.get("poster_path"),
                        "release_date": movie.get("release_date"), "vote_average": movie.get("vote_average"),
                        "cast": cast, "media_type": "movie"
                    })
                except Exception as e_detail:
                    logger.warning(f"  ➜ 获取电影 {movie.get('title')} 详情时失败: {e_detail}")
            
            page_to_fetch += 1

        # ★ 4. 循环结束后，统一保存结果
        if not recommendation_pool:
            logger.info(f"  ➜ 扫描了 {page_to_fetch - 1} 页后，仍未找到任何符合【{today_theme_name}】主题的电影，今日推荐为空。")
        
        settings_db.save_setting('recommendation_pool', recommendation_pool)
        
        # ★ 5. 关键：保存我们这次用的主题索引和页数，确保下次轮换以及补充！
        settings_db.save_setting('recommendation_theme_index', today_theme_index)
        settings_db.save_setting('recommendation_pool_page', page_to_fetch - 1)
        
        logger.debug(f"  ✅ 每日推荐池已更新为【{today_theme_name}】主题，共找到 {len(recommendation_pool)} 部电影。下次将推荐下一个主题。")

    except Exception as e:
        logger.error(f"  ➜ 每日推荐(主题轮换)更新任务执行失败: {e}", exc_info=True)


def task_replenish_recommendation_pool(processor):
    """
    为推荐池补充弹药。它会自动识别当前池的主题，并只补充同一主题的电影。
    在执行前会再次检查库存，防止因并发请求导致重复补充。
    """
    logger.info("  ➜ 开始执行【推荐池主题感知补充】任务...")
    try:
        # 1. 核心修正：在任务开始时，立刻再次检查库存
        REPLENISH_THRESHOLD = 5
        pool_data_check = settings_db.get_setting('recommendation_pool')
        pool_check = pool_data_check or []
        
        if len(pool_check) >= REPLENISH_THRESHOLD:
            logger.debug(f"  ➜ 任务启动时发现推荐池库存 ({len(pool_check)}) 已充足，无需补充。任务提前结束。")
            return
        
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        # 2. ★★★ 获取当前推荐主题 ★★★
        current_theme_index = settings_db.get_setting('recommendation_theme_index')
        if current_theme_index is None:
            logger.warning("  ➜ 补充任务中止：未找到当前推荐主题索引(recommendation_theme_index)。请先执行一次每日推荐更新任务。")
            return

        theme_list = list(DAILY_THEME.items())
        if not theme_list or current_theme_index >= len(theme_list):
            logger.error(f"  ➜ 补充任务失败：主题索引({current_theme_index})无效或主题列表为空。")
            return
            
        current_theme_name, current_theme_id = theme_list[current_theme_index]
        logger.info(f"  ➜ 当前推荐主题为【{current_theme_name}】，将按此主题进行补充。")

        # 3. 获取当前进度和池内ID
        current_pool = pool_check
        current_page_data = settings_db.get_setting('recommendation_pool_page')
        current_page = current_page_data if current_page_data is not None else 1
        next_page_to_fetch = current_page + 1

        logger.debug(f"  ➜ 当前池中有 {len(current_pool)} 部电影，准备从主题【{current_theme_name}】的第 {next_page_to_fetch} 页补充。")

        # 4. ★★★ 更换数据源 ★★★
        discover_params = {
            'with_keywords': current_theme_id,
            'sort_by': 'popularity.desc',
            'page': next_page_to_fetch,
            'include_adult': True
        }
        more_movies_data = tmdb.discover_movie_tmdb(api_key, discover_params)

        if not more_movies_data or not more_movies_data.get("results"):
            logger.warning(f"  ➜ 从主题【{current_theme_name}】第 {next_page_to_fetch} 页获取电影失败，无内容可补充。")
            # 即使没获取到，也要更新页码，防止下次重复请求失败的页
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            return

        current_pool_ids = {str(movie["id"]) for movie in current_pool}
        new_movies = more_movies_data["results"]
        new_tmdb_ids = [str(movie["id"]) for movie in new_movies]
        
        library_items_map = media_db.check_tmdb_ids_in_library(new_tmdb_ids, item_type='Movie')
        subscription_statuses = user_db.get_global_subscription_statuses_by_tmdb_ids(new_tmdb_ids)
        
        candidate_movies = [
            movie for movie in new_movies
            if str(movie["id"]) not in library_items_map
            and str(movie["id"]) not in current_pool_ids
            and str(movie["id"]) not in subscription_statuses
            and movie.get("overview", "").strip()
        ]

        if not candidate_movies:
            logger.debug(f"  ➜ 主题【{current_theme_name}】第 {next_page_to_fetch} 页的电影均不符合补充条件。")
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            return

        replenishment_list = []
        logger.debug(f"  ➜ 发现 {len(candidate_movies)} 部符合条件的电影，开始获取详情...")
        for movie in candidate_movies:
            try:
                movie_details = tmdb.get_movie_details(movie["id"], api_key)
                if not movie_details: continue
                
                cast = [
                    {"id": actor.get("id"), "name": actor.get("name"), "profile_path": actor.get("profile_path"), "character": actor.get("character")}
                    for actor in movie_details.get("credits", {}).get("cast", [])[:10]
                ]
                
                replenishment_list.append({
                    "id": movie["id"], "title": movie.get("title"),
                    "overview": movie.get("overview"), "poster_path": movie.get("poster_path"),
                    "release_date": movie.get("release_date"), "vote_average": movie.get("vote_average"),
                    "cast": cast, "media_type": "movie"
                })
            except Exception as e_detail:
                logger.warning(f"  ➜ 获取补充电影 {movie.get('title')} 详情时失败: {e_detail}")

        if replenishment_list:
            updated_pool = current_pool + replenishment_list
            settings_db.save_setting('recommendation_pool', updated_pool)
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            logger.debug(f"  ✅ 推荐池补充成功！为主题【{current_theme_name}】新增 {len(replenishment_list)} 部电影，当前总数 {len(updated_pool)}。下次将从第 {next_page_to_fetch + 1} 页开始。")
        else:
            logger.debug("  ➜ 未能成功获取任何电影详情，本次补充列表为空。")

    except Exception as e:
        logger.error(f"  ➜ 推荐池(主题感知)补充任务执行失败: {e}", exc_info=True)