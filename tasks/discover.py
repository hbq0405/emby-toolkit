# tasks/discover.py
# 影视探索任务模块
import logging
import handler.tmdb as tmdb
from database import media_db, settings_db, request_db, actor_db 
import constants
from utils import contains_chinese 

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

        # ✨ 1. 获取动态映射表 (优先数据库，预设兜底)
        from utils import DEFAULT_KEYWORD_MAPPING
        mapping = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        
        # ★★★ 修复：兼容 List (新版) 和 Dict (旧版) 两种格式 ★★★
        theme_list = []
        if isinstance(mapping, list):
            # 新版 List 格式: [{'label': 'xxx', 'ids': [...]}, ...]
            theme_list = [(item.get('label'), item) for item in mapping if item.get('ids') and item.get('label')]
        elif isinstance(mapping, dict):
            # 旧版 Dict 格式: {'xxx': {'ids': [...]}, ...}
            theme_list = [(label, info) for label, info in mapping.items() if info.get('ids')]

        if not theme_list:
            logger.error("  ➜ 每日推荐失败：关键词映射表为空或未配置 ID。")
            return

        # 2. 计算今日索引
        last_theme_index = settings_db.get_setting('recommendation_theme_index')
        if last_theme_index is None: last_theme_index = -1

        today_theme_index = (last_theme_index + 1) % len(theme_list)
        today_theme_name, today_theme_info = theme_list[today_theme_index]
        
        # ✨ 3. 组装 TMDb 关键词 ID (使用 '|' 实现 OR 逻辑，扩大搜索面)
        today_theme_ids = "|".join([str(_id) for _id in today_theme_info.get('ids', [])])
        
        logger.info(f"  ➜ 今日推荐主题: 【{today_theme_name}】 (IDs: {today_theme_ids})")

        recommendation_pool = []
        page_to_fetch = 1
        MIN_POOL_SIZE = 10
        MAX_PAGES_TO_SCAN = 5
        
        while len(recommendation_pool) < MIN_POOL_SIZE and page_to_fetch <= MAX_PAGES_TO_SCAN:
            logger.debug(f"  ➜ 正在扫描主题【{today_theme_name}】的第 {page_to_fetch}/{MAX_PAGES_TO_SCAN} 页...")
            
            discover_params = {
                'with_keywords': today_theme_ids, 
                'sort_by': 'popularity.desc',
                'page': page_to_fetch, 
                'include_adult': True
            }
            movies_data = tmdb.discover_movie_tmdb(api_key, discover_params)
            
            if not movies_data or not movies_data.get("results"):
                logger.warning(f"  ➜ 从主题【{today_theme_name}】第 {page_to_fetch} 页获取电影失败，勘探提前结束。")
                break

            popular_movies = movies_data["results"]
            tmdb_ids = [str(movie["id"]) for movie in popular_movies]

            library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type='Movie')
            subscription_statuses = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, item_type='Movie')

            candidate_movies = [
                movie for movie in popular_movies
                if f"{movie['id']}_Movie" not in library_items_map
                and str(movie["id"]) not in subscription_statuses
                and movie.get("poster_path") 
                and contains_chinese(movie.get('title') or movie.get('name')) 
                and movie.get("overview", "").strip() 
            ]

            if not candidate_movies:
                logger.debug(f"  ➜ 第 {page_to_fetch} 页没有符合条件的电影，继续扫描下一页。")
                page_to_fetch += 1
                continue

            logger.debug(f"  ➜ 在第 {page_to_fetch} 页发现 {len(candidate_movies)} 部符合条件的电影，开始获取详情...")
            
            # ★★★ 核心修改 1/3: 批量获取演员中文名 ★★★
            all_actor_ids = set()
            detailed_movies = []
            for movie in candidate_movies:
                try:
                    movie_details = tmdb.get_movie_details(movie["id"], api_key)
                    if movie_details:
                        detailed_movies.append(movie_details)
                        for actor in movie_details.get("credits", {}).get("cast", [])[:10]:
                            all_actor_ids.add(actor.get("id"))
                except Exception as e_detail:
                    logger.warning(f"  ➜ 获取电影 {movie.get('title')} 详情时失败: {e_detail}")
            
            actor_name_map = actor_db.get_actor_chinese_names_by_tmdb_ids(list(all_actor_ids))

            # ★★★ 核心修改 2/3: 组装数据时注入中文名 ★★★
            for movie_details in detailed_movies:
                cast = []
                for actor in movie_details.get("credits", {}).get("cast", [])[:10]:
                    actor_id = actor.get("id")
                    cast.append({
                        "id": actor_id,
                        "name": actor.get("name"),
                        "name_cn": actor_name_map.get(actor_id, actor.get("name")), # <-- 新增中文名
                        "profile_path": actor.get("profile_path"),
                        "character": actor.get("character") # <-- 角色名应该已经是中文
                    })
                
                recommendation_pool.append({
                    "id": movie_details["id"], "title": movie_details.get("title"),
                    "overview": movie_details.get("overview"), "poster_path": movie_details.get("poster_path"),
                    "release_date": movie_details.get("release_date"), "vote_average": movie_details.get("vote_average"),
                    "cast": cast, "media_type": "movie"
                })
            
            page_to_fetch += 1

        if not recommendation_pool:
            logger.info(f"  ➜ 扫描了 {page_to_fetch - 1} 页后，仍未找到任何符合【{today_theme_name}】主题的电影，今日推荐为空。")
        
        settings_db.save_setting('recommendation_pool', recommendation_pool)
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
        REPLENISH_THRESHOLD = 5
        pool_data_check = settings_db.get_setting('recommendation_pool')
        pool_check = pool_data_check or []
        
        if len(pool_check) >= REPLENISH_THRESHOLD:
            logger.debug(f"  ➜ 任务启动时发现推荐池库存 ({len(pool_check)}) 已充足，无需补充。任务提前结束。")
            return
        
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        current_theme_index = settings_db.get_setting('recommendation_theme_index')
        if current_theme_index is None:
            logger.warning("  🚫 补充任务中止：未找到当前推荐主题索引。请先执行一次每日推荐更新任务。")
            return

        from routes.custom_collections import DEFAULT_KEYWORD_MAPPING
        mapping = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        
        # ★★★ 修复：兼容 List (新版) 和 Dict (旧版) 两种格式 ★★★
        theme_list = []
        if isinstance(mapping, list):
            theme_list = [(item.get('label'), item) for item in mapping if item.get('ids') and item.get('label')]
        elif isinstance(mapping, dict):
            theme_list = [(label, info) for label, info in mapping.items() if info.get('ids')]
        
        current_theme_index = settings_db.get_setting('recommendation_theme_index')
        if current_theme_index is None or current_theme_index >= len(theme_list):
            return

        current_theme_name, current_theme_info = theme_list[current_theme_index]
        current_theme_ids = "|".join([str(_id) for _id in current_theme_info.get('ids', [])])
        
        logger.info(f"  ➜ 当前推荐主题为【{current_theme_name}】，准备补充内容。")

        current_pool = pool_check
        current_page_data = settings_db.get_setting('recommendation_pool_page')
        current_page = current_page_data if current_page_data is not None else 1
        next_page_to_fetch = current_page + 1

        logger.debug(f"  ➜ 当前池中有 {len(current_pool)} 部电影，准备从主题【{current_theme_name}】的第 {next_page_to_fetch} 页补充。")

        discover_params = {
            'with_keywords': current_theme_ids, 'sort_by': 'popularity.desc',
            'page': next_page_to_fetch, 'include_adult': False
        }
        more_movies_data = tmdb.discover_movie_tmdb(api_key, discover_params)

        if not more_movies_data or not more_movies_data.get("results"):
            logger.warning(f"  ➜ 从主题【{current_theme_name}】第 {next_page_to_fetch} 页获取电影失败，无内容可补充。")
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            return

        current_pool_ids = {str(movie["id"]) for movie in current_pool}
        new_movies = more_movies_data["results"]
        new_tmdb_ids = [str(movie["id"]) for movie in new_movies]
        
        library_items_map = media_db.check_tmdb_ids_in_library(new_tmdb_ids, item_type='Movie')
        subscription_statuses = request_db.get_global_subscription_statuses_by_tmdb_ids(new_tmdb_ids, item_type='Movie')
        
        candidate_movies = [
            movie for movie in new_movies
            if f"{movie['id']}_Movie" not in library_items_map
            and str(movie["id"]) not in current_pool_ids
            and str(movie["id"]) not in subscription_statuses
            and movie.get("poster_path")
            and contains_chinese(movie.get('title') or movie.get('name'))
            and movie.get("overview", "").strip()
        ]

        if not candidate_movies:
            logger.debug(f"  ➜ 主题【{current_theme_name}】第 {next_page_to_fetch} 页的电影均不符合补充条件。")
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            return

        # ★★★ 核心修改 3/3: 在补充任务中也加入同样的数据丰富逻辑 ★★★
        all_actor_ids = set()
        detailed_movies = []
        for movie in candidate_movies:
            try:
                movie_details = tmdb.get_movie_details(movie["id"], api_key)
                if movie_details:
                    detailed_movies.append(movie_details)
                    for actor in movie_details.get("credits", {}).get("cast", [])[:10]:
                        all_actor_ids.add(actor.get("id"))
            except Exception as e_detail:
                logger.warning(f"  ➜ 获取补充电影 {movie.get('title')} 详情时失败: {e_detail}")
        
        actor_name_map = actor_db.get_actor_chinese_names_by_tmdb_ids(list(all_actor_ids))

        replenishment_list = []
        for movie_details in detailed_movies:
            cast = []
            for actor in movie_details.get("credits", {}).get("cast", [])[:10]:
                actor_id = actor.get("id")
                cast.append({
                    "id": actor_id,
                    "name": actor.get("name"),
                    "name_cn": actor_name_map.get(actor_id, actor.get("name")),
                    "profile_path": actor.get("profile_path"),
                    "character": actor.get("character")
                })
            
            replenishment_list.append({
                "id": movie_details["id"], "title": movie_details.get("title"),
                "overview": movie_details.get("overview"), "poster_path": movie_details.get("poster_path"),
                "release_date": movie_details.get("release_date"), "vote_average": movie_details.get("vote_average"),
                "cast": cast, "media_type": "movie"
            })

        if replenishment_list:
            updated_pool = current_pool + replenishment_list
            settings_db.save_setting('recommendation_pool', updated_pool)
            settings_db.save_setting('recommendation_pool_page', next_page_to_fetch)
            logger.debug(f"  ✅ 推荐池补充成功！为主题【{current_theme_name}】新增 {len(replenishment_list)} 部电影，当前总数 {len(updated_pool)}。下次将从第 {next_page_to_fetch + 1} 页开始。")
        else:
            logger.debug("  ➜ 未能成功获取任何电影详情，本次补充列表为空。")

    except Exception as e:
        logger.error(f"  ➜ 推荐池(主题感知)补充任务执行失败: {e}", exc_info=True)