# tasks/discover.py
import logging
import random
import json
import os
import handler.tmdb as tmdb
from database import media_db, settings_db, user_db
import config_manager
import constants
logger = logging.getLogger(__name__)

def task_update_daily_recommendation(processor):
    """
    【V4 - 循环勘探版】
    如果第一页热门电影不满足条件，会自动扫描后续页面，
    直到凑够指定的最小推荐数量或达到扫描上限。
    """
    logger.info("  ➜ 开始执行【每日推荐池】全量更新任务...")
    try:
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        # ★ 1. 定义勘探目标和安全限制
        MIN_POOL_SIZE = 10  # 我们希望至少找到 10 部电影
        MAX_PAGES_TO_SCAN = 5 # 最多扫描 5 页，防止无限循环和API滥用

        recommendation_pool = []
        page_to_fetch = 1
        
        # ★ 2. 启动循环，直到满足条件或达到上限
        while len(recommendation_pool) < MIN_POOL_SIZE and page_to_fetch <= MAX_PAGES_TO_SCAN:
            logger.info(f"  ➜ 正在扫描第 {page_to_fetch}/{MAX_PAGES_TO_SCAN} 页热门电影...")
            
            popular_movies_data = tmdb.get_popular_movies_tmdb(api_key, {'page': page_to_fetch})
            
            # 如果某一页已经没有数据了，就提前结束
            if not popular_movies_data or not popular_movies_data.get("results"):
                logger.warning(f"  ➜ 从第 {page_to_fetch} 页获取热门电影失败，勘探提前结束。")
                break

            popular_movies = popular_movies_data["results"]
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
                continue # 直接进入下一次循环

            logger.debug(f"  ➜ 在第 {page_to_fetch} 页发现 {len(movies_with_overview)} 部符合条件的电影，开始获取详情...")
            for movie in movies_with_overview:
                try:
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
            
            # 准备扫描下一页
            page_to_fetch += 1

        # ★ 3. 循环结束后，统一保存结果
        if not recommendation_pool:
            logger.info(f"  ➜ 扫描了 {page_to_fetch - 1} 页后，仍未找到任何符合条件的电影，今日推荐为空。")
        
        settings_db.save_setting('recommendation_pool', recommendation_pool)
        # ★ 4. 关键：保存我们扫描到的最后一页的页码，这样补货时就能从下一页开始
        settings_db.save_setting('recommendation_pool_page', page_to_fetch - 1)
        
        logger.info(f"  ✅ 每日推荐池已更新，共找到 {len(recommendation_pool)} 部电影。补货将从第 {page_to_fetch} 页开始。")

    except Exception as e:
        logger.error(f"  ➜ 每日推荐更新任务执行失败: {e}", exc_info=True)


def task_replenish_recommendation_pool(processor):
    """
    【V4 - 最终防并发版】
    为推荐池补货。在执行前会再次检查库存，防止因并发请求导致重复补货。
    """
    logger.info("  ➜ 开始执行【推荐池补货】任务...")
    try:
        # ★ 核心修正：在任务开始时，立刻再次检查库存 ★
        REPLENISH_THRESHOLD = 5
        pool_data_check = settings_db.get_setting('recommendation_pool')
        pool_check = pool_data_check or []
        
        if len(pool_check) >= REPLENISH_THRESHOLD:
            logger.info(f"  ➜ 任务启动时发现推荐池库存 ({len(pool_check)}) 已充足，无需补货。任务提前结束。")
            return # 直接退出，不执行任何操作
        
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        # 注意：这里我们使用上面已经获取过一次的 pool_check 作为 current_pool，避免重复查询数据库
        current_pool = pool_check
        
        current_page_data = settings_db.get_setting('recommendation_pool_page')
        current_page = current_page_data if current_page_data is not None else 1
        next_page_to_fetch = current_page + 1

        logger.debug(f"  ➜ 当前池中有 {len(current_pool)} 部电影，准备从第 {next_page_to_fetch} 页热门电影补货。")

        more_movies_data = tmdb.get_popular_movies_tmdb(api_key, {'page': next_page_to_fetch})
        if not more_movies_data or not more_movies_data.get("results"):
            logger.warning(f"  ➜ 从第 {next_page_to_fetch} 页获取热门电影失败，无内容可补充。")
            return

        current_pool_ids = {str(movie["id"]) for movie in current_pool}
        new_popular_movies = more_movies_data["results"]
        
        new_tmdb_ids = [str(movie["id"]) for movie in new_popular_movies]
        
        library_items_map = media_db.check_tmdb_ids_in_library(new_tmdb_ids, item_type='Movie')
        subscription_statuses = user_db.get_global_subscription_statuses_by_tmdb_ids(new_tmdb_ids)
        
        candidate_movies = [
            movie for movie in new_popular_movies
            if str(movie["id"]) not in library_items_map
            and str(movie["id"]) not in current_pool_ids
            and str(movie["id"]) not in subscription_statuses
            and movie.get("overview", "").strip()
        ]

        if not candidate_movies:
            logger.info(f"  ➜ 第 {next_page_to_fetch} 页的电影均不符合补充条件，本次不补货。")
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
            logger.info(f"  ✅ 推荐池补货成功！新增 {len(replenishment_list)} 部电影，当前总数 {len(updated_pool)}。下次将从第 {next_page_to_fetch + 1} 页开始。")
        else:
            logger.info("  ➜ 未能成功获取任何电影详情，本次补货列表为空。")

    except Exception as e:
        logger.error(f"  ➜ 推荐池补货任务执行失败: {e}", exc_info=True)