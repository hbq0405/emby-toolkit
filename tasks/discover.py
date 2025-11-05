# tasks/discover.py
import logging
import random
import json
import os
import handler.tmdb as tmdb
from database import media_db, settings_db
import config_manager
import constants
logger = logging.getLogger(__name__)
def task_update_daily_recommendation(processor):
    """
    获取所有符合条件的电影详情，存入一个列表。
    """
    logger.info("  ➜ 开始执行【推荐池】更新任务...")
    try:
        # ... (获取 api_key, popular_movies, missing_movies 的逻辑不变) ...
        config = processor.config
        api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not api_key: return

        popular_movies_data = tmdb.get_popular_movies_tmdb(api_key, {'page': 1})
        if not popular_movies_data or not popular_movies_data.get("results"): return

        popular_movies = popular_movies_data["results"]
        tmdb_ids = [str(movie["id"]) for movie in popular_movies]
        library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type='Movie')
        missing_movies = [movie for movie in popular_movies if str(movie["id"]) not in library_items_map]
        movies_with_overview = [movie for movie in missing_movies if movie.get("overview", "").strip()]

        # ★★★ 核心修改：不再只选一个，而是创建一个“推荐池”列表 ★★★
        recommendation_pool = []
        
        if not movies_with_overview:
            logger.info("  ➜ 热门电影都已入库或缺少中文简介，今日推荐为空。")
        else:
            logger.debug(f"  ➜ 发现 {len(movies_with_overview)} 部符合条件的电影，开始获取详情...")
            # 遍历所有符合条件的电影
            for movie in movies_with_overview:
                try:
                    movie_details = tmdb.get_movie_details(movie["id"], api_key)
                    if not movie_details: continue

                    cast = [
                        {
                            "id": actor.get("id"), "name": actor.get("name"),
                            "profile_path": actor.get("profile_path"), "character": actor.get("character")
                        }
                        for actor in movie_details.get("credits", {}).get("cast", [])
                    ]
                    
                    recommendation_pool.append({
                        "id": movie["id"], "title": movie.get("title"),
                        "overview": movie.get("overview"), "poster_path": movie.get("poster_path"),
                        "release_date": movie.get("release_date"), "vote_average": movie.get("vote_average"),
                        "cast": cast, "media_type": "movie"
                    })
                except Exception as e_detail:
                    logger.warning(f"  ➜ 获取今日推荐电影 {movie.get('title')} 详情时失败: {e_detail}")

        # ★★★ 将整个“推荐池”列表存入数据库 ★★★
        # 我们换个 key，更符合现在的逻辑
        settings_db.save_setting('recommendation_pool', recommendation_pool)
            
        logger.debug(f"  ✅ 今日推荐更新成功，共 {len(recommendation_pool)} 部电影已存入数据库。")

    except Exception as e:
        logger.error(f"  ➜ 今日推荐更新任务执行失败: {e}", exc_info=True)