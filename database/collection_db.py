# database/collection_db.py
import psycopg2
import logging
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from .connection import get_db_connection
import config_manager
import constants
import tmdb_handler
logger = logging.getLogger(__name__)

# --- 状态中文翻译字典 ---
STATUS_TRANSLATION_MAP = {
    'in_library': '已入库',
    'subscribed': '已订阅',
    'missing': '缺失',
    'unreleased': '未上映',
    'pending_release': '未上映'
}

# ======================================================================
# 模块: 电影合集数据访问 
# ======================================================================

def get_all_collections() -> List[Dict[str, Any]]:
    """获取数据库中所有电影合集的信息。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM collections_info WHERE tmdb_collection_id IS NOT NULL ORDER BY name")
            
            final_results = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                missing_movies_data = row_dict.get('missing_movies_json')
                
                if isinstance(missing_movies_data, list):
                    row_dict['missing_movies'] = missing_movies_data
                else:
                    row_dict['missing_movies'] = []

                del row_dict['missing_movies_json']
                final_results.append(row_dict)
                
            return final_results
    except Exception as e:
        logger.error(f"DB: 读取合集状态时发生严重错误: {e}", exc_info=True)
        raise

def get_all_custom_collection_emby_ids() -> set:
    """从 custom_collections 表中获取所有非空的 emby_collection_id。"""
    
    ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_collection_id FROM custom_collections WHERE emby_collection_id IS NOT NULL")
            rows = cursor.fetchall()
            for row in rows:
                ids.add(row['emby_collection_id'])
        logger.debug(f"从数据库中获取到 {len(ids)} 个由本程序管理的自定义合集ID。")
        return ids
    except psycopg2.Error as e:
        logger.error(f"获取所有自定义合集Emby ID时发生数据库错误: {e}", exc_info=True)
        return ids

def get_collections_with_missing_movies() -> List[Dict[str, Any]]:
    """获取所有包含缺失电影的合集信息。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_collection_id, name, missing_movies_json FROM collections_info WHERE has_missing = TRUE")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取有缺失电影的合集时失败: {e}", exc_info=True)
        raise

def update_collection_movies(collection_id: str, movies: List[Dict[str, Any]]):
    """更新指定合集的电影列表和缺失状态。"""
    
    try:
        with get_db_connection() as conn:
            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = %s, has_missing = %s WHERE emby_collection_id = %s",
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 的电影列表。")
    except Exception as e:
        logger.error(f"DB: 更新合集 {collection_id} 的电影列表时失败: {e}", exc_info=True)
        raise

def update_single_movie_status_in_collection(collection_id: str, movie_tmdb_id: str, new_status: str) -> bool:
    """【修复 #2】更新合集中单个电影的状态。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = %s", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            movies = row.get('missing_movies_json')
            if not isinstance(movies, list):
                movies = []

            movie_found = False
            for movie in movies:
                if str(movie.get('tmdb_id')) == str(movie_tmdb_id):
                    movie['status'] = new_status
                    movie_found = True
                    break
            
            if not movie_found:
                conn.rollback()
                return False

            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = %s, has_missing = %s WHERE emby_collection_id = %s", 
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 中电影 {movie_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新电影状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

def batch_mark_movies_as_subscribed_in_collections(collection_ids: List[str]) -> int:
    """【V2 - PG 兼容修复版】批量将指定合集中的'missing'电影状态更新为'subscribed'。"""
    
    if not collection_ids:
        return 0

    total_updated_movies = 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            placeholders = ','.join('%s' for _ in collection_ids)
            sql_select = f"SELECT emby_collection_id, missing_movies_json FROM collections_info WHERE emby_collection_id IN ({placeholders})"
            cursor.execute(sql_select, collection_ids)
            collections_to_process = cursor.fetchall()

            if not collections_to_process:
                return 0

            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in collections_to_process:
                    collection_id = collection_row['emby_collection_id']
                    movies = collection_row.get('missing_movies_json')
                    
                    if not isinstance(movies, list):
                        continue

                    movies_changed_in_this_collection = False
                    for movie in movies:
                        if movie.get('status') == 'missing':
                            movie['status'] = 'subscribed'
                            total_updated_movies += 1
                            movies_changed_in_this_collection = True
                    
                    if movies_changed_in_this_collection:
                        new_missing_json = json.dumps(movies, ensure_ascii=False)
                        cursor.execute(
                            "UPDATE collections_info SET missing_movies_json = %s, has_missing = FALSE WHERE emby_collection_id = %s",
                            (new_missing_json, collection_id)
                        )
                
                conn.commit()
                logger.info(f"DB: 成功将 {len(collection_ids)} 个合集中的 {total_updated_movies} 部缺失电影标记为已订阅。")

            except Exception as e_trans:
                conn.rollback()
                logger.error(f"批量标记已订阅的数据库事务失败，已回滚: {e_trans}", exc_info=True)
                raise
        
        return total_updated_movies

    except Exception as e:
        logger.error(f"DB: 批量标记电影为已订阅时发生错误: {e}", exc_info=True)
        raise

# ======================================================================
# 模块 6: 自定义合集数据访问 (custom_collections Data Access)
# ======================================================================

def create_custom_collection(name: str, type: str, definition_json: str, allowed_user_ids_json: Optional[str] = None) -> int:
    """【V2 - 权限系统版】创建一个新的自定义合集，包含权限控制。"""
    
    sql = """
        INSERT INTO custom_collections (name, type, definition_json, status, created_at, allowed_user_ids)
        VALUES (%s, %s, %s, 'active', NOW(), %s) 
        RETURNING id
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (name, type, definition_json, allowed_user_ids_json))
            
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建行的ID。")
            new_id = result['id']

            conn.commit()
            logger.info(f"成功创建自定义合集 '{name}' (类型: {type})。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except psycopg2.Error as e:
        logger.error(f"创建自定义合集 '{name}' 时发生非预期的数据库错误: {e}", exc_info=True)
        raise

def get_all_custom_collections() -> List[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM custom_collections
                ORDER BY sort_order ASC, id ASC
            """)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

def get_all_active_custom_collections() -> List[Dict[str, Any]]:
    """获取所有状态为 'active' 的自定义合集"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE status = 'active' ORDER BY sort_order ASC, id ASC")
            rows = cursor.fetchall()
            logger.trace(f"  ➜ 从数据库找到 {len(rows)} 个已启用的自定义合集。")
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有已启用的自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

def get_custom_collection_by_id(collection_id: int) -> Optional[Dict[str, Any]]:
    """根据ID获取单个自定义合集的详细信息。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE id = %s", (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except psycopg2.Error as e:
        logger.error(f"根据ID {collection_id} 获取自定义合集时发生数据库错误: {e}", exc_info=True)
        return None

def update_custom_collection(collection_id: int, name: str, type: str, definition_json: str, status: str, allowed_user_ids_json: Optional[str] = None) -> bool:
    """【V3 - 权限系统版】更新一个自定义合集，包含权限控制。"""
    
    sql = """
        UPDATE custom_collections
        SET name = %s, type = %s, definition_json = %s, status = %s, allowed_user_ids = %s
        WHERE id = %s
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (name, type, definition_json, status, allowed_user_ids_json, collection_id))
            
            if cursor.rowcount > 0:
                conn.commit()
                logger.info(f"  ✅ 成功更新自定义合集 ID: {name}。")
                return True
            else:
                logger.warning(f"尝试更新自定义合集 ID {collection_id}，但在数据库中未找到该记录。")
                conn.rollback()
                return False

    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 ID {collection_id} 时发生数据库错误: {e}", exc_info=True)
        return False

def delete_custom_collection(collection_id: int) -> bool:
    """【V5 - 职责单一版】从数据库中删除一个自定义合集定义。"""
    
    sql = "DELETE FROM custom_collections WHERE id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"  ✅ 成功从数据库中删除了自定义合集定义 (ID: {collection_id})。")
                return True
            else:
                logger.warning(f"尝试删除自定义合集 (ID: {collection_id})，但在数据库中未找到该记录。")
                return False
    except psycopg2.Error as e:
        logger.error(f"删除自定义合集 (ID: {collection_id}) 时发生数据库错误: {e}", exc_info=True)
        raise

def update_custom_collections_order(ordered_ids: List[int]) -> bool:
    """根据提供的ID列表，批量更新自定义合集的 sort_order。"""
    
    if not ordered_ids:
        return True

    sql = "UPDATE custom_collections SET sort_order = %s WHERE id = %s"
    data_to_update = [(index, collection_id) for index, collection_id in enumerate(ordered_ids)]

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany(sql, data_to_update)
            conn.commit()
            logger.info(f"成功更新了 {len(ordered_ids)} 个自定义合集的顺序。")
            return True
    except psycopg2.Error as e:
        logger.error(f"批量更新自定义合集顺序时发生数据库错误: {e}", exc_info=True)
        return False

# --- 自定义合集筛选引擎所需函数 ---

def get_media_metadata_by_tmdb_id(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """根据TMDb ID从媒体元数据缓存表中获取单条记录。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = %s", (tmdb_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID {tmdb_id} 获取媒体元数据时出错: {e}", exc_info=True)
        return None

def get_all_media_metadata(item_type: str = 'Movie') -> List[Dict[str, Any]]:
    """从媒体元数据缓存表中获取指定类型的所有记录。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE item_type = %s AND in_library = TRUE", (item_type,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有媒体元数据时出错 (类型: {item_type}): {e}", exc_info=True)
        return []

def get_unique_genres() -> List[str]:
    """【V2 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的类型。"""
    
    unique_genres = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT genres_json FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                genres = row['genres_json']
                if genres:
                    try:
                        for genre in genres:
                            if genre:
                                unique_genres.add(genre.strip())
                    except TypeError:
                        logger.warning(f"处理 genres_json 时遇到意外的类型错误，内容: {genres}")
                        continue
                        
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"从数据库中成功提取出 {len(sorted_genres)} 个唯一的电影类型。")
        return sorted_genres
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一电影类型时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_studios() -> List[str]:
    """【V3 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的工作室。"""
    
    unique_studios = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT studios_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                studios = row['studios_json']
                if studios:
                    try:
                        for studio in studios:
                            if studio:
                                unique_studios.add(studio.strip())
                    except TypeError:
                        logger.warning(f"处理 studios_json 时遇到意外的类型错误，内容: {studios}")
                        continue
                        
        sorted_studios = sorted(list(unique_studios))
        logger.trace(f"从数据库中成功提取出 {len(sorted_studios)} 个跨电影和电视剧的唯一工作室。")
        return sorted_studios
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一工作室时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_tags() -> List[str]:
    """【V2 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的标签。"""
    
    unique_tags = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tags_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                tags = row['tags_json']
                if tags:
                    try:
                        for tag in tags:
                            if tag:
                                unique_tags.add(tag.strip())
                    except TypeError:
                        logger.warning(f"处理 tags_json 时遇到意外的类型错误，内容: {tags}")
                        continue
                        
        sorted_tags = sorted(list(unique_tags))
        logger.trace(f"从数据库中成功提取出 {len(sorted_tags)} 个唯一的标签。")
        return sorted_tags
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一标签时发生数据库错误: {e}", exc_info=True)
        return []

def search_unique_studios(search_term: str, limit: int = 20) -> List[str]:
    """(V3 - 智能排序版) 搜索工作室并优先返回以 search_term 开头的结果。"""
    
    if not search_term:
        return []
    
    all_studios = get_unique_studios()
    
    if not all_studios:
        return []

    search_term_lower = search_term.lower()
    
    starts_with_matches = []
    contains_matches = []
    
    for studio in all_studios:
        studio_lower = studio.lower()
        if studio_lower.startswith(search_term_lower):
            starts_with_matches.append(studio)
        elif search_term_lower in studio_lower:
            contains_matches.append(studio)
            
    final_matches = starts_with_matches + contains_matches
    logger.trace(f"智能搜索 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
    return final_matches[:limit]

def search_unique_actors(search_term: str, limit: int = 20) -> List[str]:
    """【V6.1 - PG JSON 兼容版】搜索演员。"""
    
    if not search_term:
        return []
    
    unique_actors_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT actors_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                actors = row['actors_json']
                if actors:
                    try:
                        for actor in actors:
                            actor_name = actor.get('name')
                            original_name = actor.get('original_name')
                            
                            if actor_name and actor_name.strip():
                                if actor_name not in unique_actors_map:
                                    unique_actors_map[actor_name.strip()] = (original_name or '').strip()
                    except TypeError:
                        logger.warning(f"处理 actors_json 时遇到意外的类型错误，内容: {actors}")
                        continue
        
        if not unique_actors_map:
            return []

        search_term_lower = search_term.lower()
        starts_with_matches = []
        contains_matches = []
        
        for name, original_name in sorted(unique_actors_map.items()):
            name_lower = name.lower()
            original_name_lower = original_name.lower()

            if name_lower.startswith(search_term_lower) or (original_name_lower and original_name_lower.startswith(search_term_lower)):
                starts_with_matches.append(name)
            elif search_term_lower in name_lower or (original_name_lower and search_term_lower in original_name_lower):
                contains_matches.append(name)
        
        final_matches = starts_with_matches + contains_matches
        logger.trace(f"双语搜索演员 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
        return final_matches[:limit]
        
    except psycopg2.Error as e:
        logger.error(f"提取并搜索唯一演员时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_official_ratings():
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT split_part(official_rating, '-', 2) as rating
            FROM media_metadata
            WHERE official_rating IS NOT NULL AND official_rating LIKE '%-%' AND in_library = TRUE -- ### 修改 ###
            ORDER BY rating;
        """)
        return [row['rating'] for row in cursor.fetchall()]

def upsert_collection_info(collection_data: Dict[str, Any]):
    """写入或更新一条合集检查信息到 collections_info 表。"""
    
    sql = """
        INSERT INTO collections_info 
        (emby_collection_id, name, tmdb_collection_id, item_type, status, has_missing, 
        missing_movies_json, last_checked_at, poster_path, in_library_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (emby_collection_id) DO UPDATE SET
            name = EXCLUDED.name,
            tmdb_collection_id = EXCLUDED.tmdb_collection_id,
            item_type = EXCLUDED.item_type,
            status = EXCLUDED.status,
            has_missing = EXCLUDED.has_missing,
            missing_movies_json = EXCLUDED.missing_movies_json,
            last_checked_at = EXCLUDED.last_checked_at,
            poster_path = EXCLUDED.poster_path,
            in_library_count = EXCLUDED.in_library_count;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (
                collection_data.get('emby_collection_id'),
                collection_data.get('name'),
                collection_data.get('tmdb_collection_id'),
                collection_data.get('item_type'),
                collection_data.get('status'),
                collection_data.get('has_missing'),
                collection_data.get('missing_movies_json'),
                collection_data.get('last_checked_at'),
                collection_data.get('poster_path'),
                collection_data.get('in_library_count')
            ))
            conn.commit()
            logger.info(f"成功写入/更新合集检查信息到数据库 (ID: {collection_data.get('emby_collection_id')})。")
    except psycopg2.Error as e:
        logger.error(f"写入合集检查信息时发生数据库错误: {e}", exc_info=True)
        raise

def update_custom_collection_after_sync(collection_id: int, update_data: Dict[str, Any]) -> bool:
    """在同步任务完成后，更新自定义合集的状态。"""
    
    if not update_data:
        logger.warning(f"尝试更新自定义合集 {collection_id}，但没有提供任何更新数据。")
        return False

    set_clauses = [f"{key} = %s" for key in update_data.keys()]
    values = list(update_data.values())
    
    sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = %s"
    values.append(collection_id)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 的同步后状态。")
            return True
    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 {collection_id} 同步后状态时出错: {e}", exc_info=True)
        return False

def update_single_media_status_in_custom_collection(collection_id: int, media_tmdb_id: str, new_status: str) -> bool:
    """ 更新自定义合集中单个媒体项的状态。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT generated_media_info_json FROM custom_collections WHERE id = %s", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            media_items = row.get('generated_media_info_json')
            if not isinstance(media_items, list):
                media_items = []

            item_found = False
            for item in media_items:
                if str(item.get('tmdb_id')) == str(media_tmdb_id):
                    item['status'] = new_status
                    item_found = True
                    break
            
            if not item_found:
                conn.rollback()
                return False

            missing_count = sum(1 for item in media_items if item.get('status') == 'missing')
            new_health_status = 'has_missing' if missing_count > 0 else 'ok'
            
            update_data = {
                "generated_media_info_json": json.dumps(media_items, ensure_ascii=False),
                "missing_count": missing_count,
                "health_status": new_health_status
            }
            
            set_clauses = [f"{key} = %s" for key in update_data.keys()]
            values = list(update_data.values())
            sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = %s"
            values.append(collection_id)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 中媒体 {media_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新自定义合集中媒体状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

# --- 应用并持久化媒体修正 ---
def apply_and_persist_media_correction(collection_id: int, old_tmdb_id: str, new_tmdb_id: str, season_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    【V3 - 统一修正结构版】应用一个媒体修正，并持久化。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")

            cursor.execute(
                "SELECT definition_json, generated_media_info_json FROM custom_collections WHERE id = %s FOR UPDATE", 
                (collection_id,)
            )
            row = cursor.fetchone()
            if not row:
                conn.rollback(); return None

            definition = row.get('definition_json') or {}
            media_list = row.get('generated_media_info_json') or []

            item_type = definition.get('item_type', ['Movie'])[0]
            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            
            new_details = None
            if item_type == 'Series':
                new_details = tmdb_handler.get_tv_details(int(new_tmdb_id), api_key)
            else:
                new_details = tmdb_handler.get_movie_details(int(new_tmdb_id), api_key)

            if not new_details:
                conn.rollback(); return None

            cursor.execute("SELECT emby_item_id FROM media_metadata WHERE tmdb_id = %s", (new_tmdb_id,))
            metadata_row = cursor.fetchone()
            emby_id = metadata_row['emby_item_id'] if metadata_row else None
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            release_date = new_details.get("release_date") or new_details.get("first_air_date", '')
            
            status = "missing"
            if emby_id: status = "in_library"
            elif release_date and release_date > today_str: status = "unreleased"

            corrected_media_item = {
                "tmdb_id": new_tmdb_id, "emby_id": emby_id,
                "title": new_details.get("title") or new_details.get("name"),
                "release_date": release_date, "poster_path": new_details.get("poster_path"),
                "status": status
            }
            
            if item_type == 'Series' and season_number is not None:
                corrected_media_item['season'] = int(season_number)

            item_found = False
            for i, item in enumerate(media_list):
                if str(item.get('tmdb_id')) == str(old_tmdb_id):
                    media_list[i] = corrected_media_item
                    item_found = True
                    break
            
            if not item_found:
                logger.warning(f"修正警告：在合集 {collection_id} 的当前列表中未找到旧 ID {old_tmdb_id}，但仍会保存修正规则。")

            corrections = definition.get('corrections', {})
            
            # ★★★ 核心优化：无论修正的是什么类型，都保存统一的字典结构 ★★★
            final_season = int(season_number) if item_type == 'Series' and season_number is not None else None
            corrections[str(old_tmdb_id)] = {
                "tmdb_id": str(new_tmdb_id),
                "season": final_season
            }
            definition['corrections'] = corrections
            
            new_definition_json = json.dumps(definition, ensure_ascii=False)
            new_media_list_json = json.dumps(media_list, ensure_ascii=False)
            
            cursor.execute(
                "UPDATE custom_collections SET definition_json = %s, generated_media_info_json = %s WHERE id = %s",
                (new_definition_json, new_media_list_json, collection_id)
            )
            
            conn.commit()
            logger.info(f"成功为合集 {collection_id} 应用并保存修正：{old_tmdb_id} -> {new_tmdb_id} (季号: {final_season})")
            return corrected_media_item

    except Exception as e:
        if 'conn' in locals() and conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB: 应用媒体修正时发生严重错误: {e}", exc_info=True)
        raise

def match_and_update_list_collections_on_item_add(new_item_tmdb_id: str, new_item_emby_id: str, new_item_name: str) -> List[Dict[str, Any]]:
    """【V3 - PG JSONB 查询修复版】当新媒体入库时，查找并更新所有匹配的'list'类型合集。"""
    
    collections_to_update_in_emby = []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            sql_find = """
                SELECT * FROM custom_collections 
                WHERE type = 'list' 
                  AND status = 'active' 
                  AND emby_collection_id IS NOT NULL
                  AND generated_media_info_json @> %s::jsonb
            """
            search_payload = json.dumps([{'tmdb_id': str(new_item_tmdb_id)}])
            
            cursor.execute(sql_find, (search_payload,))
            candidate_collections = cursor.fetchall()

            if not candidate_collections:
                logger.debug(f"  ➜ 未在任何榜单合集中找到 TMDb ID: {new_item_tmdb_id}。")
                return []

            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in candidate_collections:
                    collection = dict(collection_row)
                    collection_id = collection['id']
                    collection_name = collection['name']
                    
                    try:
                        media_list = collection.get('generated_media_info_json') or []
                        item_found_and_updated = False
                        
                        for media_item in media_list:
                            if str(media_item.get('tmdb_id')) == str(new_item_tmdb_id) and media_item.get('status') != 'in_library':
                                old_status_key = media_item.get('status', 'unknown')
                                new_status_key = 'in_library'
                                old_status_cn = STATUS_TRANSLATION_MAP.get(old_status_key, old_status_key)
                                new_status_cn = STATUS_TRANSLATION_MAP.get(new_status_key, new_status_key)

                                logger.info(f"  ➜ 数据库状态更新：项目《{new_item_name}》在合集《{collection_name}》中的状态将从【{old_status_cn}】更新为【{new_status_cn}】。")
                                
                                media_item['status'] = new_status_key
                                media_item['emby_id'] = new_item_emby_id 
                                item_found_and_updated = True
                                break
                        
                        if item_found_and_updated:
                            new_in_library_count = sum(1 for m in media_list if m.get('status') == 'in_library')
                            new_missing_count = sum(1 for m in media_list if m.get('status') == 'missing')
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            new_json_data = json.dumps(media_list, ensure_ascii=False)
                            
                            cursor.execute("""
                                UPDATE custom_collections
                                SET generated_media_info_json = %s,
                                    in_library_count = %s,
                                    missing_count = %s,
                                    health_status = %s
                                WHERE id = %s
                            """, (new_json_data, new_in_library_count, new_missing_count, new_health_status, collection_id))
                            
                            collections_to_update_in_emby.append({
                                'emby_collection_id': collection['emby_collection_id'],
                                'name': collection_name
                            })

                    except (json.JSONDecodeError, TypeError) as e_json:
                        logger.warning(f"解析或处理榜单合集《{collection_name}》的数据时出错: {e_json}，跳过。")
                        continue
                
                conn.commit()
                
            except Exception as e_trans:
                conn.rollback()
                logger.error(f"在更新榜单合集数据库状态的事务中发生错误: {e_trans}", exc_info=True)
                raise

        return collections_to_update_in_emby

    except psycopg2.Error as e_db:
        logger.error(f"匹配和更新榜单合集时发生数据库错误: {e_db}", exc_info=True)
        raise

def get_media_metadata_by_tmdb_ids(tmdb_ids: List[str], item_type: str) -> List[Dict[str, Any]]:
    """根据 TMDb ID 列表批量获取媒体元数据。"""
    
    if not tmdb_ids:
        return []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM media_metadata WHERE item_type = %s AND tmdb_id = ANY(%s) AND in_library = TRUE"
            cursor.execute(sql, (item_type, tmdb_ids))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID列表批量获取媒体元数据时出错: {e}", exc_info=True)
        return []

def append_item_to_filter_collection_db(collection_id: int, new_item_tmdb_id: str, new_item_emby_id: str, collection_name: str, item_name: str) -> bool:
    """当新媒体项匹配规则筛选合集时，更新数据库状态。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT generated_media_info_json, in_library_count FROM custom_collections WHERE id = %s FOR UPDATE", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                logger.warning(f"尝试向规则合集 (DB ID: {collection_id}) 追加媒体项，但未找到该合集。")
                return False

            media_list = row.get('generated_media_info_json') or []
            if not isinstance(media_list, list):
                media_list = []
            
            if any(item.get('emby_id') == new_item_emby_id for item in media_list):
                conn.rollback()
                logger.debug(f"媒体项 {new_item_emby_id} 已存在于合集 {collection_id} 的JSON缓存中，跳过追加。")
                return True

            media_list.append({
                'tmdb_id': new_item_tmdb_id,
                'emby_id': new_item_emby_id
            })
            
            new_in_library_count = (row.get('in_library_count') or 0) + 1
            
            new_json_data = json.dumps(media_list, ensure_ascii=False)
            cursor.execute(
                "UPDATE custom_collections SET generated_media_info_json = %s, in_library_count = %s WHERE id = %s",
                (new_json_data, new_in_library_count, collection_id)
            )
            conn.commit()
            logger.info(f"  ➜ 数据库状态同步：已将新媒体项 《{item_name}》 追加到规则合集 《{collection_name}》。")
            return True

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        logger.error(f"向规则合集 {collection_id} 的JSON缓存追加媒体项时发生数据库错误: {e}", exc_info=True)
        return False