# database/watchlist_db.py
import psycopg2
import logging
import json
from typing import List, Dict, Any, Optional

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 追剧数据访问
# ======================================================================

def get_all_watchlist_items() -> List[Dict[str, Any]]:
    """【新架构】获取所有被追踪的剧集项目。"""
    sql = """
        SELECT 
            tmdb_id, item_type, title as item_name, release_year,
            watching_status as status,
            paused_until, force_ended, watchlist_last_checked_at as last_checked_at,
            watchlist_tmdb_status as tmdb_status,
            watchlist_next_episode_json as next_episode_to_air_json,
            watchlist_missing_info_json as missing_info_json,
            watchlist_is_airing as is_airing,
            
            -- ★★★ 核心修复：从 JSON 数组中提取第一个 Emby ID 作为主 ID ★★★
            emby_item_ids_json,
            emby_item_ids_json->>0 AS item_id  -- 提取第一个元素作为 item_id

        FROM media_metadata
        WHERE item_type = 'Series' AND watching_status != 'NONE'
        ORDER BY first_requested_at DESC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB (新架构): 获取追剧列表失败: {e}", exc_info=True)
        raise

def add_item_to_watchlist(tmdb_id: str, item_name: str) -> bool:
    """【新架构】将一个剧集标记为“正在追剧”。"""
    sql = """
        UPDATE media_metadata
        SET watching_status = 'Watching',
            -- 首次添加时，清空可能存在的旧状态
            paused_until = NULL,
            force_ended = FALSE
        WHERE tmdb_id = %s AND item_type = 'Series';
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_id,))
            # 如果没有行被更新（说明 media_metadata 里还没有这条记录），则插入一条
            if cursor.rowcount == 0:
                insert_sql = """
                    INSERT INTO media_metadata (tmdb_id, item_type, title, watching_status)
                    VALUES (%s, 'Series', %s, 'Watching')
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET watching_status = 'Watching';
                """
                cursor.execute(insert_sql, (tmdb_id, item_name))
            return True
    except Exception as e:
        logger.error(f"DB (新架构): 添加 '{item_name}' 到追剧列表失败: {e}", exc_info=True)
        raise

def update_watchlist_item_status(tmdb_id: str, new_status: str) -> bool:
    """【新架构】更新剧集项目的追剧状态。"""
    updates = {"watching_status": new_status}
    if new_status == 'Watching':
        updates["force_ended"] = False
        updates["paused_until"] = None
    
    set_clauses = [f"{key} = %s" for key in updates.keys()]
    values = list(updates.values())
    values.append(tmdb_id)
    
    sql = f"UPDATE media_metadata SET {', '.join(set_clauses)} WHERE tmdb_id = %s AND item_type = 'Series'"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB (新架构): 更新追剧状态失败: {e}", exc_info=True)
        raise

def remove_item_from_watchlist(tmdb_id: str) -> bool:
    """【新架构】将一个剧集从追剧列表中移除（重置其追剧状态）。"""
    # 我们不删除记录，只是重置追剧相关的字段
    sql = """
        UPDATE media_metadata
        SET watching_status = 'NONE',
            paused_until = NULL,
            force_ended = FALSE,
            watchlist_last_checked_at = NULL,
            watchlist_tmdb_status = NULL,
            watchlist_next_episode_json = NULL,
            watchlist_missing_info_json = NULL,
            watchlist_is_airing = FALSE
        WHERE tmdb_id = %s AND item_type = 'Series';
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_id,))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB (新架构): 从追剧列表移除项目时失败: {e}", exc_info=True)
        raise

def get_watchlist_item_name(tmdb_id: str) -> Optional[str]:
    """【新架构】根据 tmdb_id 获取单个追剧项目的名称。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
            row = cursor.fetchone()
            return row['title'] if row else None
    except Exception as e:
        logger.warning(f"DB (新架构): 获取项目 {tmdb_id} 名称时出错: {e}")
        return None

def batch_force_end_watchlist_items(tmdb_ids: List[str]) -> int:
    """【新架构】批量将追剧项目标记为“强制完结”，并同步更新其“在播”状态。"""
    if not tmdb_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('%s' for _ in tmdb_ids)
            sql = f"""
                UPDATE media_metadata
                SET watching_status = 'Completed',
                    force_ended = TRUE,
                    watchlist_is_airing = FALSE
                WHERE tmdb_id IN ({placeholders}) AND item_type = 'Series'
            """
            cursor.execute(sql, tmdb_ids)
            conn.commit()
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"DB (新架构): 批量强制完结了 {updated_count} 个追剧项目，并同步更新了其在播状态。")
            else:
                logger.warning(f"DB (新架构): 尝试批量强制完结，但提供的ID在列表中均未找到。")
            return updated_count
    except Exception as e:
        logger.error(f"DB (新架构): 批量强制完结追剧项目时发生错误: {e}", exc_info=True)
        raise

def batch_update_watchlist_status(tmdb_ids: list, new_status: str) -> int:
    """【新架构】批量更新指定项目ID列表的追剧状态，并智能处理关联字段。"""
    if not tmdb_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            updates = {"watching_status": new_status}
            if new_status == 'Watching':
                updates["force_ended"] = False
                updates["paused_until"] = None
            
            set_clauses = [f"{key} = %s" for key in updates.keys()]
            set_clauses.append("watchlist_last_checked_at = NOW()") 
            
            values = list(updates.values())
            
            placeholders = ', '.join(['%s'] * len(tmdb_ids))
            sql = f"UPDATE media_metadata SET {', '.join(set_clauses)} WHERE tmdb_id IN ({placeholders}) AND item_type = 'Series'"
            
            values.extend(tmdb_ids)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            logger.info(f"DB (新架构): 成功将 {cursor.rowcount} 个项目的状态批量更新为 '{new_status}'，并重置了关联状态。")
            return cursor.rowcount
            
    except Exception as e:
        logger.error(f"DB (新架构): 批量更新项目状态时数据库出错: {e}", exc_info=True)
        raise

def get_watching_tmdb_ids() -> set:
    """【新架构】获取所有正在追看（状态为 'Watching'）的剧集的 TMDB ID 集合。"""
    watching_ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata WHERE watching_status = 'Watching' AND item_type = 'Series'")
            rows = cursor.fetchall()
            for row in rows:
                watching_ids.add(str(row['tmdb_id']))
    except Exception as e:
        logger.error(f"DB (新架构): 从数据库获取正在追看的TMDB ID时出错: {e}", exc_info=True)
    return watching_ids

def get_airing_series_tmdb_ids() -> set:
    """
    【新架构】获取所有被标记为“正在连载”的剧集的 TMDb ID 集合。
    这个函数直接查询 watchlist_is_airing = TRUE 的记录，简单、快速、准确。
    """
    airing_ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT tmdb_id FROM media_metadata WHERE watchlist_is_airing = TRUE AND item_type = 'Series'"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                airing_ids.add(str(row['tmdb_id']))
        logger.debug(f"DB (新架构): 通过 watchlist_is_airing 标志查询到 {len(airing_ids)} 个“连载中”的剧集。")
        return airing_ids
    except Exception as e:
        logger.error(f"DB (新架构): 从数据库获取“连载中”剧集ID时出错: {e}", exc_info=True)
        return set()
    
def get_watchlist_item_details(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """【新架构】根据 tmdb_id 获取单个追剧项目的完整字典信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT 
                    tmdb_id, item_type, title as item_name, release_year,
                    watching_status as status,
                    paused_until, force_ended, watchlist_last_checked_at as last_checked_at,
                    watchlist_tmdb_status as tmdb_status,
                    watchlist_next_episode_json as next_episode_to_air_json,
                    watchlist_missing_info_json as missing_info_json,
                    watchlist_is_airing as is_airing
                FROM media_metadata
                WHERE tmdb_id = %s AND item_type = 'Series';
            """
            cursor.execute(sql, (tmdb_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB (新架构): 获取项目 {tmdb_id} 详情时出错: {e}", exc_info=True)
        return None

def remove_seasons_from_gaps_list(tmdb_id: str, seasons_to_remove: List[int]):
    """【新架构】从指定项目的 watchlist_missing_info_json['seasons_with_gaps'] 列表中移除指定的季号。"""
    if not seasons_to_remove:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT watchlist_missing_info_json FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
                row = cursor.fetchone()
                if not row or not row.get('watchlist_missing_info_json'):
                    return

                missing_info = row['watchlist_missing_info_json']
                
                current_gaps = missing_info.get('seasons_with_gaps', [])
                if not current_gaps:
                    return
                
                updated_gaps = [s for s in current_gaps if s not in seasons_to_remove]
                missing_info['seasons_with_gaps'] = updated_gaps
                
                updated_json_str = json.dumps(missing_info)
                cursor.execute(
                    "UPDATE media_metadata SET watchlist_missing_info_json = %s WHERE tmdb_id = %s AND item_type = 'Series'",
                    (updated_json_str, tmdb_id)
                )
            conn.commit()
            logger.info(f"DB (新架构): 已为项目 {tmdb_id} 更新缺集标记，移除了季: {seasons_to_remove}")
    except Exception as e:
        logger.error(f"DB (新架构): 更新项目 {tmdb_id} 的缺集标记时出错: {e}", exc_info=True)

def batch_remove_from_watchlist(tmdb_ids: List[str]) -> int:
    """【新架构】从追剧列表中批量移除多个项目（重置其追剧状态）。"""
    if not tmdb_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('%s' for _ in tmdb_ids)
            sql = f"""
                UPDATE media_metadata
                SET watching_status = 'NONE',
                    paused_until = NULL,
                    force_ended = FALSE,
                    watchlist_last_checked_at = NULL,
                    watchlist_tmdb_status = NULL,
                    watchlist_next_episode_json = NULL,
                    watchlist_missing_info_json = NULL,
                    watchlist_is_airing = FALSE
                WHERE tmdb_id IN ({placeholders}) AND item_type = 'Series';
            """
            cursor.execute(sql, tmdb_ids)
            conn.commit()
            removed_count = cursor.rowcount
            if removed_count > 0:
                logger.info(f"DB (新架构): 成功从追剧列表批量移除了 {removed_count} 个项目。")
            return removed_count
    except Exception as e:
        logger.error(f"DB (新架构): 批量移除追剧项目时发生错误: {e}", exc_info=True)
        raise
