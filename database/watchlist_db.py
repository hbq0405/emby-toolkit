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
    """ 获取所有被追踪的剧集项目。"""
    sql = """
        SELECT 
            tmdb_id, -- ★★★ 直接使用 tmdb_id 作为主键 ★★★
            item_type, 
            title as item_name, 
            release_year,
            watching_status as status,
            paused_until, 
            force_ended, 
            watchlist_last_checked_at as last_checked_at,
            watchlist_tmdb_status as tmdb_status,
            watchlist_next_episode_json as next_episode_to_air_json,
            watchlist_missing_info_json as missing_info_json,
            watchlist_is_airing as is_airing,
            emby_item_ids_json -- 保留这个字段，以防前端其他地方需要
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

def find_detailed_missing_episodes(series_tmdb_ids: List[str]) -> List[Dict[str, Any]]:
    """
    【V3 - 健壮最终版】精确分析“中间缺失”，并返回订阅所需的所有元数据。
    - 额外返回季的 poster_path，解决海报回退异常问题。
    - 确保 missing_episodes 字段永远不会为 NULL。
    """
    if not series_tmdb_ids:
        return []

    logger.info("  ➜ 开始在本地数据库中执行精确的中间缺集分析...")
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                WITH season_stats AS (
                    SELECT
                        parent_series_tmdb_id,
                        season_number,
                        MAX(episode_number) FILTER (WHERE in_library = TRUE) as max_episode_in_library,
                        COUNT(*) FILTER (WHERE in_library = TRUE) as count_episodes_in_library
                    FROM media_metadata
                    WHERE
                        item_type = 'Episode'
                        AND parent_series_tmdb_id = ANY(%s)
                        AND season_number > 0
                    GROUP BY parent_series_tmdb_id, season_number
                )
                SELECT
                    s.parent_series_tmdb_id,
                    s.season_number,
                    -- ★★★ 修复1/2: 使用 COALESCE 确保 missing_episodes 永远是数组，而不是 NULL ★★★
                    COALESCE(
                        (SELECT array_agg(m.episode_number ORDER BY m.episode_number) FROM media_metadata m
                         WHERE m.parent_series_tmdb_id = s.parent_series_tmdb_id
                           AND m.season_number = s.season_number
                           AND m.in_library = FALSE),
                        '{}'::int[]
                    ) AS missing_episodes,
                    (SELECT tmdb_id FROM media_metadata m2
                     WHERE m2.parent_series_tmdb_id = s.parent_series_tmdb_id
                       AND m2.season_number = s.season_number
                       AND m2.item_type = 'Season' LIMIT 1) AS season_tmdb_id,
                    -- ★★★ 修复2/2: 额外查询出季的海报路径 ★★★
                    (SELECT poster_path FROM media_metadata m3
                     WHERE m3.parent_series_tmdb_id = s.parent_series_tmdb_id
                       AND m3.season_number = s.season_number
                       AND m3.item_type = 'Season' LIMIT 1) AS season_poster_path
                FROM season_stats s
                WHERE
                    s.count_episodes_in_library < s.max_episode_in_library
                    AND s.count_episodes_in_library > 0;
            """
            cursor.execute(sql, (series_tmdb_ids,))
            
            seasons_with_gaps = [dict(row) for row in cursor.fetchall()]
            
            logger.info(f"  ➜ 精确分析完成，共发现 {len(seasons_with_gaps)} 个季存在中间分集缺失。")
            return seasons_with_gaps

    except Exception as e:
        logger.error(f"在精确分析缺失分集时发生数据库错误: {e}", exc_info=True)
        return []
    
def batch_update_gaps_info(gaps_data: Dict[str, List[int]]):
    """
    批量更新多个剧集的“中间缺集”信息。
    这个函数会覆盖 watchlist_missing_info_json->'seasons_with_gaps' 的内容。
    如果一个剧集 ID 在 gaps_data 中对应一个空列表，则会清空它的缺集标记。

    :param gaps_data: 一个字典，键是 series_tmdb_id，值是包含缺集季号的列表。
                      例如: {'12345': [1, 3], '67890': []}
    """
    if not gaps_data:
        return

    # 将字典转换为适合 execute_values 的元组列表
    # 我们需要将季号列表转换为 JSON 字符串
    update_values = [
        (tmdb_id, json.dumps(season_numbers))
        for tmdb_id, season_numbers in gaps_data.items()
    ]

    sql = """
        UPDATE media_metadata AS mm
        SET
            -- 使用 jsonb_set 函数来精确地插入或替换 'seasons_with_gaps' 键
            -- COALESCE 确保即使原始 json 是 NULL 也能正常工作
            watchlist_missing_info_json = jsonb_set(
                COALESCE(mm.watchlist_missing_info_json, '{}'::jsonb),
                '{seasons_with_gaps}',
                v.gaps_json::jsonb,
                true -- 如果键不存在，则创建它
            )
        FROM (
            VALUES %s
        ) AS v(tmdb_id, gaps_json)
        WHERE mm.tmdb_id = v.tmdb_id AND mm.item_type = 'Series';
    """
    try:
        with get_db_connection() as conn:
            from psycopg2.extras import execute_values
            with conn.cursor() as cursor:
                execute_values(cursor, sql, update_values, page_size=1000)
            conn.commit()
            logger.info(f"DB: 成功批量更新了 {len(gaps_data)} 个剧集的中间缺集信息。")
    except Exception as e:
        logger.error(f"DB: 批量更新中间缺集信息时发生错误: {e}", exc_info=True)
        raise

def get_all_series_for_watchlist_scan() -> List[Dict[str, Any]]:
    """
    【新】为“一键扫描”任务从数据库获取所有剧集的基本信息。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT tmdb_id, title, emby_item_ids_json
                FROM media_metadata
                WHERE item_type = 'Series'
            """
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"为一键扫描任务获取所有剧集时出错: {e}", exc_info=True)
        return []

def batch_set_series_watching(tmdb_ids: List[str]):
    """
    【新】批量将一组指定的剧集状态更新为“追剧中”。
    同时会重置暂停日期和强制完结标记。
    """
    if not tmdb_ids:
        return
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 ANY(%s) 语法可以高效地处理列表
            sql = """
                UPDATE media_metadata
                SET
                    watching_status = 'Watching',
                    paused_until = NULL,
                    force_ended = FALSE
                WHERE
                    tmdb_id = ANY(%s) AND item_type = 'Series'
            """
            cursor.execute(sql, (tmdb_ids,))
            conn.commit()
            logger.info(f"成功将 {cursor.rowcount} 部剧集的状态批量更新为“追剧中”。")
    except Exception as e:
        conn.rollback()
        logger.error(f"批量更新剧集为“追剧中”时出错: {e}", exc_info=True)
        raise