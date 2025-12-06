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
    """ 
    增加剧集层面的状态和统计数据，用于前端聚合展示。
    """
    sql = """
        SELECT 
            s.tmdb_id, 
            'Season' as item_type,
            p.title || ' 第 ' || s.season_number || ' 季' as item_name,
            s.season_number,
            p.tmdb_id as parent_tmdb_id,
            s.release_date as release_year,
            
            -- 季的状态 (用于筛选)
            COALESCE(NULLIF(s.watching_status, 'NONE'), p.watching_status) as status,
            
            -- ★★★ 新增 1：剧集层面的状态 (用于已完结卡片的 UI 判断) ★★★
            p.watching_status as series_status,

            p.watchlist_last_checked_at as last_checked_at,
            p.watchlist_next_episode_json as next_episode_to_air_json,
            p.watchlist_missing_info_json as missing_info_json,
            p.emby_item_ids_json,
            p.watchlist_tmdb_status as tmdb_status, -- 确保拿到剧集的 TMDb 状态
            
            -- 季层面的统计 (用于追剧视图)
            (SELECT COUNT(*) FROM media_metadata e 
             WHERE e.parent_series_tmdb_id = s.parent_series_tmdb_id 
               AND e.season_number = s.season_number 
               AND e.item_type = 'Episode' 
               AND e.in_library = TRUE) as collected_count,
               
            COALESCE(NULLIF(s.total_episodes, 0), 
                (SELECT COUNT(*) FROM media_metadata e 
                 WHERE e.parent_series_tmdb_id = s.parent_series_tmdb_id 
                   AND e.season_number = s.season_number 
                   AND e.item_type = 'Episode')
            ) as total_count,
            
            -- ★★★ 新增 2：剧集层面的统计 (用于已完结视图的进度条) ★★★
            (SELECT COUNT(*) FROM media_metadata e 
             WHERE e.parent_series_tmdb_id = p.tmdb_id 
               AND e.item_type = 'Episode' 
               AND e.in_library = TRUE) as series_collected_count,
               
            p.total_episodes as series_total_episodes,
            
            s.total_episodes_locked

        FROM media_metadata s
        JOIN media_metadata p ON s.parent_series_tmdb_id = p.tmdb_id
        WHERE 
            s.item_type = 'Season'
            AND s.season_number > 0
            AND p.item_type = 'Series'
            AND p.watching_status != 'NONE'
            AND (
                (s.total_episodes = 0 OR 
                 (SELECT COUNT(*) FROM media_metadata e 
                  WHERE e.parent_series_tmdb_id = s.parent_series_tmdb_id 
                    AND e.season_number = s.season_number 
                    AND e.in_library = TRUE) < s.total_episodes)
                OR
                s.season_number = (
                    SELECT MAX(season_number) FROM media_metadata m3 
                    WHERE m3.parent_series_tmdb_id = p.tmdb_id 
                      AND m3.item_type = 'Season'
                )
                OR p.watching_status IN ('Completed', 'Paused')
            )
        ORDER BY p.first_requested_at DESC, s.season_number ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取追剧列表失败: {e}", exc_info=True)
        raise

def add_item_to_watchlist(tmdb_id: str, item_name: str) -> bool:
    """
    将一个剧集标记为“正在追剧”。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 插入或更新 Series 本身
            upsert_sql = """
                INSERT INTO media_metadata (tmdb_id, item_type, title, watching_status, force_ended, paused_until)
                VALUES (%s, 'Series', %s, 'Watching', FALSE, NULL)
                ON CONFLICT (tmdb_id, item_type) 
                DO UPDATE SET 
                    watching_status = 'Watching',
                    force_ended = FALSE,
                    paused_until = NULL;
            """
            cursor.execute(upsert_sql, (tmdb_id, item_name))
            
            # 2. ★★★ 关键修复：重置该剧集下所有子项的状态为 NONE ★★★
            # 这样子项就会自动继承父级的 'Watching' 状态，避免旧的 'Completed' 状态干扰
            reset_children_sql = """
                UPDATE media_metadata
                SET watching_status = 'NONE'
                WHERE parent_series_tmdb_id = %s;
            """
            cursor.execute(reset_children_sql, (tmdb_id,))
            
            return True
    except Exception as e:
        logger.error(f"  ➜ 添加 '{item_name}' 到追剧列表失败: {e}", exc_info=True)
        raise

def update_watchlist_item_status(tmdb_id: str, new_status: str) -> bool:
    """
    更新单个剧集项目的追剧状态。
    """
    updates = {"watching_status": new_status}
    if new_status == 'Watching':
        updates["force_ended"] = False
        updates["paused_until"] = None
    
    set_clauses = [f"{key} = %s" for key in updates.keys()]
    # 追加更新时间
    set_clauses.append("watchlist_last_checked_at = NOW()")
    
    values = list(updates.values())
    
    # ★★★ 级联更新 SQL ★★★
    sql = f"""
        UPDATE media_metadata 
        SET {', '.join(set_clauses)} 
        WHERE 
            (tmdb_id = %s AND item_type = 'Series')
            OR
            (parent_series_tmdb_id = %s)
    """
    
    # 追加 WHERE 参数
    values.append(tmdb_id)
    values.append(tmdb_id)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 更新追剧状态 {tmdb_id} 失败: {e}", exc_info=True)
        raise

def remove_item_from_watchlist(tmdb_id: str) -> bool:
    """
    将一个剧集从追剧列表中移除。
    """
    sql = """
        UPDATE media_metadata
        SET watching_status = 'NONE',
            paused_until = NULL,
            force_ended = FALSE,
            watchlist_last_checked_at = NULL,
            watchlist_tmdb_status = NULL,
            watchlist_next_episode_json = NULL,
            watchlist_missing_info_json = NULL,
            watchlist_is_airing = FALSE,
            -- 同时重置订阅状态，防止残留
            subscription_status = 'NONE',
            subscription_sources_json = '[]'::jsonb,
            ignore_reason = NULL
        WHERE 
            -- 1. 匹配剧集本身
            (tmdb_id = %s AND item_type = 'Series')
            OR
            -- 2. 匹配该剧集下的所有子项
            (parent_series_tmdb_id = %s);
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 传入两次 tmdb_id
            cursor.execute(sql, (tmdb_id, tmdb_id))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 从追剧列表移除项目 {tmdb_id} 时失败: {e}", exc_info=True)
        raise

def get_watchlist_item_name(tmdb_id: str) -> Optional[str]:
    """根据 tmdb_id 获取单个追剧项目的名称。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
            row = cursor.fetchone()
            return row['title'] if row else None
    except Exception as e:
        logger.warning(f"  ➜ 获取项目 {tmdb_id} 名称时出错: {e}")
        return None

def batch_force_end_watchlist_items(tmdb_ids: List[str]) -> int:
    """
    批量将追剧项目标记为“强制完结”。
    """
    if not tmdb_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复：使用 OR 逻辑同时匹配父剧集和子项 ★★★
            sql = """
                UPDATE media_metadata
                SET watching_status = 'Completed',
                    force_ended = TRUE,
                    watchlist_is_airing = FALSE
                WHERE 
                    -- 1. 匹配剧集本身
                    (tmdb_id = ANY(%s) AND item_type = 'Series')
                    OR
                    -- 2. 匹配该剧集下的所有子项 (季、集)
                    (parent_series_tmdb_id = ANY(%s))
            """
            # 注意：需要传入两次 tmdb_ids，分别对应两个 ANY(%s)
            cursor.execute(sql, (tmdb_ids, tmdb_ids))
            conn.commit()
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"  ➜ 批量强制完结了 {len(tmdb_ids)} 个剧集系列，共影响 {updated_count} 条记录(含子项)。")
            return updated_count
    except Exception as e:
        logger.error(f"  ➜ 批量强制完结追剧项目时发生错误: {e}", exc_info=True)
        raise

def batch_update_watchlist_status(item_ids: list, new_status: str) -> int:
    """
    批量更新指定项目ID列表的追剧状态。
    """
    if not item_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 准备更新字段
            updates = {"watching_status": new_status}
            
            # 如果是“追剧中”，需要清除暂停和强制完结标记
            if new_status == 'Watching':
                updates["force_ended"] = False
                updates["paused_until"] = None
            
            # 构建 SET 子句
            set_clauses = [f"{key} = %s" for key in updates.keys()]
            set_clauses.append("watchlist_last_checked_at = NOW()") 
            
            # 构建参数值：先放入 SET 的值
            values = list(updates.values())
            
            # ★★★ 级联更新 SQL ★★★
            sql = f"""
                UPDATE media_metadata 
                SET {', '.join(set_clauses)} 
                WHERE 
                    -- 1. 匹配剧集本身
                    (tmdb_id = ANY(%s) AND item_type = 'Series')
                    OR
                    -- 2. 匹配该剧集下的所有子项
                    (parent_series_tmdb_id = ANY(%s))
            """
            
            # 追加 WHERE 子句的参数 (两次 item_ids)
            values.append(item_ids)
            values.append(item_ids)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            logger.info(f"  ➜ 成功将 {len(item_ids)} 个剧集系列的状态批量更新为 '{new_status}'，共影响 {cursor.rowcount} 条记录。")
            return cursor.rowcount
            
    except Exception as e:
        logger.error(f"  ➜ 批量更新项目状态时数据库出错: {e}", exc_info=True)
        raise

def get_watching_tmdb_ids() -> set:
    """获取所有正在追看（状态为 'Watching'）的剧集的 TMDB ID 集合。"""
    watching_ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata WHERE watching_status = 'Watching' AND item_type = 'Series'")
            rows = cursor.fetchall()
            for row in rows:
                watching_ids.add(str(row['tmdb_id']))
    except Exception as e:
        logger.error(f"  ➜ 从数据库获取正在追看的TMDB ID时出错: {e}", exc_info=True)
    return watching_ids

def get_airing_series_tmdb_ids() -> set:
    """
    获取所有被标记为“正在连载”的剧集的 TMDb ID 集合。
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
        logger.debug(f"  ➜ 通过 watchlist_is_airing 标志查询到 {len(airing_ids)} 个“连载中”的剧集。")
        return airing_ids
    except Exception as e:
        logger.error(f"  ➜ 从数据库获取“连载中”剧集ID时出错: {e}", exc_info=True)
        return set()
    
def get_watchlist_item_details(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """根据 tmdb_id 获取单个追剧项目的完整字典信息。"""
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
        logger.error(f"  ➜ 获取项目 {tmdb_id} 详情时出错: {e}", exc_info=True)
        return None

def remove_seasons_from_gaps_list(tmdb_id: str, seasons_to_remove: List[int]):
    """从指定项目的 watchlist_missing_info_json['seasons_with_gaps'] 列表中移除指定的季号。"""
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
            logger.info(f"  ➜ 已为项目 {tmdb_id} 更新缺集标记，移除了季: {seasons_to_remove}")
    except Exception as e:
        logger.error(f"  ➜ 更新项目 {tmdb_id} 的缺集标记时出错: {e}", exc_info=True)

def batch_remove_from_watchlist(tmdb_ids: List[str]) -> int:
    """
    从追剧列表中批量移除多个项目。
    这个操作现在会彻底重置剧集本身及其所有关联子项（季、集）的
    追剧状态和订阅状态，以完全符合用户“不再关注此剧”的意图。
    """
    if not tmdb_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修改：一个 SQL 语句同时更新剧集本身和它所有的子项 ★★★
            sql = f"""
                UPDATE media_metadata
                SET 
                    -- 1. 重置追剧相关的所有字段
                    watching_status = 'NONE',
                    paused_until = NULL,
                    force_ended = FALSE,
                    watchlist_last_checked_at = NULL,
                    watchlist_tmdb_status = NULL,
                    watchlist_next_episode_json = NULL,
                    watchlist_missing_info_json = NULL,
                    watchlist_is_airing = FALSE,
                    
                    -- 2. ★★★ 关键：同时重置订阅状态，斩草除根 ★★★
                    subscription_status = 'NONE',
                    subscription_sources_json = '[]'::jsonb,
                    ignore_reason = NULL

                WHERE
                    -- 条件A: 匹配剧集本身 (顶层项目)
                    (tmdb_id = ANY(%s) AND item_type = 'Series')
                    OR
                    -- 条件B: 匹配该剧集下的所有子项 (季和集)
                    (parent_series_tmdb_id = ANY(%s));
            """
            # 需要将 tmdb_ids 列表传递两次，分别对应两个 ANY(%s)
            cursor.execute(sql, (tmdb_ids, tmdb_ids))
            conn.commit()
            
            removed_count = cursor.rowcount
            if removed_count > 0:
                # 日志现在应该反映出操作的范围更广了
                logger.info(f"  ➜ 成功从追剧列表批量移除了 {len(tmdb_ids)} 个剧集，并重置了总共 {removed_count} 个相关条目（包括子项）的追剧和订阅状态。")
            return removed_count
    except Exception as e:
        logger.error(f"  ➜ 批量移除追剧项目时发生错误: {e}", exc_info=True)
        raise

def find_detailed_missing_episodes(series_tmdb_ids: List[str]) -> List[Dict[str, Any]]:
    """
    使用 generate_series 精确计算所有类型的缺失集。
    - 能够正确处理“记录不存在”和“记录标记为不在库”两种缺失情况。
    """
    if not series_tmdb_ids:
        return []

    logger.info("  ➜ 开始在本地数据库中执行中间缺集分析...")
    
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
                    (
                        SELECT COALESCE(array_agg(missing_num ORDER BY missing_num), '{}'::int[])
                        FROM (
                            -- 1. 生成从 1 到最大集号的完整序列
                            SELECT generate_series(1, s.max_episode_in_library) AS missing_num
                            
                            EXCEPT
                            
                            -- 2. 减去所有在库的集号
                            SELECT episode_number FROM media_metadata m
                            WHERE m.parent_series_tmdb_id = s.parent_series_tmdb_id
                              AND m.season_number = s.season_number
                              AND m.in_library = TRUE
                        ) AS missing_numbers
                    ) AS missing_episodes,
                    (SELECT tmdb_id FROM media_metadata m2
                     WHERE m2.parent_series_tmdb_id = s.parent_series_tmdb_id
                       AND m2.season_number = s.season_number
                       AND m2.item_type = 'Season' LIMIT 1) AS season_tmdb_id,
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
            
            logger.info(f"  ➜ 分析完成，共发现 {len(seasons_with_gaps)} 个季存在中间分集缺失。")
            return seasons_with_gaps

    except Exception as e:
        logger.error(f"  ➜ 在分析缺失分集时发生数据库错误: {e}", exc_info=True)
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
            logger.info(f"  ➜ 成功批量更新了 {len(gaps_data)} 个剧集的中间缺集信息。")
    except Exception as e:
        logger.error(f"  ➜ 批量更新中间缺集信息时发生错误: {e}", exc_info=True)
        raise

def get_all_series_for_watchlist_scan() -> List[Dict[str, Any]]:
    """
    为“一键扫描”任务从数据库获取所有剧集的基本信息。
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
        logger.error(f"  ➜ 为一键扫描任务获取所有剧集时出错: {e}", exc_info=True)
        return []

def batch_set_series_watching(tmdb_ids: List[str]):
    """
    批量将一组指定的剧集状态更新为“追剧中”。
    修复：级联更新子项。
    """
    if not tmdb_ids:
        return
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE media_metadata
                SET
                    watching_status = 'Watching',
                    paused_until = NULL,
                    force_ended = FALSE
                WHERE
                    (tmdb_id = ANY(%s) AND item_type = 'Series')
                    OR
                    (parent_series_tmdb_id = ANY(%s))
            """
            cursor.execute(sql, (tmdb_ids, tmdb_ids))
            conn.commit()
            logger.info(f"  ➜ 成功将 {len(tmdb_ids)} 部剧集及其子项批量更新为“追剧中”。")
    except Exception as e:
        conn.rollback()
        logger.error(f"  ➜ 批量更新剧集为“追剧中”时出错: {e}", exc_info=True)
        raise

def sync_seasons_watching_status(parent_tmdb_id: str, active_season_numbers: List[int], series_status: str):
    """
    同步更新指定剧集下所有季的追剧状态。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 场景 A: 剧集整体已完结 -> 所有季标记为已完结
            if series_status == 'Completed':
                sql = """
                    UPDATE media_metadata
                    SET watching_status = 'Completed'
                    WHERE parent_series_tmdb_id = %s 
                      AND item_type = 'Season'
                      AND watching_status != 'Completed';
                """
                cursor.execute(sql, (parent_tmdb_id,))

            # 场景 B: 剧集正在追/暂停 -> 区分活跃季和过往季
            else:
                # 1. 找出当前最大的活跃季号 (例如怪奇物语是 5)
                max_active_season = max(active_season_numbers) if active_season_numbers else 0
                
                # 2. 【旧季】小于最大活跃季号的 -> 标记为 'Completed'
                #    (例如 S1-S4 设为 Completed)
                if max_active_season > 0:
                    reset_sql = """
                        UPDATE media_metadata
                        SET watching_status = 'Completed'
                        WHERE parent_series_tmdb_id = %s 
                          AND item_type = 'Season'
                          AND season_number < %s  -- 注意这里是 < (小于)
                          AND watching_status != 'Completed';
                    """
                    cursor.execute(reset_sql, (parent_tmdb_id, max_active_season))
                
                # 3. 【活跃季】在列表中的季 -> 标记为 series_status (Watching 或 Paused)
                #    (例如 S5 设为 Paused)
                if active_season_numbers:
                    update_active_sql = """
                        UPDATE media_metadata
                        SET watching_status = %s  -- ★★★ 关键：使用传入的 series_status ★★★
                        WHERE parent_series_tmdb_id = %s 
                          AND item_type = 'Season'
                          AND season_number = ANY(%s);
                    """
                    cursor.execute(update_active_sql, (series_status, parent_tmdb_id, active_season_numbers))
                    
                logger.info(f"  ➜ 更新剧集 {parent_tmdb_id} 的季状态: 活跃季 {active_season_numbers} -> {series_status}，旧季 -> 已完结。")

            conn.commit()
    except Exception as e:
        logger.error(f"  ➜ 同步剧集 {parent_tmdb_id} 的季状态时出错: {e}", exc_info=True)

def batch_set_series_watching_by_libraries(library_ids: Optional[List[str]] = None) -> int:
    """
    批量将剧集标记为“追剧中”。
    修复：级联更新子项。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 基础更新逻辑
            update_clause = """
                UPDATE media_metadata
                SET watching_status = 'Watching',
                    paused_until = NULL,
                    force_ended = FALSE
            """
            
            if not library_ids:
                # 情况 A: 更新所有 Series 及其子项
                # 注意：这里如果全量更新可能会比较慢，但逻辑上是正确的
                # 为了性能，我们限制只更新那些 Series 已经是 Watching 或者 NONE 的相关子项可能更好？
                # 但为了绝对的一致性，这里还是全量匹配
                sql = update_clause + """
                    WHERE (item_type = 'Series') OR (parent_series_tmdb_id IS NOT NULL)
                """
                cursor.execute(sql)
            else:
                # 情况 B: 指定库
                target_lib_ids = [str(lib_id) for lib_id in library_ids]
                
                # 我们需要先找出符合条件的 Series ID 列表
                find_ids_sql = """
                    SELECT DISTINCT tmdb_id FROM media_metadata WHERE item_type = 'Series' AND (
                        -- 1. 自身在库
                        (in_library = TRUE AND asset_details_json IS NOT NULL AND EXISTS (
                            SELECT 1 FROM jsonb_array_elements(asset_details_json) AS elem
                            WHERE elem->>'source_library_id' = ANY(%s)
                        ))
                        OR
                        -- 2. 子集在库 (反查)
                        (tmdb_id IN (
                            SELECT DISTINCT parent_series_tmdb_id FROM media_metadata
                            WHERE item_type = 'Episode' AND in_library = TRUE AND asset_details_json IS NOT NULL AND EXISTS (
                                SELECT 1 FROM jsonb_array_elements(asset_details_json) AS elem
                                WHERE elem->>'source_library_id' = ANY(%s)
                            )
                        ))
                    )
                """
                cursor.execute(find_ids_sql, (target_lib_ids, target_lib_ids))
                target_tmdb_ids = [row['tmdb_id'] for row in cursor.fetchall()]
                
                if not target_tmdb_ids:
                    return 0
                
                # 执行级联更新
                sql = update_clause + """
                    WHERE (tmdb_id = ANY(%s) AND item_type = 'Series')
                       OR (parent_series_tmdb_id = ANY(%s))
                """
                cursor.execute(sql, (target_tmdb_ids, target_tmdb_ids))

            updated_count = cursor.rowcount
            logger.info(f"  ➜ 已将指定库中的剧集及其子项标记为“追剧中”，影响行数: {updated_count}。")
            return updated_count

    except Exception as e:
        logger.error(f"  ➜ 按库批量更新剧集状态时出错: {e}", exc_info=True)
        raise

def _build_library_filter_sql(library_ids: List[str]) -> str:
    """
    (内部辅助) 构建用于筛选媒体库的 SQL 片段。
    逻辑：剧集本身在库中 OR 剧集的任意一集在库中。
    """
    # 确保 ID 是字符串
    lib_ids_str = [str(lid) for lid in library_ids]
    # 将列表转为 SQL 数组字符串，例如: '{123, 456}'
    array_literal = "{" + ",".join(lib_ids_str) + "}"
    
    return f"""
        AND tmdb_id IN (
            -- 1. 通过单集反查
            SELECT DISTINCT parent_series_tmdb_id
            FROM media_metadata
            WHERE item_type = 'Episode'
              AND in_library = TRUE
              AND asset_details_json IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(asset_details_json) AS elem
                  WHERE elem->>'source_library_id' = ANY('{array_literal}'::text[])
              )
            
            UNION
            
            -- 2. 直接查剧集 (防备 Series 也有资产信息)
            SELECT tmdb_id
            FROM media_metadata
            WHERE item_type = 'Series'
              AND in_library = TRUE
              AND asset_details_json IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(asset_details_json) AS elem
                  WHERE elem->>'source_library_id' = ANY('{array_literal}'::text[])
              )
        )
    """

def get_gap_scan_candidates(library_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    获取“缺集扫描”任务的候选剧集。
    
    筛选条件（全部在 SQL 中完成）：
    1. item_type = 'Series'
    2. 状态不是 'Watching' 或 'Paused' (由主任务负责)
    3. 订阅状态不是 'IGNORED' (尊重用户选择)
    4. (可选) 属于指定的媒体库
    """
    base_sql = """
        SELECT tmdb_id, title as item_name, watching_status as status, subscription_status
        FROM media_metadata
        WHERE item_type = 'Series'
          AND watching_status NOT IN ('Watching', 'Paused')
          AND subscription_status != 'IGNORED'
    """
    
    if library_ids:
        base_sql += _build_library_filter_sql(library_ids)
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(base_sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 获取缺集扫描候选列表时出错: {e}", exc_info=True)
        return []

def get_series_by_dynamic_condition(condition_sql: str, library_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    根据动态条件获取剧集列表（用于 WatchlistProcessor）。
    
    :param condition_sql: 例如 "watching_status = 'Watching'" 或 "force_ended = FALSE"
    :param library_ids: 可选的媒体库 ID 列表
    """
    # 基础查询字段，与原 _get_series_to_process 保持一致
    base_sql = """
        SELECT 
            tmdb_id,
            title AS item_name,
            watching_status AS status,
            emby_item_ids_json,
            force_ended,
            paused_until,
            last_episode_to_air_json,
            watchlist_tmdb_status AS tmdb_status,
            watchlist_missing_info_json AS missing_info_json,
            subscription_status,
            total_episodes,
            total_episodes_locked
        FROM media_metadata
        WHERE item_type = 'Series'
    """
    
    # 拼接动态条件
    if condition_sql:
        # 简单的防注入处理：确保 condition_sql 不包含分号等危险字符，
        # 但由于这是内部调用，主要依赖调用方传入正确的 SQL 片段。
        # 这里假设 condition_sql 是类似 "watching_status = '...'" 的片段
        base_sql += f" AND ({condition_sql})"
    
    # 拼接媒体库过滤
    if library_ids:
        base_sql += _build_library_filter_sql(library_ids)
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(base_sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 根据动态条件获取剧集时出错: {e}", exc_info=True)
        return []