# database/media_db.py
import logging
from typing import List, Dict, Optional, Any
import json
import psycopg2
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def check_tmdb_ids_in_library(tmdb_ids: List[str], item_type: str) -> Dict[str, str]:
    """
    接收 TMDb ID 列表，返回一个字典，映射 TMDb ID 到 Emby Item ID。
    """
    if not tmdb_ids:
        return {}

    sql = "SELECT tmdb_id, emby_item_ids_json FROM media_metadata WHERE item_type = %s AND tmdb_id = ANY(%s)"

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (item_type, tmdb_ids))
            result = {}
            for row in cursor.fetchall():
                tmdb_id = row['tmdb_id']
                emby_ids = row['emby_item_ids_json']
                if emby_ids:
                    key = f"{tmdb_id}_{item_type}"
                    result[key] = emby_ids
            return result
    except Exception as e:
        logger.error(f"DB: 检查 TMDb ID 是否在库时失败: {e}", exc_info=True)
        return {}
    
def get_tmdb_id_from_emby_id(emby_id: str) -> Optional[str]:
    """
    根据 Emby ID，从 media_metadata 表中反查出对应的 TMDB ID。
    """
    if not emby_id:
        return None
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 @> 操作符高效查询 JSONB 数组是否包含指定的 Emby ID
            sql = "SELECT tmdb_id FROM media_metadata WHERE emby_item_ids_json @> %s::jsonb"
            cursor.execute(sql, (json.dumps([emby_id]),))
            row = cursor.fetchone()
            return row['tmdb_id'] if row else None
    except psycopg2.Error as e:
        logger.error(f"根据 Emby ID {emby_id} 反查 TMDB ID 时出错: {e}", exc_info=True)
        return None

def get_media_details(tmdb_id: str, item_type: str) -> Optional[Dict[str, Any]]:
    """
    根据完整的复合主键 (tmdb_id, item_type) 获取唯一的一条媒体记录。
    """
    if not tmdb_id or not item_type:
        return None
    
    sql = "SELECT * FROM media_metadata WHERE tmdb_id = %s AND item_type = %s"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_id, item_type))
                row = cursor.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB: 获取媒体详情 (TMDb ID: {tmdb_id}, Type: {item_type}) 时失败: {e}", exc_info=True)
        return None

def get_media_details_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    根据 TMDB ID 列表，批量获取 media_metadata 表中的完整记录。
    返回一个以 tmdb_id 为键，整行记录字典为值的 map，方便快速查找。
    """
    if not tmdb_ids:
        return {}
    
    media_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM media_metadata WHERE tmdb_id = ANY(%s)"
            cursor.execute(sql, (tmdb_ids,))
            rows = cursor.fetchall()
            for row in rows:
                media_map[row['tmdb_id']] = dict(row)
        return media_map
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID列表批量获取媒体详情时出错: {e}", exc_info=True)
        return {}

def get_all_wanted_media() -> List[Dict[str, Any]]:
    """
    获取所有状态为 'WANTED' 的媒体项。
    为 Season 类型的项目额外提供 parent_series_tmdb_id。
    """
    sql = """
        SELECT 
            tmdb_id, item_type, title, release_date, poster_path, overview,
            -- ★★★ 核心修改：把这两个关键字段也查出来 ★★★
            parent_series_tmdb_id, 
            season_number, 
            subscription_sources_json
        FROM media_metadata
        WHERE subscription_status = 'WANTED'
        ORDER BY first_requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取所有待订阅(WANTED)媒体失败: {e}", exc_info=True)
        return []
    
def promote_pending_to_wanted() -> int:
    """
    检查所有状态为 'PENDING_RELEASE' 的媒体项。
    如果其发行日期已到或已过，则将其状态更新为 'WANTED'。
    返回被成功晋升状态的媒体项数量。
    """
    sql = """
        UPDATE media_metadata
        SET 
            subscription_status = 'WANTED',
            -- 可以选择性地在这里也更新一个时间戳字段，用于追踪状态变更
            last_synced_at = NOW()
        WHERE 
            subscription_status = 'PENDING_RELEASE' 
            AND release_date <= NOW();
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                promoted_count = cursor.rowcount
                conn.commit()
                return promoted_count
    except Exception as e:
        logger.error(f"DB: 晋升 PENDING_RELEASE 状态失败: {e}", exc_info=True)
        return 0

def ensure_media_record_exists(media_info_list: List[Dict[str, Any]]):
    """
    确保媒体元数据记录存在于数据库中。
    - 如果记录不存在，则创建它，订阅状态默认为 'NONE'。
    - 如果记录已存在，则只更新其基础元数据（标题、海报、父子关系等）。
    - ★★★ 这个函数【绝不】会修改已存在的订阅状态 ★★★
    """
    if not media_info_list:
        return

    logger.info(f"  ➜ [元数据注册] 准备为 {len(media_info_list)} 个媒体项目确保记录存在...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, original_title, release_date, poster_path, 
                        overview, season_number, parent_series_tmdb_id
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s,
                        %(overview)s, %(season_number)s, %(parent_series_tmdb_id)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        title = EXCLUDED.title,
                        original_title = EXCLUDED.original_title,
                        release_date = EXCLUDED.release_date,
                        poster_path = EXCLUDED.poster_path,
                        overview = EXCLUDED.overview,
                        season_number = EXCLUDED.season_number,
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        last_synced_at = NOW();
                """
                
                # 准备数据，确保所有 key 都存在，避免 psycopg2 报错
                data_for_batch = []
                for info in media_info_list:
                    data_for_batch.append({
                        "tmdb_id": info.get("tmdb_id"),
                        "item_type": info.get("item_type"),
                        "title": info.get("title"),
                        "original_title": info.get("original_title"),
                        "release_date": info.get("release_date") or None,
                        "poster_path": info.get("poster_path"),
                        "overview": info.get("overview"),
                        "season_number": info.get("season_number"),
                        "parent_series_tmdb_id": info.get("parent_series_tmdb_id")
                    })

                execute_batch(cursor, sql, data_for_batch)
                logger.info(f"  ➜ [元数据注册] 成功，影响了 {cursor.rowcount} 行。")

    except Exception as e:
        logger.error(f"  ➜ [元数据注册] 确保媒体记录存在时发生错误: {e}", exc_info=True)
        raise

def get_all_subscriptions() -> List[Dict[str, Any]]:
    """
    获取所有有订阅状态的媒体项，用于前端统一管理。
    当项目类型为 Season 时，会自动查询并拼接父剧集的标题，并额外提供父剧集的TMDb ID用于生成正确的链接。
    ★ 修改：排除 'Series' 类型，只显示具体的 'Movie' 和 'Season'，避免父剧集条目干扰列表。
    """
    sql = """
        SELECT 
            m1.tmdb_id, 
            m1.item_type, 
            CASE 
                WHEN m1.item_type = 'Season' THEN COALESCE(m2.title, '未知剧集') || ' 第 ' || m1.season_number || ' 季 '
                ELSE m1.title 
            END AS title,
            m1.release_date, 
            m1.poster_path, 
            m1.subscription_status, 
            m1.ignore_reason, 
            m1.subscription_sources_json,
            m1.first_requested_at,
            m1.last_subscribed_at,
            m1.paused_until,
            CASE
                -- 虽然排除了 Series，但保留这个逻辑也没错，主要用于 Season 跳转
                WHEN m1.item_type = 'Series' THEN m1.tmdb_id 
                WHEN m1.item_type = 'Season' THEN m1.parent_series_tmdb_id 
                ELSE NULL 
            END AS series_tmdb_id
        FROM 
            media_metadata AS m1
        LEFT JOIN 
            media_metadata AS m2 
        ON 
            m1.parent_series_tmdb_id = m2.tmdb_id AND m2.item_type = 'Series'
        WHERE 
            m1.subscription_status IN ('REQUESTED', 'WANTED', 'PENDING_RELEASE', 'IGNORED', 'SUBSCRIBED', 'PAUSED')
            -- ★★★ 核心修改：排除父剧集 (Series) ★★★
            -- 因为实际的订阅执行和下载都是以季 (Season) 为单位的
            -- 父剧集的 SUBSCRIBED 状态仅用于后台逻辑（如合集检测），不需要在前端展示
            AND m1.item_type != 'Series'
        ORDER BY 
            m1.first_requested_at DESC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取所有非在库媒体失败: {e}", exc_info=True)
        return []
    
def get_user_request_history(user_id: str, page: int = 1, page_size: int = 10, status_filter: str = 'all') -> tuple[List[Dict[str, Any]], int]:
    """
    获取用户订阅历史记录。
    ★ 修改：对于 Season 类型，关联查询父剧集标题，显示为 "剧名 第 X 季"
    """
    offset = (page - 1) * page_size
    source_filter = json.dumps([{"type": "user_request", "user_id": user_id}])

    # ★ 1. 修改条件：给字段加上别名 m1.，因为后面要进行表连接
    conditions = ["m1.subscription_sources_json @> %s::jsonb"]
    params = [source_filter]

    if status_filter == 'completed':
        conditions.append("m1.in_library = TRUE")
    elif status_filter == 'pending':
        conditions.append("m1.in_library = FALSE AND m1.subscription_status = 'REQUESTED'")
    elif status_filter == 'processing':
        conditions.append("m1.in_library = FALSE AND m1.subscription_status IN ('WANTED', 'SUBSCRIBED', 'PAUSED', 'PENDING_RELEASE')")
    elif status_filter == 'failed':
        conditions.append("m1.in_library = FALSE AND m1.subscription_status IN ('IGNORED', 'NONE')")
    
    where_sql = " AND ".join(conditions)

    # ★ 2. 修改 Count SQL：使用别名 m1
    count_sql = f"SELECT COUNT(*) FROM media_metadata m1 WHERE {where_sql};"
    
    # ★ 3. 修改 Data SQL：
    # - 加入 LEFT JOIN media_metadata m2 (用于查找父剧集)
    # - 使用 CASE WHEN 拼接标题
    data_sql = f"""
        SELECT 
            m1.tmdb_id, 
            m1.item_type, 
            CASE 
                WHEN m1.item_type = 'Season' AND m2.title IS NOT NULL THEN m2.title || ' ' || m1.title
                ELSE m1.title 
            END AS title,
            m1.subscription_status as status, 
            m1.in_library, 
            m1.first_requested_at as requested_at, 
            m1.ignore_reason as notes
        FROM media_metadata m1
        LEFT JOIN media_metadata m2 ON m1.parent_series_tmdb_id = m2.tmdb_id AND m2.item_type = 'Series'
        WHERE {where_sql}
        ORDER BY m1.first_requested_at DESC
        LIMIT %s OFFSET %s;
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 执行 Count
            cursor.execute(count_sql, tuple(params))
            total_records = cursor.fetchone()['count']
            
            # 执行 Data (追加分页参数)
            data_params = params + [page_size, offset]
            cursor.execute(data_sql, tuple(data_params))
            rows = cursor.fetchall()
            history = []
            for row in rows:
                history_item = dict(row)
                
                # ★★★ 核心逻辑：只翻译在库状态 ★★★
                # 规则: 只要在库里了，就是“已完成”，这是唯一需要后端翻译的状态。
                if history_item.get('in_library'):
                    history_item['status'] = 'completed'
                
                # 对于所有其他情况 (不在库)，status 字段将保持其在数据库中的原始值
                # (e.g., 'IGNORED', 'WANTED', 'SUBSCRIBED', 'REQUESTED', 'NONE', etc.)
                
                history.append(history_item)
            
            return history, total_records
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅历史失败: {e}", exc_info=True)
        # 保持与旧函数一致的返回类型，即使出错
        return [], 0

def sync_series_children_metadata(parent_tmdb_id: str, seasons: List[Dict], episodes: List[Dict], local_in_library_info: Dict[int, set]):
    """
    根据从 TMDB 获取的最新数据，批量同步一个剧集的所有季和集到 media_metadata 表。
    使用 ON CONFLICT DO UPDATE 实现高效的“插入或更新”。
    """
    if not parent_tmdb_id:
        return

    records_to_upsert = []

    # 1. 准备所有季的记录
    for season in seasons:
        season_num = season.get('season_number')
        season_tmdb_id = season.get('id')

        if season_num is None or season_num == 0 or not season_tmdb_id:
            continue
        
        is_season_in_library = season_num in local_in_library_info
        
        records_to_upsert.append({
            "tmdb_id": str(season_tmdb_id), "item_type": "Season",
            "parent_series_tmdb_id": parent_tmdb_id, "title": season.get('name'),
            "overview": season.get('overview'), "release_date": season.get('air_date'),
            "poster_path": season.get('poster_path'), "season_number": season_num,
            "in_library": is_season_in_library,
            # ★★★ 新增：获取季的总集数 ★★★
            "total_episodes": season.get('episode_count', 0)
        })

    # 2. 准备所有集的记录
    for episode in episodes:
        episode_tmdb_id = episode.get('id')
        if not episode_tmdb_id: continue

        season_num = episode.get('season_number')
        episode_num = episode.get('episode_number')

        is_episode_in_library = season_num in local_in_library_info and episode_num in local_in_library_info.get(season_num, set())

        records_to_upsert.append({
            "tmdb_id": str(episode_tmdb_id), "item_type": "Episode",
            "parent_series_tmdb_id": parent_tmdb_id, "title": episode.get('name'),
            "overview": episode.get('overview'), "release_date": episode.get('air_date'),
            "season_number": season_num, "episode_number": episode_num,
            "in_library": is_episode_in_library,
            # ★★★ 新增：单集没有总集数概念，设为 0 以保持字典结构一致 ★★★
            "total_episodes": 0 
        })

    if not records_to_upsert:
        return

    # 3. 执行批量“插入或更新”
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                # ★★★ 修改 SQL：添加 total_episodes 字段 ★★★
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, parent_series_tmdb_id, title, overview, 
                        release_date, poster_path, season_number, episode_number, in_library,
                        total_episodes
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, %(parent_series_tmdb_id)s, %(title)s, %(overview)s,
                        %(release_date)s, %(poster_path)s, %(season_number)s, %(episode_number)s, %(in_library)s,
                        %(total_episodes)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        title = EXCLUDED.title,
                        overview = EXCLUDED.overview,
                        release_date = EXCLUDED.release_date,
                        poster_path = EXCLUDED.poster_path,
                        season_number = EXCLUDED.season_number,
                        episode_number = EXCLUDED.episode_number,
                        in_library = EXCLUDED.in_library,
                        
                        -- ★★★ 核心逻辑：如果已锁定，则保持原值；否则更新为新值 ★★★
                        total_episodes = CASE 
                            WHEN media_metadata.total_episodes_locked = TRUE THEN media_metadata.total_episodes
                            ELSE EXCLUDED.total_episodes
                        END,
                        
                        last_synced_at = NOW();
                """
                
                data_for_batch = []
                for rec in records_to_upsert:
                    data_for_batch.append({
                        "tmdb_id": rec.get("tmdb_id"), "item_type": rec.get("item_type"),
                        "parent_series_tmdb_id": rec.get("parent_series_tmdb_id"),
                        "title": rec.get("title"), "overview": rec.get("overview"),
                        "release_date": rec.get("release_date"), "poster_path": rec.get("poster_path"),
                        "season_number": rec.get("season_number"), "episode_number": rec.get("episode_number"),
                        "in_library": rec.get("in_library", False),
                        # ★★★ 新增：传入 total_episodes ★★★
                        "total_episodes": rec.get("total_episodes", 0)
                    })

                execute_batch(cursor, sql, data_for_batch)
                logger.info(f"  ➜ [追剧联动] 成功为剧集 {parent_tmdb_id} 智能同步了 {len(data_for_batch)} 个子项目的元数据(含集数)和在库状态。")

    except Exception as e:
        logger.error(f"  ➜ [追剧联动] 在同步剧集 {parent_tmdb_id} 的子项目时发生错误: {e}", exc_info=True)

def get_series_title_by_tmdb_id(tmdb_id: str) -> Optional[str]:
    """根据 TMDB ID 精确查询剧集的标题。"""
    if not tmdb_id:
        return None
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series' LIMIT 1"
            cursor.execute(sql, (tmdb_id,))
            row = cursor.fetchone()
            return row['title'] if row else None
    except psycopg2.Error as e:
        logger.error(f"根据 TMDB ID {tmdb_id} 查询剧集标题时出错: {e}", exc_info=True)
        return None

def get_in_library_status_with_type_bulk(tmdb_ids: list) -> Dict[str, bool]:
    """
    【精确查询】传入 TMDB ID 列表，查询数据库。
    返回字典，Key 是组合键 "{tmdb_id}_{item_type}"，Value 是 in_library 状态。
    解决了 ID 相同但类型不同（电影/剧集）导致的误判问题。
    """
    if not tmdb_ids:
        return {}
    
    # 去重
    unique_ids = list(set([str(id) for id in tmdb_ids if id]))
    
    if not unique_ids:
        return {}

    sql = """
        SELECT tmdb_id, item_type, in_library 
        FROM media_metadata 
        WHERE tmdb_id = ANY(%s);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (unique_ids,))
                rows = cursor.fetchall()
                
                result_map = {}
                for row in rows:
                    # ★★★ 构造组合键：ID_类型 ★★★
                    key = f"{row['tmdb_id']}_{row['item_type']}"
                    result_map[key] = row['in_library']
                
                return result_map
    except Exception as e:
        logger.error(f"DB: 批量查询(带类型)在库状态失败: {e}", exc_info=True)
        return {}
    
def get_series_local_children_info(parent_tmdb_id: str) -> dict:
    """
    从本地数据库获取一个剧集在媒体库中的结构信息。
    返回与旧版 emby.get_series_children 兼容的格式。
    格式: { season_num: {ep_num1, ep_num2, ...} }
    """
    local_structure = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT season_number, episode_number
                FROM media_metadata
                WHERE parent_series_tmdb_id = %s
                  AND item_type = 'Episode'
                  AND in_library = TRUE
            """
            cursor.execute(sql, (parent_tmdb_id,))
            for row in cursor.fetchall():
                s_num, e_num = row['season_number'], row['episode_number']
                if s_num is not None and e_num is not None:
                    local_structure.setdefault(s_num, set()).add(e_num)
        return local_structure
    except Exception as e:
        logger.error(f"从本地数据库获取剧集 {parent_tmdb_id} 的子项目结构时失败: {e}")
        return {}

def update_media_metadata_fields(tmdb_id: str, item_type: str, updates: Dict[str, Any]):
    """
    根据传入的 updates 字典，动态更新指定媒体的字段。
    常态化更新逻辑：更新除片名/演员表之外的所有元数据。
    """
    if not tmdb_id or not item_type or not updates:
        return

    safe_updates = {
        k: v for k, v in updates.items() 
        if k not in ['title', 'actors_json', 'tmdb_id', 'item_type']
    }
    
    if not safe_updates:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 动态构建 SET 子句
                set_clauses = [f"{key} = %s" for key in safe_updates.keys()]
                # 总是更新时间戳
                set_clauses.append("last_updated_at = NOW()")
                
                sql = f"""
                    UPDATE media_metadata 
                    SET {', '.join(set_clauses)}
                    WHERE tmdb_id = %s AND item_type = %s
                """
                
                # 构建参数列表：更新值 + WHERE条件值
                params = list(safe_updates.values())
                params.extend([tmdb_id, item_type])
                
                cursor.execute(sql, tuple(params))
            conn.commit()
    except Exception as e:
        logger.error(f"更新媒体 {tmdb_id} ({item_type}) 的元数据字段时失败: {e}", exc_info=True)

def get_tmdb_to_emby_map(library_ids: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    直接从数据库生成全量映射表。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 基础 SQL
            sql = """
                SELECT tmdb_id, item_type, emby_item_ids_json 
                FROM media_metadata 
                WHERE in_library = TRUE 
                  AND emby_item_ids_json IS NOT NULL 
                  AND jsonb_array_length(emby_item_ids_json) > 0
            """
            
            params = []
            if library_ids:
                lib_ids_str = [str(lid) for lid in library_ids]
                # 构造 SQL 数组参数
                
                # ★★★ 核心修复：针对 Movie 和 Series 使用不同的过滤逻辑 ★★★
                # Movie: 直接检查自身的 asset_details_json
                # Series: 检查是否有子集(Episode)在指定库中
                sql += """
                    AND (
                        (
                            item_type = 'Movie' 
                            AND asset_details_json IS NOT NULL
                            AND EXISTS (
                                SELECT 1 
                                FROM jsonb_array_elements(asset_details_json) AS elem
                                WHERE elem->>'source_library_id' = ANY(%s)
                            )
                        )
                        OR
                        (
                            item_type = 'Series'
                            AND tmdb_id IN (
                                SELECT DISTINCT parent_series_tmdb_id
                                FROM media_metadata
                                WHERE item_type = 'Episode'
                                  AND in_library = TRUE
                                  AND asset_details_json IS NOT NULL
                                  AND EXISTS (
                                      SELECT 1 
                                      FROM jsonb_array_elements(asset_details_json) AS elem
                                      WHERE elem->>'source_library_id' = ANY(%s)
                                  )
                            )
                        )
                    )
                """
                # 需要传入两次 library_ids，分别给 Movie 和 Series 的子查询使用
                params.append(lib_ids_str)
                params.append(lib_ids_str)
            
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            
            mapping = {}
            for row in rows:
                tmdb_id = row['tmdb_id']
                item_type = row['item_type']
                emby_ids = row['emby_item_ids_json']
                
                if tmdb_id and item_type and emby_ids:
                    # 使用组合键
                    key = f"{tmdb_id}_{item_type}"
                    mapping[key] = {'Id': emby_ids[0]}
                    
            logger.info(f"  ➜ 从数据库加载了 {len(mapping)} 条 TMDb->Emby 映射关系。")
            return mapping

    except Exception as e:
        logger.error(f"从数据库生成 TMDb->Emby 映射时出错: {e}", exc_info=True)
        return {}
    
def get_user_request_stats(user_id: str) -> Dict[str, int]:
    """获取用户订阅请求的统计信息"""
    source_filter = json.dumps([{"type": "user_request", "user_id": user_id}])
    sql = """
        SELECT in_library, subscription_status, COUNT(*) as count
        FROM media_metadata
        WHERE subscription_sources_json @> %s::jsonb
        GROUP BY in_library, subscription_status;
    """
    stats = {'total': 0, 'completed': 0, 'processing': 0, 'pending': 0, 'failed': 0}
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (source_filter,))
            for row in cursor.fetchall():
                count = row['count']
                stats['total'] += count
                
                if row['in_library']:
                    stats['completed'] += count
                else:
                    status = row['subscription_status']
                    if status == 'REQUESTED':
                        stats['pending'] += count
                    elif status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED']:
                        stats['processing'] += count
                    elif status in ['IGNORED', 'NONE']:
                        stats['failed'] += count
        return stats
    except Exception as e:
        logger.error(f"DB: 获取用户统计失败: {e}", exc_info=True)
        return stats
    
def delete_media_metadata_batch(items: List[Dict[str, str]]) -> int:
    """
    【批量物理删除媒体元数据。
    仅删除 in_library = FALSE 的记录，防止误删已入库项目。
    """
    if not items:
        return 0

    # 提取 (tmdb_id, item_type) 元组列表
    targets = []
    for item in items:
        if item.get('tmdb_id') and item.get('item_type'):
            targets.append((str(item.get('tmdb_id')), item.get('item_type')))

    if not targets:
        return 0

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 构造 SQL：WHERE (tmdb_id, item_type) IN ((id1, type1), (id2, type2)...)
                # 且必须不在库中
                placeholders = ",".join(["(%s, %s)"] * len(targets))
                sql = f"""
                    DELETE FROM media_metadata 
                    WHERE (tmdb_id, item_type) IN ({placeholders})
                      AND in_library = FALSE
                """
                
                # 扁平化参数
                flat_params = [val for pair in targets for val in pair]
                
                cursor.execute(sql, tuple(flat_params))
                deleted_count = cursor.rowcount
                conn.commit()
                return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量物理删除媒体元数据失败: {e}", exc_info=True)
        return 0
    
def batch_ensure_basic_movies(movies_list: List[Dict[str, Any]]):
    """
    批量插入基础电影条目。
    如果记录已存在（无论是已入库还是已订阅），则忽略（DO NOTHING），保留现有数据。
    如果不存在，则插入基础信息（标题、海报、上映日期），并将 in_library 设为 FALSE。
    """
    if not movies_list:
        return

    # 准备插入的数据，确保字段完整
    data_to_insert = []
    for m in movies_list:
        data_to_insert.append({
            'tmdb_id': str(m['tmdb_id']),
            'item_type': 'Movie',
            'title': m.get('title'),
            'original_title': m.get('original_title'),
            'release_date': m.get('release_date') or None, # 处理空字符串
            'poster_path': m.get('poster_path'),
            'overview': m.get('overview'),
            'in_library': False, # 默认为不在库
            'subscription_status': 'NONE'
        })

    sql = """
        INSERT INTO media_metadata 
        (tmdb_id, item_type, title, original_title, release_date, poster_path, overview, in_library, subscription_status, last_updated_at)
        VALUES (%(tmdb_id)s, %(item_type)s, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(overview)s, %(in_library)s, %(subscription_status)s, NOW())
        ON CONFLICT (tmdb_id, item_type) DO NOTHING;
    """

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            from psycopg2.extras import execute_batch
            execute_batch(cursor, sql, data_to_insert)
            conn.commit()
    except Exception as e:
        logger.error(f"批量插入基础电影条目时出错: {e}", exc_info=True)

def get_user_positive_history(user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    获取指定用户的“好评”观看历史。
    修改：返回包含 tmdb_id, item_type, title 的完整字典列表，供 AI 和向量搜索使用。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 修改：SELECT 中增加 tmdb_id 和 item_type ★★★
            cursor.execute("""
                SELECT m.tmdb_id, m.item_type, m.title, m.release_year
                FROM user_media_data u
                JOIN media_metadata m ON (u.item_id = m.tmdb_id OR m.emby_item_ids_json ? u.item_id)
                WHERE u.user_id = %s
                  AND (
                      u.is_favorite = TRUE 
                      OR (
                          u.played = TRUE 
                      )
                  )
                ORDER BY u.is_favorite DESC, u.last_played_date DESC
                LIMIT %s
            """, (user_id, limit))
            
            rows = cursor.fetchall()
            
            # 直接返回字典列表
            history = [dict(row) for row in rows]
            
            if not history:
                logger.warning(f"  ➜ [数据库] 用户 {user_id} 未查到符合条件的观看历史。")
            else:
                logger.trace(f"  ➜ [数据库] 成功提取用户 {user_id} 的 {len(history)} 条好评历史 (含ID)。")
                
            return history
    except Exception as e:
        logger.error(f"获取用户 {user_id} 的观看历史失败: {e}")
        return []
    
def get_user_all_interacted_history(user_id: str) -> List[Dict[str, Any]]:
    """
    获取指定用户的所有交互历史（用于去重过滤）。
    包含：已收藏、已看完、以及【看了一部分但没看完】的所有项目。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT m.tmdb_id
                FROM user_media_data u
                JOIN media_metadata m ON (u.item_id = m.tmdb_id OR m.emby_item_ids_json ? u.item_id)
                WHERE u.user_id = %s
                  AND (
                      u.is_favorite = TRUE 
                      OR u.played = TRUE 
                      OR u.play_count > 0
                      OR u.playback_position_ticks > 0  -- ★★★ 关键：只要有播放进度，就视为已阅
                  )
            """, (user_id,))
            
            rows = cursor.fetchall()
            # 只需要 ID 用于过滤
            history = [dict(row) for row in rows]
            
            logger.trace(f"  ➜ [数据库] 为用户 {user_id} 提取到 {len(history)} 条全量交互记录(含弃坑/未完结)用于去重。")
            return history
    except Exception as e:
        logger.error(f"获取用户 {user_id} 的全量交互历史失败: {e}")
        return []
    
def get_global_popular_items(limit: int = 20) -> List[Dict[str, Any]]:
    """
    获取全站最受欢迎的媒体项（基于完整播放的用户数量）。
    用于“猜大家想看”的种子数据。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 逻辑：统计每个 tmdb_id 被多少个不同的 user_id 标记为 played=TRUE
            cursor.execute("""
                SELECT 
                    m.tmdb_id, 
                    m.item_type, 
                    m.title, 
                    m.release_year,
                    COUNT(DISTINCT u.user_id) as play_count
                FROM user_media_data u
                JOIN media_metadata m ON (u.item_id = m.tmdb_id OR m.emby_item_ids_json ? u.item_id)
                WHERE u.played = TRUE
                GROUP BY m.tmdb_id, m.item_type, m.title, m.release_year
                ORDER BY play_count DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            history = [dict(row) for row in rows]
            
            if history:
                logger.trace(f"  ➜ [数据库] 提取到全站最热 Top {len(history)} 作品 (榜首: {history[0].get('title')}, {history[0].get('play_count')}人看过)。")
            else:
                logger.warning("  ➜ [数据库] 全站暂无播放记录，无法生成全局热榜。")
                
            return history
    except Exception as e:
        logger.error(f"获取全站热门项目失败: {e}")
        return []
    
def is_emby_id_in_library(emby_id: str) -> bool:
    """
    检查指定 Emby ID 对应的媒体项是否标记为在库 (in_library = TRUE)。
    用于 Webhook 分流时的双重检查，防止洗版后的僵尸条目走错流程。
    """
    if not emby_id:
        return False
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 JSONB 包含操作符 @> 高效查找
            sql = "SELECT 1 FROM media_metadata WHERE emby_item_ids_json @> %s::jsonb AND in_library = TRUE LIMIT 1"
            cursor.execute(sql, (json.dumps([emby_id]),))
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"检查 Emby ID {emby_id} 在库状态时出错: {e}", exc_info=True)
        return False