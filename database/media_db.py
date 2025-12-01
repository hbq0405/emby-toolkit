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
                emby_ids = row['emby_item_ids_json']  # 这已经是个列表
                if emby_ids:   # 只保存非空列表
                    result[tmdb_id] = emby_ids
            return result
    except Exception as e:
        logger.error(f"DB: 检查 TMDb ID 是否在库时失败: {e}", exc_info=True)
        return {}
    
def does_series_have_valid_actor_cache(tmdb_id: str) -> bool:
    """
    检查一个剧集是否在 media_metadata 中存在有效的演员缓存。
    "有效"定义为 actors_json 字段存在且不为空数组 '[]'。
    """
    if not tmdb_id:
        return False
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM media_metadata 
                    WHERE tmdb_id = %s AND item_type = 'Series'
                      AND actors_json IS NOT NULL AND actors_json::text != '[]'
                """, (tmdb_id,))
                # 如果能查询到一行，说明缓存存在且有效
                return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"检查剧集 {tmdb_id} 演员缓存时出错: {e}", exc_info=True)
        # 安全起见，如果查询失败，我们假定缓存不存在，以便触发深度处理
        return False
    
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
    【新增】根据完整的复合主键 (tmdb_id, item_type) 获取唯一的一条媒体记录。
    这是获取单个媒体详情最可靠的方法。
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
    【V3 - 新增核心工具】根据 TMDB ID 列表，批量获取 media_metadata 表中的完整记录。
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

def get_media_in_library_status_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, bool]:
    """ 根据 TMDB ID 列表，批量查询媒体的在库状态。"""
    if not tmdb_ids: return {}
    in_library_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT tmdb_id FROM media_metadata WHERE tmdb_id = ANY(%s) AND in_library = TRUE AND item_type IN ('Movie', 'Series')"
            cursor.execute(sql, (tmdb_ids,))
            for row in cursor.fetchall():
                in_library_map[row['tmdb_id']] = True
        return in_library_map
    except psycopg2.Error as e:
        logger.error(f"批量获取媒体在库状态时出错: {e}", exc_info=True)
        return {}
    
def get_all_wanted_media() -> List[Dict[str, Any]]:
    """
    【V2 - 增加父剧信息版】获取所有状态为 'WANTED' 的媒体项。
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
    【新增】检查所有状态为 'PENDING_RELEASE' 的媒体项。
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
    【V1 - 职责单一版】
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
            CASE
                WHEN m1.item_type = 'Series' THEN m1.tmdb_id -- 如果是剧集本身，父ID就是自己
                WHEN m1.item_type = 'Season' THEN m1.parent_series_tmdb_id -- 如果是季，就用parent_series_tmdb_id
                ELSE NULL -- 电影没有父剧集ID
            END AS series_tmdb_id
        FROM 
            media_metadata AS m1
        LEFT JOIN 
            media_metadata AS m2 
        ON 
            m1.parent_series_tmdb_id = m2.tmdb_id AND m2.item_type = 'Series'
        WHERE 
            m1.subscription_status IN ('WANTED', 'PENDING_RELEASE', 'IGNORED', 'SUBSCRIBED')
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
    offset = (page - 1) * page_size
    source_filter = json.dumps([{"type": "user_request", "user_id": user_id}])

    conditions = ["subscription_sources_json @> %s::jsonb"]
    params = [source_filter]

    if status_filter == 'completed':
        conditions.append("in_library = TRUE")
    elif status_filter == 'pending':
        conditions.append("in_library = FALSE AND subscription_status = 'REQUESTED'")
    elif status_filter == 'processing':
        conditions.append("in_library = FALSE AND subscription_status IN ('WANTED', 'SUBSCRIBED', 'PENDING_RELEASE')")
    elif status_filter == 'failed':
        conditions.append("in_library = FALSE AND subscription_status IN ('IGNORED', 'NONE')")
    
    where_sql = " AND ".join(conditions)

    count_sql = f"SELECT COUNT(*) FROM media_metadata WHERE {where_sql};"
    
    data_sql = f"""
        SELECT 
            tmdb_id, item_type, title, 
            subscription_status as status, 
            in_library, 
            first_requested_at as requested_at, 
            ignore_reason as notes
        FROM media_metadata
        WHERE {where_sql}
        ORDER BY first_requested_at DESC
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
        # ★★★ 核心修改：直接从 TMDb 数据中获取真实的季 ID ★★★
        season_tmdb_id = season.get('id')

        # 如果季号或真实 ID 不存在，则跳过此记录，保证数据完整性
        if season_num is None or season_num == 0 or not season_tmdb_id:
            continue
        
        # 判断本季是否在库的逻辑保持不变
        is_season_in_library = season_num in local_in_library_info
        
        records_to_upsert.append({
            "tmdb_id": str(season_tmdb_id), "item_type": "Season", # <-- 使用修正后的真实 ID
            "parent_series_tmdb_id": parent_tmdb_id, "title": season.get('name'),
            "overview": season.get('overview'), "release_date": season.get('air_date'),
            "poster_path": season.get('poster_path'), "season_number": season_num,
            "in_library": is_season_in_library
        })

    # 2. 准备所有集的记录
    for episode in episodes:
        episode_tmdb_id = episode.get('id')
        if not episode_tmdb_id: continue

        season_num = episode.get('season_number')
        episode_num = episode.get('episode_number')

        # ★★★ 核心修改 2/4: 判断本集是否在库 ★★★
        is_episode_in_library = season_num in local_in_library_info and episode_num in local_in_library_info.get(season_num, set())

        records_to_upsert.append({
            "tmdb_id": str(episode_tmdb_id), "item_type": "Episode",
            "parent_series_tmdb_id": parent_tmdb_id, "title": episode.get('name'),
            "overview": episode.get('overview'), "release_date": episode.get('air_date'),
            "season_number": season_num, "episode_number": episode_num,
            "in_library": is_episode_in_library # <-- 使用判断结果
        })

    if not records_to_upsert:
        return

    # 3. 执行批量“插入或更新”
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                # ★★★ 核心修改 3/4: SQL语句的 ON CONFLICT 部分，也要更新 in_library 状态 ★★★
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, parent_series_tmdb_id, title, overview, 
                        release_date, poster_path, season_number, episode_number, in_library
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, %(parent_series_tmdb_id)s, %(title)s, %(overview)s,
                        %(release_date)s, %(poster_path)s, %(season_number)s, %(episode_number)s, %(in_library)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        title = EXCLUDED.title,
                        overview = EXCLUDED.overview,
                        release_date = EXCLUDED.release_date,
                        poster_path = EXCLUDED.poster_path,
                        season_number = EXCLUDED.season_number,
                        episode_number = EXCLUDED.episode_number,
                        in_library = EXCLUDED.in_library, -- <-- 关键！确保更新时也同步在库状态
                        last_synced_at = NOW();
                """
                
                # ★★★ 核心修改 4/4: 确保 in_library 字段被正确填充 ★★★
                data_for_batch = []
                for rec in records_to_upsert:
                    data_for_batch.append({
                        "tmdb_id": rec.get("tmdb_id"), "item_type": rec.get("item_type"),
                        "parent_series_tmdb_id": rec.get("parent_series_tmdb_id"),
                        "title": rec.get("title"), "overview": rec.get("overview"),
                        "release_date": rec.get("release_date"), "poster_path": rec.get("poster_path"),
                        "season_number": rec.get("season_number"), "episode_number": rec.get("episode_number"),
                        "in_library": rec.get("in_library", False) # <-- 确保这个值被正确传入
                    })

                execute_batch(cursor, sql, data_for_batch)
                logger.info(f"  ➜ [追剧联动] 成功为剧集 {parent_tmdb_id} 智能同步了 {len(data_for_batch)} 个子项目的元数据和在库状态。")

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

def get_in_library_status_for_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, bool]:
    """
    给定一个 TMDB ID 列表，批量查询它们在 media_metadata 中的 in_library 状态。
    返回一个字典，键是 TMDB ID，值是布尔值 (True/False)。
    """
    if not tmdb_ids:
        return {}
    
    sql = """
        SELECT tmdb_id, in_library 
        FROM media_metadata 
        WHERE tmdb_id = ANY(%s);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_ids,))
                # 使用字典推导式高效地构建返回结果
                return {str(row['tmdb_id']): row['in_library'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"DB: 批量查询 TMDB ID 的在库状态失败: {e}", exc_info=True)
        return {}
    
def get_all_children_for_series_batch(parent_series_tmdb_ids: List[str]) -> set:
    """
    根据父剧集ID列表，批量获取所有已存在的子项（季、集）的TMDB ID。
    返回一个集合(set)，用于进行高性能的查找。
    """
    if not parent_series_tmdb_ids:
        return set()
    
    sql = """
        SELECT tmdb_id FROM media_metadata 
        WHERE parent_series_tmdb_id = ANY(%s) AND item_type IN ('Season', 'Episode');
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (parent_series_tmdb_ids,))
                return {str(row['tmdb_id']) for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"DB: 批量查询剧集子项ID时失败: {e}", exc_info=True)
        return set()

def batch_insert_media_metadata(records: List[Dict[str, Any]]):
    """
    使用 ON CONFLICT DO NOTHING 高效地批量插入媒体元数据记录。
    如果记录已存在，则直接跳过，不做任何操作。
    """
    if not records:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_values
                
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, parent_series_tmdb_id, title, overview, 
                        release_date, poster_path, season_number, episode_number, in_library
                    ) VALUES %s
                    ON CONFLICT (tmdb_id, item_type) DO NOTHING;
                """
                
                # 准备数据元组列表
                data_tuples = [
                    (
                        str(rec.get("id")), # TMDb ID
                        'Season' if rec.get("episode_count") is not None else 'Episode', # Item Type
                        rec.get("parent_series_tmdb_id"), # Parent Series TMDB ID
                        rec.get("name"), # Title
                        rec.get("overview"), # Overview
                        rec.get("air_date"), # Release Date
                        rec.get("poster_path"), # Poster Path
                        rec.get("season_number"), # Season Number
                        rec.get("episode_number"), # Episode Number
                        False # in_library, 默认为 False
                    ) for rec in records
                ]

                execute_values(cursor, sql, data_tuples, page_size=1000)
                logger.info(f"  ➜ [元数据补全] 尝试批量插入 {len(data_tuples)} 条缺失的子项记录，成功影响了 {cursor.rowcount} 行。")

    except Exception as e:
        logger.error(f"  ➜ [元数据补全] 批量插入缺失的子项记录时发生错误: {e}", exc_info=True)
        raise

def get_series_local_children_info(parent_tmdb_id: str) -> dict:
    """
    【新】从本地数据库获取一个剧集在媒体库中的结构信息。
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

def get_series_local_episodes_overview(parent_tmdb_id: str) -> list:
    """
    【新】从本地数据库获取一个剧集所有分集的元数据，用于检查简介。
    返回一个字典列表，每个字典包含分集的基本信息。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT emby_item_ids_json->>0 AS emby_item_id, season_number, episode_number, overview
                FROM media_metadata
                WHERE parent_series_tmdb_id = %s
                  AND item_type = 'Episode'
                  AND in_library = TRUE
            """
            cursor.execute(sql, (parent_tmdb_id,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"从本地数据库获取剧集 {parent_tmdb_id} 的分集元数据时失败: {e}")
        return []

def update_media_metadata_fields(tmdb_id: str, item_type: str, updates: Dict[str, Any]):
    """
    【V2 - 通用元数据更新】
    根据传入的 updates 字典，动态更新指定媒体的字段。
    常态化更新逻辑：更新除片名/演员表之外的所有元数据。
    """
    if not tmdb_id or not item_type or not updates:
        return

    # ★★★ 核心保护机制 ★★★
    # 过滤掉空键，并强制移除不允许更新的敏感字段
    # title: 保护用户/Emby修改过的中文标题
    # actors_json: 保护演员表（通常由专门的演员任务处理）
    # tmdb_id/item_type: 主键不能改
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
    【性能优化 - 修正版 V2】直接从数据库生成全量映射表。
    Key 格式升级为 "{tmdb_id}_{item_type}"。
    修复了启用媒体库过滤时，剧集(Series)因没有资产信息而被错误过滤的问题。
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
    
def get_emby_ids_for_items(items: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    【修正版】根据 (tmdb_id, item_type) 组合，精准查询对应的 Emby ID。
    解决 TMDb ID 在电影和剧集间不唯一的问题。
    
    :param items: 包含 [{'tmdb_id': '...', 'media_type': '...'}, ...] 的列表
    :return: 返回字典，Key 为 "{tmdb_id}_{item_type}" 组合键，Value 为 {'Id': emby_id}
    """
    if not items:
        return {}

    # 过滤掉无效数据，并准备查询参数
    # 注意：数据库里的 item_type 是 'Series'，而有些来源可能是 'TV'，这里假设传入前已标准化
    # 之前的代码中 ListImporter 已经把 'tv' 转为了 'Series'，这里直接用
    query_pairs = []
    for item in items:
        tid = item.get('tmdb_id')
        mtype = item.get('media_type')
        if tid and mtype:
            query_pairs.append((str(tid), mtype))

    if not query_pairs:
        return {}

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 构造 SQL：WHERE (tmdb_id, item_type) IN (('1', 'Movie'), ('2', 'Series'), ...)
            # psycopg2 的 execute 能够处理元组列表作为 IN 的参数，但需要一点技巧
            # 这里我们使用 mogrify 或者直接构造参数列表
            
            # 为了兼容性和安全性，我们生成对应数量的占位符
            placeholders = ",".join(["(%s, %s)"] * len(query_pairs))
            sql = f"""
                SELECT tmdb_id, item_type, emby_item_ids_json 
                FROM media_metadata 
                WHERE (tmdb_id, item_type) IN ({placeholders})
                  AND emby_item_ids_json IS NOT NULL 
                  AND jsonb_array_length(emby_item_ids_json) > 0
            """
            
            # 扁平化参数列表: [id1, type1, id2, type2, ...]
            flat_params = [val for pair in query_pairs for val in pair]
            
            cursor.execute(sql, tuple(flat_params))
            rows = cursor.fetchall()
            
            mapping = {}
            for row in rows:
                tmdb_id = row['tmdb_id']
                item_type = row['item_type']
                emby_ids = row['emby_item_ids_json']
                
                if tmdb_id and item_type and emby_ids:
                    # ★★★ 使用组合键，防止 ID 冲突 ★★★
                    key = f"{tmdb_id}_{item_type}"
                    mapping[key] = {'Id': emby_ids[0]}
            
            logger.debug(f"  ➜ [精准映射] 请求查询 {len(query_pairs)} 个项目，成功匹配到 {len(mapping)} 个 Emby ID。")
            return mapping

    except Exception as e:
        logger.error(f"精准查询 Emby ID (带类型) 时出错: {e}", exc_info=True)
        return {}
    
def get_series_average_runtime(parent_tmdb_id: str) -> float:
    """
    计算指定剧集下所有分集（Episode）的平均时长。
    优先使用 runtime_minutes，如果为0或空，尝试解析 asset_details_json。
    """
    if not parent_tmdb_id:
        return 0.0
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 逻辑：
            # 1. 筛选该剧集下的所有 Episode
            # 2. 优先取 runtime_minutes
            # 3. 如果 runtime_minutes 无效，尝试从 asset_details_json 取 (这里简化处理，主要依赖 runtime_minutes，因为入库时已处理过)
            # 4. 计算平均值
            sql = """
                SELECT AVG(runtime_minutes) as avg_runtime
                FROM media_metadata
                WHERE parent_series_tmdb_id = %s 
                  AND item_type = 'Episode' 
                  AND runtime_minutes > 0
            """
            cursor.execute(sql, (str(parent_tmdb_id),))
            row = cursor.fetchone()
            if row and row['avg_runtime']:
                return float(row['avg_runtime'])
            return 0.0
    except Exception as e:
        logger.error(f"计算剧集 {parent_tmdb_id} 平均时长时出错: {e}")
        return 0.0
    
def get_runtimes_for_series_list(tmdb_ids: List[str]) -> Dict[str, float]:
    """
    只计算指定 ID 列表中的剧集平均时长。
    利用索引精准打击，避免全表扫描。
    """
    if not tmdb_ids:
        return {}
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT parent_series_tmdb_id, AVG(runtime_minutes) as avg_runtime
                FROM media_metadata
                WHERE item_type = 'Episode' 
                  AND runtime_minutes > 0
                  AND parent_series_tmdb_id = ANY(%s)
                GROUP BY parent_series_tmdb_id
            """
            cursor.execute(sql, (tmdb_ids,))
            rows = cursor.fetchall()
            
            return {
                str(row['parent_series_tmdb_id']): float(row['avg_runtime']) 
                for row in rows
            }
    except Exception as e:
        logger.error(f"批量计算指定剧集平均时长时出错: {e}")
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
                    elif status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE']:
                        stats['processing'] += count
                    elif status in ['IGNORED', 'NONE']:
                        stats['failed'] += count
        return stats
    except Exception as e:
        logger.error(f"DB: 获取用户统计失败: {e}", exc_info=True)
        return stats