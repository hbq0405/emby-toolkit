# database/actor_db.py
import psycopg2
import logging
import json
from typing import Optional, Dict, Any, List, Tuple, Set
from datetime import datetime

from .connection import get_db_connection
from . import request_db
from utils import contains_chinese
from handler.emby import get_emby_item_details
import handler.moviepilot as moviepilot
from config_manager import APP_CONFIG
import extensions 
import utils
logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 演员数据访问 (单表重构版)
# ======================================================================

class ActorDBManager:
    """
    一个专门负责与演员身份相关的数据库表进行交互的类。
    【重构版】适配 person_metadata 单表架构。
    """
    def __init__(self):
        logger.trace("ActorDBManager 初始化 (PostgreSQL mode - Single Table)。")

    def get_translation_from_db(self, cursor: psycopg2.extensions.cursor, text: str, by_translated_text: bool = False) -> Optional[Dict[str, Any]]:
        """【PostgreSQL版】从数据库获取翻译缓存，并自我净化坏数据。"""
        try:
            if by_translated_text:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE translated_text = %s"
            else:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE original_text = %s"

            cursor.execute(sql, (text,))
            row = cursor.fetchone()

            if not row:
                return None

            translated_text = row['translated_text']
            
            if translated_text and not contains_chinese(translated_text):
                original_text_key = row['original_text']
                logger.warning(f"  ➜ 发现无效的历史翻译缓存: '{original_text_key}' -> '{translated_text}'。将自动销毁此记录。")
                try:
                    cursor.execute("DELETE FROM translation_cache WHERE original_text = %s", (original_text_key,))
                except Exception as e_delete:
                    logger.error(f"  ➜ 销毁无效缓存 '{original_text_key}' 时失败: {e_delete}")
                return None
            
            return dict(row)

        except Exception as e:
            logger.error(f"  ➜ 读取翻译缓存时发生错误 for '{text}': {e}", exc_info=True)
            return None

    def save_translation_to_db(self, cursor, original_text, translated_text, engine_used):
        # 终极防御：无论传入什么鬼东西，都安全地提取为字符串
        if isinstance(translated_text, (list, tuple, set)):
            translated_text = next((x for x in translated_text if isinstance(x, str) and x.strip()), None)

        if not translated_text:
            return

        # 强制转为字符串并去除首尾空格
        translated_text = str(translated_text).strip()

        if not translated_text or not contains_chinese(translated_text):
            logger.warning(f"  ➜ 翻译结果 '{translated_text}' 不含中文或为空，已丢弃。原文: '{original_text}'")
            return

        try:
            sql = """
                INSERT INTO translation_cache (original_text, translated_text, engine_used, last_updated_at) 
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (original_text) DO UPDATE SET
                    translated_text = EXCLUDED.translated_text,
                    engine_used = EXCLUDED.engine_used,
                    last_updated_at = NOW();
            """
            cursor.execute(sql, (original_text, translated_text, engine_used))
            cursor.connection.commit()
            logger.trace(f"  ➜ 翻译缓存存DB: '{original_text}' -> '{translated_text}' (引擎: {engine_used})")
        except Exception as e:
            logger.error(f"  ➜ DB保存翻译缓存失败 for '{original_text}': {e}", exc_info=True)

    def get_full_actor_details_by_tmdb_ids(self, cursor: psycopg2.extensions.cursor, tmdb_ids: List[Any]) -> Dict[int, Dict[str, Any]]:
        """
        根据一组 TMDB ID，从 person_metadata 表中高效地获取所有演员的详细信息。
        """
        if not tmdb_ids:
            return {}

        logger.debug(f"  ➜ [演员数据管家] 正在批量查询 {len(tmdb_ids)} 位演员的详细元数据...")
        
        try:
            int_tmdb_ids = [int(tid) for tid in tmdb_ids]
        except (ValueError, TypeError):
            logger.error("  ➜ [演员数据管家] 转换演员 TMDb ID 为整数时失败，列表可能包含无效数据。")
            return {}

        try:
            # 使用 AS tmdb_id 兼容旧代码的键名习惯
            sql = "SELECT tmdb_person_id AS tmdb_id, * FROM person_metadata WHERE tmdb_person_id = ANY(%s)"
            cursor.execute(sql, (int_tmdb_ids,))
            
            results = cursor.fetchall()
            actor_details_map = {row['tmdb_id']: dict(row) for row in results}
            
            logger.debug(f"  ➜ [演员数据管家] 成功从数据库中找到了 {len(actor_details_map)} 条匹配的演员元数据。")
            return actor_details_map

        except Exception as e:
            logger.error(f"  ➜ [演员数据管家] 批量查询演员元数据时失败: {e}", exc_info=True)
            raise

    def find_person_by_any_id(self, cursor: psycopg2.extensions.cursor, **kwargs) -> Optional[dict]:
        search_criteria = [
            ("tmdb_person_id", kwargs.get("tmdb_id")),
            ("imdb_id", kwargs.get("imdb_id")),
            ("douban_celebrity_id", kwargs.get("douban_id")),
        ]
        for column, value in search_criteria:
            if not value: continue
            try:
                cursor.execute(f"SELECT * FROM person_metadata WHERE {column} = %s", (value,))
                result = cursor.fetchone()
                if result:
                    logger.debug(f"  ➜ 通过 {column}='{value}' 找到了演员记录 (tmdb: {result['tmdb_person_id']})。")
                    return result
            except psycopg2.Error as e:
                logger.error(f"  ➜ 查询 person_metadata 时出错 ({column}={value}): {e}")
        return None
    
    def enrich_actors_with_provider_ids(
        self,
        cursor: psycopg2.extensions.cursor,
        raw_emby_actors: List[Dict[str, Any]],
        emby_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        【优化版】批量补充演员 ProviderIds，解决 N+1 请求导致的入库缓慢问题。
        """
        if not raw_emby_actors:
            return []

        enriched_actors_map = {}
        ids_to_fetch_from_api = []

        for actor in raw_emby_actors:
            person_id = actor.get("Id")
            if not person_id:
                continue

            actor_copy = actor.copy()
            enriched_actors_map[person_id] = actor_copy

            provider_ids = actor_copy.get("ProviderIds") or {}
            tmdb_provider_id = provider_ids.get("Tmdb")
            
            # 1. 如果已经有 TMDb ID，跳过
            if tmdb_provider_id:
                continue

            # 2. 如果是中文名，通常不需要查外部ID（靠名字匹配即可），跳过以节省开销
            actor_name = str(actor_copy.get("Name") or "").strip()
            if actor_name and contains_chinese(actor_name):
                continue

            ids_to_fetch_from_api.append(person_id)

        if ids_to_fetch_from_api:
            # ★★★ 核心优化：使用批量获取接口，将 N 次请求合并为 1 次 ★★★
            from handler.emby import get_emby_items_by_id
            
            logger.info(f"  ➜ [演员数据管家] 正在批量获取 {len(ids_to_fetch_from_api)} 位演员的外部ID...")
            
            # get_emby_items_by_id 内部会自动处理分页（每100个一组）
            batch_details = get_emby_items_by_id(
                base_url=emby_config['url'],
                api_key=emby_config['api_key'],
                user_id=emby_config['user_id'],
                item_ids=ids_to_fetch_from_api,
                fields="ProviderIds"
            )

            # 将查到的 ProviderIds 写回 map
            for item in batch_details:
                eid = item.get("Id")
                if eid in enriched_actors_map:
                    enriched_actors_map[eid]["ProviderIds"] = item.get("ProviderIds")
                    
            logger.debug(f"  ➜ [演员数据管家] 批量补充完成。")
        else:
            logger.debug("  ➜ [演员数据管家] 无需通过 Emby API 补充演员外部ID。")

        return list(enriched_actors_map.values())
    
    def upsert_person(self, cursor: psycopg2.extensions.cursor, person_data: Dict[str, Any], emby_config: Dict[str, Any]) -> Tuple[int, str]:
        """
        【重构版】单表 Upsert，直接返回 tmdb_person_id。
        (已彻底移除 emby_person_id 字段)
        """
        tmdb_id_raw = person_data.get("id") or person_data.get("tmdb_id")
        imdb_id = str(person_data.get("imdb_id") or '').strip() or None
        douban_id = str(person_data.get("douban_id") or '').strip() or None
        name = str(person_data.get("name") or '').strip()

        tmdb_id = None
        if tmdb_id_raw and str(tmdb_id_raw).isdigit():
            try:
                tmdb_id = int(tmdb_id_raw)
            except (ValueError, TypeError):
                pass

        if not tmdb_id:
            logger.warning(f"upsert_person 调用缺少有效的 tmdb_person_id，跳过。 (原始值: {tmdb_id_raw})")
            return -1, "SKIPPED"

        if not name:
            name = "Unknown Actor"

        try:
            sql = """
                INSERT INTO person_metadata 
                (tmdb_person_id, primary_name, imdb_id, douban_celebrity_id, last_updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (tmdb_person_id) DO UPDATE SET
                    primary_name = EXCLUDED.primary_name,
                    imdb_id = COALESCE(EXCLUDED.imdb_id, person_metadata.imdb_id),
                    douban_celebrity_id = COALESCE(EXCLUDED.douban_celebrity_id, person_metadata.douban_celebrity_id),
                    last_updated_at = NOW()
                WHERE
                    person_metadata.primary_name IS DISTINCT FROM EXCLUDED.primary_name OR
                    person_metadata.imdb_id IS DISTINCT FROM COALESCE(EXCLUDED.imdb_id, person_metadata.imdb_id) OR
                    person_metadata.douban_celebrity_id IS DISTINCT FROM COALESCE(EXCLUDED.douban_celebrity_id, person_metadata.douban_celebrity_id)
                RETURNING tmdb_person_id, (CASE xmax WHEN 0 THEN 'INSERTED' ELSE 'UPDATED' END) as action;
            """
            
            cursor.execute(sql, (tmdb_id, name, imdb_id, douban_id))
            result = cursor.fetchone()

            if result:
                action = result['action']
                logger.debug(f"  ├─ 演员 '{name}' (TMDb: {tmdb_id}) 处理完成。结果: {action}")
            else:
                action = "UNCHANGED"
                logger.trace(f"  ➜ 演员 '{name}' (TMDb: {tmdb_id}) 数据无变化，标记为 UNCHANGED。")

            # 统一处理元数据更新
            if 'profile_path' in person_data or 'gender' in person_data or 'popularity' in person_data:
                self.update_actor_metadata_from_tmdb(cursor, tmdb_id, person_data)

            return tmdb_id, action

        except psycopg2.IntegrityError as ie:
            conn = cursor.connection
            conn.rollback()
            logger.error(f"upsert_person 发生数据库完整性冲突，tmdb_id={tmdb_id}: {ie}")
            return -1, "ERROR"
        except Exception as e:
            conn = cursor.connection
            conn.rollback()
            logger.error(f"upsert_person 发生未知异常，tmdb_id={tmdb_id}: {e}", exc_info=True)
            return -1, "ERROR"
    def update_actor_metadata_from_tmdb(self, cursor: psycopg2.extensions.cursor, tmdb_id: int, tmdb_data: Dict[str, Any]):
        """
        【重构版】将从 TMDb API 获取的演员详情数据，更新到 person_metadata 表中。
        """
        if not tmdb_id or not tmdb_data:
            return

        try:
            metadata = {
                "profile_path": tmdb_data.get("profile_path"),
                "gender": tmdb_data.get("gender"),
                "adult": tmdb_data.get("adult", False),
                "popularity": tmdb_data.get("popularity"),
                "original_name": tmdb_data.get("original_name")
            }

            columns = list(metadata.keys())
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['%s'] * len(columns))
            
            update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns]
            update_str = ', '.join(update_clauses) + ", last_updated_at = NOW()"

            # 使用 INSERT ... ON CONFLICT 确保即使该演员还没被 upsert_person 插入，也能安全写入
            sql = f"""
                INSERT INTO person_metadata (tmdb_person_id, primary_name, {columns_str}, last_updated_at)
                VALUES (%s, 'Unknown', {placeholders_str}, NOW())
                ON CONFLICT (tmdb_person_id) DO UPDATE SET {update_str}
            """
            
            params = [tmdb_id] + list(metadata.values())
            cursor.execute(sql, tuple(params))
            logger.trace(f"  ➜ 成功将演员 (TMDb ID: {tmdb_id}) 的元数据缓存到数据库。")

        except Exception as e:
            logger.error(f"  ➜ 缓存演员 (TMDb ID: {tmdb_id}) 元数据到数据库时失败: {e}", exc_info=True)

#   --- 获取所有演员订阅的简略列表 ---
def get_all_actor_subscriptions() -> List[Dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, tmdb_person_id, actor_name, profile_path, status, last_checked_at FROM actor_subscriptions ORDER BY added_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 获取演员订阅列表失败: {e}", exc_info=True)
        raise

#   --- 获取单个订阅的完整详情 ---
def get_single_subscription_details(subscription_id: int) -> Optional[Dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            sub_row = cursor.fetchone()
            if not sub_row:
                return None
            
            source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
            cursor.execute(
                """
                    SELECT 
                        m.tmdb_id as tmdb_media_id, 
                        m.item_type as media_type,
                        m.title, 
                        m.release_date, 
                        m.poster_path,
                        m.subscription_status as status,
                        m.emby_item_ids_json,
                        m.in_library,
                        m.ignore_reason,
                        m.season_number,
                        p.title as parent_title
                    FROM media_metadata m
                    LEFT JOIN media_metadata p ON m.parent_series_tmdb_id = p.tmdb_id
                    WHERE m.subscription_sources_json @> %s::jsonb
                    ORDER BY m.release_date DESC
                """, 
                (source_filter,)
            )
            
            tracked_media = []
            for row in cursor.fetchall():
                media_item = dict(row)
                
                if media_item['media_type'] == 'Season':
                    parent_title = media_item.get('parent_title')
                    season_num = media_item.get('season_number')
                    if parent_title and season_num is not None:
                        media_item['title'] = f"{parent_title} 第 {season_num} 季"
                
                final_status = ''
                if media_item.get('in_library'):
                    final_status = 'IN_LIBRARY'
                else:
                    backend_status = media_item.get('status')
                    if backend_status == 'SUBSCRIBED':
                        final_status = 'SUBSCRIBED'
                    elif backend_status == 'WANTED':
                        final_status = 'WANTED'
                    elif backend_status == 'IGNORED':
                        final_status = 'IGNORED'
                    else:
                        release_date = media_item.get('release_date')
                        if release_date and release_date.strftime('%Y-%m-%d') > datetime.now().strftime('%Y-%m-%d'):
                            final_status = 'PENDING_RELEASE'
                        else:
                            final_status = 'MISSING'
                
                media_item['status'] = final_status
                
                emby_ids = media_item.get('emby_item_ids_json', [])
                media_item['emby_item_id'] = emby_ids[0] if emby_ids else None
                tracked_media.append(media_item)

            emby_url = APP_CONFIG.get("emby_server_url", "").rstrip('/')
            emby_api_key = APP_CONFIG.get("emby_api_key", "")
            emby_server_id = extensions.EMBY_SERVER_ID

            response_data = {
                "id": sub_row['id'],
                "tmdb_person_id": sub_row['tmdb_person_id'],
                "actor_name": sub_row['actor_name'],
                "profile_path": sub_row['profile_path'],
                "status": sub_row['status'],
                "last_checked_at": sub_row['last_checked_at'],
                "added_at": sub_row['added_at'],
                "config": {
                    "start_year": sub_row.get('config_start_year'),
                    "media_types": [t.strip() for t in (sub_row.get('config_media_types') or '').split(',') if t.strip()],
                    "genres_include_json": sub_row.get('config_genres_include_json') or [],
                    "genres_exclude_json": sub_row.get('config_genres_exclude_json') or [],
                    "min_rating": float(sub_row.get('config_min_rating', 0.0)),
                    "main_role_only": sub_row.get('config_main_role_only', False),
                    "min_vote_count": sub_row.get('config_min_vote_count', 10)
                },
                "tracked_media": tracked_media,
                "emby_server_url": emby_url,
                "emby_api_key_for_url": emby_api_key,
                "emby_server_id": emby_server_id
            }
            
            return response_data
            
    except Exception as e:
        logger.error(f"DB: 获取订阅详情 {subscription_id} 失败: {e}", exc_info=True)
        raise

#   --- 新增演员订阅 ---
def safe_json_dumps(value):
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            return json.dumps(value, ensure_ascii=False)
    else:
        return json.dumps(value, ensure_ascii=False)

# --- 添加演员订阅 ---
def add_actor_subscription(tmdb_person_id: int, actor_name: str, profile_path: str, config: dict) -> int:
    """添加一个新的演员订阅，并返回新创建的订阅ID。"""
    start_year = config.get('start_year', 1900)
    media_types_list = config.get('media_types', ['Movie','TV'])
    if isinstance(media_types_list, list):
        media_types = ','.join(media_types_list)
    else:
        media_types = str(media_types_list)

    genres_include = safe_json_dumps(config.get('genres_include_json', []))
    genres_exclude = safe_json_dumps(config.get('genres_exclude_json', []))
    min_rating = config.get('min_rating', 6.0)
    main_role_only = config.get('main_role_only', False)
    min_vote_count = config.get('min_vote_count', 10)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            sql = """
                INSERT INTO actor_subscriptions 
                (tmdb_person_id, actor_name, profile_path, status, config_start_year, config_media_types, config_genres_include_json, config_genres_exclude_json, config_min_rating, config_main_role_only, config_min_vote_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            cursor.execute(
                sql,
                (tmdb_person_id, actor_name, profile_path, 'active', start_year, media_types, genres_include, genres_exclude, min_rating, main_role_only, min_vote_count)
            )
            
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建的演员订阅ID。")
            
            new_id = result['id']
            conn.commit()
            
            logger.info(f"  ➜ 成功添加演员订阅 '{actor_name}'。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except Exception as e:
        logger.error(f"  ➜ 添加演员订阅 '{actor_name}' 时失败: {e}", exc_info=True)
        raise

#   --- 更新演员订阅 ---
def update_actor_subscription(subscription_id: int, data: dict) -> bool:
    logger.debug(f"  ➜ 准备更新订阅ID {subscription_id}，接收到的原始数据: {data}")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
                current_sub = cursor.fetchone()
                if not current_sub:
                    return False

                current_config = {
                    'start_year': current_sub['config_start_year'],
                    'min_rating': float(current_sub['config_min_rating']),
                    'media_types': [t.strip() for t in (current_sub.get('config_media_types') or '').split(',') if t.strip()],
                    'genres_include_json': current_sub.get('config_genres_include_json') or [],
                    'genres_exclude_json': current_sub.get('config_genres_exclude_json') or [],
                    'main_role_only': current_sub.get('config_main_role_only', False),
                    'min_vote_count': current_sub.get('config_min_vote_count', 10)
                }
                
                old_config_snapshot = current_config.copy()
                incoming_config = data.get('config', data)
                final_config = {**current_config, **incoming_config}

                final_media_types_str = ','.join(final_config.get('media_types', []))
                final_genres_include_json = json.dumps(final_config.get('genres_include_json', []), ensure_ascii=False)
                final_genres_exclude_json = json.dumps(final_config.get('genres_exclude_json', []), ensure_ascii=False)

                sql = """
                    UPDATE actor_subscriptions SET
                    status = %s, config_start_year = %s, config_media_types = %s, 
                    config_genres_include_json = %s, config_genres_exclude_json = %s, config_min_rating = %s,
                    config_main_role_only = %s, config_min_vote_count = %s
                    WHERE id = %s
                """
                params = (
                    data.get('status', current_sub['status']), 
                    final_config['start_year'], 
                    final_media_types_str,
                    final_genres_include_json, 
                    final_genres_exclude_json, 
                    final_config['min_rating'],
                    final_config['main_role_only'],
                    final_config['min_vote_count'], 
                    subscription_id
                )
                
                cursor.execute(sql, params)
                logger.info(f"  ➜ 成功更新订阅ID {subscription_id} 的配置。")

                if final_config != old_config_snapshot:
                    logger.info(f"  ➜ 检测到订阅ID {subscription_id} 的筛选配置发生变更，将重置检查时间并清理历史忽略记录...")
                    cursor.execute("UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = NULL WHERE id = %s", (subscription_id,))

                    source_to_remove = {"type": "actor_subscription", "id": subscription_id}
                    source_filter = json.dumps([source_to_remove])
                    cursor.execute(
                        "SELECT tmdb_id, item_type FROM media_metadata WHERE subscription_status = 'IGNORED' AND subscription_sources_json @> %s::jsonb",
                        (source_filter,)
                    )
                    items_to_clean = cursor.fetchall()
                    for item in items_to_clean:
                        request_db.remove_subscription_source(item['tmdb_id'], item['item_type'], source_to_remove)
                    logger.info(f"  ➜ 成功清理 {len(items_to_clean)} 条旧的'忽略'记录，下次刷新时将重新评估。")
                
                conn.commit()
                return True
                
    except Exception as e:
        logger.error(f"  ➜ 更新订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

#   --- 删除演员订阅 ---
def delete_actor_subscription(subscription_id: int) -> bool:
    """删除一个演员订阅，并智能清理其在 media_metadata 中的追踪记录及 MoviePilot 订阅。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # 1. 获取订阅详情
            cursor.execute("SELECT actor_name, tmdb_person_id FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            sub_info = cursor.fetchone()
            if not sub_info:
                logger.warning(f"  ➜ 尝试删除一个不存在的订阅 (ID: {subscription_id})。")
                return True

            actor_name = sub_info['actor_name']
            source_to_remove = {
                "type": "actor_subscription", 
                "id": subscription_id,
                "name": actor_name,
                "person_id": sub_info['tmdb_person_id']
            }
            # 构建用于查询的 JSONB 过滤器
            source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
            
            # 2. 获取所有包含该订阅源的媒体项 (增加 title 和 in_library 字段)
            cursor.execute(
                """
                SELECT tmdb_id, item_type, season_number, subscription_sources_json, title, in_library 
                FROM media_metadata 
                WHERE subscription_sources_json @> %s::jsonb
                """,
                (source_filter,)
            )
            items_to_clean = cursor.fetchall()

            logger.info(f"  ➜ 正在移除演员 '{actor_name}' 的订阅，共涉及 {len(items_to_clean)} 个媒体项...")
            
            # 3. 遍历清理并智能联动 MoviePilot
            for item in items_to_clean:
                tmdb_id = item['tmdb_id']
                item_type = item['item_type']
                season_num = item['season_number']
                title = item['title'] or f"未知项目({tmdb_id})"
                in_library = item['in_library']
                sources = item['subscription_sources_json']
                
                if isinstance(sources, str):
                    try: sources = json.loads(sources)
                    except: sources = []
                        
                if not isinstance(sources, list):
                    sources = []
                    
                # 过滤掉当前要删除的演员源
                new_sources = [s for s in sources if not (s.get('type') == source_to_remove['type'] and s.get('id') == source_to_remove['id'])]
                
                # ★★★ 核心逻辑 A：判断剩下的源里，还有没有"真正的"订阅者 ★★★
                # 过滤掉 manual_admin_op 等没有 name 的系统级占位符
                real_other_sources = [s for s in new_sources if s.get('name') or s.get('type') in ['actor_subscription', 'user_request']]
                
                if not real_other_sources:
                    # 如果没有其他真正的订阅源了
                    logger.info(f"    - 《{title}》({item_type}) 已无其他有效订阅源，正在同步取消 MoviePilot 订阅...")
                    moviepilot.cancel_subscription(tmdb_id, item_type, APP_CONFIG, season=season_num)
                    
                    if not in_library:
                        # ★★★ 核心逻辑 B：如果不在库，直接物理删除这条记录，释放数据库空间！ ★★★
                        logger.info(f"    - 《{title}》不在媒体库中，直接从数据库彻底删除该条目。")
                        cursor.execute("DELETE FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (tmdb_id, item_type))
                    else:
                        # 如果在库，仅重置订阅状态，保留资产数据
                        logger.info(f"    - 《{title}》已在媒体库中，仅重置其订阅状态。")
                        cursor.execute("""
                            UPDATE media_metadata 
                            SET subscription_sources_json = '[]'::jsonb,
                                subscription_status = 'NONE'
                            WHERE tmdb_id = %s AND item_type = %s
                        """, (tmdb_id, item_type))
                else:
                    # ★★★ 核心逻辑 C：如果还有其他真实源，仅更新 JSON，保留 MoviePilot 订阅 ★★★
                    other_names = [s.get('name', '未知用户/规则') for s in real_other_sources]
                    logger.info(f"    - 《{title}》({item_type}) 仍被 {other_names} 订阅，仅移除演员 '{actor_name}' 的来源标签。")
                    cursor.execute("""
                        UPDATE media_metadata 
                        SET subscription_sources_json = %s::jsonb
                        WHERE tmdb_id = %s AND item_type = %s
                    """, (json.dumps(new_sources, ensure_ascii=False), tmdb_id, item_type))

            # 4. 最后删除订阅本身
            cursor.execute("DELETE FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            conn.commit()
            logger.info(f"  ➜ 成功删除演员 '{actor_name}' 的订阅及其所有关联追踪记录。")
            return True
            
    except Exception as e:
        logger.error(f"  ➜ 删除订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

#   --- 为演员订阅任务获取所有在库媒体数据 ---
def get_all_in_library_media_for_actor_sync() -> Tuple[Dict[str, str], Dict[str, Set[int]], Dict[str, str]]:
    emby_media_map = {}
    emby_series_seasons_map = {}
    emby_series_name_to_tmdb_id_map = {}

    sql = """
        SELECT tmdb_id, item_type, title, emby_item_ids_json 
        FROM media_metadata 
        WHERE in_library = TRUE AND item_type IN ('Movie', 'Series');
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                top_level_items = cursor.fetchall()

                series_tmdb_ids = []
                for item in top_level_items:
                    tmdb_id = str(item['tmdb_id'])
                    emby_ids = item.get('emby_item_ids_json')
                    
                    if emby_ids and len(emby_ids) > 0:
                        emby_media_map[tmdb_id] = emby_ids[0]

                    if item['item_type'] == 'Series':
                        series_tmdb_ids.append(tmdb_id)
                        normalized_name = utils.normalize_name_for_matching(item.get('title', ''))
                        if normalized_name:
                            emby_series_name_to_tmdb_id_map[normalized_name] = tmdb_id
                
                if series_tmdb_ids:
                    cursor.execute(
                        """
                        SELECT parent_series_tmdb_id, season_number 
                        FROM media_metadata 
                        WHERE in_library = TRUE AND item_type = 'Season' AND parent_series_tmdb_id = ANY(%s)
                        """,
                        (series_tmdb_ids,)
                    )
                    for row in cursor.fetchall():
                        parent_id = str(row['parent_series_tmdb_id'])
                        season_num = row['season_number']
                        if parent_id not in emby_series_seasons_map:
                            emby_series_seasons_map[parent_id] = set()
                        emby_series_seasons_map[parent_id].add(season_num)

        return emby_media_map, emby_series_seasons_map, emby_series_name_to_tmdb_id_map

    except Exception as e:
        logger.error(f"DB: 为演员同步任务准备在库媒体数据时失败: {e}", exc_info=True)
        return {}, {}, {}

#   --- 批量获取演员中文名 ---    
def get_actor_chinese_names_by_tmdb_ids(tmdb_ids: List[int]) -> Dict[int, str]:
    """
    【重构版】从 person_metadata 单表查询。
    """
    if not tmdb_ids:
        return {}

    name_map = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT tmdb_person_id, primary_name 
                    FROM person_metadata 
                    WHERE tmdb_person_id = ANY(%s)
                """
                cursor.execute(sql, (tmdb_ids,))
                rows = cursor.fetchall()
                for row in rows:
                    if row['primary_name'] and contains_chinese(row['primary_name']):
                        name_map[row['tmdb_person_id']] = row['primary_name']
        return name_map
    except Exception as e:
        logger.error(f"DB: 批量查询演员中文名时失败: {e}", exc_info=True)
        return {}