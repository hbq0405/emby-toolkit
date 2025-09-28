# database/actor_db.py
import psycopg2
import logging
import json
from typing import Optional, Dict, Any, List, Tuple

from .connection import get_db_connection
from utils import contains_chinese
from emby_handler import get_emby_item_details

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 演员数据访问 
# ======================================================================

class ActorDBManager:
    """
    一个专门负责与演员身份相关的数据库表进行交互的类。
    """
    def __init__(self):
        logger.trace("ActorDBManager 初始化 (PostgreSQL mode)。")

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
                logger.warning(f"  -> 发现无效的历史翻译缓存: '{original_text_key}' -> '{translated_text}'。将自动销毁此记录。")
                try:
                    cursor.execute("DELETE FROM translation_cache WHERE original_text = %s", (original_text_key,))
                except Exception as e_delete:
                    logger.error(f"销毁无效缓存 '{original_text_key}' 时失败: {e_delete}")
                return None
            
            return dict(row)

        except Exception as e:
            logger.error(f"DB读取翻译缓存时发生错误 for '{text}': {e}", exc_info=True)
            return None


    def save_translation_to_db(self, cursor: psycopg2.extensions.cursor, original_text: str, translated_text: Optional[str], engine_used: Optional[str]):
        """【PostgreSQL版】将翻译结果保存到数据库，增加中文校验。"""
        
        if translated_text and translated_text.strip() and not contains_chinese(translated_text):
            logger.warning(f"翻译结果 '{translated_text}' 不含中文，已丢弃。原文: '{original_text}'")
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
            logger.trace(f"翻译缓存存DB: '{original_text}' -> '{translated_text}' (引擎: {engine_used})")
        except Exception as e:
            logger.error(f"DB保存翻译缓存失败 for '{original_text}': {e}", exc_info=True)


    def find_person_by_any_id(self, cursor: psycopg2.extensions.cursor, **kwargs) -> Optional[dict]:
        
        search_criteria = [
            ("tmdb_person_id", kwargs.get("tmdb_id")),
            ("emby_person_id", kwargs.get("emby_id")),
            ("imdb_id", kwargs.get("imdb_id")),
            ("douban_celebrity_id", kwargs.get("douban_id")),
        ]
        for column, value in search_criteria:
            if not value: continue
            try:
                cursor.execute(f"SELECT * FROM person_identity_map WHERE {column} = %s", (value,))
                result = cursor.fetchone()
                if result:
                    logger.debug(f"通过 {column}='{value}' 找到了演员记录 (map_id: {result['map_id']})。")
                    return result
            except psycopg2.Error as e:
                logger.error(f"查询 person_identity_map 时出错 ({column}={value}): {e}")
        return None

    def upsert_person(self, cursor: psycopg2.extensions.cursor, person_data: Dict[str, Any], emby_config: Dict[str, Any]) -> Tuple[int, str]:
        """
        【V6 - 终极防冲突重构版】
        ...
        """
        emby_id = str(person_data.get("emby_id") or '').strip() or None
        tmdb_id_raw = person_data.get("tmdb_id")
        imdb_id = str(person_data.get("imdb_id") or '').strip() or None
        douban_id = str(person_data.get("douban_id") or '').strip() or None
        name = str(person_data.get("name") or '').strip()

        tmdb_id = None
        if tmdb_id_raw and str(tmdb_id_raw).isdigit():
            try:
                tmdb_id = int(tmdb_id_raw)
            except (ValueError, TypeError):
                pass

        # ★★★ 核心修改：将检查条件从 emby_id 更改为 tmdb_id ★★★
        if not tmdb_id:
            logger.warning(f"upsert_person 调用缺少有效的 tmdb_person_id，跳过。 (原始值: {tmdb_id_raw})")
            return -1, "SKIPPED"

        # 如果没有emby_id，这是“归档待用”模式，但仍需继续执行以保存其他ID
        if not emby_id:
            logger.debug(f"  -> [归档模式] upsert_person 缺少 emby_id，将仅处理外部ID映射。")

        try:
            cursor.execute("SAVEPOINT actor_upsert")

            # --- 步骤 1: 查找现有记录 (更强大的查找逻辑) ---
            existing_record = None
            
            # 路径 A: 正常流程，用 Emby ID 查找
            cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (emby_id,))
            existing_record = cursor.fetchone()

            # ★★★ 核心修复：如果按 emby_id 找不到，就按 tmdb_id 找 ★★★
            # 无论旧记录的 emby_id 是什么，只要 tmdb_id 匹配，就认为是同一个人
            if not existing_record and tmdb_id:
                cursor.execute("SELECT * FROM person_identity_map WHERE tmdb_person_id = %s", (tmdb_id,))
                existing_record = cursor.fetchone()
                if existing_record:
                    old_emby_id = existing_record.get('emby_person_id')
                    logger.info(f"  -> [智能重联] 演员 '{name}' (TMDb: {tmdb_id}) 已存在于数据库 (旧 Emby ID: {old_emby_id})。将更新为新的 Emby ID '{emby_id}'。")

            # --- 步骤 2: 根据查找结果，决定是 UPDATE 还是 INSERT ---
            if existing_record:
                # --- UPDATE 现有记录 ---
                map_id = existing_record['map_id']
                updates = {}

                # ★★★ 核心修正：增加对 primary_name 的更新逻辑 ★★★
                # 如果传入的名字非空，并且与数据库中已有的名字不同，则更新它
                if name and name != existing_record.get('primary_name'):
                    updates['primary_name'] = name
                
                # 核心：用新的 Emby ID 更新找到的记录
                if emby_id and existing_record.get('emby_person_id') != emby_id:
                    updates['emby_person_id'] = emby_id

                # 补充缺失的 ID 信息
                if tmdb_id and not existing_record.get('tmdb_person_id'):
                    updates['tmdb_person_id'] = tmdb_id
                if imdb_id and not existing_record.get('imdb_id'):
                    updates['imdb_id'] = imdb_id
                if douban_id and not existing_record.get('douban_celebrity_id'):
                    updates['douban_celebrity_id'] = douban_id
                
                if updates:
                    set_clauses = [f"{k} = %s" for k in updates.keys()]
                    set_clauses.append("last_updated_at = NOW()")
                    sql = f"UPDATE person_identity_map SET {', '.join(set_clauses)} WHERE map_id = %s"
                    cursor.execute(sql, tuple(updates.values()) + (map_id,))
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return map_id, "UPDATED"
                else:
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return map_id, "UNCHANGED"
            else:
                # --- INSERT 新记录 (只有在绝对找不到时才执行) ---
                if not name:
                    details = get_emby_item_details(emby_id, emby_config['url'], emby_config['api_key'], emby_config['user_id'], fields="Name")
                    name = details.get("Name") if details else "Unknown Actor"

                sql = """
                    INSERT INTO person_identity_map 
                    (primary_name, emby_person_id, tmdb_person_id, imdb_id, douban_celebrity_id, last_updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    RETURNING map_id
                """
                cursor.execute(sql, (name, emby_id, tmdb_id, imdb_id, douban_id))
                result = cursor.fetchone()
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return (result["map_id"], "INSERTED") if result else (-1, "ERROR")

        except psycopg2.IntegrityError as ie:
            # 添加一个额外的捕获，以防万一在高并发下出现竞争条件
            cursor.execute("ROLLBACK TO SAVEPOINT actor_upsert")
            logger.error(f"upsert_person 发生罕见的唯一性冲突，可能存在并发写入。emby_person_id={emby_id}, tmdb_id={tmdb_id}: {ie}")
            cursor.execute("RELEASE SAVEPOINT actor_upsert")
            return -1, "ERROR"
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT actor_upsert")
            logger.error(f"upsert_person 发生异常，emby_person_id={emby_id}: {e}", exc_info=True)
            cursor.execute("RELEASE SAVEPOINT actor_upsert")
            return -1, "ERROR"
        
    def update_actor_name_in_media_metadata(self, cursor: psycopg2.extensions.cursor, tmdb_id: int, new_name: str) -> int:
        """
        【V1 - 新增功能】
        根据演员的 TMDB ID，查找所有 media_metadata 表中 actors_json 包含该演员的记录，
        并将其名字更新为新的中文名。

        Args:
            cursor: 数据库游标。
            tmdb_id: 演员的 TMDB ID，这是最可靠的关联键。
            new_name: 已经翻译好的中文名。

        Returns:
            成功更新的媒体记录数量。
        """
        if not tmdb_id or not new_name:
            return 0

        try:
            # 这个 SQL 查询是核心。它利用 jsonb_array_elements 解构数组，
            # 使用 CASE 语句替换匹配到的演员的名字，最后用 jsonb_agg 重新聚合为新数组。
            # WHERE 子句使用 @> 操作符高效地查找包含该演员的 JSON 记录。
            sql = """
                UPDATE media_metadata
                SET
                    actors_json = (
                        SELECT jsonb_agg(
                            CASE
                                -- 演员的 'Id' 在 JSON 中可能是字符串或数字，统一转为 text 比较
                                WHEN actor->>'Id' = %s THEN
                                    jsonb_set(actor, '{Name}', to_jsonb(%s::text))
                                ELSE
                                    actor
                            END
                        )
                        FROM jsonb_array_elements(actors_json) AS actor
                    )
                WHERE
                    -- 使用 @> 操作符和索引来高效地查找包含此演员ID的记录
                    actors_json @> ('[{"Id": "' || %s || '"}]')::jsonb;
            """
            
            # 将 tmdb_id 转为字符串以匹配 JSON 内部的值
            tmdb_id_str = str(tmdb_id)
            
            cursor.execute(sql, (tmdb_id_str, new_name, tmdb_id_str))
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.debug(f"  -> 媒体库演员名同步: TMDB ID {tmdb_id} 的姓名更新为 '{new_name}'，影响了 {updated_count} 条记录。")
            
            return updated_count

        except Exception as e:
            logger.error(f"更新 media_metadata 中演员 (TMDB ID: {tmdb_id}) 姓名时失败: {e}", exc_info=True)
            # 即使这里失败，也不应该中断整个同步流程，所以只记录错误并返回0
            return 0

def get_all_emby_person_ids_from_map() -> set:
    """从 person_identity_map 表中获取所有 emby_person_id 的集合。"""
    
    ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_person_id FROM person_identity_map")
            rows = cursor.fetchall()
            for row in rows:
                ids.add(row['emby_person_id'])
        return ids
    except Exception as e:
        logger.error(f"DB: 获取所有演员映射Emby ID时失败: {e}", exc_info=True)
        raise

def delete_persons_by_emby_ids(emby_ids: list) -> int:
    """根据 Emby Person ID 列表，从 person_identity_map 表中批量删除记录。"""
    
    if not emby_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM person_identity_map WHERE emby_person_id = ANY(%s)"
            cursor.execute(sql, (emby_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"  -> 从演员映射表中删除了 {deleted_count} 条陈旧记录。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除陈旧演员映射时失败: {e}", exc_info=True)
        raise

# --- 演员订阅数据访问 ---

def get_all_actor_subscriptions() -> List[Dict[str, Any]]:
    """获取所有演员订阅的简略列表。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, tmdb_person_id, actor_name, profile_path, status, last_checked_at FROM actor_subscriptions ORDER BY added_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取演员订阅列表失败: {e}", exc_info=True)
        raise

def get_single_subscription_details(subscription_id: int) -> Optional[Dict[str, Any]]:
    """【V2 - 格式化修复版】获取单个订阅的完整详情。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            sub_row = cursor.fetchone()
            if not sub_row:
                return None
            
            cursor.execute("SELECT * FROM tracked_actor_media WHERE subscription_id = %s ORDER BY release_date DESC", (subscription_id,))
            tracked_media = [dict(row) for row in cursor.fetchall()]
            
            def _safe_json_loads(json_string, default_value=None):
                if default_value is None:
                    default_value = []
                if isinstance(json_string, str):
                    try:
                        return json.loads(json_string)
                    except json.JSONDecodeError:
                        return default_value
                return json_string if json_string is not None else default_value

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
                    "min_rating": float(sub_row.get('config_min_rating', 0.0))
                },
                "tracked_media": tracked_media
            }
            
            return response_data
            
    except Exception as e:
        logger.error(f"DB: 获取订阅详情 {subscription_id} 失败: {e}", exc_info=True)
        raise

def safe_json_dumps(value):
    """安全地将Python对象转换为JSON字符串。"""
    
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            return json.dumps(value, ensure_ascii=False)
    else:
        return json.dumps(value, ensure_ascii=False)

def add_actor_subscription(tmdb_person_id: int, actor_name: str, profile_path: str, config: dict) -> int:
    """【V3 - 最终修复版】新增一个演员订阅。"""
    
    start_year = config.get('start_year', 1900)
    media_types_list = config.get('media_types', ['Movie','TV'])
    if isinstance(media_types_list, list):
        media_types = ','.join(media_types_list)
    else:
        media_types = str(media_types_list)

    genres_include = safe_json_dumps(config.get('genres_include_json', []))
    genres_exclude = safe_json_dumps(config.get('genres_exclude_json', []))
    min_rating = config.get('min_rating', 6.0)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            sql = """
                INSERT INTO actor_subscriptions 
                (tmdb_person_id, actor_name, profile_path, status, config_start_year, config_media_types, config_genres_include_json, config_genres_exclude_json, config_min_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            cursor.execute(
                sql,
                (tmdb_person_id, actor_name, profile_path, 'active', start_year, media_types, genres_include, genres_exclude, min_rating)
            )
            
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建的演员订阅ID。")
            
            new_id = result['id']
            conn.commit()
            
            logger.info(f"DB: 成功添加演员订阅 '{actor_name}' (ID: {new_id})。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except Exception as e:
        logger.error(f"DB: 添加演员订阅 '{actor_name}' 时失败: {e}", exc_info=True)
        raise

def update_actor_subscription(subscription_id: int, data: dict) -> bool:
    """【V6 - 逻辑重构最终修复版】更新一个演员订阅的状态或配置。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            current_sub = cursor.fetchone()
            if not current_sub:
                return False

            new_status = current_sub['status']
            new_start_year = current_sub['config_start_year']
            new_min_rating = current_sub['config_min_rating']
            new_genres_include_list = current_sub.get('config_genres_include_json') or []
            new_genres_exclude_list = current_sub.get('config_genres_exclude_json') or []
            new_media_types_list = [t.strip() for t in (current_sub.get('config_media_types') or '').split(',') if t.strip()]

            new_status = data.get('status', new_status)

            config = data.get('config')
            if config is not None:
                new_start_year = config.get('start_year', new_start_year)
                new_min_rating = config.get('min_rating', new_min_rating)
                if 'media_types' in config and isinstance(config['media_types'], list):
                    new_media_types_list = config['media_types']
                if 'genres_include_json' in config and isinstance(config['genres_include_json'], list):
                    new_genres_include_list = config['genres_include_json']
                if 'genres_exclude_json' in config and isinstance(config['genres_exclude_json'], list):
                    new_genres_exclude_list = config['genres_exclude_json']

            final_media_types_str = ','.join(new_media_types_list)
            final_genres_include_json = json.dumps(new_genres_include_list, ensure_ascii=False)
            final_genres_exclude_json = json.dumps(new_genres_exclude_list, ensure_ascii=False)

            cursor.execute("""
                UPDATE actor_subscriptions SET
                status = %s, config_start_year = %s, config_media_types = %s, 
                config_genres_include_json = %s, config_genres_exclude_json = %s, config_min_rating = %s
                WHERE id = %s
            """, (new_status, new_start_year, final_media_types_str, final_genres_include_json, final_genres_exclude_json, new_min_rating, subscription_id))
            
            conn.commit()
            logger.info(f"DB: 成功更新订阅ID {subscription_id}。")
            return True
            
    except Exception as e:
        logger.error(f"DB: 更新订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

def delete_actor_subscription(subscription_id: int) -> bool:
    """删除一个演员订阅及其所有追踪的媒体。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            conn.commit()
            logger.info(f"DB: 成功删除订阅ID {subscription_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 删除订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

def get_tracked_media_by_id(media_id: int) -> Optional[Dict[str, Any]]:
    """根据 tracked_actor_media 表的主键 ID 获取单个媒体项的完整信息。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tracked_actor_media WHERE id = %s", (media_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB: 获取已追踪媒体项 {media_id} 失败: {e}", exc_info=True)
        raise

def update_tracked_media_status(media_id: int, new_status: str) -> bool:
    """根据 tracked_actor_media 表的主键 ID 更新单个媒体项的状态。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE tracked_actor_media SET status = %s, last_updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (new_status, media_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 更新已追踪媒体项 {media_id} 状态失败: {e}", exc_info=True)
        raise