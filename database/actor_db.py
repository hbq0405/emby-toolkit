# database/actor_db.py
import psycopg2
import logging
import json
from typing import Optional, Dict, Any, List, Tuple

from .connection import get_db_connection
from utils import contains_chinese

logger = logging.getLogger(__name__)

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

    def upsert_person(self, cursor: psycopg2.extensions.cursor, person_data: Dict[str, Any], emby_config: Dict[str, str]) -> Tuple[int, str]:
        """【V4.1 - 类型安全修复版】"""
        
        id_field_map = {
            "tmdb_person_id": "Tmdb",
            "imdb_id": "Imdb",
            "douban_celebrity_id": "Douban"
        }

        cursor.execute("SAVEPOINT actor_upsert")

        try:
            tmdb_id_raw = person_data.get("tmdb_id")
            tmdb_id_int = None
            if tmdb_id_raw and str(tmdb_id_raw).isdigit():
                try:
                    tmdb_id_int = int(tmdb_id_raw)
                except (ValueError, TypeError):
                    pass 
            
            new_data = {
                "primary_name": str(person_data.get("name") or '').strip(),
                "emby_person_id": str(person_data.get("emby_id") or '').strip() or None,
                "tmdb_person_id": tmdb_id_int,
                "imdb_id": str(person_data.get("imdb_id") or '').strip() or None,
                "douban_celebrity_id": str(person_data.get("douban_id") or '').strip() or None,
            }

            if not new_data["emby_person_id"]:
                logger.warning("缺失 emby_person_id，无法执行 upsert")
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return -1, "SKIPPED"

            conflict_detected = False
            for db_column, emby_provider_key in id_field_map.items():
                id_value = new_data.get(db_column)
                if id_value is None:
                    continue

                cursor.execute(
                    f"SELECT emby_person_id FROM person_identity_map WHERE {db_column} = %s",
                    (id_value,)
                )
                conflicting_records = cursor.fetchall()
                
                if conflicting_records:
                    conflicting_emby_pids = [rec['emby_person_id'] for rec in conflicting_records]
                    if new_data["emby_person_id"] not in conflicting_emby_pids:
                        conflicting_emby_pids.append(new_data["emby_person_id"])
                    
                    if len(conflicting_emby_pids) > 1:
                        logger.warning(
                            f"检测到ID冲突: {db_column} = '{id_value}' 被多个Emby PID共享: {conflicting_emby_pids}。"
                            "将执行智能合并..."
                        )
                        
                        target_emby_pid = conflicting_emby_pids[0]
                        cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (target_emby_pid,))
                        target_actor = cursor.fetchone()
                        
                        if not target_actor:
                            logger.error(f"合并失败：无法找到目标演员记录 (Emby PID: {target_emby_pid})。")
                            continue

                        target_map_id = target_actor['map_id']
                        
                        for source_emby_pid in conflicting_emby_pids[1:]:
                            cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (source_emby_pid,))
                            source_actor = cursor.fetchone()
                            
                            if not source_actor or source_actor['map_id'] == target_map_id:
                                continue

                            source_map_id = source_actor['map_id']
                            logger.info(f"  -> 正在将源记录 (Emby PID: {source_emby_pid}, map_id: {source_map_id}) 合并到目标记录 (Emby PID: {target_emby_pid}, map_id: {target_map_id})")

                            if source_actor.get('tmdb_person_id') and not target_actor.get('tmdb_person_id'):
                                cursor.execute("UPDATE person_identity_map SET tmdb_person_id = %s WHERE map_id = %s", (source_actor['tmdb_person_id'], target_map_id))
                            if source_actor.get('imdb_id') and not target_actor.get('imdb_id'):
                                cursor.execute("UPDATE person_identity_map SET imdb_id = %s WHERE map_id = %s", (source_actor['imdb_id'], target_map_id))
                            if source_actor.get('douban_celebrity_id') and not target_actor.get('douban_celebrity_id'):
                                cursor.execute("UPDATE person_identity_map SET douban_celebrity_id = %s WHERE map_id = %s", (source_actor['douban_celebrity_id'], target_map_id))

                            cursor.execute("DELETE FROM person_identity_map WHERE map_id = %s", (source_map_id,))
                            logger.info(f"  -> 合并完成，已删除多余的源记录 (map_id: {source_map_id})。")

                        new_data["emby_person_id"] = target_emby_pid

            if conflict_detected:
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return -1, "SKIPPED"

            cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (new_data["emby_person_id"],))
            existing_record = cursor.fetchone()

            if existing_record:
                existing_record = dict(existing_record)
                update_fields = {}
                
                id_fields = id_field_map.keys()
                for f in id_fields:
                    new_val = new_data.get(f)
                    if new_val is not None and not existing_record.get(f):
                        update_fields[f] = new_val

                new_name = new_data["primary_name"]
                old_name = existing_record.get("primary_name") or ""
                if new_name and new_name != old_name:
                    update_fields["primary_name"] = new_name

                if update_fields:
                    set_clauses = [f"{k} = %s" for k in update_fields.keys()]
                    set_clauses.append("last_updated_at = NOW()")
                    sql = f"UPDATE person_identity_map SET {', '.join(set_clauses)} WHERE map_id = %s"
                    cursor.execute(sql, tuple(update_fields.values()) + (existing_record["map_id"],))
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return existing_record["map_id"], "UPDATED"
                else:
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return existing_record["map_id"], "UNCHANGED"
            else:
                insert_fields = [k for k, v in new_data.items() if v is not None]
                if not insert_fields:
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return -1, "SKIPPED"

                insert_placeholders = ["%s"] * len(insert_fields)
                insert_values = [new_data[k] for k in insert_fields]

                sql_insert = f"""
                    INSERT INTO person_identity_map ({', '.join(insert_fields)}, last_updated_at) 
                    VALUES ({', '.join(insert_placeholders)}, NOW())
                    RETURNING map_id
                """
                cursor.execute(sql_insert, tuple(insert_values))
                result = cursor.fetchone()
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return (result["map_id"], "INSERTED") if result else (-1, "ERROR")

        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT actor_upsert")
            logger.error(f"upsert_person 发生异常，emby_person_id={person_data.get('emby_id')}: {e}", exc_info=True)
            cursor.execute("RELEASE SAVEPOINT actor_upsert")
            return -1, "ERROR"

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