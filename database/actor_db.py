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


    def save_translation_to_db(self, cursor: psycopg2.extensions.cursor, original_text: str, translated_text: Optional[str], engine_used: Optional[str]):
        """【PostgreSQL版】将翻译结果保存到数据库，增加中文校验。"""
        
        if translated_text and translated_text.strip() and not contains_chinese(translated_text):
            logger.warning(f"  ➜ 翻译结果 '{translated_text}' 不含中文，已丢弃。原文: '{original_text}'")
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
            logger.trace(f"  ➜ 翻译缓存存DB: '{original_text}' -> '{translated_text}' (引擎: {engine_used})")
        except Exception as e:
            logger.error(f"  ➜ DB保存翻译缓存失败 for '{original_text}': {e}", exc_info=True)


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
                    logger.debug(f"  ➜ 通过 {column}='{value}' 找到了演员记录 (map_id: {result['map_id']})。")
                    return result
            except psycopg2.Error as e:
                logger.error(f"  ➜ 查询 person_identity_map 时出错 ({column}={value}): {e}")
        return None

    def upsert_person(self, cursor: psycopg2.extensions.cursor, person_data: Dict[str, Any]) -> Tuple[int, str]:
        """
        【V8 - 终极合并重构版】
        处理演员身份数据的唯一、权威入口。
        能够智能处理所有ID的冲突，执行查找、合并、更新或插入操作。
        """
        # 1. 清理和准备所有传入的ID
        map_id = person_data.get("map_id")
        primary_name = str(person_data.get("primary_name") or person_data.get("name") or '').strip()
        emby_id = str(person_data.get("emby_person_id") or person_data.get("emby_id") or '').strip() or None
        tmdb_id = str(person_data.get("tmdb_person_id") or person_data.get("tmdb_id") or '').strip() or None
        imdb_id = str(person_data.get("imdb_id") or '').strip() or None
        douban_id = str(person_data.get("douban_celebrity_id") or person_data.get("douban_id") or '').strip() or None

        if not primary_name:
            primary_name = "Unknown Actor"

        # 2. 定义所有可能的查找键
        search_keys = [
            ('map_id', map_id),
            ('tmdb_person_id', tmdb_id),
            ('emby_person_id', emby_id),
            ('douban_celebrity_id', douban_id),
            ('imdb_id', imdb_id)
        ]

        # 3. 查找所有可能匹配的现有记录
        # 使用集合确保每个找到的 map_id 只记录一次
        found_map_ids = set()
        for key, value in search_keys:
            if value:
                try:
                    cursor.execute(f"SELECT map_id FROM person_identity_map WHERE {key} = %s", (value,))
                    result = cursor.fetchone()
                    if result:
                        found_map_ids.add(result['map_id'])
                except psycopg2.Error as e:
                    logger.error(f"  ➜ 在 upsert_person 中查找时出错 ({key}={value}): {e}")
                    # 发生查询错误时，最好回滚并返回错误，避免数据不一致
                    cursor.connection.rollback()
                    return -1, "ERROR"
        
        # 4. 决定操作：合并、更新、或插入
        try:
            if len(found_map_ids) > 1:
                # --- 合并逻辑 ---
                logger.warning(f"  ➜ 发现演员 '{primary_name}' 的多个ID指向了不同的记录 {list(found_map_ids)}，将执行合并。")
                
                # 选择第一个ID作为目标，其他的作为源
                target_map_id = sorted(list(found_map_ids))[0]
                source_map_ids = sorted(list(found_map_ids))[1:]
                
                # 将所有传入的新数据合并到目标记录
                update_data = {
                    'tmdb_person_id': tmdb_id, 'emby_person_id': emby_id,
                    'douban_celebrity_id': douban_id, 'imdb_id': imdb_id
                }
                update_clauses = []
                update_values = []
                for key, value in update_data.items():
                    if value:
                        update_clauses.append(f"{key} = COALESCE({key}, %s)")
                        update_values.append(value)
                
                if update_clauses:
                    cursor.execute(f"UPDATE person_identity_map SET {', '.join(update_clauses)} WHERE map_id = %s", tuple(update_values + [target_map_id]))

                # 删除源记录
                cursor.execute("DELETE FROM person_identity_map WHERE map_id = ANY(%s)", (source_map_ids,))
                logger.info(f"    ➜ 成功将记录 {source_map_ids} 合并到 {target_map_id}。")
                return target_map_id, "MERGED"

            elif len(found_map_ids) == 1:
                # --- 更新逻辑 ---
                target_map_id = found_map_ids.pop()
                update_data = {
                    'primary_name': primary_name, 'tmdb_person_id': tmdb_id, 'emby_person_id': emby_id,
                    'douban_celebrity_id': douban_id, 'imdb_id': imdb_id
                }
                update_clauses = []
                update_values = []
                for key, value in update_data.items():
                    if value:
                        update_clauses.append(f"{key} = COALESCE({key}, %s)")
                        update_values.append(value)
                
                if update_clauses:
                    cursor.execute(f"UPDATE person_identity_map SET {', '.join(update_clauses)} WHERE map_id = %s", tuple(update_values + [target_map_id]))
                    return target_map_id, "UPDATED"
                else:
                    return target_map_id, "NO_CHANGE"

            else:
                # --- 插入逻辑 ---
                insert_data = {
                    'primary_name': primary_name, 'tmdb_person_id': tmdb_id, 'emby_person_id': emby_id,
                    'douban_celebrity_id': douban_id, 'imdb_id': imdb_id
                }
                cols = [k for k, v in insert_data.items() if v is not None]
                vals = [v for k, v in insert_data.items() if v is not None]
                
                if not cols: return -1, "SKIPPED"

                sql = f"INSERT INTO person_identity_map ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(vals))}) RETURNING map_id"
                cursor.execute(sql, tuple(vals))
                result = cursor.fetchone()
                return result['map_id'], "INSERTED"

        except psycopg2.IntegrityError as ie:
            cursor.connection.rollback()
            logger.warning(f"upsert_person 发生罕见的并发冲突 for '{primary_name}', 已回滚: {ie}")
            return -1, "CONFLICT_ERROR"
        except Exception as e:
            cursor.connection.rollback()
            logger.error(f"upsert_person 发生未知异常 for '{primary_name}': {e}", exc_info=True)
            return -1, "UNKNOWN_ERROR"
        
    def update_actor_metadata_from_tmdb(self, cursor: psycopg2.extensions.cursor, tmdb_id: int, tmdb_data: Dict[str, Any]):
        """
        【最终实现版】将从 TMDb API 获取的演员详情数据，更新或插入到 actor_metadata 表中。
        此函数与 init_db() 中定义的表结构完全匹配。
        """
        if not tmdb_id or not tmdb_data:
            return

        try:
            # 从 TMDb 数据中精确提取 actor_metadata 表需要的字段
            metadata = {
                "tmdb_id": tmdb_id,
                "profile_path": tmdb_data.get("profile_path"),
                "gender": tmdb_data.get("gender"),
                "adult": tmdb_data.get("adult", False),
                "popularity": tmdb_data.get("popularity"),
                "original_name": tmdb_data.get("original_name") # 演员的原始（通常是外文）姓名
            }

            # 准备 SQL 语句
            columns = list(metadata.keys())
            columns_str = ', '.join(columns)
            placeholders_str = ', '.join(['%s'] * len(columns))
            
            # ON CONFLICT 语句的核心：当 tmdb_id 冲突时，更新哪些字段
            update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col != "tmdb_id"]
            # 无论如何都更新时间戳
            update_clauses.append("last_updated_at = NOW()")
            update_str = ', '.join(update_clauses)

            sql = f"""
                INSERT INTO actor_metadata ({columns_str}, last_updated_at)
                VALUES ({placeholders_str}, NOW())
                ON CONFLICT (tmdb_id) DO UPDATE SET {update_str}
            """
            
            # 执行
            cursor.execute(sql, tuple(metadata.values()))
            logger.trace(f"  ➜ 成功将演员 (TMDb ID: {tmdb_id}) 的元数据缓存到数据库。")

        except Exception as e:
            logger.error(f"  ➜ 缓存演员 (TMDb ID: {tmdb_id}) 元数据到数据库时失败: {e}", exc_info=True)

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
        logger.error(f"  ➜ 获取所有演员映射Emby ID时失败: {e}", exc_info=True)
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
        logger.error(f"  ➜ 获取演员订阅列表失败: {e}", exc_info=True)
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
            
            logger.info(f"  ➜ 成功添加演员订阅 '{actor_name}'。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except Exception as e:
        logger.error(f"  ➜ 添加演员订阅 '{actor_name}' 时失败: {e}", exc_info=True)
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
            logger.info(f"  ➜ 成功更新订阅ID {subscription_id}。")
            return True
            
    except Exception as e:
        logger.error(f"  ➜ 更新订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

def delete_actor_subscription(subscription_id: int) -> bool:
    """删除一个演员订阅及其所有追踪的媒体。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            conn.commit()
            logger.info(f"  ➜ 成功删除订阅ID {subscription_id}。")
            return True
    except Exception as e:
        logger.error(f"  ➜ 删除订阅 {subscription_id} 失败: {e}", exc_info=True)
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
        logger.error(f"  ➜ 获取已追踪媒体项 {media_id} 失败: {e}", exc_info=True)
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
        logger.error(f"  ➜ 更新已追踪媒体项 {media_id} 状态失败: {e}", exc_info=True)
        raise