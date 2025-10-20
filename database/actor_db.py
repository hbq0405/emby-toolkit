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

    # 核心批量写入函数
    def batch_upsert_actors_and_metadata(self, cursor: psycopg2.extensions.cursor, actors_list: List[Dict[str, Any]], emby_config: Dict[str, Any]) -> Dict[str, int]:
        """
        【管家函数-写】接收一个完整的演员列表，自动将数据分发到
        person_identity_map 和 actor_metadata 两个表中。
        这是所有演员数据写入的唯一入口。
        """
        if not actors_list:
            return {}

        logger.info(f"  ➜ [演员数据管家] 开始批量处理 {len(actors_list)} 位演员的写入任务...")
        stats = {"INSERTED": 0, "UPDATED": 0, "UNCHANGED": 0, "SKIPPED": 0, "ERROR": 0}

        for actor_data in actors_list:
            # 直接调用下面已经很完善的单个演员处理函数
            map_id, action = self.upsert_person(cursor, actor_data, emby_config)
            
            # 累加统计结果
            if action in stats:
                stats[action] += 1
            else:
                stats["ERROR"] += 1
        
        logger.info(f"  ➜ [演员数据管家] 批量写入完成。统计: {stats}")
        return stats

    # 核心批量读取函数
    def get_full_actor_details_by_tmdb_ids(self, cursor: psycopg2.extensions.cursor, tmdb_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        【管家函数-读】根据一组 TMDB ID，从 actor_metadata 表中高效地获取所有演员的详细信息。
        返回一个以 TMDB ID 为键，演员信息字典为值的映射。
        """
        if not tmdb_ids:
            return {}

        logger.debug(f"  ➜ [演员数据管家] 正在批量查询 {len(tmdb_ids)} 位演员的详细元数据...")
        
        try:
            # 使用 ANY(%s) 是 PostgreSQL 中处理列表参数的高效方式
            sql = "SELECT * FROM actor_metadata WHERE tmdb_id = ANY(%s)"
            cursor.execute(sql, (tmdb_ids,))
            
            results = cursor.fetchall()
            
            # 将查询结果处理成一个 {tmdb_id: {actor_details}} 的字典，方便上层调用
            actor_details_map = {row['tmdb_id']: dict(row) for row in results}
            
            logger.debug(f"  ➜ [演员数据管家] 成功从数据库中找到了 {len(actor_details_map)} 条匹配的演员元数据。")
            return actor_details_map

        except Exception as e:
            logger.error(f"  ➜ [演员数据管家] 批量查询演员元数据时失败: {e}", exc_info=True)
            return {}

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
    
    def enrich_actors_with_provider_ids(self, cursor: psycopg2.extensions.cursor, raw_emby_actors: List[Dict[str, Any]], emby_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        接收一个来自 Emby 的原始演员列表，
        高效地为他们补充 ProviderIds。
        策略：优先从本地数据库批量查询，对未找到的演员再通过 Emby API 补漏。
        """
        if not raw_emby_actors:
            return []

        logger.info(f"  ➜ [演员数据管家] 开始为 {len(raw_emby_actors)} 位演员丰富外部ID...")
        
        # 准备一个最终结果的映射，用 emby_id 作为 key
        enriched_actors_map = {actor['Id']: actor.copy() for actor in raw_emby_actors}
        
        # --- 阶段一：从本地数据库批量获取数据 ---
        emby_ids_to_check = list(enriched_actors_map.keys())
        ids_found_in_db = set()
        
        try:
            if emby_ids_to_check:
                # 使用 ANY(%s) 进行高效的批量查询
                sql = "SELECT emby_person_id, tmdb_person_id, imdb_id, douban_celebrity_id FROM person_identity_map WHERE emby_person_id = ANY(%s)"
                cursor.execute(sql, (emby_ids_to_check,))
                db_results = cursor.fetchall()

                for row in db_results:
                    emby_id = row["emby_person_id"]
                    ids_found_in_db.add(emby_id)
                    
                    # 构建 ProviderIds 字典并注入回结果
                    provider_ids = {}
                    if row.get("tmdb_person_id"):
                        provider_ids["Tmdb"] = str(row.get("tmdb_person_id"))
                    if row.get("imdb_id"):
                        provider_ids["Imdb"] = row.get("imdb_id")
                    if row.get("douban_celebrity_id"):
                        provider_ids["Douban"] = str(row.get("douban_celebrity_id"))
                    
                    if emby_id in enriched_actors_map:
                        enriched_actors_map[emby_id]["ProviderIds"] = provider_ids
                
                logger.info(f"  ➜ [演员数据管家] 从数据库缓存中找到了 {len(ids_found_in_db)} 位演员的外部ID。")
        except Exception as e:
            logger.error(f"  ➜ [演员数据管家] 批量查询演员外部ID时失败: {e}", exc_info=True)

        # --- 阶段二：为未找到的演员实时查询 Emby API ---
        ids_to_fetch_from_api = [pid for pid in emby_ids_to_check if pid not in ids_found_in_db]

        if ids_to_fetch_from_api:
            logger.info(f"  ➜ [演员数据管家] 将通过 Emby API 为剩余 {len(ids_to_fetch_from_api)} 位演员获取外部ID...")
            
            for person_id in ids_to_fetch_from_api:
                person_details = get_emby_item_details(
                    item_id=person_id, 
                    emby_server_url=emby_config['url'], 
                    emby_api_key=emby_config['api_key'], 
                    user_id=emby_config['user_id'],
                    fields="ProviderIds" # 我们只需要这一个字段
                )
                
                if person_details and person_details.get("ProviderIds"):
                    if person_id in enriched_actors_map:
                        enriched_actors_map[person_id]["ProviderIds"] = person_details.get("ProviderIds")

        # --- 阶段三：返回最终的列表 ---
        return list(enriched_actors_map.values())
    
    def rehydrate_slim_actors(self, cursor: psycopg2.extensions.cursor, slim_actors_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        接收一个简单的演员关系列表，
        从数据库中查询完整的演员信息，将其恢复成一个完整的演员列表。
        """
        if not slim_actors_list:
            return []

        logger.debug(f"  ➜ [演员数据管家-恢复] 开始为 {len(slim_actors_list)} 位演员从缓存恢复完整元数据...")
        
        # 1. 提取所有需要查询的 TMDB ID
        tmdb_ids_to_fetch = [actor['tmdb_id'] for actor in slim_actors_list if 'tmdb_id' in actor]
        if not tmdb_ids_to_fetch:
            return []

        # 2. 一次性批量查询所有演员的完整信息
        #    我们 JOIN 两张表，把所有需要的信息都拿出来
        sql = """
            SELECT
                pim.primary_name AS name,
                pim.emby_person_id,
                pim.imdb_id,
                pim.douban_celebrity_id AS douban_id,
                am.* 
            FROM
                person_identity_map pim
            JOIN
                actor_metadata am ON pim.tmdb_person_id = am.tmdb_id
            WHERE
                am.tmdb_id = ANY(%s);
        """
        cursor.execute(sql, (tmdb_ids_to_fetch,))
        full_details_rows = cursor.fetchall()
        
        # 3. 将查询结果处理成一个 {tmdb_id: {full_details}} 的字典，方便快速查找
        details_map = {row['tmdb_id']: dict(row) for row in full_details_rows}
        
        # 4. 遍历原始的“脱水”列表，进行“复水”合并
        rehydrated_list = []
        for slim_actor in slim_actors_list:
            tmdb_id = slim_actor.get('tmdb_id')
            if tmdb_id in details_map:
                # 从数据库查到的完整信息
                full_details = details_map[tmdb_id]
                
                # 合并！
                # 用 full_details 做基础，因为它包含了大部分信息
                # 然后用 slim_actor 里的 character 和 order 覆盖/补充，因为这是关系特有的
                hydrated_actor = {**full_details, **slim_actor}
                
                # 兼容一下主流程里常用的 'id' 键
                hydrated_actor['id'] = tmdb_id
                
                rehydrated_list.append(hydrated_actor)
            else:
                # 如果因为某些原因在数据库里没找到，至少保留基本信息
                rehydrated_list.append(slim_actor)
                
        # 按照原始的 order 排序
        rehydrated_list.sort(key=lambda x: x.get('order', 999))
        
        logger.debug(f"  ➜ [演员数据管家-恢复] 成功恢复 {len(rehydrated_list)} 位演员的元数据。")
        return rehydrated_list

    def upsert_person(self, cursor: psycopg2.extensions.cursor, person_data: Dict[str, Any], emby_config: Dict[str, Any]) -> Tuple[int, str]:
        """
        【V8 - 精准统计修复版】
        通过为 ON CONFLICT DO UPDATE 增加 WHERE 条件，实现真正的条件更新。
        这能准确区分数据实际被“更新”和数据因无变化而“未变”的情况，从而解决统计不准的问题。
        """
        emby_id = str(person_data.get("emby_id") or '').strip() or None
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

        if not name and emby_id:
            details = get_emby_item_details(emby_id, emby_config['url'], emby_config['api_key'], emby_config['user_id'], fields="Name")
            name = details.get("Name") if details else "Unknown Actor"
        elif not name:
            name = "Unknown Actor"

        try:
            sql = """
                INSERT INTO person_identity_map 
                (primary_name, emby_person_id, tmdb_person_id, imdb_id, douban_celebrity_id, last_updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (tmdb_person_id) DO UPDATE SET
                    -- 名字总是更新为最新的
                    primary_name = EXCLUDED.primary_name,
                    
                    -- ID字段：优先使用新传入的非空值，否则保留数据库中已有的值
                    emby_person_id = COALESCE(EXCLUDED.emby_person_id, person_identity_map.emby_person_id),
                    imdb_id = COALESCE(EXCLUDED.imdb_id, person_identity_map.imdb_id),
                    douban_celebrity_id = COALESCE(EXCLUDED.douban_celebrity_id, person_identity_map.douban_celebrity_id),
                    
                    last_updated_at = NOW()
                WHERE
                    -- 使用 IS DISTINCT FROM 来正确处理 NULL 值，确保只有在数据实际变化时才更新
                    person_identity_map.primary_name IS DISTINCT FROM EXCLUDED.primary_name OR
                    person_identity_map.emby_person_id IS DISTINCT FROM COALESCE(EXCLUDED.emby_person_id, person_identity_map.emby_person_id) OR
                    person_identity_map.imdb_id IS DISTINCT FROM COALESCE(EXCLUDED.imdb_id, person_identity_map.imdb_id) OR
                    person_identity_map.douban_celebrity_id IS DISTINCT FROM COALESCE(EXCLUDED.douban_celebrity_id, person_identity_map.douban_celebrity_id)
                RETURNING map_id, (CASE xmax WHEN 0 THEN 'INSERTED' ELSE 'UPDATED' END) as action;
            """
            
            cursor.execute(sql, (name, emby_id, tmdb_id, imdb_id, douban_id))
            result = cursor.fetchone()

            action: str
            map_id: int

            if result:
                # 如果有返回结果，说明发生了 INSERT 或 UPDATE
                map_id = result['map_id']
                action = result['action']
                logger.debug(f"  ├─ 演员 '{name}' (TMDb: {tmdb_id}) 处理完成。结果: {action} (map_id: {map_id})")
            else:
                # 如果没有返回结果，说明存在冲突但 WHERE 条件不满足，数据未发生变化
                action = "UNCHANGED"
                # 需要手动查询一下 map_id，以便后续流程使用
                cursor.execute("SELECT map_id FROM person_identity_map WHERE tmdb_person_id = %s", (tmdb_id,))
                existing_record = cursor.fetchone()
                if not existing_record:
                    logger.error(f"upsert_person 逻辑错误: 未能更新也未能找到现有演员记录 for tmdb_id={tmdb_id}")
                    return -1, "ERROR"
                map_id = existing_record['map_id']
                logger.trace(f"  ➜ 演员 '{name}' (TMDb: {tmdb_id}) 数据无变化，标记为 UNCHANGED。")

            # 统一处理元数据更新
            if 'profile_path' in person_data or 'gender' in person_data or 'popularity' in person_data:
                self.update_actor_metadata_from_tmdb(cursor, tmdb_id, person_data)

            return map_id, action

        except psycopg2.IntegrityError as ie:
            conn = cursor.connection
            conn.rollback()
            logger.error(f"upsert_person 发生数据库完整性冲突，可能是 emby_id 或其他唯一键重复。emby_id={emby_id}, tmdb_id={tmdb_id}: {ie}")
            return -1, "ERROR"
        except Exception as e:
            conn = cursor.connection
            conn.rollback()
            logger.error(f"upsert_person 发生未知异常，emby_person_id={emby_id}: {e}", exc_info=True)
            return -1, "ERROR"
        
    def disassociate_emby_ids(self, cursor: psycopg2.extensions.cursor, emby_ids: set) -> int:
        """
        将一组给定的 emby_person_id 在数据库中设为 NULL。
        这用于清理那些在 Emby 中已被删除的演员的关联关系。

        :param cursor: 数据库游标。
        :param emby_ids: 需要被清理的 Emby Person ID 集合。
        :return: 成功更新的行数。
        """
        if not emby_ids:
            return 0
        
        try:
            # 使用元组(tuple)作为IN子句的参数
            sql = """
                UPDATE person_identity_map 
                SET emby_person_id = NULL, last_updated_at = NOW() 
                WHERE emby_person_id IN %s
            """
            cursor.execute(sql, (tuple(emby_ids),))
            updated_rows = cursor.rowcount
            logger.info(f"  ➜ 数据库操作：成功将 {updated_rows} 个演员的 emby_id 置为 NULL。")
            return updated_rows
        except Exception as e:
            logger.error(f"  ➜ 批量清理 Emby ID 关联时失败: {e}", exc_info=True)
            # 即使失败也应该抛出异常，让上层事务回滚
            raise
        
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