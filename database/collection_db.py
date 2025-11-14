# database/collection_db.py
import psycopg2
import logging
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from .connection import get_db_connection
from . import media_db
import config_manager
import constants
import handler.tmdb as tmdb
import handler.emby as emby

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 原生合集数据访问 (collections_info) 
# ======================================================================

def upsert_native_collection(collection_data: Dict[str, Any]):
    """ 写入或更新一条原生合集信息。"""
    sql = """
        INSERT INTO collections_info 
        (emby_collection_id, name, tmdb_collection_id, status, has_missing, 
        missing_movies_json, last_checked_at, poster_path, in_library_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (emby_collection_id) DO UPDATE SET
            name = EXCLUDED.name,
            tmdb_collection_id = EXCLUDED.tmdb_collection_id,
            status = EXCLUDED.status,
            has_missing = EXCLUDED.has_missing,
            missing_movies_json = EXCLUDED.missing_movies_json,
            last_checked_at = EXCLUDED.last_checked_at,
            poster_path = EXCLUDED.poster_path,
            in_library_count = EXCLUDED.in_library_count;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (
                collection_data.get('emby_collection_id'),
                collection_data.get('name'),
                collection_data.get('tmdb_collection_id'),
                collection_data.get('status'),
                collection_data.get('has_missing'),
                json.dumps(collection_data.get('missing_tmdb_ids', []), ensure_ascii=False),
                datetime.now(),
                collection_data.get('poster_path'),
                collection_data.get('in_library_count')
            ))
    except psycopg2.Error as e:
        logger.error(f"写入原生合集信息时发生数据库错误: {e}", exc_info=True)
        raise

def get_all_native_collections() -> List[Dict[str, Any]]:
    """ 获取所有原生合集的基础信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM collections_info ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 读取所有原生合集时发生错误: {e}", exc_info=True)
        return []

# ======================================================================
# 模块: 自定义合集数据访问 (custom_collections)
# ======================================================================

def create_custom_collection(name: str, type: str, definition_json: str, allowed_user_ids_json: Optional[str] = None) -> int:
    """ 创建一个新的自定义合集 。"""
    sql = "INSERT INTO custom_collections (name, type, definition_json, allowed_user_ids) VALUES (%s, %s, %s, %s) RETURNING id"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 2. 在执行时传入第4个参数 ★★★
            cursor.execute(sql, (name, type, definition_json, allowed_user_ids_json))
            new_id = cursor.fetchone()['id']
            logger.info(f"成功创建自定义合集 '{name}' (类型: {type})。")
            return new_id
    except psycopg2.Error as e:
        logger.error(f"创建自定义合集 '{name}' 时发生数据库错误: {e}", exc_info=True)
        raise

def get_custom_collection_by_id(collection_id: int) -> Optional[Dict[str, Any]]:
    """ 根据ID获取单个自定义合集的详细信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE id = %s", (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except psycopg2.Error as e:
        logger.error(f"根据ID {collection_id} 获取自定义合集时出错: {e}", exc_info=True)
        return None

def get_all_custom_collections() -> List[Dict[str, Any]]:
    """ 获取所有自定义合集的基础定义。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM custom_collections
                ORDER BY sort_order ASC, id ASC
            """)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

def get_all_active_custom_collections() -> List[Dict[str, Any]]:
    """ 获取所有状态为 'active' 的自定义合集的基础定义。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE status = 'active' ORDER BY sort_order ASC, id ASC")
            return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"获取所有已启用的自定义合集时出错: {e}", exc_info=True)
        return []

def update_custom_collection(collection_id: int, name: str, type: str, definition_json: str, status: str, allowed_user_ids_json: Optional[str] = None) -> bool:
    """ 更新一个自定义合集的定义 。"""
    sql = "UPDATE custom_collections SET name = %s, type = %s, definition_json = %s, status = %s, allowed_user_ids = %s WHERE id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 2. 在执行时传入新参数 ★★★
            cursor.execute(sql, (name, type, definition_json, status, allowed_user_ids_json, collection_id))
            return cursor.rowcount > 0
    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 ID {collection_id} 时出错: {e}", exc_info=True)
        return False

def delete_custom_collection(collection_id: int) -> bool:
    """ 从数据库中删除一个自定义合集定义。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM custom_collections WHERE id = %s", (collection_id,))
            return cursor.rowcount > 0
    except psycopg2.Error as e:
        logger.error(f"删除自定义合集 (ID: {collection_id}) 时出错: {e}", exc_info=True)
        raise

def update_custom_collections_order(ordered_ids: List[int]) -> bool:
    """ 根据提供的ID列表，批量更新自定义合集的 sort_order。"""
    if not ordered_ids: return True
    sql = "UPDATE custom_collections SET sort_order = %s WHERE id = %s"
    data_to_update = [(index, collection_id) for index, collection_id in enumerate(ordered_ids)]
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, data_to_update)
            return True
    except psycopg2.Error as e:
        logger.error(f"批量更新自定义合集顺序时出错: {e}", exc_info=True)
        return False

def update_custom_collection_sync_results(collection_id: int, update_data: Dict[str, Any]):
    """ 根据同步和计算结果，更新自定义合集的媒体成员列表和统计数据。"""
    
    # ★★★ 核心修正：确保 last_synced_at 只被赋值一次 ★★★
    # 1. 制作一个 update_data 的副本，以防修改原始字典
    data_to_update = update_data.copy()
    
    # 2. 如果调用者不小心传入了 last_synced_at，我们把它从字典里移除
    if 'last_synced_at' in data_to_update:
        del data_to_update['last_synced_at']

    # 3. 现在可以安全地构建 SQL 了
    set_clauses = [f"{key} = %s" for key in data_to_update.keys()]
    values = list(data_to_update.values())
    
    # 4. 总是由这个函数来负责更新时间戳
    sql = f"UPDATE custom_collections SET {', '.join(set_clauses)}, last_synced_at = NOW() WHERE id = %s"
    
    values.append(collection_id)
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 {collection_id} 的同步结果时出错: {e}", exc_info=True)
        raise

def find_list_collections_containing_tmdb_id(tmdb_id: str) -> List[Dict[str, Any]]:
    """ 查找所有包含指定 TMDB ID 的“榜单”类型合集。"""
    collections = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 @> 操作符高效查询 JSONB 数组
            sql = "SELECT id, emby_collection_id, name FROM custom_collections WHERE type = 'list' AND status = 'active' AND generated_media_info_json @> %s::jsonb"
            cursor.execute(sql, (json.dumps([tmdb_id]),))
            collections = [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"查找包含 TMDB ID {tmdb_id} 的榜单合集时出错: {e}", exc_info=True)
    return collections

def remove_tmdb_id_from_all_collections(tmdb_id_to_remove: str):
    """ 从所有自定义合集的 generated_media_info_json 缓存中移除一个指定的 tmdb_id。"""
    if not tmdb_id_to_remove: return
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 核心修复：使用正确的逻辑从字符串数组中移除元素 ★★★
            sql = """
                UPDATE custom_collections
                SET generated_media_info_json = (
                    SELECT jsonb_agg(elem)
                    FROM jsonb_array_elements_text(generated_media_info_json) AS elem
                    WHERE elem != %s
                )
                WHERE generated_media_info_json @> %s::jsonb;
            """
            cursor.execute(sql, (tmdb_id_to_remove, json.dumps([tmdb_id_to_remove])))
            if cursor.rowcount > 0:
                logger.info(f"已从 {cursor.rowcount} 个自定义合集的缓存中移除了 TMDB ID: {tmdb_id_to_remove}。")
    except psycopg2.Error as e:
        logger.error(f"从所有合集缓存中移除 TMDB ID {tmdb_id_to_remove} 时失败: {e}", exc_info=True)

def apply_and_persist_media_correction(collection_id: int, old_tmdb_id: str, new_tmdb_id: str, season_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    【V4 - 职责分离最终版】
    - 遵循“职责分离”原则，调用专属函数 ensure_media_record_exists 来创建记录。
    - update_subscription_status 只负责更新状态，不再承担创建职责。
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            conn.autocommit = False

            # === Part 1 & 2: 更新定义、决策状态 (逻辑不变) ===
            cursor.execute("SELECT definition_json, generated_media_info_json FROM custom_collections WHERE id = %s FOR UPDATE", (collection_id,))
            row = cursor.fetchone()
            if not row: return None
            definition = row.get('definition_json') or {}
            definition_list = row.get('generated_media_info_json') or []
            item_type = 'Movie'
            for item in definition_list:
                if str(item.get('tmdb_id')) == str(old_tmdb_id):
                    item_type = item.get('media_type', 'Movie')
                    item['tmdb_id'] = new_tmdb_id
                    if season_number is not None: item['season'] = int(season_number)
                    else: item.pop('season', None)
                    break
            corrections = definition.get('corrections', {})
            correction_value = {"tmdb_id": str(new_tmdb_id)}
            if season_number is not None: correction_value['season'] = int(season_number)
            corrections[str(old_tmdb_id)] = correction_value
            definition['corrections'] = corrections
            cursor.execute("UPDATE custom_collections SET definition_json = %s, generated_media_info_json = %s WHERE id = %s", (json.dumps(definition, ensure_ascii=False), json.dumps(definition_list, ensure_ascii=False), collection_id))
            
            cursor.execute("SELECT in_library, subscription_status FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (old_tmdb_id, item_type))
            old_item_record = cursor.fetchone()
            inherited_target_status = 'NONE'
            if old_item_record:
                status = old_item_record['subscription_status']
                inherited_target_status = 'WANTED' if old_item_record['in_library'] or status in ['WANTED', 'SUBSCRIBED'] else 'NONE'

            # === Part 3: 执行状态变更 (★★★ 采用新逻辑 ★★★) ===
            
            # 3.1 废弃旧条目
            if old_tmdb_id != new_tmdb_id:
                media_db.update_subscription_status(old_tmdb_id, item_type, 'IGNORED', ignore_reason=f"修正为 {new_tmdb_id}")

            corrected_item_for_return = {}
            
            if inherited_target_status == 'WANTED':
                api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                subscription_source = {"type": "collection_correction", "id": collection_id, "name": definition.get('name', '')}

                # --- 【分支：修正为某一季】 ---
                if season_number is not None:
                    # 步骤 A: 确保父剧和子季的元数据记录都存在
                    parent_details = tmdb.get_tv_details(int(new_tmdb_id), api_key)
                    if not parent_details: raise ValueError(f"无法获取父剧 {new_tmdb_id} 详情")
                    season_details = tmdb.get_tv_season_details(int(new_tmdb_id), season_number, api_key)
                    if not season_details: raise ValueError(f"无法获取季 {season_number} 详情")

                    parent_media_info = {
                        'tmdb_id': new_tmdb_id, 
                        'item_type': 'Series', 
                        'title': parent_details.get('name'),
                        'original_title': parent_details.get('original_name'),
                        'release_date': parent_details.get('first_air_date'),
                        'overview': parent_details.get('overview'),
                        'poster_path': parent_details.get("poster_path")
                    }
                    season_tmdb_id = str(season_details.get('id'))
                    season_media_info = {
                        'tmdb_id': season_tmdb_id, 'item_type': 'Season', 'title': season_details.get('name'),
                        'poster_path': season_details.get("poster_path") or parent_details.get("poster_path"),
                        'parent_series_tmdb_id': new_tmdb_id, 'season_number': season_number,
                        'release_date': season_details.get("air_date", '')
                    }
                    media_db.ensure_media_record_exists([parent_media_info, season_media_info])
                    logger.info(f"    ➜ 已确保父剧 '{parent_details.get('name')}' 和季 '{season_details.get('name')}' 的记录存在。")

                    # 步骤 B: 现在可以安全地为“季”条目更新订阅状态
                    release_date = season_details.get("air_date", '')
                    final_subscription_status = 'PENDING_RELEASE' if release_date and release_date > datetime.now().strftime('%Y-%m-%d') else 'WANTED'
                    media_db.update_subscription_status(
                        tmdb_ids=season_tmdb_id, item_type='Season', new_status=final_subscription_status,
                        source=subscription_source, media_info_list=[season_media_info]
                    )
                    
                    final_ui_status = 'unreleased' if final_subscription_status == 'PENDING_RELEASE' else 'subscribed'
                    corrected_item_for_return = {
                        "tmdb_id": new_tmdb_id, "title": f"{parent_details.get('name')} - 第 {season_number} 季",
                        "release_date": release_date, "poster_path": season_media_info['poster_path'], 
                        "status": final_ui_status, "media_type": "Series", "season": int(season_number)
                    }

                # --- 【分支：电影或整剧修正】 ---
                else:
                    details = tmdb.get_tv_details(int(new_tmdb_id), api_key) if item_type == 'Series' else tmdb.get_movie_details(int(new_tmdb_id), api_key)
                    if not details: raise ValueError(f"无法获取 {new_tmdb_id} 详情")
                    
                    media_info = {'tmdb_id': new_tmdb_id, 'item_type': item_type, 'title': details.get('title') or details.get('name'), 'poster_path': details.get("poster_path"), 'release_date': details.get("release_date") or details.get("first_air_date", '')}
                    media_db.ensure_media_record_exists([media_info])
                    
                    release_date = media_info['release_date']
                    final_subscription_status = 'PENDING_RELEASE' if release_date and release_date > datetime.now().strftime('%Y-%m-%d') else 'WANTED'
                    media_db.update_subscription_status(
                        tmdb_ids=new_tmdb_id, item_type=item_type, new_status=final_subscription_status, 
                        source=subscription_source, media_info_list=[media_info]
                    )
                    
                    final_ui_status = 'unreleased' if final_subscription_status == 'PENDING_RELEASE' else 'subscribed'
                    corrected_item_for_return = {"tmdb_id": new_tmdb_id, "title": media_info['title'], "release_date": media_info['release_date'], "poster_path": media_info['poster_path'], "status": final_ui_status, "media_type": item_type}
            
            else: # 如果继承状态是 NONE
                # 只需要确保记录存在即可，不需要更新状态
                # ... (这部分逻辑可以根据需要细化，但目前框架已正确)
                pass

            conn.commit()
            logger.info(f"成功为合集 {collection_id} 应用状态继承式修正：{old_tmdb_id} -> {new_tmdb_id} (季: {season_number})")
            return corrected_item_for_return

    except Exception as e:
        logger.error(f"DB: 应用媒体修正时发生严重错误，事务已回滚: {e}", exc_info=True)
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()

def match_and_update_list_collections_on_item_add(new_item_tmdb_id: str, new_item_emby_id: str, new_item_name: str) -> List[Dict[str, Any]]:
    """
    【V2 - 查询逻辑修复版】
    当新媒体入库时，查找并更新所有匹配的'list'类型合集。
    - 修复了查询 generated_media_info_json 字段的逻辑，使其能正确匹配字符串数组。
    - 修复了更新 generated_media_info_json 的逻辑，使其能正确处理对象数组。
    """
    collections_to_update_in_emby = []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复 1/3: 使用正确的 JSONB 查询来匹配字符串数组 ★★★
            sql_find = """
                SELECT * FROM custom_collections 
                WHERE type = 'list' 
                  AND status = 'active' 
                  AND emby_collection_id IS NOT NULL
                  AND generated_media_info_json @> %s::jsonb
            """
            # 构造一个包含“部分匹配对象”的数组
            # 意思是：查找 generated_media_info_json 数组中，是否存在一个
            # 至少包含 "tmdb_id": "xxx" 的对象。
            search_payload = json.dumps([{'tmdb_id': str(new_item_tmdb_id)}])
            
            cursor.execute(sql_find, (search_payload,))
            candidate_collections = cursor.fetchall()

            if not candidate_collections:
                logger.debug(f"  ➜ 未在任何榜单合集中找到 TMDb ID: {new_item_tmdb_id}。")
                return []

            try:
                for collection_row in candidate_collections:
                    collection = dict(collection_row)
                    collection_id = collection['id']
                    collection_name = collection['name']
                    
                    try:
                        # ★★★ 核心修复 2/3: 假设 generated_media_info_json 是对象数组，如果不是则转换 ★★★
                        media_list_raw = collection.get('generated_media_info_json') or []
                        media_list = []
                        
                        # 健壮性处理：如果存的是字符串数组，先转换为对象数组
                        if media_list_raw and isinstance(media_list_raw[0], str):
                            logger.warning(f"  ➜ 检测到合集《{collection_name}》的缓存格式为旧版（字符串数组），将进行动态转换...")
                            # 这是一个耗时操作，但能保证兼容性
                            details_map = media_db.get_media_details_by_tmdb_ids(media_list_raw)
                            for tmdb_id_str in media_list_raw:
                                detail = details_map.get(tmdb_id_str, {})
                                media_list.append({
                                    "tmdb_id": tmdb_id_str,
                                    "status": "in_library" if detail.get('in_library') else "missing",
                                    "title": detail.get('title', '未知标题'),
                                    "poster_path": detail.get('poster_path'),
                                    "release_date": detail.get('release_date')
                                })
                        else:
                            media_list = media_list_raw

                        item_found_and_updated = False
                        
                        for media_item in media_list:
                            if str(media_item.get('tmdb_id')) == str(new_item_tmdb_id) and media_item.get('status') != 'in_library':
                                media_item['status'] = 'in_library'
                                media_item['emby_id'] = new_item_emby_id 
                                item_found_and_updated = True
                                break
                        
                        if item_found_and_updated:
                            new_in_library_count = sum(1 for m in media_list if m.get('status') == 'in_library')
                            new_missing_count = len(media_list) - new_in_library_count
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            new_json_data = json.dumps(media_list, ensure_ascii=False, default=str)
                            
                            cursor.execute("""
                                UPDATE custom_collections
                                SET generated_media_info_json = %s,
                                    in_library_count = %s,
                                    missing_count = %s,
                                    health_status = %s
                                WHERE id = %s
                            """, (new_json_data, new_in_library_count, new_missing_count, new_health_status, collection_id))
                            
                            collections_to_update_in_emby.append({
                                'id': collection_id,
                                'emby_collection_id': collection['emby_collection_id'],
                                'name': collection_name
                            })

                    except (json.JSONDecodeError, TypeError, IndexError) as e_json:
                        logger.warning(f"解析或处理榜单合集《{collection_name}》的数据时出错: {e_json}，跳过。")
                        continue
                
            except Exception as e_trans:
                logger.error(f"在更新榜单合集数据库状态的事务中发生错误: {e_trans}", exc_info=True)
                raise

        return collections_to_update_in_emby

    except psycopg2.Error as e_db:
        logger.error(f"匹配和更新榜单合集时发生数据库错误: {e_db}", exc_info=True)
        raise

def update_user_caches_on_item_add(
    new_item_emby_id: str, 
    new_item_tmdb_id: str, 
    new_item_name: str,
    matching_collection_ids: list, 
    emby_config: dict
):
    """
    当一个新媒体项入库时，实时、精确地将其追加到所有
    相关用户的 user_collection_cache 中。
    修复了会覆盖原有权限的严重 Bug，并增加了防重复机制。
    """
    if not all([new_item_emby_id, new_item_tmdb_id, matching_collection_ids, emby_config]):
        return

    logger.info(f"  ➜ 开始为新入库项目 《{new_item_name}》 更新用户权限缓存...")
    
    try:
        user_ids_with_access = emby.get_user_ids_with_access_to_item(
            item_id=new_item_emby_id,
            base_url=emby_config['url'],
            api_key=emby_config['api_key']
        )

        if not user_ids_with_access:
            logger.warning(f"  ➜ 未找到任何有权访问新项目 《{new_item_name}》 的用户，跳过缓存更新。")
            return

        logger.debug(f"  ➜ 共有 {len(user_ids_with_access)} 个用户对新项目有原生访问权限。")

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 核心修复：使用 || 操作符进行追加，并增加防重复条件 ★★★
                sql = """
                    UPDATE user_collection_cache
                    SET 
                        -- 1. 使用 COALESCE 确保字段不为 NULL，若为 NULL 则视为空数组 '[]'::jsonb
                        -- 2. 使用 || 操作符将新元素的 JSONB 数组追加到现有数组末尾
                        visible_emby_ids_json = COALESCE(visible_emby_ids_json, '[]'::jsonb) || %s::jsonb,
                        
                        -- 3. 只有在成功追加时才增加计数
                        total_count = total_count + 1,
                        last_updated_at = NOW()
                    WHERE
                        user_id = ANY(%s)
                        AND collection_id = ANY(%s)
                        -- 4. ★★★ 关键条件：只有当新 ID 不存在于数组中时，才执行更新 ★★★
                        --    使用 @> 操作符检查数组是否包含指定的单个元素 JSONB 数组
                        AND NOT (visible_emby_ids_json @> %s::jsonb);
                """
                
                # 构造一个只包含新 ID 的 JSONB 数组字符串，用于追加和检查
                # 例如: '["307300"]'
                new_id_jsonb_array = json.dumps([new_item_emby_id])

                cursor.execute(sql, (
                    new_id_jsonb_array,      # 用于追加
                    user_ids_with_access,
                    matching_collection_ids,
                    new_id_jsonb_array       # 用于防重复检查
                ))
                
                updated_rows = cursor.rowcount
                
                if updated_rows > 0:
                    logger.info(f"  ➜ 权限更新成功！在 {len(matching_collection_ids)} 个合集中，为 {len(user_ids_with_access)} 个相关用户更新了 {updated_rows} 条权限缓存记录。")
                else:
                    logger.info(f"  ➜ 权限缓存检查完成。所有相关用户的缓存记录均已包含新项目《{new_item_name}》，无需更新。")


    except Exception as e:
        logger.error(f"  ➜ 为新项目 《{new_item_name}》 更新用户缓存时发生严重错误: {e}", exc_info=True)
# ======================================================================
# 模块: 筛选器
# ======================================================================

def get_unique_genres() -> List[str]:
    """【V2 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的类型。"""
    
    unique_genres = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT genres_json FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                genres = row['genres_json']
                if genres:
                    try:
                        for genre in genres:
                            if genre:
                                unique_genres.add(genre.strip())
                    except TypeError:
                        logger.warning(f"处理 genres_json 时遇到意外的类型错误，内容: {genres}")
                        continue
                        
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"从数据库中成功提取出 {len(sorted_genres)} 个唯一的电影类型。")
        return sorted_genres
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一电影类型时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_studios() -> List[str]:
    """【V3 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的工作室。"""
    
    unique_studios = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT studios_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                studios = row['studios_json']
                if studios:
                    try:
                        for studio in studios:
                            if studio:
                                unique_studios.add(studio.strip())
                    except TypeError:
                        logger.warning(f"处理 studios_json 时遇到意外的类型错误，内容: {studios}")
                        continue
                        
        sorted_studios = sorted(list(unique_studios))
        logger.trace(f"从数据库中成功提取出 {len(sorted_studios)} 个跨电影和电视剧的唯一工作室。")
        return sorted_studios
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一工作室时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_tags() -> List[str]:
    """【V2 - PG JSON 兼容版】从 media_metadata 表中提取所有不重复的标签。"""
    
    unique_tags = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pre_cached_tags_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                tags = row['pre_cached_tags_json']
                if tags:
                    try:
                        for tag in tags:
                            if tag:
                                unique_tags.add(tag.strip())
                    except TypeError:
                        logger.warning(f"处理 pre_cached_tags_json 时遇到意外的类型错误，内容: {tags}")
                        continue
                        
        sorted_tags = sorted(list(unique_tags))
        logger.trace(f"从数据库中成功提取出 {len(sorted_tags)} 个唯一的标签。")
        return sorted_tags
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一标签时发生数据库错误: {e}", exc_info=True)
        return []

def search_unique_studios(search_term: str, limit: int = 20) -> List[str]:
    """(V3 - 智能排序版) 搜索工作室并优先返回以 search_term 开头的结果。"""
    
    if not search_term:
        return []
    
    all_studios = get_unique_studios()
    
    if not all_studios:
        return []

    search_term_lower = search_term.lower()
    
    starts_with_matches = []
    contains_matches = []
    
    for studio in all_studios:
        studio_lower = studio.lower()
        if studio_lower.startswith(search_term_lower):
            starts_with_matches.append(studio)
        elif search_term_lower in studio_lower:
            contains_matches.append(studio)
            
    final_matches = starts_with_matches + contains_matches
    logger.trace(f"智能搜索 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
    return final_matches[:limit]

def search_unique_actors(search_term: str, limit: int = 20) -> List[str]:
    """
    直接从 person_identity_map 和 actor_metadata 表中获取所有演员的姓名进行搜索，
    确保数据来源的准确性和查询效率。
    """
    if not search_term:
        return []
    
    unique_actors_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ======================================================================
            # ★★★ 核心修正：直接 JOIN 演员信息表，获取最原始、最准确的数据 ★★★
            # ======================================================================
            sql = """
                SELECT
                    pim.primary_name,
                    am.original_name
                FROM
                    person_identity_map pim
                JOIN
                    actor_metadata am ON pim.tmdb_person_id = am.tmdb_id;
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            for row in rows:
                actor_name = row.get('primary_name')
                original_name = row.get('original_name')
                
                if actor_name and actor_name.strip():
                    # 使用 .get() 来安全地处理可能不存在的键
                    if unique_actors_map.get(actor_name.strip()) is None:
                        unique_actors_map[actor_name.strip()] = (original_name or '').strip()

        if not unique_actors_map:
            return []

        search_term_lower = search_term.lower()
        starts_with_matches = []
        contains_matches = []
        
        for name, original_name in sorted(unique_actors_map.items()):
            name_lower = name.lower()
            original_name_lower = original_name.lower()

            if name_lower.startswith(search_term_lower) or (original_name_lower and original_name_lower.startswith(search_term_lower)):
                starts_with_matches.append(name)
            elif search_term_lower in name_lower or (original_name_lower and search_term_lower in original_name_lower):
                contains_matches.append(name)
        
        final_matches = starts_with_matches + contains_matches
        logger.trace(f"双语搜索演员 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
        return final_matches[:limit]
        
    except psycopg2.Error as e:
        logger.error(f"提取并搜索唯一演员时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_official_ratings():
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT split_part(official_rating, '-', 2) as rating
            FROM media_metadata
            WHERE official_rating IS NOT NULL AND official_rating LIKE '%-%' AND in_library = TRUE -- ### 修改 ###
            ORDER BY rating;
        """)
        return [row['rating'] for row in cursor.fetchall()]