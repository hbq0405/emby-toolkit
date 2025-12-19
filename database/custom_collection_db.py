# database/custom_collection_db.py
import psycopg2
import logging
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from .connection import get_db_connection
from . import media_db, request_db
import config_manager
import constants
import handler.tmdb as tmdb
import handler.emby as emby

logger = logging.getLogger(__name__)

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
    
    # 1. 制作一个 update_data 的副本
    data_to_update = update_data.copy()
    
    # 2. 移除不需要写入数据库的动态计算字段
    # ★★★ 核心修改：不再持久化存储缺失数量和健康状态，改为读取时动态计算 ★★★
    keys_to_remove = ['last_synced_at', 'missing_count', 'health_status']
    for key in keys_to_remove:
        if key in data_to_update:
            del data_to_update[key]

    # 3. 构建 SQL
    if not data_to_update:
        # 如果没有要更新的字段（例如只传了被移除的字段），仅更新时间戳
        sql = "UPDATE custom_collections SET last_synced_at = NOW() WHERE id = %s"
        values = [collection_id]
    else:
        set_clauses = [f"{key} = %s" for key in data_to_update.keys()]
        values = list(data_to_update.values())
        sql = f"UPDATE custom_collections SET {', '.join(set_clauses)}, last_synced_at = NOW() WHERE id = %s"
        values.append(collection_id)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 {collection_id} 的同步结果时出错: {e}", exc_info=True)
        raise

def apply_and_persist_media_correction(collection_id: int, old_tmdb_id: Optional[str], new_tmdb_id: str, season_number: Optional[int] = None, old_title: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    支持通过 TMDb ID 或 标题 定位并修正媒体项。
    包含完整的元数据获取和状态更新逻辑。
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            conn.autocommit = False

            # === Part 1: 锁定并读取记录 ===
            cursor.execute("SELECT definition_json, generated_media_info_json FROM custom_collections WHERE id = %s FOR UPDATE", (collection_id,))
            row = cursor.fetchone()
            if not row: return None
            
            definition = row.get('definition_json') or {}
            definition_list = row.get('generated_media_info_json') or []
            
            target_item = None
            
            # === Part 2: 在缓存列表中查找目标项目 (支持 ID 或 标题) ===
            for item in definition_list:
                # 1. 优先尝试 ID 匹配 (如果提供了 old_tmdb_id)
                if old_tmdb_id and str(item.get('tmdb_id')) == str(old_tmdb_id):
                    target_item = item
                    break
                
                # 2. 如果 ID 没匹配上（或没提供），尝试 标题 匹配
                # 注意：未识别项目的 tmdb_id 通常为 None 或 "None"
                current_id = str(item.get('tmdb_id')) if item.get('tmdb_id') else 'None'
                if not old_tmdb_id and old_title:
                    # 只有当当前项没有有效ID，且标题匹配时才算数
                    if current_id.lower() == 'none' and item.get('title') == old_title:
                        target_item = item
                        break
            
            if not target_item:
                logger.warning(f"  ➜ 修正失败：在合集 {collection_id} 中未找到 ID={old_tmdb_id} 或 Title={old_title} 的项目。")
                return None

            # 获取旧的媒体类型，用于后续逻辑
            item_type = target_item.get('media_type', 'Movie')

            # === Part 3: 更新内存中的项目数据 ===
            target_item['tmdb_id'] = new_tmdb_id
            if season_number is not None: 
                target_item['season'] = int(season_number)
            else: 
                target_item.pop('season', None)

            # === Part 4: 更新修正规则 (Corrections) ===
            corrections = definition.get('corrections', {})
            
            # 构造修正后的值
            correction_value = {"tmdb_id": str(new_tmdb_id)}
            if season_number is not None: 
                correction_value['season'] = int(season_number)
            
            # 构造修正规则的 Key
            if old_tmdb_id:
                # 传统方式：Key 是旧 ID
                correction_key = str(old_tmdb_id)
            else:
                # 新方式：Key 是 "title:原始标题"
                correction_key = f"title:{old_title}"
            
            corrections[correction_key] = correction_value
            definition['corrections'] = corrections

            # === Part 5: 写回数据库 ===
            cursor.execute(
                "UPDATE custom_collections SET definition_json = %s, generated_media_info_json = %s WHERE id = %s", 
                (json.dumps(definition, ensure_ascii=False), json.dumps(definition_list, ensure_ascii=False), collection_id)
            )
            
            # === Part 6: 状态继承与新媒体入库 (核心逻辑) ===
            
            # 6.1 如果有旧 ID，且旧 ID 不等于新 ID，将旧 ID 设为忽略
            if old_tmdb_id and old_tmdb_id != new_tmdb_id:
                 request_db.set_media_status_ignored(tmdb_ids=[old_tmdb_id], item_type=item_type, ignore_reason=f"修正为 {new_tmdb_id}")

            # 6.2 准备 API Key 和 来源信息
            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            subscription_source = {"type": "collection_correction", "id": collection_id, "name": definition.get('name', '')}
            
            corrected_item_for_return = {}

            # 6.3 处理新 ID 的入库和状态更新
            # --- 【分支 A：修正为某一季】 ---
            if season_number is not None:
                # A1. 获取父剧详情
                parent_details = tmdb.get_tv_details(int(new_tmdb_id), api_key)
                if not parent_details: raise ValueError(f"无法获取父剧 {new_tmdb_id} 详情")
                
                # A2. 获取季详情
                season_details = tmdb.get_tv_season_details(int(new_tmdb_id), season_number, api_key)
                if not season_details: raise ValueError(f"无法获取季 {season_number} 详情")

                # A3. 构造元数据对象
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
                    'tmdb_id': season_tmdb_id, 
                    'item_type': 'Season', 
                    'title': season_details.get('name'),
                    'poster_path': season_details.get("poster_path") or parent_details.get("poster_path"),
                    'parent_series_tmdb_id': new_tmdb_id, 
                    'season_number': season_number,
                    'release_date': season_details.get("air_date", '')
                }
                
                # A4. 确保记录存在
                media_db.ensure_media_record_exists([parent_media_info, season_media_info])

                # A5. 更新订阅状态
                release_date = season_details.get("air_date", '')
                final_subscription_status = 'PENDING_RELEASE' if release_date and release_date > datetime.now().strftime('%Y-%m-%d') else 'WANTED'
                
                if final_subscription_status == 'PENDING_RELEASE':
                    request_db.set_media_status_pending_release(
                        tmdb_ids=[season_tmdb_id], item_type='Season',
                        source=subscription_source, media_info_list=[season_media_info]
                    )
                else:
                    request_db.set_media_status_wanted(
                        tmdb_ids=[season_tmdb_id], item_type='Season',
                        source=subscription_source, media_info_list=[season_media_info]
                    )
                
                final_ui_status = 'unreleased' if final_subscription_status == 'PENDING_RELEASE' else 'subscribed'
                corrected_item_for_return = {
                    "tmdb_id": new_tmdb_id, 
                    "title": f"{parent_details.get('name')} - 第 {season_number} 季",
                    "release_date": release_date, 
                    "poster_path": season_media_info['poster_path'], 
                    "status": final_ui_status, 
                    "media_type": "Series", 
                    "season": int(season_number)
                }

            # --- 【分支 B：电影或整剧修正】 ---
            else:
                # B1. 获取详情
                details = tmdb.get_tv_details(int(new_tmdb_id), api_key) if item_type == 'Series' else tmdb.get_movie_details(int(new_tmdb_id), api_key)
                if not details: raise ValueError(f"无法获取 {new_tmdb_id} 详情")
                
                # B2. 构造元数据
                media_info = {
                    'tmdb_id': new_tmdb_id, 
                    'item_type': item_type, 
                    'title': details.get('title') or details.get('name'), 
                    'poster_path': details.get("poster_path"), 
                    'release_date': details.get("release_date") or details.get("first_air_date", '')
                }
                
                # B3. 确保记录存在
                media_db.ensure_media_record_exists([media_info])
                
                # B4. 更新订阅状态
                release_date = media_info['release_date']
                final_subscription_status = 'PENDING_RELEASE' if release_date and release_date > datetime.now().strftime('%Y-%m-%d') else 'WANTED'
                
                if final_subscription_status == 'PENDING_RELEASE':
                    request_db.set_media_status_pending_release(
                        tmdb_ids=[new_tmdb_id], item_type=item_type,
                        source=subscription_source, media_info_list=[media_info]
                    )
                else:
                    request_db.set_media_status_wanted(
                        tmdb_ids=[new_tmdb_id], item_type=item_type,
                        source=subscription_source, media_info_list=[media_info]
                    )
                
                final_ui_status = 'unreleased' if final_subscription_status == 'PENDING_RELEASE' else 'subscribed'
                corrected_item_for_return = {
                    "tmdb_id": new_tmdb_id, 
                    "title": media_info['title'], 
                    "release_date": media_info['release_date'], 
                    "poster_path": media_info['poster_path'], 
                    "status": final_ui_status, 
                    "media_type": item_type
                }

            conn.commit()
            logger.info(f"  ➜ 成功为合集 {collection_id} 应用修正：Key='{correction_key}' -> {new_tmdb_id} (季: {season_number})")
            return corrected_item_for_return

    except Exception as e:
        logger.error(f"  ➜ 应用媒体修正时发生严重错误，事务已回滚: {e}", exc_info=True)
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()

# ======================================================================
# 模块: 筛选器
# ======================================================================

def get_movie_genres() -> List[str]:
    """从 media_metadata 表中提取电影所有不重复的类型。"""
    
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
                        logger.warning(f"  ➜ 处理 genres_json 时遇到意外的类型错误，内容: {genres}")
                        continue
                        
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"  ➜ 从数据库中成功提取出 {len(sorted_genres)} 个唯一的电影类型。")
        return sorted_genres
        
    except psycopg2.Error as e:
        logger.error(f"  ➜ 提取唯一电影类型时发生数据库错误: {e}", exc_info=True)
        return []

def get_tv_genres() -> List[str]:
    """从 media_metadata 表中提取电视剧所有不重复的类型。"""
    
    unique_genres = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT genres_json FROM media_metadata WHERE item_type = 'Series' AND in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                genres = row['genres_json']
                if genres:
                    try:
                        for genre in genres:
                            if genre:
                                unique_genres.add(genre.strip())
                    except TypeError:
                        logger.warning(f"  ➜ 处理 genres_json 时遇到意外的类型错误，内容: {genres}")
                        continue
                        
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"  ➜ 从数据库中成功提取出 {len(sorted_genres)} 个唯一的电视剧类型。")
        return sorted_genres
        
    except psycopg2.Error as e:
        logger.error(f"  ➜ 提取唯一电视剧类型时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_studios() -> List[str]:
    """从 media_metadata 表中提取所有不重复的工作室。"""
    
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
                        logger.warning(f"  ➜ 处理 studios_json 时遇到意外的类型错误，内容: {studios}")
                        continue
                        
        sorted_studios = sorted(list(unique_studios))
        logger.trace(f"  ➜ 从数据库中成功提取出 {len(sorted_studios)} 个跨电影和电视剧的唯一工作室。")
        return sorted_studios
        
    except psycopg2.Error as e:
        logger.error(f"  ➜ 提取唯一工作室时发生数据库错误: {e}", exc_info=True)
        return []

def get_unique_tags() -> List[str]:
    """ 从 media_metadata 表中提取所有不重复的标签。"""
    
    unique_tags = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tags_json FROM media_metadata WHERE in_library = TRUE")
            rows = cursor.fetchall()
            
            for row in rows:
                tags = row['tags_json']
                if tags:
                    try:
                        for tag in tags:
                            if tag:
                                unique_tags.add(tag.strip())
                    except TypeError:
                        logger.warning(f"  ➜ 处理 tags_json 时遇到意外的类型错误，内容: {tags}")
                        continue
                        
        sorted_tags = sorted(list(unique_tags))
        logger.trace(f"  ➜ 从数据库中成功提取出 {len(sorted_tags)} 个唯一的标签。")
        return sorted_tags
        
    except psycopg2.Error as e:
        logger.error(f"  ➜ 提取唯一标签时发生数据库错误: {e}", exc_info=True)
        return []

def search_unique_studios(search_term: str, limit: int = 20) -> List[str]:
    """ 搜索工作室并优先返回以 search_term 开头的结果。"""
    
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
    logger.trace(f"  ➜ 智能搜索 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
    return final_matches[:limit]

def match_and_update_list_collections_on_item_add(new_item_tmdb_id: str, new_item_emby_id: str, new_item_name: str) -> List[Dict[str, Any]]:
    """
    当新媒体入库时，查找并更新所有匹配的 'list' 和 'ai_recommendation_global' 类型合集。
    """
    collections_to_update_in_emby = []
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                
                # 查询 type 为 list 或 ai_recommendation_global 的合集
                sql_find = """
                    SELECT * FROM custom_collections 
                    WHERE type IN ('list', 'ai_recommendation_global')
                      AND status = 'active' 
                      AND emby_collection_id IS NOT NULL
                      AND generated_media_info_json @> %s::jsonb
                """
                search_payload = json.dumps([{'tmdb_id': str(new_item_tmdb_id)}])
                cursor.execute(sql_find, (search_payload,))
                candidate_collections = cursor.fetchall()
                if not candidate_collections:
                    return []
                
                for collection_row in candidate_collections:
                    collection = dict(collection_row)
                    collection_id = collection['id']
                    collection_name = collection['name']
                    
                    try:
                        media_list_from_db = collection.get('generated_media_info_json') or []
                        all_tmdb_ids_in_collection = [str(item.get('tmdb_id')) for item in media_list_from_db if item.get('tmdb_id')]
                        if not all_tmdb_ids_in_collection:
                            continue
                        
                        in_library_status_map = media_db.get_in_library_status_with_type_bulk(all_tmdb_ids_in_collection)
                        
                        rebuilt_media_list = []
                        new_in_library_count = 0
                        for item in media_list_from_db:
                            tmdb_id = str(item.get('tmdb_id'))
                            media_type = item.get('media_type')
                            if not tmdb_id or not media_type:
                                continue
                            key = f"{tmdb_id}_{media_type}"
                            is_in_library = in_library_status_map.get(key, False)
                            
                            item['status'] = 'in_library' if is_in_library else 'missing'
                            
                            if tmdb_id == str(new_item_tmdb_id):
                                item['emby_id'] = new_item_emby_id
                            
                            rebuilt_media_list.append(item)
                            
                            if is_in_library:
                                new_in_library_count += 1
                        
                        # 只更新 generated_media_info_json 和 in_library_count，去掉 missing_count 和 health_status
                        new_json_data = json.dumps(rebuilt_media_list, ensure_ascii=False, default=str)
                        
                        cursor.execute("""
                            UPDATE custom_collections
                            SET generated_media_info_json = %s,
                                in_library_count = %s
                            WHERE id = %s
                        """, (new_json_data, new_in_library_count, collection_id))
                        
                        logger.info(f"  ➜ 已全量刷新榜单合集《{collection_name}》的缓存，当前入库: {new_in_library_count}。")
                        
                        collections_to_update_in_emby.append({
                            'id': collection_id,
                            'emby_collection_id': collection['emby_collection_id'],
                            'name': collection_name
                        })
                    except Exception as e_inner:
                        logger.error(f"  ➜ 处理合集《{collection_name}》时发生内部错误: {e_inner}", exc_info=True)
                        continue
        
        return collections_to_update_in_emby
    
    except psycopg2.Error as e_db:
        logger.error(f"  ➜ 匹配和更新榜单合集时发生数据库错误: {e_db}", exc_info=True)
        raise