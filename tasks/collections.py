# tasks/collections.py
# 原生合集与自建合集任务模块

import json
import logging
import pytz
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

# 导入需要的底层模块和共享实例
import config_manager
import constants
import emby_handler
import task_manager
import tmdb_handler
from database import collection_db, connection, settings_db
from custom_collection_handler import ListImporter, FilterEngine
from services.cover_generator import CoverGeneratorService

logger = logging.getLogger(__name__)

# ✨ 辅助函数，并发刷新合集使用
def _process_single_collection_concurrently(collection_data: dict, tmdb_api_key: str) -> dict:
    """
    【V5 - 逻辑与类型双重修复版】
    在单个线程中处理单个电影合集的所有逻辑。
    """
    collection_id = collection_data['Id']
    collection_name = collection_data.get('Name', '')
    today_str = datetime.now().strftime('%Y-%m-%d')
    item_type = 'Movie'
    
    # ★★★ 核心修复 1/2: 强制将所有来自Emby的ID转换为字符串集合，确保类型统一 ★★★
    emby_movie_tmdb_ids = {str(id) for id in collection_data.get("ExistingMovieTmdbIds", [])}
    
    in_library_count = len(emby_movie_tmdb_ids)
    status, has_missing = "ok", False
    provider_ids = collection_data.get("ProviderIds", {})
    all_movies_with_status = []
    
    tmdb_id = provider_ids.get("TmdbCollection") or provider_ids.get("TmdbCollectionId") or provider_ids.get("Tmdb")

    if not tmdb_id:
        status = "unlinked"
    else:
        details = tmdb_handler.get_collection_details_tmdb(int(tmdb_id), tmdb_api_key)
        if not details or "parts" not in details:
            status = "tmdb_error"
        else:
            # ★★★ 核心修复 2/2: 修正数据库读取逻辑 ★★★
            previous_movies_map = {}
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = %s", (collection_id,))
                row = cursor.fetchone()
                # 1. 使用字典键 'missing_movies_json' 访问，而不是索引 [0]
                # 2. psycopg2 已经自动解析了 JSONB 字段，无需再 json.loads
                if row and row.get('missing_movies_json'):
                    try:
                        previous_movies_map = {str(m['tmdb_id']): m for m in row['missing_movies_json']}
                    except (TypeError, KeyError): 
                        logger.warning(f"解析合集 '{collection_name}' 的历史数据时格式不兼容，将忽略。")
            
            for movie in details.get("parts", []):
                # 确保 TMDB ID 也为字符串，与上面创建的集合类型一致
                movie_tmdb_id = str(movie.get("id"))
                
                # 跳过没有发布日期的电影，它们通常是未完成的项目
                if not movie.get("release_date"): 
                    continue

                movie_status = "unknown"
                if movie_tmdb_id in emby_movie_tmdb_ids:
                    movie_status = "in_library"
                elif movie.get("release_date", '') > today_str:
                    movie_status = "unreleased"
                elif previous_movies_map.get(movie_tmdb_id, {}).get('status') == 'subscribed':
                    movie_status = "subscribed"
                else:
                    movie_status = "missing"

                all_movies_with_status.append({
                    "tmdb_id": movie_tmdb_id, "title": movie.get("title", ""), 
                    "release_date": movie.get("release_date"), "poster_path": movie.get("poster_path"), 
                    "status": movie_status
                })
            
            if any(m['status'] == 'missing' for m in all_movies_with_status):
                has_missing = True
                status = "has_missing"

    image_tag = collection_data.get("ImageTags", {}).get("Primary")
    poster_path = f"/Items/{collection_id}/Images/Primary?tag={image_tag}" if image_tag else None

    return {
        "emby_collection_id": collection_id, "name": collection_name, 
        "tmdb_collection_id": tmdb_id, "item_type": item_type,
        "status": status, "has_missing": has_missing, 
        "missing_movies_json": json.dumps(all_movies_with_status, ensure_ascii=False), 
        "last_checked_at": datetime.now(timezone.utc), 
        "poster_path": poster_path, 
        "in_library_count": in_library_count
    }

# ★★★ 刷新合集的后台任务函数 ★★★
def task_refresh_collections(processor):
    """
    【V2 - PG语法修正版】
    - 修复了数据库批量写入时使用 SQLite 特有语法 INSERT OR REPLACE 的问题。
    - 改为使用 PostgreSQL 标准的 ON CONFLICT ... DO UPDATE 语法，确保数据能被正确地插入或更新。
    """
    task_manager.update_status_from_thread(0, "正在获取 Emby 合集列表...")
    try:
        emby_collections = emby_handler.get_all_collections_with_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id
        )
        if emby_collections is None: raise RuntimeError("从 Emby 获取合集列表失败")

        total = len(emby_collections)
        task_manager.update_status_from_thread(5, f"共找到 {total} 个合集，准备开始并发处理...")

        # 清理数据库中已不存在的合集
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            emby_current_ids = {c['Id'] for c in emby_collections}
            # ★★★ 语法修正：PostgreSQL 的 cursor.fetchall() 返回字典列表，需要正确提取 ★★★
            cursor.execute("SELECT emby_collection_id FROM collections_info")
            db_known_ids = {row['emby_collection_id'] for row in cursor.fetchall()}
            deleted_ids = db_known_ids - emby_current_ids
            if deleted_ids:
                # executemany 需要一个元组列表
                cursor.executemany("DELETE FROM collections_info WHERE emby_collection_id = %s", [(id,) for id in deleted_ids])
            conn.commit()

        tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not tmdb_api_key: raise RuntimeError("未配置 TMDb API Key")

        processed_count = 0
        all_results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_process_single_collection_concurrently, collection, tmdb_api_key): collection for collection in emby_collections}
            
            for future in as_completed(futures):
                if processor.is_stop_requested():
                    for f in futures: f.cancel()
                    break
                
                collection_name = futures[future].get('Name', '未知合集')
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"处理合集 '{collection_name}' 时线程内发生错误: {e}", exc_info=True)
                
                processed_count += 1
                progress = 10 + int((processed_count / total) * 90)
                task_manager.update_status_from_thread(progress, f"处理中: {collection_name[:20]}... ({processed_count}/{total})")

        if processor.is_stop_requested():
            logger.warning("任务被用户中断，部分数据可能未被处理。")
        
        if all_results:
            logger.info(f"  ➜ 并发处理完成，准备将 {len(all_results)} 条结果写入数据库...")
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION;")
                try:
                    # ★★★ 核心修复：将 INSERT OR REPLACE 改为 ON CONFLICT ... DO UPDATE ★★★
                    # 1. 定义所有列和占位符
                    cols = all_results[0].keys()
                    cols_str = ", ".join(cols)
                    placeholders_str = ", ".join([f"%({k})s" for k in cols]) # 使用 %(key)s 格式
                    
                    # 2. 定义冲突时的更新规则
                    update_cols = [f"{col} = EXCLUDED.{col}" for col in cols if col != 'emby_collection_id']
                    update_str = ", ".join(update_cols)
                    
                    # 3. 构建最终的SQL
                    sql = f"""
                        INSERT INTO collections_info ({cols_str})
                        VALUES ({placeholders_str})
                        ON CONFLICT (emby_collection_id) DO UPDATE SET {update_str}
                    """
                    
                    # 4. 使用 executemany 执行
                    cursor.executemany(sql, all_results)
                    conn.commit()
                    logger.info("  ✅ 数据库写入成功！")
                except Exception as e_db:
                    logger.error(f"数据库批量写入时发生错误: {e_db}", exc_info=True)
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"刷新合集任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# ★★★ 用于统一处理自定义合集的角标逻辑 ★★★
def _get_cover_badge_text_for_collection(collection_db_info: Dict[str, Any]) -> Any:
    """
    根据自定义合集的数据库信息，智能判断并返回用于封面角标的参数。
    - 如果是特定类型的榜单，返回对应的中文字符串。
    - 否则，返回该合集在媒体库中实际包含的项目数量。
    """
    # 默认情况下，角标就是媒体库中的项目数量
    item_count_to_pass = collection_db_info.get('in_library_count', 0)
    
    collection_type = collection_db_info.get('type')
    definition = collection_db_info.get('definition_json', {})

    # 只有榜单(list)类型才需要特殊处理角标文字
    if collection_type == 'list':
        url = definition.get('url', '')
        # 根据URL或其他特征判断榜单来源
        if url.startswith('maoyan://'):
            return '猫眼'
        elif 'douban.com/doulist' in url:
            return '豆列'
        elif 'themoviedb.org/discover/' in url:
            return '探索'
        else:
            # 对于其他所有榜单类型，统一显示为'榜单'
            return '榜单'
            
    # 如果不是榜单类型，或者榜单类型不匹配任何特殊规则，则返回数字角标
    return item_count_to_pass

# --- 可复用的权限更新函数 ---
def update_user_permissions_for_collection(collection_id: int, global_ordered_emby_ids: list, user_permissions_map: dict):
    """
    为单个自定义合集，计算所有用户的专属可见媒体列表，并批量更新到 user_collection_cache 表。
    这是一个可被多处调用的独立、可复用函数。
    """
    logger.debug(f"  ➜ 正在为合集ID {collection_id} 计算并更新用户权限缓存...")
    if not user_permissions_map:
        logger.warning("  ➜ 未提供用户权限映射，跳过权限计算。")
        return

    user_collection_cache_data_to_upsert = []
    for user_id, permission_set in user_permissions_map.items():
        # 计算交集，得到该用户在此合集中可见的媒体ID
        visible_emby_ids = [emby_id for emby_id in global_ordered_emby_ids if emby_id in permission_set]
        user_collection_cache_data_to_upsert.append({
            "user_id": user_id,
            "collection_id": collection_id,
            "visible_emby_ids_json": json.dumps(visible_emby_ids),
            "total_count": len(visible_emby_ids),
        })

    if not user_collection_cache_data_to_upsert:
        logger.debug(f"  ➜ 合集ID {collection_id} 无需更新任何用户权限缓存。")
        return

    # 批量写入数据库
    with connection.get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cols = ["user_id", "collection_id", "visible_emby_ids_json", "total_count"]
            cols_str = ", ".join(cols)
            placeholders_str = ", ".join([f"%({k})s" for k in cols])
            update_cols = [f"{col} = EXCLUDED.{col}" for col in cols]
            update_str = ", ".join(update_cols)
            sql = f"""
                INSERT INTO user_collection_cache ({cols_str}, last_updated_at)
                VALUES ({placeholders_str}, NOW())
                ON CONFLICT (user_id, collection_id) DO UPDATE SET {update_str}, last_updated_at = NOW()
            """
            from psycopg2.extras import execute_batch
            execute_batch(cursor, sql, user_collection_cache_data_to_upsert)
            conn.commit()
            logger.info(f"  ✅ 成功为 {len(user_permissions_map)} 个用户更新了合集ID {collection_id} 的权限缓存。")
        except Exception as e_db:
            logger.error(f"批量写入用户合集权限缓存 (合集ID: {collection_id}) 时发生数据库错误: {e_db}", exc_info=True)
            conn.rollback()

# ★★★ 一键生成所有合集的后台任务 ★★★
def task_process_all_custom_collections(processor):
    """
    - 一键生成所有合集的后台任务。
    """
    task_name = "生成所有自建合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # ======================================================================
        # 步骤 1: 获取所有用户权限
        # ======================================================================
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户及权限...")
        all_emby_users = emby_handler.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
        if not all_emby_users: raise RuntimeError("无法从Emby获取用户列表")
        
        user_permissions_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {executor.submit(emby_handler.get_all_accessible_item_ids_for_user_optimized, processor.emby_url, processor.emby_api_key, user['Id']): user for user in all_emby_users}
            for future in as_completed(future_to_user):
                user = future_to_user[future]
                try:
                    permission_set = future.result()
                    if permission_set is not None: user_permissions_map[user['Id']] = permission_set
                except Exception as e: logger.error(f"为用户 '{user['Name']}' 获取权限时出错: {e}")
        
        # ======================================================================
        # 步骤 2: 获取所有合集数据
        # ======================================================================
        task_manager.update_status_from_thread(10, "正在获取所有启用的合集定义...")
        active_collections = collection_db.get_all_active_custom_collections()
        if not active_collections:
            task_manager.update_status_from_thread(100, "没有已启用的合集。")
            return

        total_collections = len(active_collections)
        task_manager.update_status_from_thread(12, "正在从Emby获取全库媒体数据...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        task_manager.update_status_from_thread(15, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby_handler.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}

        # ★★★ 封面生成逻辑 - 初始化 ★★★
        cover_service = None
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
                logger.trace("  ➜ 封面生成器已启用。")
        except Exception as e_cover_init:
            logger.error(f"初始化封面生成器时失败: {e_cover_init}", exc_info=True)

        # ======================================================================
        # 步骤 3: 遍历所有合集，执行核心逻辑
        # ======================================================================

        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested(): break

            collection_id = collection['id']
            collection_name = collection['name']
            progress = 20 + int((i / total_collections) * 75)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_collections}) 正在计算: {collection_name}")

            try:
                definition = collection['definition_json']
                
                # --- A. 计算全局媒体列表 (逻辑不变) ---
                tmdb_items = []
                if collection['type'] == 'list':
                    importer = ListImporter(processor.tmdb_api_key)
                    tmdb_items, _ = importer.process(definition)
                elif collection['type'] == 'filter':
                    engine = FilterEngine()
                    tmdb_items = engine.execute_filter(definition)

                # 应用修正
                corrections = definition.get('corrections', {})
                if corrections:
                    logger.info(f"  -> 检测到合集 '{collection_name}' 存在 {len(corrections)} 条修正规则，正在应用...")
                    for item in tmdb_items:
                        original_id_str = str(item.get('id'))
                        if original_id_str in corrections:
                            corrected_value = corrections[original_id_str]
                            logger.info(f"    -> 应用修正: 将源 ID {original_id_str} 替换为 {corrected_value}")
                            
                            # ★★★ 核心修复：判断修正值的类型 ★★★
                            # 如果修正值是一个字典 (新格式)，则从中提取 tmdb_id
                            if isinstance(corrected_value, dict):
                                item['id'] = corrected_value.get('tmdb_id')
                            # 否则，直接使用该值 (兼容旧的纯ID修正)
                            else:
                                item['id'] = corrected_value
                
                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未生成任何媒体ID，跳过。")
                    continue

                global_ordered_emby_ids = [tmdb_to_emby_item_map[item['id']]['Id'] for item in tmdb_items if item['id'] in tmdb_to_emby_item_map]

                # --- B. 创建/更新 Emby 物理合集 (逻辑不变) ---
                emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                    collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
                    base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
                    prefetched_collection_map=prefetched_collection_map
                )

                # --- C. 【用户权限 - 写入缓存】为每个用户计算专属列表，并【累积】到列表中 ---
                update_user_permissions_for_collection(collection_id, global_ordered_emby_ids, user_permissions_map)

                # --- D. 【健康检查 & 状态更新】根据合集类型执行不同逻辑 ---
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc),
                    "in_library_count": len(global_ordered_emby_ids),
                }

                if collection['type'] == 'list':
                    logger.info(f"  ➜ 榜单合集 '{collection_name}'，开始进行详细健康度分析...")
                    previous_media_map = {str(m.get('tmdb_id')): m for m in (collection.get('generated_media_info_json') or [])}
                    all_media_details_unordered = []
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        f_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                        for future in as_completed(f_to_item):
                            try:
                                detail = future.result()
                                if detail: all_media_details_unordered.append(detail)
                            except Exception as exc: logger.error(f"获取TMDb详情时出错: {exc}")
                    details_map = {str(d.get("id")): d for d in all_media_details_unordered}
                    tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
                    all_media_with_status, has_missing, missing_count = [], False, 0
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    for item in tmdb_items:
                        media_tmdb_id = str(item['id'])
                        media = details_map.get(media_tmdb_id)
                        if not media: continue
                        emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                        release_date = media.get("release_date") or media.get("first_air_date", '')
                        media_status = "in_library" if emby_item else ("subscribed" if previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed' else ("unreleased" if release_date and release_date > today_str else "missing"))
                        if media_status == 'missing': has_missing, missing_count = True, missing_count + 1
                        final_media_item = {"tmdb_id": media_tmdb_id, "emby_id": emby_item.get('Id') if emby_item else None, "title": media.get("title") or media.get("name"), "release_date": release_date, "poster_path": media.get("poster_path"), "status": media_status}
                        season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                        if season_number is not None: final_media_item['season'] = season_number
                        all_media_with_status.append(final_media_item)
                    update_data.update({"health_status": "has_missing" if has_missing else "ok", "missing_count": missing_count, "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False)})
                else:
                    update_data.update({"health_status": "ok", "missing_count": 0})

                collection_db.update_custom_collection_after_sync(collection_id, update_data)

                # ★★★ 封面生成逻辑 - 调用 ★★★
                if cover_service and emby_collection_id:
                    try:
                        library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                        if library_info:
                            latest_collection_info = collection_db.get_custom_collection_by_id(collection_id)
                            item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                            cover_service.generate_for_library(
                                emby_server_id='main_emby', library=library_info,
                                item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                            )
                    except Exception as e_cover:
                        logger.error(f"为合集 '{collection_name}' 生成封面时出错: {e_cover}", exc_info=True)

                # 如果刚刚处理的是一个猫眼榜单，就主动休息几秒，避免对猫眼服务器造成压力
                if collection['type'] == 'list' and collection['definition_json'].get('url', '').startswith('maoyan://'):
                    delay_seconds = 10
                    logger.info(f"  ➜ 已处理一个猫眼榜单，为避免触发反爬机制，将主动降温 {delay_seconds} 秒...")
                    time.sleep(delay_seconds)
                
            except Exception as e_coll:
                logger.error(f"处理合集 '{collection_name}' (ID: {collection_id}) 时发生错误: {e_coll}", exc_info=True)
                continue
        
        final_message = "所有自建合集均已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 处理单个自定义合集的核心任务 ---
def process_single_custom_collection(processor, custom_collection_id: int):
    """
    - 处理单个自定义合集的核心任务
    """
    task_name = f"生成单个自建合集 (ID: {custom_collection_id})"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ======================================================================
        # 步骤 1: 获取所有用户权限
        # ======================================================================
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户及权限...")
        all_emby_users = emby_handler.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
        if not all_emby_users: raise RuntimeError("无法获取用户列表")
        
        user_permissions_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {executor.submit(emby_handler.get_all_accessible_item_ids_for_user_optimized, processor.emby_url, processor.emby_api_key, user['Id']): user for user in all_emby_users}
            for future in as_completed(future_to_user):
                user = future_to_user[future]
                try:
                    permission_set = future.result()
                    if permission_set is not None: user_permissions_map[user['Id']] = permission_set
                except Exception as e: logger.error(f"为用户 '{user['Name']}' 获取权限时出错: {e}")
        
        # ======================================================================
        # 步骤 2: 获取合集定义
        # ======================================================================
        task_manager.update_status_from_thread(20, "正在读取合集定义...")
        collection = collection_db.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        
        collection_name = collection['name']
        
        # ======================================================================
        # 步骤 3: 创建物理合集
        # ======================================================================
        task_manager.update_status_from_thread(30, f"正在为《{collection_name}》计算媒体列表...")
        definition = collection['definition_json']
        tmdb_items = []
        if collection['type'] == 'list':
            importer = ListImporter(processor.tmdb_api_key)
            tmdb_items, _ = importer.process(definition)
        elif collection['type'] == 'filter':
            engine = FilterEngine()
            tmdb_items = engine.execute_filter(definition)

        # 应用修正
        corrections = definition.get('corrections', {})
        if corrections:
            logger.info(f"  -> 检测到合集 '{collection_name}' 存在 {len(corrections)} 条修正规则，正在应用...")
            for item in tmdb_items:
                original_id_str = str(item.get('id'))
                if original_id_str in corrections:
                    corrected_value = corrections[original_id_str]
                    logger.info(f"    -> 应用修正: 将源 ID {original_id_str} 替换为 {corrected_value}")
                    
                    # ★★★ 核心修复：判断修正值的类型 ★★★
                    # 如果修正值是一个字典 (新格式)，则从中提取 tmdb_id
                    if isinstance(corrected_value, dict):
                        item['id'] = corrected_value.get('tmdb_id')
                    # 否则，直接使用该值 (兼容旧的纯ID修正)
                    else:
                        item['id'] = corrected_value
        
        if not tmdb_items:
            collection_db.update_custom_collection_after_sync(custom_collection_id, {"emby_collection_id": None})
            task_manager.update_status_from_thread(100, "该合集未匹配到任何媒体。")
            return

        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        global_ordered_emby_ids = [tmdb_to_emby_item_map[item['id']]['Id'] for item in tmdb_items if item['id'] in tmdb_to_emby_item_map]

        task_manager.update_status_from_thread(70, "正在Emby中创建/更新合集...")
        emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
            collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id
        )

        # ======================================================================
        # 步骤 4: 写入用户权限缓存
        # ======================================================================
        task_manager.update_status_from_thread(90, "正在为所有用户更新此合集的权限缓存...")
        update_user_permissions_for_collection(custom_collection_id, global_ordered_emby_ids, user_permissions_map)

        # ======================================================================
        # 步骤 5: 榜单类合集健康度检查
        # ======================================================================
        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(definition.get('item_type', ['Movie'])),
            "last_synced_at": datetime.now(pytz.utc),
            "in_library_count": len(global_ordered_emby_ids),
        }

        if collection['type'] == 'list':
            logger.info(f"  ➜ 榜单合集 '{collection_name}'，开始进行详细健康度分析...")
            previous_media_map = {str(m.get('tmdb_id')): m for m in (collection.get('generated_media_info_json') or [])}
            all_media_details_unordered = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                f_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                for future in as_completed(f_to_item):
                    try:
                        detail = future.result()
                        if detail: all_media_details_unordered.append(detail)
                    except Exception as exc: logger.error(f"获取TMDb详情时出错: {exc}")
            details_map = {str(d.get("id")): d for d in all_media_details_unordered}
            tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
            all_media_with_status, has_missing, missing_count = [], False, 0
            today_str = datetime.now().strftime('%Y-%m-%d')
            for item in tmdb_items:
                media_tmdb_id = str(item['id'])
                media = details_map.get(media_tmdb_id)
                if not media: continue
                emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                release_date = media.get("release_date") or media.get("first_air_date", '')
                media_status = "in_library" if emby_item else ("subscribed" if previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed' else ("unreleased" if release_date and release_date > today_str else "missing"))
                if media_status == 'missing': has_missing, missing_count = True, missing_count + 1
                final_media_item = {"tmdb_id": media_tmdb_id, "emby_id": emby_item.get('Id') if emby_item else None, "title": media.get("title") or media.get("name"), "release_date": release_date, "poster_path": media.get("poster_path"), "status": media_status}
                season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                if season_number is not None: final_media_item['season'] = season_number
                all_media_with_status.append(final_media_item)
            update_data.update({"health_status": "has_missing" if has_missing else "ok", "missing_count": missing_count, "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False)})
        else:
            update_data.update({"health_status": "ok", "missing_count": 0})

        collection_db.update_custom_collection_after_sync(custom_collection_id, update_data)

        # ======================================================================
        # 步骤 6: 封面生成
        # ======================================================================
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled") and emby_collection_id:
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    latest_collection_info = collection_db.get_custom_collection_by_id(custom_collection_id)
                    item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                    cover_service.generate_for_library(
                        emby_server_id='main_emby', library=library_info,
                        item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                    )
        except Exception as e_cover:
            logger.error(f"为合集 '{collection_name}' 生成封面时发生错误: {e_cover}", exc_info=True)
        
        task_manager.update_status_from_thread(100, "单个自定义合集同步完成！")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")