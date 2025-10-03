# tasks/collections.py
# 原生合集与自建合集任务模块

import json
import logging
import pytz
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any
import gevent

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

# ★★★ 一键生成所有合集的后台任务 ★★★
def task_process_all_custom_collections(processor):
    """
    【V7 - 榜单类型识别 & 精确封面参数】
    """
    task_name = "生成所有自建合集"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # ... (前面的代码都不变，直到 for 循环) ...
        task_manager.update_status_from_thread(0, "正在获取所有启用的合集定义...")
        active_collections = collection_db.get_all_active_custom_collections()
        if not active_collections:
            logger.info("  ➜ 没有找到任何已启用的自定义合集，任务结束。")
            task_manager.update_status_from_thread(100, "没有已启用的合集。")
            return
        
        total = len(active_collections)
        logger.info(f"  ➜ 共找到 {total} 个已启用的自定义合集需要处理。")

        task_manager.update_status_from_thread(2, "正在从Emby获取全库媒体数据...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids: raise ValueError("未在配置中指定要处理的媒体库。")
        
        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        logger.info(f"  ➜ 已从Emby获取 {len(all_emby_items)} 个媒体项目，并创建了TMDB->Emby映射。")

        task_manager.update_status_from_thread(5, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby_handler.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}
        logger.info(f"  ➜ 已预加载 {len(prefetched_collection_map)} 个现有合集的信息。")

        cover_service = None
        cover_config = {}
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
                logger.info("  ➜ 封面生成器已启用，将在每个合集处理后尝试生成封面。")
        except Exception as e_cover_init:
            logger.error(f"初始化封面生成器时失败: {e_cover_init}", exc_info=True)


        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break

            collection_id = collection['id']
            collection_name = collection['name']
            collection_type = collection['type']
            definition = collection['definition_json']
            
            progress = 10 + int((i / total) * 90)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total}) 正在处理: {collection_name}")

            try:
                item_types_for_collection = definition.get('item_type', ['Movie'])
                tmdb_items = []
                source_type = 'filter' # 默认

                if collection_type == 'list':
                    url = definition.get('url', '')
                    if url.startswith('maoyan://'):
                        source_type = 'list_maoyan'
                        importer = ListImporter(processor.tmdb_api_key)
                        greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
                        tmdb_items = greenlet.get()
                    else:
                        importer = ListImporter(processor.tmdb_api_key)
                        tmdb_items, source_type = importer.process(definition)

                    # ▼▼▼ 入修正逻辑 ▼▼▼
                    corrections = definition.get('corrections', {})
                    if corrections:
                        logger.debug(f"  ➜ 检测到 {len(corrections)} 条修正规则，开始应用...")
                        corrected_tmdb_items = []
                        for item in tmdb_items:
                            original_id = str(item.get('id'))
                            
                            if original_id in corrections:
                                correction_info = corrections[original_id]
                                
                                # 健壮性检查：处理新旧两种修正格式
                                if isinstance(correction_info, dict):
                                    # 新格式: {'tmdb_id': '...', 'season': ...}
                                    new_id = correction_info.get('tmdb_id')
                                    new_season = correction_info.get('season')
                                    
                                    if new_id:
                                        logger.info(f"    ➜ 应用修正: {original_id} -> {new_id} (季号: {new_season})")
                                        item['id'] = new_id # 只更新 ID
                                        if new_season is not None:
                                            item['season'] = new_season # 更新或添加 season
                                        else:
                                            item.pop('season', None) # 确保移除旧的 season
                                    else:
                                        logger.warning(f"    ➜ 修正规则格式错误，跳过: {correction_info}")

                                elif isinstance(correction_info, str):
                                    # 兼容旧格式: '新ID'
                                    logger.info(f"    ➜ 应用修正 (旧格式): {original_id} -> {correction_info}")
                                    item['id'] = correction_info
                                
                            corrected_tmdb_items.append(item)

                elif collection_type == 'filter':
                    engine = FilterEngine()
                    tmdb_items = engine.execute_filter(definition)
                
                # ... (后续代码直到封面生成部分) ...
                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，跳过。")
                    collection_db.update_custom_collection_after_sync(collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]", "generated_emby_ids_json": "[]"})
                    continue

                ordered_emby_ids_in_library = [
                    tmdb_to_emby_item_map[item['id']]['Id'] 
                    for item in tmdb_items if item['id'] in tmdb_to_emby_item_map
                ]

                emby_collection_id = None # 先初始化为 None
                if not ordered_emby_ids_in_library:
                    logger.warning(f"榜单 '{collection_name}' 解析成功，但在您的媒体库中未找到任何匹配项目。将只更新数据库，不创建Emby合集。")
                else:
                    emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                        collection_name=collection_name, 
                        emby_ids_in_library=ordered_emby_ids_in_library, 
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key, 
                        user_id=processor.emby_user_id,
                        prefetched_collection_map=prefetched_collection_map
                    )
                    if not emby_collection_id:
                        raise RuntimeError("在Emby中创建或更新合集失败，请检查Emby日志。")
                
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc)
                }

                if collection_type == 'list':
                    # ... (这部分健康度检查逻辑不变) ...
                    previous_media_map = {}
                    try:
                        previous_media_list = collection.get('generated_media_info_json') or []
                        previous_media_map = {str(m.get('tmdb_id')): m for m in previous_media_list}
                    except TypeError:
                        logger.warning(f"解析合集 {collection_name} 的旧媒体JSON失败...")
                    
                    image_tag = None
                    if emby_collection_id:
                        emby_collection_details = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                        image_tag = emby_collection_details.get("ImageTags", {}).get("Primary")
                    
                    all_media_details_unordered = []
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        future_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                        for future in as_completed(future_to_item):
                            try:
                                detail = future.result()
                                if detail: all_media_details_unordered.append(detail)
                            except Exception as exc:
                                logger.error(f"获取TMDb详情时线程内出错: {exc}")
                    
                    details_map = {str(d.get("id")): d for d in all_media_details_unordered}
                    all_media_details_ordered = [details_map[item['id']] for item in tmdb_items if item['id'] in details_map]

                    tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
                    all_media_with_status, has_missing, missing_count = [], False, 0
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    
                    for media in all_media_details_ordered:
                        media_tmdb_id = str(media.get("id"))
                        emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                        
                        release_date = media.get("release_date") or media.get("first_air_date", '')
                        media_status = "unknown"
                        if emby_item: media_status = "in_library"
                        elif previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed': media_status = "subscribed"
                        elif release_date and release_date > today_str: media_status = "unreleased"
                        else: media_status, has_missing, missing_count = "missing", True, missing_count + 1
                        
                        final_media_item = {
                            "tmdb_id": media_tmdb_id,
                            "emby_id": emby_item.get('Id') if emby_item else None,
                            "title": media.get("title") or media.get("name"),
                            "release_date": release_date,
                            "poster_path": media.get("poster_path"),
                            "status": media_status
                        }

                        season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                        if season_number is not None:
                            final_media_item['season'] = season_number
                            final_media_item['title'] = f"{final_media_item['title']} 第 {season_number} 季"
                        
                        all_media_with_status.append(final_media_item)

                    update_data.update({
                        "health_status": "has_missing" if has_missing else "ok",
                        "in_library_count": len(ordered_emby_ids_in_library),
                        "missing_count": missing_count,
                        "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False),
                        "poster_path": f"/Items/{emby_collection_id}/Images/Primary?tag={image_tag}" if image_tag and emby_collection_id else None
                    })
                else: 
                    all_media_with_status = [
                        {
                            'tmdb_id': item['id'],
                            'emby_id': tmdb_to_emby_item_map.get(item['id'], {}).get('Id')
                        }
                        for item in tmdb_items
                    ]
                    update_data.update({
                        "health_status": "ok", 
                        "in_library_count": len(ordered_emby_ids_in_library),
                        "missing_count": 0, 
                        "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False), 
                        "poster_path": None
                    })
                
                collection_db.update_custom_collection_after_sync(collection_id, update_data)
                logger.info(f"  ✅ 合集 '{collection_name}' 处理完成，并已更新数据库状态。")

                if cover_service and emby_collection_id:
                    logger.info(f"  ➜ 正在为合集 '{collection_name}' 生成封面...")
                    # 1. 获取最新的 Emby 合集详情
                    library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                    if library_info:
                        # 2. 准备封面生成器需要的其他参数
                        item_types_for_collection = definition.get('item_type', ['Movie'])
                        
                        # 3. 将数据库中最新的合集信息（包含in_library_count）传递给辅助函数
                        #    我们使用 db_handler 重新获取一次，确保拿到的是刚刚更新过的最新数据
                        latest_collection_info = collection_db.get_custom_collection_by_id(collection_id)
                        item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                        
                        # 4. 调用封面生成服务
                        cover_service.generate_for_library(
                            emby_server_id='main_emby',
                            library=library_info,
                            item_count=item_count_to_pass, # <-- 使用计算好的角标参数
                            content_types=item_types_for_collection
                        )
            except Exception as e_coll:
                logger.error(f"处理合集 '{collection_name}' (ID: {collection_id}) 时发生错误: {e_coll}", exc_info=True)
                continue
        
        final_message = "所有启用的自定义合集均已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 处理单个自定义合集的核心任务 ---
def task_process_custom_collection(processor, custom_collection_id: int):
    """
    【V12 - 榜单类型识别 & 精确封面参数】
    """
    task_name = f"处理自定义合集 (ID: {custom_collection_id})"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ... (前面的代码都不变，直到 tmdb_items = [] ) ...
        task_manager.update_status_from_thread(0, "正在读取合集定义...")
        collection = collection_db.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        
        collection_name = collection['name']
        collection_type = collection['type']
        definition = collection['definition_json']
        
        item_types_for_collection = definition.get('item_type', ['Movie'])
        
        tmdb_items = []
        source_type = 'filter' # 默认

        if collection_type == 'list':
            url = definition.get('url', '')
            if url.startswith('maoyan://'):
                source_type = 'list_maoyan'
                logger.info(f"检测到猫眼榜单 '{collection_name}'，将启动异步后台任务...")
                task_manager.update_status_from_thread(10, f"正在后台获取猫眼榜单: {collection_name}...")
                importer = ListImporter(processor.tmdb_api_key)
                greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
                tmdb_items = greenlet.get()
            else:
                importer = ListImporter(processor.tmdb_api_key)
                tmdb_items, source_type = importer.process(definition)

            # ▼▼▼ 修正逻辑 ▼▼▼
            corrections = definition.get('corrections', {})
            if corrections:
                logger.debug(f"  ➜ 检测到 {len(corrections)} 条修正规则，开始应用...")
                corrected_tmdb_items = []
                for item in tmdb_items:
                    original_id = str(item.get('id'))
                    
                    if original_id in corrections:
                        correction_info = corrections[original_id]
                        
                        # 健壮性检查：处理新旧两种修正格式
                        if isinstance(correction_info, dict):
                            # 新格式: {'tmdb_id': '...', 'season': ...}
                            new_id = correction_info.get('tmdb_id')
                            new_season = correction_info.get('season')
                            
                            if new_id:
                                logger.info(f"    ➜ 应用修正: {original_id} -> {new_id} (季号: {new_season})")
                                item['id'] = new_id # 只更新 ID
                                if new_season is not None:
                                    item['season'] = new_season # 更新或添加 season
                                else:
                                    item.pop('season', None) # 确保移除旧的 season
                            else:
                                logger.warning(f"    ➜ 修正规则格式错误，跳过: {correction_info}")

                        elif isinstance(correction_info, str):
                            # 兼容旧格式: '新ID'
                            logger.info(f"    ➜ 应用修正 (旧格式): {original_id} -> {correction_info}")
                            item['id'] = correction_info
                        
                    corrected_tmdb_items.append(item)

        elif collection_type == 'filter':
            engine = FilterEngine()
            tmdb_items = engine.execute_filter(definition)
        
        # ... (后续代码直到封面生成部分) ...
        if not tmdb_items:
            logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，任务结束。")
            collection_db.update_custom_collection_after_sync(custom_collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]"})
            return

        task_manager.update_status_from_thread(70, f"已生成 {len(tmdb_items)} 个ID，正在Emby中创建/更新合集...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])

        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter=",".join(item_types_for_collection), library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        ordered_emby_ids_in_library = [tmdb_to_emby_item_map[item['id']]['Id'] for item in tmdb_items if item['id'] in tmdb_to_emby_item_map]

        if not ordered_emby_ids_in_library:
            logger.warning(f"榜单 '{collection_name}' 解析成功，但在您的媒体库中未找到任何匹配项目。将只更新数据库，不创建Emby合集。")
            emby_collection_id = None 
        else:
            emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                collection_name=collection_name, 
                emby_ids_in_library=ordered_emby_ids_in_library, 
                base_url=processor.emby_url,
                api_key=processor.emby_api_key, 
                user_id=processor.emby_user_id
            )
            if not emby_collection_id:
                raise RuntimeError("在Emby中创建或更新合集失败。")
        
        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(item_types_for_collection),
            "last_synced_at": datetime.now(pytz.utc)
        }

        if collection_type == 'list':
            # ... (这部分健康度检查逻辑不变) ...
            task_manager.update_status_from_thread(90, "榜单合集已同步，正在并行获取详情...")
            
            previous_media_map = {}
            try:
                previous_media_list = collection.get('generated_media_info_json') or []
                previous_media_map = {str(m.get('tmdb_id')): m for m in previous_media_list}
            except TypeError:
                logger.warning(f"解析合集 {collection_name} 的旧媒体JSON失败...")

            image_tag = None
            if emby_collection_id:
                emby_collection_details = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                image_tag = emby_collection_details.get("ImageTags", {}).get("Primary")
            
            all_media_details_unordered = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                for future in as_completed(future_to_item):
                    try:
                        detail = future.result()
                        if detail: all_media_details_unordered.append(detail)
                    except Exception as exc:
                        logger.error(f"获取TMDb详情时线程内出错: {exc}")

            details_map = {str(d.get("id")): d for d in all_media_details_unordered}
            all_media_details_ordered = [details_map[item['id']] for item in tmdb_items if item['id'] in details_map]
            
            tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
            all_media_with_status, has_missing, missing_count = [], False, 0
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            for media in all_media_details_ordered:
                media_tmdb_id = str(media.get("id"))
                emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                
                release_date = media.get("release_date") or media.get("first_air_date", '')
                media_status = "unknown"
                if emby_item: media_status = "in_library"
                elif previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed': media_status = "subscribed"
                elif release_date and release_date > today_str: media_status = "unreleased"
                else: media_status, has_missing, missing_count = "missing", True, missing_count + 1
                
                final_media_item = {
                    "tmdb_id": media_tmdb_id,
                    "emby_id": emby_item.get('Id') if emby_item else None,
                    "title": media.get("title") or media.get("name"),
                    "release_date": release_date,
                    "poster_path": media.get("poster_path"),
                    "status": media_status
                }

                season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                if season_number is not None:
                    final_media_item['season'] = season_number
                    final_media_item['title'] = f"{final_media_item['title']} 第 {season_number} 季"
                
                all_media_with_status.append(final_media_item)

            update_data.update({
                "health_status": "has_missing" if has_missing else "ok",
                "in_library_count": len(ordered_emby_ids_in_library),
                "missing_count": missing_count,
                "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False),
                "poster_path": f"/Items/{emby_collection_id}/Images/Primary?tag={image_tag}" if image_tag and emby_collection_id else None
            })
            logger.info(f"  ➜ 已为RSS合集 '{collection_name}' 分析健康状态。")
        else: 
            task_manager.update_status_from_thread(95, "筛选合集已生成，跳过缺失分析。")
            all_media_with_status = [{'tmdb_id': item['id'], 'emby_id': tmdb_to_emby_item_map.get(item['id'], {}).get('Id')} for item in tmdb_items]
            update_data.update({
                "health_status": "ok", "in_library_count": len(ordered_emby_ids_in_library),
                "missing_count": 0, 
                "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False), 
                "poster_path": None
            })

        collection_db.update_custom_collection_after_sync(custom_collection_id, update_data)
        logger.info(f"  ➜ 已更新自定义合集 '{collection_name}' (ID: {custom_collection_id}) 的同步状态和健康信息。")

        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and emby_collection_id:
                logger.info(f"  ➜ 检测到封面生成器已启用，将为合集 '{collection_name}' 生成封面...")
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    # ▼▼▼ 核心修改点 ▼▼▼
                    # 1. 获取最新的合集信息
                    latest_collection_info = collection_db.get_custom_collection_by_id(custom_collection_id)
                    
                    # 2. 调用辅助函数获取正确的角标参数
                    item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                        
                    # 3. 调用封面生成服务
                    cover_service.generate_for_library(
                        emby_server_id='main_emby',
                        library=library_info,
                        item_count=item_count_to_pass, # <-- 使用计算好的角标参数
                        content_types=item_types_for_collection
                    )
                else:
                    logger.warning(f"无法获取 Emby 合集 {emby_collection_id} 的详情，跳过封面生成。")
        except Exception as e:
            logger.error(f"为合集 '{collection_name}' 生成封面时发生错误: {e}", exc_info=True)

        task_manager.update_status_from_thread(100, "自定义合集同步并分析完成！")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")