# tasks/media.py
# 核心媒体处理、元数据、资产同步等

import time
import json
import logging
import psycopg2
from typing import Optional, List
from datetime import datetime, timezone
import concurrent.futures

# 导入需要的底层模块和共享实例
import task_manager
import tmdb_handler
import emby_handler
from database import connection
from utils import translate_country_list, get_unified_rating

logger = logging.getLogger(__name__)

# ★★★ 全量处理任务 ★★★
def task_run_full_scan(processor, force_reprocess: bool = False):
    """
    根据传入的 force_reprocess 参数，决定是执行标准扫描还是强制扫描。
    """
    # 1. 根据参数决定日志信息
    if force_reprocess:
        logger.info("即将执行全量处理，将处理所有媒体项...")
    else:
        logger.info("即将执行增量处理，将跳过已处理项...")


    # 3. 调用核心处理函数，并将 force_reprocess 参数透传下去
    processor.process_full_library(
        update_status_callback=task_manager.update_status_from_thread,
        force_reprocess_all=force_reprocess,
        force_fetch_from_tmdb=force_reprocess
    )

# --- 使用手动编辑的结果处理媒体项 ---
def task_manual_update(processor, item_id: str, manual_cast_list: list, item_name: str):
    """任务：使用手动编辑的结果处理媒体项"""
    processor.process_item_with_manual_cast(
        item_id=item_id,
        manual_cast_list=manual_cast_list,
        item_name=item_name
    )

def task_sync_metadata_cache(processor, item_id: str, item_name: str, episode_ids_to_add: Optional[List[str]] = None):
    """
    任务：为单个媒体项同步元数据到 media_metadata 数据库表。
    可根据是否传入 episode_ids_to_add 来决定执行模式。
    """
    sync_mode = "精准分集追加" if episode_ids_to_add else "常规元数据刷新"
    logger.trace(f"  ➜ 任务开始：同步媒体元数据缓存 ({sync_mode}) for '{item_name}' (ID: {item_id})")
    try:
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name, episode_ids_to_add=episode_ids_to_add)
        logger.trace(f"  ➜ 任务成功：同步媒体元数据缓存 for '{item_name}'")
    except Exception as e:
        logger.error(f"  ➜ 任务失败：同步媒体元数据缓存 for '{item_name}' 时发生错误: {e}", exc_info=True)
        raise

def task_sync_images(processor, item_id: str, update_description: str, sync_timestamp_iso: str):
    """
    任务：为单个媒体项同步图片和元数据文件到本地 override 目录。
    """
    logger.trace(f"任务开始：图片备份 for ID: {item_id} (原因: {update_description})")
    try:
        # --- ▼▼▼ 核心修复 ▼▼▼ ---
        # 1. 根据 item_id 获取完整的媒体详情
        item_details = emby_handler.get_emby_item_details(
            item_id, 
            processor.emby_url, 
            processor.emby_api_key, 
            processor.emby_user_id
        )
        if not item_details:
            logger.error(f"任务失败：无法获取 ID: {item_id} 的媒体详情，跳过图片备份。")
            return

        # 2. 使用获取到的 item_details 字典来调用
        processor.sync_item_images(
            item_details=item_details, 
            update_description=update_description
            # episode_ids_to_sync 参数这里不需要，sync_item_images 会自己处理
        )
        # --- ▲▲▲ 修复结束 ▲▲▲ ---

        logger.trace(f"任务成功：图片备份 for ID: {item_id}")
    except Exception as e:
        logger.error(f"任务失败：图片备份 for ID: {item_id} 时发生错误: {e}", exc_info=True)
        raise

def task_sync_all_metadata(processor, item_id: str, item_name: str):
    """
    【任务：全能元数据同步器。
    当收到 metadata.update Webhook 时，此任务会：
    1. 从 Emby 获取最新数据。
    2. 将更新持久化到 override 覆盖缓存文件。
    3. 将更新同步到 media_metadata 数据库缓存。
    """
    log_prefix = f"全能元数据同步 for '{item_name}'"
    logger.trace(f"  ➜ 任务开始：{log_prefix}")
    try:
        # 步骤 1: 获取包含了用户修改的、最新的完整媒体详情
        item_details = emby_handler.get_emby_item_details(
            item_id, 
            processor.emby_url, 
            processor.emby_api_key, 
            processor.emby_user_id,
            # 请求所有可能被用户修改的字段
            fields="ProviderIds,Type,Name,OriginalTitle,Overview,Tagline,CommunityRating,OfficialRating,Genres,Studios,Tags,PremiereDate"
        )
        if not item_details:
            logger.error(f"  ➜ {log_prefix} 失败：无法获取项目 {item_id} 的最新详情。")
            return

        # 步骤 2: 调用施工队，更新 override 文件
        processor.sync_emby_updates_to_override_files(item_details)

        # 步骤 3: 调用另一个施工队，更新数据库缓存
        # 注意：这里我们复用现有的 task_sync_metadata_cache 逻辑
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name)

        logger.trace(f"  ➜ 任务成功：{log_prefix}")
    except Exception as e:
        logger.error(f"  ➜ 任务失败：{log_prefix} 时发生错误: {e}", exc_info=True)
        raise

# ★★★ 重新处理单个项目 ★★★
def task_reprocess_single_item(processor, item_id: str, item_name_for_ui: str):
    """
    【最终版 - 职责分离】后台任务。
    此版本负责在任务开始时设置“正在处理”的状态，并执行核心逻辑。
    """
    logger.trace(f"  ➜ 后台任务开始执行 ({item_name_for_ui})")
    
    try:
        # ✨ 关键修改：任务一开始，就用“正在处理”的状态覆盖掉旧状态
        task_manager.update_status_from_thread(0, f"正在处理: {item_name_for_ui}")

        # 现在才开始真正的工作
        processor.process_single_item(
            item_id, 
            force_reprocess_this_item=True,
            force_fetch_from_tmdb=True
        )
        # 任务成功完成后的状态更新会自动由任务队列处理，我们无需关心
        logger.trace(f"  ➜ 后台任务完成 ({item_name_for_ui})")

    except Exception as e:
        logger.error(f"后台任务处理 '{item_name_for_ui}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"处理失败: {item_name_for_ui}")

# ★★★ 重新处理所有待复核项 ★★★
def task_reprocess_all_review_items(processor):
    """
    【已升级】后台任务：遍历所有待复核项并逐一以“强制在线获取”模式重新处理。
    """
    logger.trace("--- 开始执行“重新处理所有待复核项”任务 [强制在线获取模式] ---")
    try:
        # +++ 核心修改 1：同时查询 item_id 和 item_name +++
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 从 failed_log 中同时获取 ID 和 Name
            cursor.execute("SELECT item_id, item_name FROM failed_log")
            # 将结果保存为一个字典列表，方便后续使用
            all_items = [{'id': row['item_id'], 'name': row['item_name']} for row in cursor.fetchall()]
        
        total = len(all_items)
        if total == 0:
            logger.info("待复核列表中没有项目，任务结束。")
            task_manager.update_status_from_thread(100, "待复核列表为空。")
            return

        logger.info(f"共找到 {total} 个待复核项需要以“强制在线获取”模式重新处理。")

        # +++ 核心修改 2：在循环中解包 item_id 和 item_name +++
        for i, item in enumerate(all_items):
            if processor.is_stop_requested():
                logger.info("任务被中止。")
                break
            
            item_id = item['id']
            item_name = item['name'] or f"ItemID: {item_id}" # 如果名字为空，提供一个备用名

            task_manager.update_status_from_thread(int((i/total)*100), f"正在重新处理 {i+1}/{total}: {item_name}")
            
            # +++ 核心修改 3：传递所有必需的参数 +++
            task_reprocess_single_item(processor, item_id, item_name)
            
            # 每个项目之间稍作停顿
            time.sleep(2) 

    except Exception as e:
        logger.error(f"重新处理所有待复核项时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")

# ★★★ 轻量级的元数据缓存填充任务 ★★★
def task_populate_metadata_cache(processor, batch_size: int = 50, force_full_update: bool = False):
    """
    【V5 - 增量与全量同步版 - 软删除兼容】
    - 移除了基于时间戳的差异对比逻辑。
    - 快速模式: 只同步 Emby 中新增的、本地数据库不存在的媒体项。
    - 深度模式: 强制同步 Emby 媒体库中的所有媒体项，覆盖本地数据。
    - 保留了高效的分批处理、并发获取和带 SAVEPOINT 的健壮数据库写入机制。
    - ★★★ 新增：将删除逻辑改为“软删除”，更新 in_library 字段为 FALSE，并清空 emby_item_id。 ★★★
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    try:
        # ======================================================================
        # 步骤 1: 计算差异 (逻辑已简化)
        # ======================================================================
        task_manager.update_status_from_thread(0, f"阶段1/2: 计算媒体库差异 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        emby_items_index = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            fields="ProviderIds,Type,DateCreated,Name,ProductionYear,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,ProductionLocations,Tags,DateModified,OfficialRating"
        ) or []
        
        emby_items_map = {
            item.get("ProviderIds", {}).get("Tmdb"): item 
            for item in emby_items_index if item.get("ProviderIds", {}).get("Tmdb")
        }
        emby_tmdb_ids = set(emby_items_map.keys())
        logger.info(f"  ➜ 从 Emby 获取到 {len(emby_tmdb_ids)} 个有效的媒体项。")

        if processor.is_stop_requested():
            logger.info("任务在获取 Emby 媒体项后被中止。")
            return

        db_tmdb_ids = set()
        # ★★★ 修改点：查询时只获取 in_library = TRUE 的项目，这样才能正确识别“已删除”的项目 ★★★
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata WHERE in_library = TRUE") # <-- 增加条件
            db_tmdb_ids = {row["tmdb_id"] for row in cursor.fetchall()}
        logger.info(f"  ➜ 从本地数据库 media_metadata 表中获取到 {len(db_tmdb_ids)} 个【仍在库中】的媒体项。")

        if processor.is_stop_requested():
            logger.info("任务在获取本地数据库媒体项后被中止。")
            return

        # --- 核心逻辑修改 ---
        ids_to_process: set
        # items_to_delete_tmdb_ids 仍然是那些在 DB 中标记为 TRUE 但在 Emby 中已不存在的
        items_to_delete_tmdb_ids = db_tmdb_ids - emby_tmdb_ids
        
        if force_full_update:
            logger.info("  ➜ 深度同步模式：将处理 Emby 中的所有项目。")
            ids_to_process = emby_tmdb_ids
            logger.info(f"  ➜ 计算差异完成：处理 {len(ids_to_process)} 项, 软删除 {len(items_to_delete_tmdb_ids)} 项。")
        else:
            logger.info("  ➜ 快速同步模式：仅处理 Emby 中新增的项目。")
            # 新增的项目是 Emby 有，但 DB 中没有标记为 TRUE 的项目
            # 注意：这里需要考虑那些在 DB 中 in_library=FALSE 的项目，它们也应该被“处理”
            # 所以 ids_to_process 应该是 Emby 有，但 DB 中 in_library=TRUE 的项目没有的
            # 最简单的做法是，先找出所有 Emby 中的项目，然后过滤掉那些在 DB 中 in_library=TRUE 的
            
            # 获取所有在 media_metadata 中，但 in_library=FALSE 的 tmdb_id
            db_soft_deleted_tmdb_ids = set()
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT tmdb_id FROM media_metadata WHERE in_library = FALSE")
                db_soft_deleted_tmdb_ids = {row["tmdb_id"] for row in cursor.fetchall()}

            # 需要处理的项目 = (Emby中所有项目) - (DB中in_library=TRUE的项目)
            # 这会包含 Emby 中新增的，以及 Emby 中重新入库的（之前在DB中in_library=FALSE的）
            ids_to_process = emby_tmdb_ids - db_tmdb_ids
            
            logger.info(f"  ➜ 计算差异完成：新增/恢复 {len(ids_to_process)} 项, 软删除 {len(items_to_delete_tmdb_ids)} 项。")

        if items_to_delete_tmdb_ids:
            logger.info(f"  ➜ 正在从数据库中软删除 {len(items_to_delete_tmdb_ids)} 个已不存在的媒体项...")
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                ids_to_delete_list = list(items_to_delete_tmdb_ids)
                for i in range(0, len(ids_to_delete_list), 500):
                    if processor.is_stop_requested():
                        logger.info("任务在软删除冗余数据时被中止。")
                        break
                    batch_ids = ids_to_delete_list[i:i+500]
                    # ### 修改点：将 DELETE 改为 UPDATE，设置 in_library = FALSE 和 emby_item_id = NULL ###
                    sql = "UPDATE media_metadata SET in_library = FALSE, emby_item_id = NULL WHERE tmdb_id = ANY(%s)"
                    cursor.execute(sql, (batch_ids,))
                conn.commit()
            logger.info("  ➜ 冗余数据清理完成。")

        if processor.is_stop_requested():
            logger.info("任务在冗余数据清理后被中止。")
            return

        items_to_process = [emby_items_map[tmdb_id] for tmdb_id in ids_to_process]
        
        total_to_process = len(items_to_process)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "数据库已是最新，无需同步。")
            return

        logger.info(f"  ➜ 总共需要处理 {total_to_process} 项，将分 { (total_to_process + batch_size - 1) // batch_size } 个批次。")

        # ======================================================================
        # 步骤 2: 分批循环处理需要新增/更新的媒体项
        # ======================================================================
        
        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested():
                logger.info("任务在批次处理前被中止。")
                break

            batch_items = items_to_process[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (total_to_process + batch_size - 1) // batch_size
            
            logger.info(f"--- 开始处理批次 {batch_number}/{total_batches} (包含 {len(batch_items)} 个项目) ---")
            task_manager.update_status_from_thread(
                10 + int((processed_count / total_to_process) * 90), 
                f"处理批次 {batch_number}/{total_batches}..."
            )

            if processor.is_stop_requested():
                logger.info("任务在演员数据补充后被中止。")
                break

            logger.info(f"  ➜ 开始从Tmdb补充导演/国家数据...")
            tmdb_details_map = {}
            def fetch_tmdb_details(item):
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                item_type = item.get("Type")
                if not tmdb_id: return None, None
                details = None
                if item_type == 'Movie':
                    details = tmdb_handler.get_movie_details(tmdb_id, processor.tmdb_api_key)
                elif item_type == 'Series':
                    details = tmdb_handler.get_tv_details(tmdb_id, processor.tmdb_api_key)
                return tmdb_id, details

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_tmdb_id = {executor.submit(fetch_tmdb_details, item): item.get("ProviderIds", {}).get("Tmdb") for item in batch_items}
                for future in concurrent.futures.as_completed(future_to_tmdb_id):
                    if processor.is_stop_requested():
                        logger.info("任务在并发获取 TMDb 详情时被中止。")
                        break
                    tmdb_id, details = future.result()
                    if tmdb_id and details:
                        tmdb_details_map[tmdb_id] = details
            
            if processor.is_stop_requested():
                logger.info("任务在 TMDb 详情获取后被中止。")
                break

            metadata_batch = []
            for item in batch_items:
                if processor.is_stop_requested():
                    logger.info("任务在处理单个媒体项时被中止。")
                    break
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not tmdb_id: continue

                full_details_emby = item
                tmdb_details = tmdb_details_map.get(tmdb_id)

                directors, countries = [], []
                if tmdb_details:
                    item_type = full_details_emby.get("Type")
                    if item_type == 'Movie':
                        credits_data = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                        if credits_data:
                            directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        countries = translate_country_list([c['name'] for c in tmdb_details.get('production_countries', [])])
                    elif item_type == 'Series':
                        credits_data = tmdb_details.get("credits", {})
                        if credits_data:
                            directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        if not directors: directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                        countries = translate_country_list(tmdb_details.get('origin_country', []))

                studios = [s['Name'] for s in full_details_emby.get('Studios', []) if s.get('Name')]
                tags = [tag['Name'] for tag in full_details_emby.get('TagItems', []) if tag.get('Name')]
                
                # ★★★ 修复 1/2: 修正日期处理逻辑 ★★★
                # 如果日期字符串存在，则取 'T' 之前的部分；如果不存在，则直接为 None
                premiere_date_str = full_details_emby.get('PremiereDate')
                release_date = premiere_date_str.split('T')[0] if premiere_date_str else None
                
                date_created_str = full_details_emby.get('DateCreated')
                date_added = date_created_str.split('T')[0] if date_created_str else None

                official_rating = full_details_emby.get('OfficialRating') # 获取原始分级，可能为 None
                unified_rating = get_unified_rating(official_rating)    # 即使 official_rating 是 None，函数也能处理

                metadata_to_save = {
                    "tmdb_id": tmdb_id,
                    "emby_item_id": full_details_emby.get('Id'),
                    "item_type": full_details_emby.get("Type"),
                    "title": full_details_emby.get('Name'), "original_title": full_details_emby.get('OriginalTitle'),
                    "release_year": full_details_emby.get('ProductionYear'), "rating": full_details_emby.get('CommunityRating'),
                    "official_rating": official_rating,
                    "unified_rating": unified_rating,
                    "release_date": release_date,
                    "date_added": date_added,
                    "genres_json": json.dumps(full_details_emby.get('Genres', []), ensure_ascii=False),
                    "directors_json": json.dumps(directors, ensure_ascii=False),
                    "studios_json": json.dumps(studios, ensure_ascii=False),
                    "countries_json": json.dumps(countries, ensure_ascii=False),
                    "tags_json": json.dumps(tags, ensure_ascii=False),
                    "in_library": True
                }
                if metadata_to_save["item_type"] == "Series":
                    series_id = metadata_to_save["emby_item_id"]
                    children = emby_handler.get_series_children(
                        series_id=series_id,
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id,
                        include_item_types="Season,Episode",
                        fields="Id,Type,ParentIndexNumber,IndexNumber" # 请求更丰富的字段
                    )
                    
                    children_details = []
                    if children is not None:
                        for child in children:
                            child_type = child.get("Type")
                            detail = {
                                "Id": child.get("Id"),
                                "Type": child_type
                            }
                            if child_type == "Season":
                                detail["SeasonNumber"] = child.get("IndexNumber")
                            elif child_type == "Episode":
                                detail["SeasonNumber"] = child.get("ParentIndexNumber")
                                detail["EpisodeNumber"] = child.get("IndexNumber")
                            
                            children_details.append(detail)
                    else:
                        logger.warning(f"  ➜ 无法获取剧集 '{metadata_to_save['title']}' (ID: {series_id}) 的子项目详情，将存入空列表。")
                    
                    metadata_to_save["emby_children_details_json"] = json.dumps(children_details, ensure_ascii=False)
                metadata_batch.append(metadata_to_save)

            if processor.is_stop_requested():
                logger.info("任务在构建元数据批次后被中止。")
                break

            if metadata_batch:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # ★★★ 修复 2/2: 改进事务处理逻辑 ★★★
                    # 开启一个总事务
                    cursor.execute("BEGIN;")
                    for idx, metadata in enumerate(metadata_batch):
                        if processor.is_stop_requested():
                            logger.info("任务在数据库写入循环中被中止。")
                            break
                        savepoint_name = f"sp_{idx}"
                        try:
                            # 为每个条目创建一个保存点
                            cursor.execute(f"SAVEPOINT {savepoint_name};")
                            
                            columns = list(metadata.keys())
                            columns_str = ', '.join(columns)
                            placeholders_str = ', '.join(['%s'] * len(columns))
                            
                            update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns]
                            update_clauses.append("last_synced_at = EXCLUDED.last_synced_at")
                            update_str = ', '.join(update_clauses)

                            sql = f"""
                                INSERT INTO media_metadata ({columns_str}, last_synced_at)
                                VALUES ({placeholders_str}, %s)
                                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update_str}
                            """
                            sync_time = datetime.now(timezone.utc).isoformat()
                            cursor.execute(sql, tuple(metadata.values()) + (sync_time,))
                        except psycopg2.Error as e:
                            # 如果发生错误，记录它，并回滚到这个条目之前的状态
                            logger.error(f"写入 TMDB ID {metadata.get('tmdb_id')} 的元数据时发生数据库错误: {e}")
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                    
                    # 提交整个事务（所有成功的条目）
                    conn.commit()
                logger.info(f"--- 批次 {batch_number}/{total_batches} 已成功写入数据库。---")
            
            processed_count += len(batch_items)

        final_message = f"同步完成！本次处理 {processed_count}/{total_to_process} 项, 软删除 {len(items_to_delete_tmdb_ids)} 项。"
        if processor.is_stop_requested():
            final_message = "任务已中止，部分数据可能未处理。"
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_apply_main_cast_to_episodes(processor, series_id: str, episode_ids: list):
    """
    【V2 - 文件中心化重构版】
    轻量级任务：当剧集追更新增分集时，将主项目的完美演员表注入到新分集的 override 元数据文件中。
    此任务不再读写 Emby API，而是委托核心处理器的 sync_single_item_assets 方法执行精准的文件同步操作。
    """
    try:
        if not episode_ids:
            logger.info(f"  ➜ 剧集 {series_id} 追更任务跳过：未提供需要更新的分集ID。")
            return
        
        series_details_for_log = emby_handler.get_emby_item_details(series_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, fields="Name,ProviderIds")
        series_name = series_details_for_log.get("Name", f"ID:{series_id}") if series_details_for_log else f"ID:{series_id}"

        logger.info(f"  ➜ 追更任务启动：准备为剧集 《{series_name}》 的 {len(episode_ids)} 个新分集同步元数据...")

        processor.sync_single_item_assets(
            item_id=series_id,
            update_description=f"追更新增 {len(episode_ids)} 个分集",
            sync_timestamp_iso=datetime.now(timezone.utc).isoformat(),
            episode_ids_to_sync=episode_ids
        )

        logger.info(f"  ➜ 处理完成，正在通知 Emby 刷新...")
        emby_handler.refresh_emby_item_metadata(
            item_emby_id=series_id,
            emby_server_url=processor.emby_url,
            emby_api_key=processor.emby_api_key,
            user_id_for_ops=processor.emby_user_id,
            replace_all_metadata_param=True,
            item_name_for_log=series_name
        )

        # 步骤 3: 更新父剧集在元数据缓存中的 last_synced_at 时间戳 (这个逻辑可以保留)
        if series_details_for_log:
            tmdb_id = series_details_for_log.get("ProviderIds", {}).get("Tmdb")
            if tmdb_id:
                try:
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "UPDATE media_metadata SET last_synced_at = %s WHERE tmdb_id = %s AND item_type = 'Series'",
                                (datetime.now(timezone.utc), tmdb_id)
                            )
                except Exception as db_e:
                    logger.error(f"  ➜ 更新剧集《{series_name}》的时间戳时发生数据库错误: {db_e}", exc_info=True)

    except Exception as e:
        logger.error(f"  ➜ 为剧集 {series_id} 的新分集应用主演员表时发生错误: {e}", exc_info=True)
        raise