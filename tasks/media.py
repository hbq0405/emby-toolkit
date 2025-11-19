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
import handler.tmdb as tmdb
import handler.emby as emby
import handler.telegram as telegram
from database import connection
from utils import translate_country_list, get_unified_rating

logger = logging.getLogger(__name__)

# ★★★ 中文化角色名 ★★★
def task_role_translation(processor, force_full_update: bool = False):
    """
    根据传入的 force_full_update 参数，决定是执行标准扫描还是深度更新。
    """
    # 1. 根据参数决定日志信息
    if force_full_update:
        logger.info("  ➜ 即将执行深度模式，将处理所有媒体项并从TMDb获取最新数据...")
    else:
        logger.info("  ➜ 即将执行快速模式，将跳过已处理项...")


    # 3. 调用核心处理函数，并将 force_full_update 参数透传下去
    processor.process_full_library(
        update_status_callback=task_manager.update_status_from_thread,
        force_full_update=force_full_update 
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
        item_details = emby.get_emby_item_details(
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
        item_details = emby.get_emby_item_details(
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
            force_full_update=True
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
    【V6 - 适配新表结构 - 层级同步版】
    - 完全适配新的 media_metadata 表结构，支持 Movie, Series, Season, Episode 作为独立记录。
    - 同步剧集时，会将其所有在库的季、集作为单独的记录插入/更新到数据库。
    - 软删除逻辑升级：当一个剧集被删除时，会将其自身及其所有关联的季、集记录的 in_library 状态都更新为 FALSE。
    - 健壮性提升：为无法从 TMDB 找到匹配的季/集，会生成一个稳定的、可预测的 tmdb_id (例如 'series_tmdb_id-S01')。
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    try:
        def _parse_asset_details(item: dict) -> dict:
            """
            【V4 - 剧集处理最终版】
            - 如果是电影，直接分析。
            - 如果是剧集，则获取其第一季第一集作为技术代表进行分析。
            """
            from .helpers import analyze_media_asset

            item_to_analyze = None
            
            if item.get('Type') == 'Movie':
                # 对于电影，直接使用其自身信息
                item_to_analyze = item
            elif item.get('Type') == 'Series':
                # 对于剧集，调用新函数获取“代表集”的完整信息
                logger.debug(f"  ➜ 检测到剧集《{item.get('Name')}》，正在为其查找技术代表集...")
                item_to_analyze = emby.get_series_representative_episode(
                    series_id=item.get('Id'),
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
            
            # 如果最终没有可供分析的对象（比如剧集没有分集），则返回一个空结果
            if not item_to_analyze:
                logger.warning(f"  ➜ 无法为媒体项《{item.get('Name')}》(ID: {item.get('Id')}) 找到可供分析的媒体流信息。")
                return {
                    "emby_item_id": item.get("Id"),
                    "path": item.get("Path", ""), # 剧集顶层可能也有路径
                    "size_bytes": None, "container": None, "video_codec": None,
                    "audio_tracks": [], "subtitles": [],
                    "resolution_display": "Unknown", "quality_display": "Unknown",
                    "effect_display": ["SDR"]
                }

            # --- 后续逻辑与之前完全相同，只是分析的对象变成了 item_to_analyze ---

            # 1. 提取基础物理信息
            asset = {
                "emby_item_id": item.get("Id"), # ★ 注意：ID 仍然使用顶层媒体项的ID
                "path": item.get("Path", ""),   # ★ 路径也使用顶层的
                "size_bytes": item_to_analyze.get("Size"),
                "container": item_to_analyze.get("Container"),
                "video_codec": None,
                "audio_tracks": [],
                "subtitles": []
            }

            # 2. 填充音视频流信息
            media_streams = item_to_analyze.get("MediaStreams", [])
            for stream in media_streams:
                stream_type = stream.get("Type")
                if stream_type == "Video":
                    asset["video_codec"] = stream.get("Codec")
                elif stream_type == "Audio":
                    asset["audio_tracks"].append({
                        "language": stream.get("Language"), "codec": stream.get("Codec"),
                        "channels": stream.get("Channels"), "display_title": stream.get("DisplayTitle")
                    })
                elif stream_type == "Subtitle":
                    asset["subtitles"].append({
                        "language": stream.get("Language"), "display_title": stream.get("DisplayTitle")
                    })
            
            # 3. 调用权威分析引擎
            analysis_data = analyze_media_asset(item_to_analyze)
            
            # 4. 将分析结果合并
            asset.update(analysis_data)
            
            return asset
        # ======================================================================
        # 步骤 1: 计算差异 (逻辑与之前类似，但删除操作更强大)
        # ======================================================================
        task_manager.update_status_from_thread(0, f"阶段1/2: 计算媒体库差异 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        # ★★★ 改动点 1: 请求更丰富的字段，为子项目处理做准备 ★★★
        emby_items_index = emby.get_all_library_versions( # <--- 只修改这里的函数名
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            fields="ProviderIds,Type,DateCreated,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,Tags,DateModified,OfficialRating,ProductionYear,Path,PrimaryImageAspectRatio,Overview,MediaStreams,Container,Size"
        ) or []
        
        from collections import defaultdict

        # 创建一个字典，键是 tmdb_id，值是包含该 tmdb_id 所有版本Item的列表
        emby_items_map = defaultdict(list)
        for item in emby_items_index:
            if tmdb_id := item.get("ProviderIds", {}).get("Tmdb"):
                emby_items_map[tmdb_id].append(item)

        emby_tmdb_ids = set(emby_items_map.keys())
        logger.info(f"  ➜ 从 Emby 获取到 {len(emby_tmdb_ids)} 个有效的顶层媒体项 (电影/剧集)。")

        if processor.is_stop_requested(): return

        # 查询数据库中所有在库的顶层媒体项
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata WHERE in_library = TRUE AND item_type IN ('Movie', 'Series')")
            db_tmdb_ids = {row["tmdb_id"] for row in cursor.fetchall()}
        logger.info(f"  ➜ 从本地数据库获取到 {len(db_tmdb_ids)} 个【仍在库中】的顶层媒体项。")

        if processor.is_stop_requested(): return

        # --- 计算差异 ---
        items_to_delete_tmdb_ids = db_tmdb_ids - emby_tmdb_ids
        
        if force_full_update:
            ids_to_process = emby_tmdb_ids
            logger.info(f"  ➜ 深度同步：将处理 {len(ids_to_process)} 项, 标记离线 {len(items_to_delete_tmdb_ids)} 项。")
        else:
            ids_to_process = emby_tmdb_ids - db_tmdb_ids
            logger.info(f"  ➜ 快速同步：新增/恢复 {len(ids_to_process)} 项, 标记离线 {len(items_to_delete_tmdb_ids)} 项。")

        # ★★★ 改动点 2: 软删除逻辑升级，会一并处理剧集的所有子项目 ★★★
        if items_to_delete_tmdb_ids:
            logger.info(f"  ➜ 正在标记 {len(items_to_delete_tmdb_ids)} 个已不存在的媒体项及其子项为离线...")
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                ids_to_delete_list = list(items_to_delete_tmdb_ids)
                # 使用 ANY 操作符可以高效处理
                # 不仅更新顶层项目，还更新所有以它为父项目的子项目
                sql = """
                    UPDATE media_metadata 
                    SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb
                    WHERE tmdb_id = ANY(%s) OR parent_series_tmdb_id = ANY(%s)
                """
                cursor.execute(sql, (ids_to_delete_list, ids_to_delete_list))
                conn.commit()
            logger.info("  ➜ 离线项目标记完成。")

        if processor.is_stop_requested(): return

        items_to_process = [emby_items_map[tmdb_id] for tmdb_id in ids_to_process]
        total_to_process = len(items_to_process)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "数据库已是最新，无需同步。")
            return

        logger.info(f"  ➜ 总共需要处理 {total_to_process} 个媒体组 (TMDB ID)。")

        # ======================================================================
        # 步骤 2: 分批循环处理 (已完全修正)
        # ======================================================================
        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested(): break

            batch_item_groups = items_to_process[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            
            logger.info(f"--- 开始处理批次 {batch_number} (包含 {len(batch_item_groups)} 个媒体组) ---")
            task_manager.update_status_from_thread(
                10 + int((processed_count / total_to_process) * 90), 
                f"处理批次 {batch_number}..."
            )

            logger.info(f"  ➜ 开始从Tmdb补充导演/国家数据...")
            tmdb_details_map = {}
            
            # 修正后的 fetch_tmdb_details
            def fetch_tmdb_details(item_group):
                # 使用组里的第一个版本作为代表来获取信息
                item = item_group[0]
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                item_type = item.get("Type")
                if not tmdb_id: return None, None
                details = None
                if item_type == 'Movie':
                    details = tmdb.get_movie_details(tmdb_id, processor.tmdb_api_key)
                elif item_type == 'Series':
                    details = tmdb.get_tv_details(tmdb_id, processor.tmdb_api_key)
                return tmdb_id, details

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 修正并发任务的创建
                future_to_tmdb_id = {
                    executor.submit(fetch_tmdb_details, item_group): item_group[0].get("ProviderIds", {}).get("Tmdb")
                    for item_group in batch_item_groups
                }
                for future in concurrent.futures.as_completed(future_to_tmdb_id):
                    if processor.is_stop_requested(): break
                    tmdb_id, details = future.result()
                    if tmdb_id and details:
                        tmdb_details_map[tmdb_id] = details
            
            if processor.is_stop_requested(): break

            metadata_batch = []
            for item_group in batch_item_groups:
                if processor.is_stop_requested(): break
                
                # 使用组里的第一个版本作为元数据代表
                item = item_group[0]
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not tmdb_id: continue
                item_type = item.get("Type")

                tmdb_details = tmdb_details_map.get(tmdb_id)

                premiere_date_str = item.get('PremiereDate')
                release_date = premiere_date_str.split('T')[0] if premiere_date_str else None
                sub_status_to_set = "NONE" if item_type == "Movie" else None
                asset_details_list = [_parse_asset_details(version_item) for version_item in item_group]
                top_level_record = {
                    "tmdb_id": tmdb_id,
                    "item_type": item.get("Type"),
                    "title": item.get('Name'),
                    "original_title": item.get('OriginalTitle'),
                    "release_year": item.get('ProductionYear'),
                    "rating": item.get('CommunityRating'),
                    "overview": item.get('Overview'),
                    "release_date": release_date,
                    "date_added": item.get('DateCreated'),
                    "genres_json": json.dumps(item.get('Genres', []), ensure_ascii=False),
                    "in_library": True,
                    "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "subscription_status": sub_status_to_set,
                    "ignore_reason": None,
                    "emby_item_ids_json": json.dumps([v.get('Id') for v in item_group if v.get('Id')], ensure_ascii=False),
                    "official_rating": item.get('OfficialRating'),
                    "unified_rating": get_unified_rating(item.get('OfficialRating'))
                }

                # --- 步骤 B: 如果成功获取到TMDb详情，就用它来“增强”和“覆盖”保底记录 ---
                if tmdb_details:
                    # 安全地覆盖或补充字段
                    top_level_record['overview'] = tmdb_details.get('overview') or item.get('Overview')
                    top_level_record['poster_path'] = tmdb_details.get('poster_path')
                    top_level_record['studios_json'] = json.dumps([s['name'] for s in tmdb_details.get('production_companies', [])], ensure_ascii=False)

                    # 计算导演、国家、关键词
                    directors, countries, keywords = [], [], []
                    item_type = item.get("Type")
                    if item_type == 'Movie':
                        credits_data = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                        directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        country_codes = [c.get('iso_3166_1') for c in tmdb_details.get('production_countries', [])]
                        countries = translate_country_list(country_codes)
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('keywords', []) if isinstance(keywords_data, dict) else []
                        keywords = [k['name'] for k in keyword_list if k.get('name')]
                    elif item_type == 'Series':
                        directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                        countries = translate_country_list(tmdb_details.get('origin_country', []))
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('results', []) if isinstance(keywords_data, dict) else []
                        keywords = [k['name'] for k in keyword_list if k.get('name')]
                    
                    top_level_record['directors_json'] = json.dumps(directors, ensure_ascii=False)
                    top_level_record['countries_json'] = json.dumps(countries, ensure_ascii=False)
                    top_level_record['keywords_json'] = json.dumps(keywords, ensure_ascii=False)
                else:
                    # --- 步骤 C: 如果没有TMDb详情，为那些必须从TMDb获取的字段提供安全的空值 ---
                    logger.warning(f"  ➜ 未能从 TMDb 获取到 TMDB ID: {tmdb_id} ('{item.get('Name')}') 的详情，将仅使用 Emby 元数据。")
                    top_level_record['poster_path'] = None
                    top_level_record['studios_json'] = json.dumps([s['Name'] for s in item.get('Studios', [])], ensure_ascii=False) # 回退到Emby的制片厂
                    top_level_record['directors_json'] = '[]'
                    top_level_record['countries_json'] = '[]'
                    top_level_record['keywords_json'] = '[]'

                metadata_batch.append(top_level_record)

                # --- 步骤 D: 处理剧集子项目 (已集成子项离线标记逻辑) ---
                if item.get("Type") == "Series":
                    series_id = item.get('Id')
                    series_tmdb_id = tmdb_id
                    
                    # 首先，获取当前 Emby 中实际存在的所有子项
                    children = emby.get_series_children(
                        series_id=series_id, base_url=processor.emby_url, api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id, include_item_types="Season,Episode",
                        fields="Id,Type,ParentIndexNumber,IndexNumber,ProviderIds,Name,PremiereDate,Overview"
                    )

                    # ★★★★★★★★★★★★★★★ 新增的子项离线标记逻辑开始 ★★★★★★★★★★★★★★★
                    # 1. 从 Emby 返回的 children 中提取当前所有子项的 Emby ID
                    current_emby_child_ids = {child.get('Id') for child in children} if children else set()

                    # 2. 从数据库查询该剧集下，目前被标记为 "in_library" 的所有子项 Emby ID
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                SELECT emby_item_ids_json FROM media_metadata
                                WHERE parent_series_tmdb_id = %s AND in_library = TRUE AND item_type IN ('Season', 'Episode')
                            """, (series_tmdb_id,))
                            
                            db_child_ids = set()
                            for row in cursor.fetchall():
                                if row['emby_item_ids_json']:
                                    # emby_item_ids_json 是一个列表，比如 ["12345"]，我们用 update 添加所有元素
                                    db_child_ids.update(row['emby_item_ids_json'])

                            # 3. 计算差异，找出已从 Emby 删除的子项
                            deleted_child_emby_ids = list(db_child_ids - current_emby_child_ids)

                            # 4. 如果有被删除的子项，批量更新数据库
                            if deleted_child_emby_ids:
                                logger.info(f"  ➜ 发现剧集 '{item.get('Name')}' 下有 {len(deleted_child_emby_ids)} 个子项被删除，正在标记为离线...")
                                # 使用 emby_item_ids_json->>0 = ANY(%s) 进行高效批量更新
                                cursor.execute("""
                                    UPDATE media_metadata
                                    SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb
                                    WHERE parent_series_tmdb_id = %s AND emby_item_ids_json->>0 = ANY(%s)
                                """, (series_tmdb_id, deleted_child_emby_ids))
                                conn.commit()
                    # ★★★★★★★★★★★★★★★ 新增的子项离线标记逻辑结束 ★★★★★★★★★★★★★★★

                    # 继续后续的子项元数据同步逻辑...
                    if not children:
                        logger.warning(f"  ➜ 剧集 '{item.get('Name')}' 当前没有任何子项目，跳过新增/更新同步。")
                        continue

                    # 后续的 TMDb 匹配和元数据构建逻辑保持不变
                    tmdb_series_details = tmdb.get_tv_details(series_tmdb_id, processor.tmdb_api_key)
                    
                    tmdb_children_map = {}
                    if tmdb_series_details and 'seasons' in tmdb_series_details:
                        for season_info in tmdb_series_details['seasons']:
                            s_num = season_info.get('season_number')
                            if s_num is None: continue
                            tmdb_children_map[f"S{s_num}"] = season_info
                            # 为了效率，这里可以优化为只在需要时获取，但暂时保持原逻辑
                            tmdb_season_details = tmdb.get_tv_season_details(series_tmdb_id, s_num, processor.tmdb_api_key)
                            if tmdb_season_details and 'episodes' in tmdb_season_details:
                                for episode_info in tmdb_season_details['episodes']:
                                    e_num = episode_info.get('episode_number')
                                    if e_num is None: continue
                                    tmdb_children_map[f"S{s_num}E{e_num}"] = episode_info

                    for child in children:
                        child_type = child.get("Type")
                        child_record = {
                            "in_library": True,
                            "ignore_reason": None, 
                            "emby_item_ids_json": json.dumps([child.get('Id')])
                        }
                        
                        s_num = child.get("ParentIndexNumber") if child_type == "Episode" else child.get("IndexNumber")
                        e_num = child.get("IndexNumber") if child_type == "Episode" else None
                        
                        if s_num is None: continue # 跳过无效的子项

                        lookup_key = f"S{s_num}E{e_num}" if e_num is not None else f"S{s_num}"
                        tmdb_child_info = tmdb_children_map.get(lookup_key)

                        if tmdb_child_info and tmdb_child_info.get('id'):
                            child_record.update({ "tmdb_id": str(tmdb_child_info.get('id')), "title": tmdb_child_info.get('name'), "release_date": tmdb_child_info.get('air_date'), "overview": tmdb_child_info.get('overview'), "poster_path": tmdb_child_info.get('poster_path') })
                        else:
                            child_record.update({ "tmdb_id": f"{series_tmdb_id}-{lookup_key}", "title": child.get('Name'), "overview": child.get('Overview') })

                        child_record.update({ "item_type": child_type, "parent_series_tmdb_id": series_tmdb_id, "season_number": s_num, "episode_number": e_num })
                        metadata_batch.append(child_record)

            if processor.is_stop_requested(): break

            # --- 3. 数据库写入 (逻辑保持不变，但现在会写入更多行) ---
            if metadata_batch:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("BEGIN;")
                    for idx, metadata in enumerate(metadata_batch):
                        if processor.is_stop_requested(): break
                        savepoint_name = f"sp_{idx}"
                        try:
                            cursor.execute(f"SAVEPOINT {savepoint_name};")
                            
                            # 动态生成 SQL 语句
                            columns = [k for k, v in metadata.items() if v is not None]
                            values = [v for v in metadata.values() if v is not None]
                            columns_str = ', '.join(columns)
                            placeholders_str = ', '.join(['%s'] * len(values))
                            
                            # ★★★ 核心修改：为 emby_item_ids_json 生成特殊的合并更新逻辑 ★★★
                            update_clauses = []
                            columns_to_update = [c for c in columns if c not in ('tmdb_id', 'item_type')]

                            for col in columns_to_update:
                                if col in ['subscription_sources_json']:
                                    continue
                                
                                if col == 'emby_item_ids_json':
                                    update_clauses.append("""
                                        emby_item_ids_json = (
                                            SELECT jsonb_agg(DISTINCT elem)
                                            FROM (
                                                SELECT jsonb_array_elements_text(COALESCE(media_metadata.emby_item_ids_json, '[]'::jsonb)) AS elem
                                                UNION ALL
                                                SELECT jsonb_array_elements_text(EXCLUDED.emby_item_ids_json) AS elem
                                            ) AS combined
                                        )
                                    """)
                                else:
                                    # 其他所有字段（包括 in_library），都通过动态方式正常覆盖更新
                                    update_clauses.append(f"{col} = EXCLUDED.{col}")

                            # 只需要在这里重置 ignore_reason 即可
                            update_clauses.append("ignore_reason = NULL")
                            
                            update_str = ', '.join(update_clauses)

                            sql = f"""
                                INSERT INTO media_metadata ({columns_str}, last_synced_at)
                                VALUES ({placeholders_str}, NOW())
                                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update_str}, last_synced_at = NOW()
                            """
                            cursor.execute(sql, tuple(values))
                        except psycopg2.Error as e:
                            logger.error(f"写入 TMDB ID {metadata.get('tmdb_id')} 的元数据时发生数据库错误: {e}")
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                    
                    if not processor.is_stop_requested():
                        conn.commit()
                    else:
                        conn.rollback() # 如果中止，则回滚整个批次
                        logger.info("任务中止，回滚当前数据库批次。")

                logger.info(f"--- 批次 {batch_number} 已成功写入数据库。---")
            
            processed_count += len(batch_item_groups)

        final_message = f"同步完成！本次处理 {processed_count}/{total_to_process} 项, 标记离线 {len(items_to_delete_tmdb_ids)} 项。"
        if processor.is_stop_requested():
            final_message = "任务已中止，部分数据可能未处理。"
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

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
        
        series_details_for_log = emby.get_emby_item_details(series_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, fields="Name,ProviderIds")
        series_name = series_details_for_log.get("Name", f"ID:{series_id}") if series_details_for_log else f"ID:{series_id}"

        logger.info(f"  ➜ 追更任务启动：准备为剧集 《{series_name}》 的 {len(episode_ids)} 个新分集同步元数据...")

        processor.sync_single_item_assets(
            item_id=series_id,
            update_description=f"追更新增 {len(episode_ids)} 个分集",
            sync_timestamp_iso=datetime.now(timezone.utc).isoformat(),
            episode_ids_to_sync=episode_ids
        )

        logger.info(f"  ➜ 处理完成，正在通知 Emby 刷新...")
        emby.refresh_emby_item_metadata(
            item_emby_id=series_id,
            emby_server_url=processor.emby_url,
            emby_api_key=processor.emby_api_key,
            user_id_for_ops=processor.emby_user_id,
            replace_all_metadata_param=True,
            item_name_for_log=series_name
        )

        # TG通知
        if series_details_for_log:
            logger.info(f"  ➜ 正在为《{series_name}》触发追更通知...")
            telegram.send_media_notification(
                item_details=series_details_for_log,
                notification_type='update',
                new_episode_ids=episode_ids
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