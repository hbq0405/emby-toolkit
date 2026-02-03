# tasks/cleanup.py
# 媒体去重与版本管理专属任务模块

import logging
import time
from functools import cmp_to_key
from typing import List, Dict, Any, Optional
from psycopg2 import sql
from collections import defaultdict
import task_manager
import handler.emby as emby
from database import connection, cleanup_db, settings_db, maintenance_db, queries_db
from .media import task_populate_metadata_cache

logger = logging.getLogger(__name__)

# ======================================================================
# 核心逻辑：版本比较与决策
# ======================================================================

def _get_properties_for_comparison(version: Dict) -> Dict:
    """
    从 asset_details_json 的单个版本条目中，提取用于比较的标准化属性。
    包含：特效、分辨率、质量、文件大小、码率、色深、帧率、时长、字幕语言数量。
    """
    if not version or not isinstance(version, dict):
        return {
            'id': None, 'quality': 'unknown', 'resolution': 'unknown', 'effect': 'sdr', 'filesize': 0,
            'video_bitrate_mbps': 0, 'bit_depth': 8, 'frame_rate': 0, 'runtime_minutes': 0,
            'codec': 'unknown', 'subtitle_count': 0, 'subtitle_languages': []
        }

    # ★★★ 核心修改：直接读取数据库中已有的分析结果，不再重复造轮子 ★★★
    
    # 1. 获取字幕语言列表 (例如 ['chi', 'eng'])
    # parse_full_asset_details 已经帮我们生成了这个字段
    subtitle_langs = version.get('subtitle_languages_raw', [])
    
    # 2. 获取字幕数量
    # 优先使用 raw 列表的长度，如果列表为空但有 display 字符串，尝试解析一下（兜底）
    subtitle_count = len(subtitle_langs)
    if subtitle_count == 0:
        # 尝试从原始 subtitles 列表获取长度 (如果存在)
        raw_subs = version.get('subtitles', [])
        if raw_subs:
            subtitle_count = len(raw_subs)

    # 3. 获取其他标准化属性 (直接读，或者做简单的归一化)
    quality = str(version.get("quality_display", "未知")).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl")
    resolution = version.get("resolution_display", "未知")
    
    # 特效处理：数据库里存的是 display 格式 (如 "DoVi_P8")，我们需要转成小写 (如 "dovi_p8") 以便比较
    effect_raw = version.get("effect_display", "SDR")
    # 兼容旧数据可能是列表的情况
    if isinstance(effect_raw, list):
        effect_raw = effect_raw[0] if effect_raw else "SDR"
    effect = str(effect_raw).lower()

    codec = version.get("codec_display", "未知")

    raw_id = version.get("emby_item_id")
    int_id = int(raw_id) if raw_id and str(raw_id).isdigit() else 0

    return {
        "id": version.get("emby_item_id"),
        "path": version.get("path"),
        
        "quality": quality,
        "resolution": resolution,
        "effect": effect,
        "codec": codec,
        
        "filesize": version.get("size_bytes", 0),
        "video_bitrate_mbps": version.get("video_bitrate_mbps") or 0,
        "bit_depth": version.get("bit_depth") or 8,
        "frame_rate": version.get("frame_rate") or 0,
        "runtime_minutes": version.get("runtime_minutes") or 0,
        "date_added": version.get("date_added_to_library") or "",
        "int_id": int_id,
        "subtitle_count": subtitle_count,
        "subtitle_languages": subtitle_langs
    }

def _compare_versions(v1: Dict[str, Any], v2: Dict[str, Any], rules: List[Dict[str, Any]]) -> int:
    """
    比较两个版本 v1 和 v2。
    返回: 1 (v1优), -1 (v2优), 0 (相当)
    """
    for rule in rules:
        if not rule.get('enabled'):
            continue
            
        rule_type = rule.get('id')
        # 获取偏好设置，默认为 'desc' (降序，即大/高优先)
        preference = rule.get('priority', 'desc')
        
        # --- 1. 按码率 (Bitrate) ---
        if rule_type == 'bitrate':
            br1 = v1.get('video_bitrate_mbps') or 0
            br2 = v2.get('video_bitrate_mbps') or 0
            if abs(br1 - br2) > 1.0: # 1Mbps 容差
                if preference == 'asc':
                    return 1 if br1 < br2 else -1 # 保留低码率
                else:
                    return 1 if br1 > br2 else -1 # 保留高码率 (默认)

        # --- 2. 按色深 (Bit Depth) ---
        elif rule_type == 'bit_depth':
            bd1 = v1.get('bit_depth') or 8
            bd2 = v2.get('bit_depth') or 8
            if bd1 != bd2:
                if preference == 'asc':
                    return 1 if bd1 < bd2 else -1 # 保留低色深 (8bit)
                else:
                    return 1 if bd1 > bd2 else -1 # 保留高色深 (10bit)

        # --- 3. 按帧率 (Frame Rate) ---
        elif rule_type == 'frame_rate':
            fr1 = v1.get('frame_rate') or 0
            fr2 = v2.get('frame_rate') or 0
            if abs(fr1 - fr2) > 2.0: # 2fps 容差
                if preference == 'asc':
                    return 1 if fr1 < fr2 else -1 # 保留低帧率 (24fps)
                else:
                    return 1 if fr1 > fr2 else -1 # 保留高帧率 (60fps)

        # --- 4. 按时长 (Runtime) ---
        elif rule_type == 'runtime':
            rt1 = v1.get('runtime_minutes') or 0
            rt2 = v2.get('runtime_minutes') or 0
            if abs(rt1 - rt2) > 2: # 2分钟容差
                if preference == 'asc':
                    return 1 if rt1 < rt2 else -1 # 保留短时长
                else:
                    return 1 if rt1 > rt2 else -1 # 保留长时长

        # --- 5. 按文件大小 ---
        elif rule_type == 'filesize':
            fs1 = v1.get('filesize') or 0
            fs2 = v2.get('filesize') or 0
            # 文件大小通常差异明显，直接比
            if fs1 != fs2:
                if preference == 'asc':
                    return 1 if fs1 < fs2 else -1 # 保留小体积
                else:
                    return 1 if fs1 > fs2 else -1 # 保留大体积

        # --- 6. 按列表优先级 (分辨率, 质量, 特效, 编码) ---
        elif rule_type in ['resolution', 'quality', 'effect', 'codec']:
            val1 = v1.get(rule_type)
            val2 = v2.get(rule_type)
            priority_list = rule.get("priority", [])
            
            # 标准化处理
            if rule_type == "resolution":
                def normalize_res(res):
                    s = str(res).lower()
                    if s == '2160p': return '4k'
                    return s
                priority_list = [normalize_res(p) for p in priority_list]
                val1 = normalize_res(val1)
                val2 = normalize_res(val2)

            elif rule_type == "quality":
                priority_list = [str(p).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl") for p in priority_list]
            
            elif rule_type == "effect":
                priority_list = [str(p).lower().replace(" ", "_") for p in priority_list]

            elif rule_type == "codec":
                def normalize_codec(c):
                    s = str(c).upper()
                    if s in ['H265', 'X265']: return 'HEVC'
                    if s in ['H264', 'X264', 'AVC']: return 'H.264'
                    return s
                priority_list = [normalize_codec(p) for p in priority_list]
                val1 = normalize_codec(val1)
                val2 = normalize_codec(val2)

            try:
                idx1 = priority_list.index(val1) if val1 in priority_list else 999
                idx2 = priority_list.index(val2) if val2 in priority_list else 999
                if idx1 != idx2:
                    return 1 if idx1 < idx2 else -1 # 索引越小优先级越高
            except (ValueError, TypeError):
                continue
        
        # --- 7. ★★★ 新增：按字幕 (Subtitle) ★★★ ---
        elif rule_type == 'subtitle':
            # 优先比较是否有中文字幕
            has_chi1 = 'chi' in v1.get('subtitle_languages', []) or 'yue' in v1.get('subtitle_languages', [])
            has_chi2 = 'chi' in v2.get('subtitle_languages', []) or 'yue' in v2.get('subtitle_languages', [])
            
            if has_chi1 != has_chi2:
                # 有中文的优先
                return 1 if has_chi1 else -1
            return 0

        # --- 8. 按入库时间 (Date Added / ID) ---
        elif rule_type == 'date_added':
            # 1. 优先比较日期字符串 (ISO格式字符串可以直接比较大小)
            d1 = v1.get('date_added')
            d2 = v2.get('date_added')
            
            if d1 and d2 and d1 != d2:
                if preference == 'asc':
                    return 1 if d1 < d2 else -1 # 保留最早入库 (Oldest)
                else:
                    return 1 if d1 > d2 else -1 # 保留最新入库 (Newest)
            
            # 2. 如果日期相同或无效，使用 ID 进行兜底比较
            id1 = v1.get('int_id')
            id2 = v2.get('int_id')
            
            if id1 != id2:
                if preference == 'asc':
                    return 1 if id1 < id2 else -1 # 保留ID小的 (最早)
                else:
                    return 1 if id1 > id2 else -1 # 保留ID大的 (最新)

    return 0

def _determine_best_version_by_rules(versions: List[Dict[str, Any]]) -> Optional[str]:
    """
    根据规则决定最佳版本，返回最佳版本的 ID。
    """
    # 获取规则，如果没有则使用默认全集
    rules = settings_db.get_setting('media_cleanup_rules')
    if not rules:
        rules = [
            {"id": "runtime", "enabled": True}, # 时长优先
            {"id": "effect", "enabled": True, "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]},
            {"id": "resolution", "enabled": True, "priority": ["4k", "1080p", "720p", "480p"]},
            {"id": "bit_depth", "enabled": True}, # 色深
            {"id": "bitrate", "enabled": True},   # 码率
            {"id": "codec", "enabled": True, "priority": ["AV1", "HEVC", "H.264", "VP9"]},
            {"id": "quality", "enabled": True, "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            # ★★★ 新增默认规则：字幕 ★★★
            {"id": "subtitle", "enabled": True, "priority": "desc"}, # 字幕多的/有中文的优先
            {"id": "frame_rate", "enabled": False}, # 帧率默认关闭
            {"id": "filesize", "enabled": True},
            {"id": "date_added", "enabled": True, "priority": "asc"}
        ]

    # 提取属性
    version_properties = [_get_properties_for_comparison(v) for v in versions if v]

    # 使用自定义比较函数排序
    # cmp_to_key 需要一个返回负数、0、正数的函数，逻辑与我们的 _compare_versions (1, -1) 相反
    # 我们定义的 _compare_versions: 1 (v1优), -1 (v2优)
    # sort(reverse=True): 大的排前面。所以 v1 优于 v2 时，cmp 应返回 1
    def compare_wrapper(v1, v2):
        return _compare_versions(v1, v2, rules)

    sorted_versions = sorted(version_properties, key=cmp_to_key(compare_wrapper), reverse=True)
    
    return sorted_versions[0]['id'] if sorted_versions else None

# ======================================================================
# 任务函数
# ======================================================================

def task_scan_for_cleanup_issues(processor):
    """
    扫描数据库，生成精简的清理索引。
    """
    task_name = "扫描媒体库重复项"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    # 前置增量同步 
    logger.info("  ➜ [前置操作] 正在执行增量元数据同步，以确保多版本信息已入库...")
    try:
        # 调用 media 模块的同步任务 (增量模式)
        task_populate_metadata_cache(processor, force_full_update=False)
    except Exception as e:
        logger.error(f"  ⚠️ 前置同步失败: {e}，将尝试基于现有数据扫描。", exc_info=True)

    task_manager.update_status_from_thread(0, "正在准备扫描...")

    try:
        library_ids_to_scan = settings_db.get_setting('media_cleanup_library_ids') or []
        keep_one_per_res = settings_db.get_setting('media_cleanup_keep_one_per_res') or False
        
        # ★★★ 核心优化：使用 queries_db.query_virtual_library_items 进行带权限的范围筛选 ★★★
        logger.info(f"  ➜ 正在计算扫描范围 (基于用户 {processor.emby_user_id} 的权限)...")
        
        # 1. 获取允许的电影 (Movie)
        allowed_movies, _ = queries_db.query_virtual_library_items(
            rules=[], 
            logic='AND',
            user_id=processor.emby_user_id, 
            limit=1000000, 
            offset=0,
            item_types=['Movie'], 
            target_library_ids=library_ids_to_scan if library_ids_to_scan else None
        )
        
        # 2. 获取允许的剧集 (Series)
        allowed_series, _ = queries_db.query_virtual_library_items(
            rules=[], 
            logic='AND',
            user_id=processor.emby_user_id, 
            limit=1000000, 
            offset=0,
            item_types=['Series'], 
            target_library_ids=library_ids_to_scan if library_ids_to_scan else None
        )
        
        # 提取 TMDb ID
        allowed_movie_tmdb_ids = [m['tmdb_id'] for m in allowed_movies if m.get('tmdb_id')]
        allowed_series_tmdb_ids = [s['tmdb_id'] for s in allowed_series if s.get('tmdb_id')]
        
        total_scope = len(allowed_movie_tmdb_ids) + len(allowed_series_tmdb_ids)
        logger.info(f"  ➜ 扫描范围确定：{len(allowed_movie_tmdb_ids)} 部电影, {len(allowed_series_tmdb_ids)} 部剧集。")

        if total_scope == 0:
            task_manager.update_status_from_thread(100, "扫描中止：当前用户视角下没有可见的媒体项。")
            return

        # 3. 构建 SQL 查询
        #    逻辑：
        #    - 如果是 Movie，检查其 tmdb_id 是否在 allowed_movie_tmdb_ids 中
        #    - 如果是 Episode，检查其 parent_series_tmdb_id 是否在 allowed_series_tmdb_ids 中
        #    这样就完美继承了 Series 的目录权限
        
        sql_query = sql.SQL("""
            SELECT t.tmdb_id, t.item_type, t.asset_details_json
            FROM media_metadata AS t
            WHERE 
                t.in_library = TRUE 
                AND jsonb_array_length(t.asset_details_json) > 1
                AND (
                    (t.item_type = 'Movie' AND t.tmdb_id = ANY(%(movie_ids)s))
                    OR
                    (t.item_type = 'Episode' AND t.parent_series_tmdb_id = ANY(%(series_ids)s))
                )
        """)
        
        params = {
            'movie_ids': allowed_movie_tmdb_ids,
            'series_ids': allowed_series_tmdb_ids
        }

        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query, params)
                multi_version_items = cursor.fetchall()

        total_items = len(multi_version_items)
        if total_items == 0:
            cleanup_db.clear_pending_cleanup_tasks()
            task_manager.update_status_from_thread(100, "扫描完成：未发现任何多版本媒体。")
            return

        task_manager.update_status_from_thread(10, f"发现 {total_items} 组多版本媒体，开始分析...")
        
        cleanup_index_entries = []
        for i, item in enumerate(multi_version_items):
            progress = 10 + int((i / total_items) * 80)
            # 获取标题用于日志
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (item['tmdb_id'], item['item_type']))
                    title_row = cursor.fetchone()
                    display_title = title_row['title'] if title_row else '未知媒体'
            
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_items}) 正在分析: {display_title}")

            versions_from_db = item['asset_details_json']
            raw_versions = item['asset_details_json']
            unique_versions_map = {}
            for v in raw_versions:
                eid = v.get('emby_item_id')
                if eid:
                    unique_versions_map[eid] = v
            
            versions_from_db = list(unique_versions_map.values())

            # ★★★ 二次检查：去重后如果只剩1个版本，说明是脏数据，直接跳过 ★★★
            if len(versions_from_db) < 2: continue

            # =================================================
            # ★★★ 核心逻辑分叉 ★★★
            # =================================================
            best_id_or_ids = None
            
            if keep_one_per_res:
                # --- 模式 A: 保留每种分辨率的最佳版本 ---
                
                # 1. 按分辨率分组
                res_groups = defaultdict(list)
                for v in versions_from_db:
                    # 获取标准化后的分辨率 (例如 "4K", "1080p")
                    props = _get_properties_for_comparison(v)
                    res_key = props.get('resolution', 'unknown')
                    res_groups[res_key].append(v)
                
                # 2. 在每组内选出最佳
                best_ids_set = set()
                for res, group_versions in res_groups.items():
                    best_in_group = _determine_best_version_by_rules(group_versions)
                    if best_in_group:
                        best_ids_set.add(best_in_group)
                
                # 3. 判断是否需要清理
                # 如果选出的最佳版本数量 等于 总版本数量，说明每个版本都是它那个分辨率的独苗，无需清理
                if len(best_ids_set) == len(versions_from_db):
                    continue 
                
                # 4. 直接传递 Python 列表
                best_id_or_ids = list(best_ids_set)
                
            else:
                # --- 模式 B: 传统模式 (只留一个) ---
                best_id_or_ids = _determine_best_version_by_rules(versions_from_db)

            # 构建前端展示用的精简信息
            versions_for_frontend = []
            for v in versions_from_db:
                props = _get_properties_for_comparison(v)
                versions_for_frontend.append({
                    'id': v.get('emby_item_id'),
                    'path': v.get('path'),
                    'filesize': v.get('size_bytes', 0),
                    'quality': props.get('quality'), # 使用标准化后的
                    'resolution': props.get('resolution'),
                    'effect': props.get('effect'),
                    'video_bitrate_mbps': props.get('video_bitrate_mbps'),
                    'bit_depth': props.get('bit_depth'),
                    'frame_rate': props.get('frame_rate'),
                    'runtime_minutes': props.get('runtime_minutes'),
                    'codec': props.get('codec'),
                    'subtitle_count': props.get('subtitle_count'),
                    'subtitle_languages': props.get('subtitle_languages')
                })

            cleanup_index_entries.append({
                "tmdb_id": item['tmdb_id'], 
                "item_type": item['item_type'],
                "versions_info_json": versions_for_frontend,
                "best_version_json": best_id_or_ids,
            })

        task_manager.update_status_from_thread(90, f"分析完成，正在写入数据库...")

        cleanup_db.clear_pending_cleanup_tasks()
        
        if cleanup_index_entries:
            cleanup_db.batch_upsert_cleanup_index(cleanup_index_entries)

        final_message = f"扫描完成！共发现 {len(cleanup_index_entries)} 组需要清理的多版本媒体。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_execute_cleanup(processor, task_ids: List[int], **kwargs):
    """
    执行指定的一批媒体去重任务。
    """
    if not task_ids:
        task_manager.update_status_from_thread(-1, "任务失败：缺少任务ID")
        return

    task_name = "执行媒体去重"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ★★★ 1. 读取删除延迟配置 ★★★
        delete_delay = settings_db.get_setting('media_cleanup_delete_delay') or 0
        if delete_delay > 0:
            logger.info(f"  ➜ 已启用删除延迟策略，每删除一个文件将等待 {delete_delay} 秒。")

        tasks_to_execute = cleanup_db.get_cleanup_index_by_ids(task_ids)
        total = len(tasks_to_execute)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：未找到指定的清理任务。")
            return

        deleted_count = 0
        processed_task_ids = []
        for i, task in enumerate(tasks_to_execute):
            if processor.is_stop_requested():
                logger.warning("  🚫 任务被用户中止。")
                break
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (task['tmdb_id'], task['item_type']))
                    title_row = cursor.fetchone()
                    item_name = title_row['title'] if title_row else '未知媒体'

            raw_best_val = task['best_version_json']
            safe_ids_set = set()

            if raw_best_val:
                if isinstance(raw_best_val, list):
                    safe_ids_set = set(str(x) for x in raw_best_val)
                else:
                    safe_ids_set.add(str(raw_best_val))

            if not safe_ids_set:
                logger.error(f"  🚫 严重错误：无法确定 '{item_name}' 的保留版本... 跳过。")
                continue

            versions = task['versions_info_json']
            task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在清理: {item_name}")

            for version in versions:
                version_id_to_check = str(version.get('id'))
                
                if version_id_to_check not in safe_ids_set:
                    logger.warning(f"  ➜ 准备删除劣质版本 (ID: {version_id_to_check}): {version.get('path')}")
                    
                    success = emby.delete_item_sy(
                        item_id=version_id_to_check,
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    )
                    if success:
                        deleted_count += 1
                        logger.info(f"  ➜ 成功删除 ID: {version_id_to_check}")
                        
                        try:
                            maintenance_db.cleanup_deleted_media_item(
                                item_id=version_id_to_check,
                                item_name=item_name,
                                item_type=task['item_type']
                            )
                        except Exception as cleanup_e:
                            logger.error(f"  ➜ 善后清理失败: {cleanup_e}", exc_info=True)

                        # ★★★ 2. 执行延迟 (仅在删除成功后) ★★★
                        if delete_delay > 0:
                            logger.debug(f"    ⏳ [防风控] 等待 {delete_delay} 秒...")
                            time.sleep(delete_delay)

                    else:
                        logger.error(f"  ➜ 删除 ID: {version_id_to_check} 失败！")
            
            processed_task_ids.append(task['id'])

        if processed_task_ids:
            cleanup_db.batch_update_cleanup_index_status(processed_task_ids, 'processed')

        final_message = f"清理完成！共处理 {len(processed_task_ids)} 个任务，尝试删除了 {deleted_count} 个多余版本。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")