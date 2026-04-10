# tasks/cleanup.py
# 媒体去重与版本管理专属任务模块

import os
import shutil
import logging
import time
from functools import cmp_to_key
from typing import List, Dict, Any, Optional
from psycopg2 import sql
from collections import defaultdict
import task_manager
import handler.emby as emby
from handler.p115_service import WebhookDeleteBuffer
import config_manager
import constants
from database import connection, cleanup_db, settings_db, maintenance_db, queries_db, media_db

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

    # 1. 获取字幕语言列表 (例如 ['chi', 'eng'])
    subtitle_langs = version.get('subtitle_languages_raw', [])
    
    # 2. 获取字幕数量
    subtitle_count = len(subtitle_langs)
    if subtitle_count == 0:
        raw_subs = version.get('subtitles', [])
        if raw_subs:
            subtitle_count = len(raw_subs)

    # 3. 获取其他标准化属性
    quality = str(version.get("quality_display", "未知")).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl")
    resolution = version.get("resolution_display", "未知")
    
    effect_raw = version.get("effect_display", "SDR")
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

def _compare_versions(v1: Dict[str, Any], v2: Dict[str, Any], rules: List[Dict[str, Any]], item_name: str = "") -> int:
    """
    比较两个版本 v1 和 v2。
    返回: 1 (v1优), -1 (v2优), 0 (相当)
    """
    # 构建用于日志展示的版本简写 (例如: [4K|HEVC|15.2GB])
    def get_desc(v):
        fs_gb = round(v.get('filesize', 0) / (1024**3), 2)
        return f"[{v.get('resolution')}|{v.get('codec')}|{fs_gb}GB]"
    
    v1_desc = get_desc(v1)
    v2_desc = get_desc(v2)

    for rule in rules:
        if not rule.get('enabled'):
            continue
            
        rule_type = rule.get('id')
        preference = rule.get('priority', 'desc')
        result = 0
        reason_detail = ""
        
        # --- 1. 按码率 (Bitrate) ---
        if rule_type == 'bitrate':
            br1 = v1.get('video_bitrate_mbps') or 0
            br2 = v2.get('video_bitrate_mbps') or 0
            if abs(br1 - br2) > 1.0: # 1Mbps 容差
                result = 1 if (br1 < br2 if preference == 'asc' else br1 > br2) else -1
                reason_detail = f"码率 {br1} vs {br2} Mbps"

        # --- 2. 按色深 (Bit Depth) ---
        elif rule_type == 'bit_depth':
            bd1 = v1.get('bit_depth') or 8
            bd2 = v2.get('bit_depth') or 8
            if bd1 != bd2:
                result = 1 if (bd1 < bd2 if preference == 'asc' else bd1 > bd2) else -1
                reason_detail = f"色深 {bd1} vs {bd2} bit"

        # --- 3. 按帧率 (Frame Rate) ---
        elif rule_type == 'frame_rate':
            fr1 = v1.get('frame_rate') or 0
            fr2 = v2.get('frame_rate') or 0
            if abs(fr1 - fr2) > 2.0: # 2fps 容差
                result = 1 if (fr1 < fr2 if preference == 'asc' else fr1 > fr2) else -1
                reason_detail = f"帧率 {fr1} vs {fr2} fps"

        # --- 4. 按时长 (Runtime) ---
        elif rule_type == 'runtime':
            rt1 = v1.get('runtime_minutes') or 0
            rt2 = v2.get('runtime_minutes') or 0
            if abs(rt1 - rt2) > 2: # 2分钟容差
                result = 1 if (rt1 < rt2 if preference == 'asc' else rt1 > rt2) else -1
                reason_detail = f"时长 {rt1} vs {rt2} 分钟"

        # --- 5. 按文件大小 ---
        elif rule_type == 'filesize':
            fs1 = v1.get('filesize') or 0
            fs2 = v2.get('filesize') or 0
            if fs1 != fs2:
                result = 1 if (fs1 < fs2 if preference == 'asc' else fs1 > fs2) else -1
                reason_detail = f"体积 {round(fs1/(1024**3),2)} vs {round(fs2/(1024**3),2)} GB"

        # --- 6. 按列表优先级 (分辨率, 质量, 特效, 编码) ---
        elif rule_type in ['resolution', 'quality', 'effect', 'codec']:
            val1 = v1.get(rule_type)
            val2 = v2.get(rule_type)
            priority_list = rule.get("priority", [])
            
            if rule_type == "resolution":
                def normalize_res(res):
                    s = str(res).lower()
                    if s == '2160p': return '4k'
                    return s
                priority_list = [normalize_res(p) for p in priority_list]
                val1, val2 = normalize_res(val1), normalize_res(val2)

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
                val1, val2 = normalize_codec(val1), normalize_codec(val2)

            try:
                idx1 = priority_list.index(val1) if val1 in priority_list else 999
                idx2 = priority_list.index(val2) if val2 in priority_list else 999
                if idx1 != idx2:
                    result = 1 if idx1 < idx2 else -1
                    reason_detail = f"{val1} vs {val2}"
            except (ValueError, TypeError):
                pass
        
        # --- 7. ★★★ 修复：按字幕 (Subtitle) ★★★ ---
        elif rule_type == 'subtitle':
            # 扩充中文字幕代码库，防止误判
            chi_codes = {'chi', 'zho', 'zh', 'yue', 'chs', 'cht', 'zh-cn', 'zh-tw', 'zh-hk'}
            
            langs1 = [str(l).lower() for l in v1.get('subtitle_languages', [])]
            langs2 = [str(l).lower() for l in v2.get('subtitle_languages', [])]
            
            has_chi1 = any(l in chi_codes for l in langs1)
            has_chi2 = any(l in chi_codes for l in langs2)
            
            if has_chi1 != has_chi2:
                result = 1 if has_chi1 else -1
                reason_detail = f"中字: {'有' if has_chi1 else '无'} vs {'有' if has_chi2 else '无'}"
            # 如果都有中字，或者都没有中字，result 保持为 0，进入平局，交给下一条规则判断！

        # --- 8. 按入库时间 (Date Added / ID) ---
        elif rule_type == 'date_added':
            d1, d2 = v1.get('date_added'), v2.get('date_added')
            if d1 and d2 and d1 != d2:
                result = 1 if (d1 < d2 if preference == 'asc' else d1 > d2) else -1
                reason_detail = f"入库时间 {d1[:10]} vs {d2[:10]}"
            else:
                id1, id2 = v1.get('int_id'), v2.get('int_id')
                if id1 != id2:
                    result = 1 if (id1 < id2 if preference == 'asc' else id1 > id2) else -1
                    reason_detail = f"内部ID {id1} vs {id2}"

        # ★★★ 决斗日志输出 ★★★
        if result != 0:
            winner = v1_desc if result == 1 else v2_desc
            loser = v2_desc if result == 1 else v1_desc
            logger.info(f"  ⚔️ [去重对决] {item_name}: {winner} 击败 {loser} ➜ 命中策略 [{rule_type}] ({reason_detail})")
            return result

    return 0

def _determine_best_version_by_rules(versions: List[Dict[str, Any]], item_name: str = "") -> Optional[str]:
    """
    根据规则决定最佳版本，返回最佳版本的 ID。
    """
    rules = settings_db.get_setting('media_cleanup_rules')
    if not rules:
        rules = [
            {"id": "runtime", "enabled": True}, 
            {"id": "effect", "enabled": True, "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]},
            {"id": "resolution", "enabled": True, "priority": ["4k", "1080p", "720p", "480p"]},
            {"id": "bit_depth", "enabled": True}, 
            {"id": "bitrate", "enabled": True},   
            {"id": "codec", "enabled": True, "priority": ["AV1", "HEVC", "H.264", "VP9"]},
            {"id": "quality", "enabled": True, "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            {"id": "subtitle", "enabled": True, "priority": "desc"}, 
            {"id": "frame_rate", "enabled": False}, 
            {"id": "filesize", "enabled": True},
            {"id": "date_added", "enabled": True, "priority": "asc"}
        ]

    version_properties = [_get_properties_for_comparison(v) for v in versions if v]

    def compare_wrapper(v1, v2):
        # 传入 item_name 以便在日志中显示剧名
        return _compare_versions(v1, v2, rules, item_name)

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
    task_manager.update_status_from_thread(0, "正在准备扫描...")

    try:
        library_ids_to_scan = settings_db.get_setting('media_cleanup_library_ids') or []
        keep_one_per_res = settings_db.get_setting('media_cleanup_keep_one_per_res') or False
        
        logger.info(f"  ➜ 正在计算扫描范围 (基于用户 {processor.emby_user_id} 的权限)...")
        
        allowed_movies, _ = queries_db.query_virtual_library_items(
            rules=[], logic='AND', user_id=processor.emby_user_id, limit=1000000, offset=0,
            item_types=['Movie'], target_library_ids=library_ids_to_scan if library_ids_to_scan else None
        )
        
        allowed_series, _ = queries_db.query_virtual_library_items(
            rules=[], logic='AND', user_id=processor.emby_user_id, limit=1000000, offset=0,
            item_types=['Series'], target_library_ids=library_ids_to_scan if library_ids_to_scan else None
        )
        
        allowed_movie_tmdb_ids = [m['tmdb_id'] for m in allowed_movies if m.get('tmdb_id')]
        allowed_series_tmdb_ids = [s['tmdb_id'] for s in allowed_series if s.get('tmdb_id')]
        
        total_scope = len(allowed_movie_tmdb_ids) + len(allowed_series_tmdb_ids)
        logger.info(f"  ➜ 扫描范围确定：{len(allowed_movie_tmdb_ids)} 部电影, {len(allowed_series_tmdb_ids)} 部剧集。")

        if total_scope == 0:
            task_manager.update_status_from_thread(100, "扫描中止：当前用户视角下没有可见的媒体项。")
            return

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
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (item['tmdb_id'], item['item_type']))
                    title_row = cursor.fetchone()
                    display_title = title_row['title'] if title_row else '未知媒体'
            
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_items}) 正在分析: {display_title}")

            raw_versions = item['asset_details_json']
            unique_versions_map = {}
            for v in raw_versions:
                eid = v.get('emby_item_id')
                if eid:
                    unique_versions_map[eid] = v
            
            versions_from_db = list(unique_versions_map.values())

            if len(versions_from_db) < 2: continue

            best_id_or_ids = None
            
            if keep_one_per_res:
                res_groups = defaultdict(list)
                for v in versions_from_db:
                    props = _get_properties_for_comparison(v)
                    res_key = props.get('resolution', 'unknown')
                    res_groups[res_key].append(v)
                
                best_ids_set = set()
                for res, group_versions in res_groups.items():
                    # ★ 传入 display_title 以便打印日志
                    best_in_group = _determine_best_version_by_rules(group_versions, item_name=f"{display_title} ({res})")
                    if best_in_group:
                        best_ids_set.add(best_in_group)
                
                if len(best_ids_set) == len(versions_from_db):
                    continue 
                
                best_id_or_ids = list(best_ids_set)
                
            else:
                # ★ 传入 display_title 以便打印日志
                best_id_or_ids = _determine_best_version_by_rules(versions_from_db, item_name=display_title)

            versions_for_frontend = []
            for v in versions_from_db:
                props = _get_properties_for_comparison(v)
                versions_for_frontend.append({
                    'id': v.get('emby_item_id'),
                    'path': v.get('path'),
                    'filesize': v.get('size_bytes', 0),
                    'quality': props.get('quality'), 
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
    【重构版】执行指定的一批媒体去重任务。
    采用物理删除 STRM -> 追溯清理空目录 -> 联动删除 115 -> 清理本地 DB -> 通知 Emby 刷新的优雅方案。
    """
    if not task_ids:
        task_manager.update_status_from_thread(-1, "任务失败：缺少任务ID")
        return

    task_name = "执行媒体去重"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    config = config_manager.APP_CONFIG
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    sync_delete = config.get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False) 
    api_delete = settings_db.get_setting('media_cleanup_api_delete', False)  
    
    try:
        tasks_to_execute = cleanup_db.get_cleanup_index_by_ids(task_ids)
        total = len(tasks_to_execute)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：未找到指定的清理任务。")
            return

        deleted_count = 0
        processed_task_ids = []
        
        # 用于在所有任务结束后统一执行批量操作
        all_deleted_paths = []
        all_pickcodes_to_delete = []

        for i, task in enumerate(tasks_to_execute):
            if processor.is_stop_requested():
                logger.warning("  ➜ 任务被用户中止。")
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
                logger.error(f"  ➜ 严重错误：无法确定 '{item_name}' 的保留版本... 跳过。")
                continue

            versions = task['versions_info_json']
            task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在清理: {item_name}")

            for version in versions:
                version_id_to_check = str(version.get('id'))
                file_path = version.get('path')
                
                if version_id_to_check not in safe_ids_set and api_delete == False:
                    logger.warning(f"  ➜ 准备物理删除劣质版本 (ID: {version_id_to_check}): {file_path}")
                    
                    # 1. 获取 PC 码 (用于后续联动删除 115 网盘文件)
                    pc = media_db.get_pickcode_by_emby_id(version_id_to_check)
                    if pc:
                        all_pickcodes_to_delete.append(pc)
                        
                    # 2. 物理删除本地文件 (STRM, mediainfo, 字幕, NFO)
                    if file_path and os.path.exists(file_path):
                        try:
                            # 删除主文件
                            os.remove(file_path)
                            all_deleted_paths.append(file_path)
                            logger.debug(f"  ➜ 已删除主文件: {file_path}")
                            
                            base_dir = os.path.dirname(file_path)
                            base_name = os.path.splitext(os.path.basename(file_path))[0]
                            
                            # 删除 mediainfo.json
                            mi_path = os.path.join(base_dir, f"{base_name}-mediainfo.json")
                            if os.path.exists(mi_path):
                                os.remove(mi_path)
                                
                            # 删除同名字幕和 NFO
                            for f in os.listdir(base_dir):
                                if f.startswith(base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup', 'nfo']:
                                    sub_path = os.path.join(base_dir, f)
                                    try:
                                        os.remove(sub_path)
                                    except: pass
                                    
                            # 3. 向上追溯删除空目录 (连锅端)
                            curr_dir = base_dir
                            protected_dirs = {os.path.abspath(local_root)} if local_root else set()
                            
                            while curr_dir and os.path.abspath(curr_dir) not in protected_dirs:
                                if os.path.exists(curr_dir):
                                    has_media = False
                                    for root_dir, _, files in os.walk(curr_dir):
                                        for f in files:
                                            ext = f.split('.')[-1].lower()
                                            if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov'}:
                                                has_media = True
                                                break
                                        if has_media: break
                                    
                                    if not has_media:
                                        shutil.rmtree(curr_dir)
                                        logger.info(f"  ➜ [完美擦屁股] 目录已无媒体文件，连锅端删除: {curr_dir}")
                                        curr_dir = os.path.dirname(curr_dir)
                                    else:
                                        break
                                else:
                                    break
                        except Exception as e:
                            logger.error(f"  ➜ 物理删除文件失败: {e}")
                    else:
                        logger.debug(f"  ➜ 本地文件不存在或路径无法访问，跳过物理删除: {file_path}")
                        # 即使本地文件不存在，也把路径加进去，让 Emby 去扫这个路径发现它没了
                        if file_path:
                            all_deleted_paths.append(file_path)

                    # 4. 清理本地数据库记录 (善后)
                    maintenance_db.cleanup_deleted_media_item(
                        item_id=version_id_to_check,
                        item_name=item_name,
                        item_type=task['item_type']
                    )
                    
                    deleted_count += 1
                    logger.info(f"  ➜ 成功处理劣质版本 ID: {version_id_to_check}")
                else:
                    emby.delete_item_sy(version_id_to_check, processor.emby_url, processor.emby_api_key, processor.user_id)
                    logger.info(f"  ➜ 通过 API 删除了版本 ID: {version_id_to_check}")
                    deleted_count += 1

            processed_task_ids.append(task['id'])

        if processed_task_ids:
            cleanup_db.batch_update_cleanup_index_status(processed_task_ids, 'processed')

        # 5. 联动删除 115 网盘文件 (利用 WebhookDeleteBuffer 的批量处理能力)
        if all_pickcodes_to_delete and sync_delete and api_delete == False:
            logger.info(f"  ➜ 正在将 {len(all_pickcodes_to_delete)} 个文件加入 115 网盘联动删除队列...")
            WebhookDeleteBuffer.add(all_pickcodes_to_delete)

        # 6. 通知 Emby 刷新 (让 Emby 自己发现文件没了并清理数据库)
        if all_deleted_paths and sync_delete and api_delete == False:
            logger.info(f"  ➜ 正在通知 Emby 刷新 {len(all_deleted_paths)} 个被删除的路径...")
            emby.notify_emby_file_changes(
                file_paths=all_deleted_paths,
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                update_type="Deleted"
            )

        final_message = f"清理完成！共处理 {len(processed_task_ids)} 个任务，成功清理了 {deleted_count} 个多余版本。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")