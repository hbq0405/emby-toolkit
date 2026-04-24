# tasks/p115.py
import logging
import os
import re
import json
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
import handler.tmdb as tmdb

# 从 115 服务主模块导入核心类和辅助函数
from handler.p115_service import (
    P115Service,
    P115CacheManager,
    P115RecordManager,
    P115DeleteBuffer,
    SmartOrganizer,
    get_config,
    _parse_115_size,
    _identify_media_enhanced
)

logger = logging.getLogger(__name__)

def task_scan_and_organize_115(processor=None):
    """
    [任务链] 主动扫描 115 待整理目录 (V3 流水线并发版：边扫边理，火力全开)
    """
    logger.info("=== 开始执行 115 待整理目录扫描 (并发模式) ===")

    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)

    update_progress(5, "正在初始化 115 客户端与目录扫描...")

    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    # 通知监控服务进入蓄水池模式
    try:
        from monitor_service import pause_queue_processing, resume_queue_processing
        pause_queue_processing()
    except Exception as e:
        logger.warning(f"  ➜ 无法暂停监控队列: {e}")
        resume_queue_processing = lambda: None

    config = get_config()
    cid_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
    save_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_NAME, '待整理')
    enable_organize = config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)
    use_ai = config.get(constants.CONFIG_OPTION_AI_RECOGNITION, False)
    ai_translator = processor.ai_translator if processor and hasattr(processor, 'ai_translator') else None

    configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
    allowed_exts = set(e.lower() for e in configured_exts)
    if not allowed_exts:
        allowed_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg', 'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

    if not cid_val or str(cid_val) == '0':
        logger.error("  ➜ 未配置待整理目录，跳过。")
        return
    if not enable_organize:
        logger.warning("  ➜ 未开启智能整理开关，仅扫描不处理。")
        return
        
    try:
        save_cid = int(cid_val)
        save_name = str(save_val)

        # 1. 准备 '未识别' 目录
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        unidentified_folder_name = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME, "未识别")
        
        if not unidentified_cid or str(unidentified_cid) == '0':
            unidentified_folder_name = "未识别"
            try:
                search_res = client.fs_files({'cid': save_cid, 'search_value': unidentified_folder_name, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                if search_res.get('data'):
                    for item in search_res['data']:
                        if item.get('fn') == unidentified_folder_name and str(item.get('fc')) == '0':
                            unidentified_cid = item.get('fid')
                            break
            except: pass

            if not unidentified_cid:
                try:
                    mk_res = client.fs_mkdir(unidentified_folder_name, save_cid)
                    if mk_res.get('state'): unidentified_cid = mk_res.get('cid')
                except: pass

        logger.info(f"  ➜ 正在拉取 [{save_name}] 根目录列表...")
        
        # =================================================================
        # 步骤一：主线程拉取根目录列表
        # =================================================================
        root_items = []
        offset = 0
        limit = 1000
        while True:
            res = {}
            for retry in range(3):
                try:
                    res = client.fs_files({'cid': save_cid, 'limit': limit, 'offset': offset, 'o': 'user_utime', 'asc': 0, 'record_open_time': 0, 'count_folders': 0})
                    break 
                except Exception as e:
                    if '405' in str(e) or 'Method Not Allowed' in str(e): time.sleep(3)
                    else: raise

            data = res.get('data', [])
            if not data: break 
            
            for item in data:
                name = item.get('fn') or item.get('n') or item.get('file_name')
                if not name: continue
                item_id = item.get('fid') or item.get('file_id')
                
                # 忽略未识别目录
                if str(item_id) == str(unidentified_cid) or (not config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID) and name == '未识别'):
                    continue
                    
                root_items.append(item)

            if len(data) < limit: break
            offset += limit

        total_root_items = len(root_items)
        if total_root_items == 0:
            logger.info("  ➜ 待整理目录为空，任务结束。")
            update_progress(100, "待整理目录为空。")
            return

        logger.info(f"  ➜ 根目录拉取完毕，共发现 {total_root_items} 个待处理项，启动流水线并发整理...")

        # =================================================================
        # 步骤二：定义单个根目录项的流水线处理函数 (扫盘 -> 打散 -> 整理)
        # =================================================================
        def process_root_item(root_item):
            top_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name')
            top_id = root_item.get('fid') or root_item.get('file_id')
            fc_val = str(root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type'))
            is_folder = (fc_val == '0')

            local_processed = 0
            local_unidentified = []

            # 过滤蓝光原盘特殊目录
            if is_folder and top_name.upper() in ['BDMV', 'CERTIFICATE', 'ANY!', 'VIDEO_TS', 'AUDIO_TS', 'PLAYLIST', 'CLIPINF', 'STREAM', 'BACKUP']:
                return 0, []

            groups_to_process = []

            if not is_folder:
                # 单个文件直接成组
                ext = top_name.split('.')[-1].lower() if '.' in top_name else ''
                if ext in allowed_exts:
                    is_tv_hint = bool(re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第[一二三四五六七八九十\d]+季', top_name, re.IGNORECASE))
                    groups_to_process.append({
                        "top_name": top_name,
                        "files": [root_item],
                        "is_tv": is_tv_hint,
                        "has_season_dir": False
                    })
                else:
                    if ext not in ['clpi', 'mpls', 'bdmv', 'jar', 'bup', 'ifo']:
                        local_unidentified.append(root_item)
            else:
                # 是文件夹，进行同步扫盘
                gathered_files = []
                is_tv_group = False
                has_season_dir = False
                
                def sync_scan(current_cid, depth=0):
                    nonlocal is_tv_group, has_season_dir
                    if depth > 5: return
                    
                    c_offset = 0
                    while True:
                        try:
                            c_res = client.fs_files({'cid': current_cid, 'limit': 1000, 'offset': c_offset, 'record_open_time': 0, 'count_folders': 0})
                        except Exception:
                            time.sleep(1.5)
                            continue
                            
                        c_data = c_res.get('data', [])
                        if not c_data: break
                        
                        for child in c_data:
                            c_name = child.get('fn') or child.get('n') or child.get('file_name')
                            c_id = child.get('fid') or child.get('file_id')
                            c_fc = str(child.get('fc') if child.get('fc') is not None else child.get('type'))
                            c_is_folder = (c_fc == '0')
                            
                            c_is_tv_hint = bool(re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第[一二三四五六七八九十\d]+季', c_name, re.IGNORECASE))
                            c_is_season_dir = c_is_folder and bool(re.search(r'^(Season\s?\d+|S\d+|第[一二三四五六七八九十\d]+季)$', c_name, re.IGNORECASE))
                            
                            if c_is_folder:
                                if c_is_season_dir or c_is_tv_hint:
                                    is_tv_group = True
                                    if c_is_season_dir: has_season_dir = True
                                
                                has_tmdb = bool(re.search(r'(?:tmdb|tmdbid)[=\-_]*(\d+)', top_name, re.IGNORECASE))
                                
                                # ★ 核心提速：如果是剧集或已标记TMDB，绝不可能是大杂烩，直接把文件夹当做 item 塞进去，不再深入！
                                if depth > 0 and (is_tv_group or has_tmdb):
                                    gathered_files.append(child)
                                else:
                                    sync_scan(c_id, depth + 1)
                                
                                # 将目录加入垃圾回收器
                                P115DeleteBuffer.add(fids=[], base_cids=[c_id])
                            else:
                                c_ext = c_name.split('.')[-1].lower() if '.' in c_name else ''
                                if c_ext in allowed_exts:
                                    gathered_files.append(child)
                                    if c_is_tv_hint: is_tv_group = True
                                else:
                                    if c_ext not in ['clpi', 'mpls', 'bdmv', 'jar', 'bup', 'ifo']:
                                        local_unidentified.append(child)
                                        
                        if len(c_data) < 1000: break
                        c_offset += 1000

                # 执行同步扫盘
                sync_scan(top_id, 0)
                
                # ★ 智能打散逻辑
                has_tmdb = bool(re.search(r'(?:tmdb|tmdbid)[=\-_]*(\d+)', top_name, re.IGNORECASE))
                valid_video_files = []
                for f in gathered_files:
                    f_name = f.get('fn', '')
                    ext = f_name.split('.')[-1].lower() if '.' in f_name else ''
                    if ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']:
                        file_size = _parse_115_size(f.get('fs') or f.get('size'))
                        if file_size > 50 * 1024 * 1024:
                            valid_video_files.append(f)
                
                if not is_tv_group and not has_tmdb and len(valid_video_files) > 1:
                    logger.info(f"  ➜ [智能打散] 检测到疑似混合大目录 '{top_name}'，执行打散处理...")
                    assigned_fids = set()
                    for v_file in valid_video_files:
                        v_name = v_file.get('fn') or v_file.get('n') or v_file.get('file_name', '')
                        v_base = v_name.rsplit('.', 1)[0] if '.' in v_name else v_name
                        new_group = {"top_name": v_name, "files": [v_file], "is_tv": False, "has_season_dir": False}
                        assigned_fids.add(v_file.get('fid') or v_file.get('file_id'))
                        
                        for other_file in gathered_files:
                            o_fid = other_file.get('fid') or other_file.get('file_id')
                            if o_fid in assigned_fids: continue
                            o_name = other_file.get('fn') or other_file.get('n') or other_file.get('file_name', '')
                            if o_name.startswith(v_base):
                                new_group["files"].append(other_file)
                                assigned_fids.add(o_fid)
                        groups_to_process.append(new_group)
                    
                    for f in gathered_files:
                        f_id = f.get('fid') or f.get('file_id')
                        if f_id not in assigned_fids:
                            o_name = f.get('fn') or f.get('n') or f.get('file_name', '')
                            groups_to_process.append({"top_name": o_name, "files": [f], "is_tv": False, "has_season_dir": False})
                else:
                    groups_to_process.append({
                        "top_name": top_name,
                        "files": gathered_files,
                        "is_tv": is_tv_group,
                        "has_season_dir": has_season_dir
                    })

            # 遍历处理该根目录下的所有组
            for group in groups_to_process:
                g_top_name = group["top_name"]
                g_files = group["files"]
                if not g_files: continue
                
                forced_type = 'tv' if group["is_tv"] else None
                season_num = None
                if forced_type == 'tv':
                    m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', g_top_name)
                    m2 = re.search(r'Season\s*(\d{1,4})\b', g_top_name, re.IGNORECASE)
                    m3 = re.search(r'第(\d{1,4})季', g_top_name)
                    if m1: season_num = int(m1.group(1))
                    elif m2: season_num = int(m2.group(1))
                    elif m3: season_num = int(m3.group(1))

                tmdb_id, media_type, title = _identify_media_enhanced(
                    g_top_name, main_dir_name=g_top_name, has_season_subdirs=group["has_season_dir"],
                    forced_media_type=forced_type, ai_translator=ai_translator, use_ai=use_ai, is_folder=False
                )
                
                if not tmdb_id:
                    logger.warning(f"  ➜ 无法识别媒体组: {g_top_name}，打入未识别。")
                    local_unidentified.extend(g_files)
                    continue
                    
                try:
                    organizer = SmartOrganizer(client, tmdb_id, media_type, title, ai_translator, use_ai)
                    if season_num is not None: organizer.forced_season = season_num
                    target_cid = organizer.get_target_cid(season_num=season_num)
                    
                    # 执行整理 (跳过单次 GC)
                    if organizer.execute(g_files, target_cid, skip_gc=True):
                        local_processed += len(g_files)
                except Exception as e:
                    logger.error(f"  ➜ 整理出错 (组: {g_top_name}): {e}")

            # 移入未识别
            if local_unidentified and unidentified_cid:
                u_fids = [i.get('fid') or i.get('file_id') for i in local_unidentified]
                try:
                    client.fs_move(u_fids, unidentified_cid)
                    
                    from handler.telegram import send_unrecognized_notification
                    from handler.p115_service import P115RecordManager
                    
                    for item in local_unidentified:
                        name = item.get('fn') or item.get('n') or item.get('file_name', '')
                        item_id = item.get('fid') or item.get('file_id')
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        
                        if ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']:
                            send_unrecognized_notification(name, reason="正则、MP辅助与AI均无法匹配到有效的 TMDb 数据")
                            pc = item.get('pc') or item.get('pick_code') 
                            P115RecordManager.add_or_update_record(
                                item_id, name, 'unrecognized', 
                                target_cid=unidentified_cid, category_name="未识别", pick_code=pc 
                            )
                except Exception as e:
                    logger.error(f"  ➜ 移入未识别失败: {e}")

            return local_processed, len(local_unidentified)

        # =================================================================
        # 步骤三：启动线程池并发处理
        # =================================================================
        max_workers = int(config.get(constants.CONFIG_OPTION_115_MAX_WORKERS, 3))
        total_processed = 0
        total_unidentified = 0
        completed_roots = 0

        import concurrent.futures
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_root_item, item): item for item in root_items}
            
            for future in concurrent.futures.as_completed(futures):
                completed_roots += 1
                try:
                    p_count, u_count = future.result()
                    total_processed += p_count
                    total_unidentified += u_count
                except Exception as e:
                    logger.error(f"  ➜ 处理根目录项时发生异常: {e}")
                
                prog = 10 + int((completed_roots / total_root_items) * 90)
                update_progress(prog, f"正在并发整理... ({completed_roots}/{total_root_items})")

        # ★ 任务结束前，触发一次全局待整理目录清理
        from handler.p115_service import P115DeleteBuffer
        P115DeleteBuffer.add(check_save_path=True)
        
        final_msg = f"扫描结束！成功归类 {total_processed} 个，移入未识别 {total_unidentified} 个。"
        logger.info(f"=== {final_msg} ===")
        update_progress(100, final_msg)

    except Exception as e:
        logger.error(f"  ➜ 115 扫描任务异常: {e}", exc_info=True)
        update_progress(100, f"扫描异常结束: {e}")
    finally:
        try:
            resume_queue_processing()
        except:
            pass

def task_sync_115_directory_tree(processor=None):
    """
    主动同步 115 分类目录下的所有子目录到本地 DB 缓存。
    这能彻底解决 115 API search_value 失效导致的老目录无法识别问题。
    ★ 终极版：支持自动清理本地已失效的旧目录缓存。
    """
    logger.info("=== 开始全量同步 115 目录树到本地数据库 ===")
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    client = P115Service.get_client()
    if not client: 
        update_progress(100, "115 客户端未初始化，任务结束。")
        return

    raw_rules = settings_db.get_setting('p115_sorting_rules')
    if not raw_rules: 
        update_progress(100, "未配置分类规则，无需同步。")
        return
    
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
    
    target_dirs = {}
    for rule in rules:
        if rule.get('enabled', True) and rule.get('cid'):
            cid_str = str(rule['cid'])
            if cid_str and cid_str != '0':
                display_name = rule.get('category_path') or rule.get('dir_name') or rule.get('name') or f"CID:{cid_str}"
                target_dirs[cid_str] = display_name

    if not target_dirs:
        update_progress(100, "未找到有效的分类目标目录 CID，任务结束。")
        return

    total_cached = 0
    total_cleaned = 0
    total_cids = len(target_dirs)
    
    for idx, (cid, dir_name) in enumerate(target_dirs.items()):
        base_prog = int((idx / total_cids) * 100)
        update_progress(base_prog, f"  ➜ 正在扫描第 {idx+1}/{total_cids} 个分类目录: [{dir_name}] ...")
        
        offset = 0
        limit = 1000
        page_count = 0
        
        # ★ 核心新增：记录本次从网盘真实扫到的所有子目录 ID
        current_valid_sub_cids = set()
        
        while True:
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                update_progress(100, "任务已被用户手动终止。")
                return

            try:
                res = client.fs_files({'cid': cid, 'limit': limit, 'offset': offset, 'record_open_time': 0, 'count_folders': 0})
                data = res.get('data', [])
                
                if not data: 
                    break
                
                page_count += 1
                dir_count_in_page = 0
                
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        for item in data:
                            fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                            if str(fc_val) == '0':
                                sub_cid = item.get('fid') or item.get('file_id')
                                sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                                if sub_cid and sub_name:
                                    # 记录有效的子目录 ID
                                    current_valid_sub_cids.add(str(sub_cid))
                                    
                                    current_local_path = os.path.join(dir_name, str(sub_name))
                                    
                                    cursor.execute("""
                                        INSERT INTO p115_filesystem_cache (id, parent_id, name, local_path)
                                        VALUES (%s, %s, %s, %s)
                                        ON CONFLICT (parent_id, name)
                                        DO UPDATE SET 
                                            id = EXCLUDED.id, 
                                            local_path = EXCLUDED.local_path,
                                            updated_at = NOW()
                                    """, (str(sub_cid), str(cid), str(sub_name), current_local_path))
                                    total_cached += 1
                                    dir_count_in_page += 1
                        conn.commit()
                
                update_progress(base_prog, f"  ➜ [{dir_name}] | 翻阅第 {page_count} 页 | 新增/更新 {dir_count_in_page} 个目录...")
                
                if len(data) < limit:
                    break
                    
                offset += limit
                
            except Exception as e:
                logger.error(f"  ➜ 同步目录树异常 [{dir_name}]: {e}")
                break 

        # =================================================================
        # ★★★ 核心新增：清理本地数据库中多余的失效目录 ★★★
        # =================================================================
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. 先查出本地数据库里，属于当前父目录(cid)的所有子目录 ID
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE parent_id = %s", (str(cid),))
                    db_sub_cids = {row['id'] for row in cursor.fetchall()}
                    
                    # 2. 找出“在本地数据库里，但不在网盘真实列表里”的失效 ID
                    invalid_cids = db_sub_cids - current_valid_sub_cids
                    
                    # 3. 执行删除
                    if invalid_cids:
                        # 转换成元组供 SQL IN 语句使用
                        invalid_cids_tuple = tuple(invalid_cids)
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id IN %s", (invalid_cids_tuple,))
                        conn.commit()
                        
                        cleaned_count = len(invalid_cids)
                        total_cleaned += cleaned_count
                        logger.info(f"  ➜ [{dir_name}] 清理了 {cleaned_count} 个已失效的本地目录缓存。")
        except Exception as e:
            logger.error(f"  ➜ 清理失效目录异常 [{dir_name}]: {e}")

    update_progress(100, f"=== 同步结束！共更新 {total_cached} 个目录，清理 {total_cleaned} 个失效缓存 ===")

def task_full_sync_strm_and_subs(processor=None):
    """
    【V4 终极上帝视角版】全量生成 STRM 与 同步字幕
    利用 115 分类目录级全局拉取 (type=4/1) + 动态 API 溯源 + 本地 DB 目录树缓存，实现秒级增量同步！
    """
    config = get_config()
    download_subs = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True)
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    min_size_mb = int(config.get(constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE, 10))
    MIN_VIDEO_SIZE = min_size_mb * 1024 * 1024
    
    start_msg = "=== ➜ 开始极速全量同步 STRM 与 字幕 ===" if download_subs else "=== ➜ 开始极速全量同步 STRM (跳过字幕) ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    logger.info(start_msg)
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    # ★ 通知监控服务进入蓄水池模式，防止全量同步触发海量刮削
    try:
        from monitor_service import pause_queue_processing, resume_queue_processing
        pause_queue_processing()
    except Exception as e:
        logger.warning(f"  ➜ 无法暂停监控队列: {e}")
        resume_queue_processing = lambda: None # 兜底防报错

    try:
        local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
        etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
        
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}
        
        allowed_exts = set(e.lower() for e in config.get(constants.CONFIG_OPTION_115_EXTENSIONS, []))
        if not allowed_exts:
            allowed_exts = known_video_exts | known_sub_exts
        
        if not local_root or not etk_url:
            update_progress(100, "错误：未配置本地 STRM 根目录或 ETK 访问地址！")
            return

        client = P115Service.get_client()
        if not client: return

        raw_rules = settings_db.get_setting('p115_sorting_rules')
        if not raw_rules: 
            update_progress(100, "错误：未配置分类规则！")
            return
        rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules

        # 获取重命名配置，用于判断 STRM 直链是否需要带文件名
        rename_config = settings_db.get_setting('p115_rename_config') or {}

        # =================================================================
        # 阶段 1: 加载规则与本地目录树缓存到内存 (耗时: 毫秒级)
        # =================================================================
        update_progress(5, "  ➜ 正在加载本地目录树缓存到内存...")
        
        cid_to_rel_path = {}  
        target_cids = set()   
        
        for r in rules:
            if r.get('enabled', True) and r.get('cid') and str(r['cid']) != '0':
                cid = str(r['cid'])
                target_cids.add(cid)
                cid_to_rel_path[cid] = r.get('category_path') or r.get('dir_name', '未识别')

        # 加载 DB 中的目录树 (新增提取 local_path)
        dir_cache = {} 
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id, parent_id, name, local_path FROM p115_filesystem_cache")
                    for row in cursor.fetchall():
                        dir_cache[str(row['id'])] = {
                            'pid': str(row['parent_id']), 
                            'name': str(row['name']),
                            'local_path': row['local_path']
                        }
        except Exception as e:
            update_progress(100, f"读取本地目录缓存失败: {e}")
            return

        # 动态 API 路径缓存池 (防止重复请求 115 接口)
        dynamic_path_cache = {}

        # 内存路径推导函数 (★ 终极修复版：DB缓存 + API动态溯源)
        def resolve_local_dir(pid, target_cid):
            pid = str(pid)
            # 1. 如果文件直接在分类根目录下
            if pid in cid_to_rel_path:
                return cid_to_rel_path[pid]
                
            # 2. 如果刚才已经通过 API 查过这个目录了，直接秒回
            if pid in dynamic_path_cache:
                return dynamic_path_cache[pid]

            # 3. 尝试使用数据库中已有的 local_path
            if pid in dir_cache and dir_cache[pid].get('local_path'):
                return dir_cache[pid]['local_path']
                
            # 4. 尝试在数据库缓存中向上追溯
            parts = []
            curr = pid
            while curr and curr in dir_cache:
                parts.append(dir_cache[curr]['name'])
                curr = dir_cache[curr]['pid']
                
                if curr in cid_to_rel_path:
                    parts.append(cid_to_rel_path[curr])
                    parts.reverse()
                    resolved_path = os.path.join(*parts)
                    dynamic_path_cache[pid] = resolved_path # 存入内存池
                    return resolved_path

            # 5. ★ 终极兜底：缓存穿透时，主动向 115 请求该目录的真实路径
            try:
                dir_info = client.fs_files({'cid': pid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                path_nodes = dir_info.get('path', [])
                if path_nodes:
                    start_idx = -1
                    for i, p_node in enumerate(path_nodes):
                        if str(p_node.get('cid') or p_node.get('file_id')) == target_cid:
                            start_idx = i + 1
                            break
                    if start_idx != -1:
                        sub_folders = [str(p.get('name') or p.get('file_name')).strip() for p in path_nodes[start_idx:]]
                        base_cat_path = cid_to_rel_path.get(target_cid, '未识别')
                        resolved_path = os.path.join(base_cat_path, *sub_folders) if sub_folders else base_cat_path
                        dynamic_path_cache[pid] = resolved_path # 存入内存池，同目录文件不再请求
                        logger.debug(f"  ➜ [API溯源] 成功动态推导路径: {resolved_path}")
                        return resolved_path
            except Exception as e:
                logger.debug(f"  ➜ 动态查询目录路径失败 (pid: {pid}): {e}")

            return None

        # =================================================================
        # 阶段 2: 分类目录级全局拉取 (耗时: 秒级/分钟级)
        # =================================================================
        sync_has_errors = False
        valid_local_files = set()
        files_generated = 0
        subs_downloaded = 0
        
        fetch_types = [4] # 4=视频
        if download_subs: fetch_types.append(1) # 1=文档(含字幕)

        total_targets = len(target_cids)
        
        for idx, target_cid in enumerate(target_cids):
            category_name = cid_to_rel_path.get(target_cid, "未知分类")
            base_prog = 10 + int((idx / total_targets) * 80)
            update_progress(base_prog, f"  ➜ 正在全局拉取分类 [{category_name}] 下的所有文件...")
            
            for f_type in fetch_types:
                type_name = "视频" if f_type == 4 else "文档/字幕"
                offset = 0
                limit = 1000
                page = 1
                
                while True:
                    if processor and getattr(processor, 'is_stop_requested', lambda: False)(): return
                    
                    try:
                        # ★ 核心：指定 cid 并传入 type，强制 115 在该分类下进行全局递归检索！
                        res = client.fs_files({'cid': target_cid, 'type': f_type, 'limit': limit, 'offset': offset, 'record_open_time': 0})
                        if not res.get('state') and res.get('code'):
                            logger.error(f"  ➜ API 返回异常状态 (可能触发流控): {res}")
                            sync_has_errors = True
                            break
                        data = res.get('data', [])
                        if not data: break
                        
                        logger.info(f"  ➜ [{category_name}] - [{type_name}] 获取第 {page} 页 ({len(data)} 个文件)...")
                        
                        for item in data:
                            # 兼容 OpenAPI 键名
                            name = item.get('fn') or item.get('n') or item.get('file_name', '')
                            ext = name.split('.')[-1].lower() if '.' in name else ''
                            if ext not in allowed_exts: continue
                            
                            pc = item.get('pc') or item.get('pick_code')
                            # 115 返回的文件数据中，pid/cid 代表它所在的父目录 ID
                            pid = item.get('pid') or item.get('cid') or item.get('parent_id')
                            if not pc or not pid: continue
                            
                            # ★ 瞬间推导本地路径 (使用终极修复版函数)
                            rel_dir = resolve_local_dir(pid, target_cid)
                                
                            if not rel_dir: 
                                logger.warning(f"  ➜ 彻底无法推导路径，跳过文件: {name} (pid: {pid})")
                                continue 
                                
                            current_local_path = os.path.join(local_root, rel_dir)
                            os.makedirs(current_local_path, exist_ok=True)
                            
                            # 处理视频 STRM
                            if ext in known_video_exts:
                                raw_size = item.get('fs') or item.get('size')
                                file_size = _parse_115_size(raw_size)
                                safe_file_size = int(file_size) if str(file_size).isdigit() else 0
                                
                                if 0 < safe_file_size < MIN_VIDEO_SIZE:
                                    size_mb = safe_file_size / (1024 * 1024)
                                    logger.debug(f"  ➜ [全量同步] 视频体积过小 ({size_mb:.2f} MB)，判定为花絮/样本/广告，跳过生成 STRM: {name}")
                                    continue # 直接跳过当前文件，不生成 STRM 也不写缓存
                                strm_name = os.path.splitext(name)[0] + ".strm"
                                strm_path = os.path.join(current_local_path, strm_name)
                                
                                # ==================================================
                                # ★ 动态计算 STRM 内容 (支持挂载模式与直链模式)
                                # ==================================================
                                if not etk_url.startswith('http'):
                                    # 挂载模式
                                    mount_prefix = etk_url
                                    mount_path = os.path.join(mount_prefix, rel_dir, name)
                                    content = mount_path.replace('\\', '/')
                                else:
                                    # 默认的 ETK 302 直链模式
                                    content = f"{etk_url}/api/p115/play/{pc}"
                                    if rename_config.get('strm_url_fmt') == 'with_name':
                                        content = f"{content}/{name}"
                                
                                need_write = True
                                if os.path.exists(strm_path):
                                    try:
                                        with open(strm_path, 'r', encoding='utf-8') as f:
                                            old_content = f.read().strip()
                                            if old_content == content: 
                                                need_write = False
                                            else:
                                                logger.debug(f"  ➜ [更新] 内容不一致触发覆盖 -> 旧: [{old_content}] | 新: [{content}]")
                                    except Exception as e: pass
                                            
                                if need_write:
                                    with open(strm_path, 'w', encoding='utf-8') as f: f.write(content)
                                    if not os.path.exists(strm_path):
                                        logger.debug(f"  ➜ [新增] 生成 STRM: {strm_name}")
                                    files_generated += 1
                                    
                                valid_local_files.add(os.path.abspath(strm_path))
                                
                                # ==================================================
                                # ★ 写入本地数据库缓存 (p115_filesystem_cache)
                                # ==================================================
                                fid = item.get('fid') or item.get('file_id')
                                sha1 = item.get('sha1') or item.get('sha')
                                if pc and fid:
                                    file_local_path = os.path.join(rel_dir, name).replace('\\', '/')
                                    P115CacheManager.save_file_cache(
                                        fid=fid, parent_id=pid, name=name, 
                                        sha1=sha1, pick_code=pc, 
                                        local_path=file_local_path, size=file_size 
                                    )
                                    
                            # 处理字幕下载
                            elif ext in known_sub_exts and download_subs:
                                sub_path = os.path.join(current_local_path, name)
                                if not os.path.exists(sub_path):
                                    try:
                                        import requests
                                        url_obj = client.download_url(pc, user_agent="Mozilla/5.0")
                                        if url_obj:
                                            headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                            resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                                            resp.raise_for_status()
                                            with open(sub_path, 'wb') as f:
                                                for chunk in resp.iter_content(8192): f.write(chunk)
                                            logger.info(f"  ⬇️ [增量] 下载字幕: {name}")
                                            subs_downloaded += 1
                                    except Exception as e:
                                        logger.error(f"  ➜ 下载字幕失败 [{name}]: {e}")
                                        
                                valid_local_files.add(os.path.abspath(sub_path))

                        if len(data) < limit: break
                        offset += limit
                        page += 1
                        
                    except Exception as e:
                        logger.error(f"  ➜ 全局拉取异常 (cid={target_cid}, type={f_type}): {e}")
                        sync_has_errors = True
                        break

        logger.info(f"  ➜ 增量同步完成！新增/更新 STRM: {files_generated} 个, 下载字幕: {subs_downloaded} 个。")

        # =================================================================
        # 阶段 3: 本地失效文件清理 (耗时: 秒级)
        # =================================================================
        if enable_cleanup:
            if sync_has_errors:
                logger.warning("  🛑 致命警告：本次同步过程中发生 API 异常或触发 115 流控！为防止灾难性误删，已强制跳过本地清理阶段！")
            elif not valid_local_files and files_generated == 0:
                logger.warning("  ➜ 警告：本次同步未获取到任何有效文件，为防止误删，已跳过本地清理阶段！")
            else:
                update_progress(90, "  ➜ 正在比对并清理本地失效文件与空壳目录...")
                cleaned_files = 0
                cleaned_dirs = 0
                import shutil  # 引入 shutil 用于连锅端
                
                for cid, rel_path in cid_to_rel_path.items():
                    target_local_dir = os.path.join(local_root, rel_path)
                    if not os.path.exists(target_local_dir): continue
                    
                    # 1. 先清理失效的 STRM 和 字幕文件
                    for root_dir, dirs, files in os.walk(target_local_dir):
                        for file in files:
                            ext = file.split('.')[-1].lower()
                            if ext in known_sub_exts or ext == 'strm':
                                file_path = os.path.abspath(os.path.join(root_dir, file))
                                if file_path not in valid_local_files:
                                    try:
                                        os.remove(file_path)
                                        cleaned_files += 1
                                        logger.debug(f"  ➜ [清理] 删除失效文件: {file}")
                                    except Exception as e:
                                        logger.warning(f"  ➜ 删除文件失败 {file}: {e}")
                    
                    # 2. ★ 终极暴力清理：自下而上扫描，只要没有 STRM，无视任何残留文件直接连锅端！
                    for root_dir, dirs, files in os.walk(target_local_dir, topdown=False):
                        for d in dirs:
                            dir_path = os.path.join(root_dir, d)
                            if not os.path.exists(dir_path):
                                continue
                                
                            # 检查该目录及其所有子目录中，是否还存在任何 .strm 文件
                            has_strm = False
                            for r, _, fs in os.walk(dir_path):
                                if any(f.lower().endswith('.strm') for f in fs):
                                    has_strm = True
                                    break
                                    
                            # 如果没有 STRM，判定为空壳目录，直接物理超度（连带里面的 nfo/jpg 一起扬了）
                            if not has_strm:
                                try:
                                    shutil.rmtree(dir_path)
                                    cleaned_dirs += 1
                                    logger.debug(f"  ➜ [清理] 删除无 STRM 的空壳目录: {dir_path}")
                                except Exception as e:
                                    logger.warning(f"  ➜ 删除目录失败 {dir_path}: {e}")
                            
                logger.info(f"  ➜ 清理完成: 删除了 {cleaned_files} 个失效文件, {cleaned_dirs} 个无STRM的空壳目录。")

        update_progress(100, "=== 全量生成STRM任务结束 ===")

    except Exception as e:
        logger.error(f"  ➜ 全量同步任务异常: {e}", exc_info=True)
        update_progress(100, f"任务异常结束: {e}")
    finally:
        # ★ 任务结束（无论成功失败），务必解除监控队列抑制，恢复处理
        try:
            resume_queue_processing()
        except:
            pass

def task_sync_music_library(processor=None):
    """
    独立音乐库全量同步任务：增量生成 STRM + 下载附属文件(封面/歌词) + 自动清理
    """
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)

    config = get_config()
    from database import settings_db
    import constants
    import os
    import shutil
    
    music_cid = settings_db.get_setting('p115_music_root_cid')
    music_root_name = settings_db.get_setting('p115_music_root_name') or "音乐库"
    music_root_name = music_root_name.strip('/')
    
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    # ★ 复用下载字幕的开关来控制是否下载音乐附属文件
    download_aux = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True) 
    
    if not music_cid or str(music_cid) == '0':
        msg = "未配置音乐库根目录，跳过同步。"
        logger.warning(msg)
        update_progress(100, msg)
        return
        
    if not local_root or not etk_url:
        msg = "未配置本地 STRM 根目录或 ETK 访问地址！"
        logger.error(msg)
        update_progress(100, msg)
        return

    start_msg = f"=== 🎵 开始同步音乐库 [{music_root_name}] ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    logger.info(start_msg)
    update_progress(5, f"正在连接 115 获取 [{music_root_name}] 目录信息...")

    client = P115Service.get_client()
    if not client: 
        update_progress(100, "115 客户端未初始化，同步失败。")
        return

    audio_exts = {'mp3', 'flac', 'wav', 'ape', 'm4a', 'aac', 'ogg', 'wma', 'alac'}
    # ★ 定义需要下载的附属文件扩展名
    aux_exts = {'lrc', 'jpg', 'jpeg', 'png', 'nfo', 'txt', 'cue'}
    
    music_local_base = os.path.join(local_root, music_root_name)
    os.makedirs(music_local_base, exist_ok=True)

    files_generated = 0
    files_skipped = 0
    aux_downloaded = 0
    dirs_scanned = 0
    valid_local_files = set() 
    sync_has_errors = False

    def _recursive_sync(current_cid, current_local_path):
        nonlocal files_generated, files_skipped, aux_downloaded, dirs_scanned, sync_has_errors
        
        dirs_scanned += 1
        display_path = os.path.basename(current_local_path) or music_root_name
        update_progress(50, f"正在扫描: {display_path} (已扫 {dirs_scanned} 个目录)")
        
        offset = 0
        limit = 1000
        
        while True:
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                logger.info("音乐库同步任务被手动终止。")
                update_progress(100, "任务已手动终止。")
                return

            try:
                res = client.fs_files({'cid': current_cid, 'limit': limit, 'offset': offset, 'record_open_time': 0})
                if not res.get('state') and res.get('code'):
                    logger.error(f"  ➜ API 返回异常状态 (可能触发流控): {res}")
                    sync_has_errors = True
                    break
                data = res.get('data', [])
                if not data: break
                
                for item in data:
                    name = item.get('fn') or item.get('n') or item.get('file_name', '')
                    fc_val = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                    item_id = item.get('fid') or item.get('file_id')
                    
                    if fc_val == '0': # 文件夹
                        sub_local_path = os.path.join(current_local_path, name)
                        os.makedirs(sub_local_path, exist_ok=True)
                        
                        P115CacheManager.save_cid(item_id, current_cid, name)
                        rel_dir = os.path.relpath(sub_local_path, local_root).replace('\\', '/')
                        P115CacheManager.update_local_path(item_id, rel_dir)
                        
                        _recursive_sync(item_id, sub_local_path)
                        
                    elif fc_val == '1': # 文件
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        pc = item.get('pc') or item.get('pick_code')
                        if not pc: continue
                        
                        # ==========================================
                        # 1. 处理音频文件 -> 生成 STRM
                        # ==========================================
                        if ext in audio_exts:
                            strm_name = os.path.splitext(name)[0] + ".strm"
                            strm_path = os.path.join(current_local_path, strm_name)
                            
                            if not etk_url.startswith('http'):
                                rel_p = os.path.relpath(strm_path, local_root)
                                content = os.path.join(etk_url, rel_p).replace('\\', '/')
                                content = content[:-5] + f".{ext}" 
                            else:
                                content = f"{etk_url}/api/p115/play/{pc}/{name}"
                                
                            need_write = True
                            if os.path.exists(strm_path):
                                try:
                                    with open(strm_path, 'r', encoding='utf-8') as f:
                                        old_content = f.read().strip()
                                        if old_content == content: 
                                            need_write = False
                                except Exception: pass
                                            
                            if need_write:
                                with open(strm_path, 'w', encoding='utf-8') as f:
                                    f.write(content)
                                files_generated += 1
                            else:
                                files_skipped += 1
                                
                            valid_local_files.add(os.path.abspath(strm_path))
                            
                            if (files_generated + files_skipped) % 200 == 0:
                                logger.info(f"  ➜ 进度: 新增/更新 {files_generated} 首, 跳过 {files_skipped} 首...")
                            
                            sha1 = item.get('sha1') or item.get('sha')
                            file_size = _parse_115_size(item.get('fs') or item.get('size'))
                            rel_dir = os.path.relpath(current_local_path, local_root)
                            file_local_path = os.path.join(rel_dir, name).replace('\\', '/')
                            
                            P115CacheManager.save_file_cache(
                                fid=item_id, parent_id=current_cid, name=name,
                                sha1=sha1, pick_code=pc,
                                local_path=file_local_path, size=file_size
                            )
                            
                        # ==========================================
                        # ★ 2. 处理附属文件 -> 直接下载到本地
                        # ==========================================
                        elif ext in aux_exts and download_aux:
                            aux_path = os.path.join(current_local_path, name)
                            if not os.path.exists(aux_path):
                                try:
                                    import requests
                                    url_obj = client.download_url(pc, user_agent="Mozilla/5.0")
                                    if url_obj:
                                        headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                        resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                                        resp.raise_for_status()
                                        with open(aux_path, 'wb') as f:
                                            for chunk in resp.iter_content(8192): f.write(chunk)
                                        logger.info(f"  ⬇️ [增量] 下载音乐附属文件: {name}")
                                        aux_downloaded += 1
                                except Exception as e:
                                    logger.error(f"  ➜ 下载音乐附属文件失败 [{name}]: {e}")
                            
                            # 无论是否刚刚下载，只要网盘里有，就加入有效名单，防止被清理
                            valid_local_files.add(os.path.abspath(aux_path))
                            
                if len(data) < limit: break
                offset += limit
            except Exception as e:
                logger.error(f"同步音乐目录异常 (CID:{current_cid}): {e}")
                sync_has_errors = True
                break

    _recursive_sync(music_cid, music_local_base)
    
    # =================================================================
    # ★ 本地失效文件清理阶段 (包含附属文件)
    # =================================================================
    cleaned_files = 0
    cleaned_dirs = 0
    
    if enable_cleanup:
        if sync_has_errors:
            logger.warning("  🛑 致命警告：音乐库同步过程中发生 API 异常或触发流控！为防止灾难性误删，已强制跳过本地清理阶段！")
        elif not valid_local_files and files_generated == 0 and files_skipped == 0:
            logger.warning("  ➜ 警告：本次同步未获取到任何有效文件，为防止误删，已跳过本地清理阶段！")
        else:
            update_progress(90, "  ➜ 正在比对并清理本地失效文件与空壳目录...")
            
            if os.path.exists(music_local_base):
                # 1. 清理失效的 STRM 和 附属文件
                for root_dir, dirs, files in os.walk(music_local_base):
                    for file in files:
                        ext = file.split('.')[-1].lower()
                        # ★ 检查范围扩大：包含 strm 和所有附属扩展名
                        if ext == 'strm' or ext in aux_exts:
                            file_path = os.path.abspath(os.path.join(root_dir, file))
                            if file_path not in valid_local_files:
                                try:
                                    os.remove(file_path)
                                    cleaned_files += 1
                                    logger.debug(f"  ➜ [清理] 删除失效文件: {file}")
                                except Exception: pass
                
                # 2. 自下而上扫描，清理空壳目录 (逻辑不变：只要没有 STRM 就连锅端)
                for root_dir, dirs, files in os.walk(music_local_base, topdown=False):
                    for d in dirs:
                        dir_path = os.path.join(root_dir, d)
                        if not os.path.exists(dir_path): continue
                            
                        has_strm = False
                        for r, _, fs in os.walk(dir_path):
                            if any(f.lower().endswith('.strm') for f in fs):
                                has_strm = True
                                break
                                
                        if not has_strm:
                            try:
                                shutil.rmtree(dir_path)
                                cleaned_dirs += 1
                                logger.debug(f"  ➜ [清理] 删除无 STRM 的空壳目录: {dir_path}")
                            except Exception: pass

    end_msg = f"=== 🎵 音乐库同步完成！新增/更新: {files_generated} 首, 下载附属: {aux_downloaded} 个 ==="
    if enable_cleanup:
        end_msg += f" | 清理失效文件: {cleaned_files} 个, 空目录: {cleaned_dirs} 个"
        
    logger.info(end_msg)
    update_progress(100, f"同步完成！生成 {files_generated} 首，下载 {aux_downloaded} 个附属文件。")

# ======================================================================
# ★★★ 115 生活事件增量监控 (秒级同步 STRM) ★★★
# ======================================================================
def task_monitor_115_life_events(processor=None):
    """
    读取 115 生活事件，对比本地缓存，增量生成/删除 STRM。
    支持目录递归扫描，完美处理“移动整个文件夹”的场景。
    全面接入 P115CacheManager，逻辑更严密。
    """
    config = get_config()
    if not config.get(constants.CONFIG_OPTION_115_LIFE_MONITOR_ENABLED, False):
        return

    client = P115Service.get_client()
    if not client:
        return

    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    update_progress(5, "=== ➜ 开始检查 115 增量生活事件 ===")

    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    download_subs = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True)
    rename_config = settings_db.get_setting('p115_rename_config') or {}
    
    known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
    known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}
    allowed_exts = set(e.lower() for e in config.get(constants.CONFIG_OPTION_115_EXTENSIONS, []))
    if not allowed_exts: allowed_exts = known_video_exts | known_sub_exts

    raw_rules = settings_db.get_setting('p115_sorting_rules')
    if not raw_rules: return
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
    
    target_cids = set()
    cid_to_rel_path = {}
    for r in rules:
        if r.get('enabled', True) and r.get('cid') and str(r['cid']) != '0':
            cid = str(r['cid'])
            target_cids.add(cid)
            cid_to_rel_path[cid] = r.get('category_path') or r.get('dir_name', '未识别')

    events_to_delete = [] 
    added_count = 0
    deleted_count = 0

    # 辅助函数：推导本地路径
    def resolve_local_dir(pid):
        pid = str(pid)
        if pid in cid_to_rel_path: return cid_to_rel_path[pid]
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    parts = []
                    curr = pid
                    while curr:
                        cursor.execute("SELECT parent_id, name FROM p115_filesystem_cache WHERE id = %s", (curr,))
                        row = cursor.fetchone()
                        if not row: break
                        parts.append(row['name'])
                        curr = str(row['parent_id'])
                        if curr in cid_to_rel_path:
                            parts.append(cid_to_rel_path[curr])
                            parts.reverse()
                            return os.path.join(*parts)
        except: pass
        return None

    # 辅助函数：通知 Emby
    def _notify_emby(path):
        emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        if emby_url and emby_api_key:
            try:
                from handler import emby
                emby.notify_emby_file_changes([path], emby_url, emby_api_key, update_type="Deleted")
            except: pass

    # ★ 核心处理逻辑 (全面接入 P115CacheManager)
    def process_node(file_id, file_name, parent_id, pick_code, is_folder, b_type, file_sha1, file_size):
        nonlocal added_count, deleted_count
        
        # 1. 获取旧状态
        old_local_path = P115CacheManager.get_local_path(file_id)
        
        # 2. 获取新状态
        new_rel_dir = None
        if b_type != 22: 
            new_rel_dir = resolve_local_dir(parent_id)

        # ==========================================
        # 分支 1：删除或移出监控目录
        # ==========================================
        if old_local_path and not new_rel_dir:
            full_local_path = os.path.join(local_root, old_local_path)
            db_ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            
            if db_ext in known_video_exts:
                strm_full = os.path.splitext(full_local_path)[0] + ".strm"
                if os.path.exists(strm_full):
                    os.remove(strm_full)
                    deleted_count += 1
                    logger.info(f"  ➜ [事件] 删除失效 STRM: {os.path.basename(strm_full)}")
                    _notify_emby(strm_full)
            elif db_ext in known_sub_exts:
                if os.path.exists(full_local_path):
                    os.remove(full_local_path)
                    logger.info(f"  ➜ [事件] 删除失效字幕: {file_name}")
            else:
                if os.path.exists(full_local_path) and os.path.isdir(full_local_path):
                    import shutil
                    shutil.rmtree(full_local_path)
                    deleted_count += 1
                    logger.info(f"  ➜ [事件] 删除失效目录: {file_name}")
                    _notify_emby(os.path.dirname(full_local_path))
            
            # 清理数据库
            if is_folder: 
                # 1. 递归找出本地缓存中所有子孙节点的 FID 和 PC 码
                descendant_fids = []
                descendant_pcs = []
                cids_to_check = [str(file_id)]
                
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            # 广度优先遍历，扒出所有子孙
                            while cids_to_check:
                                current_cid = cids_to_check.pop(0)
                                cursor.execute("SELECT id, pick_code FROM p115_filesystem_cache WHERE parent_id = %s", (current_cid,))
                                for row in cursor.fetchall():
                                    fid = str(row['id'])
                                    pc = row['pick_code']
                                    descendant_fids.append(fid)
                                    if pc: descendant_pcs.append(pc)
                                    cids_to_check.append(fid) # 把子节点也加进去继续往下查
                                    
                            # 2. 批量删除整理记录 (斩草除根)
                            if descendant_pcs:
                                cursor.execute("DELETE FROM p115_organize_records WHERE pick_code = ANY(%s)", (descendant_pcs,))
                            if descendant_fids:
                                cursor.execute("DELETE FROM p115_organize_records WHERE file_id = ANY(%s)", (descendant_fids,))
                                
                            # 3. 批量删除缓存表中的所有子孙节点
                            if descendant_fids:
                                cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = ANY(%s)", (descendant_fids,))
                            
                            # 4. 最后删除目录本身的缓存
                            cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s", (str(file_id),))
                            
                        conn.commit()
                        if descendant_fids:
                            logger.info(f"  ➜ [事件] 级联清理完成: 移除了 {len(descendant_fids)} 个子文件的缓存与整理记录。")
                except Exception as e:
                    logger.error(f"  ➜ [事件] 级联清理目录缓存与记录失败: {e}")
            else: 
                # 单文件删除逻辑
                P115CacheManager.delete_files([file_id])
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            if pick_code:
                                cursor.execute("DELETE FROM p115_organize_records WHERE pick_code = %s", (pick_code,))
                            else:
                                cursor.execute("DELETE FROM p115_organize_records WHERE file_id = %s", (str(file_id),))
                            conn.commit()
                except Exception as e:
                    logger.error(f"  ➜ [事件] 清理 115 历史整理记录失败: {e}")

        # ==========================================
        # 分支 2：新增、移入、改名、同目录移动
        # ==========================================
        elif new_rel_dir:
            file_local_path = os.path.join(new_rel_dir, file_name).replace('\\', '/')
            
            # ★ 核心逻辑 1：如果路径完全没变，说明是 MP/TG 实时处理过的，直接跳过！
            if old_local_path == file_local_path:
                return
                
            # ★ 核心逻辑 2：如果以前存在，且路径变了 (移动/改名)，需要先删掉旧的本地文件！
            if old_local_path and old_local_path != file_local_path:
                old_full_path = os.path.join(local_root, old_local_path)
                old_ext = old_local_path.split('.')[-1].lower() if '.' in old_local_path else ''
                
                if old_ext in known_video_exts:
                    old_strm = os.path.splitext(old_full_path)[0] + ".strm"
                    if os.path.exists(old_strm): 
                        os.remove(old_strm)
                        _notify_emby(old_strm)
                elif old_ext in known_sub_exts:
                    if os.path.exists(old_full_path): os.remove(old_full_path)
                elif is_folder:
                    if os.path.exists(old_full_path) and os.path.isdir(old_full_path):
                        import shutil
                        shutil.rmtree(old_full_path)
                        _notify_emby(os.path.dirname(old_full_path))

            # 开始生成新文件/目录
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            current_local_path = os.path.join(local_root, new_rel_dir)
            
            if not is_folder and ext in allowed_exts:
                if ext in known_video_exts:
                    min_size_mb = int(config.get(constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE, 10))
                    MIN_VIDEO_SIZE = min_size_mb * 1024 * 1024
                    # 确保 file_size 是整数
                    safe_file_size = int(file_size) if str(file_size).isdigit() else 0
                    if 0 < safe_file_size < MIN_VIDEO_SIZE:
                        size_mb = safe_file_size / (1024 * 1024)
                        logger.debug(f"  ➜ [事件] 视频体积过小 ({size_mb:.2f} MB)，判定为花絮/样本/广告，忽略生成 STRM: {file_name}")
                        return # 直接跳过，不生成 STRM，也不记录缓存
                os.makedirs(current_local_path, exist_ok=True)
                
                if ext in known_video_exts and pick_code:
                    strm_name = os.path.splitext(file_name)[0] + ".strm"
                    strm_path = os.path.join(current_local_path, strm_name)
                    
                    if not etk_url.startswith('http'):
                        rel_p = os.path.relpath(strm_path, local_root)
                        content = os.path.join(etk_url, rel_p).replace('\\', '/')
                        content = content[:-5] + f".{ext}"
                    else:
                        content = f"{etk_url}/api/p115/play/{pick_code}"
                        if rename_config.get('strm_url_fmt') == 'with_name':
                            content = f"{content}/{file_name}"
                            
                    with open(strm_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    
                    P115CacheManager.save_file_cache(
                        fid=file_id, parent_id=parent_id, name=file_name, 
                        sha1=file_sha1, pick_code=pick_code, 
                        local_path=file_local_path, size=file_size
                    )
                    
                    added_count += 1
                    action_str = "移动/改名" if old_local_path else "新增"
                    logger.info(f"  ➜ [事件] {action_str} STRM: {file_name}")
                    
                    try:
                        from monitor_service import enqueue_file_actively
                        enqueue_file_actively(strm_path)
                    except: pass

                elif ext in known_sub_exts and download_subs and pick_code:
                    sub_path = os.path.join(current_local_path, file_name)
                    if not os.path.exists(sub_path):
                        try:
                            url_obj = client.download_url(pick_code, user_agent="Mozilla/5.0")
                            if url_obj:
                                import requests
                                headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                                resp.raise_for_status()
                                with open(sub_path, 'wb') as f:
                                    for chunk in resp.iter_content(8192): f.write(chunk)
                                logger.info(f"  ⬇️ [事件] 下载字幕: {file_name}")
                        except: pass
                            
            else:
                # 是目录，或者是不在白名单的文件，当做目录/空壳记录
                os.makedirs(os.path.join(current_local_path, file_name), exist_ok=True)
                P115CacheManager.save_cid(file_id, parent_id, file_name)
                P115CacheManager.update_local_path(file_id, file_local_path)

    # ★ 递归扫描目录内容的函数
    def process_recursive(file_id, file_name, parent_id, pick_code, is_folder, b_type, file_sha1, file_size):
        # 1. 先处理当前节点
        process_node(file_id, file_name, parent_id, pick_code, is_folder, b_type, file_sha1, file_size)
        
        # 2. 如果是目录，且不是删除事件，则拉取里面的所有文件！
        if is_folder and b_type != 22:
            try:
                offset = 0
                while True:
                    res = client.fs_files({'cid': file_id, 'limit': 1000, 'offset': offset})
                    items = res.get('data', [])
                    if not items: break
                    
                    for item in items:
                        c_fid = str(item.get('fid') or item.get('file_id'))
                        c_fname = item.get('fn') or item.get('n') or item.get('file_name')
                        c_pid = file_id
                        c_pc = item.get('pc') or item.get('pick_code')
                        c_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                        c_is_folder = (c_fc == '0')
                        c_sha1 = item.get('sha1') or item.get('sha')
                        c_size = _parse_115_size(item.get('fs') or item.get('size'))
                        
                        # 递归调用
                        process_recursive(c_fid, c_fname, c_pid, c_pc, c_is_folder, b_type, c_sha1, c_size)
                        
                    if len(items) < 1000: break
                    offset += 1000
            except Exception as e:
                logger.error(f"  ➜ 递归拉取目录 {file_name} 失败: {e}")

    try:
        res = client.life_behavior_detail({"limit": 100, "offset": 0})
        
        if res.get('state'):
            records = res.get('data', {}).get('list', [])

            for record in records:
                relation_id = record.get('id')
                
                try:
                    b_type = int(record.get('type', 0))
                except: continue
                
                if b_type not in [2, 6, 14, 22]: continue
                
                file_id = str(record.get('file_id') or '')
                file_name = record.get('file_name') or ''
                parent_id = str(record.get('parent_id') or '')
                pick_code = record.get('pick_code') or ''
                file_sha1 = record.get('sha1') or ''
                file_size = record.get('file_size') or 0
                
                fc = str(record.get('file_category', '1'))
                is_folder = (fc == '0')
                
                if not file_id: continue

                # ★ 调用递归处理函数
                process_recursive(file_id, file_name, parent_id, pick_code, is_folder, b_type, file_sha1, file_size)
                        
                # 映射为 Life API 需要的字符串
                TYPE_MAP = {2: "upload_file", 6: "move_file", 14: "receive_files", 22: "delete_file"}
                b_type_str = TYPE_MAP.get(b_type, str(b_type))
                
                events_to_delete.append({"relation_id": relation_id, "behavior_type": b_type_str})

    except Exception as e:
        logger.error(f"  ➜ 获取生活事件异常: {e}", exc_info=True)

    # 4. 批量清空已处理的事件
    if events_to_delete:
        try:
            chunk_size = 50
            for i in range(0, len(events_to_delete), chunk_size):
                chunk = events_to_delete[i:i+chunk_size]
                del_res = client.life_batch_delete(chunk)
                if not del_res.get('state'):
                    logger.warning(f"  ➜ 清空生活事件失败: {del_res}")
            logger.debug(f"  ➜ 成功清空 {len(events_to_delete)} 条已处理的生活事件。")
        except Exception as e:
            logger.error(f"  ➜ 清空生活事件异常: {e}")

    update_progress(100, f"=== 增量检查完成！新增/移动: {added_count}, 删除: {deleted_count} ===")

# ======================================================================
# ★★★ 后台守护线程：定时触发生活事件监控 ★★★
# ======================================================================
class LifeEventMonitorDaemon:
    _timer = None
    _lock = threading.Lock()

    @classmethod
    def start_or_update(cls):
        with cls._lock:
            if cls._timer:
                cls._timer.cancel()
                cls._timer = None
                
            config = get_config()
            if config.get(constants.CONFIG_OPTION_115_LIFE_MONITOR_ENABLED, False):
                interval_mins = config.get(constants.CONFIG_OPTION_115_LIFE_MONITOR_INTERVAL, 5)
                interval_secs = max(5, interval_mins) * 60 # 最少 5 分钟
                
                logger.info(f"  ⏱️ [守护进程] 115 生活事件监控已启动，间隔: {interval_mins} 分钟。")
                cls._schedule_next(interval_secs)

    @classmethod
    def _schedule_next(cls, interval_secs):
        cls._timer = threading.Timer(interval_secs, cls._run_task, args=(interval_secs,))
        cls._timer.daemon = True
        cls._timer.start()

    @classmethod
    def _run_task(cls, interval_secs):
        # ★ 增加心跳日志，证明守护线程活着
        logger.info("  💓 [守护进程] 定时触发 115 生活事件监控...")
        try:
            task_monitor_115_life_events()
        except Exception as e:
            logger.error(f"生活事件监控守护线程异常: {e}")
        finally:
            with cls._lock:
                if get_config().get(constants.CONFIG_OPTION_115_LIFE_MONITOR_ENABLED, False):
                    cls._schedule_next(interval_secs)