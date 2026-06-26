# routes/webhook.py

import collections
import threading
import time
import os
import re
import json
import random
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from typing import Optional, List
from gevent import spawn_later, spawn, sleep
from gevent.event import Event
from gevent.lock import Semaphore

import task_manager
import handler.emby as emby
from handler.p115_copy_play import cleanup_for_playback_stop
from handler import p115_play_pool
import config_manager
import constants
import handler.telegram as telegram
import extensions
from extensions import SYSTEM_UPDATE_MARKERS, SYSTEM_UPDATE_LOCK, RECURSION_SUPPRESSION_WINDOW, DELETING_COLLECTIONS, UPDATING_IMAGES, UPDATING_METADATA
from core_processor import MediaProcessor
from tasks.watchlist import task_process_watchlist
from tasks.users import task_auto_sync_template_on_policy_change
from tasks.media import task_sync_all_metadata
from handler.custom_collection import RecommendationEngine
from handler import tmdb_collections as collections_handler
from services.cover_generator import CoverGeneratorService
from database import custom_collection_db, tmdb_collection_db, settings_db, user_db, maintenance_db, media_db, queries_db, watchlist_db
from database.connection import get_db_connection
from database.log_db import LogDBManager
from handler.p115_service import P115Service, SmartOrganizer, get_config
try:
    from p115client import P115Client
except ImportError:
    P115Client = None
import logging
logger = logging.getLogger(__name__)

# 创建一个新的蓝图
webhook_bp = Blueprint('webhook_bp', __name__)

# --- 模块级变量 ---
WEBHOOK_BATCH_QUEUE = collections.deque()
WEBHOOK_BATCH_LOCK = threading.Lock()
WEBHOOK_BATCH_DEBOUNCE_TIME = 30
WEBHOOK_BATCH_DEBOUNCER = None
WEBHOOK_REQUEUE_DELAY = 5
WEBHOOK_PENDING_TASKS = collections.deque()
WEBHOOK_PENDING_TASKS_LOCK = threading.Lock()
WEBHOOK_PENDING_TASKS_DRAINER = None

UPDATE_DEBOUNCE_TIMERS = {}
UPDATE_DEBOUNCE_LOCK = threading.Lock()
UPDATE_DEBOUNCE_TIME = 15
# --- 视频流预检常量 ---
STREAM_CHECK_MAX_RETRIES = 6   # 最大重试次数 
STREAM_CHECK_INTERVAL = 10      # 每次轮询间隔(秒)
STREAM_CHECK_SEMAPHORE = Semaphore(5) # 限制并发预检的数量，防止大量入库时查挂 Emby
# 神医 API 专属排队锁 (严格串行，防止 Emby 429 报错)
SYNDROME_API_LOCK = Semaphore(1)


# --- MP 单文件上传智能合并缓冲池 ---
MP_BATCH_QUEUE = {}
MP_BATCH_LOCK = threading.Lock()


def _should_skip_non_etk_strm_webhook(item_type: str, item_name: str, item_path: str) -> bool:
    """Webhook 只处理 ETK 自己生成的 STRM，避免第三方 STRM 进入整理/刮削链路。"""
    if str(item_type or '') not in {'Movie', 'Episode'}:
        return False
    path = str(item_path or '').strip()
    if not path.lower().endswith('.strm'):
        return False
    try:
        from monitor_service import _is_etk_standard_strm
        if _is_etk_standard_strm(path):
            return False
    except Exception as e:
        logger.warning(f"  ➜ [Webhook] STRM 标准校验失败，已跳过：{item_name or os.path.basename(path)}，原因：{e}")
        return True
    logger.warning(f"  ➜ [Webhook] 非 ETK 标准 STRM，已跳过：{item_name or os.path.basename(path)}")
    return True


def _submit_webhook_media_task(
    task_name,
    *,
    task_function=None,
    processor_type='media',
    from_pending_queue=False,
    **kwargs,
):
    task_function = task_function or _handle_full_processing_flow
    task_payload = {
        "task_name": task_name,
        "task_function": task_function,
        "processor_type": processor_type,
        "kwargs": dict(kwargs),
    }
    submitted = task_manager.submit_task(
        task_function,
        task_name=task_name,
        processor_type=processor_type,
        **kwargs,
    )
    if submitted:
        if from_pending_queue:
            logger.info(f"  ➜ [Webhook队列] 任务 '{task_name}' 已从待提交队列成功分派。")
        return True

    if from_pending_queue:
        logger.debug(f"  ➜ [Webhook队列] 任务 '{task_name}' 分派时媒体任务仍繁忙，稍后继续尝试。")
        return False

    logger.info(f"  ➜ [Webhook队列] 任务 '{task_name}' 因媒体任务繁忙，已加入待提交队列。")
    _enqueue_pending_webhook_task(task_payload)
    return False


def _is_same_pending_webhook_task(existing_task, new_task):
    return (
        existing_task.get("task_name") == new_task.get("task_name")
        and existing_task.get("processor_type") == new_task.get("processor_type")
        and existing_task.get("task_function") == new_task.get("task_function")
        and existing_task.get("kwargs") == new_task.get("kwargs")
    )


def _schedule_pending_webhook_drain(delay=WEBHOOK_REQUEUE_DELAY):
    global WEBHOOK_PENDING_TASKS_DRAINER
    with WEBHOOK_PENDING_TASKS_LOCK:
        if WEBHOOK_PENDING_TASKS_DRAINER is not None:
            return
        WEBHOOK_PENDING_TASKS_DRAINER = spawn_later(delay, _drain_pending_webhook_tasks)


def _enqueue_pending_webhook_task(task_payload):
    with WEBHOOK_PENDING_TASKS_LOCK:
        for pending_task in WEBHOOK_PENDING_TASKS:
            if _is_same_pending_webhook_task(pending_task, task_payload):
                logger.debug(f"  ➜ [Webhook队列] 任务 '{task_payload['task_name']}' 已在待提交队列中，跳过重复入队。")
                break
        else:
            WEBHOOK_PENDING_TASKS.append(task_payload)
            logger.info(
                f"  ➜ [Webhook队列] 当前待提交任务数: {len(WEBHOOK_PENDING_TASKS)} "
                f"(最新: {task_payload['task_name']})"
            )

    _schedule_pending_webhook_drain()


def _drain_pending_webhook_tasks():
    global WEBHOOK_PENDING_TASKS_DRAINER
    try:
        while True:
            with WEBHOOK_PENDING_TASKS_LOCK:
                if not WEBHOOK_PENDING_TASKS:
                    return
                task_payload = WEBHOOK_PENDING_TASKS[0]

            submitted = _submit_webhook_media_task(
                task_payload["task_name"],
                task_function=task_payload["task_function"],
                processor_type=task_payload["processor_type"],
                from_pending_queue=True,
                **task_payload["kwargs"],
            )
            if not submitted:
                return

            with WEBHOOK_PENDING_TASKS_LOCK:
                if WEBHOOK_PENDING_TASKS and _is_same_pending_webhook_task(WEBHOOK_PENDING_TASKS[0], task_payload):
                    WEBHOOK_PENDING_TASKS.popleft()
    finally:
        with WEBHOOK_PENDING_TASKS_LOCK:
            WEBHOOK_PENDING_TASKS_DRAINER = None
            has_pending_tasks = bool(WEBHOOK_PENDING_TASKS)
        if has_pending_tasks:
            _schedule_pending_webhook_drain()


def _first_mp_detail_value(data, *keys):
    for key in keys:
        value = data.get(key)
        if value not in (None, '', [], {}):
            return value
    return None


def _normalize_mp_detail_size(value):
    if value in (None, '', [], {}):
        return None
    try:
        if isinstance(value, (int, float)):
            size = int(value)
            return size if size > 0 else None
        text = str(value).strip().replace(',', '')
        if not text:
            return None
        if re.fullmatch(r'\d+(?:\.\d+)?', text):
            size = int(float(text))
            return size if size > 0 else None
    except Exception:
        return None
    return None


def _refresh_mp_file_info_from_115(client, file_info):
    """Use 115 file detail as the authority for MP webhook file identity."""
    try:
        file_id = file_info.get('file_id')
        if not file_id:
            return

        info_res = client.fs_get_info(file_id)
        if info_res and info_res.get('state') and info_res.get('data'):
            data = info_res['data']
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                return

            detail_size = _normalize_mp_detail_size(
                _first_mp_detail_value(data, 'size_byte', 'fs', 'file_size', 'size', 's')
            )
            if detail_size:
                old_size = _normalize_mp_detail_size(file_info.get('size') or file_info.get('fs'))
                file_info['size'] = detail_size
                file_info['fs'] = detail_size
                if old_size and old_size != detail_size:
                    logger.warning(
                        f"  ➜ [MP上传] 已用 115 实时详情修正文件大小: "
                        f"fid={file_id}, {old_size} -> {detail_size} | {file_info.get('name')}"
                    )
                else:
                    logger.debug(
                        f"  ➜ [MP上传] 已采用 115 实时详情文件大小: "
                        f"fid={file_id}, size={detail_size} | {file_info.get('name')}"
                    )

            detail_sha1 = _first_mp_detail_value(data, 'sha1', 'sha', 'file_sha1')
            if detail_sha1:
                file_info['sha1'] = str(detail_sha1).strip().upper()

            detail_pickcode = _first_mp_detail_value(data, 'pc', 'pick_code', 'pickcode')
            if detail_pickcode:
                file_info['pickcode'] = str(detail_pickcode).strip()

            detail_name = _first_mp_detail_value(data, 'fn', 'n', 'file_name', 'name')
            if detail_name and not file_info.get('name'):
                file_info['name'] = str(detail_name)

            real_parent_id = data.get('parent_id') or data.get('pid') or data.get('cid')
            if not real_parent_id and 'paths' in data and isinstance(data['paths'], list) and len(data['paths']) > 0:
                last_path_node = data['paths'][-1]
                real_parent_id = last_path_node.get('file_id') or last_path_node.get('cid')
            
            if real_parent_id and str(real_parent_id) != str(file_info.get('parent_id')):
                old_parent_id = file_info.get('parent_id')
                file_info['parent_id'] = str(real_parent_id)
                logger.info(
                    f"  ➜ [MP上传] 父目录已修正: "
                    f"{old_parent_id} -> {real_parent_id} | {file_info.get('name')}"
                )
    except Exception as e:
        logger.warning(f"  ➜ [MP上传] 查询 115 文件详情失败，沿用 MP 通知字段: {file_info.get('name')} -> {e}")

def _flush_mp_batch(key):
    """缓冲结束，将收集到的同集视频和字幕打包送入核心处理"""
    with MP_BATCH_LOCK:
        if key not in MP_BATCH_QUEUE:
            return
        task = MP_BATCH_QUEUE.pop(key)

    files = task.get('files') or []
    if not files:
        return

    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [MP合并整理] 115 客户端未初始化，任务取消。")
        return

    tmdb_id, media_type, season_num, episode_num = key
    title = files[0].get('title') or ''

    video_text = "包含视频" if task.get('has_video', False) else "仅字幕或附属文件"
    logger.info(
        f"  ➜ [MP合并整理] 缓冲结束，开始处理 {len(files)} 个文件，{video_text}，TMDb：{tmdb_id}"
    )

    try:
        organizer = SmartOrganizer(client, tmdb_id, media_type, title)

        if season_num is not None and str(season_num).isdigit():
            organizer.forced_season = int(season_num)

        file_nodes = []
        for f in files:
            _refresh_mp_file_info_from_115(client, f)
            file_nodes.append({
                'fid': f.get('file_id'),
                'file_id': f.get('file_id'),
                'fn': f.get('name'),
                'file_name': f.get('name'),
                'fc': '1',
                'type': '1',
                'pid': f.get('parent_id'),
                'parent_id': f.get('parent_id'),
                'pc': f.get('pickcode'),
                'pick_code': f.get('pickcode'),
                'sha1': f.get('sha1'),
                'size': f.get('size'),
                'fs': f.get('fs') or f.get('size'),
                '115_path': f.get('115_path'), # ★ 核心新增：将 115 物理路径传递给底层
                '_forced_season': f.get('season_num'),
                '_forced_episode': f.get('episode_num'),
                '_skip_gc': True,   
                '_from_mp': True    
            })

        config = get_config()
        mp_classify_enabled = bool(config.get(constants.CONFIG_OPTION_115_MP_CLASSIFY, False))

        if mp_classify_enabled:
            logger.info("  ➜ [MP直出] MP分类已开启：跳过整理/归类/重命名，直接生成 STRM 和 -mediainfo.json。")
            ok = organizer.execute_mp_passthrough(file_nodes)
            if not ok:
                logger.warning("  ➜ [MP直出] 直出处理未完全成功。")
        else:
            target_cid = organizer.get_target_cid(
                season_num=organizer.forced_season if hasattr(organizer, 'forced_season') else None
            )

            if target_cid:
                organizer.execute(file_nodes, target_cid)
            else:
                logger.info("  ➜ [MP合并整理] 未命中分类规则，保持原样。")

        from handler.p115_service import P115DeleteBuffer
        P115DeleteBuffer.add(check_save_path=True)

    except Exception as e:
        logger.error(f"  ➜ [MP合并整理] 失败: {e}", exc_info=True)

def _process_mp_passthrough_immediate(file_info):
    """MP直出模式：跳过缓冲，直接处理单文件"""
    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [MP直出] 115 客户端未初始化，任务取消。")
        return

    tmdb_id = file_info.get('tmdb_id')
    media_type = file_info.get('media_type')
    title = file_info.get('title') or ''
    file_name = file_info.get('name')

    _refresh_mp_file_info_from_115(client, file_info)
    file_name = file_info.get('name') or file_name

    logger.info(f"  ➜ [MP直出] 开始处理单文件：{file_name}。")
    logger.debug(f"  ➜ [MP直出] 单文件处理详情：TMDb={tmdb_id}, 类型={media_type}")

    try:
        organizer = SmartOrganizer(client, tmdb_id, media_type, title)
        season_num = file_info.get('season_num')
        if season_num is not None and str(season_num).isdigit():
            organizer.forced_season = int(season_num)

        file_nodes = [{
            'fid': file_info.get('file_id'),
            'file_id': file_info.get('file_id'),
            'fn': file_name,
            'file_name': file_name,
            'fc': '1',
            'type': '1',
            'pid': file_info.get('parent_id'),
            'parent_id': file_info.get('parent_id'),
            'pc': file_info.get('pickcode'),
            'pick_code': file_info.get('pickcode'),
            'sha1': file_info.get('sha1'),
            'size': file_info.get('size'),
            'fs': file_info.get('fs') or file_info.get('size'),
            '115_path': file_info.get('115_path'),
            '_forced_season': season_num,
            '_forced_episode': file_info.get('episode_num'),
            '_skip_gc': True,   
            '_from_mp': True    
        }]

        ok = organizer.execute_mp_passthrough(file_nodes)
        if not ok:
            logger.warning("  ➜ [MP直出] 直出处理未完全成功。")

        from handler.p115_service import P115DeleteBuffer
        P115DeleteBuffer.add(check_save_path=True)

    except Exception as e:
        logger.error(f"  ➜ [MP直出] 失败: {e}", exc_info=True)

def _enqueue_mp_file(file_info):
    """将 MP 上传的文件加入缓冲池 (视频叫醒字幕机制)"""
    with MP_BATCH_LOCK:
        # 以 TMDB ID + 季号 + 集号 作为唯一批次 Key
        key = (file_info['tmdb_id'], file_info['media_type'], file_info.get('season_num'), file_info.get('episode_num'))
        
        if key not in MP_BATCH_QUEUE:
            MP_BATCH_QUEUE[key] = {
                'files': [],
                'timer': None,
                'has_video': False
            }
        
        task = MP_BATCH_QUEUE[key]
        task['files'].append(file_info)
        
        file_name = file_info['name']
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        is_video = ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']
        
        if is_video:
            task['has_video'] = True
        
        # 只要有新文件进来，就重置计时器
        if task['timer'] is not None:
            task['timer'].kill()
        
        # ★ 核心机制：如果视频到了，只等 5 秒(防并发)就发车；如果只有字幕，最多等 2 小时！
        delay = 5.0 if task['has_video'] else 7200.0
        
        logger.info(f"  ➜ [MP缓冲] 文件 '{file_name}' 加入队列。当前批次 {len(task['files'])} 个文件。最多等待 {delay} 秒后合并执行...")
        task['timer'] = spawn_later(delay, _flush_mp_batch, key)


def _shared_resource_auto_share_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        value = cfg.get('p115_shared_resource_enabled', False)
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
        return bool(value)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取共享资源总开关失败，跳过自动登记: {e}")
        return False


def _run_shared_auto_share_batch_detached(task_name: str, register_items: List[dict]):
    """共享供给侧登记必须脱离 task_manager 单线程队列。

    Webhook 本身已经运行在 task_manager 的单 worker + task_lock 中。
    如果这里再 submit_task，会在同一线程内二次获取 task_lock，导致 Webhook 任务假死。

    Rapid v2 不再判断“是否有人需要”。只要共享资源开关已启用，
    媒体入库完成并补齐指纹后，就立即把本机秒传源登记到中心。
    """
    items = [dict(x or {}) for x in (register_items or []) if isinstance(x, dict)]
    if not items:
        return

    if not _shared_resource_auto_share_enabled():
        logger.debug(f"  ➜ [共享资源] 共享资源未启用，跳过自动登记: {task_name}")
        return

    def _runner():
        created_total = 0
        failed_total = 0
        try:
            from tasks.shared_resource_tasks import (
                trigger_shared_rapid_register_batch_for_library_items,
                trigger_shared_rapid_register_for_library_item,
            )

            logger.info(f"  ➜ [共享资源] 入库后自动登记共享源：{task_name}，共 {len(items)} 个文件。")
            if len(items) > 1:
                batch_result = trigger_shared_rapid_register_batch_for_library_items(None, items) or {}
                try:
                    created_total += int(batch_result.get('created', 0) or 0)
                except Exception:
                    pass
                failed_total += int(batch_result.get('failed', 0) or 0)
                raw_result = batch_result.get('raw_batch_result') or {}
                logger.info(
                    "  ➜ [共享资源] 媒体信息预上传完成：%s，本次上传 %s 个，中心已有 %s 个，可用于秒传校验 %s 个。",
                    task_name,
                    raw_result.get('uploaded_count') or 0,
                    raw_result.get('skipped_existing') or 0,
                    raw_result.get('count') or 0,
                )
                logger.info(
                    "  ➜ [共享资源] 入库共享源登记完成：%s，共 %s 个文件，成功 %s 个，失败 %s 个。",
                    task_name,
                    len(items),
                    created_total,
                    failed_total,
                )
                return
            for item in items:
                try:
                    result = trigger_shared_rapid_register_for_library_item(None, **item) or {}
                    try:
                        created_total += int(result.get('created', 0) or result.get('registered_count', 0) or 0)
                    except Exception:
                        pass
                    if not result.get('ok'):
                        failed_total += 1
                        logger.debug(
                            "  ➜ [共享资源] 入库登记未成功: %s，message=%s",
                            item.get('emby_item_id') or item.get('tmdb_id') or '-',
                            result.get('message', '') if isinstance(result, dict) else '',
                        )
                except Exception as item_err:
                    failed_total += 1
                    logger.warning(
                        "  ➜ [共享资源] 入库登记单项失败: %s -> %s",
                        item.get('emby_item_id') or item.get('tmdb_id') or '-',
                        item_err,
                        exc_info=True,
                    )

            logger.info(
                "  ➜ [共享资源] 入库共享源登记完成：%s，共 %s 个文件，成功 %s 个，失败 %s 个。",
                task_name,
                len(items),
                created_total,
                failed_total,
            )
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 自动登记失败: {task_name} -> {e}", exc_info=True)

    threading.Thread(
        target=_runner,
        name=f"shared-rapid-register-{str(task_name)[:40]}",
        daemon=True,
    ).start()


def _run_shared_auto_share_detached(task_name: str, **kwargs):
    _run_shared_auto_share_batch_detached(task_name, [kwargs])


def _submit_shared_auto_share_after_library_ready(
    item_details: dict,
    item_id: str,
    item_type: str,
    tmdb_id: str,
    *,
    new_episode_ids: Optional[List[str]] = None,
):
    """媒体入库完成后，异步登记 Rapid v2 共享源。

    新逻辑下客户端不再负责季包一致性和完结季成包判断：
    - Movie：入库完成即登记电影源；
    - Series + new_episode_ids：入库完成即登记本轮新增分集源；
    - 中心端根据单集资产池自行归类、凑整季、判定 pool_complete。
    """
    try:
        if not tmdb_id:
            return

        title = item_details.get('Name') or ''
        year = item_details.get('ProductionYear') or ''

        if item_type == 'Movie':
            _run_shared_auto_share_batch_detached(
                f"Rapid电影共享源登记: {title or tmdb_id}",
                [{
                    'item_type': 'Movie',
                    'tmdb_id': str(tmdb_id),
                    'emby_item_id': str(item_id),
                    'title': title,
                    'year': year,
                }],
            )
            return

        if item_type == 'Series':
            precise_episode_ids = []
            for eid in new_episode_ids or []:
                eid = str(eid or '').strip()
                if eid and eid not in precise_episode_ids:
                    precise_episode_ids.append(eid)
            if not precise_episode_ids:
                return
            _run_shared_auto_share_batch_detached(
                f"Rapid分集共享源登记: {title or tmdb_id}",
                [{
                    'item_type': 'Episode',
                    'emby_item_id': eid,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'title': title,
                    'year': year,
                } for eid in precise_episode_ids],
            )
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 提交 webhook Rapid 共享源登记失败: {e}", exc_info=True)

def _get_processor_local_strm_root(processor) -> str:
    """从 MediaProcessor / 配置中提取本地 STRM 根目录，用于补齐 p115_filesystem_cache.local_path。"""
    candidates = []

    if processor:
        for attr in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            try:
                value = getattr(processor, attr, None)
                if value:
                    candidates.append(value)
            except Exception:
                pass

    try:
        nb_config = get_config() or {}
        for key in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            value = nb_config.get(key)
            if value:
                candidates.append(value)
    except Exception:
        pass

    try:
        app_config = config_manager.APP_CONFIG or {}
        for key in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            value = app_config.get(key)
            if value:
                candidates.append(value)
    except Exception:
        pass

    for value in candidates:
        text = str(value).strip().replace('\\', '/').rstrip('/')
        if text:
            return text

    return ''

def _repair_webhook_p115_fingerprints_for_emby_ids(
    processor,
    item_name_for_log: str,
    emby_item_ids,
    *,
    expected_item_type: Optional[str] = None,
    log_prefix: str = "Webhook指纹补齐",
) -> int:
    """Webhook 入库后按 Emby ID 找 media_metadata 行，并执行 115 指纹体检补齐。"""
    ids = [str(x).strip() for x in (emby_item_ids or []) if str(x or '').strip()]
    if not ids:
        return 0

    try:
        rows_to_repair = []
        seen_keys = set()

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for emby_id in ids:
                    if expected_item_type:
                        cursor.execute(
                            """
                            SELECT *
                            FROM media_metadata
                            WHERE item_type = %s
                              AND emby_item_ids_json::text LIKE %s
                            """,
                            (expected_item_type, f'%"{emby_id}"%')
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT *
                            FROM media_metadata
                            WHERE emby_item_ids_json::text LIKE %s
                            """,
                            (f'%"{emby_id}"%',)
                        )

                    for row in cursor.fetchall() or []:
                        row_dict = dict(row)
                        dedupe_key = (
                            row_dict.get("id"),
                            row_dict.get("tmdb_id"),
                            row_dict.get("parent_series_tmdb_id"),
                            row_dict.get("item_type"),
                            row_dict.get("season_number"),
                            row_dict.get("episode_number"),
                        )
                        if dedupe_key in seen_keys:
                            continue

                        seen_keys.add(dedupe_key)
                        rows_to_repair.append(row_dict)

        if not rows_to_repair:
            logger.debug(
                f"  ➜ [指纹补齐] 未在 media_metadata 中找到可体检记录: "
                f"{item_name_for_log} ids={ids} type={expected_item_type or 'Any'}"
            )
            return 0

        logger.info(
            f"  ➜ [指纹补齐] 正在为《{item_name_for_log}》执行 115 指纹体检，"
            f"共 {len(rows_to_repair)} 条记录。"
        )

        from tasks.p115_fingerprint_helpers import repair_p115_fingerprints_for_rows

        local_root = _get_processor_local_strm_root(processor)
        if not local_root:
            logger.warning(
                "  ➜ [指纹补齐] 未获取到 local_strm_root，本次只能补齐 PC/SHA1/115 缓存基础字段，无法可靠写入 local_path。"
            )

        repair_p115_fingerprints_for_rows(
            processor=processor,
            rows=rows_to_repair,
            local_root=local_root,
            update_db=True,
            allow_api_fetch=True,
            log_prefix=log_prefix,
        )

        return len(rows_to_repair)

    except Exception as e:
        logger.warning(f"  ➜ [指纹补齐] 执行失败: {e}", exc_info=True)
        return 0

def _handle_full_processing_flow(processor: 'MediaProcessor', item_id: str, force_full_update: bool, new_episode_ids: Optional[List[str]] = None, is_new_item: bool = True):
    """
    【Webhook 统一入口】
    统一处理 新入库(New) 和 追更(Update) 两种情况。
    """
    if not processor:
        logger.error(f"  ➜ 完整处理流程中止：核心处理器 (MediaProcessor) 未初始化。")
        return

    item_details = emby.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  ➜ 无法获取项目 {item_id} 的详情，任务中止。")
        return
    
    item_name_for_log = item_details.get("Name", f"ID:{item_id}")
    item_type = item_details.get("Type")
    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")

    # 1. 核心调用：优先执行元数据处理 (process_single_item)
    processed_successfully = processor.process_single_item(
        item_id, 
        force_full_update=force_full_update,
        specific_episode_ids=new_episode_ids 
    )
    
    if not processed_successfully:
        logger.warning(f"  ➜ 项目 '{item_name_for_log}' 的元数据处理未成功完成，跳过后续步骤。")
        return

    # 2. 媒体入库后先做 115 指纹体检，再登记 Rapid 共享源。
    # Rapid 登记依赖 PC/SHA1/FID/缓存字段，体检要放在登记前。
    precise_new_episode_ids = [str(x).strip() for x in (new_episode_ids or []) if str(x or '').strip()]
    if item_type == "Movie":
        _repair_webhook_p115_fingerprints_for_emby_ids(
            processor,
            item_name_for_log,
            [item_id],
            expected_item_type="Movie",
            log_prefix="Webhook电影指纹补齐",
        )
    elif item_type == "Series" and precise_new_episode_ids:
        _repair_webhook_p115_fingerprints_for_emby_ids(
            processor,
            item_name_for_log,
            precise_new_episode_ids,
            expected_item_type="Episode",
            log_prefix="Webhook新集指纹补齐",
        )

    # 3. 共享资源供给侧实时触发：电影/本轮新增分集均在 Webhook 入库完成后登记；中心端负责后续整季归类。
    _submit_shared_auto_share_after_library_ready(
        item_details,
        item_id,
        item_type,
        tmdb_id,
        new_episode_ids=precise_new_episode_ids,
    )

    # 3. 智能追剧判断 - 初始入库
    if is_new_item and item_type == "Series":
        processor.check_and_add_to_watchlist(item_details)

    # 3. 后续处理
    if is_new_item:
        try:
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            item_name = item_details.get("Name", f"ID:{item_id}")
            
            # --- 匹配 List (榜单) 类型的合集 (保持不变) ---
            # 榜单类合集是静态的，需要将新入库的项目加入到 Emby 实体合集中
            if tmdb_id:
                updated_list_collections = custom_collection_db.match_and_update_list_collections_on_item_add(
                    new_item_tmdb_id=tmdb_id,
                    new_item_emby_id=item_id,
                    new_item_name=item_name
                )
                
                if updated_list_collections:
                    logger.info(f"  ➜ 《{item_name}》匹配到 {len(updated_list_collections)} 个榜单类合集，正在追加...")
                    for collection_info in updated_list_collections:
                        emby.append_item_to_collection(
                            collection_id=collection_info['emby_collection_id'],
                            item_emby_id=item_id,
                            base_url=processor.emby_url,
                            api_key=processor.emby_api_key,
                            user_id=processor.emby_user_id
                        )

            # ★★★ 移除 Filter 类合集的匹配逻辑 ★★★
            # Filter 类合集现在是基于 SQL 实时查询的，不需要在入库时做任何操作。
            # 只要 media_metadata 表更新了（process_single_item 已完成），SQL 查询自然能查到它。

        except Exception as e:
            logger.error(f"  ➜ 为新入库项目 '{item_name_for_log}' 匹配榜单合集时发生意外错误: {e}", exc_info=True)

        # --- 封面生成逻辑 (保持不变) ---
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
                # ... (获取 library_info 的逻辑) ...
                library_info = emby.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                if library_info:
                    library_id = library_info.get("Id")
                    library_name = library_info.get("Name", library_id)
                    
                    if library_info.get('CollectionType') in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                        server_id = 'main_emby'
                        library_unique_id = f"{server_id}-{library_id}"
                        if library_unique_id not in cover_config.get("exclude_libraries", []):
                            # ... (获取 item_count) ...
                            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
                            collection_type = library_info.get('CollectionType')
                            item_type_to_query = TYPE_MAP.get(collection_type)
                            item_count = 0
                            if library_id and item_type_to_query:
                                item_count = emby.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0

                            logger.info(f"  ➜ 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
                            cover_service = CoverGeneratorService(config=cover_config)
                            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)

            # ★★★ 移除 update_user_caches_on_item_add 调用 ★★★
            # 权限现在是实时的，不需要补票了。

        except Exception as e:
            logger.error(f"  ➜ 在新入库后执行封面生成时发生错误: {e}", exc_info=True)

        # ======================================================================
        # ★★★  TMDb 合集自动补全 ★★★
        # ======================================================================
        try:
            # 1. 检查类型 (只处理电影)
            # ★★★ 修复：直接使用 item_details 和 tmdb_id，不再依赖 item_metadata ★★★
            current_type = item_details.get('Type')
            current_tmdb_id = tmdb_id  # 这个变量在函数前面已经定义过了
            current_name = item_name   # 这个变量在函数前面也定义过了

            if current_type == 'Movie' and current_tmdb_id:
                # 2. 检查开关
                config = settings_db.get_setting('native_collections_config') or {}
                is_auto_complete_enabled = config.get('auto_complete_enabled', False)

                if is_auto_complete_enabled:
                    logger.trace(f"  ➜ 正在检查电影 '{current_name}' 所属 TMDb 合集...")
                    # 直接调用 handler
                    collections_handler.check_and_subscribe_collection_from_movie(
                        movie_tmdb_id=str(current_tmdb_id),
                        movie_name=current_name,
                        movie_emby_id=item_id
                    )
        except Exception as e:
            logger.warning(f"  ➜ 检查所属 TMDb 合集时发生错误: {e}")

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

    # 4. ★★★ 通知分流 ★★★
    try:
        # 如果提供了 new_episode_ids，说明是追更通知
        # 如果 is_new_item 为 True，说明是新入库通知
        notif_type = 'update' if (new_episode_ids and not is_new_item) else 'new'
        
        telegram.send_media_notification(
            item_details=item_details, 
            notification_type=notif_type, 
            new_episode_ids=new_episode_ids
        )
    except Exception as e:
        logger.error(f"触发通知失败: {e}")

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

    # 打标
    if is_new_item: 
        try:
            # 1. 从数据库获取最新记录
            db_record = media_db.get_media_details(str(tmdb_id), item_type)
            
            if db_record:
                # 2. 提取 Library ID
                # asset_details_json 是一个列表，取第一个即可
                assets = db_record.get('asset_details_json')
                lib_id = None
                if assets and isinstance(assets, list) and len(assets) > 0:
                    lib_id = assets[0].get('source_library_id')
                
                # 3. 提取修正后的分级 (US)
                # official_rating_json: {"US": "XXX", "DE": "18"}
                ratings = db_record.get('official_rating_json')
                us_rating = None
                if ratings and isinstance(ratings, dict):
                    us_rating = ratings.get('US')
                
                if lib_id:
                    # 既然数据都在手里了，不需要延迟，直接干！
                    logger.info(f"  ➜ [自动打标] 基于数据库最新元数据执行自动打标，分级：{us_rating}。")
                    logger.debug(f"  ➜ [自动打标] 媒体库 ID：{lib_id}")
                    # 这里的 lib_name 传个占位符即可，不影响逻辑，只影响日志
                    _handle_immediate_tagging_with_lib(item_id, item_name_for_log, lib_id, "DB_Source", known_rating=us_rating)
                else:
                    logger.warning(f"  ➜ [自动打标] 数据库记录中未找到 来源库，跳过打标。")
            else:
                logger.warning(f"  ➜ [自动打标] 无法从数据库读取刚写入的记录，跳过打标。")

        except Exception as e:
            logger.warning(f"  ➜ [自动打标] 触发打标失败: {e}")

    # 刷新智能追剧状态 
    if item_type == "Series" and tmdb_id:
        def _async_trigger_watchlist():
            try:
                watching_ids = watchlist_db.get_watching_tmdb_ids()
                
                # ★★★ 核心破局点：打破“不见兔子不撒鹰”的死锁 ★★★
                is_watching = str(tmdb_id) in watching_ids
                has_new_episodes = bool(new_episode_ids) # 明确有新集物理文件入库
                
                # 如果既不在追剧列表中，又没有新集入库，才真正跳过
                if not is_watching and not has_new_episodes:
                    logger.debug(f"  ➜ [智能追剧] 剧集 《{item_name_for_log}》 当前不在追剧列表中，且无新集触发，跳过刷新。")
                    return
                
                # 如果不在追剧列表中，但是有新集入库 -> 新季复活，强制唤醒！
                if not is_watching and has_new_episodes:
                    logger.info(f"  ➜ [智能追剧]  《{item_name_for_log}》 检测到有新集入库，重新开始追剧！")

                # =======================================================

                precise_new_episode_ids = [str(x).strip() for x in (new_episode_ids or []) if str(x or '').strip()]

                # 新集指纹体检与共享源登记均已在 Webhook 中完成；watchlist_processor 只负责追剧状态刷新。
                refresh_scope_text = (
                    f"本次只刷新 {len(precise_new_episode_ids)} 个新增分集。"
                    if precise_new_episode_ids
                    else "本次刷新整部剧集。"
                )
                logger.info(
                    f"  ➜ [智能追剧] 触发单项刷新：{refresh_scope_text}"
                )
                task_manager.submit_task(
                    task_process_watchlist,
                    task_name=f"刷新智能追剧: 《{item_name_for_log}》",
                    processor_type='watchlist', 
                    tmdb_id=str(tmdb_id),
                    new_episode_ids=precise_new_episode_ids or None
                )
            except Exception as e:
                logger.error(f"  ➜ 触发智能追剧任务失败: {e}")

        # 启动协程，不等待结果，直接让当前 Webhook 任务结束
        spawn(_async_trigger_watchlist)

def _handle_immediate_tagging_with_lib(item_id, item_name, lib_id, lib_name, known_rating=None):
    """
    自动打标 (支持分级过滤)。
    增加 known_rating 参数：如果调用方已经知道确切分级（如从数据库查到的），直接使用，不再查询 Emby。
    """
    try:
        processor = extensions.media_processor_instance
        tagging_config = settings_db.get_setting('auto_tagging_rules') or []
        
        # 只有当没有传入 known_rating 时，才需要去 Emby 查
        item_details = None 
        
        for rule in tagging_config:
            target_libs = rule.get('library_ids', [])
            if not target_libs or lib_id in target_libs:
                tags = rule.get('tags', [])
                rating_filters = rule.get('rating_filters', [])
                
                if tags:
                    # ★★★ 核心修改：分级匹配逻辑 ★★★
                    if rating_filters:
                        # 1. 优先使用传入的已知分级 (数据库里的真理)
                        current_rating = known_rating
                        
                        # 2. 如果没传，且还没查过 Emby，则去查 (兜底逻辑)
                        if not current_rating and item_details is None:
                            item_details = emby.get_emby_item_details(
                                item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                                fields="OfficialRating"
                            )
                            if item_details:
                                current_rating = item_details.get('OfficialRating')
                        
                        # 3. 执行匹配
                        if not current_rating:
                            continue # 拿不到分级，跳过
                            
                        target_codes = queries_db._expand_rating_labels(rating_filters)
                        
                        # 兼容 "US: XXX" 和 "XXX" 两种格式
                        rating_code = current_rating.split(':')[-1].strip()
                        
                        if rating_code not in target_codes:
                            logger.debug(f"  ➜ 媒体项 '{item_name}' 分级 '{current_rating}' 不满足规则限制 {rating_filters}，跳过打标。")
                            continue

                    if rating_filters:
                        rule_desc = f"分级 '{','.join(rating_filters)}'"
                    else:
                        rule_desc = f"库 '{lib_name}'"

                    logger.info(f"  ➜ 媒体项 '{item_name}' 命中 {rule_desc} 规则，追加标签: {tags}")
                    emby.add_tags_to_item(item_id, tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                break 
    except Exception as e:
        logger.error(f"  ➜ [自动打标] 失败: {e}")

# --- 辅助函数 ---
def _process_batch_webhook_events():
    global WEBHOOK_BATCH_DEBOUNCER
    with WEBHOOK_BATCH_LOCK:
        items_in_batch = list(set(WEBHOOK_BATCH_QUEUE))
        WEBHOOK_BATCH_QUEUE.clear()
        WEBHOOK_BATCH_DEBOUNCER = None

    if not items_in_batch:
        return

    logger.info(f"  ➜ 防抖计时器到期，开始批量处理 {len(items_in_batch)} 个 Emby Webhook 新增/入库事件。")

    parent_items = collections.defaultdict(lambda: {
        "name": "", "type": "", "episode_ids": set()
    })
    
    for item_id, item_name, item_type in items_in_batch:
        parent_id = item_id
        parent_name = item_name
        parent_type = item_type
        
        if item_type == "Episode":
            series_id = emby.get_series_id_from_child_id(
                item_id, extensions.media_processor_instance.emby_url,
                extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=item_name
            )
            if not series_id:
                logger.warning(f"  ➜ 批量处理中，分集 '{item_name}' 未找到所属剧集，跳过。")
                continue
            
            parent_id = series_id
            parent_type = "Series"
            
            # 将具体的分集ID添加到记录中
            parent_items[parent_id]["episode_ids"].add(item_id)
            
            # 更新父项的名字（只需一次）
            if not parent_items[parent_id]["name"]:
                series_details = emby.get_emby_item_details(parent_id, extensions.media_processor_instance.emby_url, extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, fields="Name")
                parent_items[parent_id]["name"] = series_details.get("Name", item_name) if series_details else item_name
        else:
            # 如果事件是电影或剧集容器本身，也记录下来
            parent_items[parent_id]["name"] = parent_name
        
        # 更新父项的类型
        parent_items[parent_id]["type"] = parent_type

    logger.info(f"  ➜ 批量事件去重后，将为 {len(parent_items)} 个独立媒体项分派任务。")

    for parent_id, item_info in parent_items.items():
        parent_name = item_info['name']
        parent_type = item_info['type']
        episode_ids = list(item_info["episode_ids"])
        
        # 1. 检查是否已处理
        is_already_processed = parent_id in extensions.media_processor_instance.processed_items_cache

        # 2. 检查数据库是否在线 (处理“僵尸数据”)
        if is_already_processed:
            # 这一步很快，只是查一下 media_metadata 表的 in_library 字段
            is_online_in_db = media_db.is_emby_id_in_library(parent_id)
            
            # ★★★ 优化核心：如果不在线，直接踢出缓存，视为新项目重跑 ★★★
            if not is_online_in_db:
                logger.info(f"  ➜ 缓存命中 '{parent_name}'，但数据库标记为离线/缺失。清除缓存，触发重新入库流程。")
                
                # 从内存缓存中移除
                if parent_id in extensions.media_processor_instance.processed_items_cache:
                    del extensions.media_processor_instance.processed_items_cache[parent_id]
                
                # 标记为未处理，后续逻辑会把它当作“新入库”来执行完整的数据库修复
                is_already_processed = False
        # 3. 统一分派任务
        task_name_prefix = "Webhook追更" if is_already_processed and episode_ids else "Webhook入库"
        
        logger.info(f"  ➜ 为 '{parent_name}' 分派任务: {task_name_prefix} (分集数: {len(episode_ids)})")
        
        _submit_webhook_media_task(
            f"{task_name_prefix}: {parent_name}",
            item_id=parent_id,
            force_full_update=False,
            new_episode_ids=episode_ids if episode_ids else None,
            is_new_item=not is_already_processed
        )

    logger.info("  ➜ 所有 Webhook 批量任务已完成分派或进入待提交队列。")

def _trigger_metadata_update_task(item_id, item_name):
    """触发元数据同步任务"""
    logger.info(f"  ➜ 防抖计时器到期，开始同步《{item_name}》的元数据缓存。")
    logger.debug(f"  ➜ 元数据缓存同步对象：item_id={item_id}")
    task_manager.submit_task(
        task_sync_all_metadata,
        task_name=f"元数据同步: {item_name}",
        processor_type='media',
        item_id=item_id,
        item_name=item_name
    )

def _enqueue_webhook_event(item_id, item_name, item_type):
    """
    将事件加入批量处理队列，并管理防抖计时器 (滑动窗口防抖)。
    """
    global WEBHOOK_BATCH_DEBOUNCER
    with WEBHOOK_BATCH_LOCK:
        WEBHOOK_BATCH_QUEUE.append((item_id, item_name, item_type))
        logger.debug(f"  ➜ [队列] 项目 '{item_name}' ({item_type}) 已加入处理队列。当前积压: {len(WEBHOOK_BATCH_QUEUE)}")
        
        # ★★★ 核心修复：滑动窗口防抖 ★★★
        # 只要有新文件进来，就无情地杀掉旧的计时器，重新开始 30 秒倒计时！
        if WEBHOOK_BATCH_DEBOUNCER is not None:
            WEBHOOK_BATCH_DEBOUNCER.kill()
            logger.debug("  ➜ [队列] 检测到连续入库，已重置批量处理计时器。")
            
        logger.info(f"  ➜ [队列] 启动批量处理计时器，将在 {WEBHOOK_BATCH_DEBOUNCE_TIME} 秒后执行。")
        WEBHOOK_BATCH_DEBOUNCER = spawn_later(WEBHOOK_BATCH_DEBOUNCE_TIME, _process_batch_webhook_events)

def _dispatch_item(item_id, item_name, item_type):
    """
    智能分发媒体项：
    - 电影 (Movie)：直接交由核心处理器处理，跳过队列，加快入库速度。
    - 剧集/分集 (Series/Episode)：进入防抖队列，合并处理，避免整剧入库时 TG 通知轰炸。
    """
    if item_type == 'Movie':
        logger.info(f"  ➜ [分发] 电影 '{item_name}' 跳过防抖队列，直接分派处理任务。")
        
        # 1. 检查是否已处理
        is_already_processed = item_id in extensions.media_processor_instance.processed_items_cache

        # 2. 检查数据库是否在线 (处理“僵尸数据”)
        if is_already_processed:
            is_online_in_db = media_db.is_emby_id_in_library(item_id)
            if not is_online_in_db:
                logger.info(f"  ➜ 缓存命中 '{item_name}'，但数据库标记为离线/缺失。清除缓存，触发重新入库流程。")
                if item_id in extensions.media_processor_instance.processed_items_cache:
                    del extensions.media_processor_instance.processed_items_cache[item_id]
                is_already_processed = False
        
        task_name_prefix = "Webhook追更" if is_already_processed else "Webhook入库"
        
        # 直接提交给任务管理器，不经过 WEBHOOK_BATCH_QUEUE
        _submit_webhook_media_task(
            f"{task_name_prefix}: {item_name}",
            item_id=item_id,
            force_full_update=False,
            new_episode_ids=None,
            is_new_item=not is_already_processed
        )
    else:
        # 剧集、分集等进入防抖队列，等待合并
        _enqueue_webhook_event(item_id, item_name, item_type)

def _wait_for_stream_data_and_enqueue(item_id, item_name, item_type, file_path=None):
    """
    预检视频流数据 + P115Center 神医联动 (完美闭环版)
    """
    if item_type not in ['Movie', 'Episode']:
        _dispatch_item(item_id, item_name, item_type)
        return

    logger.info(f"  ➜ [预检] 开始检查《{item_name}》的媒体信息。")
    logger.debug(f"  ➜ [预检] 媒体信息检查对象：item_id={item_id}")

    app_config = config_manager.APP_CONFIG
    emby_url = app_config.get("emby_server_url")
    emby_key = app_config.get("emby_api_key")
    p115_generate_mediainfo = app_config.get("p115_generate_mediainfo", False)
    processor = extensions.media_processor_instance
    emby_user_id = processor.emby_user_id

    # =========================================================
    # 1. 获取物理路径
    # =========================================================
    # 如果 Webhook 没有传过来路径，才去主动请求 Emby API 兜底
    if not file_path:
        try:
            item_details = emby.get_emby_item_details(item_id, emby_url, emby_key, emby_user_id, fields="Path,MediaSources")
            if item_details:
                file_path = item_details.get("Path")
                if not file_path and item_details.get("MediaSources"):
                    file_path = item_details["MediaSources"][0].get("Path")
        except Exception as e:
            logger.warning(f"  ➜ [预检] 获取路径失败: {e}")
    
    if not p115_generate_mediainfo:
        try:
            # =========================================================
            # 1. 统一调用核心处理器的双指纹提取方法
            # 只支持 ETK 官方 STRM/HTTP PC 播放地址。
            # =========================================================
            pc, sha1 = processor._extract_115_fingerprints(file_path)
            
            if pc or sha1:
                logger.debug(f"  ➜ [路径解析] 成功提取指纹 -> PC: {pc}, SHA1: {sha1}")
            else:
                # =========================================================
                # 2. 养子 (数据库兜底) - 通过 Emby ID 查 PC 码
                # =========================================================
                if item_id:
                    pc = media_db.get_pickcode_by_emby_id(item_id)
                    if pc:
                        logger.debug(f"  ➜ [数据库兜底] 成功通过 Emby ID ({item_id}) 查到 PC 码。")

            # =========================================================
            # 3. 补全 SHA1 (内部自带 115 API 兜底)
            # =========================================================
            if not sha1 and pc:
                sha1 = processor._get_sha1_by_pickcode(pc)
                logger.debug(f"  ➜ [路径解析] 成功提取SHA1: {sha1}")
            
            if sha1:
                media_data = None
                need_upload = False
                is_from_local = False
                
                # --- 提前查询 115 真实文件大小 (供本地严格比对使用) ---
                file_size_115 = 0
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT size FROM p115_filesystem_cache WHERE sha1 = %s", (sha1,))
                            row = cursor.fetchone()
                            if row and row['size']:
                                file_size_115 = row['size']
                except Exception as e_db:
                    logger.warning(f"  ➜ [数据校验] 查询本地文件大小失败: {e_db}")

                # --- 第一步：优先查询本地数据库缓存 ---
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = %s", (sha1,))
                            row = cursor.fetchone()
                            if row and row.get('mediainfo_json'):
                                media_data = row['mediainfo_json']
                                if isinstance(media_data, str):
                                    media_data = json.loads(media_data)
                                is_from_local = True
                                logger.info(f"  ➜ [本地缓存] 命中本地数据库 (SHA1: {sha1})，下发给神医恢复...")
                                # 更新命中次数
                                cursor.execute("UPDATE p115_mediainfo_cache SET hit_count = hit_count + 1 WHERE sha1 = %s", (sha1,))
                                conn.commit()
                except Exception as e_db:
                    logger.warning(f"  ➜ [本地缓存] 查询本地数据库失败: {e_db}")

                # --- 第二步：本地没有，再查中心服务器 ---
                if not is_from_local and getattr(processor, 'p115_center', None):
                    logger.info(f"  ➜ [P115Center] 本地无缓存，开始查询中心服务器 (SHA1: {sha1})")
                    
                    # ★ 撤销传入 file_size_115，防止中心服务器因 HTTP 波动拒收
                    resp = processor.p115_center.download_emby_mediainfo_data([sha1])
                    media_data = resp.get(sha1)
                    
                    if media_data:
                        logger.info(f"  ➜ [P115Center] 命中中心缓存，下发给神医恢复...")
                    else:
                        logger.info(f"  ➜ [P115Center] 中心无缓存，通知神医提取媒体信息...")
                        need_upload = True

                # --- 第三步：轮询调用神医接口，死等纯净数据 ---
                res_json = None
                max_api_polls = 15  
                
                for poll_attempt in range(max_api_polls):
                    with SYNDROME_API_LOCK:
                        res_json = emby.sync_item_media_info(
                            item_id=item_id, 
                            media_data=media_data, 
                            base_url=emby_url,
                            api_key=emby_key
                        )
                        sleep(1)
                        
                    if res_json == []:
                        logger.info(f"  ➜ [神医] 已触发媒体信息提取，等待数据返回... ({poll_attempt+1}/{max_api_polls})")
                        sleep(5) 
                        continue
                    elif not res_json:
                        break

                    # =========================================================
                    # ★★★ 神医返回数据 Size 校验机制 (0.5% 科学容错版) ★★★
                    # =========================================================
                    syndrome_size = 0
                    if isinstance(res_json, list) and len(res_json) > 0:
                        syndrome_size = res_json[0].get("MediaSourceInfo", {}).get("Size", 0)
                    elif isinstance(res_json, dict):
                        syndrome_size = res_json.get("MediaSourceInfo", {}).get("Size", res_json.get("Size", 0))
                    
                    if syndrome_size > 0 and file_size_115 > 0:
                        diff = abs(syndrome_size - file_size_115)
                        error_margin = diff / file_size_115
                        
                        # ★ 采用使用 0.5% (0.005) 的百分比容错率
                        if error_margin > 0.005:
                            logger.error(f"  🚨 [数据校验] 严重警告！神医大小({syndrome_size})与115真实大小({file_size_115})误差达 {error_margin*100:.3f}%！")
                            logger.error(f"  🚨 [数据校验] 判定为同名异版脏数据！正在调用神医接口清除错误缓存，强制重新提取...")
                            
                            # 1. 清除旧的脏数据
                            emby.clear_item_media_info(item_id, emby_url, emby_key)
                            
                            # 重置变量，利用 continue 进入下一轮循环
                            res_json = None
                            media_data = None # ★ 必须置空，让神医去提取物理文件，而不是再次注入脏数据
                            is_from_local = False
                            need_upload = True # ★ 标记需要反哺中心服务器
                            
                            sleep(2) 
                            continue

                    break

                if res_json:
                    if media_data:
                        logger.info(f"  ➜ [神医] 媒体信息恢复成功！(数据源: {'本地数据库' if is_from_local else '中心服务器'})")
                    else:
                        logger.info(f"  ➜ [神医] 媒体信息提取成功！")

                    if not is_from_local:
                        try:
                            json_str = json.dumps(res_json, ensure_ascii=False)
                            with get_db_connection() as conn:
                                with conn.cursor() as cursor:
                                    cursor.execute("""
                                        INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                                        VALUES (%s, %s::jsonb)
                                        ON CONFLICT (sha1) DO UPDATE SET mediainfo_json = EXCLUDED.mediainfo_json
                                    """, (sha1, json_str))
                                    conn.commit()
                            logger.info(f"  ➜ [本地缓存] 媒体信息已备份至本地数据库。")
                        except Exception as e_db:
                            logger.warning(f"  ➜ [本地缓存] 写入数据库失败: {e_db}")
                    
                    if need_upload and getattr(processor, 'p115_center', None):
                        try:
                            # ★ 核心修复：传入 syndrome_size，既满足接口规范，又防止中心服务器 0 容错拒收
                            if syndrome_size > 0:
                                processor.p115_center.upload_emby_mediainfo_data(sha1, res_json, size=syndrome_size)
                            else:
                                processor.p115_center.upload_emby_mediainfo_data(sha1, res_json)
                            logger.info(f"  ➜ [P115Center] 成功将媒体信息反哺至中心服务器。")
                        except Exception as e_up:
                            logger.warning(f"  ➜ [P115Center] 反哺中心服务器失败: {e_up}")

        except Exception as e:
            logger.error(f"  ➜ [P115Center] 联动异常: {e}")

    # =========================================================
    # 2. 物理文件与视频流兜底检查逻辑 
    # =========================================================
    for i in range(STREAM_CHECK_MAX_RETRIES):
        try:
            has_valid_video_stream = False
            
            # 1. 优先检查物理文件
            if file_path:
                mediainfo_path = os.path.splitext(file_path)[0] + "-mediainfo.json"
                if os.path.exists(mediainfo_path):
                    has_valid_video_stream = True
            
            # 2. 兜底检查：实时查询 Emby 的 MediaSources
            if not has_valid_video_stream:
                current_details = emby.get_emby_item_details(
                    item_id, emby_url, emby_key, emby_user_id, fields="MediaSources"
                )
                if current_details and current_details.get("MediaSources"):
                    for source in current_details["MediaSources"]:
                        for stream in source.get("MediaStreams", []):
                            if stream.get("Type") == "Video" and stream.get("Width") and stream.get("Height"):
                                has_valid_video_stream = True
                                break
                        if has_valid_video_stream:
                            break
            
            if has_valid_video_stream:
                logger.info(f"  ➜ [预检] 成功检测到 '{item_name}' 的有效视频流数据，准备分发。")
                # 调用智能分发
                _dispatch_item(item_id, item_name, item_type)
                return
            
            logger.debug(f"  ➜ [预检] '{item_name}' 暂无有效视频流数据，等待提取 ({i+1}/{STREAM_CHECK_MAX_RETRIES})...")
            sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

        except Exception as e:
            logger.error(f"  ➜ [预检] 检查 '{item_name}' 时发生错误: {e}")
            sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

    # 超时强制入库
    logger.warning(f"  ➜ [预检] 超时！未检测到 '{item_name}' 的有效视频流数据。强制分发。")
    # ★ 修改：改为调用智能分发
    _dispatch_item(item_id, item_name, item_type)

# --- Webhook 路由 ---
@webhook_bp.route('/webhook/emby', methods=['POST'])
@extensions.processor_ready_required
def emby_webhook():
    data = request.json
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # ★★★            魔法日志 - START            ★★★
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # try:
    #     import json
    #     # 使用 WARNING 级别和醒目的 emoji，让它在日志中脱颖而出
    #     logger.warning("✨✨✨ [魔法日志] 收到原始 Emby Webhook 负载，内容如下: ✨✨✨")
    #     # 将整个 JSON 数据格式化后打印出来
    #     logger.warning(json.dumps(data, indent=2, ensure_ascii=False))
    # except Exception as e:
    #     logger.error(f"[魔法日志] 记录原始 Webhook 时出错: {e}")
    # # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # # ★★★             魔法日志 - END             ★★★
    # # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    event_type = data.get("Event") # Emby
    mp_event_type = data.get("type") # MP
    # ======================================================================
    # ★★★ 处理神医插件的 deep.delete (深度删除) 事件 ★★★
    # ======================================================================
    if event_type == "deep.delete":
        logger.info("  ➜ 收到神医助手深度删除通知，准备执行清理流程...")
        
        item_from_webhook = data.get("Item", {})
        original_item_id = item_from_webhook.get("Id")
        original_item_type = item_from_webhook.get("Type")
        original_item_name = item_from_webhook.get("Name", "未知项目")
        # 如果是分集，提取所属剧集 ID，供后续清理主库使用
        series_id_from_webhook = item_from_webhook.get("SeriesId") if original_item_type == "Episode" else None

        # --------------------------------------------------------
        # 任务 1: 联动删除 115 网盘文件 (所有层级均生效，且必须在清理数据库前执行！)
        # --------------------------------------------------------
        nb_config = get_config()
        if nb_config.get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False):
            try:
                pickcodes = []
                # 数据库兜底：此时数据库还没被删，可以按 Movie/Episode/Season/Series 层级收集实际文件 PC。
                if original_item_id:
                    logger.debug(f"  ➜ [深度删除] 尝试通过 Emby ID ({original_item_id}) 查库收集 PC 码...")
                    pickcodes = media_db.get_pickcodes_for_deleted_emby_item(original_item_id, original_item_type)

                if pickcodes:
                    logger.info(f"  ➜ 成功提取到 {len(pickcodes)} 个 115 提取码，交由后台执行联动删除。")
                    from handler.p115_service import delete_115_files_by_webhook
                    spawn(delete_115_files_by_webhook, pickcodes)
                else:
                    logger.warning("  ➜ 深度删除通知中未找到有效的 ETK 直链或 PC 码，跳过网盘清理。")
            except Exception as e:
                logger.error(f"  ➜ 解析深度删除通知失败: {e}", exc_info=True)
        else:
            logger.debug("  ➜ 联动删除未开启，跳过网盘清理。")

        # --------------------------------------------------------
        # ★★★ 任务 2: 联动清理 MoviePilot (支持精准单集与辅种) ★★★
        # --------------------------------------------------------
        mp_config = settings_db.get_setting('mp_config') or {}
        del_history = mp_config.get('link_delete_transfer_history', False)
        del_files = mp_config.get('link_delete_download_files', False)

        if (del_history or del_files) and original_item_id:
            try:
                # 尝试从本地数据库反查 TMDb ID 和季集信息 (此时数据库还没被删，完美拿到！)
                db_tmdb_id = None
                db_season = None
                db_episode = None
                db_title = original_item_name
                db_item_type = original_item_type

                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 使用 LIKE 模糊匹配 JSON 数组中的 ID
                        cursor.execute("SELECT tmdb_id, item_type, season_number, episode_number, title, parent_series_tmdb_id FROM media_metadata WHERE emby_item_ids_json::text LIKE %s", (f'%"{original_item_id}"%',))
                        row = cursor.fetchone()
                        if row:
                            db_item_type = row['item_type']
                            if db_item_type == 'Movie':
                                db_tmdb_id = row['tmdb_id']
                                db_title = row['title']
                            elif db_item_type == 'Episode':
                                db_tmdb_id = row['parent_series_tmdb_id']
                                db_season = row['season_number']
                                db_episode = row['episode_number']
                                # 剧集需要用主剧名去搜 MP
                                cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (db_tmdb_id,))
                                p_row = cursor.fetchone()
                                if p_row: db_title = p_row['title']
                            elif db_item_type == 'Season':
                                db_tmdb_id = row['parent_series_tmdb_id']
                                db_season = row['season_number']
                                cursor.execute("SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (db_tmdb_id,))
                                p_row = cursor.fetchone()
                                if p_row: db_title = p_row['title']
                            elif db_item_type == 'Series':
                                db_tmdb_id = row['tmdb_id']
                                db_title = row['title']

                if db_tmdb_id:
                    logger.info(f"  ➜ [深度删除] 触发 MP 联动清理: {db_title} (TMDb:{db_tmdb_id}, S:{db_season}, E:{db_episode})")
                    from handler.moviepilot import smart_cleanup_mp_media
                    # 异步执行，防止阻塞 Webhook
                    spawn(smart_cleanup_mp_media, str(db_tmdb_id), db_item_type, db_season, db_episode, db_title, None, del_history, del_files)
                else:
                    logger.warning(f"  ➜ [深度删除] 无法从本地数据库反查到 Emby ID {original_item_id} 的 TMDb 信息，跳过 MP 联动清理。")
            except Exception as e:
                logger.error(f"  ➜ [深度删除] MP 联动清理失败: {e}", exc_info=True)

        # --------------------------------------------------------
        # 任务 3: 清理本地数据库、日志与内存缓存 (网盘删完再删本地)
        # --------------------------------------------------------
        if original_item_id:
            try:
                logger.info(f"  ➜ [深度删除] 开始清理本地数据库与缓存: {original_item_name} ({original_item_type})")
                
                # 1. 清理主媒体库记录 (★ 所有层级均生效，删一集就清理一集的记录)
                # ★ 修复：日志和内存缓存的清理已下沉到此函数内部，利用其完善的多版本善后逻辑，防止误清理
                maintenance_db.cleanup_deleted_media_item(
                    item_id=original_item_id,
                    item_name=original_item_name,
                    item_type=original_item_type,
                    series_id_from_webhook=series_id_from_webhook
                )

            except Exception as e:
                logger.error(f"  ➜ [深度删除] 清理本地数据库与缓存失败: {e}", exc_info=True)

        # 深度删除处理完毕，直接返回 200，不再往下走
        return jsonify({"status": "deep_delete_processed"}), 200
    # ======================================================================
    # ★★★ 处理 MoviePilot transfer.complete 事件 ★★★
    # ======================================================================
    if mp_event_type in ["transfer.complete", "transfer.subtitle.complete"]:
        nb_config = get_config()
        if not nb_config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False):
            logger.debug("  ➜ 智能整理未开启，忽略 MP 通知。")
            return jsonify({"status": "ignored_smart_organize_disabled"}), 200

        try:
            transfer_info = data.get("data", {}).get("transferinfo", {})
            media_info = data.get("data", {}).get("mediainfo", {})
            meta_info = data.get("data", {}).get("meta", {}) 
            
            target_item = transfer_info.get("target_item", {})
            target_dir = transfer_info.get("target_diritem", {})
            source_item = transfer_info.get("fileitem") or data.get("data", {}).get("fileitem") or {}
            
            file_id = target_item.get("fileid")
            file_name = target_item.get("name")
            file_type = target_item.get("type") 
            pickcode = target_item.get("pickcode")
            dir_cid = target_dir.get("fileid")

            tmdb_id = media_info.get("tmdb_id")
            media_type_cn = media_info.get("type") 
            title = media_info.get("title")
            
            begin_season = meta_info.get("begin_season")
            begin_episode = meta_info.get("begin_episode")
            
            if not tmdb_id or not file_id:
                logger.warning("  ➜ MP 通知缺少 tmdb_id 或 file_id，无法处理。")
                return jsonify({"status": "ignored_missing_data"}), 200

            media_type = 'tv' if media_type_cn == '电视剧' else 'movie'
            
            if file_type == 'file':
                file_info = {
                    'file_id': file_id,
                    'name': file_name,
                    'parent_id': dir_cid,
                    'pickcode': pickcode,
                    'tmdb_id': tmdb_id,
                    'media_type': media_type,
                    'title': title,
                    'season_num': begin_season,
                    'episode_num': begin_episode,
                    'size': None,
                    'fs': None,
                    '115_path': target_item.get("path") # ★ 核心新增：直接提取 115 物理路径
                }
                
                log_prefix = "MP字幕上传" if mp_event_type == "transfer.subtitle.complete" else "MP视频上传"
                
                config = get_config()
                mp_classify_enabled = bool(config.get(constants.CONFIG_OPTION_115_MP_CLASSIFY, False))
                
                if mp_classify_enabled:
                    logger.info(f"  ➜ [{log_prefix}] 收到文件：{file_name}。MP 直出已开启，直接处理。")
                    spawn(_process_mp_passthrough_immediate, file_info)
                    return jsonify({"status": "processing_single_file_passthrough"}), 200
                else:
                    logger.info(f"  ➜ [{log_prefix}] 收到文件：{file_name}。已加入合并缓冲池，等待同集字幕或其他版本。")
                    _enqueue_mp_file(file_info)
                    return jsonify({"status": "processing_single_file"}), 200
            else:
                logger.debug(f"  ➜ [MP上传] 忽略非文件类型的通知: {file_name}")
                return jsonify({"status": "ignored_not_file"}), 200

        except Exception as e:
            logger.error(f"  ➜ [MP上传] 处理失败: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500
        
    logger.debug(f"  ➜ 收到Emby Webhook: {event_type}")

    USER_DATA_EVENTS = [
        "item.markfavorite", "item.unmarkfavorite",
        "item.markplayed", "item.markunplayed",
        "playback.start", "playback.pause", "playback.stop",
        "item.rate"
    ]

    if event_type == "user.policyupdated":
        updated_user = data.get("User", {})
        updated_user_id = updated_user.get("Id")
        updated_user_name = updated_user.get("Name", "未知用户")
        
        if not updated_user_id:
            return jsonify({"status": "event_ignored_no_user_id"}), 200

        # --- 立即反查并更新本地 Policy ---
        try:
            def _update_local_policy_task():
                try:
                    # 获取最新详情
                    user_details = emby.get_user_details(
                        updated_user_id, 
                        config_manager.APP_CONFIG.get("emby_server_url"), 
                        config_manager.APP_CONFIG.get("emby_api_key")
                    )
                    if user_details and 'Policy' in user_details:
                        # 更新数据库
                        user_db.upsert_emby_users_batch([user_details])
                        logger.info(f"  ➜ Webhook: 已更新用户 {updated_user_id} 的本地权限缓存。")
                except Exception as e:
                    logger.error(f"  ➜ Webhook 更新本地 Policy 失败: {e}")

            # 异步执行，不阻塞 Webhook 返回
            spawn(_update_local_policy_task)
        except Exception as e:
            logger.error(f"启动 Policy 更新任务失败: {e}")

        # ★★★ 核心逻辑: 在处理前，先检查信号旗 ★★★
        with SYSTEM_UPDATE_LOCK:
            last_update_time = SYSTEM_UPDATE_MARKERS.get(updated_user_id)
            # 如果找到了标记，并且时间戳在我们的抑制窗口期内
            if last_update_time and (time.time() - last_update_time) < RECURSION_SUPPRESSION_WINDOW:
                logger.debug(f"  ➜ 忽略由系统内部同步触发的用户 '{updated_user_name}' 的权限更新 Webhook。")
                # 为了保险起见，用完就删掉这个标记
                del SYSTEM_UPDATE_MARKERS[updated_user_id]
                # 直接返回成功，不再创建任何后台任务
                return jsonify({"status": "event_ignored_system_triggered"}), 200
        
        # 如果上面的检查通过了（即这是一个正常的手动操作），才继续执行原来的逻辑
        logger.info(f"  ➜ 检测到用户 '{updated_user_name}' 的权限策略已更新，将分派后台任务检查模板同步。")
        task_manager.submit_task(
            task_auto_sync_template_on_policy_change,
            task_name=f"自动同步权限 (源: {updated_user_name})",
            processor_type='media',
            updated_user_id=updated_user_id
        )
        return jsonify({"status": "auto_sync_task_submitted"}), 202

    if event_type in USER_DATA_EVENTS:
        user_from_webhook = data.get("User", {})
        user_id = user_from_webhook.get("Id")
        user_name = user_from_webhook.get("Name")
        user_name_for_log = user_name or user_id
        item_from_webhook = data.get("Item", {})
        item_id_from_webhook = item_from_webhook.get("Id")
        item_type_from_webhook = item_from_webhook.get("Type")

        if not user_id or not item_id_from_webhook:
            return jsonify({"status": "event_ignored_missing_data"}), 200


        id_to_update_in_db = None
        if item_type_from_webhook in ['Movie', 'Series']:
            id_to_update_in_db = item_id_from_webhook
        elif item_type_from_webhook == 'Episode':
            series_id = emby.get_series_id_from_child_id(
                item_id=item_id_from_webhook,
                base_url=config_manager.APP_CONFIG.get("emby_server_url"),
                api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                user_id=user_id
            )
            if series_id:
                id_to_update_in_db = series_id
        
        if not id_to_update_in_db:
            return jsonify({"status": "event_ignored_unsupported_type_or_not_found"}), 200

        update_data = {"user_id": user_id, "item_id": id_to_update_in_db}
        
        if event_type in ["item.markfavorite", "item.unmarkfavorite", "item.markplayed", "item.markunplayed", "item.rate"]:
            user_data_from_item = item_from_webhook.get("UserData", {})
            if 'IsFavorite' in user_data_from_item:
                update_data['is_favorite'] = user_data_from_item['IsFavorite']
            if 'Played' in user_data_from_item:
                update_data['played'] = user_data_from_item['Played']
                if user_data_from_item['Played']:
                    update_data['playback_position_ticks'] = 0
                    update_data['last_played_date'] = datetime.now(timezone.utc)

        elif event_type in ["playback.start", "playback.pause", "playback.stop"]:
            playback_info = data.get("PlaybackInfo", {})
            if playback_info:
                position_ticks = playback_info.get('PositionTicks')
                if position_ticks is not None:
                    update_data['playback_position_ticks'] = position_ticks
                
                update_data['last_played_date'] = datetime.now(timezone.utc)
                
                if event_type == "playback.stop":
                    if playback_info.get('PlayedToCompletion') is True:
                        update_data['played'] = True
                        update_data['playback_position_ticks'] = 0
                    else:
                        update_data['played'] = False
                    try:
                        cleanup_data = dict(data)
                        cleanup_data['_etk_webhook_remote_addr'] = request.remote_addr or ''
                        spawn(cleanup_for_playback_stop, cleanup_data)
                        spawn(p115_play_pool.cleanup_for_playback_stop, cleanup_data)
                    except Exception as e:
                        logger.error(f"  ➜ [复制播放] 停止播放清理任务分配失败: {e}")

            # 发送有灵魂的图文播放通知 
            notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
            if 'playback' in notify_types and event_type in ["playback.start", "playback.pause", "playback.stop"]:
                try:
                    # 使用 spawn 异步丢给后台处理，杜绝网络波动卡住 Emby Webhook 导致延迟
                    spawn(telegram.send_playback_notification, data)
                except Exception as e:
                    logger.error(f"  ➜ 发送播放通知任务分配失败: {e}")

        try:
            if len(update_data) > 2:
                user_db.upsert_user_media_data(update_data)
                user_db.upsert_user_media_data(update_data)
                item_name_for_log = f"ID:{id_to_update_in_db}"
                try:
                    # 为了日志，只请求 Name 字段，提高效率
                    item_details_for_log = emby.get_emby_item_details(
                        item_id=id_to_update_in_db,
                        emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                        emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                        user_id=user_id,
                        fields="Name"
                    )
                    if item_details_for_log and item_details_for_log.get("Name"):
                        item_name_for_log = item_details_for_log.get("Name")
                except Exception:
                    # 如果获取失败，不影响主流程，日志中继续使用ID
                    pass
                logger.trace(f"  ➜ Webhook: 已更新用户 '{user_name_for_log}' 对项目 '{item_name_for_log}' 的状态 ({event_type})。")
                return jsonify({"status": "user_data_updated"}), 200
            else:
                logger.debug(f"  ➜ Webhook '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"  ➜ 通过 Webhook 更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    trigger_events = ["item.add", "library.new", "metadata.update", "image.update", "collection.items.removed", "deep.delete", "None"]
    if event_type not in trigger_events:
        logger.debug(f"  ➜ Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200
    
    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_type = item_from_webhook.get("Type")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_path = item_from_webhook.get("Path")
    
    # 如果是分集，将名字格式化为 "剧名 - 集名"，方便日志搜索
    raw_name = item_from_webhook.get("Name", "未知项目")
    series_name = item_from_webhook.get("SeriesName")
    
    if original_item_type == "Episode" and series_name:
        original_item_name = f"{series_name} - {raw_name}"
    else:
        original_item_name = raw_name
    
    trigger_types = ["Movie", "Series", "Season", "Episode", "BoxSet", "Audio"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"  ➜ Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    # ======================================================================
    # ★★★ 处理 collection.items.removed (检查是否变空消失) ★★★
    # ======================================================================
    if event_type == "collection.items.removed":
        # Emby 发送此事件时，Item 指的是合集本身
        collection_id = item_from_webhook.get("Id")
        collection_name = item_from_webhook.get("Name")

        if collection_id in DELETING_COLLECTIONS:
            logger.debug(f"  ➜ Webhook: 忽略合集 '{collection_name}' 的移除通知 (正在执行手动删除)。")
            return jsonify({"status": "ignored_manual_deletion"}), 200
        
        if collection_id:
            logger.info(f"  ➜ Webhook: 合集 '{collection_name}' 有成员移除，正在检查合集存活状态...")
            
            def _check_collection_survival_task(processor=None):
                details = emby.get_emby_item_details(
                    item_id=collection_id,
                    emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                    emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                    user_id=config_manager.APP_CONFIG.get("emby_user_id"),
                    fields="Id",
                    silent_404=True
                )
                
                if not details:
                    logger.info(f"  ➜ 合集《{collection_name}》已在 Emby 中消失，正在同步删除本地记录。")
                    logger.debug(f"  ➜ 已消失合集 ID：{collection_id}")
                    tmdb_collection_db.delete_native_collection_by_emby_id(collection_id)
                else:
                    logger.debug(f"  ➜ 合集 '{collection_name}' 依然存在，无需操作。")

            task_manager.submit_task(
                _check_collection_survival_task,
                task_name=f"检查合集存活: {collection_name}",
                processor_type='media'
            )
            return jsonify({"status": "collection_removal_check_started"}), 202

    # 过滤不在处理范围的媒体库
    if event_type in ["item.add", "library.new", "metadata.update", "image.update"]:
        processor = extensions.media_processor_instance
        
        # --- 【拦截 1】如果是系统正在生成的封面，直接拦截，不查库，不报错 ---
        if event_type == "image.update" and original_item_id in UPDATING_IMAGES:
            logger.debug(f"  ➜ Webhook: 忽略项目 '{original_item_name}' 的图片更新通知 (系统生成的封面)。")
            return jsonify({"status": "ignored_self_triggered_update"}), 200
        
        # --- 【拦截 2】如果是系统正在更新元数据，直接拦截 ---
        if event_type == "metadata.update" and original_item_id in UPDATING_METADATA:
            logger.debug(f"  ➜ Webhook: 忽略项目 '{original_item_name}' 的元数据更新通知 (系统触发的更新)。")
            return jsonify({"status": "ignored_self_triggered_metadata_update"}), 200

        # --- 【拦截 3】如果是合集(BoxSet)，它没有物理路径，直接跳过库路径检查 ---
        if original_item_type == "BoxSet":
            logger.trace(f"  ➜ Webhook: 项目 '{original_item_name}' 是合集类型，跳过媒体库路径检查。")
            library_info = None 
        else:
            # 正常的媒体项，才去获取所属库信息
            library_info = emby.get_library_root_for_item(
                original_item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, 
                item_path=original_item_path # ★★★ 核心优化：直接把 Webhook 传来的 Path 喂进去
            )
        
        if library_info:
            lib_id = library_info.get("Id")
            lib_name = library_info.get("Name", "未知库")
            allowed_libs = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS) or []

            # 【关键拦截点】
            if lib_id not in allowed_libs and original_item_type != "Audio":
                logger.trace(f"  ➜ Webhook: 项目 '{original_item_name}' 所属库 '{lib_name}' (ID: {lib_id}) 不在处理范围内，已跳过。")
                return jsonify({"status": "ignored_library"}), 200

        if _should_skip_non_etk_strm_webhook(original_item_type, original_item_name, original_item_path):
            return jsonify({"status": "ignored_non_etk_strm"}), 200

    # ======================================================================
    # ★★★ 处理音乐 (Audio) 入库事件 ★★★
    # ======================================================================
    if event_type in ["item.add", "library.new"] and original_item_type == "Audio":
        logger.info(f"  ➜ [音乐入库] 检测到音频文件 '{original_item_name}'，直接触发神医提取媒体信息...")
        processor = extensions.media_processor_instance
        
        def _trigger_audio_info():
            # 稍微等 2 秒，确保 Emby 数据库已经把这个条目完全落盘
            sleep(2)
            emby.trigger_media_info_refresh(
                original_item_id, 
                processor.emby_url, 
                processor.emby_api_key, 
                processor.emby_user_id
            )
            
        # 异步触发，绝不阻塞 Webhook 主线程
        spawn(_trigger_audio_info)
        return jsonify({"status": "audio_media_info_triggered", "item_id": original_item_id}), 202
    
    # ======================================================================
    # ★★★ 处理视频入库事件 (原有的逻辑保持不变) ★★★
    # ======================================================================
    if event_type in ["item.add", "library.new"]:
        spawn(_wait_for_stream_data_and_enqueue, original_item_id, original_item_name, original_item_type, original_item_path)
        
        logger.info(f"  ➜ Webhook: 收到入库事件 '{original_item_name}'，已分派预检任务。")
        return jsonify({"status": "processing_started_with_stream_check", "item_id": original_item_id}), 202

    # --- 为 元数据更新 事件准备变量 ---
    id_to_process = original_item_id
    name_for_task = original_item_name
    
    if original_item_type == "Episode":
        series_id = emby.get_series_id_from_child_id(
            original_item_id, extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=original_item_name
        )
        if not series_id:
            logger.warning(f"  ➜ Webhook '{event_type}': 剧集 '{original_item_name}' 未找到所属剧集，跳过。")
            return jsonify({"status": "event_ignored_episode_no_series_id"}), 200
        id_to_process = series_id
        
        full_series_details = emby.get_emby_item_details(
            item_id=id_to_process, emby_server_url=extensions.media_processor_instance.emby_url,
            emby_api_key=extensions.media_processor_instance.emby_api_key, user_id=extensions.media_processor_instance.emby_user_id
        )
        if full_series_details:
            name_for_task = full_series_details.get("Name", f"未知剧集(ID:{id_to_process})")

    # --- 处理元数据更新事件 ---
    if event_type == "metadata.update":
        with UPDATE_DEBOUNCE_LOCK:
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"  ➜ 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的元数据更新事件为准。")

            logger.info(f"  ➜ 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的元数据同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_metadata_update_task,
                item_id=id_to_process,
                item_name=name_for_task
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        return jsonify({"status": "metadata_update_task_debounced", "item_id": id_to_process}), 202

    return jsonify({"status": "event_unhandled"}), 500
