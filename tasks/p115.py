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
    _identify_media_enhanced,
    _transfer_context_to_recognition_hints,
    resolve_p115_sorting_target_by_local_path,
)
from handler.p115_media_analyzer import P115MediaAnalyzerMixin
from handler.tg_media_candidate import candidate_to_recognition_hints, lookup_candidate_hint_for_name

logger = logging.getLogger(__name__)

TV_HINT_RE = re.compile(
    r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|'
    r'(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|'
    r'(?:^|[ \.\-\_\[\(])e\d{1,4}\b|'
    r'第[一二三四五六七八九十\d]+季',
    re.IGNORECASE,
)
SEASON_DIR_RE = re.compile(r'^(Season\s?\d+|S\d+|第[一二三四五六七八九十\d]+季)$', re.IGNORECASE)
SEASON_NUM_RE = re.compile(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?')
SEASON_TEXT_RE = re.compile(r'Season\s*(\d{1,4})\b', re.IGNORECASE)
SEASON_ZH_RE = re.compile(r'第(\d{1,4})季')
TMDB_TAG_RE = re.compile(r'(?:tmdb|tmdbid)[=\-_]*(\d+)', re.IGNORECASE)
GENERIC_PACKAGE_SEGMENT_RE = re.compile(
    r'^(?:19\d{2}|20\d{2}|'
    r'电影|电视剧|剧集|动漫|动画|综艺|纪录片|纪录|'
    r'合集|系列|全套|打包|大包|资源|待整理|转存|新片|影视|网盘|'
    r'collection|collections|series|pack|package|movie|movies|tv|shows?|anime|misc|unknown)$',
    re.IGNORECASE,
)
KNOWN_VIDEO_EXTS = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
KNOWN_SKIP_EXTS = {'clpi', 'mpls', 'bdmv', 'jar', 'bup', 'ifo'}
MIN_BIG_PACKAGE_VIDEO_SIZE = 50 * 1024 * 1024


def _p115_response_path_contains_cid(response, target_cid):
    path_nodes = response.get('path') if isinstance(response, dict) else None
    if not path_nodes:
        return True

    target = str(target_cid or '')
    for node in path_nodes:
        if not isinstance(node, dict):
            continue
        node_id = str(
            node.get('cid')
            or node.get('file_id')
            or node.get('id')
            or node.get('parent_id')
            or ''
        )
        if node_id == target:
            return True
    return False


def _normalize_batch_recognition_hints(hints, *, is_tv=False, preserve_episode=False):
    """
    批量整理时，组级 hints 只能安全复用标题/TMDb/季级信息。

    单条 TG 候选携带的 episode_number 往往只代表“频道消息里展示的最新集”，
    不能直接灌给整包中的每个文件；否则 E08 可能污染同批的 E05/E06/E07。
    """
    normalized = candidate_to_recognition_hints(hints or {})
    if not normalized:
        return {}

    normalized = dict(normalized)

    if not is_tv:
        normalized.pop("season_number", None)
        normalized.pop("episode_number", None)
        normalized.pop("is_special", None)
        return normalized

    if not preserve_episode:
        normalized.pop("episode_number", None)

    return normalized


def _merge_authority_hints(primary, fallback, *, is_tv=False, preserve_episode=False):
    merged = _normalize_batch_recognition_hints(fallback or {}, is_tv=is_tv, preserve_episode=preserve_episode)
    authoritative = _normalize_batch_recognition_hints(primary or {}, is_tv=is_tv, preserve_episode=preserve_episode)
    if not authoritative:
        return merged
    result = dict(merged)
    result.update({k: v for k, v in authoritative.items() if v not in (None, "", [], {})})
    if authoritative.get("matched_rules"):
        result["matched_rules"] = list(authoritative.get("matched_rules") or [])
    if authoritative.get("evidence"):
        result["evidence"] = list(authoritative.get("evidence") or [])
    if authoritative.get("source_kind"):
        result["source_kind"] = authoritative.get("source_kind")
    if authoritative.get("source_kinds"):
        result["source_kinds"] = list(authoritative.get("source_kinds") or [])
    return result


def _name_has_tv_hint(name):
    return bool(name and TV_HINT_RE.search(str(name)))


def _name_is_season_dir(name):
    return bool(name and SEASON_DIR_RE.search(str(name).strip()))


def _name_has_tmdb_tag(name):
    return bool(name and TMDB_TAG_RE.search(str(name)))


def _extract_season_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        m1 = SEASON_NUM_RE.search(value)
        m2 = SEASON_TEXT_RE.search(value)
        m3 = SEASON_ZH_RE.search(value)
        if m1:
            return int(m1.group(1))
        if m2:
            return int(m2.group(1))
        if m3:
            return int(m3.group(1))
    return None


def _extract_episode_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        match = re.search(
            r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b'
            r'|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b'
            r'|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b'
            r'|第(\d{1,4})[集话話回]',
            value,
            re.IGNORECASE,
        )
        if match:
            episode = match.group(2) or match.group(3) or match.group(4) or match.group(5)
            if episode is not None:
                return int(episode)
    return None


def _extract_part_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', value)
        if match:
            return int(match.group(2))
    return None


def _is_related_sidecar_name(video_name, other_name):
    video_name = str(video_name or '')
    other_name = str(other_name or '')
    if not video_name or not other_name:
        return False

    video_base = video_name.rsplit('.', 1)[0] if '.' in video_name else video_name
    if other_name.startswith(video_base):
        return True

    video_season = _extract_season_number(video_name)
    other_season = _extract_season_number(other_name)
    video_episode = _extract_episode_number(video_name)
    other_episode = _extract_episode_number(other_name)
    video_part = _extract_part_number(video_name)
    other_part = _extract_part_number(other_name)

    if video_episode is None or other_episode is None:
        return False

    if video_season is not None and other_season is not None and video_season != other_season:
        return False

    if video_part is not None and other_part is not None and video_part != other_part:
        return False

    return video_episode == other_episode


def _is_generic_package_segment(name):
    if not name:
        return True
    normalized = str(name).strip().strip('/').strip()
    if not normalized:
        return True
    return bool(GENERIC_PACKAGE_SEGMENT_RE.match(normalized))


def _normalize_context_label(name):
    if not name:
        return ''
    normalized = TMDB_TAG_RE.sub('', str(name))
    normalized = re.sub(r'[\s\-\._]+', '', normalized)
    return normalized.lower()


def _split_rel_dir_segments(rel_dir):
    return [seg.strip() for seg in str(rel_dir or '').split('/') if seg and seg.strip()]


def _choose_big_package_context_name(top_name, rel_dir):
    segments = _split_rel_dir_segments(rel_dir)
    for segment in reversed(segments):
        if not _name_is_season_dir(segment) and not _is_generic_package_segment(segment):
            return segment
    return top_name


def _analyze_nested_root_structure(top_name, gathered_files):
    valid_video_files = []
    contexts = set()
    nested_contexts = set()
    top_norm = _normalize_context_label(top_name)

    for item in list(gathered_files or []):
        file_name = item.get('fn') or item.get('n') or item.get('file_name', '')
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        if ext not in KNOWN_VIDEO_EXTS:
            continue
        file_size = _parse_115_size(item.get('fs') or item.get('size'))
        if file_size <= MIN_BIG_PACKAGE_VIDEO_SIZE:
            continue

        valid_video_files.append(item)
        context_name = _choose_big_package_context_name(top_name, item.get('_etk_rel_dir', ''))
        context_norm = _normalize_context_label(context_name)
        if context_name and context_norm:
            contexts.add(context_name)
            if top_norm and context_norm != top_norm:
                nested_contexts.add(context_name)
            elif not top_norm and item.get('_etk_rel_dir'):
                nested_contexts.add(context_name)

    has_multiple_contexts = len({_normalize_context_label(name) for name in contexts if name}) > 1
    has_nested_specific_context = len(nested_contexts) > 0
    should_force_filewise = len(valid_video_files) > 1 and (has_multiple_contexts or has_nested_specific_context)

    return {
        "valid_video_files": valid_video_files,
        "contexts": sorted(contexts),
        "nested_contexts": sorted(nested_contexts),
        "has_multiple_contexts": has_multiple_contexts,
        "has_nested_specific_context": has_nested_specific_context,
        "should_force_filewise": should_force_filewise,
    }


def _should_use_filewise_big_package(top_name, is_tv_group, has_season_dir, has_tmdb, valid_video_files):
    if is_tv_group or has_season_dir or has_tmdb:
        return False
    return len(valid_video_files) > 1


def _should_force_nested_package_scan(top_name):
    if not _is_generic_package_segment(top_name):
        return False
    if _name_has_tmdb_tag(top_name):
        return False
    if _name_has_tv_hint(top_name) or _name_is_season_dir(top_name):
        return False
    return True


def _build_standard_season_dir_groups(top_name, gathered_files, *, is_tv_group=False, has_season_dir=False):
    if not is_tv_group or not has_season_dir:
        return []

    grouped = {}
    ungrouped_videos = []

    for item in list(gathered_files or []):
        file_name = item.get('fn') or item.get('n') or item.get('file_name', '')
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        rel_dir = item.get('_etk_rel_dir', '')
        season_num = _extract_season_number(rel_dir)

        if season_num is None:
            if ext in KNOWN_VIDEO_EXTS:
                ungrouped_videos.append(item)
            continue

        grouped.setdefault(season_num, {
            "top_name": top_name,
            "files": [],
            "is_tv": True,
            "has_season_dir": True,
            "forced_season": season_num,
        })["files"].append(item)

    if ungrouped_videos or len(grouped) <= 1:
        return []

    return [grouped[s_num] for s_num in sorted(grouped)]


def _build_filewise_big_package_groups(gathered_files, top_name, ai_translator=None, use_ai=False):
    grouped = {}
    unresolved = []
    assigned_ids = set()
    all_items = list(gathered_files or [])
    shared_context = P115CacheManager.get_transfer_context(top_name)
    shared_context_hints = _transfer_context_to_recognition_hints(shared_context)

    valid_video_files = []
    for item in all_items:
        file_name = item.get('fn') or item.get('n') or item.get('file_name', '')
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        if ext in KNOWN_VIDEO_EXTS:
            file_size = _parse_115_size(item.get('fs') or item.get('size'))
            if file_size > MIN_BIG_PACKAGE_VIDEO_SIZE:
                valid_video_files.append(item)

    for video_item in valid_video_files:
        video_fid = video_item.get('fid') or video_item.get('file_id')
        if video_fid in assigned_ids:
            continue

        file_name = video_item.get('fn') or video_item.get('n') or video_item.get('file_name', '')
        rel_dir = video_item.get('_etk_rel_dir', '')
        context_name = _choose_big_package_context_name(top_name, rel_dir)
        identify_main_dir = context_name or top_name
        if not _name_has_tmdb_tag(identify_main_dir) and _name_has_tmdb_tag(top_name):
            identify_main_dir = top_name
        forced_type = 'tv' if (_name_has_tv_hint(file_name) or _name_has_tv_hint(rel_dir)) else None
        season_num = _extract_season_number(file_name, rel_dir, context_name)
        candidate_hints = lookup_candidate_hint_for_name(
            file_name,
            alt_texts=[context_name, top_name],
            media_type=forced_type,
            season_number=season_num,
        )
        recognition_hints = _merge_authority_hints(
            shared_context_hints,
            candidate_hints,
            is_tv=(forced_type == 'tv'),
        )
        file_sha1 = video_item.get('sha1') or video_item.get('sha')
        if shared_context and str(shared_context.get("media_type") or "") in ("tv", "movie"):
            tmdb_id = str(shared_context.get("tmdb_id") or "").strip() or None
            media_type = str(shared_context.get("media_type") or "").strip() or forced_type
            title = shared_context.get("title") or context_name or file_name
            if media_type == "tv" and season_num is None:
                season_num = shared_context.get("season_number")
        else:
            tmdb_id, media_type, title = _identify_media_enhanced(
                file_name,
                main_dir_name=context_name,
                has_season_subdirs=False,
                forced_media_type=forced_type,
                ai_translator=ai_translator,
                use_ai=use_ai,
                is_folder=False,
                sha1=file_sha1,
                recognition_hints=recognition_hints,
            )

        related_items = [video_item]
        assigned_ids.add(video_fid)

        for other_item in all_items:
            other_fid = other_item.get('fid') or other_item.get('file_id')
            if other_fid in assigned_ids:
                continue
            other_name = other_item.get('fn') or other_item.get('n') or other_item.get('file_name', '')
            other_dir = other_item.get('_etk_rel_dir', '')
            if other_dir != rel_dir:
                continue
            if _is_related_sidecar_name(file_name, other_name):
                related_items.append(other_item)
                assigned_ids.add(other_fid)

        if not tmdb_id:
            unresolved.extend(related_items)
            continue

        if media_type == 'tv' and season_num is None:
            season_num = _extract_season_number(context_name, rel_dir)

        group_key = (
            tmdb_id,
            media_type,
            title,
            season_num if media_type == 'tv' else None,
        )
        if group_key not in grouped:
            grouped[group_key] = {
                "top_name": context_name or file_name,
                "files": [],
                "is_tv": media_type == 'tv',
                "has_season_dir": False,
                "identified_tmdb_id": tmdb_id,
                "identified_media_type": media_type,
                "identified_title": title,
                "recognition_hints": recognition_hints or {},
                "forced_season": season_num,
            }
        grouped[group_key]["files"].extend(related_items)
        if grouped[group_key]["forced_season"] is None and season_num is not None:
            grouped[group_key]["forced_season"] = season_num

    for item in all_items:
        item_id = item.get('fid') or item.get('file_id')
        if item_id in assigned_ids:
            continue
        unresolved.append(item)

    return list(grouped.values()), unresolved




def _manual_correct_as_list(value):
    """把前端传入的 record_ids/ids 宽松归一成列表。"""
    if value in (None, '', [], {}):
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, (list, tuple, set)):
                return list(parsed)
        except Exception:
            pass
        return [x.strip() for x in text.split(',') if x.strip()]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _manual_correct_normalize_season(value):
    if value in (None, '', [], {}):
        return None
    try:
        return int(value)
    except Exception:
        return value


def _manual_correct_task_update(progress, message):
    try:
        import task_manager
        task_manager.update_status_from_thread(progress, message)
    except Exception:
        pass
    logger.info(message)


def task_manual_correct_organize_records(
    processor=None,
    record_ids=None,
    ids=None,
    items=None,
    tmdb_id=None,
    media_type=None,
    target_cid=None,
    season_num=None,
    **kwargs,
):
    """通用任务入口：批量手动重组整理记录。

    该任务专门给 /api/tasks/run 调用，必须走 media 处理器队列，避免手动重组
    与 Webhook 入库、高频刷新、网盘扫描等整理链路并发串门。
    """
    normalized_items = []

    if isinstance(items, str):
        text = items.strip()
        if text:
            try:
                items = json.loads(text)
            except Exception as e:
                raise ValueError(f"items 不是合法 JSON: {e}")
        else:
            items = []

    if items:
        if not isinstance(items, (list, tuple)):
            raise ValueError("items 必须是数组")
        for item in items:
            if not isinstance(item, dict):
                continue
            record_id = item.get('id') or item.get('record_id')
            if not record_id:
                continue
            normalized_items.append({
                'id': record_id,
                'tmdb_id': str(item.get('tmdb_id') or '').strip(),
                'media_type': str(item.get('media_type') or 'movie').strip(),
                'target_cid': str(item.get('target_cid') or '').strip(),
                'season_num': _manual_correct_normalize_season(item.get('season_num')),
            })
    else:
        rid_list = _manual_correct_as_list(record_ids if record_ids not in (None, '', [], {}) else ids)
        if not rid_list and kwargs.get('record_id'):
            rid_list = [kwargs.get('record_id')]
        for record_id in rid_list:
            normalized_items.append({
                'id': record_id,
                'tmdb_id': str(tmdb_id or '').strip(),
                'media_type': str(media_type or 'movie').strip(),
                'target_cid': str(target_cid or '').strip(),
                'season_num': _manual_correct_normalize_season(season_num),
            })

    if not normalized_items:
        raise ValueError("没有可重组的整理记录")

    grouped = {}
    group_order = []
    for item in normalized_items:
        if not item.get('tmdb_id') or not item.get('media_type') or not item.get('target_cid'):
            raise ValueError(f"重组参数不完整: record_id={item.get('id')}")
        key = (item['tmdb_id'], item['media_type'], item['target_cid'], item.get('season_num'))
        if key not in grouped:
            grouped[key] = []
            group_order.append(key)
        grouped[key].append(item['id'])

    total = sum(len(grouped[k]) for k in group_order)
    done = 0
    _manual_correct_task_update(0, f"  ➜ [手动重组任务] 已进入媒体任务队列，待处理 {total} 条记录 / {len(group_order)} 个分组")

    # 延迟导入，避免 tasks.p115 与 handler.p115_service 顶层互相导入形成循环。
    from handler.p115_service import _batch_manual_correct

    for index, key in enumerate(group_order, start=1):
        if processor and getattr(processor, 'is_stop_requested', lambda: False)():
            raise InterruptedError("手动重组任务已被用户中止")

        group_record_ids = grouped[key]
        group_tmdb_id, group_media_type, group_target_cid, group_season_num = key
        progress = int((done / max(total, 1)) * 90)
        _manual_correct_task_update(
            progress,
            f"  ➜ [手动重组任务] ({index}/{len(group_order)}) 开始重组 {len(group_record_ids)} 条记录 -> TMDb {group_tmdb_id}"
        )

        _batch_manual_correct(
            group_record_ids,
            group_tmdb_id,
            group_media_type,
            group_target_cid,
            group_season_num,
        )
        done += len(group_record_ids)

    result = {'groups': len(group_order), 'records': total}
    _manual_correct_task_update(100, f"  ➜ [手动重组任务] 完成：共处理 {total} 条记录 / {len(group_order)} 个分组")
    return result


# ★ 构建一个轻量级的独立探测器，供增量/全量同步任务生成 mediainfo 使用
class _StandaloneProber(P115MediaAnalyzerMixin):
    def __init__(self, client):
        self.client = client

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
            force_nested_package_scan = bool(is_folder and _should_force_nested_package_scan(top_name))
            transfer_context = P115CacheManager.get_transfer_context(top_name)
            transfer_context_hints = _transfer_context_to_recognition_hints(transfer_context)

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
                    is_tv_hint = _name_has_tv_hint(top_name)
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
                
                def sync_scan(current_cid, depth=0, rel_dir=""):
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
                            child_rel_dir = f"{rel_dir}/{c_name}".strip('/') if c_is_folder else rel_dir
                            child['_etk_rel_dir'] = rel_dir
                            
                            c_is_tv_hint = _name_has_tv_hint(c_name)
                            c_is_season_dir = c_is_folder and _name_is_season_dir(c_name)
                            
                            if c_is_folder:
                                if not force_nested_package_scan and (c_is_season_dir or c_is_tv_hint):
                                    is_tv_group = True
                                    if c_is_season_dir: has_season_dir = True
                                
                                has_tmdb = _name_has_tmdb_tag(top_name)
                                
                                # ★ 核心提速：如果是剧集或已标记TMDB，绝不可能是大杂烩，直接把文件夹当做 item 塞进去，不再深入！
                                if depth > 0 and not force_nested_package_scan and (is_tv_group or has_tmdb):
                                    child['_etk_rel_dir'] = rel_dir
                                    gathered_files.append(child)
                                else:
                                    sync_scan(c_id, depth + 1, child_rel_dir)
                                
                                # 将目录加入垃圾回收器
                                P115DeleteBuffer.add(fids=[], base_cids=[c_id])
                            else:
                                c_ext = c_name.split('.')[-1].lower() if '.' in c_name else ''
                                if c_ext in allowed_exts:
                                    gathered_files.append(child)
                                    if c_is_tv_hint and not force_nested_package_scan:
                                        is_tv_group = True
                                else:
                                    if c_ext not in KNOWN_SKIP_EXTS:
                                        local_unidentified.append(child)
                                        
                        if len(c_data) < 1000: break
                        c_offset += 1000

                # 执行同步扫盘
                sync_scan(top_id, 0, "")
                
                # ★ 大包逐文件识别逻辑
                has_tmdb = _name_has_tmdb_tag(top_name)
                structure_info = _analyze_nested_root_structure(top_name, gathered_files)
                valid_video_files = structure_info["valid_video_files"]
                should_use_filewise = (
                    structure_info["should_force_filewise"]
                    or force_nested_package_scan
                    or _should_use_filewise_big_package(top_name, is_tv_group, has_season_dir, has_tmdb, valid_video_files)
                )
                season_dir_groups = _build_standard_season_dir_groups(
                    top_name,
                    gathered_files,
                    is_tv_group=is_tv_group,
                    has_season_dir=has_season_dir,
                )

                if season_dir_groups:
                    logger.info(f"  ➜ [分季批处理] 检测到标准多季目录，已拆分为 {len(season_dir_groups)} 个季批次。")
                    groups_to_process.extend(season_dir_groups)
                elif should_use_filewise:
                    logger.info(f"  ➜ [大包模式] 检测到深层嵌套资源目录 '{top_name}'，执行逐文件识别...")
                    filewise_groups, filewise_unresolved = _build_filewise_big_package_groups(
                        gathered_files,
                        top_name,
                        ai_translator=ai_translator,
                        use_ai=use_ai,
                    )
                    groups_to_process.extend(filewise_groups)
                    local_unidentified.extend(filewise_unresolved)
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
                season_num = group.get("forced_season")
                recognition_hints = group.get("recognition_hints")
                tmdb_id = group.get("identified_tmdb_id")
                media_type = group.get("identified_media_type")
                title = group.get("identified_title")

                if forced_type == 'tv' and season_num is None:
                    season_num = _extract_season_number(g_top_name)

                if transfer_context and not tmdb_id:
                    tmdb_id = str(transfer_context.get("tmdb_id") or "").strip() or None
                    media_type = str(transfer_context.get("media_type") or "").strip() or media_type or forced_type
                    title = transfer_context.get("title") or title or g_top_name
                    if media_type == 'tv' and season_num is None:
                        season_num = transfer_context.get("season_number")

                recognition_hints = _merge_authority_hints(
                    transfer_context_hints,
                    recognition_hints,
                    is_tv=((media_type or forced_type) == 'tv'),
                )

                if not tmdb_id:
                    # 👇 核心修改：提取组内第一个视频的 SHA1，传给识别函数，直接从 RAW 提取 TMDb ID！
                    group_sha1 = None
                    for f in g_files:
                        f_name = f.get('fn', '')
                        ext = f_name.split('.')[-1].lower() if '.' in f_name else ''
                        if ext in KNOWN_VIDEO_EXTS:
                            group_sha1 = f.get('sha1') or f.get('sha')
                            if group_sha1:
                                break

                    recognition_hints = _merge_authority_hints(
                        transfer_context_hints,
                        lookup_candidate_hint_for_name(g_top_name, alt_texts=[top_name], media_type=forced_type),
                        is_tv=(forced_type == 'tv')
                    )
                    tmdb_id, media_type, title = _identify_media_enhanced(
                        g_top_name, main_dir_name=g_top_name, has_season_subdirs=group["has_season_dir"],
                        forced_media_type=forced_type, ai_translator=ai_translator, use_ai=use_ai, is_folder=False,
                        sha1=group_sha1,
                        recognition_hints=recognition_hints
                    )
                
                if not tmdb_id:
                    logger.warning(f"  ➜ 无法识别媒体组: {g_top_name}，打入未识别。")
                    local_unidentified.extend(g_files)
                    continue
                    
                try:
                    organizer_hints = _merge_authority_hints(
                        transfer_context_hints,
                        recognition_hints,
                        is_tv=(media_type == 'tv')
                    )
                    organizer = SmartOrganizer(
                        client,
                        tmdb_id,
                        media_type,
                        title,
                        ai_translator,
                        use_ai,
                        recognition_hints=organizer_hints,
                    )
                    organizer.recognition_hints = organizer_hints
                    if season_num is not None: organizer.forced_season = season_num
                    
                    # 执行整理 (直接传 None，让 execute 内部统一计算最终的 target_cid)
                    if organizer.execute(g_files, None, skip_gc=True):
                        local_processed += len(g_files)
                        if transfer_context:
                            P115CacheManager.delete_transfer_context(top_name, g_top_name, title)
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
                        
                        if ext in KNOWN_VIDEO_EXTS:
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
        P115DeleteBuffer.add(check_save_path=True)
        
        final_msg = f"扫描结束！成功归类 {total_processed} 个，移入未识别 {total_unidentified} 个。"
        logger.info(f"=== {final_msg} ===")
        update_progress(100, final_msg)

    except Exception as e:
        logger.error(f"  ➜ 115 扫描任务异常: {e}", exc_info=True)
        update_progress(100, f"扫描异常结束: {e}")

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
    save_cid = str(config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID) or '').strip()
    save_name = str(config.get(constants.CONFIG_OPTION_115_SAVE_PATH_NAME) or '待整理').strip()
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
        
        if not local_root or not etk_url or not etk_url.startswith('http'):
            update_progress(100, "错误：请配置 http(s) 开头的 ETK 访问地址。")
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
        remote_file_ids = set()
        synced_cache_file_ids = set()
        synced_cache_dir_ids = set()
        successful_target_cids = set()
        remote_dir_local_path = {}
        remote_dir_cache_rows = {}
        path_anomaly_file_ids = []
        path_anomaly_file_id_set = set()
        path_anomaly_names = []
        path_anomaly_move_failed = 0

        def first_present(*values):
            for value in values:
                if value is not None and str(value).strip() != '':
                    return value
            return None

        def node_id(node):
            if not isinstance(node, dict):
                return ''
            return str(
                node.get('id') or node.get('cid') or node.get('file_id') or
                node.get('fid') or node.get('parent_id') or ''
            ).strip()

        def node_name(node):
            if not isinstance(node, dict):
                return ''
            return str(node.get('name') or node.get('file_name') or node.get('fn') or node.get('n') or '').strip()

        def remember_remote_dir(dir_id, parent_id, name, rel_path):
            dir_id = str(dir_id or '').strip()
            parent_id = str(parent_id or '').strip()
            name = str(name or '').strip()
            rel_path = str(rel_path or '').strip()
            if not dir_id or not parent_id or not name or not rel_path:
                return
            synced_cache_dir_ids.add(dir_id)
            remote_dir_local_path[dir_id] = rel_path
            remote_dir_cache_rows[dir_id] = {
                'parent_id': parent_id,
                'name': name,
                'local_path': rel_path.replace('\\', '/')
            }
            dir_cache[dir_id] = {'pid': parent_id, 'name': name, 'local_path': rel_path}

        def remember_ancestor_dirs(ancestors, target_cid, category_name, file_id=None):
            if not isinstance(ancestors, (list, tuple)):
                return None

            target = str(target_cid)
            file_id = str(file_id or '').strip()
            start_idx = -1
            for i, anc in enumerate(ancestors):
                if node_id(anc) == target:
                    start_idx = i + 1
                    break
            if start_idx == -1:
                return None

            parent_id = target
            parts = []
            rel_dir = category_name
            for anc in ancestors[start_idx:]:
                current_id = node_id(anc)
                current_name = node_name(anc)
                if not current_id or not current_name:
                    continue
                if file_id and current_id == file_id:
                    break

                parts.append(current_name)
                rel_path = os.path.join(category_name, *parts)
                remember_remote_dir(current_id, parent_id, current_name, rel_path)
                parent_id = current_id
                rel_dir = rel_path

            return rel_dir

        # 内存路径推导函数 (★ 终极修复版：DB缓存 + API动态溯源)
        def resolve_local_dir(pid, target_cid):
            pid = str(pid)
            # 1. 如果文件直接在分类根目录下
            if pid in cid_to_rel_path:
                return cid_to_rel_path[pid]
                
            # 2. 如果本次远端扫描已经带出了目录链，优先使用远端真实路径
            if pid in remote_dir_local_path:
                return remote_dir_local_path[pid]

            # 3. 如果刚才已经通过 API 查过这个目录了，直接秒回
            if pid in dynamic_path_cache:
                return dynamic_path_cache[pid]

            # 4. 优先向 115 请求真实路径，避免旧 p115_filesystem_cache 把 STRM 继续生成到脏路径
            try:
                dir_info = client.fs_files({'cid': pid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                path_nodes = dir_info.get('path', [])
                if path_nodes:
                    base_cat_path = cid_to_rel_path.get(target_cid, '未识别')
                    resolved_path = remember_ancestor_dirs(path_nodes, target_cid, base_cat_path)
                    if resolved_path:
                        dynamic_path_cache[pid] = resolved_path # 存入内存池，同目录文件不再请求
                        logger.debug(f"  ➜ [API溯源] 成功动态推导路径: {resolved_path}")
                        return resolved_path
            except Exception as e:
                logger.debug(f"  ➜ 动态查询目录路径失败 (pid: {pid}): {e}")

            # 5. 远端路径不可用时，才使用数据库缓存兜底
            if pid in dir_cache and dir_cache[pid].get('local_path'):
                return dir_cache[pid]['local_path']
                
            # 6. 最后尝试在数据库缓存中向上追溯
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

            return None

        def queue_path_anomaly_file(fid, name, reason):
            fid = str(fid or '').strip()
            if not fid:
                logger.warning(
                    f"  ➜ [全量同步] 发现无法推导路径的 115 文件，但缺少 fid，无法自动移动：{name or '-'}，原因={reason}"
                )
                return False
            if fid in path_anomaly_file_id_set:
                return False
            path_anomaly_file_id_set.add(fid)
            path_anomaly_file_ids.append(fid)
            path_anomaly_names.append(str(name or fid))
            remote_file_ids.discard(fid)
            logger.warning(
                f"  ➜ [全量同步] 发现无法推导路径的 115 文件，已加入待整理移动队列：{name or fid}，原因={reason}"
            )
            return True

        def move_path_anomaly_files_to_inbox():
            nonlocal path_anomaly_move_failed
            if not path_anomaly_file_ids:
                return 0
            if not save_cid or save_cid == '0':
                logger.warning(
                    f"  ➜ [全量同步] 已发现 {len(path_anomaly_file_ids)} 个无法推导路径的 115 文件，"
                    "但未配置待整理目录 CID，无法自动移动。"
                )
                return 0

            moved_count = 0
            batch_size = 100
            update_progress(88, f"  ➜ 正在把 {len(path_anomaly_file_ids)} 个路径异常文件移动到 [{save_name}]...")
            for start in range(0, len(path_anomaly_file_ids), batch_size):
                batch = path_anomaly_file_ids[start:start + batch_size]
                try:
                    resp = client.fs_move(batch, save_cid)
                    if resp.get('state'):
                        moved_count += len(batch)
                    else:
                        path_anomaly_move_failed += len(batch)
                        logger.warning(f"  ➜ [全量同步] 路径异常文件移动失败：{resp}")
                except Exception as e:
                    path_anomaly_move_failed += len(batch)
                    logger.warning(f"  ➜ [全量同步] 路径异常文件移动异常：{e}")
            return moved_count

        def process_full_sync_items(items, target_cid, category_name):
            nonlocal files_generated, subs_downloaded, root_anomaly_skipped
            for item in items:
                # 兼容 OpenAPI、Cookie 和 p115client 标准化字段
                name = first_present(item.get('fn'), item.get('n'), item.get('file_name'), item.get('name')) or ''
                ext = name.split('.')[-1].lower() if '.' in name else ''
                if ext not in allowed_exts:
                    continue

                pc = first_present(item.get('pc'), item.get('pick_code'), item.get('pickcode'))
                # 115 返回的文件数据中，pid/cid/parent_id 代表它所在的父目录 ID
                pid = first_present(item.get('pid'), item.get('cid'), item.get('parent_id'))
                fid = first_present(item.get('fid'), item.get('file_id'), item.get('id'))
                if fid:
                    remote_file_ids.add(str(fid))
                ancestors = item.get('ancestors') or item.get('paths') or item.get('path')
                ancestor_rel_dir = remember_ancestor_dirs(ancestors, target_cid, category_name, fid)
                if not pc or pid is None:
                    continue
                pid_text = str(pid).strip()
                if not pid_text:
                    continue
                if pid_text == '0' and not item.get('_etk_rel_dir'):
                    fid = first_present(item.get('fid'), item.get('file_id'), item.get('id'))
                    if queue_path_anomaly_file(fid, name, "父目录为根目录"):
                        root_anomaly_skipped += 1
                    continue

                rel_dir = item.get('_etk_rel_dir') or ancestor_rel_dir or resolve_local_dir(pid, target_cid)
                if not rel_dir:
                    if queue_path_anomaly_file(fid, name, f"无法推导本地路径(pid={pid})"):
                        root_anomaly_skipped += 1
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
                        continue
                    strm_name = os.path.splitext(name)[0] + ".strm"
                    strm_path = os.path.join(current_local_path, strm_name)

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
                        except Exception:
                            pass

                    if need_write:
                        was_existing_strm = os.path.exists(strm_path)
                        with open(strm_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        if not was_existing_strm:
                            logger.debug(f"  ➜ [新增] 生成 STRM: {strm_name}")
                        else:
                            changed_strm_files.add(os.path.abspath(strm_path))
                        files_generated += 1

                    valid_local_files.add(os.path.abspath(strm_path))

                    sha1 = item.get('sha1') or item.get('sha')

                    # 生成 Mediainfo (等同 MP 直出逻辑)
                    if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                        mediainfo_filename = os.path.splitext(name)[0] + "-mediainfo.json"
                        mediainfo_filepath = os.path.join(current_local_path, mediainfo_filename)

                        if os.path.exists(mediainfo_filepath):
                            valid_local_files.add(os.path.abspath(mediainfo_filepath))
                        else:
                            try:
                                mediainfo_text = None
                                if sha1:
                                    mediainfo_text = P115CacheManager.get_mediainfo_cache_text(sha1)

                                if not mediainfo_text:
                                    prober = _StandaloneProber(client)
                                    probe_item = {'fid': fid, 'pc': pc, 'sha1': sha1, 'fn': name, 'fs': raw_size}
                                    mediainfo_obj = prober._probe_mediainfo_with_ffprobe(probe_item, sha1=sha1, silent_log=True)
                                    if mediainfo_obj:
                                        probe_sha1 = sha1 or probe_item.get('sha1')
                                        if probe_sha1:
                                            probe_sha1 = str(probe_sha1).upper()
                                            P115CacheManager.save_mediainfo_cache(probe_sha1, mediainfo_obj)
                                            sha1 = probe_sha1
                                        mediainfo_text = json.dumps(mediainfo_obj, ensure_ascii=False, indent=2)

                                if mediainfo_text:
                                    with open(mediainfo_filepath, "w", encoding="utf-8") as f:
                                        f.write(mediainfo_text)
                                    logger.info(f"  ➜ [全量同步] 媒体信息已生成 -> {mediainfo_filename}")
                            except Exception as e:
                                logger.error(f"  ➜ [全量同步] 生成媒体信息失败: {e}")

                            if os.path.exists(mediainfo_filepath):
                                valid_local_files.add(os.path.abspath(mediainfo_filepath))

                    # 写入本地数据库缓存 (p115_filesystem_cache)
                    if pc and fid:
                        file_local_path = os.path.join(rel_dir, name).replace('\\', '/')
                        synced_cache_file_ids.add(str(fid))
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
                                    for chunk in resp.iter_content(8192):
                                        f.write(chunk)
                                logger.info(f"  ⬇️ [增量] 下载字幕: {name}")
                                subs_downloaded += 1
                        except Exception as e:
                            logger.error(f"  ➜ 下载字幕失败 [{name}]: {e}")

                    valid_local_files.add(os.path.abspath(sub_path))

        def run_cookie_fast_sync(target_cid, category_name, progress):
            raw_p115_client = getattr(client, 'raw_client', None)
            if raw_p115_client is None and hasattr(client, 'native_client'):
                raw_p115_client = client.native_client()
            if not raw_p115_client:
                return None

            try:
                from p115client.tool.iterdir import iter_files_with_path_skim
            except Exception as e:
                logger.debug(f"  ➜ Cookie 极速遍历组件不可用，改用 OpenAPI 拉取：{e}")
                return None

            try:
                update_progress(progress, f"  ➜ 正在使用 Cookie 极速遍历分类 [{category_name}] ...")
                iterator = iter_files_with_path_skim(
                    raw_p115_client,
                    int(target_cid),
                    with_ancestors=True,
                    max_workers=2,
                )

                count = 0
                for info in iterator:
                    if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                        return 0
                    fid = info.get('fid') or info.get('file_id') or info.get('id')
                    is_dir = info.get('is_dir') or info.get('ico') == 'folder' or str(info.get('fc', '')) == '0'
                    if not fid or is_dir:
                        continue

                    rel_dir = remember_ancestor_dirs(
                        info.get('ancestors') or info.get('paths') or info.get('path'),
                        target_cid,
                        category_name,
                        fid,
                    )
                    if rel_dir:
                        info['_etk_rel_dir'] = rel_dir

                    process_full_sync_items((info,), target_cid, category_name)
                    count += 1
                    if count % 500 == 0:
                        update_progress(progress, f"  ➜ [{category_name}] Cookie 极速遍历中，已处理 {count} 个文件...")

                return count
            except Exception as e:
                text = str(e)
                if '405' in text or '403' in text or 'Method Not Allowed' in text:
                    logger.warning("  ➜ Cookie 极速遍历被 115 拒绝，自动改用 OpenAPI 慢速拉取。")
                else:
                    logger.warning(f"  ➜ Cookie 极速遍历失败，自动改用 OpenAPI 慢速拉取：{e}")
                return None

        # =================================================================
        # 阶段 2: 分类目录级全局拉取 (耗时: 秒级/分钟级)
        # =================================================================
        sync_has_errors = False
        valid_local_files = set()
        files_generated = 0
        subs_downloaded = 0
        root_anomaly_skipped = 0
        changed_strm_files = set()
        
        fetch_types = [4] # 4=视频
        if download_subs: fetch_types.append(1) # 1=文档(含字幕)

        total_targets = len(target_cids)
        
        for idx, target_cid in enumerate(target_cids):
            category_name = cid_to_rel_path.get(target_cid, "未知分类")
            base_prog = 10 + int((idx / total_targets) * 80)
            target_invalid = False
            fast_count = run_cookie_fast_sync(target_cid, category_name, base_prog)
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                return
            if fast_count:
                update_progress(base_prog, f"  ➜ [{category_name}] Cookie 极速遍历完成：{fast_count} 个文件")
                successful_target_cids.add(target_cid)
                continue
            if fast_count == 0:
                logger.warning(f"  ➜ [{category_name}] Cookie 极速遍历没有返回文件，改用 OpenAPI 复查。")

            update_progress(base_prog, f"  ➜ 正在使用 OpenAPI 拉取分类 [{category_name}] 下的所有文件...")
            
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
                        if not _p115_response_path_contains_cid(res, target_cid):
                            logger.warning(
                                f"  ➜ [{category_name}] OpenAPI 返回路径不包含目标 cid={target_cid}，"
                                "可能远端目录已删除或本地缓存过期，已跳过该分类的兜底同步。"
                            )
                            sync_has_errors = True
                            target_invalid = True
                            break
                        data = res.get('data', [])
                        if not data: break
                        
                        logger.info(f"  ➜ [{category_name}] - [{type_name}] 获取第 {page} 页 ({len(data)} 个文件)...")
                        process_full_sync_items(data, target_cid, category_name)

                        if len(data) < limit: break
                        offset += limit
                        page += 1
                        
                    except Exception as e:
                        logger.error(f"  ➜ 全局拉取异常 (cid={target_cid}, type={f_type}): {e}")
                        sync_has_errors = True
                        break
                if target_invalid:
                    break
            if not target_invalid:
                successful_target_cids.add(target_cid)

        logger.info(f"  ➜ 增量同步完成！新增/更新 STRM: {files_generated} 个, 下载字幕: {subs_downloaded} 个。")
        moved_path_anomaly_count = move_path_anomaly_files_to_inbox()
        if root_anomaly_skipped:
            sample_names = "、".join(path_anomaly_names[:3])
            if len(path_anomaly_names) > 3:
                sample_names += " ..."
            logger.warning(
                f"  ➜ [全量同步] 已处理 {root_anomaly_skipped} 个 115 路径异常文件："
                f"移动到 [{save_name}] {moved_path_anomaly_count} 个，失败 {path_anomaly_move_failed} 个。示例：{sample_names}"
            )

        # =================================================================
        # 阶段 2.5: 对账并清理 p115_filesystem_cache 与本地 STRM，使其收敛到远端当前状态
        # =================================================================
        if sync_has_errors:
            logger.warning("  ➜ [三方对账] 本次远端同步存在异常，跳过缓存与本地 STRM 清理以避免误删。")
        else:
            try:
                target_cid_list = list(successful_target_cids)
                deleted_cache_files = 0
                deleted_cache_dirs = 0
                cleaned_strm_files = 0

                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        for dir_id, row in remote_dir_cache_rows.items():
                            cursor.execute("""
                                INSERT INTO p115_filesystem_cache (id, parent_id, name, local_path)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (parent_id, name)
                                DO UPDATE SET
                                    id = EXCLUDED.id,
                                    local_path = EXCLUDED.local_path,
                                    updated_at = NOW()
                            """, (dir_id, row['parent_id'], row['name'], row['local_path']))

                        if target_cid_list:
                            cursor.execute("""
                                WITH RECURSIVE managed_tree(id) AS (
                                    SELECT id
                                    FROM p115_filesystem_cache
                                    WHERE parent_id = ANY(%s)
                                    UNION
                                    SELECT c.id
                                    FROM p115_filesystem_cache c
                                    JOIN managed_tree mt ON c.parent_id = mt.id
                                )
                                DELETE FROM p115_filesystem_cache d
                                USING managed_tree mt
                                WHERE d.id = mt.id
                                  AND NOT (d.id = ANY(%s))
                                  AND NOT (d.id = ANY(%s))
                                  AND (
                                      COALESCE(d.pick_code, '') <> ''
                                      OR COALESCE(d.sha1, '') <> ''
                                      OR COALESCE(d.size, 0) > 0
                                  )
                            """, (target_cid_list, target_cid_list, list(remote_file_ids)))
                            deleted_cache_files = cursor.rowcount or 0

                            while True:
                                cursor.execute("""
                                    WITH RECURSIVE managed_tree(id) AS (
                                        SELECT id
                                        FROM p115_filesystem_cache
                                        WHERE parent_id = ANY(%s)
                                        UNION
                                        SELECT c.id
                                        FROM p115_filesystem_cache c
                                        JOIN managed_tree mt ON c.parent_id = mt.id
                                    )
                                    DELETE FROM p115_filesystem_cache d
                                    USING managed_tree mt
                                    WHERE d.id = mt.id
                                      AND NOT (d.id = ANY(%s))
                                      AND NOT (d.id = ANY(%s))
                                      AND COALESCE(d.pick_code, '') = ''
                                      AND COALESCE(d.sha1, '') = ''
                                      AND COALESCE(d.size, 0) = 0
                                      AND NOT EXISTS (
                                          SELECT 1 FROM p115_filesystem_cache child WHERE child.parent_id = d.id
                                      )
                                    RETURNING d.id
                                """, (target_cid_list, target_cid_list, list(synced_cache_dir_ids)))
                                batch_deleted = len(cursor.fetchall())
                                if not batch_deleted:
                                    break
                                deleted_cache_dirs += batch_deleted

                        conn.commit()

                valid_strm_paths = {p for p in valid_local_files if p.lower().endswith('.strm')}
                for cid in target_cid_list:
                    rel_path = cid_to_rel_path.get(cid)
                    if not rel_path:
                        continue
                    target_local_dir = os.path.join(local_root, rel_path)
                    if not os.path.exists(target_local_dir):
                        continue
                    for root_dir, _, files in os.walk(target_local_dir):
                        for filename in files:
                            if not filename.lower().endswith('.strm'):
                                continue
                            strm_path = os.path.abspath(os.path.join(root_dir, filename))
                            if strm_path in valid_strm_paths:
                                continue
                            try:
                                os.remove(strm_path)
                                cleaned_strm_files += 1
                                logger.debug(f"  ➜ [三方对账] 删除远端已不存在的 STRM: {strm_path}")
                            except Exception as e:
                                logger.warning(f"  ➜ [三方对账] 删除失效 STRM 失败 {strm_path}: {e}")

                logger.info(
                    f"  ➜ [三方对账] 缓存新增/更新目录 {len(remote_dir_cache_rows)} 条，"
                    f"清理失踪文件缓存 {deleted_cache_files} 条、空目录缓存 {deleted_cache_dirs} 条、"
                    f"本地失效 STRM {cleaned_strm_files} 个。"
                )
            except Exception as e:
                logger.error(f"  ➜ [三方对账] 缓存/STRM 对账失败: {e}", exc_info=True)
        # =================================================================
        # 阶段 3: 本地失效文件清理 (耗时: 秒级)
        # =================================================================
        if enable_cleanup:
            if sync_has_errors:
                logger.warning("  🛑 致命警告：本次同步过程中发生 API 异常或触发 115 流控！为防止灾难性误删，已强制跳过本地清理阶段！")
            elif not valid_local_files and files_generated == 0:
                logger.warning("  ➜ 警告：本次同步未获取到任何有效文件，为防止误删，已跳过本地清理阶段！")
            else:
                update_progress(90, "  ➜ 正在比稳并清理本地失效文件与空壳目录...")
                cleaned_files = 0
                cleaned_dirs = 0
                import shutil
                import re  # ★ 引入正则模块
                
                # ★ 提取所有有效 STRM 的基础路径 (不含扩展名)
                valid_strm_bases = set()
                for p in valid_local_files:
                    if p.lower().endswith('.strm'):
                        valid_strm_bases.add(os.path.splitext(p)[0])

                # 目录级元数据白名单 (精确匹配)
                exact_metadata_names = {
                    'tvshow.nfo', 'season.nfo', 'movie.nfo', 'collection.nfo',
                    'poster.jpg', 'folder.jpg', 'fanart.jpg', 'landscape.jpg', 
                    'logo.png', 'clearlogo.png', 'banner.jpg', 'backdrop.jpg',
                    'theme.mp3'
                }
                
                for cid, rel_path in cid_to_rel_path.items():
                    target_local_dir = os.path.join(local_root, rel_path)
                    if not os.path.exists(target_local_dir): continue
                    
                    # 1. ★ 智能清理：清理失效的 STRM 及其衍生的 nfo, jpg, mediainfo, 字幕等
                    for root_dir, dirs, files in os.walk(target_local_dir):
                        # 找出当前目录下所有有效的 strm 基础路径
                        current_dir_valid_bases = [
                            b for b in valid_strm_bases 
                            if os.path.dirname(b) == root_dir
                        ]

                        for file in files:
                            file_path = os.path.abspath(os.path.join(root_dir, file))
                            file_lower = file.lower()
                            
                            # 规则 1: 如果文件在有效名单中 (有效的 strm, 刚下载的字幕, 刚生成的 mediainfo)，保留
                            if file_path in valid_local_files:
                                continue
                                
                            # 规则 2: 如果是目录级别的元数据文件 (精确匹配)，保留
                            if file_lower in exact_metadata_names:
                                continue
                                
                            # 规则 2.1: ★ 兼容 Emby/Jellyfin 的季级别海报/元数据 (例如 season01-poster.jpg, season-specials-fanart.jpg)
                            if re.match(r'^season.*(?:poster|fanart|banner|landscape|logo|clearlogo|thumb)\.(?:jpg|png)$', file_lower):
                                continue
                            # 兼容 season01.nfo 等季级别 NFO
                            if re.match(r'^season.*\.nfo$', file_lower):
                                continue
                                
                            # 规则 3: 检查该文件是否是当前目录下某个有效 strm 的衍生文件
                            is_derivative = False
                            for valid_base in current_dir_valid_bases:
                                if file_path.startswith(valid_base + '.') or file_path.startswith(valid_base + '-'):
                                    is_derivative = True
                                    break
                            
                            if is_derivative:
                                continue
                                
                            # 规则 4: 既不是有效文件，也不是目录元数据，也不是有效 strm 的衍生文件 -> 判定为失效/孤儿文件，删除！
                            try:
                                os.remove(file_path)
                                cleaned_files += 1
                                logger.debug(f"  ➜ [清理] 删除失效/孤儿文件: {file}")
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
                            
                logger.info(f"  ➜ 清理完成: 删除了 {cleaned_files} 个失效/孤儿文件, {cleaned_dirs} 个无STRM的空壳目录。")

        if changed_strm_files:
            try:
                from handler import emby
                emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
                emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
                if emby_url and emby_api_key:
                    update_progress(98, f"  ➜ 正在通知 Emby 扫描 {len(changed_strm_files)} 个已更新 STRM...")
                    emby.notify_emby_file_changes(
                        list(changed_strm_files),
                        emby_url,
                        emby_api_key,
                        update_type="Modified",
                    )
                else:
                    logger.warning("  ➜ 未配置 Emby 地址或 API Key，跳过全量生成 STRM 后的 Emby 扫描。")
            except Exception as e:
                logger.warning(f"  ➜ 通知 Emby 扫描全量生成 STRM 变更失败: {e}")

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
        
    if not local_root or not etk_url or not etk_url.startswith('http'):
        msg = "请配置 http(s) 开头的 ETK 访问地址。"
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

    if not local_root or not etk_url or not etk_url.startswith('http'):
        logger.warning("  ➜ [事件] 未配置 http(s) 开头的 ETK 访问地址，跳过 STRM 增量生成。")
        return

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

    # 动态 API 路径缓存池 (防止重复请求 115 接口)
    dynamic_path_cache = {}

    # 辅助函数：推导本地路径 (★ 终极修复版：加入 API 溯源与防误删保护)
    def resolve_local_dir(pid):
        pid = str(pid)
        if pid in cid_to_rel_path: return cid_to_rel_path[pid]
        if pid in dynamic_path_cache: return dynamic_path_cache[pid]
        
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
                            resolved_path = os.path.join(*parts)
                            dynamic_path_cache[pid] = resolved_path
                            return resolved_path
        except: pass
        
        # ★ 终极兜底：缓存穿透时，主动向 115 请求该目录的真实路径
        try:
            dir_info = client.fs_files({'cid': pid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
            if not dir_info.get('state'):
                # 如果 API 明确返回失败（比如目录不存在），那说明真不在了
                if dir_info.get('code') in [20004, 70004]: 
                    return None
                # 其他 API 错误（如流控），抛出异常触发保护
                raise Exception(f"API Error: {dir_info}")
                
            path_nodes = dir_info.get('path', [])
            if path_nodes:
                start_idx = -1
                target_cid_found = None
                for i, p_node in enumerate(path_nodes):
                    node_cid = str(p_node.get('cid') or p_node.get('file_id'))
                    if node_cid in target_cids:
                        start_idx = i + 1
                        target_cid_found = node_cid
                        break
                if start_idx != -1 and target_cid_found:
                    sub_folders = [str(p.get('name') or p.get('file_name')).strip() for p in path_nodes[start_idx:]]
                    base_cat_path = cid_to_rel_path.get(target_cid_found, '未识别')
                    resolved_path = os.path.join(base_cat_path, *sub_folders) if sub_folders else base_cat_path
                    dynamic_path_cache[pid] = resolved_path # 存入内存池
                    logger.debug(f"  ➜ [API溯源] 成功动态推导路径: {resolved_path}")
                    return resolved_path
                else:
                    # 确实不在监控目录内
                    return None
        except Exception as e:
            logger.warning(f"  ➜ 动态查询目录路径失败 (pid: {pid})，为防止误删，跳过该事件: {e}")
            return "API_ERROR_PROTECT"

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
            # ★ 触发保护机制，直接跳过，不删也不加
            if new_rel_dir == "API_ERROR_PROTECT":
                return 

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
                    
                # ★ 同步删除 Mediainfo
                mi_full = os.path.splitext(full_local_path)[0] + "-mediainfo.json"
                if os.path.exists(mi_full):
                    os.remove(mi_full)
                    logger.info(f"  ➜ [事件] 删除失效媒体信息: {os.path.basename(mi_full)}")
                    
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
                        
                    # ★ 同步删除旧的 Mediainfo
                    old_mi = os.path.splitext(old_full_path)[0] + "-mediainfo.json"
                    if os.path.exists(old_mi):
                        os.remove(old_mi)
                        
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
                    
                    # ==================================================
                    # ★ 生成 Mediainfo (等同 MP 直出逻辑)
                    # ==================================================
                    if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                        try:
                            mediainfo_filename = os.path.splitext(file_name)[0] + "-mediainfo.json"
                            mediainfo_filepath = os.path.join(current_local_path, mediainfo_filename)
                            if not os.path.exists(mediainfo_filepath):
                                mediainfo_text = None
                                if file_sha1:
                                    mediainfo_text = P115CacheManager.get_mediainfo_cache_text(file_sha1)

                                if not mediainfo_text:
                                    prober = _StandaloneProber(client)
                                    probe_item = {
                                        'fid': file_id, 'file_id': file_id,
                                        'pc': pick_code, 'pick_code': pick_code,
                                        'sha1': file_sha1,
                                        'fn': file_name, 'file_name': file_name,
                                        'fs': file_size, 'size': file_size
                                    }
                                    mediainfo_obj = prober._probe_mediainfo_with_ffprobe(probe_item, sha1=file_sha1, silent_log=False)
                                    if mediainfo_obj:
                                        probe_sha1 = file_sha1 or probe_item.get('sha1') or probe_item.get('sha')
                                        if probe_sha1:
                                            probe_sha1 = str(probe_sha1).upper()
                                            P115CacheManager.save_mediainfo_cache(probe_sha1, mediainfo_obj)
                                            file_sha1 = probe_sha1
                                        mediainfo_text = json.dumps(mediainfo_obj, ensure_ascii=False, indent=2)

                                if mediainfo_text:
                                    with open(mediainfo_filepath, "w", encoding="utf-8") as f:
                                        f.write(mediainfo_text)
                                    logger.info(f"  ➜ [事件] 媒体信息已生成 -> {mediainfo_filename}")
                        except Exception as e:
                            logger.error(f"  ➜ [事件] 生成媒体信息失败: {e}")
                    
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

# ======================================================================
# ★★★ 洗版优先级一键重算任务 ★★★
# ======================================================================
def _ensure_washing_priority_snapshot_columns():
    """确保洗版优先级快照字段存在，便于增量升级和手动补丁兼容。"""
    from database.connection import get_db_connection
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE p115_filesystem_cache
                ADD COLUMN IF NOT EXISTS washing_level INTEGER,
                ADD COLUMN IF NOT EXISTS washing_snapshot_json JSONB DEFAULT '{}'::jsonb;
            """)
            cursor.execute("""
                ALTER TABLE media_metadata
                ADD COLUMN IF NOT EXISTS washing_level INTEGER,
                ADD COLUMN IF NOT EXISTS washing_snapshot_json JSONB DEFAULT '{}'::jsonb;
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_mm_washing_level
                ON media_metadata (washing_level)
                WHERE in_library = TRUE AND item_type IN ('Movie','Episode');
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_p115_washing_level
                ON p115_filesystem_cache (washing_level)
                WHERE washing_level IS NOT NULL;
            """)
        conn.commit()


def _jsonish_to_obj(value, default=None):
    if default is None:
        default = []
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default
    return default


def _as_clean_list(value):
    value = _jsonish_to_obj(value, [])
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        if item in (None, '', [], {}):
            continue
        out.append(str(item).strip())
    return out


def _safe_int_or_none(value):
    try:
        if value in (None, ''):
            return None
        return int(value)
    except Exception:
        return None


def _first_non_empty(*values):
    for value in values:
        if value not in (None, '', [], {}):
            return value
    return None


def _make_washing_identity(row, file_name=None):
    item_type = str(row.get('item_type') or '').strip()
    media_type = 'movie' if item_type == 'Movie' else 'series'
    identity = {
        'title': row.get('title') or '',
        'tmdb_id': str(row.get('tmdb_id') or '').strip(),
        'file_name': file_name or '',
        'item_type': item_type,
        'media_type': media_type,
        'parent_series_tmdb_id': str(row.get('parent_series_tmdb_id') or '').strip() or None,
        'season_number': _safe_int_or_none(row.get('season_number')),
        'episode_number': _safe_int_or_none(row.get('episode_number')),
    }
    return {k: v for k, v in identity.items() if v not in (None, '', [], {})}


def _extract_asset_file_name(asset_details, index=0):
    assets = _jsonish_to_obj(asset_details, [])
    if isinstance(assets, dict):
        assets = [assets]
    if not isinstance(assets, list) or not assets:
        return ''
    candidates = []
    if index < len(assets):
        candidates.append(assets[index])
    candidates.extend(assets)
    for asset in candidates:
        if not isinstance(asset, dict):
            continue
        path = asset.get('path') or asset.get('Path') or asset.get('file_path') or asset.get('FilePath')
        name = asset.get('file_name') or asset.get('FileName') or asset.get('name') or asset.get('Name')
        if name:
            return str(name)
        if path:
            return os.path.basename(str(path).replace('\\', '/'))
    return ''


def _lookup_p115_cache_row(cursor, sha1='', pick_code=''):
    sha1 = str(sha1 or '').strip().upper()
    pick_code = str(pick_code or '').strip()
    clauses = []
    params = []
    order_params = []
    if pick_code:
        clauses.append('pick_code = %s')
        params.append(pick_code)
        order_params.append(pick_code)
    if sha1:
        clauses.append('UPPER(sha1) = %s')
        params.append(sha1)
    if not clauses:
        return None

    order_sql = 'updated_at DESC NULLS LAST'
    if pick_code:
        order_sql = 'CASE WHEN pick_code = %s THEN 0 ELSE 1 END, updated_at DESC NULLS LAST'

    cursor.execute(f"""
        SELECT id, parent_id, name, sha1, pick_code, local_path, size,
               washing_level, washing_snapshot_json
        FROM p115_filesystem_cache
        WHERE {' OR '.join(clauses)}
        ORDER BY {order_sql}
        LIMIT 1
    """, tuple(params + order_params))
    row = cursor.fetchone()
    return dict(row) if row else None


def _lookup_mediainfo_by_sha1(cursor, sha1):
    sha1 = str(sha1 or '').strip().upper()
    if not sha1:
        return None
    cursor.execute("""
        SELECT sha1, mediainfo_json
        FROM p115_mediainfo_cache
        WHERE sha1 = %s OR UPPER(sha1) = %s
        LIMIT 1
    """, (sha1, sha1))
    row = cursor.fetchone()
    return dict(row) if row else None


def _build_priority_input(
    raw_info,
    *,
    file_name='',
    file_size=0,
    original_lang='',
    media_type='',
    tmdb_id='',
    season_num=None,
    episode_num=None,
    need_clean_version_check=False,
):
    from handler.resubscribe_service import WashingService

    parsed = WashingService._safe_parse_jsonish(raw_info)
    if parsed is None:
        parsed = raw_info

    if isinstance(parsed, list):
        if parsed and isinstance(parsed[0], dict):
            info = dict(parsed[0])
        else:
            info = {}
    elif isinstance(parsed, dict):
        info = dict(parsed)
    else:
        info = {}

    if file_name:
        info['filename'] = file_name
    if file_size:
        info['_file_size'] = int(file_size or 0)
    if original_lang:
        info['_original_lang'] = original_lang
    if media_type:
        info['_media_type'] = media_type
    if tmdb_id:
        info['_tmdb_id'] = tmdb_id
    if season_num is not None:
        info['_season_num'] = season_num
    if episode_num is not None:
        info['_episode_num'] = episode_num
    if need_clean_version_check:
        info['_need_clean_version_check'] = True
    return info


def _infer_target_cid_from_local_path(cache_row=None, local_path=''):
    """用 p115_filesystem_cache.local_path 反推分类目录 CID。

    这是 MP 直出/旧数据的零 API 兜底：不向 115 向上溯源，
    只根据本地 STRM 相对路径匹配 p115_sorting_rules.category_path。
    """
    cache_row = cache_row or {}
    path = local_path or cache_row.get('local_path') or ''
    try:
        target = resolve_p115_sorting_target_by_local_path(path)
    except Exception as e:
        logger.debug(f"  ➜ [洗版优先级重算] 通过 local_path 推导分类 CID 失败: {path} -> {e}")
        return '', ''

    if not target:
        return '', ''
    return str(target.get('cid') or '').strip(), str(target.get('category_path') or '').strip()


def _has_normal_washing_priorities(priorities):
    """判断优先级列表里是否真的包含普通优先级规则。

    WashingService._load_priorities 会合并 media_type='All' 的全局规则。
    如果当前 target_cid 没命中分类规则，但存在全局排除规则，priorities 也不会为空。
    这种情况下仍应视作“未命中普通规则”，继续尝试 local_path 反推分类 CID。
    """
    if not isinstance(priorities, list):
        return False
    return any(isinstance(rule, dict) and not bool(rule.get('is_exclude', False)) for rule in priorities)


def _lookup_organize_record_target_cid(cursor, *, cache_row=None, pick_code='', sha1=''):
    """从 115 整理记录反查分类目标 CID。

    p115_filesystem_cache.parent_id 只是文件当前所在父目录，剧集通常是剧名/季目录，
    不能拿来匹配洗版优先级规则。真正的分类目标目录应以
    p115_organize_records.target_cid 为准。

    MP 直出模式通常没有 p115_organize_records，且不允许为了重算去频繁调用 115 API
    向上溯源；这种情况返回空，交给全局无目录规则兜底，或标记为未命中规则。
    """
    cache_row = cache_row or {}
    pick_code = str(pick_code or cache_row.get('pick_code') or '').strip()
    file_id = str(cache_row.get('id') or cache_row.get('fid') or '').strip()

    where = []
    params = []
    if pick_code:
        where.append('pick_code = %s')
        params.append(pick_code)
    if file_id:
        where.append('file_id = %s')
        params.append(file_id)

    if not where:
        return ''

    cursor.execute(f"""
        SELECT target_cid
        FROM p115_organize_records
        WHERE ({' OR '.join(where)})
          AND target_cid IS NOT NULL
          AND target_cid <> ''
        ORDER BY processed_at DESC NULLS LAST, id DESC
        LIMIT 1
    """, tuple(params))
    rec = cursor.fetchone()
    return str(rec.get('target_cid') or '').strip() if rec else ''


def _backfill_organize_record_target_cid(cursor, *, cache_row=None, row=None, target_cid='', category_name=''):
    """把 local_path 推导出的真实分类 CID 回填到 p115_organize_records。

    一键重算已经通过 p115_filesystem_cache.local_path 零 API 推导出了分类根目录 CID，
    顺手写回整理记录后，下次重算就能优先从 p115_organize_records.target_cid
    直接命中，不必每次都再跑路径前缀匹配。
    """
    cache_row = cache_row or {}
    row = row or {}
    target_cid = str(target_cid or '').strip()
    if not target_cid:
        return False

    file_id = str(cache_row.get('id') or cache_row.get('fid') or '').strip()
    pick_code = str(cache_row.get('pick_code') or '').strip()
    if not file_id and not pick_code:
        return False

    category_name = str(category_name or '').strip()
    original_name = str(cache_row.get('name') or row.get('title') or file_id or pick_code).strip()
    renamed_name = str(cache_row.get('name') or original_name).strip()
    tmdb_id = str(row.get('tmdb_id') or '').strip() or None
    item_type = str(row.get('item_type') or '').strip()
    media_type = 'movie' if item_type == 'Movie' else 'tv'
    season_number = row.get('season_number')

    where = []
    params = [target_cid, category_name or None, tmdb_id, media_type, season_number]
    if pick_code:
        where.append('pick_code = %s')
        params.append(pick_code)
    if file_id:
        where.append('file_id = %s')
        params.append(file_id)

    if where:
        cursor.execute(f"""
            UPDATE p115_organize_records
            SET target_cid = %s,
                category_name = COALESCE(NULLIF(category_name, ''), %s),
                tmdb_id = COALESCE(tmdb_id, %s),
                media_type = COALESCE(media_type, %s),
                season_number = COALESCE(season_number, %s)
            WHERE {' OR '.join(where)}
            RETURNING id
        """, tuple(params))
        if cursor.fetchone():
            return True

    if not file_id:
        return False

    try:
        cursor.execute("""
            INSERT INTO p115_organize_records
                (file_id, pick_code, original_name, status, tmdb_id, media_type,
                 target_cid, category_name, renamed_name, processed_at, season_number)
            VALUES (%s, %s, %s, 'success', %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (file_id)
            DO UPDATE SET
                target_cid = EXCLUDED.target_cid,
                category_name = COALESCE(NULLIF(p115_organize_records.category_name, ''), EXCLUDED.category_name),
                tmdb_id = COALESCE(p115_organize_records.tmdb_id, EXCLUDED.tmdb_id),
                media_type = COALESCE(p115_organize_records.media_type, EXCLUDED.media_type),
                season_number = COALESCE(p115_organize_records.season_number, EXCLUDED.season_number)
            RETURNING id
        """, (
            file_id,
            pick_code or None,
            original_name,
            tmdb_id,
            media_type,
            target_cid,
            category_name or None,
            renamed_name or None,
            season_number,
        ))
        return bool(cursor.fetchone())
    except Exception as e:
        logger.debug(
            f"  ➜ [洗版优先级重算] 回填 p115_organize_records.target_cid 失败: "
            f"file_id={file_id}, pc={(pick_code or '-')[:8]}, target={target_cid}, err={e}"
        )
        return False


def _resolve_media_original_language(cursor, row):
    """解析媒体原始语种；分集自身为空时回退父剧条目的 original_language。

    media_metadata 里 Episode 行通常没有 original_language，真实值在父级
    Series 行上。一键重算洗版优先级时，如果这里拿不到 zh/ja/ko 等
    原语种，后续“原产国豁免音轨/字幕规则”就无法生效。
    """
    original_lang = str((row or {}).get('original_language') or '').strip()
    if original_lang:
        return original_lang

    item_type = str((row or {}).get('item_type') or '').strip()
    if item_type != 'Episode':
        return ''

    parent_tmdb_id = str((row or {}).get('parent_series_tmdb_id') or '').strip()
    if not parent_tmdb_id:
        return ''

    try:
        cursor.execute("""
            SELECT original_language
            FROM media_metadata
            WHERE item_type = 'Series'
              AND tmdb_id = %s
              AND NULLIF(original_language, '') IS NOT NULL
            ORDER BY in_library DESC, last_updated_at DESC NULLS LAST
            LIMIT 1
        """, (parent_tmdb_id,))
        parent = cursor.fetchone()
        return str((parent or {}).get('original_language') or '').strip() if parent else ''
    except Exception as e:
        logger.debug(
            f"  ➜ [洗版优先级重算] 查询父剧原语种失败: "
            f"parent_tmdb={parent_tmdb_id}, err={e}"
        )
        return ''


def _evaluate_washing_level_for_row(cursor, row, *, only_update_p115=True):
    """重算单个 media_metadata 媒体项的洗版优先级，并写回 p115/cache + media_metadata。"""
    from handler.resubscribe_service import WashingService

    sha1s = _as_clean_list(row.get('file_sha1_json'))
    pickcodes = _as_clean_list(row.get('file_pickcode_json'))
    count = max(len(sha1s), len(pickcodes))
    evaluated_at = datetime.utcnow().isoformat() + 'Z'
    item_type = str(row.get('item_type') or '').strip()
    db_media_type = 'Movie' if item_type == 'Movie' else 'Series'
    media_type = 'movie' if item_type == 'Movie' else 'series'
    original_lang = _resolve_media_original_language(cursor, row)

    versions = []
    stats = {
        'version_count': count,
        'evaluated_versions': 0,
        'missing_raw': 0,
        'missing_identity': 0,
        'no_priority_rules': 0,
        'backfilled_target_cid': 0,
    }

    if count <= 0:
        snapshot_data = {
            'reason': '缺少 SHA1/PC，无法重算洗版优先级',
            'media_type': media_type,
            'evaluated_at': evaluated_at
        }
        cursor.execute("""
            UPDATE media_metadata
            SET washing_level = NULL,
                washing_snapshot_json = %s::jsonb,
                last_updated_at = NOW()
            WHERE tmdb_id = %s AND item_type = %s
        """, (json.dumps(snapshot_data, ensure_ascii=False), row.get('tmdb_id'), row.get('item_type')))
        stats['missing_identity'] += 1
        return stats

    for idx in range(count):
        sha1 = sha1s[idx].upper() if idx < len(sha1s) else ''
        pc = pickcodes[idx] if idx < len(pickcodes) else ''
        cache_row = _lookup_p115_cache_row(cursor, sha1=sha1, pick_code=pc)
        
        cache_snapshot = {}
        if cache_row:
            sha1 = sha1 or str(cache_row.get('sha1') or '').strip().upper()
            pc = pc or str(cache_row.get('pick_code') or '').strip()
            cache_snapshot = cache_row.get('washing_snapshot_json') or {}
            if isinstance(cache_snapshot, str):
                try: cache_snapshot = json.loads(cache_snapshot)
                except: cache_snapshot = {}

        file_name = (
            (cache_row or {}).get('name')
            or _extract_asset_file_name(row.get('asset_details_json'), idx)
            or ''
        )
        file_size = int((cache_row or {}).get('size') or 0)
        
        organize_target_cid = _lookup_organize_record_target_cid(
            cursor,
            cache_row=cache_row,
            pick_code=pc,
            sha1=sha1,
        )
        
        row_snapshot = row.get('washing_snapshot_json') or {}
        if isinstance(row_snapshot, str):
            try: row_snapshot = json.loads(row_snapshot)
            except: row_snapshot = {}

        target_cid = str(_first_non_empty(
            organize_target_cid,
            cache_snapshot.get('target_cid'),
            row_snapshot.get('target_cid'),
            ''
        ) or '').strip()
        
        inferred_target_cid, inferred_category_path = _infer_target_cid_from_local_path(cache_row)
        identity = _make_washing_identity(row, file_name=file_name)

        level = None
        reason = '未计算'

        raw_row = _lookup_mediainfo_by_sha1(cursor, sha1) if sha1 else None
        if not raw_row or raw_row.get('mediainfo_json') in (None, '', [], {}):
            level = 0
            reason = '缺少媒体流信息，无法重算洗版优先级'
            stats['missing_raw'] += 1
        else:
            try:
                priorities = WashingService._load_priorities(db_media_type, target_cid)

                if (not _has_normal_washing_priorities(priorities)) and inferred_target_cid and inferred_target_cid != target_cid:
                    fallback_priorities = WashingService._load_priorities(db_media_type, inferred_target_cid)
                    if _has_normal_washing_priorities(fallback_priorities):
                        old_target_cid = target_cid
                        target_cid = inferred_target_cid
                        priorities = fallback_priorities
                        logger.debug(
                            f"  ➜ [洗版优先级重算] local_path 推导分类 CID 生效: "
                            f"{old_target_cid or '-'} -> {target_cid} ({inferred_category_path or '-'}) | {file_name}"
                        )

                if not _has_normal_washing_priorities(priorities):
                    level = None
                    if inferred_target_cid and target_cid != inferred_target_cid:
                        reason = f'原目标分类CID({target_cid or "-"})只命中全局/排除规则，local_path 推导分类CID({inferred_target_cid})后仍未命中普通优先级规则'
                    elif target_cid:
                        reason = f'目标分类CID({target_cid})未命中普通洗版优先级规则'
                    else:
                        reason = '缺少整理目标分类CID，且 local_path 未命中分类规则'
                    stats['no_priority_rules'] += 1
                else:
                    priority_input = _build_priority_input(
                        raw_row.get('mediainfo_json'),
                        file_name=file_name,
                        file_size=file_size,
                        original_lang=original_lang,
                        media_type=media_type,
                        tmdb_id=row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '',
                        season_num=row.get('season_number'),
                        episode_num=row.get('episode_number'),
                        need_clean_version_check=WashingService._priorities_need_clean_version(priorities),
                    )
                    norm = WashingService._normalize_info(priority_input)
                    level, reason = WashingService.get_level(norm, priorities)
                    if inferred_target_cid and target_cid == inferred_target_cid and inferred_category_path:
                        reason = f"{reason}（分类路径: {inferred_category_path}）"
                    stats['evaluated_versions'] += 1
            except Exception as e:
                level = 0
                reason = f'重算异常: {e}'
                logger.warning(f"  ➜ [洗版优先级重算] 版本评分失败 sha1={sha1[:12]}...: {e}", exc_info=True)

        if inferred_target_cid and target_cid == inferred_target_cid and organize_target_cid != inferred_target_cid:
            try:
                if _backfill_organize_record_target_cid(
                    cursor,
                    cache_row=cache_row,
                    row=row,
                    target_cid=inferred_target_cid,
                    category_name=inferred_category_path,
                ):
                    stats['backfilled_target_cid'] += 1
            except Exception as e:
                logger.debug(
                    f"  ➜ [洗版优先级重算] 回填整理记录 target_cid 异常: "
                    f"sha1={sha1[:12]}..., target={inferred_target_cid}, err={e}"
                )

        version = {
            'fid': str((cache_row or {}).get('id') or ''),
            'sha1': sha1,
            'size': file_size,
            'level': level,
            'reason': reason,
            'identity': identity,
            'file_name': file_name,
            'pick_code': pc,
            'media_type': media_type,
            'target_cid': target_cid,
            'evaluated_at': evaluated_at,
        }
        versions.append(version)

        if cache_row and cache_row.get('id'):
            new_cache_snapshot = {
                'reason': reason,
                'target_cid': target_cid or None,
                'media_type': media_type,
                'identity': identity,
                'evaluated_at': evaluated_at
            }
            cursor.execute("""
                UPDATE p115_filesystem_cache
                SET washing_level = %s,
                    washing_snapshot_json = %s::jsonb,
                    updated_at = NOW()
                WHERE id = %s
            """, (
                level,
                json.dumps(new_cache_snapshot, ensure_ascii=False),
                cache_row.get('id'),
            ))

    def _best_sort_key(v):
        lvl = v.get('level')
        if isinstance(lvl, int) and lvl > 0:
            return (0, lvl)
        if lvl is None:
            return (2, 999999)
        return (1, abs(int(lvl or 0)))

    best = sorted(versions, key=_best_sort_key)[0] if versions else {}
    best_level = best.get('level') if best else None
    
    new_mm_snapshot = {
        'versions': versions,
        'reason': best.get('reason') if best else '无有效版本',
        'sha1': best.get('sha1') if best else None,
        'target_cid': best.get('target_cid') if best else None,
        'media_type': media_type,
        'evaluated_at': evaluated_at
    }

    cursor.execute("""
        UPDATE media_metadata
        SET washing_level = %s,
            washing_snapshot_json = %s::jsonb,
            last_updated_at = NOW()
        WHERE tmdb_id = %s AND item_type = %s
    """, (
        best_level,
        json.dumps(new_mm_snapshot, ensure_ascii=False),
        row.get('tmdb_id'),
        row.get('item_type'),
    ))

    return stats


def task_recalculate_library_washing_priorities(processor=None, item_type='all', limit=None):
    """重算当前媒体库所有电影/分集的洗版优先级快照。"""
    from database.connection import get_db_connection

    try:
        import task_manager
    except Exception:
        task_manager = None

    def _yield_frontend_log_polling():
        """让 gevent/前端日志轮询有机会实时取到进度。"""
        try:
            from gevent import sleep as gevent_sleep
            gevent_sleep(0.01) # ★ 核心修复：强制让出 10ms，保证前端 WebSocket/轮询能拿到数据
        except Exception:
            try:
                time.sleep(0.01)
            except Exception:
                pass

    def _flush_log_handlers():
        try:
            root_logger = logging.getLogger()
            for handler in list(getattr(root_logger, 'handlers', []) or []):
                try:
                    handler.flush()
                except Exception:
                    pass
        except Exception:
            pass

    def update_task_status(prog, msg):
        """只刷新顶部任务状态，不写实时日志，避免日志刷屏。"""
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)
        _yield_frontend_log_polling()

    def update_progress(prog, msg):
        """刷新顶部任务状态 + 写入实时日志。"""
        update_task_status(prog, msg)
        logger.info(msg)
        _flush_log_handlers()
        _yield_frontend_log_polling()

    _ensure_washing_priority_snapshot_columns()

    item_type = str(item_type or 'all').strip().lower()
    allowed_types = ['Movie', 'Episode']
    if item_type in ('movie', 'movies'):
        allowed_types = ['Movie']
    elif item_type in ('episode', 'episodes', 'series', 'tv'):
        allowed_types = ['Episode']

    stats = {
        'scanned_items': 0,
        'updated_items': 0,
        'evaluated_versions': 0,
        'missing_raw': 0,
        'missing_identity': 0,
        'no_priority_rules': 0,
        'backfilled_target_cid': 0,
        'errors': 0,
        'started_at': datetime.utcnow().isoformat() + 'Z',
        'finished_at': None,
    }

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            params = [allowed_types]
            limit_sql = ''
            if limit:
                try:
                    limit = int(limit)
                    if limit > 0:
                        limit_sql = 'LIMIT %s'
                        params.append(limit)
                except Exception:
                    limit_sql = ''

            cursor.execute(f"""
                SELECT mm.tmdb_id, mm.item_type, mm.parent_series_tmdb_id, mm.season_number, mm.episode_number,
                       mm.title,
                       COALESCE(NULLIF(mm.original_language, ''), NULLIF(parent.original_language, '')) AS original_language,
                       mm.file_sha1_json, mm.file_pickcode_json,
                       mm.asset_details_json, mm.washing_snapshot_json
                FROM media_metadata mm
                LEFT JOIN LATERAL (
                    SELECT p.original_language
                    FROM media_metadata p
                    WHERE mm.item_type = 'Episode'
                      AND p.item_type = 'Series'
                      AND p.tmdb_id = mm.parent_series_tmdb_id
                      AND NULLIF(p.original_language, '') IS NOT NULL
                    ORDER BY p.in_library DESC, p.last_updated_at DESC NULLS LAST
                    LIMIT 1
                ) parent ON TRUE
                WHERE mm.in_library = TRUE
                  AND mm.item_type = ANY(%s)
                ORDER BY mm.item_type ASC, COALESCE(mm.title, '') ASC, mm.tmdb_id ASC
                {limit_sql}
            """, tuple(params))
            rows = cursor.fetchall() or []
            total_rows = len(rows)
            update_progress(1, f"  ➜ [洗版优先级重算] 开始执行，待处理 {total_rows} 个媒体项")

            for row in rows:
                stats['scanned_items'] += 1
                try:
                    item_stats = _evaluate_washing_level_for_row(cursor, dict(row))
                    for key in ('evaluated_versions', 'missing_raw', 'missing_identity', 'no_priority_rules', 'backfilled_target_cid'):
                        stats[key] += int(item_stats.get(key) or 0)
                    stats['updated_items'] += 1
                except Exception as e:
                    stats['errors'] += 1
                    logger.warning(
                        f"  ➜ [洗版优先级重算] 媒体项失败: "
                        f"tmdb={row.get('tmdb_id')}, type={row.get('item_type')}, err={e}",
                        exc_info=True,
                    )

                # ★ 核心修复：加快前端刷新频率 (每 10 条刷新状态，每 50 条打印日志并提交)
                if stats['scanned_items'] % 10 == 0:
                    progress = 5 + int((stats['scanned_items'] / max(total_rows, 1)) * 90)
                    status_msg = (
                        f"  ➜ [洗版优先级重算] 进度: {stats['scanned_items']}/{total_rows}，"
                        f"已更新 {stats['updated_items']}，缺 RAW {stats['missing_raw']}，"
                        f"未命中规则 {stats['no_priority_rules']}，回填CID {stats['backfilled_target_cid']}"
                    )
                    update_task_status(min(progress, 95), status_msg)

                if stats['scanned_items'] % 50 == 0:
                    conn.commit()
                    progress = 5 + int((stats['scanned_items'] / max(total_rows, 1)) * 90)
                    update_progress(
                        min(progress, 95),
                        f"  ➜ [洗版优先级重算] 进度: {stats['scanned_items']}/{total_rows}，"
                        f"已更新 {stats['updated_items']}，缺 RAW {stats['missing_raw']}，"
                        f"未命中规则 {stats['no_priority_rules']}，回填CID {stats['backfilled_target_cid']}"
                    )

            conn.commit()

    stats['finished_at'] = datetime.utcnow().isoformat() + 'Z'
    update_progress(
        100,
        f"  ➜ [洗版优先级重算] 完成：总数 {stats['scanned_items']}，"
        f"已更新 {stats['updated_items']}，缺 RAW {stats['missing_raw']}，"
        f"未命中规则 {stats['no_priority_rules']}，回填CID {stats['backfilled_target_cid']}"
    )
    return stats

def submit_washing_priority_recalculate_task(item_type='all', limit=None):
    """提交后台任务：重算媒体库所有资源的洗版优先级快照。"""
    try:
        import task_manager
    except Exception as e:
        raise RuntimeError(f"无法导入 task_manager: {e}")

    item_type = item_type or 'all'
    limit = limit if limit not in ('', None) else None

    def _task(processor=None):
        return task_recalculate_library_washing_priorities(
            processor=processor,
            item_type=item_type,
            limit=limit,
        )

    task_manager.submit_task(
        _task,
        task_name="重算媒体库洗版优先级",
        processor_type='media'
    )
