# tasks/shared_resource_tasks.py
# Rapid v2 共享资源任务：登记本地媒体库索引、长轮询消费中心事件。
import hashlib
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List

import requests
import task_manager
from database import shared_credit_db, shared_share_db, settings_db
from database.connection import get_db_connection
from handler.shared_center_client import SharedCenterClient, shared_center_enabled
from handler.shared_subscription_service import poll_and_consume_once

logger = logging.getLogger(__name__)

_LISTENER_THREAD = None
_LISTENER_STOP = threading.Event()
_LISTENER_LOCK = threading.Lock()
_FULL_SHARE_LOCK = threading.Lock()


def _cfg_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭'):
        return False
    return bool(default)


def _shared_resource_switch_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return _cfg_bool(cfg.get('p115_shared_resource_enabled'), False)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取共享资源总开关失败，按未启用处理: {e}")
        return False


def _enabled() -> bool:
    return _shared_resource_switch_enabled() and shared_center_enabled()


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _file_size_from_cache(file_info: Dict[str, Any]) -> int:
    """登记源前补齐 size；中心端秒传必须依赖 size。"""
    size = _safe_int(file_info.get('size') or file_info.get('file_size') or file_info.get('size_bytes') or file_info.get('fileSize'), 0)
    if size > 0:
        return size
    sha1 = _norm_sha1(file_info.get('sha1'))
    pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
    try:
        from handler.p115_service import P115CacheManager
        row = None
        if sha1 and hasattr(P115CacheManager, 'get_file_cache_by_sha1'):
            row = P115CacheManager.get_file_cache_by_sha1(sha1)
        if not row and pc and hasattr(P115CacheManager, 'get_file_cache_by_pickcode'):
            row = P115CacheManager.get_file_cache_by_pickcode(pc)
        if row:
            row = dict(row)
            size = _safe_int(row.get('size'), 0)
            if size > 0:
                file_info['size'] = size
                if not file_info.get('file_name') and row.get('name'):
                    file_info['file_name'] = row.get('name')
                return size
    except Exception:
        pass
    try:
        clauses, args = [], []
        if sha1:
            clauses.append('UPPER(sha1)=%s')
            args.append(sha1)
        if pc:
            clauses.append('pick_code=%s')
            args.append(pc)
        if clauses:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT size, name FROM p115_filesystem_cache WHERE {' OR '.join(clauses)} ORDER BY CASE WHEN COALESCE(size,0)>0 THEN 0 ELSE 1 END LIMIT 1", args)
                    row = cur.fetchone()
                    if row:
                        row = dict(row)
                        size = _safe_int(row.get('size'), 0)
                        if size > 0:
                            file_info['size'] = size
                            if not file_info.get('file_name') and row.get('name'):
                                file_info['file_name'] = row.get('name')
                            return size
    except Exception:
        pass
    return 0


def _safe_int_or_none(value):
    try:
        if value in (None, ''):
            return None
        return int(float(value))
    except Exception:
        return None


def _norm_sha1(value: str) -> str:
    text = str(value or '').strip().upper()
    return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''


def _norm_preid(value: str) -> str:
    return _norm_sha1(value)


def _extract_p115_down_url(resp: Any) -> str:
    if isinstance(resp, str):
        return resp
    if not isinstance(resp, dict):
        return ''
    data = resp.get('data')
    if isinstance(data, dict):
        for item in data.values():
            if not isinstance(item, dict):
                continue
            url_obj = item.get('url')
            if isinstance(url_obj, dict) and url_obj.get('url'):
                return str(url_obj.get('url'))
            if isinstance(url_obj, str) and url_obj:
                return url_obj
            for key in ('downurl', 'download_url', 'url'):
                if isinstance(item.get(key), str) and item.get(key):
                    return str(item.get(key))
    for key in ('url', 'downurl', 'download_url'):
        if isinstance(resp.get(key), str) and resp.get(key):
            return str(resp.get(key))
    return ''


def _p115_range_bytes_by_pick_code(pick_code: str, start: int, end: int) -> bytes:
    """按用户配置的 115 API 优先级读取文件 Range。

    这里是为了计算 preid（文件前 128KB SHA1）。它只是读取源文件直链，
    不属于 upload/init 秒传调度，所以必须尊重 p115_api_priority：
    - cookie 优先：先 Cookie download_url，再 OpenAPI downurl；
    - openapi 优先：先 OpenAPI downurl，再 Cookie download_url。

    取直链和 Range GET 必须使用同一个 User-Agent，否则 115 可能返回 403。
    """
    pick_code = str(pick_code or '').strip()
    if not pick_code:
        return b''

    try:
        from handler.p115_service import P115Service, get_115_api_priority, get_115_tokens, get_115_ua
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 导入 115 客户端失败，无法计算 preid: {e}")
        return b''

    try:
        p115 = P115Service.get_client()
        if not p115:
            return b''

        priority = get_115_api_priority()
        try:
            _, _, _, app_type = get_115_tokens()
        except Exception:
            app_type = 'web'

        ua_candidates = []
        for ua in (
            get_115_ua(app_type or 'web'),
            get_115_ua('web'),
            get_115_ua('mac'),
        ):
            ua = str(ua or '').strip()
            if ua and ua not in ua_candidates:
                ua_candidates.append(ua)
        if not ua_candidates:
            ua_candidates.append('Mozilla/5.0')

        if priority == 'cookie':
            method_order = [('download_url', 'Cookie'), ('openapi_downurl', 'OpenAPI')]
        else:
            method_order = [('openapi_downurl', 'OpenAPI'), ('download_url', 'Cookie')]

        range_header = f'bytes={int(start)}-{int(end)}'
        last_status = None

        for method_name, label in method_order:
            method = getattr(p115, method_name, None)
            if not callable(method):
                continue

            for ua in ua_candidates:
                down_url = ''
                try:
                    # 关键：获取直链和后续 Range GET 必须使用同一个 UA。
                    down_url = _extract_p115_down_url(method(pick_code, user_agent=ua))
                except TypeError:
                    try:
                        down_url = _extract_p115_down_url(method(pick_code, ua))
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源] 获取 115 直链失败({label}, positional-ua): {e}")
                except Exception as e:
                    logger.debug(f"  ➜ [共享资源] 获取 115 直链失败({label}): {e}")

                if not down_url:
                    continue

                try:
                    headers = {
                        'Range': range_header,
                        'User-Agent': ua,
                        'Accept': '*/*',
                        'Connection': 'close',
                    }
                    r = requests.get(down_url, headers=headers, timeout=45, allow_redirects=True)
                    last_status = r.status_code
                    if r.status_code == 206 and r.content:
                        logger.debug(
                            f"  ➜ [共享资源] preid Range 读取成功: api={label}, "
                            f"range={range_header}, bytes={len(r.content)}, pc={pick_code[:8]}..."
                        )
                        return r.content or b''

                    # 必须拒绝 200，避免服务端忽略 Range 后拉完整大文件。
                    logger.warning(
                        f"  ➜ [共享资源] 读取 preid Range 失败: api={label}, "
                        f"HTTP={r.status_code}, range={range_header}, pc={pick_code[:8]}..."
                    )
                except Exception as e:
                    logger.debug(
                        f"  ➜ [共享资源] Range GET 异常: api={label}, "
                        f"range={range_header}, pc={pick_code[:8]}..., err={e}"
                    )

        if last_status:
            logger.warning(
                f"  ➜ [共享资源] 已按 {priority} 优先级尝试读取 preid Range，仍失败，"
                f"最后 HTTP={last_status}: pc={pick_code[:8]}..."
            )
        return b''

    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取 115 文件 Range 计算 preid 失败: pc={pick_code[:8]}..., err={e}")
        return b''


def _save_preid_to_p115_cache(file_info: Dict[str, Any], preid: str) -> None:
    preid = _norm_preid(preid)
    if not preid:
        return
    sha1 = _norm_sha1(file_info.get('sha1'))
    fid = str(file_info.get('fid') or file_info.get('file_id') or '').strip()
    pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
    clauses, args = [], []
    if fid:
        clauses.append('id=%s')
        args.append(fid)
    if pc:
        clauses.append('pick_code=%s')
        args.append(pc)
    if sha1:
        clauses.append('UPPER(sha1)=%s')
        args.append(sha1)
    if not clauses:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE p115_filesystem_cache
                    SET preid=%s, updated_at=NOW()
                    WHERE {' OR '.join(clauses)}
                    """,
                    [preid, *args],
                )
            conn.commit()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 回写 p115_filesystem_cache.preid 失败: {e}")


def _lookup_preid_from_p115_cache(file_info: Dict[str, Any]) -> str:
    meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    preid = _norm_preid(file_info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
    if preid:
        return preid
    sha1 = _norm_sha1(file_info.get('sha1'))
    fid = str(file_info.get('fid') or file_info.get('file_id') or '').strip()
    pc = str(file_info.get('pick_code') or file_info.get('pc') or meta.get('pick_code') or meta.get('pc') or '').strip()
    clauses, args = [], []
    if fid:
        clauses.append('id=%s')
        args.append(fid)
    if pc:
        clauses.append('pick_code=%s')
        args.append(pc)
    if sha1:
        clauses.append('UPPER(sha1)=%s')
        args.append(sha1)
    if not clauses:
        return ''
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT preid
                    FROM p115_filesystem_cache
                    WHERE {' OR '.join(clauses)}
                      AND preid IS NOT NULL AND preid <> ''
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    args,
                )
                row = cur.fetchone()
                return _norm_preid((row or {}).get('preid')) if row else ''
    except Exception:
        return ''


def _ensure_file_preid(file_info: Dict[str, Any]) -> str:
    """确保单个 115 文件拥有 preid。

    preid = 文件前 128KB SHA1，是 115 upload/init 的基础秒传参数。
    只读取 128KB，不读取完整文件；计算结果写回 p115_filesystem_cache，后续登记中心一同带上。
    """
    if not isinstance(file_info, dict):
        return ''
    preid = _lookup_preid_from_p115_cache(file_info)
    if not preid:
        pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
        chunk = _p115_range_bytes_by_pick_code(pc, 0, 131071)
        if chunk:
            preid = hashlib.sha1(chunk).hexdigest().upper()
            _save_preid_to_p115_cache(file_info, preid)
            logger.info(f"  ➜ [共享资源] 已计算并缓存 preid: {file_info.get('file_name') or file_info.get('name') or file_info.get('sha1')} -> {preid[:12]}...")
    if preid:
        file_info['preid'] = preid
        meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
        meta = dict(meta or {})
        meta.setdefault('preid', preid)
        file_info['rapid_meta_json'] = meta
    return preid


def _json_obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _raw_for_file(file_info: Dict[str, Any]) -> Dict[str, Any]:
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return {}
    row = shared_share_db.raw_ffprobe_for_sha1(sha1) or {}
    raw = row.get('raw_ffprobe_json') if isinstance(row.get('raw_ffprobe_json'), dict) else {}
    if not raw:
        return {}
    # 补齐中心需要的 _etk。中心会清理 cookie/pc/url，不泄露 CK。
    raw = dict(raw)
    etk = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
    etk = dict(etk or {})
    etk.setdefault('sha1', sha1)
    if file_info.get('tmdb_id'):
        etk.setdefault('tmdb_id', str(file_info.get('tmdb_id')))
    if file_info.get('item_type') in ('Movie', 'Episode', 'Season'):
        etk.setdefault('type', 'movie' if file_info.get('item_type') == 'Movie' else 'tv')
    if file_info.get('season_number') not in (None, ''):
        etk.setdefault('season_number', _safe_int(file_info.get('season_number')))
    if file_info.get('episode_number') not in (None, ''):
        etk.setdefault('episode_number', _safe_int(file_info.get('episode_number')))
    raw['_etk'] = etk
    return raw


def _raw_video_stream(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = raw or {}
    if isinstance(raw.get('MediaSourceInfo'), dict):
        streams = raw.get('MediaSourceInfo', {}).get('MediaStreams') or []
        return next((s for s in streams if str(s.get('Type') or '').lower() == 'video'), {}) or {}
    if isinstance(raw.get('MediaStreams'), list):
        return next((s for s in raw.get('MediaStreams') if str(s.get('Type') or '').lower() == 'video'), {}) or {}
    if isinstance(raw.get('streams'), list):
        return next((s for s in raw.get('streams') if str(s.get('codec_type') or s.get('type') or '').lower() == 'video'), {}) or {}
    return {}


def _video_resolution(video: Dict[str, Any]) -> str:
    try:
        width = int(video.get('Width') or video.get('width') or 0)
        height = int(video.get('Height') or video.get('height') or 0)
    except Exception:
        width = height = 0
    if width >= 7600: return '8K'
    if width >= 3800: return '4K'
    if width >= 1900: return '1080p'
    if width >= 1200: return '720p'
    return f'{height}p' if height else ''


def _effect_key(raw: Dict[str, Any]) -> str:
    text = json.dumps(_raw_video_stream(raw), ensure_ascii=False, default=str).upper()
    if 'DOLBY' in text or 'DOVI' in text or 'DVHE' in text:
        return 'DV'
    if 'HDR10+' in text or 'SMPTE2094' in text:
        return 'HDR10+'
    if 'HLG' in text or 'ARIB-STD-B67' in text:
        return 'HLG'
    if 'HDR10' in text or 'SMPTE2084' in text or 'BT2020' in text:
        return 'HDR10'
    if 'HDR' in text:
        return 'HDR'
    return 'SDR'


def _stream_type(stream: Dict[str, Any]) -> str:
    return str(stream.get('Type') or stream.get('codec_type') or stream.get('type') or '').strip().lower()


def _raw_streams(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = raw or {}
    if isinstance(raw.get('MediaSourceInfo'), dict):
        streams = raw.get('MediaSourceInfo', {}).get('MediaStreams') or []
    elif isinstance(raw.get('MediaStreams'), list):
        streams = raw.get('MediaStreams') or []
    elif isinstance(raw.get('streams'), list):
        streams = raw.get('streams') or []
    else:
        streams = []
    return [s for s in streams if isinstance(s, dict)]


def _codec_display(value: Any) -> str:
    text = str(value or '').strip()
    low = text.lower()
    return {
        'hevc': 'HEVC', 'h265': 'HEVC', 'x265': 'HEVC',
        'h264': 'H.264', 'avc': 'H.264', 'x264': 'H.264',
        'av1': 'AV1', 'vp9': 'VP9',
    }.get(low, text.upper() if text else '')


def _fraction_to_float(value: Any):
    text = str(value or '').strip()
    if not text:
        return None
    try:
        if '/' in text:
            a, b = text.split('/', 1)
            b = float(b)
            return float(a) / b if b else None
        return float(text)
    except Exception:
        return None


def _fps_display(video: Dict[str, Any]) -> str:
    value = (
        video.get('AverageFrameRate') or video.get('RealFrameRate') or video.get('FrameRate') or
        video.get('avg_frame_rate') or video.get('r_frame_rate')
    )
    fps = _fraction_to_float(value)
    if fps is None or fps <= 0:
        return ''
    if abs(fps - round(fps)) < 0.03:
        return f'{int(round(fps))} fps'
    return f'{fps:.2f} fps'


def _track_display(stream: Dict[str, Any]) -> Dict[str, Any]:
    lang = str(stream.get('Language') or stream.get('language') or stream.get('lang') or '').strip()
    codec = _codec_display(stream.get('Codec') or stream.get('codec_name') or stream.get('codec'))
    channels = stream.get('Channels') or stream.get('channels') or stream.get('channel_layout')
    title = str(stream.get('DisplayTitle') or stream.get('Title') or stream.get('title') or '').strip()
    parts = []
    if lang:
        parts.append(lang)
    if codec:
        parts.append(codec)
    if channels:
        ch_text = str(channels)
        if ch_text.isdigit():
            ch_text = f'{ch_text}ch'
        parts.append(ch_text)
    if title and title not in parts:
        parts.append(title)
    return {
        'language': lang,
        'codec': codec,
        'channels': channels,
        'title': title,
        'display': ' · '.join([str(x) for x in parts if str(x).strip()]),
    }


def _media_signature(raw: Dict[str, Any]) -> Dict[str, Any]:
    streams = _raw_streams(raw)
    video = next((s for s in streams if _stream_type(s) == 'video'), {}) or {}
    audio_list = [_track_display(s) for s in streams if _stream_type(s) == 'audio']
    subtitle_list = [_track_display(s) for s in streams if _stream_type(s) in ('subtitle', 'subtitles')]
    codec = _codec_display(video.get('Codec') or video.get('codec_name') or video.get('codec'))
    bit_depth = video.get('BitDepth') or video.get('bits_per_raw_sample') or video.get('bits_per_sample')
    fps = _fps_display(video)
    resolution = _video_resolution(video)
    effect = _effect_key(raw)
    return {
        'resolution': resolution,
        'resolution_display': resolution,
        'effect': effect,
        'effect_key': effect,
        'codec': codec,
        'video_codec': codec,
        'codec_display': codec,
        'bit_depth': bit_depth,
        'fps': fps,
        'frame_rate': fps,
        'audio_list': [x for x in audio_list if x.get('display') or x.get('codec') or x.get('language')],
        'subtitle_list': [x for x in subtitle_list if x.get('display') or x.get('codec') or x.get('language')],
    }


def _prepare_raw_upload_entry(file_info: Dict[str, Any]) -> Dict[str, Any]:
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return {}
    raw = _raw_for_file(file_info)
    if not raw:
        return {}
    return {
        'sha1': sha1,
        'size': _file_size_from_cache(file_info) or None,
        'raw_ffprobe_json': raw,
    }


def _upload_raw_batch(client: SharedCenterClient, files: List[Dict[str, Any]]) -> Dict[str, Any]:
    entries = []
    for f in files or []:
        entry = _prepare_raw_upload_entry(f)
        if entry:
            entries.append(entry)
    if not entries:
        return {'ok': True, 'uploaded': {}, 'count': 0, 'errors': []}

    uploaded = {}
    errors = []
    try:
        if hasattr(client, 'upload_raw_ffprobe_batch'):
            resp = client.upload_raw_ffprobe_batch(entries)
            ok_items = resp.get('items') or resp.get('uploaded') or []
            for item in ok_items:
                sha = _norm_sha1((item or {}).get('sha1'))
                if sha:
                    uploaded[sha] = True
            # 中心旧版本可能只返回 count；这种情况下视本批次都成功，避免回退逐个再刷屏。
            if not uploaded and int(resp.get('count') or 0) == len(entries) and not resp.get('errors'):
                uploaded = {_norm_sha1(x.get('sha1')): True for x in entries if _norm_sha1(x.get('sha1'))}
            errors = resp.get('errors') or []
            if uploaded:
                return {'ok': not errors, 'uploaded': uploaded, 'count': len(uploaded), 'errors': errors}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] RAW 批量上传失败，回退逐个上传: {e}")

    # 兼容未升级中心：单个逐个传，但只作为 fallback。
    for entry in entries:
        sha = _norm_sha1(entry.get('sha1'))
        try:
            client.upload_raw_ffprobe(sha, entry.get('raw_ffprobe_json') or {}, size=entry.get('size'))
            uploaded[sha] = True
        except Exception as e:
            errors.append({'sha1': sha, 'error': str(e)})
    return {'ok': not errors, 'uploaded': uploaded, 'count': len(uploaded), 'errors': errors}


def _upload_raw_if_needed(client: SharedCenterClient, file_info: Dict[str, Any]) -> bool:
    entry = _prepare_raw_upload_entry(file_info)
    if not entry:
        return False
    client.upload_raw_ffprobe(entry['sha1'], entry['raw_ffprobe_json'], size=entry.get('size'))
    return True


def _file_payload_common(file_info: Dict[str, Any], raw_uploaded: bool = False) -> Dict[str, Any]:
    raw = _raw_for_file(file_info) if raw_uploaded else {}
    sig = _media_signature(raw) if raw else {}
    preid = _ensure_file_preid(file_info)
    return {
        'sha1': _norm_sha1(file_info.get('sha1')),
        'preid': preid or None,
        'size': _file_size_from_cache(file_info) or None,
        'file_name': file_info.get('file_name') or file_info.get('name') or '',
        'quality': sig.get('resolution') or '',
        'has_raw_ffprobe': bool(raw_uploaded),
        'media_signature_json': sig,
        'rapid_meta_json': {
            'fid': file_info.get('fid') or file_info.get('file_id') or '',
            'pick_code': file_info.get('pick_code') or file_info.get('pc') or '',
            'relative_path': file_info.get('relative_path') or '',
            'preid': preid or '',
        },
    }


def _completed_status_from_files(files: List[Dict[str, Any]], expected_count: int = 0) -> Dict[str, Any]:
    eps = sorted({_safe_int(f.get('episode_number'), 0) for f in files if _safe_int(f.get('episode_number'), 0) > 0})
    if expected_count and len(eps) < expected_count:
        return {'status': 'incomplete', 'message': f'未集齐：{len(eps)}/{expected_count}'}
    signatures = []
    for f in files:
        sig = f.get('media_signature_json') if isinstance(f.get('media_signature_json'), dict) else {}
        if sig.get('resolution') or sig.get('effect_key'):
            signatures.append((sig.get('resolution') or '', sig.get('effect_key') or '', sig.get('codec') or ''))
    if signatures and len(set(signatures)) > 1:
        return {'status': 'inconsistent', 'message': '完结季文件存在分辨率/HDR/编码不一致，暂不作为收藏季源派发'}
    if not files:
        return {'status': 'incomplete', 'message': '没有可登记的视频文件'}
    return {'status': 'available', 'message': '完结季一致性校验通过'}


def register_candidate_to_center(candidate: Dict[str, Any], *, source_provider: str = 'manual_rapid') -> Dict[str, Any]:
    """把本地媒体库中的电影/分集/季登记到 Rapid v2 中心。

    规则：
    - Movie：登记 movie_sources；
    - Episode：登记 season_episode_sources，追更池不做版本一致性要求；
    - Season：先把每集登记到追更池，再尝试登记 completed_season_source。完结收藏季才做一致性校验。
    """
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用或中心未配置'}
    candidate = dict(candidate or {})
    item_type = str(candidate.get('item_type') or '').strip()
    files = shared_share_db.collect_files_for_candidate(candidate)
    if not files and item_type in ('Season', 'Episode'):
        repair_result = shared_share_db.repair_candidate_fingerprints(candidate, log_result=True)
        files = shared_share_db.collect_files_for_candidate(candidate)
    else:
        repair_result = {}
    if not files:
        return {
            'ok': False,
            'message': '未找到可共享的视频文件，请先确认 p115_filesystem_cache / media_metadata 已补齐 SHA1、PC 和大小',
            'fingerprint_repair': repair_result or {},
        }

    for _f in files:
        _ensure_file_preid(_f)
    root = shared_share_db.candidate_root_from_files(files)
    client = SharedCenterClient()
    raw_batch_result = _upload_raw_batch(client, files)
    uploaded_sha1s = raw_batch_result.get('uploaded') or {}
    results = []
    uploaded = int(raw_batch_result.get('count') or 0)
    errors = list(raw_batch_result.get('errors') or [])
    tmdb_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    if item_type == 'Movie':
        tmdb_id = str(candidate.get('tmdb_id') or '').strip()

    for f in files:
        f.setdefault('tmdb_id', tmdb_id)
        if item_type == 'Movie':
            f.setdefault('item_type', 'Movie')
        else:
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', candidate.get('season_number'))
        try:
            sha_for_raw = _norm_sha1(f.get('sha1'))
            raw_ok = bool(uploaded_sha1s.get(sha_for_raw))
            common = _file_payload_common(f, raw_uploaded=raw_ok)
            if item_type == 'Movie':
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Movie',
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'source_provider': source_provider,
                    **common,
                }
                resp = client.register_movie_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'movie', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id, 'item_type': 'Movie',
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
            else:
                ep_no = _safe_int_or_none(f.get('episode_number')) or _safe_int_or_none(candidate.get('episode_number'))
                season_no = _safe_int_or_none(f.get('season_number')) or _safe_int_or_none(candidate.get('season_number'))
                if season_no is None or ep_no is None:
                    errors.append({'file': f.get('file_name'), 'error': '缺少 season_number/episode_number'})
                    continue
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Episode',
                    'season_number': season_no,
                    'episode_number': ep_no,
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'source_provider': source_provider,
                    **common,
                }
                resp = client.register_episode_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'episode', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                    'item_type': 'Episode', 'season_number': season_no, 'episode_number': ep_no,
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记分集/电影失败: {f.get('file_name') or f.get('sha1')} -> {e}")
            errors.append({'file': f.get('file_name') or f.get('sha1'), 'error': str(e)})

    completed_resp = None
    if item_type == 'Season':
        completed_files = []
        for f in files:
            sha1 = _norm_sha1(f.get('sha1'))
            if not sha1:
                continue
            raw = _raw_for_file(f)
            sig = _media_signature(raw) if raw else {}
            preid = _ensure_file_preid(f)
            completed_files.append({
                'episode_number': _safe_int(f.get('episode_number'), 0), 'sha1': sha1, 'preid': preid or None, 'size': _file_size_from_cache(f) or None,
                'file_name': f.get('file_name') or '', 'quality': sig.get('resolution') or '', 'media_signature_json': sig,
                'rapid_meta_json': {'fid': f.get('fid'), 'pick_code': f.get('pick_code'), 'relative_path': f.get('relative_path'), 'preid': preid or ''},
            })
        expected = _safe_int(candidate.get('expected_episode_count') or candidate.get('total_episodes'), 0)
        consistency = shared_share_db.repair_candidate_fingerprints(candidate, log_result=True)
        status = _completed_status_from_files(completed_files, expected)
        common_signature = {}
        for _cf in completed_files:
            sig = _cf.get('media_signature_json') if isinstance(_cf.get('media_signature_json'), dict) else {}
            if sig:
                common_signature = sig
                break
        if isinstance(consistency, dict) and consistency:
            reason = consistency.get('reason')
            if consistency.get('ok'):
                status = {'status': 'available', 'message': consistency.get('message') or '完结季一致性校验通过'}
            elif reason == 'episode_count_insufficient':
                status = {'status': 'incomplete', 'message': consistency.get('message') or '完结季本地集数不足'}
            elif reason in ('season_asset_inconsistent', 'asset_details_missing'):
                status = {'status': 'inconsistent', 'message': consistency.get('message') or '完结季一致性校验失败'}
            elif reason not in ('not_season', None):
                status = {'status': 'inconsistent', 'message': consistency.get('message') or '完结季一致性校验异常'}
        try:
            payload = {
                'tmdb_id': tmdb_id,
                'item_type': 'Season',
                'season_number': _safe_int(candidate.get('season_number'), 0),
                'title': candidate.get('title'),
                'release_year': candidate.get('release_year'),
                'expected_episode_count': expected or None,
                'status': status['status'],
                'status_message': status['message'],
                'manifest_hash': shared_share_db.manifest_hash(completed_files),
                'source_provider': 'rapid_completed_season',
                'is_clean_version': bool(candidate.get('is_clean_version', False)),
                'clean_version_confidence': candidate.get('clean_version_confidence'),
                'clean_version_meta_json': candidate.get('clean_version_meta_json') or {},
                'media_signature_json': common_signature,
                'files': completed_files,
            }
            completed_resp = client.register_completed_season_source(payload)
            center_item = completed_resp.get('item') or {}
            local = shared_share_db.upsert_local_source({
                'source_kind': 'completed_season', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                'item_type': 'Season', 'season_number': candidate.get('season_number'), 'title': candidate.get('title'),
                'release_year': candidate.get('release_year'), 'source_provider': 'rapid_completed_season',
                'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                'status': status['status'], 'center_status': 'reported', 'manifest_hash': payload['manifest_hash'],
                'file_count': len(completed_files), 'total_size': sum(_safe_int(x.get('size'), 0) for x in completed_files),
                'is_clean_version': payload['is_clean_version'], 'clean_version_confidence': payload['clean_version_confidence'],
                'clean_version_meta_json': payload['clean_version_meta_json'], 'raw_json': {'candidate': candidate, 'center_response': completed_resp, 'status': status, 'consistency': consistency, 'root': root},
            })
            shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': bool(_raw_for_file(f))} for f in files])
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记完结季源失败: {candidate.get('title') or tmdb_id} -> {e}")
            errors.append({'completed_season': candidate.get('title') or tmdb_id, 'error': str(e)})

    return {
        'ok': bool(results or completed_resp),
        'registered_count': len(results),
        'raw_uploaded_count': uploaded,
        'completed_season': completed_resp,
        'errors': errors,
        'root': root,
        'fingerprint_repair': repair_result or {},
        'message': f"已登记 {len(results)} 个分集/电影源" + ("，已更新完结季源" if completed_resp else ''),
    }



def _candidate_from_emby_item_id(emby_item_id: str) -> Dict[str, Any]:
    emby_item_id = str(emby_item_id or '').strip()
    if not emby_item_id:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE emby_item_ids_json::text ILIKE %s
                    ORDER BY date_added DESC NULLS LAST
                    LIMIT 1
                    """,
                    (f'%{emby_item_id}%',),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 按 Emby ID 查询媒体行失败: {emby_item_id} -> {e}")
        return {}

def trigger_shared_rapid_register_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'created': 0, 'message': '共享资源未启用'}
    db_row = _candidate_from_emby_item_id(kwargs.get('emby_item_id'))
    item_type = kwargs.get('item_type') or kwargs.get('share_item_type') or db_row.get('item_type')
    candidate = {
        'tmdb_id': kwargs.get('tmdb_id') or db_row.get('tmdb_id') or kwargs.get('parent_series_tmdb_id'),
        'parent_series_tmdb_id': kwargs.get('parent_series_tmdb_id') or db_row.get('parent_series_tmdb_id'),
        'item_type': item_type,
        'season_number': kwargs.get('season_number') if kwargs.get('season_number') not in (None, '') else db_row.get('season_number'),
        'episode_number': kwargs.get('episode_number') if kwargs.get('episode_number') not in (None, '') else db_row.get('episode_number'),
        'title': kwargs.get('title') or kwargs.get('name') or db_row.get('title'),
        'release_year': kwargs.get('year') or kwargs.get('release_year') or db_row.get('release_year'),
    }
    if candidate.get('item_type') == 'Episode' and db_row.get('tmdb_id'):
        candidate['tmdb_id'] = db_row.get('tmdb_id')
        candidate['parent_series_tmdb_id'] = candidate.get('parent_series_tmdb_id') or kwargs.get('parent_series_tmdb_id')
    result = register_candidate_to_center(candidate, source_provider='rapid_auto_library')
    result['created'] = result.get('registered_count', 0)
    return result


def trigger_shared_auto_share_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：Rapid v2 不创建 115 分享，只登记本地秒传源。"""
    return trigger_shared_rapid_register_for_library_item(processor, **kwargs)


def trigger_completed_season_pack_share_task(processor=None, *, parent_series_tmdb_id: str = '', season_number=None, title: str = '', year: str = '', **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：完结季不再创建分享包，只更新 completed_season_source manifest。"""
    parent = str(parent_series_tmdb_id or kwargs.get('tmdb_id') or '').strip()
    if not parent:
        return {'ok': False, 'created': 0, 'message': '缺少父剧 TMDb ID'}
    try:
        season_no = int(float(season_number))
    except Exception:
        return {'ok': False, 'created': 0, 'message': f'无效季号: {season_number}'}
    candidate = {
        'tmdb_id': parent,
        'parent_series_tmdb_id': parent,
        'item_type': 'Season',
        'season_number': season_no,
        'title': title or kwargs.get('series_name') or kwargs.get('name') or parent,
        'release_year': year or kwargs.get('release_year'),
        'expected_episode_count': kwargs.get('expected_episode_count') or kwargs.get('total_episodes'),
        'is_clean_version': kwargs.get('is_clean_version', False),
        'clean_version_confidence': kwargs.get('clean_version_confidence'),
        'clean_version_meta_json': kwargs.get('clean_version_meta_json') or {},
    }
    result = register_candidate_to_center(candidate, source_provider='rapid_completed_season')
    result['created'] = result.get('registered_count', 0) + (1 if result.get('completed_season') else 0)
    result.setdefault('episode_cancelled', 0)
    return result


def share_all_library(max_items: int = 100000) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    if not _FULL_SHARE_LOCK.acquire(blocking=False):
        return {'ok': False, 'message': '全库登记任务正在运行'}
    try:
        candidates = shared_share_db.all_library_share_candidates(limit=max_items)
        total = len(candidates)
        ok = failed = 0
        for idx, cand in enumerate(candidates, 1):
            try:
                res = register_candidate_to_center(cand, source_provider='rapid_all_library')
                if res.get('ok'):
                    ok += 1
                else:
                    failed += 1
                if idx % 20 == 0:
                    logger.info(f"  ➜ [共享资源] 一键登记媒体库进度：{idx}/{total}，成功 {ok}，失败 {failed}")
            except Exception as e:
                failed += 1
                logger.warning(f"  ➜ [共享资源] 一键登记媒体库失败: {cand.get('title') or cand.get('tmdb_id')} -> {e}")
        logger.info(f"  ➜ [共享资源] 一键登记媒体库完成：候选 {total}，成功 {ok}，失败 {failed}")
        return {'ok': True, 'total': total, 'success': ok, 'failed': failed}
    finally:
        _FULL_SHARE_LOCK.release()


def _event_listener_loop():
    logger.info('  ➜ [共享事件监听] Rapid v2 长轮询监听已启动。')
    while not _LISTENER_STOP.is_set():
        try:
            if not _enabled():
                time.sleep(15)
                continue
            poll_and_consume_once(timeout=25, limit=10)
        except Exception as e:
            logger.warning(f"  ➜ [共享事件监听] 本轮处理失败: {e}")
            time.sleep(10)
    logger.info('  ➜ [共享事件监听] Rapid v2 长轮询监听已停止。')


def ensure_shared_device_event_listener() -> bool:
    global _LISTENER_THREAD
    if not _enabled():
        return False
    with _LISTENER_LOCK:
        if _LISTENER_THREAD and _LISTENER_THREAD.is_alive():
            return True
        _LISTENER_STOP.clear()
        _LISTENER_THREAD = threading.Thread(target=_event_listener_loop, name='shared-rapid-event-listener', daemon=True)
        _LISTENER_THREAD.start()
        return True


def ensure_share_request_event_listener() -> bool:
    return ensure_shared_device_event_listener()


def _sync_center_credit() -> Dict[str, Any]:
    client = SharedCenterClient()
    me = client.me()
    stats = client.stats()
    ledger = client.credit_ledger(limit=500)
    snapshot = {
        'device_id': me.get('id'),
        'credit': int(me.get('credit') or 0),
        'wanted_gaps': int(stats.get('active_gap_devices') or 0),
        'shared_sources': int(stats.get('movie_sources') or 0) + int(stats.get('episode_sources') or 0) + int(stats.get('completed_season_sources') or 0),
        'raw_ffprobe': int(stats.get('raw_ffprobe') or 0),
        'remote_devices': int(stats.get('devices') or 0),
        'raw_json': {'me': me, 'stats': stats},
    }
    saved = shared_credit_db.upsert_credit_snapshot(snapshot)
    synced = shared_credit_db.sync_center_credit_ledger(ledger.get('items') or [], device_snapshot=me)
    return {'snapshot': saved, 'synced_ledger': synced}


def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    if not _enabled():
        if not maintenance_silent:
            logger.info('  ➜ [共享资源维护] 共享资源未启用，跳过。')
        return {'ok': False, 'message': '共享资源未启用'}
    result = {'ok': True}
    try:
        result['device_event_listener'] = ensure_shared_device_event_listener()
    except Exception as e:
        result['listener_error'] = str(e)
    try:
        result['credit'] = _sync_center_credit()
    except Exception as e:
        result['credit_error'] = str(e)
    if not maintenance_silent:
        logger.info(f"  ➜ [共享资源维护] Rapid v2 维护完成: {result}")
    return result


def trigger_shared_resource_maintenance_task() -> bool:
    try:
        return bool(task_manager.submit_task(task_shared_resource_maintenance, task_name='共享资源维护', processor_type='media'))
    except Exception:
        threading.Thread(target=task_shared_resource_maintenance, name='shared-rapid-maintenance', daemon=True).start()
        return True


def trigger_share_all_library_task() -> bool:
    try:
        return bool(task_manager.submit_task(lambda processor=None: share_all_library(), task_name='一键登记媒体库', processor_type='media'))
    except Exception:
        threading.Thread(target=share_all_library, name='shared-rapid-share-all', daemon=True).start()
        return True


def task_shared_share_status_sync_high_freq(processor=None, maintenance_silent: bool = True):
    """兼容旧任务名：Rapid v2 没有 115 分享状态同步，转为共享资源维护。"""
    return task_shared_resource_maintenance(processor=processor, maintenance_silent=maintenance_silent)
