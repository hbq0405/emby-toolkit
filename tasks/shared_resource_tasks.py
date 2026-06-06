# tasks/shared_resource_tasks.py
# Rapid v2 共享资源任务：登记本地媒体库索引、长轮询消费中心事件。
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List

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


def _media_signature(raw: Dict[str, Any]) -> Dict[str, Any]:
    video = _raw_video_stream(raw)
    return {
        'resolution': _video_resolution(video),
        'effect_key': _effect_key(raw),
        'codec': str(video.get('Codec') or video.get('codec_name') or video.get('codec') or '').lower(),
    }


def _upload_raw_if_needed(client: SharedCenterClient, file_info: Dict[str, Any]) -> bool:
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return False
    raw = _raw_for_file(file_info)
    if not raw:
        return False
    client.upload_raw_ffprobe(sha1, raw, size=_safe_int(file_info.get('size'), 0) or None)
    return True


def _file_payload_common(file_info: Dict[str, Any], raw_uploaded: bool = False) -> Dict[str, Any]:
    raw = _raw_for_file(file_info) if raw_uploaded else {}
    sig = _media_signature(raw) if raw else {}
    return {
        'sha1': _norm_sha1(file_info.get('sha1')),
        'size': _safe_int(file_info.get('size'), 0) or None,
        'file_name': file_info.get('file_name') or file_info.get('name') or '',
        'quality': sig.get('resolution') or '',
        'has_raw_ffprobe': bool(raw_uploaded),
        'media_signature_json': sig,
        'rapid_meta_json': {
            'fid': file_info.get('fid') or file_info.get('file_id') or '',
            'pick_code': file_info.get('pick_code') or file_info.get('pc') or '',
            'relative_path': file_info.get('relative_path') or '',
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
    if not files:
        return {'ok': False, 'message': '未找到可共享的视频文件，请先确认 p115_filesystem_cache / media_metadata 已补齐 SHA1、PC 和大小'}

    client = SharedCenterClient()
    results = []
    uploaded = 0
    errors = []
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
            raw_ok = _upload_raw_if_needed(client, f)
            if raw_ok:
                uploaded += 1
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
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
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
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
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
            completed_files.append({
                'episode_number': _safe_int(f.get('episode_number'), 0), 'sha1': sha1, 'size': _safe_int(f.get('size'), 0) or None,
                'file_name': f.get('file_name') or '', 'quality': sig.get('resolution') or '', 'media_signature_json': sig,
                'rapid_meta_json': {'fid': f.get('fid'), 'pick_code': f.get('pick_code'), 'relative_path': f.get('relative_path')},
            })
        expected = _safe_int(candidate.get('expected_episode_count') or candidate.get('total_episodes'), 0)
        status = _completed_status_from_files(completed_files, expected)
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
                'files': completed_files,
            }
            completed_resp = client.register_completed_season_source(payload)
            center_item = completed_resp.get('item') or {}
            local = shared_share_db.upsert_local_source({
                'source_kind': 'completed_season', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                'item_type': 'Season', 'season_number': candidate.get('season_number'), 'title': candidate.get('title'),
                'release_year': candidate.get('release_year'), 'source_provider': 'rapid_completed_season',
                'status': status['status'], 'center_status': 'reported', 'manifest_hash': payload['manifest_hash'],
                'file_count': len(completed_files), 'total_size': sum(_safe_int(x.get('size'), 0) for x in completed_files),
                'is_clean_version': payload['is_clean_version'], 'clean_version_confidence': payload['clean_version_confidence'],
                'clean_version_meta_json': payload['clean_version_meta_json'], 'raw_json': {'candidate': candidate, 'center_response': completed_resp, 'status': status},
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

def trigger_shared_auto_share_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
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
    result = register_candidate_to_center(candidate, source_provider='auto_library')
    result['created'] = result.get('registered_count', 0)
    return result


def share_all_library(max_items: int = 100000) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    if not _FULL_SHARE_LOCK.acquire(blocking=False):
        return {'ok': False, 'message': '全库共享任务正在运行'}
    try:
        candidates = shared_share_db.all_library_share_candidates(limit=max_items)
        total = len(candidates)
        ok = failed = 0
        for idx, cand in enumerate(candidates, 1):
            try:
                res = register_candidate_to_center(cand, source_provider='share_all_library')
                if res.get('ok'):
                    ok += 1
                else:
                    failed += 1
                if idx % 20 == 0:
                    logger.info(f"  ➜ [共享资源] 一键共享媒体库进度：{idx}/{total}，成功 {ok}，失败 {failed}")
            except Exception as e:
                failed += 1
                logger.warning(f"  ➜ [共享资源] 一键共享媒体库失败: {cand.get('title') or cand.get('tmdb_id')} -> {e}")
        logger.info(f"  ➜ [共享资源] 一键共享媒体库完成：候选 {total}，成功 {ok}，失败 {failed}")
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
        return bool(task_manager.submit_task(lambda processor=None: share_all_library(), task_name='一键共享媒体库', processor_type='media'))
    except Exception:
        threading.Thread(target=share_all_library, name='shared-rapid-share-all', daemon=True).start()
        return True
