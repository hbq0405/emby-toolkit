# handler/shared_subscription_service.py
# 统一订阅共享资源消费入口：登记缺口、优先从中心共享池转存或虚拟入库。
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

import config_manager
import constants
from database.connection import get_db_connection
from handler.p115_service import P115Service, P115CacheManager
from handler.p115_media_analyzer import P115MediaAnalyzerMixin
from handler.shared_center_client import SharedCenterClient, shared_center_enabled, shared_resource_mode

logger = logging.getLogger(__name__)

VIDEO_EXTS = {'.mkv', '.mp4', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.flv', '.rmvb', '.webm', '.iso'}


class _MediainfoBuilder(P115MediaAnalyzerMixin):
    pass


def _cfg(name: str, fallback: str, default=None):
    key = getattr(constants, name, fallback)
    return (config_manager.APP_CONFIG or {}).get(key, default)


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _norm_sha1(value: str) -> str:
    return str(value or '').strip().upper()


def _sanitize_filename(name: str) -> str:
    name = str(name or '').strip()
    name = re.sub(r'[\\/:*?"<>|]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or 'Unknown'


def _media_year(value) -> str:
    m = re.search(r'((?:19|20)\d{2})', str(value or ''))
    return m.group(1) if m else ''


def _build_gap_item(*, tmdb_id, item_type, title='', season_number=None, episode_number=None, year='') -> Dict[str, Any]:
    item_type = str(item_type or '').strip()
    return {
        'tmdb_id': str(tmdb_id or ''),
        'item_type': item_type,
        'season_number': int(season_number) if season_number not in (None, '') else None,
        'episode_number': int(episode_number) if episode_number not in (None, '') else None,
        'title': title or None,
        'release_year': int(year) if str(year or '').isdigit() else None,
    }


def _build_center_queries(item: Dict[str, Any], title: str, tmdb_id, item_type: str, parent_tmdb_id=None, season_number=None, year='') -> List[Dict[str, Any]]:
    """把本地待订阅项转换成中心查询。Season/Series 查询依赖中心端支持按季/剧返回 Episode 源。"""
    item_type = str(item_type or '').strip()
    queries = []
    if item_type == 'Movie':
        queries.append(_build_gap_item(tmdb_id=tmdb_id, item_type='Movie', title=title, year=year))
    elif item_type == 'Season':
        sid = parent_tmdb_id or item.get('parent_series_tmdb_id') or tmdb_id
        queries.append(_build_gap_item(tmdb_id=sid, item_type='Season', title=title, season_number=season_number, year=year))
    elif item_type == 'Series':
        queries.append(_build_gap_item(tmdb_id=tmdb_id, item_type='Series', title=title, year=year))
    return [q for q in queries if q.get('tmdb_id')]


def report_shared_gap(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='') -> bool:
    if not shared_center_enabled():
        return False
    client = SharedCenterClient()
    if not client.ready:
        logger.warning('  ➜ [共享资源] 已启用但中心地址/token 未配置，跳过缺口登记。')
        return False
    gaps = _build_center_queries(item, title or item.get('title'), tmdb_id or item.get('tmdb_id'), item_type or item.get('item_type'), parent_tmdb_id, season_number, year)
    try:
        client.report_gaps(gaps)
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 登记缺口失败: {e}")
        return False


def _flatten_search_results(search_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    sources = []
    for block in (search_data or {}).get('results') or []:
        for row in block.get('items') or []:
            if isinstance(row, dict):
                sources.append(row)
    # 去重：中心 MVP 可能同一个季分享返回多集，共享码相同但 sha1 不同，不能只按 share_code 去重。
    seen = set()
    unique = []
    for src in sources:
        key = (src.get('source_id'), src.get('sha1'))
        if key in seen:
            continue
        seen.add(key)
        unique.append(src)
    return unique


def _get_local_strm_root() -> str:
    return str(_cfg('CONFIG_OPTION_LOCAL_STRM_ROOT', 'local_strm_root', '/mnt/media') or '/mnt/media')


def _virtual_rel_dir(source: Dict[str, Any], context: Dict[str, Any]) -> str:
    title = _sanitize_filename(context.get('title') or source.get('title') or '共享资源')
    item_type = str(source.get('item_type') or context.get('item_type') or '')
    season = source.get('season_number') or context.get('season_number')
    if item_type in ('Episode', 'Season', 'Series') or season is not None:
        try:
            return f"共享虚拟/{title}/Season {int(season or 1):02d}"
        except Exception:
            return f"共享虚拟/{title}/Season 01"
    year = context.get('year') or source.get('release_year') or ''
    suffix = f" ({year})" if year else ''
    return f"共享虚拟/{title}{suffix}"


def _build_virtual_id(source: Dict[str, Any]) -> str:
    sha1 = _norm_sha1(source.get('sha1'))
    sid = str(source.get('source_id') or '')
    if sha1:
        return f"virt_{sha1[:20].lower()}"
    return f"virt_{uuid.uuid4().hex}"


def _write_text_file(path: str, text: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text, encoding='utf-8')
    try:
        from monitor_service import enqueue_file_actively
        enqueue_file_actively(path)
    except Exception:
        pass


def _save_raw_and_write_mediainfo(source: Dict[str, Any], raw_map: Dict[str, Dict[str, Any]], mediainfo_path: str) -> bool:
    sha1 = _norm_sha1(source.get('sha1'))
    raw = raw_map.get(sha1)
    if not raw:
        return False
    file_name = source.get('file_name') or f'{sha1}.mkv'
    file_node = {
        'fn': file_name,
        'file_name': file_name,
        'size': _safe_int(source.get('size'), 0),
        'fs': _safe_int(source.get('size'), 0),
        'sha1': sha1,
    }
    try:
        builder = _MediainfoBuilder()
        emby_obj = builder._build_emby_mediainfo_from_ffprobe(raw, file_node, sha1=sha1)
        if not emby_obj:
            return False
        P115CacheManager.save_mediainfo_cache(sha1, emby_obj, raw)
        _write_text_file(mediainfo_path, json.dumps(emby_obj, ensure_ascii=False, indent=2))
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟] 生成 mediainfo 失败: sha1={sha1[:12]}, err={e}")
        return False


def _upsert_virtual_item(source: Dict[str, Any], context: Dict[str, Any], strm_path: str, mediainfo_path: str = ''):
    virtual_id = _build_virtual_id(source)
    raw_json = {
        'center_source': source,
        'context': context,
        'virtual_protocol': f'etk-shared://{virtual_id}',
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_virtual_items(
                    virtual_id, source_id, source_key, source_provider,
                    tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    title, release_year, sha1, size, file_name, quality,
                    strm_path, mediainfo_path, share_code, receive_code, contributor_id,
                    cache_parent_id, cache_parent_name, status, raw_json, updated_at
                )
                VALUES(
                    %s,%s,%s,'shared_center',
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,'virtual_ready',%s::jsonb,NOW()
                )
                ON CONFLICT(virtual_id) DO UPDATE SET
                    source_id=EXCLUDED.source_id,
                    tmdb_id=EXCLUDED.tmdb_id,
                    item_type=EXCLUDED.item_type,
                    parent_series_tmdb_id=EXCLUDED.parent_series_tmdb_id,
                    season_number=EXCLUDED.season_number,
                    episode_number=EXCLUDED.episode_number,
                    title=EXCLUDED.title,
                    release_year=EXCLUDED.release_year,
                    sha1=EXCLUDED.sha1,
                    size=CASE WHEN EXCLUDED.size > 0 THEN EXCLUDED.size ELSE shared_virtual_items.size END,
                    file_name=EXCLUDED.file_name,
                    strm_path=EXCLUDED.strm_path,
                    mediainfo_path=COALESCE(NULLIF(EXCLUDED.mediainfo_path,''), shared_virtual_items.mediainfo_path),
                    share_code=EXCLUDED.share_code,
                    receive_code=EXCLUDED.receive_code,
                    contributor_id=EXCLUDED.contributor_id,
                    cache_parent_id=EXCLUDED.cache_parent_id,
                    cache_parent_name=EXCLUDED.cache_parent_name,
                    raw_json=EXCLUDED.raw_json,
                    status=CASE WHEN shared_virtual_items.status='deleted' THEN 'virtual_ready' ELSE shared_virtual_items.status END,
                    updated_at=NOW()
                """,
                (
                    virtual_id,
                    source.get('source_id'),
                    source.get('source_key'),
                    str(source.get('tmdb_id') or context.get('tmdb_id') or ''),
                    source.get('item_type') or context.get('item_type') or 'Movie',
                    context.get('parent_tmdb_id') or (str(source.get('tmdb_id')) if source.get('item_type') in ('Episode','Season','Series') else None),
                    source.get('season_number') or context.get('season_number'),
                    source.get('episode_number'),
                    context.get('title') or source.get('title') or source.get('file_name'),
                    _safe_int(source.get('release_year') or context.get('year'), None),
                    _norm_sha1(source.get('sha1')),
                    _safe_int(source.get('size'), 0),
                    source.get('file_name') or '',
                    source.get('quality') or '',
                    strm_path,
                    mediainfo_path,
                    source.get('share_code') or '',
                    source.get('receive_code') or '',
                    source.get('contributor_id') or source.get('provider_id') or '',
                    str(_cfg('CONFIG_OPTION_115_SHARED_CACHE_CID', 'p115_shared_cache_cid', '') or ''),
                    str(_cfg('CONFIG_OPTION_115_SHARED_CACHE_NAME', 'p115_shared_cache_name', '') or ''),
                    json.dumps(raw_json, ensure_ascii=False),
                )
            )
            conn.commit()
    return virtual_id


def _consume_virtual(client: SharedCenterClient, sources: List[Dict[str, Any]], context: Dict[str, Any]) -> Dict[str, Any]:
    root = _get_local_strm_root()
    sha1s = [_norm_sha1(s.get('sha1')) for s in sources if s.get('sha1')]
    raw_map = {}
    try:
        raw_result = client.fetch_raw_ffprobe_batch(sha1s)
        for item in raw_result.get('items') or []:
            if item.get('status') == 'ok' and item.get('raw_ffprobe_json'):
                raw_map[_norm_sha1(item.get('sha1'))] = item.get('raw_ffprobe_json')
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟] 拉取 raw_ffprobe_json 失败，将先生成 STRM 占位: {e}")

    created = 0
    for source in sources:
        file_name = source.get('file_name') or f"{source.get('sha1')}.mkv"
        rel_dir = _virtual_rel_dir(source, context)
        stem = os.path.splitext(_sanitize_filename(file_name))[0]
        strm_path = os.path.join(root, rel_dir, f"{stem}.strm")
        mediainfo_path = os.path.join(root, rel_dir, f"{stem}-mediainfo.json")
        virtual_id = _build_virtual_id(source)
        _write_text_file(strm_path, f"etk-shared://{virtual_id}")
        has_mi = _save_raw_and_write_mediainfo(source, raw_map, mediainfo_path)
        _upsert_virtual_item(source, context, strm_path, mediainfo_path if has_mi else '')
        created += 1
    return {'success': created > 0, 'mode': 'virtual', 'count': created, 'action_type': '共享虚拟'}


def _consume_permanent(client: SharedCenterClient, sources: List[Dict[str, Any]], context: Dict[str, Any]) -> Dict[str, Any]:
    p115 = P115Service.get_client()
    if not p115:
        raise RuntimeError('115 客户端未初始化')
    target_cid = str(
        _cfg('CONFIG_OPTION_115_MEDIA_ROOT_CID', 'p115_media_root_cid', '')
        or _cfg('CONFIG_OPTION_115_SHARED_CACHE_CID', 'p115_shared_cache_cid', '')
        or ''
    )
    if not target_cid:
        raise RuntimeError('未配置 115 媒体库根目录 CID，无法永久转存共享资源')

    # 同一个季/剧分享可能返回多集，按 share_code 去重，避免重复转存同一分享包。
    unique = []
    seen_share = set()
    for src in sources:
        code = src.get('share_code') or src.get('source_id')
        if code in seen_share:
            continue
        seen_share.add(code)
        unique.append(src)

    ok = 0
    errors = []
    for src in unique:
        share_code = src.get('share_code') or ''
        receive_code = src.get('receive_code') or ''
        if not share_code:
            errors.append(f"{src.get('file_name')}: 缺少分享码")
            continue
        resp = p115.share_import(share_code, receive_code, target_cid)
        text = json.dumps(resp, ensure_ascii=False) if isinstance(resp, dict) else str(resp)
        success = isinstance(resp, dict) and (resp.get('state') is True or resp.get('errno') in (0, '0') or resp.get('code') in (0, '0', 200, '200') or '已存在' in text)
        if success:
            ok += 1
            try:
                client.report_transfer(src.get('source_id'), 'success', expected_sha1=_norm_sha1(src.get('sha1')), expected_size=_safe_int(src.get('size'), 0) or None, message='permanent import submitted')
            except Exception:
                pass
        else:
            errors.append(f"{src.get('file_name')}: {text[:120]}")
            try:
                client.report_transfer(src.get('source_id'), 'failed', expected_sha1=_norm_sha1(src.get('sha1')), expected_size=_safe_int(src.get('size'), 0) or None, message=text[:180])
            except Exception:
                pass
    if ok > 0:
        try:
            import task_manager
            task_manager.trigger_115_organize_task()
        except Exception:
            pass
    return {'success': ok > 0, 'mode': 'permanent', 'count': ok, 'action_type': '共享永久转存', 'errors': errors}


def try_consume_shared_resource(item: Dict[str, Any], title: str, tmdb_id, item_type: str, parent_tmdb_id=None, season_number=None, year='') -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'success': False, 'reported_gap': False}

    client = SharedCenterClient()
    if not client.ready:
        logger.warning('  ➜ [共享资源] 已启用但中心地址/token 未配置，跳过共享池。')
        return {'enabled': True, 'success': False, 'reported_gap': False}

    queries = _build_center_queries(item, title, tmdb_id, item_type, parent_tmdb_id, season_number, year)
    if not queries:
        return {'enabled': True, 'success': False, 'reported_gap': False}

    sources = []
    try:
        data = client.search_sources(queries, limit_per_item=200)
        sources = _flatten_search_results(data)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询中心共享池失败: {e}")

    if not sources:
        reported = False
        try:
            client.report_gaps(queries)
            reported = True
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 中心未命中，登记缺口失败: {e}")
        return {'enabled': True, 'success': False, 'reported_gap': reported}

    context = {
        'title': title,
        'tmdb_id': str(tmdb_id or ''),
        'item_type': item_type,
        'parent_tmdb_id': str(parent_tmdb_id or ''),
        'season_number': season_number,
        'year': year,
    }

    mode = shared_resource_mode()
    if mode == 'virtual':
        return _consume_virtual(client, sources, context)
    return _consume_permanent(client, sources, context)
