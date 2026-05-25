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
from handler.p115_service import P115Service, P115CacheManager, SmartOrganizer
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
    elif item_type == 'Episode':
        sid = parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
        s_num = season_number if season_number not in (None, '') else item.get('season_number')
        e_num = item.get('episode_number')
        # 中心 Episode 查询以“父剧 TMDb + SxxEyy”为主；部分旧数据可能按单集 TMDb 登记，保留兜底。
        if sid:
            queries.append(_build_gap_item(tmdb_id=sid, item_type='Episode', title=title, season_number=s_num, episode_number=e_num, year=year))
        if tmdb_id and str(tmdb_id) != str(sid or ''):
            queries.append(_build_gap_item(tmdb_id=tmdb_id, item_type='Episode', title=title, season_number=s_num, episode_number=e_num, year=year))
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


def _sanitize_rel_path(path: str) -> str:
    """清理相对路径，保留 / 分层；用于复用 115 正式整理的分类目录。"""
    parts = []
    for part in re.split(r'[\\/]+', str(path or '')):
        part = _sanitize_filename(part)
        if part and part not in ('.', '..'):
            parts.append(part)
    return '/'.join(parts)


def _path_node_id(node: Dict[str, Any]) -> str:
    return str(
        (node or {}).get('cid')
        or (node or {}).get('file_id')
        or (node or {}).get('fid')
        or (node or {}).get('id')
        or ''
    )


def _path_node_name(node: Dict[str, Any]) -> str:
    return str(
        (node or {}).get('file_name')
        or (node or {}).get('fn')
        or (node or {}).get('name')
        or (node or {}).get('n')
        or ''
    ).strip()


def _strip_media_root_from_local_path(local_path: str) -> str:
    """p115_filesystem_cache.local_path 有时包含媒体库根目录名，这里转成本地 STRM 根目录下的相对分类路径。"""
    path = _sanitize_rel_path(local_path)
    if not path:
        return ''
    media_root_name = str(_cfg('CONFIG_OPTION_115_MEDIA_ROOT_NAME', 'p115_media_root_name', '') or '').strip('/\\')
    if media_root_name:
        parts = [p for p in path.split('/') if p]
        if media_root_name in parts:
            idx = parts.index(media_root_name)
            return '/'.join(parts[idx + 1:])
    return path


def _derive_category_path_from_115(client, target_cid: str) -> str:
    """按正式整理逻辑，从 115 path 面包屑推导分类目录相对路径。"""
    if not client or not target_cid:
        return ''
    try:
        res = client.fs_files({'cid': str(target_cid), 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
        path_nodes = (res or {}).get('path') or (res or {}).get('paths') or (res or {}).get('breadcrumb') or []
        if not isinstance(path_nodes, list):
            return ''

        media_root_cid = str(_cfg('CONFIG_OPTION_115_MEDIA_ROOT_CID', 'p115_media_root_cid', '0') or '0')
        start_idx = 0
        found_root = False
        if media_root_cid == '0':
            start_idx = 0 if str(target_cid) == '0' else 1
            found_root = True
        else:
            for idx, node in enumerate(path_nodes):
                if _path_node_id(node) == media_root_cid:
                    start_idx = idx + 1
                    found_root = True
                    break

        if found_root and start_idx < len(path_nodes):
            rel = '/'.join(_path_node_name(n) for n in path_nodes[start_idx:] if _path_node_name(n))
            return _sanitize_rel_path(rel)
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟] 从 115 面包屑推导分类路径失败: cid={target_cid}, err={e}")
    return ''


def _resolve_category_rel_path(organizer: SmartOrganizer, target_cid: str, client=None) -> str:
    """复用正式 115 整理规则，把目标 CID 转成本地 STRM 分类相对目录。"""
    target_cid = str(target_cid or '')
    matched_rule = None
    for rule in getattr(organizer, 'rules', []) or []:
        if str(rule.get('cid') or '') == target_cid:
            matched_rule = rule
            break

    if matched_rule:
        if matched_rule.get('category_path'):
            return _sanitize_rel_path(matched_rule.get('category_path'))

    # 优先复用本地 115 缓存里的 local_path。
    try:
        cached_path = P115CacheManager.get_local_path(target_cid)
        cached_rel = _strip_media_root_from_local_path(cached_path)
        if cached_rel:
            return cached_rel
    except Exception:
        pass

    # 再按正式整理逻辑从 115 path 面包屑推导，并回写到规则，避免下次重复查。
    derived = _derive_category_path_from_115(client, target_cid)
    if derived:
        if matched_rule is not None:
            try:
                matched_rule['category_path'] = derived
                settings_db.save_setting('p115_sorting_rules', organizer.rules)
            except Exception:
                pass
        return derived

    if matched_rule:
        return _sanitize_rel_path(matched_rule.get('dir_name') or matched_rule.get('name') or '未识别')
    return '未识别'


def _build_standard_root_name(organizer: SmartOrganizer, media_type: str, fallback_title: str) -> str:
    cfg = getattr(organizer, 'rename_config', {}) or {}
    details = getattr(organizer, 'details', {}) or {}
    title = details.get('title') or fallback_title or getattr(organizer, 'original_title', '')
    original_title = details.get('original_title') or title
    main_format = cfg.get('main_dir_format', ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'])
    try:
        root_name = organizer._build_name_from_format(
            main_format,
            is_tv=(media_type == 'tv'),
            original_title=original_title,
        )
    except Exception:
        root_name = ''
    if not root_name:
        root_name = title
    return _sanitize_rel_path(root_name) or _sanitize_filename(fallback_title or 'Unknown')


def _build_standard_season_dir(organizer: SmartOrganizer, season_number) -> str:
    try:
        s_num = int(season_number)
    except Exception:
        s_num = 1
    cfg = getattr(organizer, 'rename_config', {}) or {}
    details = getattr(organizer, 'details', {}) or {}
    original_title = details.get('original_title') or details.get('title') or getattr(organizer, 'original_title', '')
    season_format = cfg.get('season_dir_format', ['season_name_en'])
    try:
        name = organizer._build_name_from_format(
            season_format,
            is_tv=True,
            season_num=s_num,
            original_title=original_title,
        )
    except Exception:
        name = ''
    return _sanitize_filename(name or f"Season {s_num:02d}")


def _legacy_virtual_rel_dir(source: Dict[str, Any], context: Dict[str, Any]) -> str:
    """兜底路径：不再生成到“共享虚拟”根目录，避免和正式媒体分类割裂。"""
    title = _sanitize_filename(context.get('title') or source.get('title') or '共享资源')
    item_type = str(source.get('item_type') or context.get('item_type') or '')
    season = source.get('season_number') or context.get('season_number')
    if item_type in ('Episode', 'Season', 'Series') or season is not None:
        try:
            return f"未识别/{title}/Season {int(season or 1):02d}"
        except Exception:
            return f"未识别/{title}/Season 01"
    year = context.get('year') or source.get('release_year') or ''
    suffix = f" ({year})" if year else ''
    return f"未识别/{title}{suffix}"


def _virtual_rel_dir(source: Dict[str, Any], context: Dict[str, Any]) -> str:
    """虚拟入库 STRM 目录。

    这里不再固定写入“共享虚拟”，而是复用正式 115 入库的 SmartOrganizer：
    - 根据 p115_sorting_rules / 历史整理记忆判断分类目录；
    - 根据 p115_rename_config 生成主目录和季目录；
    - 只生成本地 STRM，不移动 115 文件。
    """
    title = context.get('title') or source.get('title') or source.get('file_name') or '共享资源'
    item_type = str(source.get('item_type') or context.get('item_type') or '')
    season = source.get('season_number') or context.get('season_number')

    media_type = 'movie'
    if item_type in ('Episode', 'Season', 'Series') or str(context.get('item_type') or '') in ('Season', 'Series') or season is not None:
        media_type = 'tv'

    tmdb_id = context.get('parent_tmdb_id') if media_type == 'tv' else None
    tmdb_id = tmdb_id or context.get('tmdb_id') or source.get('tmdb_id')

    try:
        tmdb_id_int = int(str(tmdb_id))
    except Exception:
        logger.warning(f"  ➜ [共享虚拟] 缺少可用 TMDb ID，无法复用正式分类规则，使用未识别目录: {tmdb_id}")
        return _legacy_virtual_rel_dir(source, context)

    try:
        p115_client = P115Service.get_client()
        organizer = SmartOrganizer(p115_client, tmdb_id_int, media_type, title, None, False)
        if media_type == 'tv' and season is not None:
            try:
                organizer.forced_season = int(season)
            except Exception:
                pass

        target_cid = organizer.get_target_cid(season_num=int(season) if media_type == 'tv' and season is not None else None)
        category_rel = _resolve_category_rel_path(organizer, target_cid, p115_client) if target_cid else '未识别'
        root_name = _build_standard_root_name(organizer, media_type, title)

        if media_type == 'tv':
            season_dir = _build_standard_season_dir(organizer, season or 1)
            return os.path.join(category_rel, root_name, season_dir)
        return os.path.join(category_rel, root_name)
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟] 复用正式分类规则失败，使用未识别目录: {e}", exc_info=True)
        return _legacy_virtual_rel_dir(source, context)


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


def _remove_empty_parents(path: str, stop_root: str):
    try:
        stop = Path(stop_root).resolve()
        current = Path(path).resolve().parent
        while current != stop and stop in current.parents:
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent
    except Exception:
        pass


def _media_root_cid() -> str:
    # 正式媒体根目录必须和临时转存目录分离。
    # 旧版曾 fallback 到 p115_shared_cache_cid，会导致“转正”目标仍在临时目录下。
    return str(
        _cfg('CONFIG_OPTION_115_MEDIA_ROOT_CID', 'p115_media_root_cid', '')
        or '0'
    ).strip() or '0'


def _safe_path_parts(rel_dir: str) -> List[str]:
    parts = []
    for part in str(rel_dir or '').replace('\\', '/').split('/'):
        part = part.strip()
        if not part or part in ('.', '..'):
            continue
        parts.append(_sanitize_filename(part))
    return parts


def ensure_virtual_target_by_rel_dir(rel_dir: str, client=None) -> Dict[str, str]:
    """按本地 STRM 相对目录，在 115 正式媒体根目录下确保对应目录存在。

    虚拟入库只生成本地 STRM，真实文件首次播放时临时转存到缓存目录。
    手动“转正”时需要把真实文件移动到正式媒体库目录，因此创建虚拟项时就把
    target_parent_id 解析/创建好；老数据缺失时 promote 接口也会调用本函数兜底。
    """
    parts = _safe_path_parts(rel_dir)
    if not parts:
        return {}

    client = client or P115Service.get_client()
    if not client:
        raise RuntimeError('未配置可用的 115 客户端，无法解析正式媒体目录')

    current_cid = _media_root_cid()
    if not current_cid or str(current_cid) == '0':
        raise RuntimeError('未配置 115 正式媒体库根目录 CID（p115_media_root_cid），无法为虚拟入库解析转正目录')
    built_parts = []
    for part in parts:
        built_parts.append(part)
        res = client.fs_mkdir(part, current_cid)
        if not res or not res.get('state'):
            # fs_mkdir 本身已经做“已存在”回收；这里再查一次 DB 缓存兜底。
            cached = None
            try:
                cached = P115CacheManager.get_cid(current_cid, part)
            except Exception:
                cached = None
            if not cached:
                raise RuntimeError(f"创建/定位正式目录失败: {part} -> {res}")
            next_cid = str(cached)
        else:
            data = res.get('data') if isinstance(res.get('data'), dict) else {}
            next_cid = str(
                res.get('cid')
                or res.get('file_id')
                or res.get('id')
                or data.get('cid')
                or data.get('file_id')
                or data.get('id')
                or ''
            ).strip()
            if not next_cid:
                cached = P115CacheManager.get_cid(current_cid, part)
                next_cid = str(cached or '').strip()
        if not next_cid:
            raise RuntimeError(f"无法取得正式目录 CID: {part}")
        try:
            P115CacheManager.save_cid(next_cid, current_cid, part)
            P115CacheManager.update_local_path(next_cid, '/'.join(built_parts))
        except Exception:
            pass
        current_cid = next_cid

    return {
        'target_parent_id': current_cid,
        'target_parent_name': parts[-1],
        'target_rel_dir': '/'.join(parts),
    }


def ensure_virtual_target_from_strm_path(strm_path: str, client=None) -> Dict[str, str]:
    """通过已生成 STRM 路径反推正式 115 目标目录，给旧虚拟项转正兜底。"""
    if not strm_path:
        return {}
    root = os.path.abspath(_get_local_strm_root())
    parent_dir = os.path.abspath(os.path.dirname(strm_path))
    try:
        rel_dir = os.path.relpath(parent_dir, root)
    except Exception:
        return {}
    if rel_dir.startswith('..') or os.path.isabs(rel_dir):
        return {}
    return ensure_virtual_target_by_rel_dir(rel_dir, client=client)


def _cleanup_old_virtual_files(virtual_id: str, new_strm_path: str, new_mediainfo_path: str = ''):
    """同一个 virtual_id 重新生成到分类目录时，清掉旧的“共享虚拟/...”残留文件。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strm_path, mediainfo_path FROM shared_virtual_items WHERE virtual_id=%s",
                    (virtual_id,),
                )
                row = cur.fetchone()
        if not row:
            return
        row = dict(row)
        root = _get_local_strm_root()
        for old_path, new_path in (
            (row.get('strm_path'), new_strm_path),
            (row.get('mediainfo_path'), new_mediainfo_path),
        ):
            if old_path and old_path != new_path and os.path.exists(old_path):
                try:
                    os.remove(old_path)
                    logger.info(f"  ➜ [共享虚拟] 已清理旧投影文件: {old_path}")
                    _remove_empty_parents(old_path, root)
                except Exception as e:
                    logger.debug(f"  ➜ [共享虚拟] 清理旧投影文件失败: {old_path}, err={e}")
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


def _upsert_virtual_item(source: Dict[str, Any], context: Dict[str, Any], strm_path: str, mediainfo_path: str = '', target_info: Dict[str, str] = None):
    target_info = target_info or {}
    virtual_id = _build_virtual_id(source)
    raw_json = {
        'center_source': source,
        'context': context,
        'virtual_protocol': f'etk-shared://{virtual_id}',
        'target_info': target_info,
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
                    cache_parent_id, cache_parent_name, target_parent_id, target_parent_name, status, raw_json, updated_at
                )
                VALUES(
                    %s,%s,%s,'shared_center',
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,'virtual_ready',%s::jsonb,NOW()
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
                    target_parent_id=COALESCE(NULLIF(EXCLUDED.target_parent_id,''), shared_virtual_items.target_parent_id),
                    target_parent_name=COALESCE(NULLIF(EXCLUDED.target_parent_name,''), shared_virtual_items.target_parent_name),
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
                    str(target_info.get('target_parent_id') or ''),
                    str(target_info.get('target_parent_name') or ''),
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
        target_info = {}
        try:
            target_info = ensure_virtual_target_by_rel_dir(rel_dir, client=P115Service.get_client())
        except Exception as e:
            logger.warning(f"  ➜ [共享虚拟] 解析正式转正目录失败，后续转正时会再次尝试: {e}")
        stem = os.path.splitext(_sanitize_filename(file_name))[0]
        strm_path = os.path.join(root, rel_dir, f"{stem}.strm")
        mediainfo_path = os.path.join(root, rel_dir, f"{stem}-mediainfo.json")
        virtual_id = _build_virtual_id(source)
        _cleanup_old_virtual_files(virtual_id, strm_path, mediainfo_path)
        _write_text_file(strm_path, f"etk-shared://{virtual_id}")
        has_mi = _save_raw_and_write_mediainfo(source, raw_map, mediainfo_path)
        _upsert_virtual_item(source, context, strm_path, mediainfo_path if has_mi else '', target_info=target_info)
        created += 1
    return {'success': created > 0, 'mode': 'virtual', 'count': created, 'action_type': '共享虚拟'}


def _consume_permanent(client: SharedCenterClient, sources: List[Dict[str, Any]], context: Dict[str, Any]) -> Dict[str, Any]:
    p115 = P115Service.get_client()
    if not p115:
        raise RuntimeError('115 客户端未初始化')
    # 中心资源库“转存”不是直接入正式媒体库，而是先接收到 115 待整理目录，
    # 再触发原有 115 智能整理流程。
    target_cid = str(
        _cfg('CONFIG_OPTION_115_SAVE_PATH_CID', 'p115_save_path_cid', '')
        or ''
    ).strip()
    if not target_cid or target_cid == '0':
        raise RuntimeError('未配置 115 待整理目录 CID（p115_save_path_cid），无法转存共享资源')

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
                client.report_transfer(src.get('source_id'), 'failed', expected_sha1=_norm_sha1(src.get('sha1')), expected_size=_safe_int(src.get('size'), 0) or None, message=f'external_share_import_failed: {text[:160]}')
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


def consume_center_sources(source_ids: List[str], mode: str = 'permanent', context: Dict[str, Any] = None) -> Dict[str, Any]:
    """按中心 source_id 手动消费共享资源。

    用于前端“中心资源库”标签页：管理员可以直接选择中心已有版本，按永久转存或虚拟入库处理。
    """
    if not shared_center_enabled():
        return {'enabled': False, 'success': False, 'message': '共享资源未启用'}

    source_ids = [str(x or '').strip() for x in (source_ids or []) if str(x or '').strip()]
    if not source_ids:
        return {'enabled': True, 'success': False, 'message': '缺少 source_ids'}

    client = SharedCenterClient()
    if not client.ready:
        return {'enabled': True, 'success': False, 'message': '共享中心地址或 device_token 未配置'}

    if not hasattr(client, 'list_sources'):
        return {'enabled': True, 'success': False, 'message': 'SharedCenterClient 缺少 list_sources 方法，请同步 handler/shared_center_client.py'}

    data = client.list_sources(source_ids=source_ids, limit=len(source_ids), include_raw=True)
    sources = [x for x in (data.get('items') or []) if isinstance(x, dict)]
    if not sources:
        return {'enabled': True, 'success': False, 'message': '中心未返回可用资源'}

    first = sources[0]
    ctx = dict(context or {})
    ctx.setdefault('title', first.get('title') or first.get('file_name') or '')
    ctx.setdefault('tmdb_id', first.get('tmdb_id') or '')
    ctx.setdefault('item_type', first.get('item_type') or '')
    ctx.setdefault('season_number', first.get('season_number'))
    ctx.setdefault('year', first.get('release_year'))

    selected_mode = str(mode or '').strip().lower()
    if selected_mode not in ('permanent', 'virtual'):
        selected_mode = shared_resource_mode()

    if selected_mode == 'virtual':
        return _consume_virtual(client, sources, ctx)
    return _consume_permanent(client, sources, ctx)
