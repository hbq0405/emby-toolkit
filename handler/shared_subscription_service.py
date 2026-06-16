# handler/shared_subscription_service.py
# Rapid v2 共享资源消费入口：中心调度，本机 CK 执行秒传/入库。
import base64
import concurrent.futures
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Tuple

import config_manager
import constants
from database import settings_db
from handler.p115_service import P115Service, P115CacheManager, SmartOrganizer
from handler.p115_media_analyzer import P115MediaAnalyzerMixin
from handler.shared_center_client import SharedCenterClient, shared_center_enabled

logger = logging.getLogger(__name__)

_ORGANIZE_KICK_LOCK = threading.Lock()
_LAST_ORGANIZE_KICK_AT = 0

VIDEO_EXTS = {'.mkv', '.mp4', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.flv', '.rmvb', '.webm', '.iso'}

# 中心转存结果上报重试队列：秒传/分享转存已经成功落盘时，中心 report
# 如果遇到 TLS/反代瞬断，不能只打一条 warning 就丢失热度、扣点和 lease 释放。
_PENDING_TRANSFER_REPORT_LOCK = threading.Lock()
_PENDING_TRANSFER_REPORT_DRAIN_LOCK = threading.Lock()
_PENDING_TRANSFER_REPORT_QUEUE_FILE = 'shared_transfer_report_retry_queue.jsonl'
_PENDING_TRANSFER_REPORT_MAX_ITEMS = 1000
_PENDING_TRANSFER_REPORT_RETRY_BASE_SECONDS = 30
_PENDING_TRANSFER_REPORT_RETRY_MAX_SECONDS = 3600
_PENDING_TRANSFER_REPORT_DEFAULT_DRAIN_LIMIT = 20



class _MediainfoBuilder(P115MediaAnalyzerMixin):
    pass


def _kick_115_organize_detached(reason: str = '', delay: float = 3.0) -> Dict[str, Any]:
    global _LAST_ORGANIZE_KICK_AT
    now = time.time()
    with _ORGANIZE_KICK_LOCK:
        if now - _LAST_ORGANIZE_KICK_AT < 10:
            return {'started': False, 'message': '115 整理扫描刚触发过，本次不重复启动'}
        _LAST_ORGANIZE_KICK_AT = now

    def _runner():
        if delay and delay > 0:
            time.sleep(delay)
        try:
            from tasks.p115 import task_scan_and_organize_115
            logger.info(f"  ➜ [共享资源] 准备扫描待整理...")
            task_scan_and_organize_115()
        except Exception as e:
            logger.error(f"  ➜ [共享资源] 触发 115 待整理扫描失败: {e}", exc_info=True)

    threading.Thread(target=_runner, name='shared-rapid-import-organize', daemon=True).start()
    return {'started': True, 'message': '已异步触发 115 待整理扫描'}


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


def _normalize_source_kind(value: str) -> str:
    """把前端/中心返回的 Movie、display_type、旧 kind 统一成中心接口认可的小写 source_kind。"""
    text = str(value or '').strip().lower().replace('-', '_')
    if text in ('movie', 'movie_file', 'movie_folder', 'film'):
        return 'movie'
    if text in ('episode', 'episode_file', 'single'):
        return 'episode'
    if text in ('logical_episode', 'logical_episode_asset'):
        return 'logical_episode'
    if text in ('logical_season', 'season_version_group'):
        return 'logical_season'
    if text == 'completed_season':
        return 'deprecated_completed_season'
    if text in ('season', 'season_pack', 'tv_pack', 'pack'):
        return 'logical_season'
    if text in ('season_hub', 'hub', 'ongoing_hub'):
        return 'season_hub'
    return text


def _rapid_size_to_int(value, default=0) -> int:
    """把中心端/本地缓存里的 size / file_size / 0.69 GB 统一转成字节。"""
    try:
        if value in (None, '', [], {}):
            return default
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(',', '')
        if not text:
            return default
        if re.fullmatch(r'\d+(?:\.0+)?', text):
            return int(float(text))
        m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|G|MB|M|KB|K|B)?', text, re.I)
        if not m:
            return default
        n = float(m.group(1))
        unit = (m.group(2) or 'B').upper()
        if unit == 'TB': n *= 1024 ** 4
        elif unit in ('GB', 'G'): n *= 1024 ** 3
        elif unit in ('MB', 'M'): n *= 1024 ** 2
        elif unit in ('KB', 'K'): n *= 1024
        return int(n)
    except Exception:
        return default


def _dict_size_candidates(data: Dict[str, Any]) -> List[Any]:
    if not isinstance(data, dict):
        return []
    values = []
    for key in ('size', 'file_size', 'filesize', 'size_bytes', 'fileSize', 'file_size_bytes', 'total_size'):
        values.append(data.get(key))
    for nested_key in ('rapid_meta_json', 'rapid_meta', 'media_signature_json', 'summary_json', 'raw_summary_json', 'version_summary'):
        nested = data.get(nested_key)
        if isinstance(nested, str):
            try:
                nested = json.loads(nested)
            except Exception:
                nested = None
        if isinstance(nested, dict):
            for key in ('size', 'file_size', 'filesize', 'size_bytes', 'fileSize', 'file_size_bytes'):
                values.append(nested.get(key))
    return values


def _lookup_p115_cache_for_file(file_info: Dict[str, Any]) -> Dict[str, Any]:
    from database.connection import get_db_connection
    from handler.p115_service import P115CacheManager
    sha1 = _norm_sha1((file_info or {}).get('sha1'))
    meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    pick_code = str((file_info or {}).get('pick_code') or (file_info or {}).get('pc') or meta.get('pick_code') or meta.get('pc') or '').strip()
    if not sha1 and not pick_code:
        return {}
    manager_row = {}
    try:
        if pick_code and hasattr(P115CacheManager, 'get_file_cache_by_pickcode'):
            row = P115CacheManager.get_file_cache_by_pickcode(pick_code)
            if row:
                manager_row = dict(row)
        if not manager_row and sha1 and hasattr(P115CacheManager, 'get_file_cache_by_sha1'):
            row = P115CacheManager.get_file_cache_by_sha1(sha1)
            if row:
                manager_row = dict(row)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询 P115CacheManager 补 size 失败: {e}")
    try:
        clauses, args = [], []
        if sha1:
            clauses.append("UPPER(sha1)=%s")
            args.append(sha1)
        if pick_code:
            clauses.append("pick_code=%s")
            args.append(pick_code)
        if not clauses:
            return {}
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, parent_id, name, local_path, sha1, pick_code, preid, size, updated_at
                    FROM p115_filesystem_cache
                    WHERE {' OR '.join(clauses)}
                    ORDER BY CASE WHEN COALESCE(size,0) > 0 THEN 0 ELSE 1 END,
                             updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    args,
                )
                row = cur.fetchone()
                sql_row = dict(row) if row else {}
                if manager_row:
                    merged = dict(manager_row)
                    merged.update({k: v for k, v in sql_row.items() if v not in (None, '')})
                    return merged
                return sql_row
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 直接查询 p115_filesystem_cache 补 size/preid 失败: {e}")
    return manager_row or {}


def _normalize_rapid_file_info(file_info: Dict[str, Any]) -> Dict[str, Any]:
    info = dict(file_info or {})
    meta = info.get('rapid_meta_json') if isinstance(info.get('rapid_meta_json'), dict) else {}
    preid = _norm_sha1(info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
    sha1 = _norm_sha1(info.get('sha1') or info.get('file_sha1') or meta.get('sha1') or meta.get('file_sha1'))
    if sha1:
        info['sha1'] = sha1
    if preid:
        info['preid'] = preid
        meta = dict(meta)
        meta.setdefault('preid', preid)
        info['rapid_meta_json'] = meta
    file_name = str(info.get('file_name') or info.get('name') or meta.get('file_name') or meta.get('name') or '').strip()
    if file_name:
        info['file_name'] = file_name
    size = 0
    for candidate in _dict_size_candidates(info):
        size = _rapid_size_to_int(candidate, 0)
        if size > 0:
            break
    cache_row = None
    if size <= 0 or not preid:
        cache_row = _lookup_p115_cache_for_file(info)
        if cache_row:
            size = _rapid_size_to_int(cache_row.get('size'), 0)
            if not info.get('file_name') and cache_row.get('name'):
                info['file_name'] = cache_row.get('name')
            meta = dict(meta)
            if cache_row.get('id') and not meta.get('fid'):
                meta['fid'] = str(cache_row.get('id'))
            cache_preid = _norm_sha1(cache_row.get('preid'))
            if cache_row.get('pick_code') and not meta.get('pick_code'):
                meta['pick_code'] = str(cache_row.get('pick_code'))
            if cache_preid and not info.get('preid'):
                info['preid'] = cache_preid
                meta.setdefault('preid', cache_preid)
            if meta:
                info['rapid_meta_json'] = meta
    if size > 0:
        info['size'] = size
        info['file_size'] = size
    return info


def _target_cid() -> str:
    cid = str(_cfg('CONFIG_OPTION_115_SAVE_PATH_CID', 'p115_save_path_cid', '') or '').strip()
    if not cid or cid == '0':
        raise RuntimeError('未配置 115 待整理目录 CID（p115_save_path_cid），无法秒传共享资源')
    return cid


def _safe_115_folder_name(value: Any, fallback: str = '共享季包') -> str:
    """生成 115 临时接收目录名：保留 TMDb/Season 识别信息，清理路径危险字符。"""
    text = str(value or '').strip()
    if not text:
        text = fallback
    text = re.sub(r'[\\/:*?"<>|\r\n\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip(' ._-')
    return (text or fallback)[:180]


def _first_nonempty(*values):
    for value in values:
        if value not in (None, '', [], {}):
            return value
    return None


def _season_package_context(payload: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = payload or {}
    first = next((f for f in (files or []) if isinstance(f, dict)), {}) or {}

    season_number = _first_nonempty(
        payload.get('season_number'), first.get('season_number')
    )
    try:
        season_number = int(float(season_number)) if season_number not in (None, '') else None
    except Exception:
        season_number = None
    if season_number is None:
        season_number, _ = _guess_se_from_source(first, payload)

    tmdb_id = str(_first_nonempty(
        payload.get('parent_series_tmdb_id'), payload.get('series_tmdb_id'), payload.get('parent_tmdb_id'),
        first.get('parent_series_tmdb_id'), first.get('series_tmdb_id'), first.get('parent_tmdb_id'),
        payload.get('tmdb_id'), first.get('tmdb_id'),
    ) or '').strip()

    title = str(_first_nonempty(
        payload.get('title'), payload.get('name'), payload.get('series_title'), payload.get('series_name'),
        first.get('title'), first.get('series_title'), first.get('series_name'),
    ) or '').strip()

    year = str(_first_nonempty(
        payload.get('release_year'), payload.get('year'), first.get('release_year'), first.get('year')
    ) or '').strip()
    if not re.fullmatch(r'(19|20)\d{2}', year):
        year = ''

    return {
        'tmdb_id': tmdb_id,
        'title': title,
        'release_year': year,
        'season_number': season_number,
    }


def _build_season_package_temp_dir_name(
    *,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    ctx = _season_package_context(payload, files)
    title = _safe_115_folder_name(ctx.get('title') or f'共享季包 {str(source_id or "")[:8]}')
    year_part = f" ({ctx['release_year']})" if ctx.get('release_year') else ''
    tmdb_part = f" {{tmdb={ctx['tmdb_id']}}}" if ctx.get('tmdb_id') else ''

    season_number = ctx.get('season_number')
    if season_number is not None:
        season_part = f" - Season {int(season_number):02d}"
    else:
        season_part = ' - Season'

    name = _safe_115_folder_name(f'{title}{year_part}{tmdb_part}{season_part}', fallback=f'共享季包 {str(source_id or "")[:8]}')
    ctx.update({'source_kind': source_kind, 'source_id': source_id})
    return name, ctx


def _prepare_rapid_target_dir_for_source(
    *,
    base_target_cid: str,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """季包秒传先落到待整理下的临时标准剧目录，避免整理阶段把每集当 root item 单独处理。"""
    normalized_kind = _normalize_source_kind(source_kind)
    files = [f for f in (files or []) if isinstance(f, dict)]

    # 单集/电影仍保持原逻辑：直接秒传到待整理根目录。
    if normalized_kind not in ('season_hub', 'logical_season') or len(files) <= 1:
        return {
            'target_cid': str(base_target_cid),
            'base_target_cid': str(base_target_cid),
            'season_package_temp_dir': False,
        }

    folder_name, ctx = _build_season_package_temp_dir_name(
        source_kind=normalized_kind,
        source_id=source_id,
        payload=payload or {},
        files=files,
    )

    try:
        p115 = P115Service.get_client()
        if not p115:
            raise RuntimeError('115 客户端未初始化')

        mk_resp = p115.fs_mkdir(folder_name, str(base_target_cid))
        if not isinstance(mk_resp, dict) or not mk_resp.get('state'):
            raise RuntimeError(str(mk_resp))

        temp_cid = str(
            mk_resp.get('cid')
            or mk_resp.get('file_id')
            or mk_resp.get('id')
            or (mk_resp.get('data') or {}).get('file_id')
            or (mk_resp.get('data') or {}).get('cid')
            or ''
        ).strip()
        if not temp_cid:
            raise RuntimeError(f'创建成功但未返回 CID: {mk_resp}')

        # 给整理扫描补一个权威上下文；目录名已经带 {tmdb=}，这里是双保险。
        try:
            if ctx.get('tmdb_id') and ctx.get('title'):
                P115CacheManager.save_transfer_context(
                    root_name=folder_name,
                    tmdb_id=ctx.get('tmdb_id'),
                    media_type='tv',
                    title=ctx.get('title'),
                    season_number=ctx.get('season_number'),
                    source='shared-permanent-import',
                    source_kind=normalized_kind,
                    source_kinds=[normalized_kind, 'shared_transfer_context'],
                    confidence='high',
                    authority_role='expected',
                    evidence=[f'rapid:{normalized_kind}:{source_id}'],
                )
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 保存季包整理上下文失败：{folder_name} -> {e}")

        logger.info(
            f"  ➜ [共享资源] 季包秒传启用临时接收目录：{folder_name} "
            f"(cid={temp_cid}, files={len(files)})"
        )
        return {
            'target_cid': temp_cid,
            'base_target_cid': str(base_target_cid),
            'season_package_temp_dir': True,
            'folder_name': folder_name,
            'folder_cid': temp_cid,
            'context': ctx,
        }
    except Exception as e:
        # 季包必须落临时目录，不能回退到待整理根目录；否则一季几十集会再次被整理成
        # 根目录单文件，失败时也难以清理已秒传的部分文件。
        logger.warning(
            f"  ➜ [共享资源] 创建季包临时接收目录失败，拒绝本次季包秒传："
            f"source={normalized_kind}:{source_id}, err={e}"
        )
        return {
            'target_cid': str(base_target_cid),
            'base_target_cid': str(base_target_cid),
            'season_package_temp_dir': False,
            'temp_dir_required': True,
            'temp_dir_error': str(e),
        }


def _cleanup_rapid_temp_dir(rapid_target: Dict[str, Any], *, reason: str = '') -> Dict[str, Any]:
    """删除季包秒传临时接收目录。

    完结季/公共季包必须全量秒传成功才允许进入整理；只要有一集失败，就把临时目录
    整个删除，避免 8/9 这种半季被批量移动入库。
    """
    if not isinstance(rapid_target, dict) or not rapid_target.get('season_package_temp_dir'):
        return {'ok': True, 'skipped': True}
    folder_cid = str(rapid_target.get('folder_cid') or rapid_target.get('target_cid') or '').strip()
    folder_name = str(rapid_target.get('folder_name') or folder_cid or '').strip()
    if not folder_cid:
        return {'ok': False, 'skipped': True, 'message': '缺少临时目录 CID'}
    try:
        p115 = P115Service.get_client()
        if not p115:
            raise RuntimeError('115 客户端未初始化')
        resp = p115.fs_delete([folder_cid])
        ok = bool(isinstance(resp, dict) and resp.get('state'))
        if ok:
            logger.warning(
                f"  ➜ [共享资源] 已删除季包临时接收目录：{folder_name} "
                f"(cid={folder_cid})，原因：{reason or 'season package aborted'}"
            )
        else:
            logger.warning(
                f"  ➜ [共享资源] 删除季包临时接收目录失败：{folder_name} "
                f"(cid={folder_cid})，resp={resp}"
            )
        return {'ok': ok, 'response': resp, 'folder_cid': folder_cid, 'folder_name': folder_name}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 删除季包临时接收目录异常：{folder_name} (cid={folder_cid}) -> {e}")
        return {'ok': False, 'error': str(e), 'folder_cid': folder_cid, 'folder_name': folder_name}


def _event_transfer_lease_id(payload: Dict[str, Any] = None, event: Dict[str, Any] = None) -> str:
    payload = payload if isinstance(payload, dict) else {}
    event = event if isinstance(event, dict) else {}
    for value in (
        payload.get('rapid_transfer_lease_id'), payload.get('transfer_lease_id'), payload.get('lease_id'),
        event.get('rapid_transfer_lease_id'), event.get('transfer_lease_id'), event.get('lease_id'),
    ):
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _client_report_transfer(
    client: SharedCenterClient,
    source_kind: str,
    source_id: str,
    result: str,
    *,
    success_count: int = 0,
    total_count: int = 0,
    message: str = '',
    lease_id: str = '',
    transfer_mode: str = '',
    share_channel_id: str = '',
) -> Dict[str, Any]:
    """上报转存结果；新版中心用 lease_id/transfer_mode/share_channel_id 精确释放与结算。"""
    extra = {}
    lease_id = str(lease_id or '').strip()
    transfer_mode = str(transfer_mode or '').strip()
    share_channel_id = str(share_channel_id or '').strip()
    if lease_id:
        extra['lease_id'] = lease_id
    if transfer_mode:
        extra['transfer_mode'] = transfer_mode
    if share_channel_id:
        extra['share_channel_id'] = share_channel_id

    base_kwargs = {
        'success_count': success_count,
        'total_count': total_count,
        'message': message,
    }
    try:
        return client.report_transfer(
            source_kind,
            source_id,
            result,
            **base_kwargs,
            **extra,
        ) or {}
    except TypeError:
        # 旧 SharedCenterClient.report_transfer 不认识 lease_id/transfer_mode/share_channel_id；
        # 中心端可通过 message 中的“115 分享/分享转存”兼容识别 share 模式。
        return client.report_transfer(
            source_kind,
            source_id,
            result,
            **base_kwargs,
        ) or {}




def _pending_transfer_report_queue_path() -> str:
    """本地持久化队列路径。默认落在工作目录 data/ 下，不依赖额外建表。"""
    configured = str(
        os.environ.get('ETK_SHARED_TRANSFER_REPORT_QUEUE')
        or _cfg('CONFIG_OPTION_115_SHARED_TRANSFER_REPORT_QUEUE', 'p115_shared_transfer_report_queue_path', '')
        or ''
    ).strip()
    if configured:
        path = os.path.abspath(os.path.expanduser(configured))
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        return path

    app_cfg = config_manager.APP_CONFIG if isinstance(config_manager.APP_CONFIG, dict) else {}
    for key in ('data_dir', 'DATA_DIR', 'config_dir', 'CONFIG_DIR', 'db_dir', 'DB_DIR'):
        base = str(app_cfg.get(key) or '').strip()
        if not base:
            continue
        try:
            base = os.path.abspath(os.path.expanduser(base))
            os.makedirs(base, exist_ok=True)
            return os.path.join(base, _PENDING_TRANSFER_REPORT_QUEUE_FILE)
        except Exception:
            pass

    for base in (os.path.join(os.getcwd(), 'data'), os.getcwd()):
        try:
            base = os.path.abspath(base)
            os.makedirs(base, exist_ok=True)
            return os.path.join(base, _PENDING_TRANSFER_REPORT_QUEUE_FILE)
        except Exception:
            pass
    return os.path.join('/tmp', _PENDING_TRANSFER_REPORT_QUEUE_FILE)


def _pending_transfer_report_key(payload: Dict[str, Any]) -> str:
    payload = payload if isinstance(payload, dict) else {}
    parts = [
        str(payload.get('source_kind') or ''),
        str(payload.get('source_id') or ''),
        str(payload.get('result') or ''),
        str(payload.get('lease_id') or ''),
        str(payload.get('transfer_mode') or ''),
        str(payload.get('share_channel_id') or ''),
        str(_safe_int(payload.get('success_count'), 0)),
        str(_safe_int(payload.get('total_count'), 0)),
        str(payload.get('message') or '')[:300],
    ]
    return '|'.join(parts)


def _load_pending_transfer_reports_locked() -> List[Dict[str, Any]]:
    path = _pending_transfer_report_queue_path()
    if not os.path.exists(path):
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    if isinstance(item, dict) and isinstance(item.get('payload'), dict):
                        rows.append(item)
                except Exception:
                    continue
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取转存上报重试队列失败：{e}")
        return []
    return rows[-_PENDING_TRANSFER_REPORT_MAX_ITEMS:]


def _write_pending_transfer_reports_locked(rows: List[Dict[str, Any]]) -> None:
    path = _pending_transfer_report_queue_path()
    rows = [r for r in (rows or []) if isinstance(r, dict) and isinstance(r.get('payload'), dict)]
    rows = rows[-_PENDING_TRANSFER_REPORT_MAX_ITEMS:]
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    if not rows:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
        return
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        for item in rows:
            f.write(json.dumps(item, ensure_ascii=False, default=str) + '\n')
    os.replace(tmp_path, path)


def _pending_transfer_report_next_delay(retry_count: int) -> int:
    retry_count = max(0, int(retry_count or 0))
    delay = _PENDING_TRANSFER_REPORT_RETRY_BASE_SECONDS * (2 ** min(retry_count, 6))
    return max(_PENDING_TRANSFER_REPORT_RETRY_BASE_SECONDS, min(delay, _PENDING_TRANSFER_REPORT_RETRY_MAX_SECONDS))


def _enqueue_pending_transfer_report(payload: Dict[str, Any], error: str = '') -> Dict[str, Any]:
    payload = dict(payload or {})
    key = _pending_transfer_report_key(payload)
    now = time.time()
    with _PENDING_TRANSFER_REPORT_LOCK:
        rows = _load_pending_transfer_reports_locked()
        found = None
        for item in rows:
            if item.get('key') == key:
                found = item
                break
        if found is None:
            found = {
                'key': key,
                'payload': payload,
                'created_at': now,
                'retry_count': 0,
            }
            rows.append(found)
        else:
            found['payload'] = payload
        found['updated_at'] = now
        found['next_retry_at'] = min(float(found.get('next_retry_at') or (now + 15)), now + 15)
        found['last_error'] = str(error or '')[:1000]
        _write_pending_transfer_reports_locked(rows)
    logger.warning(
        f"  ➜ [共享资源] 转存结果上报失败，已写入本地重试队列："
        f"{payload.get('source_kind')}:{payload.get('source_id')}，result={payload.get('result')}，err={str(error)[:180]}"
    )
    return {'queued': True, 'queue_key': key, 'queue_path': _pending_transfer_report_queue_path()}


def _client_report_transfer_with_retry_queue(
    client: SharedCenterClient,
    source_kind: str,
    source_id: str,
    result: str,
    *,
    success_count: int = 0,
    total_count: int = 0,
    message: str = '',
    lease_id: str = '',
    transfer_mode: str = '',
    share_channel_id: str = '',
) -> Dict[str, Any]:
    payload = {
        'source_kind': _normalize_source_kind(source_kind),
        'source_id': str(source_id or '').strip(),
        'result': str(result or '').strip(),
        'success_count': max(0, _safe_int(success_count, 0)),
        'total_count': max(0, _safe_int(total_count, 0)),
        'message': str(message or '')[:1000],
        'lease_id': str(lease_id or '').strip(),
        'transfer_mode': str(transfer_mode or '').strip(),
        'share_channel_id': str(share_channel_id or '').strip(),
    }
    try:
        return _client_report_transfer(
            client,
            payload['source_kind'],
            payload['source_id'],
            payload['result'],
            success_count=payload['success_count'],
            total_count=payload['total_count'],
            message=payload['message'],
            lease_id=payload['lease_id'],
            transfer_mode=payload['transfer_mode'],
            share_channel_id=payload['share_channel_id'],
        ) or {}
    except Exception as e:
        queued = _enqueue_pending_transfer_report(payload, error=str(e))
        return {'ok': False, 'pending_report_queued': True, 'error': str(e), **queued}


def _drain_pending_transfer_reports(client: SharedCenterClient = None, *, limit: int = None, force: bool = False) -> Dict[str, Any]:
    """重放上次因网络/TLS 抖动失败的 /transfers/report。"""
    if not shared_center_enabled():
        return {'ok': False, 'skipped': True, 'reason': 'shared_center_disabled'}
    if not _PENDING_TRANSFER_REPORT_DRAIN_LOCK.acquire(blocking=False):
        return {'ok': True, 'skipped': True, 'reason': 'drain_already_running'}
    try:
        limit = max(1, int(limit or _PENDING_TRANSFER_REPORT_DEFAULT_DRAIN_LIMIT))
        now = time.time()
        with _PENDING_TRANSFER_REPORT_LOCK:
            rows = _load_pending_transfer_reports_locked()
            if not rows:
                return {'ok': True, 'checked': 0, 'sent': 0, 'remaining': 0}
            retry_client = client or SharedCenterClient()
            due: List[Dict[str, Any]] = []
            remaining: List[Dict[str, Any]] = []
            for item in rows:
                if len(due) < limit and (force or float(item.get('next_retry_at') or 0) <= now):
                    due.append(item)
                else:
                    remaining.append(item)
            if not due:
                return {'ok': True, 'checked': len(rows), 'sent': 0, 'remaining': len(rows)}

            sent = 0
            failed = 0
            for item in due:
                payload = item.get('payload') if isinstance(item.get('payload'), dict) else {}
                try:
                    _client_report_transfer(
                        retry_client,
                        payload.get('source_kind'),
                        payload.get('source_id'),
                        payload.get('result'),
                        success_count=payload.get('success_count') or 0,
                        total_count=payload.get('total_count') or 0,
                        message=payload.get('message') or '',
                        lease_id=payload.get('lease_id') or '',
                        transfer_mode=payload.get('transfer_mode') or '',
                        share_channel_id=payload.get('share_channel_id') or '',
                    )
                    sent += 1
                except Exception as e:
                    retry_count = _safe_int(item.get('retry_count'), 0) + 1
                    item['retry_count'] = retry_count
                    item['updated_at'] = now
                    item['next_retry_at'] = now + _pending_transfer_report_next_delay(retry_count)
                    item['last_error'] = str(e)[:1000]
                    remaining.append(item)
                    failed += 1
            _write_pending_transfer_reports_locked(remaining)
        if sent:
            logger.info(f"  ➜ [共享资源] 已补报历史转存结果：成功 {sent} 条，失败待重试 {failed} 条")
        elif failed:
            logger.debug(f"  ➜ [共享资源] 历史转存结果补报仍失败：{failed} 条待重试")
        return {'ok': True, 'checked': len(due), 'sent': sent, 'failed': failed, 'remaining': len(remaining)}
    finally:
        _PENDING_TRANSFER_REPORT_DRAIN_LOCK.release()

def _report_transfer_failed_safely(
    client: SharedCenterClient,
    *,
    source_kind: str,
    source_id: str,
    files: List[Dict[str, Any]],
    errors: List[Any],
    message: str = '',
    lease_id: str = '',
) -> Dict[str, Any]:
    fail_kind = _normalize_source_kind(source_kind)
    if fail_kind not in ('movie', 'episode', 'logical_episode', 'logical_season') or not source_id:
        return {'ok': False, 'skipped': True, 'reason': 'unsupported_source_kind'}
    resp = _client_report_transfer_with_retry_queue(
        client,
        fail_kind,
        source_id,
        'failed',
        success_count=0,
        total_count=len(files or []),
        message=(message or json.dumps(errors or [], ensure_ascii=False))[:1000],
        lease_id=lease_id,
    ) or {}
    if resp.get('pending_report_queued'):
        logger.debug(f"  ➜ [共享资源] 秒传失败结果已加入补报队列：{fail_kind}:{source_id} -> {resp.get('error')}")
    return resp


def _rapid_success(resp: Any) -> bool:
    if isinstance(resp, dict):
        if resp.get('state') is True or resp.get('success') is True:
            return True
        code = str(resp.get('errno') if resp.get('errno') is not None else resp.get('code') if resp.get('code') is not None else '')
        if code in ('0', '200'):
            return True
        data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
        status = str(data.get('status') if data.get('status') is not None else resp.get('status') if resp.get('status') is not None else '')
        if status in ('2', 'success', 'done'):
            return True
    text = json.dumps(resp, ensure_ascii=False, default=str).lower() if isinstance(resp, (dict, list)) else str(resp or '').lower()
    return any(k in text for k in ('成功', '已存在', 'already', 'exist', 'success', '秒传成功'))


def _call_rapid_method(p115, *, target_cid: str, sha1: str, size: int, file_name: str, pick_code: str = '', rapid_meta: Dict[str, Any] = None):
    """适配不同 115 客户端的秒传方法名。CK 只在本机使用，不上传中心。"""
    rapid_meta = dict(rapid_meta or {})
    candidates = [
        ('rapid_upload', ({'cid': target_cid, 'target_cid': target_cid, 'sha1': sha1, 'size': size, 'file_size': size, 'file_name': file_name, 'preid': rapid_meta.get('preid') or rapid_meta.get('pre_sha1') or '', **rapid_meta},)),
        ('upload_file_by_sha1', (target_cid, sha1, size, file_name)),
        ('fs_rapid_upload', (target_cid, sha1, size, file_name)),
        ('fs_upload_by_sha1', (target_cid, sha1, size, file_name)),
        ('upload_by_sha1', (target_cid, sha1, size, file_name)),
        ('add_file_by_sha1', (target_cid, sha1, size, file_name)),
        ('rapid_save', (target_cid, sha1, size, file_name)),
    ]
    last_error = None
    for method_name, args in candidates:
        method = getattr(p115, method_name, None)
        if not callable(method):
            continue
        try:
            return method(*args)
        except TypeError as e:
            last_error = e
            try:
                return method(cid=target_cid, target_cid=target_cid, sha1=sha1, size=size, file_size=size, file_name=file_name, **rapid_meta)
            except Exception as e2:
                last_error = e2
        except Exception as e:
            last_error = e
    if last_error:
        raise RuntimeError(f'调用 115 秒传接口失败：{last_error}')
    raise RuntimeError('当前 P115Service 客户端未提供秒传方法，请补充 rapid_upload/upload_file_by_sha1 等接口')



def _rapid_sign_request_from_response(resp: Any) -> Dict[str, Any]:
    """从 p115_service 的 status=7 响应里提取 sign_key/sign_check。"""
    if not isinstance(resp, dict):
        return {}
    candidates = [resp]
    for key in ('response', 'signed_response'):
        if isinstance(resp.get(key), dict):
            candidates.append(resp.get(key))
    for item in candidates:
        if not isinstance(item, dict):
            continue
        sign_key = item.get('_rapid_sign_key') or item.get('sign_key')
        sign_check = item.get('_rapid_sign_check') or item.get('sign_check')
        data = item.get('data') if isinstance(item.get('data'), dict) else {}
        sign_key = sign_key or data.get('sign_key')
        sign_check = sign_check or data.get('sign_check')
        if sign_key and sign_check:
            return {
                'sign_key': str(sign_key),
                'sign_check': str(sign_check),
                'backend': str(item.get('_rapid_sign_backend') or item.get('_rapid_upload_backend') or item.get('backend') or ''),
                'stage': str(item.get('_rapid_sign_stage') or ''),
                'required': bool(item.get('_rapid_sign_required', True)),
            }
    return {}


def _rapid_sign_job_status_text(status: str) -> str:
    status = str(status or '').strip()
    return {
        'pending': '等待中心分配签名设备',
        'claimed': '签名设备已领取但未回传',
        'done': '签名已完成',
        'failed': '签名设备执行失败',
        'expired': '签名任务已超时',
        'cancelled': '签名任务已取消',
    }.get(status, status or '未知状态')


def _rapid_sign_job_failure_reason(wait_resp: Any, *, job_id: str, holder_id: str = '', elapsed: float = 0) -> Tuple[str, str, bool]:
    resp = wait_resp if isinstance(wait_resp, dict) else {}
    job_obj = resp.get('job') if isinstance(resp.get('job'), dict) else {}
    status = str(resp.get('status') or job_obj.get('status') or '').strip()
    result_meta = job_obj.get('result_meta_json') if isinstance(job_obj.get('result_meta_json'), dict) else {}
    message = str(job_obj.get('message') or resp.get('message') or result_meta.get('message') or '').strip()
    abort_transfer = bool(resp.get('abort_transfer') or result_meta.get('abort_transfer'))
    holder = str(job_obj.get('claimed_by') or job_obj.get('holder_id') or holder_id or '').strip()
    elapsed_text = f"{int(elapsed)}s" if elapsed and elapsed > 0 else ''

    if message:
        reason = message
    elif status == 'claimed':
        reason = '源设备已领取签名任务，但等待超时仍未回传签名，可能是源设备离线、任务卡住或源文件读取失败'
    elif status == 'pending':
        reason = '中心端暂未找到可用签名设备'
    elif status == 'failed':
        reason = '源设备签名失败'
    elif status == 'expired':
        reason = '中心端等待签名超时'
    elif status == 'cancelled':
        reason = '中心端取消了签名任务'
    else:
        reason = '中心端未返回可用签名'

    parts = [f"任务={job_id}", f"状态={_rapid_sign_job_status_text(status)}"]
    if holder:
        parts.append(f"签名设备={holder}")
    if elapsed_text:
        parts.append(f"等待={elapsed_text}")
    parts.append(f"原因={reason}")
    if abort_transfer:
        parts.append("本次转存已中止")
    return '，'.join(parts), reason, abort_transfer


def _retry_rapid_with_center_sign(*, client: SharedCenterClient, p115, file_info: Dict[str, Any], target_cid: str, sha1: str, size: int, file_name: str, rapid_meta: Dict[str, Any], first_resp: Any) -> Dict[str, Any]:
    sign_req = _rapid_sign_request_from_response(first_resp)
    if not sign_req:
        return {'ok': False, 'response': first_resp, 'skipped': True, 'message': '未发现 sign_key/sign_check'}
    source_kind = str(file_info.get('source_kind') or rapid_meta.get('source_kind') or '').strip()
    source_id = str(file_info.get('source_id') or file_info.get('source_ref_id') or rapid_meta.get('source_id') or rapid_meta.get('source_ref_id') or '').strip()
    if not source_kind or not source_id:
        logger.warning(
            f"  ➜ [负载均衡签名] 秒传需要签名但缺少 source_kind/source_id，无法向中心创建 sign_job: "
            f"sha1={sha1[:12]}..., file={file_name}"
        )
        return {'ok': False, 'response': first_resp, 'skipped': True, 'message': '缺少 source_kind/source_id'}

    logger.warning(
        f"  ➜ [负载均衡签名] 秒传返回 status=7，准备请求中心调度在线 holder："
        f"source={source_kind}:{source_id}, sha1={sha1[:12]}..., backend={sign_req.get('backend') or '-'}, "
        f"sign_check={sign_req.get('sign_check')}"
    )
    create_resp = client.create_rapid_sign_job({
        'source_kind': source_kind,
        'source_id': source_id,
        'sha1': sha1,
        'size': size,
        'file_name': file_name,
        'preid': rapid_meta.get('preid') or file_info.get('preid') or '',
        'backend': sign_req.get('backend') or '',
        'sign_key': sign_req.get('sign_key'),
        'sign_check': sign_req.get('sign_check'),
        'request_meta_json': {
            'stage': sign_req.get('stage') or '',
            'target_cid': target_cid,
            'rapid_transfer_token': str(file_info.get('_rapid_transfer_token') or rapid_meta.get('_rapid_transfer_token') or ''),
            'package_transfer': bool(file_info.get('_rapid_is_package_transfer') or rapid_meta.get('_rapid_is_package_transfer')),
        },
    })
    job_id = str(create_resp.get('job_id') or (create_resp.get('job') or {}).get('job_id') or '').strip()
    holder_id = str(create_resp.get('holder_id') or (create_resp.get('job') or {}).get('holder_id') or '').strip()
    if not job_id:
        raise RuntimeError(f'中心未返回 sign_job id: {create_resp}')
    logger.info(f"  ➜ [负载均衡签名] 签名任务已创建：等待源客户端签名...")
    wait_started_at = time.time()
    wait_resp = client.wait_rapid_sign_job(job_id, timeout=75)
    wait_elapsed = time.time() - wait_started_at
    status = str(wait_resp.get('status') or (wait_resp.get('job') or {}).get('status') or '')
    sign_val = str(wait_resp.get('sign_val') or (wait_resp.get('job') or {}).get('sign_val') or '').strip().upper()
    if status != 'done' or not _norm_sha1(sign_val):
        job_obj = (wait_resp.get('job') or {}) if isinstance(wait_resp, dict) else {}
        job_message = str(job_obj.get('message') or wait_resp.get('message') or '') if isinstance(wait_resp, dict) else ''
        result_meta = job_obj.get('result_meta_json') if isinstance(job_obj.get('result_meta_json'), dict) else {}
        abort_transfer = bool(wait_resp.get('abort_transfer') or result_meta.get('abort_transfer'))
        no_retry = status in ('failed', 'expired', 'cancelled') or abort_transfer or any(
            x in job_message for x in ('所有 holder', '无可用 holder', 'no rapid sign holder available', 'holder 未领取签名任务', '资源签名不可用')
        )
        human_detail, human_reason, human_abort = _rapid_sign_job_failure_reason(
            wait_resp,
            job_id=job_id,
            holder_id=holder_id,
            elapsed=wait_elapsed,
        )
        abort_transfer = abort_transfer or human_abort
        logger.warning(f"  ➜ [负载均衡签名] 签名失败：{file_name}，{human_detail}")
        logger.debug(f"  ➜ [负载均衡签名] sign_job 原始响应：job_id={job_id}, status={status}, no_retry={no_retry}, abort={abort_transfer}, resp={str(wait_resp)[:500]}")
        return {
            'ok': False,
            'response': first_resp,
            'sign_job': wait_resp,
            'message': human_reason or job_message or wait_resp.get('message') or f'sign_job 未完成: {status}',
            'no_retry': bool(no_retry),
            'abort_transfer': bool(abort_transfer or status in ('failed', 'expired', 'cancelled')),
        }

    signed_meta = dict(rapid_meta or {})
    signed_meta['sign_key'] = sign_req.get('sign_key')
    signed_meta['sign_val'] = sign_val
    logger.info(
        f"  ➜ [负载均衡签名] 已收到签名，开始秒传："
        f"{file_name}"
    )
    signed_resp = _call_rapid_method(
        p115,
        target_cid=target_cid,
        sha1=sha1,
        size=size,
        file_name=file_name,
        pick_code=str(file_info.get('pick_code') or ''),
        rapid_meta=signed_meta,
    )
    ok = _rapid_success(signed_resp)
    logger.trace(
        f"  ➜ [负载均衡签名] 带中心 sign_val 重试完成：ok={ok}, "
        f"source={source_kind}:{source_id}, sha1={sha1[:12]}..."
    )
    return {'ok': ok, 'response': signed_resp, 'sign_job': wait_resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}


def _remember_rapid_preid_hint(
    file_info: Dict[str, Any],
    *,
    target_cid: str,
    sha1: str,
    size: int,
    file_name: str,
    rapid_meta: Dict[str, Any] = None,
    response: Any = None,
) -> str:
    """共享秒传成功后，把中心已知 preid 喂给整理缓存链路。"""
    rapid_meta = dict(rapid_meta or {})
    preid = _norm_sha1(
        (file_info or {}).get('preid')
        or rapid_meta.get('preid')
        or rapid_meta.get('pre_sha1')
        or rapid_meta.get('pre_sha1_128k')
    )
    if not preid:
        return ''

    hint_payload = {
        'sha1': sha1,
        'preid': preid,
        'file_name': file_name,
        'name': file_name,
        'size': size,
        'file_size': size,
        'parent_id': str(target_cid or ''),
        'target_cid': str(target_cid or ''),
        'source_kind': (file_info or {}).get('source_kind') or rapid_meta.get('source_kind') or '',
        'source_id': (file_info or {}).get('source_id') or (file_info or {}).get('source_ref_id') or rapid_meta.get('source_id') or '',
    }

    if isinstance(response, dict):
        for key in ('fid', 'file_id', 'id', 'pick_code', 'pickcode', 'pc'):
            value = response.get(key)
            if value not in (None, '', [], {}):
                hint_payload[key] = value
        data = response.get('data') if isinstance(response.get('data'), dict) else {}
        for key in ('fid', 'file_id', 'id', 'pick_code', 'pickcode', 'pc'):
            value = data.get(key)
            if value not in (None, '', [], {}) and key not in hint_payload:
                hint_payload[key] = value

    try:
        cached_preid = P115CacheManager.register_preid_hint(
            hint_payload,
            sha1=sha1,
            preid=preid,
            parent_id=str(target_cid or ''),
            file_name=file_name,
            size=size,
            source='shared_rapid_transfer',
        )
        if cached_preid:
            logger.debug(
                f"  ➜ [共享资源] 已缓存共享秒传 preid 提示："
                f"{file_name}, sha1={sha1[:12]}..., preid={cached_preid[:12]}..., target_cid={target_cid}"
            )
        return cached_preid or ''
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 缓存共享秒传 preid 提示失败：{file_name} -> {e}")
        return ''


def _remember_share_preid_hints(
    files: List[Dict[str, Any]],
    *,
    target_cid: str,
    source_kind: str = 'logical_season',
    source_id: str = '',
    response: Any = None,
) -> int:
    """分享转存成功后，把中心 manifest 里的 preid 预登记给整理扫描。

    115 分享转存不会返回每个文件的 preid，后续待整理扫描从 /files 也拿不到 preid；
    但中心 manifest 已经有 sha1/size/preid。这里复用 Rapid 的 preid hint 机制，
    让 P115CacheManager.save_file_cache / ensure_file_preid 在扫描新文件时直接命中，
    避免再取直链 Range 0-131071 在线计算。
    """
    count = 0
    target_cid = str(target_cid or '').strip()
    for item in files or []:
        if not isinstance(item, dict):
            continue
        info = _normalize_rapid_file_info(item)
        meta = info.get('rapid_meta_json') if isinstance(info.get('rapid_meta_json'), dict) else {}
        sha1 = _norm_sha1(info.get('sha1') or meta.get('sha1'))
        preid = _norm_sha1(info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
        if not sha1 or not preid:
            continue
        file_name = str(info.get('file_name') or info.get('name') or meta.get('file_name') or meta.get('name') or sha1).strip()
        size = _rapid_size_to_int(info.get('size') or info.get('file_size') or meta.get('size'), 0)
        hint_meta = dict(meta or {})
        hint_meta.setdefault('preid', preid)
        hint_meta.setdefault('source_kind', source_kind or info.get('source_kind') or '')
        hint_meta.setdefault('source_id', source_id or info.get('source_id') or info.get('source_ref_id') or '')
        if _remember_rapid_preid_hint(
            info,
            target_cid=target_cid,
            sha1=sha1,
            size=size,
            file_name=file_name,
            rapid_meta=hint_meta,
            response=response,
        ):
            count += 1
    if count:
        logger.info(
            f"  ➜ [共享资源] 分享转存已预登记中心 preid 提示："
            f"{count}/{len(files or [])}，target_cid={target_cid or '-'}"
        )
    return count


def _json_obj(value) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _chunks(values: List[Any], size: int):
    size = max(1, int(size or 1))
    for i in range(0, len(values or []), size):
        yield values[i:i + size]


def _extract_raw_items_from_batch_response(resp: Any) -> List[Dict[str, Any]]:
    if not isinstance(resp, dict):
        return []
    items = resp.get('items') or resp.get('data') or resp.get('results') or []
    return [x for x in items if isinstance(x, dict)] if isinstance(items, list) else []


def _decode_zstd_b64_raw(value: Any) -> Dict[str, Any]:
    text = str(value or '').strip()
    if not text:
        return {}
    try:
        compressed = base64.b64decode(text)
    except Exception:
        return {}
    try:
        import zstandard as zstd
        raw_bytes = zstd.ZstdDecompressor().decompress(compressed)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 解压中心 RAW zstd 失败，准备降级单条 RAW：{e}")
        return {}
    try:
        raw = json.loads(raw_bytes.decode('utf-8'))
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 解析中心 RAW JSON 失败，准备降级单条 RAW：{e}")
        return {}


def _raw_from_batch_item(item: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    raw = item.get('raw_ffprobe_json') or item.get('raw_json') or item.get('raw') or {}
    if isinstance(raw, dict) and raw:
        return raw
    return _decode_zstd_b64_raw(item.get('raw_zstd_b64') or item.get('raw_zstd_base64') or item.get('raw_compressed_b64'))


def _call_center_raw_batch(client: SharedCenterClient, sha1s: List[str]) -> Dict[str, Dict[str, Any]]:
    """走中心批量 RAW 拉取，但默认请求 zstd_base64，避免中心端巨型 JSON 负优化。"""
    sha1s = [x for x in (sha1s or []) if _norm_sha1(x)]
    if not sha1s:
        return {}

    method = None
    for name in ('get_raw_ffprobe_batch', 'fetch_raw_ffprobe_batch', 'get_raw_batch'):
        candidate = getattr(client, name, None)
        if callable(candidate):
            method = candidate
            break
    if not method:
        return {}

    def fetch_chunk(chunk: List[str]) -> Dict[str, Dict[str, Any]]:
        chunk_out: Dict[str, Dict[str, Any]] = {}
        try:
            try:
                resp = method(chunk, return_compressed=True)
            except TypeError:
                try:
                    resp = method({'sha1_list': chunk, 'return_compressed': True})
                except TypeError:
                    resp = method(chunk)
            for item in _extract_raw_items_from_batch_response(resp):
                sha1 = _norm_sha1(item.get('sha1'))
                raw = _raw_from_batch_item(item)
                if sha1 and isinstance(raw, dict) and raw:
                    chunk_out[sha1] = raw
        except Exception as e:
            logger.debug(
                f"  ➜ [共享资源] 批量拉取中心 RAW 失败，降级单条："
                f"batch={len(chunk)}, err={e}"
            )
        return chunk_out

    # 压缩 RAW 批量可以适当放大，但仍分片，避免一季几百/上千集形成超大响应。
    chunks = list(_chunks(sha1s, 48))
    out: Dict[str, Dict[str, Any]] = {}
    if len(chunks) == 1:
        return fetch_chunk(chunks[0])

    workers = min(4, len(chunks))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='center-raw-batch') as executor:
        futures = [executor.submit(fetch_chunk, chunk) for chunk in chunks]
        for future in concurrent.futures.as_completed(futures):
            try:
                out.update(future.result() or {})
            except Exception:
                pass
    return out


def _fetch_single_center_raw(client: SharedCenterClient, sha1: str) -> Tuple[str, Dict[str, Any]]:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return '', {}
    try:
        resp = client.get_raw_ffprobe(sha1)
        raw = (resp or {}).get('raw_ffprobe_json') or (resp or {}).get('raw') or {}
        if isinstance(raw, dict) and raw:
            return sha1, raw
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 拉取中心 RAW 失败: sha1={sha1[:12]}..., err={e}")
    return sha1, {}


def _call_center_raw_single_parallel(client: SharedCenterClient, sha1s: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量缺失/旧中心兜底：并发单条拉取，避免回到纯串行。"""
    sha1s = [x for x in (sha1s or []) if _norm_sha1(x)]
    if not sha1s:
        return {}
    if len(sha1s) == 1:
        sha1, raw = _fetch_single_center_raw(client, sha1s[0])
        return {sha1: raw} if sha1 and raw else {}

    out: Dict[str, Dict[str, Any]] = {}
    workers = min(8, len(sha1s))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='center-raw-get') as executor:
        futures = [executor.submit(_fetch_single_center_raw, client, sha1) for sha1 in sha1s]
        for future in concurrent.futures.as_completed(futures):
            try:
                sha1, raw = future.result()
                if sha1 and isinstance(raw, dict) and raw:
                    out[sha1] = raw
            except Exception:
                pass
    return out


def _load_center_raw_map(client: SharedCenterClient, files: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """转存/秒传前把中心 RAW 拉到本地，用于洗版预检和本地 MediaInfo 缓存。"""
    raw_map: Dict[str, Dict[str, Any]] = {}
    missing = []
    for item in files or []:
        sha1 = _norm_sha1((item or {}).get('sha1'))
        if not sha1:
            continue
        raw = (item or {}).get('raw_ffprobe_json') or (item or {}).get('raw_json') or (item or {}).get('raw')
        if isinstance(raw, dict) and raw:
            raw_map[sha1] = raw
            continue
        if sha1 not in missing:
            missing.append(sha1)

    if missing:
        batch_started = time.time()
        batch_map = _call_center_raw_batch(client, missing)
        if batch_map:
            raw_map.update(batch_map)
        logger.debug(
            f"  ➜ [共享资源] 批量拉取中心 RAW(zstd)：命中 {len(batch_map)}/{len(missing)}，"
            f"耗时 {time.time() - batch_started:.1f}s"
        )

    # 批量未命中的再并发单条兜底：旧中心、旧 client、缺 zstandard 依赖都不会退回纯串行。
    remain = [sha1 for sha1 in missing if sha1 not in raw_map]
    if remain:
        single_started = time.time()
        single_map = _call_center_raw_single_parallel(client, remain)
        if single_map:
            raw_map.update(single_map)
        logger.info(
            f"  ➜ [共享资源] 并发单条拉取中心 RAW：命中 {len(single_map)}/{len(remain)}，"
            f"耗时 {time.time() - single_started:.1f}s"
        )
    return raw_map


def _cache_center_raw_as_local_mediainfo(file_info: Dict[str, Any], raw: Dict[str, Any]) -> bool:
    sha1 = _norm_sha1((file_info or {}).get('sha1'))
    if not sha1 or not isinstance(raw, dict) or not raw:
        return False
    file_node = {
        'fn': (file_info or {}).get('file_name') or (file_info or {}).get('name') or sha1,
        'file_name': (file_info or {}).get('file_name') or (file_info or {}).get('name') or sha1,
        'sha1': sha1,
        'fs': _rapid_size_to_int((file_info or {}).get('size') or (file_info or {}).get('file_size'), 0),
        'size': _rapid_size_to_int((file_info or {}).get('size') or (file_info or {}).get('file_size'), 0),
    }
    try:
        builder = _MediainfoBuilder()
        emby_obj = builder._build_emby_mediainfo_from_ffprobe(raw, file_node, sha1=sha1)
        if not emby_obj:
            return False
        P115CacheManager.save_mediainfo_cache(sha1, emby_obj, raw)
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 中心 RAW 转本地 MediaInfo 失败: {file_node.get('file_name')} -> {e}")
        return False


def _guess_se_from_source(src: Dict[str, Any], context: Dict[str, Any] = None):
    context = context or {}
    s_num = (src or {}).get('season_number') if (src or {}).get('season_number') not in (None, '') else context.get('season_number')
    e_num = (src or {}).get('episode_number') if (src or {}).get('episode_number') not in (None, '') else context.get('episode_number')
    try:
        s_num = int(float(s_num)) if s_num not in (None, '') else None
    except Exception:
        s_num = None
    try:
        e_num = int(float(e_num)) if e_num not in (None, '') else None
    except Exception:
        e_num = None
    if s_num is None or e_num is None:
        name = str((src or {}).get('file_name') or (src or {}).get('name') or '')
        m = re.search(r'[Ss](\d{1,3})[. _-]*[Ee](\d{1,4})', name)
        if m:
            if s_num is None:
                s_num = int(m.group(1))
            if e_num is None:
                e_num = int(m.group(2))
    return s_num, e_num


def _source_parent_series_tmdb_id(src: Dict[str, Any], context: Dict[str, Any] = None) -> str:
    context = context or {}
    for value in (
        context.get('parent_series_tmdb_id'), context.get('parent_tmdb_id'),
        (src or {}).get('parent_series_tmdb_id'), (src or {}).get('series_tmdb_id'), (src or {}).get('parent_tmdb_id'),
        context.get('tmdb_id'), (src or {}).get('tmdb_id'),
    ):
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _washing_new_level(
    sha1: str,
    file_name: str,
    file_size: int,
    target_cid: str,
    media_type: str,
    original_lang: str = '',
    has_external_subtitle: bool = False,
    tmdb_id: str = '',
    season_num=None,
    episode_num=None,
):
    try:
        from handler.resubscribe_service import WashingService
        raw_info = WashingService._get_raw_info_by_sha1(sha1)
        if isinstance(raw_info, list) and raw_info:
            new_info = dict(raw_info[0])
        elif isinstance(raw_info, dict):
            new_info = dict(raw_info)
        else:
            return 999, '无法读取本地 MediaInfo'
        new_info['filename'] = file_name
        new_info['_file_size'] = file_size
        new_info['_original_lang'] = original_lang
        new_info['has_external_subtitle'] = has_external_subtitle
        new_info['_media_type'] = media_type
        new_info['_tmdb_id'] = tmdb_id
        new_info['_season_num'] = season_num
        new_info['_episode_num'] = episode_num
        db_media_type = 'Movie' if str(media_type).lower() == 'movie' else 'Series'
        priorities = WashingService._load_priorities(db_media_type, target_cid)
        if not priorities:
            return 999, '未配置优先级规则'
        new_info['_need_clean_version_check'] = WashingService._priorities_need_clean_version(priorities)
        norm_new = WashingService._normalize_info(new_info)
        return WashingService.get_level(norm_new, priorities)
    except Exception as e:
        return 999, f'读取洗版优先级失败: {e}'


def _raw_quality_score(src: Dict[str, Any], raw: Dict[str, Any]) -> int:
    text = f"{(src or {}).get('file_name') or ''} {json.dumps(raw or {}, ensure_ascii=False)[:4000]}".upper()
    score = 0
    if '2160' in text or '3840' in text or '4K' in text:
        score += 40
    elif '1080' in text or '1920' in text:
        score += 20
    elif '720' in text:
        score += 10
    if 'REMUX' in text:
        score += 30
    elif 'WEB-DL' in text or 'WEBDL' in text:
        score += 18
    elif 'WEBRIP' in text:
        score += 10
    if 'DOLBY' in text or 'DOVI' in text or re.search(r'\bDV\b', text):
        score += 12
    elif 'HDR10+' in text:
        score += 10
    elif 'HDR10' in text or 'HDR' in text:
        score += 6
    if 'HEVC' in text or 'H.265' in text or 'H265' in text:
        score += 5
    size_gb = (_rapid_size_to_int((src or {}).get('size'), 0) or 0) / 1024 / 1024 / 1024
    score += min(int(size_gb), 30)
    return score


def _block_clean_version_transfer_enabled() -> bool:
    try:
        return bool((settings_db.get_shared_resource_config() or {}).get('p115_shared_block_clean_version_transfer', False))
    except Exception:
        return False


def _episode_transfer_disabled_enabled() -> bool:
    """读取“禁用单集共享秒传”开关。

    优先使用 shared_resource_config，兼容后续如果把同名 key 放进 APP_CONFIG。
    启用后只跳过 episode / season_hub 这类按集消费的共享源，
    不影响 movie 和 completed_season 完结季包。
    """
    key = 'p115_shared_disable_episode_transfer'
    try:
        shared_cfg = settings_db.get_shared_resource_config() or {}
        if key in shared_cfg:
            return _boolish_local(shared_cfg.get(key), False)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取单集秒传禁用开关失败，降级 APP_CONFIG：{e}")
    return _boolish_local(_cfg('CONFIG_OPTION_115_SHARED_DISABLE_EPISODE_TRANSFER', key, False), False)


def _episode_transfer_disabled_guard(source_kind: str, source_id: str = '', payload: Dict[str, Any] = None) -> Dict[str, Any]:
    """启用 p115_shared_disable_episode_transfer 时，跳过单集/连载集消费。

    season_hub 是公共连载季壳，但实际消费的是 episode 源；因此也归为
    “单集秒传”并跳过。completed_season 是整季收藏包，不在此处拦截。
    """
    if not _episode_transfer_disabled_enabled():
        return {'blocked': False}
    payload = payload if isinstance(payload, dict) else {}
    normalized_kind = _normalize_source_kind(source_kind)
    if normalized_kind == 'completed_season' and payload.get('hub_id') and not payload.get('source_id'):
        normalized_kind = 'season_hub'
    if not normalized_kind and payload.get('hub_id'):
        normalized_kind = 'season_hub'
    if normalized_kind not in ('episode', 'logical_episode', 'season_hub'):
        return {'blocked': False, 'source_kind': normalized_kind}
    sid = str(source_id or payload.get('source_id') or payload.get('source_ref_id') or payload.get('hub_id') or payload.get('id') or '').strip()
    title = str(payload.get('title') or payload.get('name') or payload.get('file_name') or sid or '').strip()
    message = f"跳过单集秒传：{title or sid or normalized_kind}"
    return {
        'blocked': True,
        'source_kind': normalized_kind,
        'source_id': sid,
        'message': message,
    }


def _event_episode_transfer_disabled_guard(event: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    event_kind = _normalize_source_kind(
        (event or {}).get('source_kind')
        or payload.get('source_kind')
        or payload.get('kind')
        or payload.get('item_type')
        or payload.get('display_type')
        or ''
    )
    if not event_kind and (payload.get('hub_id') or payload.get('is_season_hub')):
        event_kind = 'season_hub'
    event_id = str(
        (event or {}).get('source_ref_id')
        or payload.get('source_id')
        or payload.get('source_ref_id')
        or payload.get('hub_id')
        or payload.get('id')
        or ''
    ).strip()
    return _episode_transfer_disabled_guard(event_kind, event_id, payload)


def _center_clean_version_flagged(source_kind: str, payload: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    """消费端只信中心端标签，不再根据 RAW/TMDb 现场识别纯净版。"""
    if str(source_kind or '').strip() != 'completed_season':
        return {'blocked': False}
    candidates = [payload] + [f for f in (files or []) if isinstance(f, dict)]
    for item in candidates:
        if not isinstance(item, dict):
            continue
        meta = _json_obj(item.get('clean_version_meta_json') or item.get('clean_version_meta'))
        if bool(item.get('is_clean_version') or meta.get('is_clean_version')):
            return {'blocked': True, 'source': item, 'meta': meta}
    return {'blocked': False}



def _current_organize_conflict_mode(default: str = 'skip') -> str:
    """读取 115 整理覆盖模式，并统一成 skip/replace/keep_both。"""
    try:
        rename_config = settings_db.get_setting('p115_rename_config') or {}
        if isinstance(rename_config, str):
            try:
                rename_config = json.loads(rename_config)
            except Exception:
                rename_config = {}
        mode = str((rename_config or {}).get('conflict_mode') or default or '').strip().lower()
    except Exception:
        mode = str(default or '').strip().lower()

    aliases = {
        'overwrite': 'replace',
        '洗版': 'replace',
        '替换': 'replace',
        'skip_existing': 'skip',
        '跳过': 'skip',
        'keep': 'keep_both',
        'both': 'keep_both',
        'keepboth': 'keep_both',
        '保留两者': 'keep_both',
    }
    mode = aliases.get(mode, mode)
    if mode not in ('skip', 'replace', 'keep_both'):
        return str(default or 'skip').strip().lower() or 'skip'
    return mode

def _preflight_context(source_kind: str, source_id: str, payload: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    first = next((f for f in (files or []) if isinstance(f, dict)), {}) or {}
    return {
        'source_kind': source_kind,
        'source_id': source_id,
        'title': payload.get('title') or first.get('title') or first.get('file_name') or '',
        'tmdb_id': payload.get('tmdb_id') or first.get('tmdb_id') or '',
        'parent_series_tmdb_id': payload.get('parent_series_tmdb_id') or payload.get('series_tmdb_id') or first.get('parent_series_tmdb_id') or first.get('series_tmdb_id') or '',
        'parent_tmdb_id': payload.get('parent_tmdb_id') or payload.get('parent_series_tmdb_id') or first.get('parent_series_tmdb_id') or '',
        'item_type': payload.get('item_type') or first.get('item_type') or '',
        'season_number': payload.get('season_number') if payload.get('season_number') not in (None, '') else first.get('season_number'),
        'episode_number': payload.get('episode_number') if payload.get('episode_number') not in (None, '') else first.get('episode_number'),
        'release_year': payload.get('release_year') or first.get('release_year') or '',
    }


def _prepare_files_before_rapid_transfer(
    client: SharedCenterClient,
    *,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """秒传前预处理：缓存中心 RAW；replace 模式下执行洗版预检。

    纯净版不在这里识别，只根据中心 is_clean_version 标签做策略拦截。
    """
    files = [dict(f or {}) for f in (files or []) if isinstance(f, dict)]
    source_label = f"{source_kind or '-'}:{source_id or '-'}"
    preflight_started_at = time.time()
    logger.debug(f"  ➜ [共享资源] 秒传前预检开始：source={source_label}, files={len(files)}")

    conflict_mode = _current_organize_conflict_mode(default='skip')
    if conflict_mode == 'replace':
        files, inventory_gate = _replace_mode_short_circuit_best_inventory(
            source_kind=source_kind,
            source_id=source_id,
            payload=payload,
            files=files,
        )
        if not files:
            message = inventory_gate.get('message') or '本地库存已是洗版优先级 1，跳过共享秒传。'
            logger.debug(
                f"  ➜ [共享资源] 秒传前预检提前结束：source={source_label}, "
                f"reason={inventory_gate.get('reason') or '-'}，耗时 {time.time() - preflight_started_at:.1f}s"
            )
            return [], {
                'raw_cached_count': 0,
                'raw_cache_errors': [],
                'washing_checked': False,
                'washing_rejected': False,
                'inventory_best_short_circuit': True,
                'errors': [message],
                'message': message,
                'inventory_gate': inventory_gate,
            }

    raw_started_at = time.time()
    logger.info(f"  ➜ [共享资源] 秒传前预检：开始拉取 {len(files)} 条媒体信息")
    raw_map = _load_center_raw_map(client, files)
    logger.debug(
        f"  ➜ [共享资源] 秒传前预检：中心 RAW 拉取完成，"
        f"命中 {len(raw_map)}/{len(files)}，耗时 {time.time() - raw_started_at:.1f}s"
    )

    cached = 0
    cache_errors = []
    for f in files:
        sha1 = _norm_sha1(f.get('sha1'))
        raw = raw_map.get(sha1)
        if not raw:
            continue
        file_name = f.get('file_name') or f.get('name') or sha1
        try:
            if _cache_center_raw_as_local_mediainfo(f, raw):
                cached += 1
            else:
                cache_errors.append(file_name or sha1)
                logger.warning(f"  ➜ [共享资源] 秒传前预检：RAW 转本地 MediaInfo 失败：{file_name}")
        except Exception as e:
            cache_errors.append(file_name or sha1)
            logger.warning(f"  ➜ [共享资源] 秒传前预检：RAW 转本地 MediaInfo 异常：{file_name} -> {e}")
    logger.debug(
        f"  ➜ [共享资源] 秒传前预检：RAW 缓存完成，成功 {cached}/{len(raw_map)}，"
        f"失败 {len(cache_errors)}"
    )

    if conflict_mode != 'replace':
        logger.info(
            f"  ➜ [共享资源] 秒传前预检结束：当前覆盖模式为 {conflict_mode or '未配置'}，"
            f"跳过洗版预检，耗时 {time.time() - preflight_started_at:.1f}s"
        )
        return files, {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': False,
            'message': f'当前覆盖模式为 {conflict_mode or "未配置"}，跳过洗版预检',
        }

    p115 = P115Service.get_client()
    if not p115:
        logger.warning(f"  ➜ [共享资源] 秒传前预检失败：115 客户端未初始化，source={source_label}")
        return [], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': True,
            'errors': ['115 客户端未初始化，无法执行洗版预检'],
        }

    try:
        from handler.resubscribe_service import WashingService
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 秒传前预检失败：导入 WashingService 失败，source={source_label}, err={e}")
        return [], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': True,
            'errors': [f'导入 WashingService 失败，拒绝秒传: {e}'],
        }

    context = _preflight_context(source_kind, source_id, payload, files)
    candidates = []
    errors = []
    hard_reject = False
    is_completed_pack = str(source_kind or '') == 'completed_season'
    is_ongoing_hub = str(source_kind or '') == 'season_hub'
    target_cache: Dict[Tuple[str, str, Any], Dict[str, Any]] = {}

    logger.debug(
        f"  ➜ [共享资源] 洗版预检开始：source={source_label}, files={len(files)}, "
        f"completed_pack={is_completed_pack}, ongoing_hub={is_ongoing_hub}"
    )

    def _reject_completed_pack_now(message: str):
        """完结季包一票否定：任一视频不合格，立刻拒绝整包，不继续预检后续集。"""
        logger.warning(
            f"  ➜ [共享资源] 洗版预检一票否定：source={source_label}, reason={message}，"
            f"耗时 {time.time() - preflight_started_at:.1f}s"
        )
        return [], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': True,
            'errors': errors[:50] or [message],
            'veto_file': message,
        }

    for idx, src in enumerate(files):
        file_name = src.get('file_name') or src.get('name') or _norm_sha1(src.get('sha1'))
        ext = os.path.splitext(str(file_name or ''))[1].lower()
        if ext and ext not in VIDEO_EXTS:
            logger.debug(f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 跳过非视频：{file_name}")
            candidates.append({'file': src, 'score': 0, 'index': idx, 'episode': None, 'reason': 'non_video'})
            continue

        sha1 = _norm_sha1(src.get('sha1'))
        raw = raw_map.get(sha1)
        logger.info(
            f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 准备："
            f"{file_name}"
        )
        if not raw:
            msg = f"{file_name}: 中心缺少 RAW，洗版预检拒绝秒传"
            logger.warning(f"  ➜ [共享资源] {msg}")
            errors.append(msg)
            if is_completed_pack:
                return _reject_completed_pack_now(msg)
            continue
        if file_name in cache_errors or sha1 in cache_errors:
            msg = f"{file_name}: RAW 无法转换为本地 MediaInfo，洗版预检拒绝秒传"
            logger.warning(f"  ➜ [共享资源] {msg}")
            errors.append(msg)
            if is_completed_pack:
                return _reject_completed_pack_now(msg)
            continue

        source_item_type = str(src.get('item_type') or context.get('item_type') or '')
        media_type = 'movie' if source_item_type == 'Movie' else 'tv'
        if media_type == 'movie':
            tmdb_for_washing = str(src.get('tmdb_id') or context.get('tmdb_id') or '')
        else:
            tmdb_for_washing = str(_source_parent_series_tmdb_id(src, context) or '')
        if not tmdb_for_washing:
            msg = f"{file_name}: 缺少 TMDb ID，洗版预检拒绝秒传"
            logger.warning(f"  ➜ [共享资源] {msg}")
            errors.append(msg)
            if is_completed_pack:
                return _reject_completed_pack_now(msg)
            continue

        s_num, e_num = _guess_se_from_source(src, context)
        target_key = (media_type, str(tmdb_for_washing), s_num if media_type == 'tv' else None)
        cached_target = target_cache.get(target_key)
        if cached_target:
            target_cid_for_washing = cached_target.get('target_cid') or ''
            original_lang = cached_target.get('original_lang') or ''
            logger.info(
                f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 复用目标目录："
                f"tmdb={tmdb_for_washing}, season={s_num if s_num is not None else '-'}, target_cid={target_cid_for_washing}"
            )
        else:
            target_started_at = time.time()
            logger.debug(
                f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 计算目标目录："
                f"tmdb={tmdb_for_washing}, media_type={media_type}, season={s_num if s_num is not None else '-'}, file={file_name}"
            )
            try:
                organizer = SmartOrganizer(
                    p115,
                    int(tmdb_for_washing),
                    media_type,
                    context.get('title') or src.get('title') or file_name,
                    None,
                    False,
                )
                if media_type == 'tv' and s_num is not None:
                    organizer.forced_season = int(s_num)
                target_cid_for_washing = organizer.get_target_cid(season_num=s_num if media_type == 'tv' else None)
                original_lang = (organizer.raw_metadata or {}).get('lang_code')
                target_cache[target_key] = {
                    'target_cid': str(target_cid_for_washing),
                    'original_lang': original_lang or '',
                }
                logger.debug(
                    f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 目标目录完成："
                    f"target_cid={target_cid_for_washing}, lang={original_lang or '-'}, "
                    f"耗时 {time.time() - target_started_at:.1f}s"
                )
            except Exception as e:
                msg = f"{file_name}: 无法计算洗版目标目录，拒绝秒传 -> {e}"
                logger.warning(f"  ➜ [共享资源] {msg}")
                errors.append(msg)
                if is_completed_pack:
                    return _reject_completed_pack_now(msg)
                continue

        file_size = _rapid_size_to_int(src.get('size') or src.get('file_size'), 0)
        decision_started_at = time.time()
        logger.debug(
            f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 调用规则："
            f"target_cid={target_cid_for_washing}, tmdb={tmdb_for_washing}, "
            f"S{s_num if s_num is not None else '-'}E{e_num if e_num is not None else '-'}, size={file_size}"
        )
        action, reason = WashingService.decide_washing_action(
            sha1=sha1,
            file_name=file_name,
            file_size=file_size,
            target_cid=str(target_cid_for_washing),
            media_type=media_type,
            tmdb_id=str(tmdb_for_washing),
            season_num=s_num,
            episode_num=e_num,
            original_lang=original_lang,
            is_active_washing=False,
            has_external_subtitle=False,
        )
        logger.debug(
            f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 规则结果："
            f"{file_name} -> {action}，{reason}，耗时 {time.time() - decision_started_at:.1f}s"
        )
        if action in ('REJECT', 'SKIP'):
            msg = f"{file_name}: 洗版预检 [{action}] {reason}"
            errors.append(msg)
            if is_completed_pack:
                return _reject_completed_pack_now(msg)
            continue

        level_started_at = time.time()
        logger.debug(f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 计算评分：{file_name}")
        level, level_reason = _washing_new_level(
            sha1,
            file_name,
            file_size,
            str(target_cid_for_washing),
            media_type,
            original_lang=original_lang,
            has_external_subtitle=False,
            tmdb_id=str(tmdb_for_washing),
            season_num=s_num,
            episode_num=e_num,
        )
        logger.info(
            f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 评分完成："
            f"level={level}, reason={level_reason}, 耗时 {time.time() - level_started_at:.1f}s"
        )
        level_score = (1000 - min(level, 999)) * 100000
        action_score = 20000 if action == 'REPLACE' else 10000
        quality_score = _raw_quality_score(src, raw)
        candidates.append({
            'file': src,
            'score': level_score + action_score + quality_score,
            'index': idx,
            'episode': e_num,
            'action': action,
            'reason': reason or level_reason,
        })

    if hard_reject:
        logger.warning(
            f"  ➜ [共享资源] 洗版预检拒绝：source={source_label}, "
            f"通过 {len(candidates)}/{len(files)}，错误 {len(errors)}，耗时 {time.time() - preflight_started_at:.1f}s"
        )
        return [], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': True,
            'errors': errors[:50],
        }

    if not candidates:
        logger.warning(
            f"  ➜ [共享资源] 洗版预检无可用候选：source={source_label}, "
            f"errors={len(errors)}，耗时 {time.time() - preflight_started_at:.1f}s"
        )
        return [], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': True,
            'errors': errors[:50] or ['所有中心源均未通过洗版预检'],
        }

    if is_ongoing_hub:
        best_by_episode: Dict[Any, Dict[str, Any]] = {}
        for cand in sorted(candidates, key=lambda x: (x.get('score') or 0, -(x.get('index') or 0)), reverse=True):
            key = cand.get('episode') if cand.get('episode') is not None else f"idx:{cand.get('index')}"
            if key not in best_by_episode:
                best_by_episode[key] = cand
        selected = [best_by_episode[k] for k in sorted(best_by_episode, key=lambda x: _safe_int(x, 999999) if not str(x).startswith('idx:') else 999999)]
        logger.info(
            f"  ➜ [共享资源] 连载公共包洗版预检按集选源：原始 {len(files)} 个，选中 {len(selected)} 个，"
            f"跳过/拒绝 {len(errors)} 个，耗时 {time.time() - preflight_started_at:.1f}s。"
        )
        return [c['file'] for c in selected], {
            'raw_cached_count': cached,
            'raw_cache_errors': cache_errors[:20],
            'washing_checked': True,
            'washing_rejected': False,
            'selected_count': len(selected),
            'errors': errors[:50],
        }

    # 电影/单集/完结季：预检通过的文件全部进入秒传；完结季如果任一视频被拒绝，前面已 hard_reject。
    logger.info(
        f"  ➜ [共享资源] 洗版预检通过："
        f"选中 {len(candidates)}/{len(files)}，跳过/拒绝 {len(errors)}，耗时 {time.time() - preflight_started_at:.1f}s"
    )
    return [c['file'] for c in sorted(candidates, key=lambda x: x.get('index') or 0)], {
        'raw_cached_count': cached,
        'raw_cache_errors': cache_errors[:20],
        'washing_checked': True,
        'washing_rejected': False,
        'selected_count': len(candidates),
        'errors': errors[:50],
    }

def rapid_save_file(file_info: Dict[str, Any], *, target_cid: str = '') -> Dict[str, Any]:
    p115 = P115Service.get_client()
    if not p115:
        raise RuntimeError('115 客户端未初始化')
    target_cid = str(target_cid or _target_cid()).strip()
    file_info = _normalize_rapid_file_info(file_info or {})
    sha1 = _norm_sha1(file_info.get('sha1'))
    size = _rapid_size_to_int(file_info.get('size') or file_info.get('file_size'), 0)
    file_name = str(file_info.get('file_name') or file_info.get('name') or sha1).strip() or sha1
    if not sha1:
        raise RuntimeError('缺少合法 SHA1，无法秒传')
    if size <= 0:
        raise RuntimeError(
            f'中心源缺少文件大小，无法秒传：{file_name}；'
            f'请源端重新登记该资源，或先修复 p115_filesystem_cache.size 后再登记。'
        )
    rapid_meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    rapid_meta = dict(rapid_meta or {})
    preid = _norm_sha1(file_info.get('preid') or rapid_meta.get('preid') or rapid_meta.get('pre_sha1') or rapid_meta.get('pre_sha1_128k'))
    if preid:
        rapid_meta.setdefault('preid', preid)
    logger.info(f"  ➜ [共享资源] 准备执行 115 秒传：{file_name}")
    resp = _call_rapid_method(
        p115,
        target_cid=target_cid,
        sha1=sha1,
        size=size,
        file_name=file_name,
        pick_code=str(file_info.get('pick_code') or ''),
        rapid_meta=rapid_meta,
    )
    if _rapid_success(resp):
        _remember_rapid_preid_hint(
            file_info,
            target_cid=target_cid,
            sha1=sha1,
            size=size,
            file_name=file_name,
            rapid_meta=rapid_meta,
            response=resp,
        )
        return {'ok': True, 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}

    sign_req = _rapid_sign_request_from_response(resp)
    if sign_req and shared_center_enabled():
        try:
            client = SharedCenterClient()
            retry = _retry_rapid_with_center_sign(
                client=client, p115=p115, file_info=file_info, target_cid=target_cid,
                sha1=sha1, size=size, file_name=file_name, rapid_meta=rapid_meta, first_resp=resp,
            )
            if retry.get('ok'):
                _remember_rapid_preid_hint(
                    file_info,
                    target_cid=target_cid,
                    sha1=sha1,
                    size=size,
                    file_name=file_name,
                    rapid_meta=rapid_meta,
                    response=retry.get('response'),
                )
                return retry
            return {
                'ok': False,
                'response': retry.get('response') or resp,
                'sha1': sha1,
                'file_name': file_name,
                'target_cid': target_cid,
                'sign_job': retry.get('sign_job'),
                'message': retry.get('message') or '中心 holder 签名未完成',
                'no_retry': bool(retry.get('no_retry')),
                'abort_transfer': bool(retry.get('abort_transfer')),
            }
        except Exception as e:
            err_text = str(e)
            no_retry = 'no rapid sign holder available' in err_text or '无可用 holder' in err_text
            logger.warning(f"  ➜ [负载均衡签名] 中心 holder 签名闭环失败：{e}")
            return {
                'ok': False,
                'response': resp,
                'sha1': sha1,
                'file_name': file_name,
                'target_cid': target_cid,
                'message': err_text,
                'no_retry': bool(no_retry),
                'abort_transfer': bool(no_retry),
            }

    return {'ok': False, 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}


def _event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get('payload_json') if isinstance(event.get('payload_json'), dict) else None
    if payload is None:
        payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    return dict(payload or {})


def _looks_like_logical_season_group_id(value: Any) -> bool:
    text = str(value or '').strip().lower()
    # 中心逻辑季组当前使用 svg_ 前缀；额外兼容后续/旧灰度可能出现的 lsg_/logical_season_。
    return bool(text and re.match(r'^(svg_|lsg_|logical_season_)', text))


def _logical_season_group_id_from_payload(payload: Dict[str, Any], fallback: Any = '') -> str:
    """从中心资源行里提取逻辑季 group_id。

    切到逻辑季包后，前端/旧缓存偶尔仍把资源行标成 completed_season。
    这里统一把 logical_group_id/group_id/logical_group.group_id/source_id(svg_) 归一成
    logical_season，避免再调用已经停用的 completed_season_manifest。
    """
    payload = payload if isinstance(payload, dict) else {}
    logical_group = payload.get('logical_group') if isinstance(payload.get('logical_group'), dict) else {}
    candidates = (
        payload.get('logical_group_id'),
        payload.get('group_id'),
        logical_group.get('group_id'),
        logical_group.get('source_id'),
        payload.get('logical_season_group_id'),
        payload.get('source_id'),
        payload.get('source_ref_id'),
        fallback,
    )
    for value in candidates:
        text = str(value or '').strip()
        if _looks_like_logical_season_group_id(text):
            return text
    # 有明确逻辑季字段但 group_id 没有固定前缀时，也信任显式字段。
    for value in (payload.get('logical_group_id'), payload.get('group_id'), logical_group.get('group_id')):
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _legacy_completed_source_should_use_logical(payload: Dict[str, Any], source_id: str = '') -> str:
    payload = payload if isinstance(payload, dict) else {}
    group_id = _logical_season_group_id_from_payload(payload, source_id)
    if not group_id:
        return ''
    if _looks_like_logical_season_group_id(group_id):
        return group_id
    logical_group = payload.get('logical_group') if isinstance(payload.get('logical_group'), dict) else {}
    channel = _completed_share_channel_from_payload(payload) if '_completed_share_channel_from_payload' in globals() else {}
    channel = channel if isinstance(channel, dict) else {}
    raw_channel = channel.get('raw_json') if isinstance(channel.get('raw_json'), dict) else {}
    has_logical_marker = bool(
        payload.get('logical_pool_complete')
        or payload.get('pool_complete')
        or payload.get('logical_shadow_only')
        or payload.get('logical_import_available')
        or payload.get('logical_group_id')
        or payload.get('group_id')
        or logical_group
        or isinstance(payload.get('best_asset_map'), dict)
        or str(channel.get('share_kind') or raw_channel.get('share_kind') or '').strip() == 'logical_season'
    )
    return group_id if has_logical_marker else ''


def _event_sources(event: Dict[str, Any], client: SharedCenterClient) -> Tuple[str, str, List[Dict[str, Any]]]:
    payload = _event_payload(event)
    source_kind = _normalize_source_kind(event.get('source_kind') or payload.get('source_kind') or '')
    source_id = str(
        event.get('source_ref_id')
        or payload.get('source_id')
        or payload.get('source_ref_id')
        or payload.get('hub_id')
        or payload.get('id')
        or ''
    ).strip()
    if not source_kind:
        if payload.get('hub_id') or payload.get('is_season_hub'):
            source_kind = 'season_hub'
        else:
            source_kind = _normalize_source_kind(
                payload.get('kind') or payload.get('item_type') or payload.get('display_type') or ''
            )

    logical_group_id = ''
    if source_kind == 'completed_season':
        logical_group_id = _legacy_completed_source_should_use_logical(payload, source_id)
        if logical_group_id:
            old_source_id = source_id
            source_kind = 'logical_season'
            source_id = logical_group_id
            payload['source_kind'] = 'logical_season'
            payload['source_id'] = source_id
            payload['source_ref_id'] = source_id
            try:
                event['source_kind'] = 'logical_season'
                event['source_ref_id'] = source_id
                event['payload_json'] = payload
            except Exception:
                pass
            logger.info(
                f"  ➜ [共享资源] 已将旧 completed_season 转存事件改道为逻辑季包："
                f"{old_source_id or '-'} -> {source_id}"
            )

    # 逻辑季包展开出来的单集资产：前端直接提交 logical_episode + asset_id + rapid 参数，
    # 中心端 lease/sign/report 均按 shared_episode_assets.asset_id 结算；本机只负责执行单文件秒传。
    if source_kind == 'logical_episode':
        file_info = dict(payload or {})
        file_info['source_kind'] = 'logical_episode'
        file_info['source_id'] = source_id or file_info.get('asset_id') or file_info.get('source_ref_id') or ''
        file_info['source_ref_id'] = file_info['source_id']
        if not file_info.get('file_name') and file_info.get('name'):
            file_info['file_name'] = file_info.get('name')
        rapid_meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
        rapid_meta = dict(rapid_meta or {})
        rapid_meta.setdefault('source_kind', 'logical_episode')
        rapid_meta.setdefault('source_id', file_info['source_id'])
        rapid_meta.setdefault('preid', file_info.get('preid') or '')
        if file_info.get('pick_code') and not rapid_meta.get('pick_code'):
            rapid_meta['pick_code'] = file_info.get('pick_code')
        if file_info.get('file_id') and not rapid_meta.get('file_id'):
            rapid_meta['file_id'] = file_info.get('file_id')
        file_info['rapid_meta_json'] = rapid_meta
        return source_kind, file_info['source_id'], [file_info]

    # 逻辑完结季包：资源行可能已带 best_asset_map；没有则向中心懒取 manifest。
    # 每个文件按 logical_episode 执行签名/秒传，但整季成功上报 logical_season。
    if source_kind == 'logical_season':
        manifest_item = {}
        raw_files = []
        best_asset_map = payload.get('best_asset_map') if isinstance(payload.get('best_asset_map'), dict) else {}
        if best_asset_map:
            for ep in sorted(best_asset_map.keys(), key=lambda x: _safe_int(x, 0)):
                item = best_asset_map.get(ep)
                if isinstance(item, dict):
                    f = dict(item)
                    f.setdefault('episode_number', _safe_int(ep, 0))
                    raw_files.append(f)
        if not raw_files:
            try:
                manifest = client.logical_season_manifest(source_id) if hasattr(client, 'logical_season_manifest') else {}
                manifest_item = (manifest.get('item') if isinstance(manifest, dict) and isinstance(manifest.get('item'), dict) else {}) or {}
                raw_files = (manifest.get('files') or manifest.get('items') or []) if isinstance(manifest, dict) else []
                logger.info(f"  ➜ [共享资源] 逻辑季包已补拉文件列表：group={source_id}, files={len(raw_files or [])}")
            except Exception as e:
                logger.warning(f"  ➜ [共享资源] 拉取逻辑季包文件列表失败：group={source_id}, err={e}")
        source_payload = {**manifest_item, **payload}
        files = []
        for item in raw_files or []:
            if not isinstance(item, dict):
                continue
            f = dict(item)
            asset_id = str(f.get('asset_id') or f.get('source_id') or f.get('source_ref_id') or '').strip()
            f.setdefault('tmdb_id', source_payload.get('tmdb_id'))
            f.setdefault('parent_series_tmdb_id', source_payload.get('parent_series_tmdb_id') or source_payload.get('series_tmdb_id') or source_payload.get('tmdb_id'))
            f.setdefault('series_tmdb_id', source_payload.get('series_tmdb_id') or source_payload.get('parent_series_tmdb_id') or source_payload.get('tmdb_id'))
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', source_payload.get('season_number'))
            f.setdefault('title', source_payload.get('title'))
            f.setdefault('release_year', source_payload.get('release_year'))
            f['source_kind'] = 'logical_episode'
            f['source_id'] = asset_id
            f['source_ref_id'] = asset_id
            rapid_meta = f.get('rapid_meta_json') if isinstance(f.get('rapid_meta_json'), dict) else {}
            rapid_meta = dict(rapid_meta or {})
            rapid_meta.setdefault('source_kind', 'logical_episode')
            rapid_meta.setdefault('source_id', asset_id)
            rapid_meta.setdefault('preid', f.get('preid') or '')
            if f.get('file_id') and not rapid_meta.get('file_id'):
                rapid_meta['file_id'] = f.get('file_id')
            if f.get('pick_code') and not rapid_meta.get('pick_code'):
                rapid_meta['pick_code'] = f.get('pick_code')
            f['rapid_meta_json'] = rapid_meta
            files.append(f)
        return source_kind, source_id, files

    # display-list 里的 Pack 如果是公共连载季壳，通常只有 hub_id，没有 completed source_id。
    # 这种壳不能走 completed_season_manifest，否则会拿不到 7-8 这类 children 分集。
    if source_kind == 'completed_season' and payload.get('hub_id') and not payload.get('source_id'):
        source_kind = 'season_hub'
        source_id = str(payload.get('hub_id') or source_id or '').strip()

    # 兼容中心返回的 completed season 包：列表接口只给源摘要，真正文件清单要再取 manifest。
    # 如果 manifest 为空，不能再显示“秒传完成 0/0”，这属于 manifest 缺失/旧数据，需要重新登记该季。
    if source_kind == 'completed_season':
        # 新中心已停用旧 completed-season manifest。能识别为逻辑季的旧事件必须在上面改道；
        # 到这里仍是 completed_season，直接给业务错误，避免抛 RuntimeError 打 500。
        method = getattr(client, 'completed_season_manifest', None)
        if not callable(method):
            raise RuntimeError('旧 completed-season manifest 已停用，当前资源缺少 logical_season group_id，请刷新中心资源库后重试。')
        try:
            manifest = method(source_id)
        except RuntimeError as e:
            raise RuntimeError('旧 completed-season manifest 已停用，当前资源没有可识别的 logical_season group_id，请刷新中心资源库后重试。') from e
        manifest_item = (manifest.get('item') if isinstance(manifest, dict) and isinstance(manifest.get('item'), dict) else {}) or {}
        source_payload = {**manifest_item, **payload}
        files = (manifest.get('files') or manifest.get('items') or []) if isinstance(manifest, dict) else []
        if not files and isinstance(manifest, dict):
            data = manifest.get('data') if isinstance(manifest.get('data'), dict) else {}
            files = data.get('files') or data.get('items') or []
        if not files and isinstance(payload.get('files'), list):
            files = payload.get('files') or []
        files = [dict(f or {}) for f in files if isinstance(f, dict)]
        for f in files:
            f.setdefault('tmdb_id', source_payload.get('tmdb_id'))
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', source_payload.get('season_number'))
            f.setdefault('title', source_payload.get('title'))
            f.setdefault('release_year', source_payload.get('release_year'))
            f.setdefault('is_clean_version', bool(source_payload.get('is_clean_version')))
            f.setdefault('clean_version_confidence', source_payload.get('clean_version_confidence'))
            f.setdefault('clean_version_meta_json', source_payload.get('clean_version_meta_json') or {})
            f.setdefault('source_kind', 'completed_season')
            f.setdefault('source_id', source_id)
            f.setdefault('source_ref_id', source_id)
        return source_kind, source_id, files

    # 公共连载季包：中心 display-list 返回 season_hub 壳，真正可秒传文件在 pack_items/children 中。
    # 资源库为了快，列表页可能不带 children；订阅秒传不能只吃壳，必须懒加载 display-children。
    # 每个子项仍然是 episode 源；转存和贡献流水按 episode 上报，不把 season_hub 当作某个设备的源。
    if source_kind == 'season_hub':
        raw_files = []
        for key in ('pack_items', 'children', 'files', 'items'):
            value = payload.get(key)
            if isinstance(value, list) and value:
                raw_files = value
                break

        if not raw_files:
            try:
                child_resp = client.list_display_children(
                    source_kind='season_hub',
                    source_id=source_id,
                    hub_id=str(payload.get('hub_id') or source_id or '').strip(),
                    limit=20000,
                ) or {}
                containers = [child_resp]
                data = child_resp.get('data') if isinstance(child_resp, dict) and isinstance(child_resp.get('data'), dict) else {}
                if data:
                    containers.append(data)
                for box in containers:
                    if not isinstance(box, dict):
                        continue
                    for key in ('pack_items', 'children', 'files', 'items'):
                        value = box.get(key)
                        if isinstance(value, list) and value:
                            raw_files = value
                            break
                    if raw_files:
                        break
                logger.info(
                    f"  ➜ [共享资源] 公共连载季包已补拉子项：hub={source_id}, children={len(raw_files or [])}"
                )
            except Exception as e:
                logger.warning(f"  ➜ [共享资源] 拉取公共连载季包子项失败：hub={source_id}, err={e}")

        files = []
        for item in raw_files or []:
            if not isinstance(item, dict):
                continue
            f = dict(item)
            f.setdefault('tmdb_id', payload.get('tmdb_id'))
            f.setdefault('parent_series_tmdb_id', payload.get('parent_series_tmdb_id') or payload.get('series_tmdb_id') or payload.get('tmdb_id'))
            f.setdefault('series_tmdb_id', payload.get('series_tmdb_id') or payload.get('parent_series_tmdb_id') or payload.get('tmdb_id'))
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', payload.get('season_number'))
            f.setdefault('title', payload.get('title'))
            f.setdefault('release_year', payload.get('release_year'))
            f['source_kind'] = 'episode'
            f['source_id'] = f.get('source_id') or f.get('source_ref_id') or f.get('episode_source_id') or ''
            f['source_ref_id'] = f.get('source_ref_id') or f.get('source_id') or ''
            files.append(f)
        return source_kind, source_id, files

    file_info = dict(payload or {})
    file_info.setdefault('source_kind', source_kind)
    file_info.setdefault('source_id', source_id)
    return source_kind, source_id, [file_info]



_CURRENT_CENTER_DEVICE_ID_CACHE = {'value': '', 'loaded_at': 0.0}


def _current_center_device_id(client: SharedCenterClient = None) -> str:
    """读取当前中心设备 ID，用于消费端兜底排除本机源。"""
    now = time.time()
    cached = _CURRENT_CENTER_DEVICE_ID_CACHE.get('value') or ''
    if cached and now - float(_CURRENT_CENTER_DEVICE_ID_CACHE.get('loaded_at') or 0) < 300:
        return cached
    try:
        c = client or SharedCenterClient()
        me = c.me() if hasattr(c, 'me') else {}
        device_id = str((me or {}).get('id') or (me or {}).get('device_id') or '').strip()
        if device_id:
            _CURRENT_CENTER_DEVICE_ID_CACHE['value'] = device_id
            _CURRENT_CENTER_DEVICE_ID_CACHE['loaded_at'] = now
            return device_id
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 获取当前中心设备 ID 失败，跳过本机源判断: {e}")
    return cached or ''


def _normalize_episode_numbers(value) -> List[int]:
    if value in (None, '', [], {}):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = re.split(r'[，,\s]+', value.strip()) if value.strip() else []
    if isinstance(value, dict):
        value = value.get('episodes') or value.get('missing') or value.get('missing_episode_numbers') or value.values()
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    out: List[int] = []
    for item in value:
        try:
            n = int(float(item))
            if n > 0 and n not in out:
                out.append(n)
        except Exception:
            pass
    return sorted(out)


def _requested_missing_episodes_from_payload(payload: Dict[str, Any], event: Dict[str, Any] = None) -> List[int]:
    event = event or {}
    for container in (payload or {}, event or {}):
        for key in (
            '_requested_missing_episode_numbers', 'requested_missing_episode_numbers',
            'missing_episode_numbers', 'missing_episodes', 'episode_numbers'
        ):
            nums = _normalize_episode_numbers(container.get(key)) if isinstance(container, dict) else []
            if nums:
                return nums
        context = container.get('_consume_context') if isinstance(container, dict) and isinstance(container.get('_consume_context'), dict) else {}
        for key in ('missing_episode_numbers', 'missing_episodes'):
            nums = _normalize_episode_numbers(context.get(key))
            if nums:
                return nums
    return []


def _boolish_local(value, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启', '是'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭', '否'):
        return False
    return bool(default)


def _same_device_id(left: Any, right: Any) -> bool:
    return bool(str(left or '').strip() and str(left or '').strip() == str(right or '').strip())


def _file_is_own_center_source(file_info: Dict[str, Any], payload: Dict[str, Any], client: SharedCenterClient) -> bool:
    """消费端兜底排除本机登记的中心源，避免统一订阅吃到自己的回旋镖。"""
    file_info = file_info if isinstance(file_info, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    if _boolish_local(file_info.get('is_mine'), False) or _boolish_local(payload.get('is_mine'), False):
        return True
    current_id = ''
    for obj in (file_info, payload):
        for key in ('provider_device_id', 'contributor_id', 'device_id', 'holder_id'):
            value = str(obj.get(key) or '').strip()
            if not value:
                continue
            if not current_id:
                current_id = _current_center_device_id(client)
            if _same_device_id(value, current_id):
                return True
    return False


def _local_movie_in_library(tmdb_id: Any) -> bool:
    tmdb = str(tmdb_id or '').strip()
    if not tmdb:
        return False
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM media_metadata
                    WHERE item_type='Movie'
                      AND tmdb_id=%s
                      AND COALESCE(in_library, FALSE)=TRUE
                    LIMIT 1
                    """,
                    (tmdb,),
                )
                return cur.fetchone() is not None
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询本地电影入库状态失败: tmdb={tmdb}, err={e}")
        return False


def _local_episode_in_library(parent_series_tmdb_id: Any, season_number: Any, episode_number: Any) -> bool:
    parent = str(parent_series_tmdb_id or '').strip()
    season = _safe_int_or_none(season_number)
    episode = _safe_int_or_none(episode_number)
    if not parent or season is None or episode is None:
        return False
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM media_metadata
                    WHERE item_type='Episode'
                      AND parent_series_tmdb_id=%s
                      AND season_number=%s
                      AND episode_number=%s
                      AND COALESCE(in_library, FALSE)=TRUE
                    LIMIT 1
                    """,
                    (parent, season, episode),
                )
                return cur.fetchone() is not None
    except Exception as e:
        logger.debug(
            f"  ➜ [共享资源] 查询本地分集入库状态失败: tmdb={parent}, "
            f"S{season}E{episode}, err={e}"
        )
        return False



def _local_movie_washing_snapshot(tmdb_id: Any) -> Dict[str, Any]:
    """读取本地电影入库洗版快照；只用于 replace 秒传前短路。"""
    tmdb = str(tmdb_id or '').strip()
    if not tmdb:
        return {}
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tmdb_id, title, washing_level, washing_snapshot_json
                    FROM media_metadata
                    WHERE item_type='Movie'
                      AND tmdb_id=%s
                      AND COALESCE(in_library, FALSE)=TRUE
                    ORDER BY CASE
                                WHEN washing_level = 1 THEN 0
                                WHEN washing_level IS NOT NULL AND washing_level > 0 THEN 1
                                ELSE 2
                             END,
                             washing_level ASC NULLS LAST,
                             last_updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (tmdb,),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询本地电影洗版快照失败: tmdb={tmdb}, err={e}")
        return {}


def _local_episode_washing_snapshot(parent_series_tmdb_id: Any, season_number: Any, episode_number: Any) -> Dict[str, Any]:
    """读取本地分集入库洗版快照；只用于 replace 秒传前短路。"""
    parent = str(parent_series_tmdb_id or '').strip()
    season = _safe_int_or_none(season_number)
    episode = _safe_int_or_none(episode_number)
    if not parent or season is None or episode is None:
        return {}
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT parent_series_tmdb_id, season_number, episode_number, title,
                        washing_level, washing_snapshot_json
                    FROM media_metadata
                    WHERE item_type='Episode'
                      AND parent_series_tmdb_id=%s
                      AND season_number=%s
                      AND episode_number=%s
                      AND COALESCE(in_library, FALSE)=TRUE
                    ORDER BY CASE
                                WHEN washing_level = 1 THEN 0
                                WHEN washing_level IS NOT NULL AND washing_level > 0 THEN 1
                                ELSE 2
                             END,
                             washing_level ASC NULLS LAST,
                             last_updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (parent, season, episode),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.debug(
            f"  ➜ [共享资源] 查询本地分集洗版快照失败: tmdb={parent}, "
            f"S{season}E{episode}, err={e}"
        )
        return {}


def _is_inventory_best_washing_level(snapshot: Dict[str, Any]) -> bool:
    try:
        return int((snapshot or {}).get('washing_level')) == 1
    except Exception:
        return False


def _replace_mode_short_circuit_best_inventory(
    *,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """replace 模式下，RAW/洗版预检前先看库存优先级。

    本地已经是优先级 1 的电影/单集没有必要再拉中心 RAW、算目标目录、调用规则。
    完结季必须整季洗版：只有整包所有视频对应的本地集都已是优先级 1，才整包短路；
    只要有一集不是 1，就保留整包进入后续预检。
    """
    payload = payload if isinstance(payload, dict) else {}
    files = [dict(f or {}) for f in (files or []) if isinstance(f, dict)]
    normalized_source_kind = _normalize_source_kind(source_kind)
    context = _preflight_context(source_kind, source_id, payload, files)
    source_label = f"{source_kind or '-'}:{source_id or '-'}"

    best_skips = []
    kept = []
    completed_video_checks = []

    for f in files:
        file_name = f.get('file_name') or f.get('name') or f.get('sha1') or ''
        ext = os.path.splitext(str(file_name or ''))[1].lower()
        is_video = (not ext) or ext in VIDEO_EXTS
        if not is_video:
            kept.append(f)
            continue

        item_type = str(f.get('item_type') or context.get('item_type') or payload.get('item_type') or '').strip()
        file_kind = _normalize_source_kind(f.get('source_kind') or source_kind or '')
        is_movie = file_kind == 'movie' or item_type == 'Movie'
        is_episode_like = file_kind in ('episode', 'season_hub', 'completed_season') or item_type in ('Episode', 'Season')

        if is_movie:
            movie_tmdb = f.get('tmdb_id') or payload.get('tmdb_id') or context.get('tmdb_id')
            snap = _local_movie_washing_snapshot(movie_tmdb)
            if _is_inventory_best_washing_level(snap):
                best_skips.append(file_name)
                continue
            kept.append(f)
            continue

        if is_episode_like:
            s_num, e_num = _guess_se_from_source(f, context)
            parent_tmdb = _source_parent_series_tmdb_id(f, context)
            snap = _local_episode_washing_snapshot(parent_tmdb, s_num, e_num)
            is_best = _is_inventory_best_washing_level(snap)
            if normalized_source_kind == 'completed_season':
                completed_video_checks.append({
                    'file': f,
                    'file_name': file_name,
                    'known': bool(parent_tmdb and s_num is not None and e_num is not None),
                    'best': is_best,
                    'snapshot': snap,
                })
                kept.append(f)
                continue
            if is_best:
                best_skips.append(f"S{s_num if s_num is not None else '-'}E{e_num if e_num is not None else '-'} {file_name}".strip())
                continue
            kept.append(f)
            continue

        kept.append(f)

    if normalized_source_kind == 'completed_season' and completed_video_checks:
        # 完结季不能单集洗。只有整季所有视频都已在库内达到优先级 1，才整包短路。
        if all(x.get('known') and x.get('best') for x in completed_video_checks):
            message = f"本地完结季库存所有分集均已是最佳版本，跳过整季秒传：{payload.get('title') or source_id}"
            logger.info(f"  ➜ [共享资源] {message}")
            return [], {
                'checked': True,
                'short_circuit': True,
                'reason': 'completed_pack_inventory_best_level_1',
                'message': message,
                'best_count': len(completed_video_checks),
                'kept_count': 0,
                'skipped': {'inventory_best_level_1': [x.get('file_name') for x in completed_video_checks[:20]]},
            }
        return files, {
            'checked': True,
            'short_circuit': False,
            'message': '完结季未达到最佳版本，继续整季洗版预检。',
            'best_count': sum(1 for x in completed_video_checks if x.get('best')),
            'kept_count': len(files),
        }

    if best_skips:
        logger.debug(
            f"  ➜ [共享资源] 洗版优先级对比：source={source_label}，"
            f"本地已是最佳版本，跳过 {len(best_skips)} 个，保留 {len(kept)} 个进入预检。"
        )

    reason = 'inventory_best_level_1' if files and not kept and best_skips else ''
    message = '本地已是最佳版本，跳过共享秒传。' if reason else 'replace 库存优先级检查完成。'
    return kept, {
        'checked': True,
        'short_circuit': bool(reason),
        'reason': reason,
        'message': message,
        'best_count': len(best_skips),
        'kept_count': len(kept),
        'skipped': {'inventory_best_level_1': best_skips[:20]} if best_skips else {},
    }


def _filter_files_before_transfer(
    *,
    client: SharedCenterClient,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
    requested_missing_episode_numbers: List[int] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """在 RAW/洗版预检前做业务级过滤，严格服从整理覆盖模式。

    - keep_both：只排除本机源；不做缺集过滤、不做已入库过滤，命中订阅就秒传；
    - replace：电影/单集交给洗版预检比较；完结季保留整包进入洗版，不能被缺集列表裁成单集；
    - skip：只要本地已存在同电影/同集，就拒绝该项入库。
    """
    payload = payload if isinstance(payload, dict) else {}
    files = [dict(f or {}) for f in (files or []) if isinstance(f, dict)]
    missing_set = set(_normalize_episode_numbers(requested_missing_episode_numbers))
    conflict_mode = _current_organize_conflict_mode(default='skip')
    normalized_source_kind = _normalize_source_kind(source_kind)
    is_completed_pack_source = normalized_source_kind == 'completed_season'

    kept: List[Dict[str, Any]] = []
    skipped = {
        'self_source': [],
        'not_requested_episode': [],
        'already_in_library': [],
        'unknown_identity': [],
    }
    context = _preflight_context(source_kind, source_id, payload, files)
    source_label = f"{source_kind or '-'}:{source_id or '-'}"

    # keep_both 是显式多版本模式：订阅命中后只兜底排除本机源，其他一律交给秒传。
    if conflict_mode == 'keep_both':
        for f in files:
            file_name = f.get('file_name') or f.get('name') or f.get('sha1') or ''
            if _file_is_own_center_source(f, payload, client):
                skipped['self_source'].append(file_name)
                continue
            kept.append(f)

        total_skipped = sum(len(v) for v in skipped.values())
        if total_skipped:
            logger.info(
                f"  ➜ [共享资源] 秒传前匹配过滤：source={source_label}, conflict_mode=keep_both，"
                f"输入 {len(files)}，保留 {len(kept)}，仅跳过本机源 {len(skipped['self_source'])}。"
            )
        else:
            logger.info(
                f"  ➜ [共享资源] keep_both 模式：source={source_label}，"
                f"跳过缺集/已入库过滤，直接保留 {len(kept)}/{len(files)} 个中心文件。"
            )

        if files and not kept and skipped['self_source'] and len(skipped['self_source']) == len(files):
            reason = 'all_self_source'
            message = '你已有该资源，已跳过秒传。'
        else:
            reason = ''
            message = 'keep_both 模式：已跳过缺集/已入库过滤，命中订阅直接秒传。'

        return kept, {
            'checked': True,
            'source_kind': source_kind,
            'source_id': source_id,
            'input_count': len(files),
            'kept_count': len(kept),
            'skipped_count': total_skipped,
            'requested_missing_episode_numbers': sorted(missing_set),
            'conflict_mode': conflict_mode,
            'skipped': {k: v[:20] for k, v in skipped.items() if v},
            'reason': reason,
            'message': message,
        }

    for f in files:
        file_name = f.get('file_name') or f.get('name') or f.get('sha1') or ''
        if _file_is_own_center_source(f, payload, client):
            skipped['self_source'].append(file_name)
            continue

        item_type = str(f.get('item_type') or context.get('item_type') or payload.get('item_type') or '').strip()
        file_kind = _normalize_source_kind(f.get('source_kind') or source_kind or '')
        is_movie = file_kind == 'movie' or item_type == 'Movie'
        is_episode_like = file_kind in ('episode', 'season_hub', 'completed_season') or item_type in ('Episode', 'Season')

        if is_movie:
            movie_tmdb = f.get('tmdb_id') or payload.get('tmdb_id') or context.get('tmdb_id')
            if conflict_mode == 'skip' and _local_movie_in_library(movie_tmdb):
                skipped['already_in_library'].append(file_name)
                continue
            # replace 模式不在这里拦截已入库电影，交给洗版预检决定 ACCEPT/REPLACE/SKIP/REJECT。
            kept.append(f)
            continue

        if is_episode_like:
            s_num, e_num = _guess_se_from_source(f, context)
            parent_tmdb = _source_parent_series_tmdb_id(f, context)

            # replace + 完结季收藏包必须整包进入洗版预检，不能按缺集列表裁成单集。
            if not (conflict_mode == 'replace' and is_completed_pack_source):
                if e_num is not None and missing_set and int(e_num) not in missing_set:
                    skipped['not_requested_episode'].append(f"E{int(e_num):02d} {file_name}".strip())
                    continue
                if e_num is None and missing_set:
                    skipped['unknown_identity'].append(file_name)
                    continue

            if conflict_mode == 'skip':
                if e_num is None:
                    # skip 模式必须能确定集身份；否则无法证明“同集不存在”，宁可拒绝。
                    skipped['unknown_identity'].append(file_name)
                    continue
                if _local_episode_in_library(parent_tmdb, s_num, e_num):
                    skipped['already_in_library'].append(f"E{int(e_num):02d} {file_name}".strip())
                    continue

            # replace 模式不因本地已有同集拦截，交给洗版预检；skip 模式已在上面处理。
            kept.append(f)
            continue

        kept.append(f)

    total_skipped = sum(len(v) for v in skipped.values())
    if total_skipped:
        logger.info(
            f"  ➜ [共享资源] 秒传前匹配过滤：source={source_label}, conflict_mode={conflict_mode}, "
            f"输入 {len(files)}，保留 {len(kept)}，跳过 {total_skipped} "
            f"(非缺失集 {len(skipped['not_requested_episode'])}, 已入库 {len(skipped['already_in_library'])}, "
            f"本机源 {len(skipped['self_source'])}, 身份不明 {len(skipped['unknown_identity'])})"
        )

    if conflict_mode == 'replace' and is_completed_pack_source and files:
        logger.info(
            f"  ➜ [共享资源] replace 模式完结季整包洗版：source={source_label}，"
            f"保留 {len(kept)}/{len(files)} 个文件进入整季洗版预检，缺集过滤已禁用。"
        )

    reason = ''
    if files and not kept:
        if skipped['self_source'] and len(skipped['self_source']) == len(files):
            reason = 'all_self_source'
            message = '你已有该资源，已跳过秒传。'
        elif skipped['not_requested_episode'] and len(skipped['not_requested_episode']) == len(files):
            reason = 'no_requested_episode'
            message = f"中心当前资源不包含本机缺失集 {sorted(missing_set)}，已跳过。"
        elif skipped['already_in_library'] and len(skipped['already_in_library']) == len(files):
            reason = 'all_already_in_library'
            if conflict_mode == 'skip':
                message = '本地已存在同电影/同集，conflict_mode=skip，已拒绝重复入库。'
            else:
                message = '中心返回的集本机均已入库，已跳过重复秒传。'
        else:
            reason = 'all_filtered'
            message = '中心返回的资源经缺集/已入库/本机源过滤后无可秒传文件。'
    else:
        if conflict_mode == 'replace':
            message = 'replace 模式：已保留候选进入洗版预检。'
        elif conflict_mode == 'skip':
            message = 'skip 模式：已拒绝本地存在的同电影/同集，保留可入库候选。'
        else:
            message = '秒传前匹配过滤完成'

    return kept, {
        'checked': True,
        'source_kind': source_kind,
        'source_id': source_id,
        'input_count': len(files),
        'kept_count': len(kept),
        'skipped_count': total_skipped,
        'requested_missing_episode_numbers': sorted(missing_set),
        'conflict_mode': conflict_mode,
        'skipped': {k: v[:20] for k, v in skipped.items() if v},
        'reason': reason,
        'message': message,
    }


def _completed_share_channel_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    for key in ('share_channel', 'logical_season_share_channel', 'completed_share_channel', 'logical_season_share_channel', 'logical_share_channel'):
        value = payload.get(key)
        if isinstance(value, dict) and value:
            return dict(value)
    return {}


def _completed_share_channel_for_transfer(client: SharedCenterClient, source_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """读取逻辑季包可转存分享通道。只信中心托管 channel，不扫描 115 全量分享列表。"""
    source_id = str(source_id or '').strip()
    payload = payload if isinstance(payload, dict) else {}
    source_kind = _normalize_source_kind(payload.get('source_kind') or '')
    channel = _completed_share_channel_from_payload(payload)
    if str((channel or {}).get('status') or '').lower() == 'valid' and channel.get('share_code'):
        return channel
    if not source_id:
        return {}
    try:
        if source_kind != 'logical_season':
            return {}
        if not hasattr(client, 'get_logical_season_share_channel'):
            return {}
        resp = client.get_logical_season_share_channel(source_id) or {}
        item = resp.get('item') if isinstance(resp.get('item'), dict) else {}
        if str((item or {}).get('status') or '').lower() == 'valid' and item.get('share_code'):
            return dict(item)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询分享通道失败：source={source_kind or '-'}:{source_id}, err={e}")
    return {}


def _share_import_resp_text(resp: Any, error: str = '') -> str:
    try:
        base = json.dumps(resp, ensure_ascii=False, default=str) if isinstance(resp, (dict, list)) else str(resp or '')
    except Exception:
        base = str(resp or '')
    return (base + ' ' + str(error or '')).strip()


def _share_import_resp_code(resp: Any) -> str:
    if isinstance(resp, dict):
        for key in ('errno', 'code', 'errNo', 'err_no'):
            value = resp.get(key)
            if value not in (None, ''):
                return str(value)
    return ''


def _is_share_import_already_saved(resp: Any) -> bool:
    """115 返回“已经转存/接收过”只代表本账号幂等限制，不代表共享源失效。

    融合版要求文件进入待整理目录；因此这里不能直接当作转存成功，
    只能归为本机账号侧问题，后续自动回退 Rapid。
    """
    code = _share_import_resp_code(resp)
    text = _share_import_resp_text(resp).lower()
    return (
        code == '4100024'
        or '4100024' in text
        or '你已经转存过' in text
        or '已经转存过' in text
        or '转存过该文件' in text
        or '已接收过' in text
        or '已经接收过' in text
        or '重复接收' in text
        or '无需重复' in text
        or 'already received' in text
        or 'already saved' in text
    )


def _is_share_import_local_account_issue(resp: Any, error: str = '') -> bool:
    """本机账号/频率/空间/幂等问题，不应污染中心 share_channel 状态。"""
    if _is_share_import_already_saved(resp):
        return True
    code = _share_import_resp_code(resp)
    if code in ('4200041',):
        return True
    text = _share_import_resp_text(resp, error).lower()
    return any(k in text for k in (
        '空间不足', '超过限制', '转存超限', '任务上限', '频繁',
        '你已被限制接收', '限制接收', '被限制接收', '接收功能受限', '接收功能被限制',
        '你已被限制转存', '限制转存', '被限制转存', '转存功能受限', '转存功能被限制',
        '770004', '990001', '4100010', '4100025', '4200041',
        'quota', 'limit', 'too many', 'rate', 'account', 'permission',
    ))


def _is_share_import_source_dead(resp: Any, error: str = '') -> bool:
    """只有明确死链/提取码错误/源文件删除，才允许把中心通道置为异常。"""
    if _is_share_import_local_account_issue(resp, error):
        return False
    code = _share_import_resp_code(resp)
    if code in ('4100005',):
        return True
    text = _share_import_resp_text(resp, error).lower()
    return any(k in text for k in (
        '分享已取消', '分享已失效', '分享不存在', '取消分享', '已取消', '已失效',
        '提取码错误', '访问码错误', '密码错误',
        '文件(夹)已被移动或删除', '已被移动或删除', '源文件不存在',
        'share not found', 'expired', 'cancelled', 'canceled', 'not found', 'deleted',
    ))


def _share_import_success(resp: Any) -> bool:
    """分享转存成功判定。

    注意：4100024/已经转存过不能算成功；融合版要把资源放进待整理目录，
    这种幂等限制只能走 Rapid 兜底。
    """
    if _is_share_import_already_saved(resp):
        return False
    text = _share_import_resp_text(resp).lower()
    if isinstance(resp, dict):
        if resp.get('state') is True or resp.get('success') is True:
            return True
        code = _share_import_resp_code(resp)
        if code in ('0', '200'):
            return True
    return any(k in text for k in ('转存成功', '接收成功', '保存成功', 'receive success', 'successfully'))


def _share_import_failed_status(resp: Any, error: str = '') -> str:
    if _is_share_import_source_dead(resp, error):
        return 'expired'
    text = _share_import_resp_text(resp, error).lower()
    if any(x in text for x in ('审核', '处理中', 'pending', 'review', 'processing')):
        return 'pending_review'
    if _is_share_import_local_account_issue(resp, error):
        return 'local_account_issue'
    return 'import_failed'


def _share_import_receive_title(resp: Any) -> str:
    if not isinstance(resp, dict):
        return ''
    data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
    for item in (data, resp):
        if not isinstance(item, dict):
            continue
        for key in ('receive_title', 'file_name', 'fileName', 'name', 'title'):
            value = str(item.get(key) or '').strip()
            if value:
                return value
    return ''


def _p115_item_name(item: Dict[str, Any]) -> str:
    item = item if isinstance(item, dict) else {}
    return str(item.get('fn') or item.get('file_name') or item.get('n') or item.get('name') or '').strip()


def _p115_item_id(item: Dict[str, Any]) -> str:
    item = item if isinstance(item, dict) else {}
    return str(item.get('fid') or item.get('file_id') or item.get('cid') or item.get('id') or '').strip()


def _locate_share_imported_item(p115, *, parent_cid: str, receive_title: str, max_retries: int = 3) -> Dict[str, Any]:
    """参考影巢逻辑：share_import 成功后按 receive_title 在待整理根目录定位真实 file_id。"""
    parent_cid = str(parent_cid or '').strip()
    receive_title = str(receive_title or '').strip()
    if not parent_cid or not receive_title or not p115 or not hasattr(p115, 'fs_files'):
        return {}
    for attempt in range(1, max(1, int(max_retries or 3)) + 1):
        wait_time = attempt * 2
        logger.debug(
            f"  ➜ [共享资源] 等待 {wait_time}s 后定位分享转存目录 "
            f"({attempt}/{max_retries})：{receive_title}"
        )
        time.sleep(wait_time)
        try:
            resp = p115.fs_files({'cid': parent_cid, 'search_value': receive_title, 'limit': 10})
            items = resp.get('data') if isinstance(resp, dict) else []
            if not isinstance(items, list):
                items = []
            for item in items:
                if isinstance(item, dict) and _p115_item_name(item) == receive_title:
                    logger.info(f"  ➜ [共享资源] 已定位分享转存目录：{receive_title} (fid={_p115_item_id(item) or '-'})")
                    return dict(item)
            logger.debug(f"  ➜ [共享资源] 第 {attempt}/{max_retries} 次未定位到分享转存目录，等待 115 索引同步。")
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 定位分享转存目录失败({attempt}/{max_retries})：{e}")
    logger.warning(f"  ➜ [共享资源] 未能定位分享转存目录：{receive_title}，交由全局待整理扫描兜底。")
    return {}


def _save_share_import_transfer_context(*, root_name: str, source_id: str, payload: Dict[str, Any], files: List[Dict[str, Any]], channel_id: str = '', source_kind: str = 'logical_season') -> Dict[str, Any]:
    root_name = str(root_name or '').strip()
    if not root_name:
        return {'ok': False, 'skipped': True, 'reason': 'root_name_missing'}
    try:
        ctx = _season_package_context(payload or {}, files or [])
        if not ctx.get('tmdb_id') or not ctx.get('title'):
            return {'ok': False, 'skipped': True, 'reason': 'context_missing', 'context': ctx}
        normalized_kind = _normalize_source_kind(source_kind or 'logical_season')
        if normalized_kind != 'logical_season':
            normalized_kind = 'logical_season'
        P115CacheManager.save_transfer_context(
            root_name=root_name,
            tmdb_id=ctx.get('tmdb_id'),
            media_type='tv',
            title=ctx.get('title'),
            season_number=ctx.get('season_number'),
            source='shared-share-import',
            source_kind=normalized_kind,
            source_kinds=[normalized_kind, 'shared_share_import', 'shared_transfer_context'],
            confidence='high',
            authority_role='expected',
            evidence=[f'share:{normalized_kind}:{source_id}', f'channel:{channel_id or "-"}'],
        )
        logger.debug(f"  ➜ [共享资源] 已保存分享转存整理上下文：{root_name} -> tmdb={ctx.get('tmdb_id')}, season={ctx.get('season_number')}")
        return {'ok': True, 'context': ctx, 'root_name': root_name}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 保存分享转存整理上下文失败：{root_name} -> {e}")
        return {'ok': False, 'error': str(e), 'root_name': root_name}



def _prepare_share_import_target_dir(
    *,
    base_target_cid: str,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """分享转存季包前，先在待整理根目录创建标准剧/季目录。

    文件列表分享如果直接 import 到待整理根目录，整理扫描会把一堆视频当作
    root item 分散处理，无法按同一个剧季上下文聚合。这里强制创建：

        剧名 (年份) {tmdb=xxx} - Season 02

    然后把 share_import 的目标 CID 指向该标准目录。失败时不能回退为
    直接丢根目录，只能让外层走 Rapid 回退或报错。
    """
    normalized_kind = _normalize_source_kind(source_kind)
    if normalized_kind != 'logical_season':
        return {
            'target_cid': str(base_target_cid or ''),
            'base_target_cid': str(base_target_cid or ''),
            'share_import_temp_dir': False,
            'season_package_temp_dir': False,
            'skipped': True,
            'reason': 'not_season_share_source',
        }

    base_target_cid = str(base_target_cid or '').strip()
    if not base_target_cid:
        return {
            'target_cid': '',
            'base_target_cid': '',
            'share_import_temp_dir': False,
            'season_package_temp_dir': False,
            'temp_dir_required': True,
            'temp_dir_error': '缺少待整理根目录 CID',
        }

    folder_name, ctx = _build_season_package_temp_dir_name(
        source_kind=normalized_kind,
        source_id=source_id,
        payload=payload or {},
        files=files or [],
    )

    try:
        p115 = P115Service.get_client()
        if not p115:
            raise RuntimeError('115 客户端未初始化')

        mk_resp = p115.fs_mkdir(folder_name, base_target_cid)
        if not isinstance(mk_resp, dict) or not mk_resp.get('state'):
            raise RuntimeError(str(mk_resp))

        temp_cid = str(
            mk_resp.get('cid')
            or mk_resp.get('file_id')
            or mk_resp.get('id')
            or (mk_resp.get('data') or {}).get('file_id')
            or (mk_resp.get('data') or {}).get('cid')
            or ''
        ).strip()
        if not temp_cid:
            raise RuntimeError(f'创建成功但未返回 CID: {mk_resp}')

        try:
            if ctx.get('tmdb_id') and ctx.get('title'):
                P115CacheManager.save_transfer_context(
                    root_name=folder_name,
                    tmdb_id=ctx.get('tmdb_id'),
                    media_type='tv',
                    title=ctx.get('title'),
                    season_number=ctx.get('season_number'),
                    source='shared-share-import',
                    source_kind=normalized_kind,
                    source_kinds=[normalized_kind, 'shared_share_import', 'shared_transfer_context'],
                    confidence='high',
                    authority_role='expected',
                    evidence=[f'share:{normalized_kind}:{source_id}'],
                )
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 保存分享转存标准目录上下文失败：{folder_name} -> {e}")

        logger.info(
            f"  ➜ [共享资源] 分享转存启用标准接收目录：{folder_name} "
            f"(cid={temp_cid}, base_cid={base_target_cid})"
        )
        return {
            'target_cid': temp_cid,
            'base_target_cid': base_target_cid,
            'share_import_temp_dir': True,
            # 复用 _cleanup_rapid_temp_dir 的字段，失败时可直接删除空目录。
            'season_package_temp_dir': True,
            'folder_name': folder_name,
            'folder_cid': temp_cid,
            'context': ctx,
            'mkdir_response': mk_resp,
        }
    except Exception as e:
        logger.warning(
            f"  ➜ [共享资源] 创建分享转存标准接收目录失败，拒绝直接转存到待整理根目录："
            f"source={normalized_kind}:{source_id}, err={e}"
        )
        return {
            'target_cid': base_target_cid,
            'base_target_cid': base_target_cid,
            'share_import_temp_dir': False,
            'season_package_temp_dir': False,
            'temp_dir_required': True,
            'temp_dir_error': str(e),
            'folder_name': folder_name,
        }

def _client_call_rapid_transfer_lease(client: SharedCenterClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    method = getattr(client, 'acquire_transfer_lease', None)
    if callable(method):
        return method(payload)
    for name in ('post', '_post'):
        fn = getattr(client, name, None)
        if callable(fn):
            return fn('/api/v1/transfers/lease', payload)
    for name in ('request', '_request'):
        fn = getattr(client, name, None)
        if callable(fn):
            try:
                return fn('POST', '/api/v1/transfers/lease', json=payload)
            except TypeError:
                return fn('POST', '/api/v1/transfers/lease', payload)
    return {'ok': True, 'skipped': True, 'reason': 'lease_client_method_missing'}


def _wait_rapid_transfer_lease_for_fallback(
    client: SharedCenterClient,
    *,
    source_kind: str,
    source_id: str,
    payload: Dict[str, Any],
    event_id: str = '',
    max_wait_seconds: int = 60,
) -> Dict[str, Any]:
    """分享转存失败回退 Rapid 前再申请秒传许可；纯分享路径不会提前等待。"""
    existing = _event_transfer_lease_id(payload)
    if existing:
        return {'ok': True, 'skipped': True, 'reason': 'lease_already_present', 'lease_id': existing}
    source_kind = _normalize_source_kind(source_kind)
    source_id = str(source_id or '').strip()
    if source_kind not in ('movie', 'episode', 'logical_season') or not source_id:
        return {'ok': True, 'skipped': True, 'reason': 'unsupported_source'}

    request_payload = {
        'source_kind': source_kind,
        'source_id': source_id,
        'sha1': None,
        'transfer_mode': 'rapid',
        'request_meta_json': {
            'event_id': str(event_id or ''),
            'event_type': str((payload or {}).get('event_type') or ''),
            'client_gate': 'shared_subscription_service_share_fallback_v1',
            'reason': 'share_import_failed_before_rapid_fallback',
        },
    }
    deadline = time.time() + max(10, int(max_wait_seconds or 60))
    attempts = 0
    last_resp: Dict[str, Any] = {}
    label = str((payload or {}).get('title') or source_id)
    while True:
        attempts += 1
        try:
            resp = _client_call_rapid_transfer_lease(client, request_payload) or {}
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] Rapid 回退秒传许可接口不可用，按旧流程继续：{source_kind}:{source_id}, err={e}")
            return {'ok': True, 'skipped': True, 'reason': 'lease_api_unavailable', 'error': str(e)}
        last_resp = resp if isinstance(resp, dict) else {'raw': resp}
        if last_resp.get('ok') and last_resp.get('allow') is False and not last_resp.get('deferred'):
            logger.info(
                f"  ➜ [共享资源] 分享转存失败后 Rapid 回退被中心明确拒绝："
                f"{source_kind}:{source_id}, reason={last_resp.get('reason') or 'not_allowed'}"
            )
            return {'ok': True, 'blocked': True, 'lease': last_resp, 'attempts': attempts}
        if last_resp.get('allow') or (last_resp.get('ok') and not last_resp.get('deferred') and last_resp.get('allow') is not False):
            lease_id = str(last_resp.get('lease_id') or '').strip()
            if lease_id:
                payload['rapid_transfer_lease_id'] = lease_id
                payload['transfer_lease_id'] = lease_id
            logger.info(f"  ➜ [共享资源] 分享转存失败后 Rapid 回退许可已发放：{label}")
            return {'ok': True, 'lease': last_resp, 'lease_id': lease_id, 'attempts': attempts}

        retry_after = _safe_int(last_resp.get('retry_after'), 30)
        retry_after = max(5, min(retry_after, 120))
        reason = str(last_resp.get('reason') or 'deferred')
        if time.time() + retry_after > deadline:
            logger.warning(
                f"  ➜ [共享资源] 分享转存失败后 Rapid 回退许可等待超时，转入旧流程："
                f"{source_kind}:{source_id}, reason={reason}, last={last_resp}"
            )
            return {'ok': True, 'lease_timeout': True, 'lease': last_resp, 'attempts': attempts}
        logger.debug(f"  ➜ [共享资源] Rapid 回退许可排队中：{label}，{retry_after}s 后重试，reason={reason}")
        time.sleep(retry_after)


def _try_completed_season_share_transfer(
    *,
    client: SharedCenterClient,
    source_id: str,
    payload: Dict[str, Any],
    files: List[Dict[str, Any]],
    target_cid: str,
) -> Dict[str, Any]:
    """逻辑完结季优先走 115 分享转存。

    分享转存不走 Rapid 许可，但不能直接把文件列表丢到待整理根目录。
    必须先在待整理下创建标准剧/季目录，再把 share_import 目标指向该目录，
    这样整理扫描才能按一个项目聚合处理。
    """
    transfer_source_kind = _normalize_source_kind(payload.get('source_kind') or 'logical_season')
    if transfer_source_kind != 'logical_season':
        return {'ok': False, 'skipped': True, 'reason': 'not_season_share_source'}
    channel = _completed_share_channel_for_transfer(client, source_id, payload)
    if not channel:
        return {'ok': False, 'skipped': True, 'reason': 'no_valid_share_channel'}
    share_code = str(channel.get('share_code') or '').strip()
    receive_code = str(channel.get('receive_code') or '').strip()
    channel_id = str(channel.get('channel_id') or '').strip()
    if not share_code:
        return {'ok': False, 'skipped': True, 'reason': 'share_code_missing', 'channel': channel}
    try:
        p115 = P115Service.get_client()
        if not p115:
            raise RuntimeError('115 客户端未初始化')
        title = payload.get('title') or payload.get('share_title') or source_id
        base_target_cid = str(target_cid or '').strip()
        share_target = _prepare_share_import_target_dir(
            base_target_cid=base_target_cid,
            source_kind=transfer_source_kind,
            source_id=source_id,
            payload=payload,
            files=files,
        )
        if share_target.get('temp_dir_required') and not share_target.get('share_import_temp_dir'):
            message = f"创建分享转存标准目录失败，拒绝直接转存到待整理根目录：{share_target.get('temp_dir_error') or 'unknown'}"
            logger.warning(f"  ➜ [共享资源] {message}")
            return {
                'ok': False,
                'attempted': True,
                'transfer_mode': 'share',
                'channel': channel,
                'status': 'temp_dir_failed',
                'message': message,
                'share_target': share_target,
            }

        import_target_cid = str(share_target.get('target_cid') or base_target_cid).strip()
        folder_name = str(share_target.get('folder_name') or '').strip()
        logger.info(
            f"  ➜ [共享资源] 季包优先走 115 分享转存：《{title}》，"
            f"channel={channel_id or '-'}，target_cid={import_target_cid}（标准目录：{folder_name or '-'}）"
        )
        resp = p115.share_import(share_code, receive_code, import_target_cid)
        if _share_import_success(resp):
            receive_title = _share_import_receive_title(resp)
            located_item = _locate_share_imported_item(
                p115,
                parent_cid=import_target_cid,
                receive_title=receive_title,
                max_retries=3,
            ) if receive_title else {}
            located_cid = _p115_item_id(located_item)
            hint_parent_cid = located_cid or import_target_cid
            context_result = _save_share_import_transfer_context(
                root_name=folder_name or receive_title or str(payload.get('title') or title or ''),
                source_id=source_id,
                payload=payload,
                files=files,
                channel_id=channel_id,
                source_kind=transfer_source_kind,
            )
            preid_hint_count = _remember_share_preid_hints(
                files,
                target_cid=hint_parent_cid,
                source_kind=transfer_source_kind,
                source_id=source_id,
                response=resp,
            )
            video_count = len(files or []) or int(channel.get('file_count') or 1)
            report = _client_report_transfer_with_retry_queue(
                client,
                transfer_source_kind,
                source_id,
                'success',
                success_count=video_count,
                total_count=video_count,
                message=(
                    f"本机通过 115 分享转存成功：{video_count} 个视频；"
                    f"channel={channel_id or '-'}；share_import；"
                    f"folder={folder_name or '-'}；receive_title={receive_title or '-'}"
                ),
                transfer_mode='share',
                share_channel_id=channel_id,
            )
            if report.get('pending_report_queued'):
                logger.warning(
                    f"  ➜ [共享资源] 分享转存已成功，但中心上报失败，已加入补报队列："
                    f"source={transfer_source_kind}:{source_id}, channel={channel_id or '-'}"
                )
            _kick_115_organize_detached(reason=f'share:{source_id}', delay=1.0 if located_item else 3.0)
            return {
                'ok': True,
                'transfer_mode': 'share',
                'channel': channel,
                'channel_id': channel_id,
                'response': resp,
                'receive_title': receive_title,
                'located_item': located_item,
                'target_cid': import_target_cid,
                'base_target_cid': base_target_cid,
                'imported_cid': located_cid,
                'preid_hint_parent_cid': hint_parent_cid,
                'standard_folder_name': folder_name,
                'share_target': share_target,
                'report': report,
                'preid_hint_count': preid_hint_count,
                'context_result': context_result,
            }

        status = _share_import_failed_status(resp)
        msg = f"115 分享转存失败，准备回退 Rapid：{resp}"
        cleanup_result = _cleanup_rapid_temp_dir(share_target, reason=msg)

        # 只在确认分享源自身异常时污染中心通道状态。
        # 空间不足/频控/已转存过等都是消费端本机账号问题，不能把共享池 valid 通道改成 import_failed。
        should_update_center = channel_id and status in {'expired', 'pending_review'}
        if should_update_center:
            try:
                client.update_logical_season_share_status(channel_id, {
                    'status': status,
                    'review_status': 'expired' if status == 'expired' else 'pending',
                    'status_message': str(msg)[:1000],
                    'raw_json': {'share_import_response': resp, 'consumer_source_id': source_id, 'failure_scope': 'share_channel', 'share_target': share_target, 'cleanup_result': cleanup_result},
                })
            except Exception as e:
                logger.debug(f"  ➜ [共享资源] 上报分享转存失败状态失败：channel={channel_id}, err={e}")
        elif status == 'local_account_issue':
            logger.warning(
                f"  ➜ [共享资源] 115 分享转存命中本机账号限制/幂等限制，不更新中心通道状态，直接回退 Rapid："
                f"source={source_id}, channel={channel_id or '-'}"
            )
        else:
            logger.warning(
                f"  ➜ [共享资源] 115 分享转存返回未知失败，不污染中心通道状态，先回退 Rapid："
                f"source={source_id}, channel={channel_id or '-'}"
            )
        logger.warning(f"  ➜ [共享资源] {msg}")
        return {'ok': False, 'attempted': True, 'transfer_mode': 'share', 'channel': channel, 'response': resp, 'status': status, 'message': msg, 'share_target': share_target, 'cleanup_result': cleanup_result}
    except Exception as e:
        share_target = locals().get('share_target') if isinstance(locals().get('share_target'), dict) else {}
        cleanup_result = _cleanup_rapid_temp_dir(share_target, reason=f'分享转存异常：{e}') if share_target.get('share_import_temp_dir') else {}
        status = _share_import_failed_status({}, str(e))
        if channel_id and status in {'expired', 'pending_review'}:
            try:
                client.update_logical_season_share_status(channel_id, {
                    'status': status,
                    'review_status': 'expired' if status == 'expired' else 'pending',
                    'status_message': f'115 分享转存异常，准备回退 Rapid：{e}'[:1000],
                    'raw_json': {'share_import_exception': str(e), 'consumer_source_id': source_id, 'failure_scope': 'share_channel', 'share_target': share_target, 'cleanup_result': cleanup_result},
                })
            except Exception:
                pass
        logger.warning(f"  ➜ [共享资源] 115 分享转存异常，准备回退 Rapid：source={source_id}, status={status}, err={e}")
        return {'ok': False, 'attempted': True, 'transfer_mode': 'share', 'channel': channel, 'status': status, 'message': str(e), 'share_target': share_target, 'cleanup_result': cleanup_result}

def _handle_pro_quota_auth_event(client: SharedCenterClient, event: Dict[str, Any], *, ack: bool = True) -> Dict[str, Any]:
    event_id = str((event or {}).get('event_id') or '')
    try:
        resp = client.report_current_pro_quota_auth()
        quota = resp.get('pro_quota') or resp.get('quota') or {}
        tier = quota.get('pro_tier') or quota.get('tier') or '-'
        balance = quota.get('quota_balance') if quota.get('quota_balance') is not None else quota.get('balance')
        cap = quota.get('balance_cap') or quota.get('cap') or 0
        daily = quota.get('daily_grant') or 0
        message = f"Pro额度认证已上报：等级={tier}，今日赠送={daily}，累计={balance}/{cap}"
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='ok', message=message[:500])
            except Exception:
                pass
        logger.debug(f"  ➜ [共享资源] {message}")
        return {'ok': True, 'event_id': event_id, 'event_type': 'pro_quota_auth_check', 'quota': quota, 'message': message}
    except Exception as e:
        msg = f"Pro额度认证上报失败：{e}"
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='failed', message=msg[:500])
            except Exception:
                pass
        logger.warning(f"  ➜ [共享资源] {msg}")
        return {'ok': False, 'event_id': event_id, 'event_type': 'pro_quota_auth_check', 'message': msg}

def consume_device_event(event: Dict[str, Any], *, ack: bool = True) -> Dict[str, Any]:
    client = SharedCenterClient()
    try:
        _drain_pending_transfer_reports(client, limit=5)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 补报历史转存结果失败，跳过本轮：{e}")
    event_id = str(event.get('event_id') or '')
    payload = _event_payload(event)
    lease_id = _event_transfer_lease_id(payload, event)
    event_type = str(event.get('event_type') or payload.get('event_type') or '').strip()
    if event_type == 'pro_quota_auth_check':
        return _handle_pro_quota_auth_event(client, event, ack=ack)

    # 消费端再兜底排除本机共享源；即使手动中心资源库/批量探测返回了 is_mine，
    # 也不能秒传自己的资源形成回旋镖。

    episode_disabled = _event_episode_transfer_disabled_guard(event, payload)
    if episode_disabled.get('blocked'):
        message = episode_disabled.get('message') or '已按配置跳过单集秒传'
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='ok', message=message[:500])
            except Exception:
                pass
        logger.info(f"  ➜ [共享资源] {message}")
        return {
            'ok': False,
            'skipped': True,
            'message': message,
            'event_id': event_id,
            'source_kind': episode_disabled.get('source_kind') or '',
            'source_id': episode_disabled.get('source_id') or '',
            'success_count': 0,
            'total': 0,
            'errors': [],
            'skip_reason': 'episode_transfer_disabled',
            'episode_transfer_filter': {'enabled': True, **episode_disabled},
        }

    source_kind, source_id, files = _event_sources(event, client)
    episode_disabled = _episode_transfer_disabled_guard(source_kind, source_id, payload)
    if episode_disabled.get('blocked'):
        message = episode_disabled.get('message') or '已按配置跳过单集秒传'
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='ok', message=message[:500])
            except Exception:
                pass
        logger.info(f"  ➜ [共享资源] {message}")
        return {
            'ok': False,
            'skipped': True,
            'message': message,
            'event_id': event_id,
            'source_kind': episode_disabled.get('source_kind') or source_kind,
            'source_id': episode_disabled.get('source_id') or source_id,
            'success_count': 0,
            'total': 0,
            'errors': [],
            'skip_reason': 'episode_transfer_disabled',
            'episode_transfer_filter': {'enabled': True, **episode_disabled},
        }
    if not source_kind or not source_id:
        if ack and event_id:
            client.ack_device_events([event_id], result='failed', message='事件缺少 source_kind/source_id')
        return {'ok': False, 'message': '事件缺少 source_kind/source_id', 'event_id': event_id, 'success_count': 0, 'total': 0, 'errors': []}

    if not files:
        message = '中心返回的文件清单为空，无法秒传；如果这是完结季收藏源，请重新手动登记/一键全库登记该季，让中心重建 manifest。'
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='failed', message=message)
            except Exception:
                pass
        return {
            'ok': False, 'message': message, 'event_id': event_id,
            'source_kind': source_kind, 'source_id': source_id,
            'success_count': 0, 'total': 0, 'errors': [{'error': message}],
        }

    original_file_count = len(files)
    requested_missing_episode_numbers = _requested_missing_episodes_from_payload(payload, event)
    files, match_filter = _filter_files_before_transfer(
        client=client,
        source_kind=source_kind,
        source_id=source_id,
        payload=payload,
        files=files,
        requested_missing_episode_numbers=requested_missing_episode_numbers,
    )
    if not files:
        message = match_filter.get('message') or '中心返回的资源无需秒传'
        if ack and event_id:
            try:
                # 这是业务跳过，不是源失效；ACK ok，避免中心反复推同一条事件。
                client.ack_device_events([event_id], result='ok', message=message[:500])
            except Exception:
                pass
        return {
            'ok': False,
            'skipped': True,
            'message': message,
            'event_id': event_id,
            'source_kind': source_kind,
            'source_id': source_id,
            'success_count': 0,
            'total': 0,
            'original_total': original_file_count,
            'errors': [],
            'match_filter': match_filter,
        }

    base_target_cid = _target_cid()

    clean_flag = _center_clean_version_flagged(source_kind, payload, files)
    if _block_clean_version_transfer_enabled() and clean_flag.get('blocked'):
        meta = clean_flag.get('meta') or {}
        message = (
            f"已按配置跳过中心标记的纯净版完结季：{payload.get('title') or source_id}"
            + (f"，命中 {meta.get('hit_count')}/{meta.get('comparable_count')} 集" if meta.get('hit_count') is not None and meta.get('comparable_count') is not None else '')
        )
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='ok', message=message)
            except Exception:
                pass
        return {
            'ok': False,
            'message': message,
            'event_id': event_id,
            'source_kind': source_kind,
            'source_id': source_id,
            'success_count': 0,
            'total': len(files),
            'errors': [message],
            'clean_version_rejected': True,
            'clean_version_filter': {'enabled': True, 'blocked': clean_flag},
        }

    files, preflight = _prepare_files_before_rapid_transfer(
        client,
        source_kind=source_kind,
        source_id=source_id,
        payload=payload,
        files=files,
    )
    if not files:
        message = '共享资源未通过转存前预检'
        if preflight.get('errors'):
            message = str((preflight.get('errors') or [message])[0])
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='ok', message=message[:500])
            except Exception:
                pass
        return {
            'ok': False,
            'message': message,
            'event_id': event_id,
            'source_kind': source_kind,
            'source_id': source_id,
            'success_count': 0,
            'total': 0,
            'errors': preflight.get('errors') or [message],
            'preflight': preflight,
            'washing_rejected': bool(preflight.get('washing_rejected')),
        }

    is_package_transfer = source_kind in ('season_hub', 'logical_season') and len(files) > 1
    payload['source_kind'] = source_kind
    payload['source_id'] = source_id
    payload['source_ref_id'] = source_id

    # 完结季如果已有可用 115 分享通道，先直接转存到待整理根目录；
    # 不等 Rapid 秒传许可，也不提前创建 Rapid 临时目录。
    share_transfer = {}
    if source_kind == 'logical_season':
        share_transfer = _try_completed_season_share_transfer(
            client=client,
            source_id=source_id,
            payload=payload,
            files=files,
            target_cid=base_target_cid,
        )
        if share_transfer.get('ok'):
            if ack and event_id:
                try:
                    client.ack_device_events([event_id], result='ok', message=f"转存 {len(files)}/{len(files)}")
                except Exception as e:
                    logger.debug(f"  ➜ [共享资源] ACK 中心事件失败: {e}")
            return {
                'ok': True,
                'message': f"转存完成：{len(files)}/{len(files)}",
                'event_id': event_id,
                'source_kind': source_kind,
                'source_id': source_id,
                'success_count': len(files),
                'total': len(files),
                'errors': [],
                'transfer_mode': 'share',
                'share_transfer': share_transfer,
                'preflight': locals().get('preflight', {}),
                'rapid_target': {},
            }
        if share_transfer.get('attempted'):
            logger.warning(
                f"  ➜ [共享资源] 季包分享转存未成功，自动回退 Rapid 秒传："
                f"source={source_kind}:{source_id}, reason={share_transfer.get('status') or share_transfer.get('reason') or '-'}"
            )
            # 只有回退 Rapid 时才等待秒传许可；纯分享转存不占用许可队列。
            fallback_lease = _wait_rapid_transfer_lease_for_fallback(
                client,
                source_kind=source_kind,
                source_id=source_id,
                payload=payload,
                event_id=event_id,
            )
            share_transfer['rapid_fallback_lease'] = fallback_lease
            if fallback_lease.get('blocked'):
                lease = fallback_lease.get('lease') if isinstance(fallback_lease.get('lease'), dict) else {}
                message = lease.get('message') or '中心秒传许可拒绝，跳过 Rapid 回退'
                if ack and event_id:
                    try:
                        client.ack_device_events([event_id], result='skipped', message=message[:500])
                    except Exception:
                        pass
                return {
                    'ok': True,
                    'skipped': True,
                    'blocked': True,
                    'blocked_reason': lease.get('reason') or 'transfer_lease_blocked',
                    'message': message,
                    'event_id': event_id,
                    'source_kind': source_kind,
                    'source_id': source_id,
                    'success_count': 0,
                    'total': len(files),
                    'errors': [],
                    'transfer_mode': 'rapid',
                    'share_transfer': share_transfer,
                }
            lease_id = _event_transfer_lease_id(payload, event)

    # 到这里才进入 Rapid 秒传分支；季包秒传必须先创建标准临时剧目录。
    rapid_target = _prepare_rapid_target_dir_for_source(
        base_target_cid=base_target_cid,
        source_kind=source_kind,
        source_id=source_id,
        payload=payload,
        files=files,
    )
    target_cid = str(rapid_target.get('target_cid') or base_target_cid)

    if is_package_transfer and rapid_target.get('temp_dir_required') and not rapid_target.get('season_package_temp_dir'):
        message = f"季包临时接收目录创建失败，放弃本次整季入库：{rapid_target.get('temp_dir_error') or 'unknown'}"
        _report_transfer_failed_safely(client, source_kind=source_kind, source_id=source_id, files=files, errors=[message], message=message, lease_id=lease_id)
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='failed', message=message[:500])
            except Exception:
                pass
        return {
            'ok': False,
            'message': message,
            'event_id': event_id,
            'source_kind': source_kind,
            'source_id': source_id,
            'success_count': 0,
            'total': len(files),
            'errors': [{'error': message}],
            'share_transfer': share_transfer,
            'preflight': locals().get('preflight', {}),
            'rapid_target': rapid_target,
            'aborted_season_package': True,
        }

    ok_count = 0
    errors = []
    success_sources = []
    transfer_token = f"rapid:{event_id or source_id}:{int(time.time() * 1000)}"
    abort_event = threading.Event()

    def _rapid_transfer_one(raw_file: Dict[str, Any]) -> Dict[str, Any]:
        f = dict(raw_file or {})
        f.setdefault('source_kind', source_kind)
        f.setdefault('source_id', source_id)
        f.setdefault('source_ref_id', source_id)
        f['_rapid_transfer_token'] = transfer_token
        f['_rapid_is_package_transfer'] = bool(is_package_transfer)
        file_source_kind = _normalize_source_kind(f.get('source_kind') or source_kind or '')
        file_source_id = str(f.get('source_id') or f.get('source_ref_id') or source_id or '').strip()
        file_label = f.get('file_name') or f.get('name') or f.get('sha1') or 'unknown'
        if abort_event.is_set():
            return {
                'ok': False,
                'file': f,
                'error': {'file': file_label, 'error': 'transfer_aborted_by_center', 'abort_transfer': True},
            }
        try:
            result = rapid_save_file(f, target_cid=target_cid)
            if result.get('ok'):
                result['attempt'] = 1
                return {'ok': True, 'kind': file_source_kind, 'id': file_source_id, 'file': f, 'result': result}
            error = {
                'file': file_label,
                'response': result.get('response'),
                'result': result,
                'attempt': 1,
                'abort_transfer': bool(result.get('abort_transfer')),
            }
            if result.get('no_retry') or result.get('abort_transfer'):
                abort_event.set()
                logger.warning(
                    f"  ➜ [共享资源] 中心已终止本次秒传：{file_label}，"
                    f"reason={result.get('message') or '-'}"
                )
            return {'ok': False, 'file': f, 'error': error}
        except Exception as e:
            return {'ok': False, 'file': f, 'error': {'file': file_label, 'error': str(e), 'attempt': 1}}

    # 完结季/公共季包并发发起签名请求；失败后的重派只由中心端在同一个 sign_job 内完成。
    parallel_transfer = is_package_transfer
    if parallel_transfer:
        max_workers = max(1, min(len(files), 8))
        logger.info(f"  ➜ [共享资源] 季包秒传启用并发签名调度：files={len(files)}, workers={max_workers}, local_retries=0")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='shared-rapid-transfer') as executor:
            future_map = {executor.submit(_rapid_transfer_one, f): f for f in files}
            for future in concurrent.futures.as_completed(future_map):
                try:
                    item = future.result()
                except concurrent.futures.CancelledError:
                    continue
                if item.get('ok'):
                    ok_count += 1
                    success_sources.append((item.get('kind'), item.get('id'), item.get('file') or {}))
                else:
                    errors.append(item.get('error') or {'file': (item.get('file') or {}).get('sha1'), 'error': 'unknown'})
                    if (item.get('error') or {}).get('abort_transfer'):
                        abort_event.set()
                        for other in future_map:
                            if other is not future:
                                other.cancel()
    else:
        for f in files:
            if abort_event.is_set():
                break
            item = _rapid_transfer_one(f)
            if item.get('ok'):
                ok_count += 1
                success_sources.append((item.get('kind'), item.get('id'), item.get('file') or {}))
            else:
                errors.append(item.get('error') or {'file': (item.get('file') or {}).get('sha1'), 'error': 'unknown'})
                if (item.get('error') or {}).get('abort_transfer'):
                    abort_event.set()
                    break

    report_errors = []
    report_results = []
    skipped_report_sources = []
    cleanup_result = {}

    # 季包必须全量成功。任何一集由中心判定不可签名/不可秒传，整季放弃入库，删除临时目录，
    # 不上报 success，不触发整理，避免 8/9 半季污染媒体库。消费端贡献点也不会被扣除。
    if is_package_transfer and ok_count != len(files):
        message = f'季包秒传不完整，已放弃整季入库：成功 {ok_count}/{len(files)}，失败 {len(errors)} 个文件'
        cleanup_result = _cleanup_rapid_temp_dir(rapid_target, reason=message)
        fail_report = _report_transfer_failed_safely(
            client,
            source_kind=source_kind,
            source_id=source_id,
            files=files,
            errors=errors,
            message=message,
            lease_id=lease_id,
        )
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='failed', message=message[:500])
            except Exception as e:
                logger.debug(f"  ➜ [共享资源] ACK 中心事件失败: {e}")
        logger.warning(f"  ➜ [共享资源] {message}；临时目录清理结果：{cleanup_result}")
        return {
            'ok': False,
            'message': message,
            'event_id': event_id,
            'source_kind': source_kind,
            'source_id': source_id,
            'success_count': 0,
            'rapid_success_count': ok_count,
            'total': len(files),
            'errors': errors,
            'failed_report': fail_report,
            'cleanup_result': cleanup_result,
            'preflight': locals().get('preflight', {}),
            'rapid_target': locals().get('rapid_target', {}),
            'aborted_season_package': True,
        }

    # 不在“秒传成功”阶段登记 holder。文件只是落入待整理目录，尚未完成整理入库；
    # 只有 webhook 入库后触发自动共享登记，才代表本机真正具备可签名能力。
    if ok_count:
        report_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
        if is_package_transfer and source_kind == 'logical_season' and ok_count == len(files):
            report_groups[(source_kind, source_id)] = {'count': len(files), 'file': (files[0] if files else {})}
        else:
            for report_kind, report_id, report_file in success_sources:
                report_kind = _normalize_source_kind(report_kind)
                if report_kind not in ('movie', 'episode', 'logical_episode', 'logical_season') or not report_id:
                    skipped_report_sources.append({'source_kind': report_kind, 'source_id': report_id, 'file': (report_file or {}).get('file_name') or (report_file or {}).get('sha1')})
                    continue
                key = (report_kind, report_id)
                group = report_groups.setdefault(key, {'count': 0, 'file': report_file})
                group['count'] += 1
        if not report_groups:
            logger.warning(
                f"  ➜ [共享资源] 秒传已成功但没有可上报的中心源，热度不会增加："
                f"source={source_kind}:{source_id}, skipped={skipped_report_sources[:5]}"
            )
        for (report_kind, report_id), group in report_groups.items():
            report_file = group.get('file') or {}
            success_file_count = max(1, int(group.get('count') or 1))
            report_resp = _client_report_transfer_with_retry_queue(
                client,
                report_kind,
                report_id,
                'success',
                success_count=success_file_count,
                total_count=success_file_count,
                message=f'本机秒传成功：{success_file_count} 个视频；{report_file.get("file_name") or report_file.get("sha1") or report_id}',
                lease_id=lease_id,
            )
            report_results.append({'source_kind': report_kind, 'source_id': report_id, **(report_resp or {})})
            if report_resp and report_resp.get('inserted') is False:
                logger.info(f"  ➜ [共享资源] 秒传成功已上报过，本次不重复增加热度：{report_kind}:{report_id}")
            if report_resp.get('pending_report_queued'):
                err = {'source_kind': report_kind, 'source_id': report_id, 'queued': True, 'error': report_resp.get('error')}
                report_errors.append(err)
                logger.warning(f"  ➜ [共享资源] 上报秒传成功失败，已加入补报队列，热度/扣点稍后补齐: {err}")
        _kick_115_organize_detached(reason=f'rapid:{source_kind}:{source_id}')
    else:
        _report_transfer_failed_safely(
            client,
            source_kind=source_kind,
            source_id=source_id,
            files=files,
            errors=errors,
            message=json.dumps(errors, ensure_ascii=False)[:1000],
            lease_id=lease_id,
        )

    if ack and event_id:
        try:
            client.ack_device_events([event_id], result='ok' if ok_count else 'failed', message=f'秒传 {ok_count}/{len(files)}')
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] ACK 中心事件失败: {e}")

    message = f'秒传完成：{ok_count}/{len(files)}' if ok_count else (errors[0].get('error') if errors and isinstance(errors[0], dict) and errors[0].get('error') else f'秒传失败：0/{len(files)}')
    return {
        'ok': ok_count > 0, 'message': message, 'event_id': event_id,
        'source_kind': source_kind, 'source_id': source_id,
        'success_count': ok_count, 'total': len(files), 'errors': errors,
        'report_results': report_results, 'report_errors': report_errors,
        'skipped_report_sources': skipped_report_sources,
        'preflight': locals().get('preflight', {}),
        'rapid_target': locals().get('rapid_target', {}),
        'share_transfer': locals().get('share_transfer', {}),
    }


def poll_and_consume_once(timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    client = SharedCenterClient()
    if not client.ready:
        return {'ok': False, 'message': '共享中心未配置'}
    try:
        _drain_pending_transfer_reports(client, limit=_PENDING_TRANSFER_REPORT_DEFAULT_DRAIN_LIMIT)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 补报历史转存结果失败，跳过本轮：{e}")
    resp = client.poll_device_events(timeout=timeout, limit=limit)
    events = resp.get('items') or resp.get('events') or []
    results = [consume_device_event(event) for event in events]
    return {'ok': True, 'event_count': len(events), 'results': results}


def _build_gap_query(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='') -> Dict[str, Any]:
    item = item or {}
    typ = item_type or item.get('item_type') or 'Movie'
    if typ == 'Episode':
        typ = 'Season'
    parent = parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
    query_tmdb = str(parent or tmdb_id or item.get('tmdb_id') or '').strip()
    if typ == 'Movie':
        query_tmdb = str(tmdb_id or item.get('tmdb_id') or '').strip()
    return {
        'tmdb_id': query_tmdb,
        'item_type': typ,
        'season_number': season_number if season_number not in (None, '') else item.get('season_number'),
        'episode_number': None,
        'title': title or item.get('title'),
        'release_year': year or item.get('release_year'),
    }


def _probe_subscriptions_batch_no_gap(client: SharedCenterClient, queries: List[Dict[str, Any]], limit_per_item: int = 200) -> Dict[str, Any]:
    """只查询共享池候选，不登记缺口。

    新中心端已取消普通缺口登记；这里额外向兼容实现传递禁用标记。
    如果旧客户端封装不支持这些关键字，则退回旧签名，但本函数自身绝不再调用
    report_gaps。
    """
    safe_queries = []
    for q in queries or []:
        if not isinstance(q, dict):
            continue
        item = dict(q)
        item['_disable_gap_report'] = True
        item['disable_gap_report'] = True
        item['report_gap'] = False
        item['register_gap'] = False
        safe_queries.append(item)

    try:
        return client.probe_subscriptions_batch(
            safe_queries,
            limit_per_item=limit_per_item,
            report_gap=False,
            disable_gap_report=True,
            register_gap=False,
        )
    except TypeError:
        try:
            return client.probe_subscriptions_batch(
                safe_queries,
                limit_per_item=limit_per_item,
                report_gap=False,
            )
        except TypeError:
            try:
                return client.probe_subscriptions_batch(
                    safe_queries,
                    limit_per_item=limit_per_item,
                    disable_gap_report=True,
                )
            except TypeError:
                return client.probe_subscriptions_batch(safe_queries, limit_per_item=limit_per_item)


def _normalize_probe_item_for_center(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把 subscriptions.py 传来的 prepared context 转成中心查询对象。

    subscriptions.py 传入的对象通常长这样：
    {item, tmdb_id, item_type, title, season_number, parent_tmdb_id, year}
    其中 Season 的 tmdb_id 可能是季自身 ID；中心 Rapid v2 必须使用父剧 TMDb + season_number。
    """
    raw = raw or {}
    if isinstance(raw.get('item'), dict):
        return _build_gap_query(
            raw.get('item') or {},
            title=raw.get('title') or '',
            tmdb_id=raw.get('tmdb_id'),
            item_type=raw.get('item_type') or '',
            parent_tmdb_id=raw.get('parent_tmdb_id') or raw.get('parent_series_tmdb_id') or raw.get('series_tmdb_id'),
            season_number=raw.get('season_number'),
            year=raw.get('year') or raw.get('release_year') or '',
        )
    return _build_gap_query(
        raw,
        title=raw.get('title') or '',
        tmdb_id=raw.get('tmdb_id'),
        item_type=raw.get('item_type') or '',
        parent_tmdb_id=raw.get('parent_tmdb_id') or raw.get('parent_series_tmdb_id') or raw.get('series_tmdb_id'),
        season_number=raw.get('season_number'),
        year=raw.get('year') or raw.get('release_year') or '',
    )


def batch_probe_shared_resources(items: List[Dict[str, Any]], limit_per_item: int = 200) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'supported': False, 'items': [], 'message': 'shared center disabled', 'by_key': {}}
    client = SharedCenterClient()
    queries = [_normalize_probe_item_for_center(x) for x in (items or []) if isinstance(x, dict)]
    queries = [q for q in queries if q.get('tmdb_id') and q.get('item_type')]
    if not queries:
        return {'supported': True, 'items': [], 'hit_count': 0, 'gap_count': 0, 'by_key': {}}
    resp = _probe_subscriptions_batch_no_gap(client, queries, limit_per_item=limit_per_item)
    # subscriptions.py 期待 by_key；这里按 Rapid v2 规范键补一个映射。
    by_key = {}
    for row in resp.get('items') or resp.get('results') or []:
        query = row.get('query') if isinstance(row.get('query'), dict) else {}
        key = row.get('request_key') or query.get('request_key')
        if not key:
            key = _subscription_probe_request_key(query)
        if key:
            by_key[key] = row
    resp['by_key'] = by_key
    resp.setdefault('supported', True)
    return resp


def _subscription_probe_request_key(query: Dict[str, Any]) -> str:
    query = query or {}
    return '|'.join([
        str(query.get('item_type') or ''),
        str(query.get('tmdb_id') or ''),
        str(query.get('season_number') if query.get('season_number') is not None else ''),
        str(query.get('episode_number') if query.get('episode_number') is not None else ''),
    ])


def _flatten_sources_from_probe(resp_or_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp_or_row or {}
    if isinstance(data.get('sources'), list):
        return [x for x in data.get('sources') or [] if isinstance(x, dict)]
    out = []
    for item in data.get('items') or data.get('results') or []:
        for src in item.get('sources') or []:
            if isinstance(src, dict):
                out.append(src)
    return out


def _consume_sources(
    sources: List[Dict[str, Any]],
    *,
    report_gap: bool = False,
    missing_episode_numbers: List[int] = None,
    consume_context: Dict[str, Any] = None,
) -> Dict[str, Any]:
    if not sources:
        return {'enabled': True, 'success': False, 'reported_gap': False, 'mode': 'rapid', 'count': 0}
    ok = 0
    errors = []
    skipped = []
    tried = 0
    missing_episode_numbers = _normalize_episode_numbers(missing_episode_numbers)
    consume_context = dict(consume_context or {})
    if missing_episode_numbers:
        consume_context['missing_episode_numbers'] = missing_episode_numbers

    for src in sources[:20]:
        tried += 1
        payload = dict(src or {})
        if consume_context:
            payload['_consume_context'] = consume_context
        if missing_episode_numbers:
            # 统一订阅已经知道缺集时，必须把缺集号透传到消费层；
            # season_hub / completed_season 会在 RAW/洗版预检前按集号裁剪。
            payload['_requested_missing_episode_numbers'] = missing_episode_numbers
        event_source_kind = payload.get('source_kind') or payload.get('kind')
        if not event_source_kind and (payload.get('hub_id') or payload.get('is_season_hub')):
            event_source_kind = 'season_hub'
        if not event_source_kind:
            event_source_kind = payload.get('item_type') or payload.get('display_type')
        event_source_kind = _normalize_source_kind(event_source_kind)
        if event_source_kind == 'completed_season' and payload.get('hub_id') and not payload.get('source_id'):
            event_source_kind = 'season_hub'

        event = {
            'event_id': '',
            'source_kind': event_source_kind,
            'source_ref_id': (
                payload.get('source_id')
                or payload.get('source_ref_id')
                or payload.get('hub_id')
                or payload.get('id')
            ),
            'payload_json': payload,
        }
        result = consume_device_event(event, ack=False)
        if result.get('ok'):
            ok += int(result.get('success_count') or 1)
            # 电影 / 单集命中一个即可；完结季一次事件会包含多文件。
            if payload.get('source_kind') in ('movie', 'episode'):
                break
        elif result.get('skipped'):
            skipped.append({
                'source_id': payload.get('source_id') or payload.get('source_ref_id'),
                'message': result.get('message'),
                'match_filter': result.get('match_filter'),
            })
        else:
            errors.extend(result.get('errors') or [{'source_id': payload.get('source_id'), 'message': result.get('message')}])
    return {
        'enabled': True,
        'success': ok > 0,
        'reported_gap': False,
        'mode': 'rapid',
        'action_type': '共享资源秒传',
        'count': ok,
        'tried_sources': tried,
        'skipped_sources': skipped,
        'missing_episode_numbers': missing_episode_numbers,
        'errors': errors,
    }


def try_consume_shared_resource(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='', missing_episode_numbers=None, **_kwargs) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'success': False, 'reported_gap': False}
    query = _build_gap_query(item or {}, title, tmdb_id, item_type, parent_tmdb_id, season_number, year)
    client = SharedCenterClient()
    try:
        resp = _probe_subscriptions_batch_no_gap(client, [query], limit_per_item=50)
        sources = _flatten_sources_from_probe(resp)
        return _consume_sources(
            sources,
            report_gap=False,
            missing_episode_numbers=missing_episode_numbers,
            consume_context={
                'title': title,
                'tmdb_id': tmdb_id,
                'item_type': item_type,
                'parent_tmdb_id': parent_tmdb_id,
                'season_number': season_number,
                'year': year,
            },
        )
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 秒传中心资源失败: {e}")
        return {'enabled': True, 'success': False, 'reported_gap': False, 'message': str(e)}


def try_consume_preprobed_shared_resource(probe_row: Dict[str, Any] = None, item: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    sources = _flatten_sources_from_probe(probe_row or {})
    if sources:
        return _consume_sources(
            sources,
            report_gap=False,
            missing_episode_numbers=kwargs.get('missing_episode_numbers'),
            consume_context=kwargs,
        )
    # 未命中时不再登记缺口，等待中心端入池广播事件。
    return {'enabled': True, 'success': False, 'reported_gap': False, 'mode': 'rapid', 'count': 0}



def consume_center_source_payload(source: Dict[str, Any], mode: str = 'rapid', context: Dict[str, Any] = None) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'ok': False, 'success': False, 'message': '共享资源未启用'}
    source = dict(source or {})
    if context:
        for k, v in dict(context or {}).items():
            source.setdefault(k, v)
    source_kind = _normalize_source_kind(source.get('source_kind') or source.get('kind') or '')
    source_id = str(source.get('source_id') or source.get('source_ref_id') or source.get('episode_source_id') or '').strip()
    if not source_kind:
        source_kind = _normalize_source_kind(source.get('item_type') or source.get('display_type') or '')
    if not source_id and source_kind == 'episode':
        source_id = str(source.get('episode_source_id') or '').strip()
    if not source_kind or not source_id:
        return {'enabled': True, 'ok': False, 'success': False, 'message': '中心源缺少 source_kind/source_id，无法秒传'}
    if mode:
        source.setdefault('preferred_transfer_mode', str(mode or '').strip().lower())
        source.setdefault('transfer_mode', str(mode or '').strip().lower())
    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    result = consume_device_event(event, ack=False)
    result['success'] = bool(result.get('ok'))
    result['count'] = int(result.get('success_count') or 0)
    result['action_type'] = '共享资源转存' if result.get('transfer_mode') == 'share' else '共享资源秒传'
    return result


def consume_center_sources(source_ids: List[str] = None, mode: str = 'rapid', context: Dict[str, Any] = None, source: Dict[str, Any] = None) -> Dict[str, Any]:
    if isinstance(source, dict) and source:
        return consume_center_source_payload(source, mode=mode, context=context)
    ids = [str(x or '').strip() for x in (source_ids or []) if str(x or '').strip()]
    if not ids:
        return {'enabled': True, 'success': False, 'ok': False, 'message': '缺少 Rapid v2 source payload'}
    client = SharedCenterClient()
    try:
        resp = client.list_sources(source_ids=ids, limit=max(len(ids), 1))
        sources = [x for x in (resp.get('items') or []) if isinstance(x, dict)]
    except Exception as e:
        return {'enabled': True, 'success': False, 'ok': False, 'message': f'按 source_id 查询中心源失败: {e}'}
    return _consume_sources(sources, report_gap=False)
