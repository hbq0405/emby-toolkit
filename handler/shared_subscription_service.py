# handler/shared_subscription_service.py
# Rapid v2 共享资源消费入口：中心调度，本机 CK 执行秒传/入库。
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
            logger.info(f"  ➜ [共享资源] 异步触发 115 待整理扫描: {reason or 'rapid-import'}")
            task_scan_and_organize_115()
        except Exception as e:
            logger.error(f"  ➜ [共享资源] 异步触发 115 待整理扫描失败: {e}", exc_info=True)

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
    if text in ('completed_season', 'season', 'season_pack', 'tv_pack', 'pack'):
        return 'completed_season'
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
    if normalized_kind not in ('completed_season', 'season_hub') or len(files) <= 1:
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


def _report_transfer_failed_safely(
    client: SharedCenterClient,
    *,
    source_kind: str,
    source_id: str,
    files: List[Dict[str, Any]],
    errors: List[Any],
    message: str = '',
) -> Dict[str, Any]:
    fail_kind = _normalize_source_kind(source_kind)
    if fail_kind not in ('movie', 'episode', 'completed_season') or not source_id:
        return {'ok': False, 'skipped': True, 'reason': 'unsupported_source_kind'}
    try:
        return client.report_transfer(
            fail_kind,
            source_id,
            'failed',
            success_count=0,
            total_count=len(files or []),
            message=(message or json.dumps(errors or [], ensure_ascii=False))[:1000],
        ) or {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 上报秒传失败失败：{fail_kind}:{source_id} -> {e}")
        return {'ok': False, 'error': str(e)}


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


def _register_local_rapid_holder(client: SharedCenterClient, *, source_kind: str, source_id: str, file_info: Dict[str, Any], message_prefix: str = '') -> None:
    try:
        meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
        sha1 = _norm_sha1(file_info.get('sha1') or meta.get('sha1'))
        if not sha1:
            return
        client.register_rapid_sign_holder({
            'sha1': sha1,
            'size': _rapid_size_to_int(file_info.get('size') or file_info.get('file_size') or meta.get('size'), 0) or None,
            'source_kind': source_kind or file_info.get('source_kind') or '',
            'source_id': source_id or file_info.get('source_id') or file_info.get('source_ref_id') or '',
            'file_name': file_info.get('file_name') or file_info.get('name') or meta.get('file_name') or '',
            'preid': file_info.get('preid') or meta.get('preid') or '',
            'meta_json': {'from': 'rapid_transfer_success'},
        })
        logger.debug(f"  ➜ [负载均衡签名] 已登记本机为源客户端")
    except Exception as e:
        logger.debug(f"  ➜ [负载均衡签名] 登记本机 holder 失败: {e}")


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
        'request_meta_json': {'stage': sign_req.get('stage') or '', 'target_cid': target_cid},
    })
    job_id = str(create_resp.get('job_id') or (create_resp.get('job') or {}).get('job_id') or '').strip()
    holder_id = str(create_resp.get('holder_id') or (create_resp.get('job') or {}).get('holder_id') or '').strip()
    if not job_id:
        raise RuntimeError(f'中心未返回 sign_job id: {create_resp}')
    logger.info(f"  ➜ [负载均衡签名] 签名任务已创建：等待源客户端签名...")
    wait_resp = client.wait_rapid_sign_job(job_id, timeout=75)
    status = str(wait_resp.get('status') or (wait_resp.get('job') or {}).get('status') or '')
    sign_val = str(wait_resp.get('sign_val') or (wait_resp.get('job') or {}).get('sign_val') or '').strip().upper()
    if status != 'done' or not _norm_sha1(sign_val):
        logger.warning(f"  ➜ [负载均衡签名] sign_job 未完成：job_id={job_id}, status={status}, resp={str(wait_resp)[:500]}")
        return {'ok': False, 'response': first_resp, 'sign_job': wait_resp, 'message': f'sign_job 未完成: {status}'}

    signed_meta = dict(rapid_meta or {})
    signed_meta['sign_key'] = sign_req.get('sign_key')
    signed_meta['sign_val'] = sign_val
    logger.info(
        f"  ➜ [负载均衡签名] 已收到签名，开始秒传："
        f"job_id={job_id}, sign_val={sign_val[:12]}..., file={file_name}"
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

    for sha1 in missing:
        if sha1 in raw_map:
            continue
        try:
            resp = client.get_raw_ffprobe(sha1)
            raw = (resp or {}).get('raw_ffprobe_json') or (resp or {}).get('raw') or {}
            if isinstance(raw, dict) and raw:
                raw_map[sha1] = raw
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 拉取中心 RAW 失败: sha1={sha1[:12]}..., err={e}")
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


def _washing_new_level(sha1: str, file_name: str, file_size: int, target_cid: str,
                       media_type: str, original_lang: str = '', has_external_subtitle: bool = False):
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
        norm_new = WashingService._normalize_info(new_info)
        db_media_type = 'Movie' if str(media_type).lower() == 'movie' else 'Series'
        priorities = WashingService._load_priorities(db_media_type, target_cid)
        if not priorities:
            return 999, '未配置优先级规则'
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
            logger.info(
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
    logger.info(f"  ➜ [共享资源] 秒传前预检：开始拉取中心 RAW，source={source_label}, files={len(files)}")
    raw_map = _load_center_raw_map(client, files)
    logger.info(
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
    logger.info(
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

    logger.info(
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
            f"{file_name}，sha1={(sha1[:12] + '...') if sha1 else '-'}"
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
            logger.info(
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
                logger.info(
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
        logger.info(
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
        logger.info(
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
        logger.info(f"  ➜ [共享资源] 洗版预检[{idx + 1}/{len(files)}] 计算评分：{file_name}")
        level, level_reason = _washing_new_level(
            sha1,
            file_name,
            file_size,
            str(target_cid_for_washing),
            media_type,
            original_lang=original_lang,
            has_external_subtitle=False,
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
        f"  ➜ [共享资源] 洗版预检通过：source={source_label}, "
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
    logger.info(f"  ➜ [共享资源] 准备执行 115 秒传：{file_name}, sha1={sha1[:8]}..., preid={(preid[:8] + '...') if preid else '-'}, size={size}, target_cid={target_cid}")
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
                return retry
            resp = retry.get('response') or resp
        except Exception as e:
            logger.warning(f"  ➜ [负载均衡签名] 中心 holder 签名闭环失败：{e}")

    return {'ok': False, 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}


def _event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get('payload_json') if isinstance(event.get('payload_json'), dict) else None
    if payload is None:
        payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    return dict(payload or {})


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

    # display-list 里的 Pack 如果是公共连载季壳，通常只有 hub_id，没有 completed source_id。
    # 这种壳不能走 completed_season_manifest，否则会拿不到 7-8 这类 children 分集。
    if source_kind == 'completed_season' and payload.get('hub_id') and not payload.get('source_id'):
        source_kind = 'season_hub'
        source_id = str(payload.get('hub_id') or source_id or '').strip()

    # 兼容中心返回的 completed season 包：列表接口只给源摘要，真正文件清单要再取 manifest。
    # 如果 manifest 为空，不能再显示“秒传完成 0/0”，这属于 manifest 缺失/旧数据，需要重新登记该季。
    if source_kind == 'completed_season':
        manifest = client.completed_season_manifest(source_id)
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
            message = f"本地完结季库存所有分集均已是洗版优先级 1，跳过整季秒传：{payload.get('title') or source_id}"
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
            'message': '完结季未达到整包库存优先级 1，继续整季洗版预检。',
            'best_count': sum(1 for x in completed_video_checks if x.get('best')),
            'kept_count': len(files),
        }

    if best_skips:
        logger.debug(
            f"  ➜ [共享资源] 洗版优先级对比：source={source_label}，"
            f"本地已是优先级1，跳过 {len(best_skips)} 个，保留 {len(kept)} 个进入预检。"
        )

    reason = 'inventory_best_level_1' if files and not kept and best_skips else ''
    message = '本地库存已是洗版优先级 1，跳过共享秒传。' if reason else 'replace 库存优先级检查完成。'
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
            message = '中心返回的是本机共享源，已跳过，避免秒传自己的资源。'
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
            message = '中心返回的是本机共享源，已跳过，避免秒传自己的资源。'
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
        logger.info(f"  ➜ [共享资源] {message}")
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
    event_id = str(event.get('event_id') or '')
    payload = _event_payload(event)
    event_type = str(event.get('event_type') or payload.get('event_type') or '').strip()
    if event_type == 'pro_quota_auth_check':
        return _handle_pro_quota_auth_event(client, event, ack=ack)

    # 消费端再兜底排除本机共享源；即使手动中心资源库/批量探测返回了 is_mine，
    # 也不能秒传自己的资源形成回旋镖。

    source_kind, source_id, files = _event_sources(event, client)
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

    rapid_target = _prepare_rapid_target_dir_for_source(
        base_target_cid=base_target_cid,
        source_kind=source_kind,
        source_id=source_id,
        payload=payload,
        files=files,
    )
    target_cid = str(rapid_target.get('target_cid') or base_target_cid)

    is_package_transfer = source_kind in ('completed_season', 'season_hub') and len(files) > 1
    if is_package_transfer and rapid_target.get('temp_dir_required') and not rapid_target.get('season_package_temp_dir'):
        message = f"季包临时接收目录创建失败，放弃本次整季入库：{rapid_target.get('temp_dir_error') or 'unknown'}"
        _report_transfer_failed_safely(client, source_kind=source_kind, source_id=source_id, files=files, errors=[message], message=message)
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
            'preflight': locals().get('preflight', {}),
            'rapid_target': rapid_target,
            'aborted_season_package': True,
        }

    ok_count = 0
    errors = []
    success_sources = []
    RAPID_TRANSFER_MAX_RETRIES = 3

    def _rapid_transfer_one(raw_file: Dict[str, Any]) -> Dict[str, Any]:
        f = dict(raw_file or {})
        f.setdefault('source_kind', source_kind)
        f.setdefault('source_id', source_id)
        f.setdefault('source_ref_id', source_id)
        file_source_kind = _normalize_source_kind(f.get('source_kind') or source_kind or '')
        file_source_id = str(f.get('source_id') or f.get('source_ref_id') or source_id or '').strip()
        file_label = f.get('file_name') or f.get('name') or f.get('sha1') or 'unknown'
        last_error = None
        attempts = RAPID_TRANSFER_MAX_RETRIES + 1
        for attempt in range(1, attempts + 1):
            try:
                if attempt > 1:
                    logger.warning(
                        f"  ➜ [共享资源] 秒传重试 {attempt - 1}/{RAPID_TRANSFER_MAX_RETRIES}：{file_label}"
                    )
                    time.sleep(min(1.5 * attempt, 6.0))
                result = rapid_save_file(f, target_cid=target_cid)
                if result.get('ok'):
                    result['attempt'] = attempt
                    return {'ok': True, 'kind': file_source_kind, 'id': file_source_id, 'file': f, 'result': result}
                last_error = {'file': file_label, 'response': result.get('response'), 'result': result, 'attempt': attempt}
            except Exception as e:
                last_error = {'file': file_label, 'error': str(e), 'attempt': attempt}
        return {'ok': False, 'file': f, 'error': last_error or {'file': file_label, 'error': 'unknown'}}

    # 完结季/公共季包通常会触发多文件 status=7 签名；并发发起秒传，中心才能把 sign_job
    # 同时派给多个 holder，避免一集一集串行等待。单文件/电影仍走轻量串行。
    parallel_transfer = is_package_transfer
    if parallel_transfer:
        max_workers = max(1, min(len(files), 8))
        logger.info(f"  ➜ [共享资源] 季包秒传启用并发签名调度：files={len(files)}, workers={max_workers}, retries={RAPID_TRANSFER_MAX_RETRIES}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='shared-rapid-transfer') as executor:
            future_map = {executor.submit(_rapid_transfer_one, f): f for f in files}
            for future in concurrent.futures.as_completed(future_map):
                item = future.result()
                if item.get('ok'):
                    ok_count += 1
                    success_sources.append((item.get('kind'), item.get('id'), item.get('file') or {}))
                else:
                    errors.append(item.get('error') or {'file': (item.get('file') or {}).get('sha1'), 'error': 'unknown'})
    else:
        for f in files:
            item = _rapid_transfer_one(f)
            if item.get('ok'):
                ok_count += 1
                success_sources.append((item.get('kind'), item.get('id'), item.get('file') or {}))
            else:
                errors.append(item.get('error') or {'file': (item.get('file') or {}).get('sha1'), 'error': 'unknown'})

    report_errors = []
    report_results = []
    skipped_report_sources = []
    cleanup_result = {}

    # 季包必须全量成功。任何一集连续重试后仍失败，整季放弃入库，删除临时目录，
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

    # 到这里才说明成功文件会留在网盘里；此时再登记本机 holder，避免季包失败清理后留下假 holder。
    for report_kind, report_id, report_file in success_sources:
        _register_local_rapid_holder(client, source_kind=report_kind, source_id=report_id, file_info=report_file or {})

    if ok_count:
        report_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for report_kind, report_id, report_file in success_sources:
            report_kind = _normalize_source_kind(report_kind)
            if report_kind not in ('movie', 'episode', 'completed_season') or not report_id:
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
            try:
                report_resp = client.report_transfer(
                    report_kind,
                    report_id,
                    'success',
                    success_count=success_file_count,
                    total_count=success_file_count,
                    message=f'本机秒传成功：{success_file_count} 个视频；{report_file.get("file_name") or report_file.get("sha1") or report_id}',
                )
                report_results.append({'source_kind': report_kind, 'source_id': report_id, **(report_resp or {})})
                if report_resp and report_resp.get('inserted') is False:
                    logger.info(f"  ➜ [共享资源] 秒传成功已上报过，本次不重复增加热度：{report_kind}:{report_id}")
            except Exception as e:
                err = {'source_kind': report_kind, 'source_id': report_id, 'error': str(e)}
                report_errors.append(err)
                logger.warning(f"  ➜ [共享资源] 上报秒传成功失败，热度不会增加: {err}")
        _kick_115_organize_detached(reason=f'rapid:{source_kind}:{source_id}')
    else:
        _report_transfer_failed_safely(
            client,
            source_kind=source_kind,
            source_id=source_id,
            files=files,
            errors=errors,
            message=json.dumps(errors, ensure_ascii=False)[:1000],
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
    }


def poll_and_consume_once(timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    client = SharedCenterClient()
    if not client.ready:
        return {'ok': False, 'message': '共享中心未配置'}
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


def report_shared_gap(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='') -> bool:
    """普通缺口登记已废弃。

    Rapid v2 现在由中心端“有效资源入池广播”驱动消费端补缺/洗版，
    客户端不再向中心写 wanted_gaps / wanted_gap_devices。保留函数只为兼容
    旧导入，避免其他模块 import 失败。
    """
    logger.debug(
        "  ➜ [共享资源] 普通缺口登记已废弃，跳过 report_shared_gap："
        f"title={title or (item or {}).get('title') or '-'}, "
        f"tmdb={tmdb_id or (item or {}).get('tmdb_id') or '-'}, item_type={item_type or (item or {}).get('item_type') or '-'}"
    )
    return False


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


def _reported_gap_from_probe(resp_or_row: Dict[str, Any]) -> bool:
    """普通缺口登记已废弃，探测结果里的 gap 字段统一忽略。"""
    return False


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
    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    result = consume_device_event(event, ack=False)
    result['success'] = bool(result.get('ok'))
    result['count'] = int(result.get('success_count') or 0)
    result['action_type'] = '共享资源秒传'
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
