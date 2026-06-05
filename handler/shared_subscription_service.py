# handler/shared_subscription_service.py
# 统一订阅共享资源消费入口：登记缺口、优先从中心共享池永久转存。
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Tuple

import config_manager
import constants
from database.connection import get_db_connection
from database import settings_db, shared_share_db
from handler.p115_service import P115Service, P115CacheManager, SmartOrganizer
from handler.p115_media_analyzer import P115MediaAnalyzerMixin
from handler.shared_center_client import SharedCenterClient, shared_center_enabled, shared_resource_mode

logger = logging.getLogger(__name__)

VIDEO_EXTS = {'.mkv', '.mp4', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.flv', '.rmvb', '.webm', '.iso'}
_ORGANIZE_KICK_LOCK = threading.Lock()
_LAST_ORGANIZE_KICK_AT = 0
def _kick_115_organize_detached(reason: str = '', delay: float = 3.0) -> Dict[str, Any]:
    """共享资源永久转存成功后，绕过单线程 task_manager，异步踢 115 待整理扫描。"""
    global _LAST_ORGANIZE_KICK_AT

    now = time.time()
    with _ORGANIZE_KICK_LOCK:
        if now - _LAST_ORGANIZE_KICK_AT < 10:
            return {
                'started': False,
                'message': '115 整理扫描刚触发过，本次不重复启动',
            }
        _LAST_ORGANIZE_KICK_AT = now

    def _runner():
        if delay and delay > 0:
            time.sleep(delay)
        try:
            from tasks.p115 import task_scan_and_organize_115
            logger.info(f"  ➜ [共享资源] 异步触发 115 待整理扫描: {reason or 'shared-permanent-import'}")
            task_scan_and_organize_115()
        except Exception as e:
            logger.error(f"  ➜ [共享资源] 异步触发 115 待整理扫描失败: {e}", exc_info=True)

    threading.Thread(
        target=_runner,
        name='shared-permanent-import-organize',
        daemon=True,
    ).start()

    return {
        'started': True,
        'message': '已异步触发 115 待整理扫描',
    }

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
def _normalize_episode_number_list(value) -> List[int]:
    """共享池按季查询后，本地用缺集号列表做精确过滤。"""
    if value in (None, ''):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = re.split(r'[，,\s]+', value.strip()) if value.strip() else []
    if not isinstance(value, (list, tuple, set)):
        value = [value]

    out = []
    for v in value:
        try:
            n = int(float(v))
            if n > 0 and n not in out:
                out.append(n)
        except Exception:
            pass
    return sorted(out)
def _tv_parent_tmdb_id(context: Dict[str, Any] | None = None, source: Dict[str, Any] | None = None) -> str:
    """统一提取父剧 TMDb ID。

    共享中心对 Episode/Season 的 tmdb_id 可能是“父剧 ID”，也可能是
    “季/集自身 ID”。自动转正按同剧同季计数，必须优先使用 context / source
    里的 parent_series_tmdb_id / parent_tmdb_id，不能把每一集自己的 tmdb_id
    当成父剧，否则每集都会被单独统计成 watched=1。
    """
    ctx = context or {}
    src = source or {}
    item_type = str(src.get('item_type') or ctx.get('item_type') or '').strip()
    season = src.get('season_number') if src.get('season_number') not in (None, '') else ctx.get('season_number')
    episode = src.get('episode_number') if src.get('episode_number') not in (None, '') else ctx.get('episode_number')

    for value in (
        ctx.get('parent_series_tmdb_id'),
        ctx.get('series_tmdb_id'),
        ctx.get('parent_tmdb_id'),
        src.get('parent_series_tmdb_id'),
        src.get('series_tmdb_id'),
    ):
        value = str(value or '').strip()
        if value:
            return value

    # 只有明确是剧/季，或没有集号时，才允许用 tmdb_id 当父剧兜底。
    # 对 Episode 不要优先拿 source.tmdb_id，否则中心如果存的是“集自身 ID”，
    # 自动转正计数会永远卡在 1/阈值。
    if item_type in ('Series', 'Season') or (season not in (None, '') and episode in (None, '')):
        for value in (ctx.get('tmdb_id'), src.get('tmdb_id')):
            value = str(value or '').strip()
            if value:
                return value

    return ''
def _norm_sha1(value: str) -> str:
    return str(value or '').strip().upper()
def _share_import_resp_text(resp: Any) -> str:
    try:
        return json.dumps(resp, ensure_ascii=False)
    except Exception:
        return str(resp or '')
def _share_import_resp_code(resp: Any) -> str:
    if isinstance(resp, dict):
        for key in ('errno', 'code', 'errNo'):
            value = resp.get(key)
            if value not in (None, ''):
                return str(value)
    return ''
def _source_identity_code(src: Dict[str, Any]) -> str:
    if not isinstance(src, dict):
        return ''
    return str(src.get('share_code') or src.get('source_id') or '').strip()
def _is_share_import_already_saved(resp: Any) -> bool:
    """115 返回“你已经转存过该文件”时，只代表本账号幂等限制，不代表中心共享源失效。"""
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
def _share_import_success(resp: Any) -> bool:
    text = _share_import_resp_text(resp).lower()
    if _is_share_import_already_saved(resp):
        return True
    if isinstance(resp, dict):
        if resp.get('state') is True or resp.get('success') is True:
            return True
        code = _share_import_resp_code(resp)
        if code in ('0', '200'):
            return True
    return any(k in text for k in ('已存在', '已经转存', '转存过', 'already', 'exist'))
def _is_share_import_local_account_issue(resp: Any) -> bool:
    """本机账号/频率/空间/幂等问题，不应上报中心 failed。"""
    if _is_share_import_already_saved(resp):
        return True
    text = _share_import_resp_text(resp).lower()
    return any(k in text for k in (
        '空间不足', '超过限制', '转存超限', '任务上限', '频繁',
        '770004', '990001', '4100010', '4100025',
        'quota', 'limit', 'too many', 'rate',
    ))
def _is_share_import_source_dead(resp: Any) -> bool:
    """只有明确死链/提取码错误/源文件删除，才允许向中心上报 failed。"""
    if _is_share_import_local_account_issue(resp):
        return False
    code = _share_import_resp_code(resp)
    if code in ('4100005',):
        return True
    text = _share_import_resp_text(resp).lower()
    return any(k in text for k in (
        '分享已取消', '分享已失效', '分享不存在', '取消分享', '已取消', '已失效',
        '提取码错误', '访问码错误', '密码错误',
        '文件(夹)已被移动或删除', '已被移动或删除', '源文件不存在',
        'share not found', 'expired', 'cancelled', 'canceled', 'not found', 'deleted',
    ))
def _find_local_p115_file_by_sha1(sha1: str) -> Dict[str, Any]:
    """按 SHA1 兜底判断本账号是否已经有这个文件。

    只查 p115_filesystem_cache：这是本地 115 文件树缓存，命中即说明该 SHA1
    已经在本账号某处存在；因此无需再次 share_import，也绝不能因为 115 返回
    4100024 去污染中心共享源状态。
    """
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, parent_id, name, local_path, sha1, pick_code, size, updated_at
                    FROM p115_filesystem_cache
                    WHERE sha1 IS NOT NULL
                      AND sha1 <> ''
                      AND UPPER(sha1) = %s
                    ORDER BY
                        CASE WHEN COALESCE(pick_code, '') <> '' THEN 0 ELSE 1 END,
                        updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (sha1,),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 按 SHA1 查询 p115_filesystem_cache 失败: sha1={sha1}, err={e}")
    return {}
def _source_relevant_to_context(src: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """判断中心源是否和本次消费目标相关，用于按 SHA1 跳过重复转存。"""
    if not src or not context:
        return True
    item_type = str(context.get('item_type') or '').strip()
    if item_type == 'Episode':
        ctx_s = _safe_int(context.get('season_number'), -999)
        ctx_e = _safe_int(context.get('episode_number'), -999)
        src_s_raw = src.get('season_number')
        src_e_raw = src.get('episode_number')
        # 中心季包/旧数据可能没有集号；这种记录仍视为与当前目标相关。
        if src_e_raw not in (None, ''):
            if _safe_int(src_e_raw, -998) != ctx_e:
                return False
        if src_s_raw not in (None, '') and ctx_s != -999:
            if _safe_int(src_s_raw, -998) != ctx_s:
                return False
        return True
    if item_type == 'Season':
        ctx_s = _safe_int(context.get('season_number'), -999)
        src_s_raw = src.get('season_number')
        if not (src_s_raw in (None, '') or ctx_s == -999 or _safe_int(src_s_raw, -998) == ctx_s):
            return False
        missing_eps = _normalize_episode_number_list(context.get('missing_episode_numbers'))
        src_e_raw = src.get('episode_number')
        # SUBSCRIBED 补库会带缺集列表：中心按季返回，客户端只消费缺失单集；
        # 季包/旧数据没有 episode_number 时继续保留，因为它可能覆盖整季。
        if missing_eps and src_e_raw not in (None, ''):
            return _safe_int(src_e_raw, -998) in missing_eps
        return True
    if item_type == 'Movie':
        src_type = str(src.get('item_type') or '').strip()
        return src_type in ('', 'Movie')
    return True
def _local_existing_hit_for_import_group(src: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """同一 share_code 可能聚合多条中心源；优先用本次目标相关源的 SHA1 查本地。

    SUBSCRIBED 补库场景可能是“本季已有一部分、缺一部分”。如果中心返回的是季包，
    不能因为包内任意一集已在本地就跳过整个季包；只有相关文件全部已存在时才跳过。
    """
    rows = src.get('_group_sources') if isinstance(src, dict) else None
    rows = [r for r in (rows or [src]) if isinstance(r, dict)]
    relevant_rows = [r for r in rows if _source_relevant_to_context(r, context)] or rows

    item_type = str((context or {}).get('item_type') or '').strip()
    missing_eps = _normalize_episode_number_list((context or {}).get('missing_episode_numbers'))
    partial_season_recheck = item_type == 'Season' and bool(missing_eps)

    if partial_season_recheck:
        checked = 0
        first_hit = None
        for row in relevant_rows:
            sha1 = _norm_sha1(row.get('sha1'))
            if not sha1:
                continue
            checked += 1
            local = _find_local_p115_file_by_sha1(sha1)
            if local and first_hit is None:
                first_hit = {'source': row, 'local': local}
            elif not local:
                # 至少还有一个相关文件本地不存在，不能跳过本次导入。
                return {}
        if checked > 0 and first_hit:
            return first_hit
        return {}

    # 先查与本次目标相关的 SHA1；若命中，说明同一个文件已经在本账号存在。
    for row in relevant_rows:
        sha1 = _norm_sha1(row.get('sha1'))
        if not sha1:
            continue
        local = _find_local_p115_file_by_sha1(sha1)
        if local:
            return {'source': row, 'local': local}

    # 最后兜底查代表行，防止中心旧数据缺少 season/episode 导致相关性判断失准。
    sha1 = _norm_sha1(src.get('sha1') if isinstance(src, dict) else '')
    if sha1:
        local = _find_local_p115_file_by_sha1(sha1)
        if local:
            return {'source': src, 'local': local}
    return {}
def _episode_guard_key(parent_tmdb_id, season_number, episode_number) -> str:
    parent = str(parent_tmdb_id or '').strip()
    season = _safe_int(season_number, -1)
    episode = _safe_int(episode_number, -1)
    if not parent or season < 0 or episode < 0:
        return ''
    return f'{parent}|{season}|{episode}'
def _collect_episode_guard_keys(sources: List[Dict[str, Any]], context: Dict[str, Any]) -> List[str]:
    keys = set()
    context_parent = _tv_parent_tmdb_id(context, None)
    context_key = _episode_guard_key(
        context_parent,
        context.get('season_number'),
        context.get('episode_number'),
    )
    if context_key:
        keys.add(context_key)

    for src in sources or []:
        if not isinstance(src, dict) or not _source_relevant_to_context(src, context):
            continue
        parent = _tv_parent_tmdb_id(context, src) or context_parent
        key = _episode_guard_key(
            parent,
            src.get('season_number') if src.get('season_number') not in (None, '') else context.get('season_number'),
            src.get('episode_number') if src.get('episode_number') not in (None, '') else context.get('episode_number'),
        )
        if key:
            keys.add(key)
    return sorted(keys)
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
    """把本地待订阅项转换成中心查询。

    关键约定：剧集缺口只按季登记/查询，不再按 Episode 建缺口。
    客户端拿到同季共享源后，再用本地缺集列表精确匹配具体 SxxEyy。
    """
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
        sid = parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or tmdb_id
        s_num = season_number if season_number not in (None, '') else item.get('season_number')
        # Episode 只用于本地精确消费，中心缺口/搜索统一提升到 Season 粒度。
        # 这样一季 1000 集也只会产生一个 open gap。
        if sid and s_num not in (None, ''):
            queries.append(_build_gap_item(tmdb_id=sid, item_type='Season', title=title, season_number=s_num, year=year))
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
def _episode_transfer_disabled() -> bool:
    return bool(settings_db.get_shared_resource_config().get('p115_shared_disable_episode_transfer', False))
def _filter_sources_by_episode_transfer_policy(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _episode_transfer_disabled():
        return list(sources or [])
    filtered = []
    blocked = 0
    for src in sources or []:
        item_type = str((src or {}).get('item_type') or '').strip().lower()
        if item_type == 'episode':
            blocked += 1
            continue
        filtered.append(src)
    if blocked:
        logger.info(f"  ➜ [共享资源] 已按配置过滤中心单集资源 {blocked} 条。")
    return filtered
def _guess_se_from_source(src: Dict[str, Any], context: Dict[str, Any]):
    s_num = src.get('season_number') if src.get('season_number') not in (None, '') else context.get('season_number')
    # ★ 核心修复：补充从 context 兜底获取 episode_number
    e_num = src.get('episode_number') if src.get('episode_number') not in (None, '') else context.get('episode_number')

    try:
        s_num = int(s_num) if s_num not in (None, '') else None
    except Exception:
        s_num = None
    try:
        e_num = int(e_num) if e_num not in (None, '') else None
    except Exception:
        e_num = None

    if s_num is None or e_num is None:
        name = str(src.get('file_name') or '')
        m = re.search(r'[Ss](\d{1,3})[. _-]*[Ee](\d{1,4})', name)
        if m:
            if s_num is None:
                s_num = int(m.group(1))
            if e_num is None:
                e_num = int(m.group(2))

    return s_num, e_num
def _load_center_raw_map(client: SharedCenterClient, sources: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    raw_map = {}

    # 手动中心资源库 include_raw=True 时，source 里可能已经带 raw。
    for src in sources or []:
        sha1 = _norm_sha1(src.get('sha1'))
        raw = src.get('raw_ffprobe_json')
        if sha1 and isinstance(raw, dict):
            raw_map[sha1] = raw

    missing_sha1s = []
    for src in sources or []:
        sha1 = _norm_sha1(src.get('sha1'))
        if sha1 and sha1 not in raw_map and sha1 not in missing_sha1s:
            missing_sha1s.append(sha1)

    if missing_sha1s and hasattr(client, 'fetch_raw_ffprobe_batch'):
        data = client.fetch_raw_ffprobe_batch(missing_sha1s)
        for item in (data or {}).get('items') or []:
            sha1 = _norm_sha1(item.get('sha1'))
            raw = item.get('raw_ffprobe_json')
            if sha1 and item.get('status') == 'ok' and isinstance(raw, dict):
                raw_map[sha1] = raw

    return raw_map
def _share_source_rows(src: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = (src or {}).get('_group_sources') if isinstance(src, dict) else None
    rows = [x for x in (rows or []) if isinstance(x, dict)]
    if rows:
        return rows
    return [src] if isinstance(src, dict) else []
def _source_status_rank_for_retry(value: str) -> int:
    value = str(value or '').strip().lower()
    if value == 'alive':
        return 0
    if value == 'pending':
        return 1
    return 2
def _source_backup_rank(src: Dict[str, Any]) -> int:
    try:
        return int(float((src or {}).get('_backup_rank') or 999999))
    except Exception:
        return 999999
def _source_success_count(src: Dict[str, Any]) -> int:
    try:
        return int(float((src or {}).get('success_count') or (src or {}).get('_package_success_count') or 0))
    except Exception:
        return 0
def _source_fail_count(src: Dict[str, Any]) -> int:
    try:
        return int(float((src or {}).get('fail_count') or (src or {}).get('_package_fail_count') or 0))
    except Exception:
        return 0
def _source_retry_sort_key(src: Dict[str, Any]):
    rows = _share_source_rows(src)
    best_rank = min([_source_backup_rank(r) for r in rows] + [_source_backup_rank(src)])
    status_rank = min([_source_status_rank_for_retry(r.get('status')) for r in rows] + [_source_status_rank_for_retry((src or {}).get('status'))])
    success = sum(_source_success_count(r) for r in rows) or _source_success_count(src)
    fail = sum(_source_fail_count(r) for r in rows) or _source_fail_count(src)
    first_time = min([str((r or {}).get('last_verified_at') or (r or {}).get('created_at') or '') for r in rows] or [''])
    created = min([str((r or {}).get('created_at') or '') for r in rows] or [''])
    return (best_rank, status_rank, success, fail, first_time, created, str((src or {}).get('source_id') or ''))
def _season_pack_retry_fingerprint(rows: List[Dict[str, Any]], context: Dict[str, Any] = None) -> str:
    rows = [dict(r or {}) for r in (rows or []) if r]
    if not rows:
        return ''
    first = rows[0]
    tmdb_id = str(
        first.get('tmdb_id')
        or (context or {}).get('parent_series_tmdb_id')
        or (context or {}).get('parent_tmdb_id')
        or (context or {}).get('tmdb_id')
        or ''
    ).strip()
    season = _safe_int(first.get('season_number') if first.get('season_number') not in (None, '') else (context or {}).get('season_number'), None)
    sha1s = sorted({_norm_sha1(r.get('sha1')) for r in rows if _norm_sha1(r.get('sha1'))})
    if not tmdb_id or season is None or not sha1s:
        return ''
    return f"season_pack:{tmdb_id}:S{int(season):02d}:{'|'.join(sha1s)}"
def _permanent_resource_key_for_rows(rows: List[Dict[str, Any]], context: Dict[str, Any] = None) -> str:
    """永久转存冗余组 key：同 SHA1 / 同季包完整指纹归为一组。"""
    rows = [dict(r or {}) for r in (rows or []) if r]
    explicit_keys = [str(r.get('_resource_key') or '').strip() for r in rows if str(r.get('_resource_key') or '').strip()]
    if explicit_keys and len(set(explicit_keys)) == 1:
        return explicit_keys[0]
    if not rows:
        return ''
    first = rows[0]
    item_type = str(first.get('item_type') or (context or {}).get('item_type') or '').strip().lower()
    share_code = str(first.get('share_code') or '').strip()
    if item_type == 'season' and share_code:
        return _season_pack_retry_fingerprint(rows, context)
    sha1 = _norm_sha1(first.get('sha1'))
    if not sha1:
        return ''
    return '|'.join([
        item_type,
        str(first.get('tmdb_id') or (context or {}).get('tmdb_id') or ''),
        str(first.get('season_number') if first.get('season_number') is not None else (context or {}).get('season_number') or ''),
        str(first.get('episode_number') if first.get('episode_number') is not None else (context or {}).get('episode_number') or ''),
        sha1,
    ])
def _permanent_resource_key(src: Dict[str, Any], context: Dict[str, Any] = None) -> str:
    return _permanent_resource_key_for_rows(_share_source_rows(src), context=context)
def _build_permanent_import_plan(sources: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """把中心源整理为“资源版本 -> 多个备份分享码”的重试计划。"""
    package_map: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for src in sources or []:
        if not isinstance(src, dict):
            continue
        code = str(src.get('share_code') or src.get('source_id') or '').strip()
        if not code:
            code = f"source:{src.get('source_id') or len(order)}"
        if code not in package_map:
            package_map[code] = {'primary': dict(src), 'rows': []}
            order.append(code)
        package_map[code]['rows'].append(dict(src))

    alternatives: List[Dict[str, Any]] = []
    for code in order:
        data = package_map.get(code) or {}
        primary = dict(data.get('primary') or {})
        rows = [dict(r) for r in (data.get('rows') or [])]
        primary['_group_sources'] = rows or [primary]
        primary['_permanent_resource_key'] = _permanent_resource_key_for_rows(primary['_group_sources'], context)
        alternatives.append(primary)

    groups: Dict[str, Dict[str, Any]] = {}
    group_order: List[str] = []
    for alt in alternatives:
        resource_key = str(alt.get('_permanent_resource_key') or _permanent_resource_key(alt, context) or '').strip()
        if not resource_key:
            resource_key = f"share:{alt.get('share_code') or alt.get('source_id') or len(group_order)}"
        if resource_key not in groups:
            groups[resource_key] = {'resource_key': resource_key, 'alternatives': []}
            group_order.append(resource_key)
        groups[resource_key]['alternatives'].append(alt)

    plan = []
    for resource_key in group_order:
        group = groups[resource_key]
        alts = sorted(group['alternatives'], key=_source_retry_sort_key)
        plan.append({'resource_key': resource_key, 'alternatives': alts})
    return plan
def _cache_center_raw_as_local_mediainfo(src: Dict[str, Any], raw: Dict[str, Any]) -> bool:
    """中心 RAW -> 本地 p115_mediainfo_cache.mediainfo_json，供 WashingService 读取。"""
    sha1 = _norm_sha1(src.get('sha1'))
    if not sha1 or not isinstance(raw, dict):
        return False

    file_node = {
        'fn': src.get('file_name') or sha1,
        'file_name': src.get('file_name') or sha1,
        'sha1': sha1,
        'fs': _safe_int(src.get('size'), 0),
        'size': _safe_int(src.get('size'), 0),
    }

    try:
        builder = _MediainfoBuilder()
        emby_obj = builder._build_emby_mediainfo_from_ffprobe(raw, file_node, sha1=sha1)
        if not emby_obj:
            return False
        P115CacheManager.save_mediainfo_cache(sha1, emby_obj, raw)
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 中心 RAW 转本地 MediaInfo 失败: {src.get('file_name')} -> {e}")
        return False
def _washing_new_level(sha1: str, file_name: str, file_size: int, target_cid: str,
                       media_type: str, original_lang: str = '', has_external_subtitle: bool = False):
    """读取 WashingService 的真实规则优先级。level 越小优先级越高。"""
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
def _raw_quality_score(src: Dict[str, Any], raw: Dict[str, Any]) -> int:
    """同一洗版优先级下的兜底排序。主裁判仍是 WashingService。"""
    text = f"{src.get('file_name') or ''} {json.dumps(raw or {}, ensure_ascii=False)[:4000]}".upper()
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

    size_gb = (_safe_int(src.get('size'), 0) or 0) / 1024 / 1024 / 1024
    score += min(int(size_gb), 30)
    return score
def _select_sources_by_washing_before_import(
    client: SharedCenterClient,
    p115,
    sources: List[Dict[str, Any]],
    context: Dict[str, Any],
    raw_map: Dict[str, Dict[str, Any]] = None
) -> tuple[List[Dict[str, Any]], List[str]]:
    """永久转存前按洗版规则筛选中心源。

    同一个 share_code 视为一个包：
    - 包内只要有任意一个视频是 ACCEPT/REPLACE，就允许转存整包；
    - 只有当包内所有视频都被 REJECT/SKIP 时，才拒绝整包；
    - 多个包均合格时，选择洗版优先级最高的包。
    """
    from handler.resubscribe_service import WashingService

    if raw_map is None:
        raw_map = _load_center_raw_map(client, sources)
    errors = []

    groups = {}
    order = []
    for src in sources or []:
        code = src.get('share_code') or src.get('source_id')
        if not code:
            errors.append(f"{src.get('file_name')}: 缺少分享码")
            continue
        if code not in groups:
            groups[code] = []
            order.append(code)
        groups[code].append(src)

    candidates = []

    for idx, code in enumerate(order):
        rows = groups.get(code) or []
        rejected = False
        group_best_level = 999
        group_action_rank = 0
        group_quality = 0
        group_reasons = []

        for src in rows:
            file_name = src.get('file_name') or ''
            ext = os.path.splitext(file_name)[1].lower()
            if ext not in VIDEO_EXTS:
                continue

            sha1 = _norm_sha1(src.get('sha1'))
            raw = raw_map.get(sha1)
            if not raw:
                rejected = True
                errors.append(f"{file_name}: 中心缺少 RAW，洗版预检拒绝转存")
                break

            if not _cache_center_raw_as_local_mediainfo(src, raw):
                rejected = True
                errors.append(f"{file_name}: RAW 无法转换为本地 MediaInfo，洗版预检拒绝转存")
                break

            source_item_type = str(src.get('item_type') or context.get('item_type') or '')
            media_type = 'movie' if source_item_type == 'Movie' else 'tv'

            if media_type == 'movie':
                tmdb_for_washing = str(src.get('tmdb_id') or context.get('tmdb_id') or '')
            else:
                tmdb_for_washing = str(
                    context.get('parent_tmdb_id')
                    or src.get('parent_series_tmdb_id')
                    or src.get('tmdb_id')
                    or context.get('tmdb_id')
                    or ''
                )

            s_num, e_num = _guess_se_from_source(src, context)

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

                target_cid_for_washing = organizer.get_target_cid(
                    season_num=s_num if media_type == 'tv' else None
                )
                original_lang = (organizer.raw_metadata or {}).get('lang_code')
            except Exception as e:
                rejected = True
                errors.append(f"{file_name}: 无法计算洗版目标目录，拒绝转存 -> {e}")
                break

            file_size = _safe_int(src.get('size'), 0)

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

            # ★ 回退为一票否决：只要包内有任意一个视频被拒绝/跳过，整个包就拒绝，避免转存残缺季包
            if action in ('REJECT', 'SKIP'):
                rejected = True
                # 直接把具体的文件名和拒绝原因加入到 errors 中，这样日志和前端都能直接看到
                errors.append(f"[{code}] {file_name}: 洗版预检 [{action}] {reason}")
                break

            level, level_reason = _washing_new_level(
                sha1,
                file_name,
                file_size,
                str(target_cid_for_washing),
                media_type,
                original_lang=original_lang,
                has_external_subtitle=False,
            )

            if level > 0:
                group_best_level = min(group_best_level, level)

            group_action_rank = max(group_action_rank, 2 if action == 'REPLACE' else 1)
            group_quality += _raw_quality_score(src, raw)
            group_reasons.append(f"{file_name}: {action}; level={level}; {reason or level_reason}")

        if rejected:
            continue

        if rows:
            # level 越小越好；无规则 level=999，走质量兜底。
            score = (1000 - min(group_best_level, 999)) * 100000 + group_action_rank * 10000 + group_quality
            candidates.append({
                'score': score,
                'index': idx,
                'share_code': code,
                'rows': rows,
                'resource_key': _permanent_resource_key_for_rows(rows, context),
                'reasons': group_reasons,
            })

    if not candidates:
        return [], errors or ['所有中心共享源均未通过洗版预检']

    candidates.sort(key=lambda x: (x['score'], -x['index']), reverse=True)

    # ★ 修复：Season SUBSCRIBED 补库时，不能在整季 51 个缺集中只全局选 1 个最佳源。
    # 应该按“每一集”各自选出最佳版本；同一集的同版本备份分享仍保留给后续重试。
    missing_eps = _normalize_episode_number_list((context or {}).get('missing_episode_numbers'))
    is_partial_season_recheck = (
        str((context or {}).get('item_type') or '').strip() == 'Season'
        and bool(missing_eps)
    )

    def _candidate_single_episode_number(candidate):
        eps = set()
        for row in candidate.get('rows') or []:
            _, e_num = _guess_se_from_source(row, context)
            e_num = _safe_int(e_num, None)
            if e_num is not None and (not missing_eps or e_num in missing_eps):
                eps.add(e_num)
        return next(iter(eps)) if len(eps) == 1 else None

    if is_partial_season_recheck:
        best_by_episode = {}

        # candidates 已经按分数从高到低排好了；第一次遇到的就是该集最佳版本。
        for candidate in candidates:
            ep_num = _candidate_single_episode_number(candidate)
            if ep_num is None:
                continue
            if ep_num not in best_by_episode:
                best_by_episode[ep_num] = candidate

        if best_by_episode:
            wanted_pairs = {
                (
                    ep_num,
                    str(best.get('resource_key') or '').strip(),
                )
                for ep_num, best in best_by_episode.items()
            }

            selected_candidates = []
            seen_candidate = set()

            # 同一集选定最佳 resource_key 后，把该 resource_key 的备份分享也带上。
            for candidate in candidates:
                ep_num = _candidate_single_episode_number(candidate)
                resource_key = str(candidate.get('resource_key') or '').strip()
                if (ep_num, resource_key) not in wanted_pairs:
                    continue

                dedupe_key = (candidate.get('share_code'), resource_key)
                if dedupe_key in seen_candidate:
                    continue
                seen_candidate.add(dedupe_key)
                selected_candidates.append(candidate)

            selected_candidates.sort(key=lambda x: (
                _safe_int(_candidate_single_episode_number(x), 999999),
                -x['score'],
                x['index'],
            ))

            selected_rows = []
            for candidate in selected_candidates:
                selected_rows.extend(candidate.get('rows') or [])

            logger.info(
                f"  ➜ [共享资源] SUBSCRIBED 补库洗版预检按缺集选源: "
                f"缺集={missing_eps}, 选中={len(best_by_episode)} 集/{len(selected_candidates)} 个分享, "
                f"示例={[c.get('share_code') for c in selected_candidates[:5]]}"
            )

            return selected_rows, errors

    # 普通电影 / 单集 / 非补库场景：保持原来的“全局选最佳版本”逻辑。
    best = candidates[0]
    best_resource_key = best.get('resource_key') or ''
    selected_candidates = [c for c in candidates if best_resource_key and c.get('resource_key') == best_resource_key]
    if not selected_candidates:
        selected_candidates = [best]

    # 洗版只决定“该入哪个版本”；同版本的多个备份分享全部保留给永久转存重试。
    selected_candidates.sort(key=lambda x: (-x['score'], x['index']))
    selected_rows = []
    for candidate in selected_candidates:
        selected_rows.extend(candidate.get('rows') or [])

    logger.info(
        f"  ➜ [共享资源] 洗版预检选定中心源版本: share={best['share_code']}, "
        f"score={best['score']}, backups={len(selected_candidates)}, reasons={best['reasons'][:3]}"
    )

    return selected_rows, errors
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

    raw_map = _load_center_raw_map(client, sources)

    # ★ 核心修复：解决重复写入缓存的问题
    # 永久转存前预检：
    # - replace：提前调用洗版模块裁决，洗版模块内部会负责写入缓存；
    # - skip / keep_both：不做洗版预检，直接在这里遍历写入缓存。
    rename_config = settings_db.get_setting('p115_rename_config') or {}
    if rename_config.get('conflict_mode') == 'replace':
        sources, washing_errors = _select_sources_by_washing_before_import(
            client,
            p115,
            sources,
            context,
            raw_map=raw_map
        )
        if not sources:
            logger.info(f"  ➜ [共享资源] 已被洗版预检拒绝: {washing_errors[:5]}")
            return {
                'success': False,
                'mode': 'permanent',
                'count': 0,
                'action_type': '共享永久转存',
                'errors': washing_errors,
                'washing_rejected': True,
            }
    else:
        logger.info(f"  ➜ [共享资源] 当前覆盖模式为 {rename_config.get('conflict_mode')}，跳过洗版预检。")
        # 非洗版模式下，在这里统一写入缓存
        for src in sources:
            sha1 = _norm_sha1(src.get('sha1'))
            raw = raw_map.get(sha1)
            if raw:
                _cache_center_raw_as_local_mediainfo(src, raw)

    import_plan = _build_permanent_import_plan(sources, context)
    ok = 0
    skipped_existing = 0
    failed_resources = 0
    errors = []

    for plan_item in import_plan:
        resource_key = plan_item.get('resource_key') or ''
        alternatives = plan_item.get('alternatives') or []
        if not alternatives:
            continue

        group_done = False
        group_had_local_account_issue = False
        if len(alternatives) > 1:
            logger.info(
                "  ➜ [共享资源] 永久转存启用备份重试：resource=%s, alternatives=%s",
                resource_key[:96] or '-', len(alternatives)
            )

        for alt_index, src in enumerate(alternatives, start=1):
            share_code = src.get('share_code') or ''
            receive_code = src.get('receive_code') or ''
            if not share_code:
                errors.append(f"{src.get('file_name')}: 缺少分享码")
                continue

            if alt_index > 1:
                logger.warning(
                    "  ➜ [共享资源] 主分享转存失败，切换备用分享继续尝试：resource=%s, backup=%s/%s, share=%s",
                    resource_key[:96] or '-', alt_index, len(alternatives), share_code
                )

            # 关键兜底：真正调用 115 share_import 前，先按中心源 SHA1 查本地 115 文件树缓存。
            # 命中说明这个文件已经在本账号存在，直接跳过转存，避免 115 返回 4100024 后再误伤中心源。
            local_hit = _local_existing_hit_for_import_group(src, context)
            if local_hit:
                hit_src = local_hit.get('source') or src
                local = local_hit.get('local') or {}
                skipped_existing += 1
                logger.info(
                    "  ➜ [共享资源] 本地 p115_filesystem_cache 已存在相同 SHA1，跳过重复转存："
                    f"share={share_code}, sha1={_norm_sha1(hit_src.get('sha1'))}, "
                    f"local={local.get('name') or local.get('id')}, pick_code={local.get('pick_code') or '-'}"
                )
                group_done = True
                break

            import_target_cid = str(target_cid)
            import_container = {}

            resp = p115.share_import(share_code, receive_code, import_target_cid)
            text = _share_import_resp_text(resp)
            is_already_saved = _is_share_import_already_saved(resp)
            success = _share_import_success(resp)

            if success:
                ok += 1
                group_done = True
                if is_already_saved:
                    # 4100024 是本账号已经接收过该分享，不是本次真实转存成功；不要向中心重复报 success，
                    # 但也绝不能报 failed。触发一次整理扫描，让已存在文件尽快被识别入库。
                    logger.info(
                        f"  ➜ [共享资源] 115 提示本账号已转存过，视为本地幂等命中，跳过中心 failed 上报：share={share_code}"
                    )
                else:
                    logger.info(
                        f"  ➜ [共享资源] 已成功转存：share={share_code}, cid={import_target_cid}, "
                        f"backup={alt_index}/{len(alternatives)}"
                    )
                    try:
                        client.report_transfer(
                            src.get('source_id'),
                            'success',
                            expected_sha1=_norm_sha1(src.get('sha1')),
                            expected_size=_safe_int(src.get('size'), 0) or None,
                            message='permanent import submitted',
                        )
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源] 上报转存成功失败：share={share_code}, err={e}")
                break
            else:
                logger.warning(
                    f"  ➜ [共享资源] 115分享转存失败：share={share_code}, cid={import_target_cid}, "
                    f"backup={alt_index}/{len(alternatives)}, resp={str(resp)[:300]}"
                )

            errors.append(f"{src.get('file_name')}: {text[:120]}")

            if _is_share_import_local_account_issue(resp):
                group_had_local_account_issue = True
                logger.warning(
                    "  ➜ [共享资源] 转存失败属于本账号限制/幂等问题，跳过向中心上报 failed，也不继续切换备份，"
                    f"避免误伤资源提供者：share={share_code}, resp={text[:180]}"
                )
                break
            elif _is_share_import_source_dead(resp):
                try:
                    client.report_transfer(
                        src.get('source_id'),
                        'failed',
                        expected_sha1=_norm_sha1(src.get('sha1')),
                        expected_size=_safe_int(src.get('size'), 0) or None,
                        message=f'external_share_import_failed: {text[:160]}',
                    )
                except Exception:
                    pass
            else:
                logger.warning(
                    "  ➜ [共享资源] 转存失败原因不确定，先只记本地错误，不上报中心 failed，继续尝试同资源备份："
                    f"share={share_code}, resp={text[:180]}"
                )

        if not group_done:
            failed_resources += 1
            if len(alternatives) > 1 and not group_had_local_account_issue:
                logger.warning(
                    "  ➜ [共享资源] 同资源所有备份分享均转存失败：resource=%s, alternatives=%s",
                    resource_key[:96] or '-', len(alternatives)
                )

    if ok > 0:
        kick_result = _kick_115_organize_detached(
            reason=f"共享资源转存成功 {ok} 个",
            delay=3.0,
        )
        logger.info(f"  ➜ [共享资源] 115 待整理扫描触发结果: {kick_result}")
    elif skipped_existing > 0:
        logger.info(f"  ➜ [共享资源] 本地已存在 {skipped_existing} 个共享源，未重复调用 115 转存。")

    return {
        'success': (ok > 0 or skipped_existing > 0),
        'mode': 'permanent',
        'count': ok,
        'skipped_existing': skipped_existing,
        'failed_resources': failed_resources,
        'action_type': '共享永久转存',
        'errors': errors,
    }

def _subscription_probe_request_key(query: Dict[str, Any]) -> str:
    return '|'.join([
        str((query or {}).get('item_type') or '').strip(),
        str((query or {}).get('tmdb_id') or '').strip(),
        str((query or {}).get('season_number') if (query or {}).get('season_number') is not None else ''),
        str((query or {}).get('episode_number') if (query or {}).get('episode_number') is not None else ''),
    ])


def _filter_sources_for_request(
    sources: List[Dict[str, Any]],
    item: Dict[str, Any],
    item_type: str,
    season_number=None,
    exclude_share_codes: List[str] | None = None,
) -> tuple[List[Dict[str, Any]], int, List[int]]:
    """统一订阅消费前的本地精确过滤。中心按季返回，客户端按缺集/单集再裁剪。"""
    sources = list(sources or [])
    req_s_num = season_number if season_number not in (None, '') else (item or {}).get('season_number')
    req_e_num = (item or {}).get('episode_number')
    req_missing_eps = _normalize_episode_number_list((item or {}).get('missing_episode_numbers'))

    if req_e_num is not None and str(req_e_num).strip() != '':
        filtered_sources = []
        for src in sources:
            src_s_num = src.get('season_number')
            src_e_num = src.get('episode_number')
            if src_s_num is not None and str(src_s_num).strip() != '' and req_s_num not in (None, ''):
                if int(src_s_num) != int(req_s_num):
                    continue
            if src_e_num is not None and str(src_e_num).strip() != '':
                if int(src_e_num) != int(req_e_num):
                    continue
            filtered_sources.append(src)
        sources = filtered_sources
    elif req_missing_eps and str(item_type or '').strip() == 'Season':
        filtered_sources = []
        for src in sources:
            src_s_num = src.get('season_number')
            src_e_num = src.get('episode_number')
            if src_s_num is not None and str(src_s_num).strip() != '' and req_s_num not in (None, ''):
                if int(src_s_num) != int(req_s_num):
                    continue
            # 单集源必须在缺失列表内；季包没有集号，保留，因为它可能覆盖整季。
            if src_e_num is not None and str(src_e_num).strip() != '':
                if int(src_e_num) not in req_missing_eps:
                    continue
            filtered_sources.append(src)
        if len(filtered_sources) != len(sources):
            logger.info(
                f"  ➜ [共享资源] SUBSCRIBED 补库按缺集过滤中心源：{len(sources)} -> {len(filtered_sources)}，"
                f"缺失集={req_missing_eps}"
            )
        sources = filtered_sources

    excluded_codes = {
        str(code or '').strip()
        for code in (exclude_share_codes or [])
        if str(code or '').strip()
    }
    excluded_hits = 0
    if excluded_codes:
        filtered_sources = []
        for src in sources:
            code = _source_identity_code(src)
            if code and code in excluded_codes:
                excluded_hits += 1
                continue
            filtered_sources.append(src)
        if excluded_hits:
            logger.info(f"  ➜ [共享资源] 已过滤 {excluded_hits} 个本轮已消费的 share_code，避免重复转存同一季包。")
        sources = filtered_sources

    return sources, excluded_hits, req_missing_eps


def _consume_sources_for_subscription(
    client: SharedCenterClient,
    sources: List[Dict[str, Any]],
    item: Dict[str, Any],
    title: str,
    tmdb_id,
    item_type: str,
    parent_tmdb_id=None,
    season_number=None,
    year='',
    exclude_share_codes: List[str] | None = None,
    force_mode: str | None = None,
    reported_gap: bool = False,
) -> Dict[str, Any]:
    sources = _filter_sources_by_episode_transfer_policy(sources or [])
    sources, excluded_hits, req_missing_eps = _filter_sources_for_request(
        sources,
        item,
        item_type,
        season_number=season_number,
        exclude_share_codes=exclude_share_codes,
    )

    if not sources:
        if excluded_hits:
            return {
                'enabled': True,
                'success': False,
                'reported_gap': False,
                'skipped_existing': True,
                'matched_share_codes': [],
                'covered_episode_keys': [],
            }
        return {'enabled': True, 'success': False, 'reported_gap': bool(reported_gap)}

    context = {
        'title': title,
        'tmdb_id': str(tmdb_id or ''),
        'item_type': item_type,
        'parent_tmdb_id': str(parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or ''),
        'parent_series_tmdb_id': str(parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or ''),
        'season_number': season_number,
        'episode_number': item.get('episode_number'),
        'missing_episode_numbers': req_missing_eps,
        'year': year,
    }

    override_mode = str(force_mode or '').strip().lower()
    if override_mode == 'virtual':
        logger.info('  ➜ [共享资源] 虚拟入库已移除，本次共享池消费改为永久转存。')
    mode = 'permanent'
    matched_share_codes = sorted({_source_identity_code(src) for src in sources if _source_identity_code(src)})
    covered_episode_keys = _collect_episode_guard_keys(sources, context)
    result = _consume_permanent(client, sources, context)
    result['mode'] = mode
    result['matched_share_codes'] = matched_share_codes
    result['covered_episode_keys'] = covered_episode_keys
    return result


def batch_probe_shared_resources(prepared_items: List[Dict[str, Any]], limit_per_item: int = 200) -> Dict[str, Any]:
    """统一订阅批量探测中心共享池。

    prepared_items 由 subscriptions.py 提前整理，包含原始 item 与父剧/季号/年份等上下文。
    返回 by_key，后续逐条处理时直接取中心本轮批量结果，避免 N 次中心查询/登记缺口。
    """
    if not shared_center_enabled():
        return {'enabled': False, 'supported': False, 'by_key': {}}

    client = SharedCenterClient()
    if not client.ready:
        logger.warning('  ➜ [共享资源] 已启用但中心地址/token 未配置，跳过共享池批量探测。')
        return {'enabled': True, 'supported': False, 'by_key': {}}

    request_items = []
    context_by_key: Dict[str, Dict[str, Any]] = {}
    for prepared in prepared_items or []:
        if not isinstance(prepared, dict):
            continue
        item = prepared.get('item') if isinstance(prepared.get('item'), dict) else {}
        queries = _build_center_queries(
            item,
            prepared.get('title') or item.get('title'),
            prepared.get('tmdb_id') or item.get('tmdb_id'),
            prepared.get('item_type') or item.get('item_type'),
            prepared.get('parent_tmdb_id'),
            prepared.get('season_number'),
            prepared.get('year'),
        )
        for query in queries:
            key = _subscription_probe_request_key(query)
            if not key or key in context_by_key:
                continue
            query = dict(query)
            query['request_key'] = key
            missing_eps = _normalize_episode_number_list(item.get('missing_episode_numbers'))
            if missing_eps:
                query['missing_episode_numbers'] = missing_eps
            request_items.append(query)
            context_by_key[key] = prepared

    if not request_items:
        return {'enabled': True, 'supported': True, 'by_key': {}}

    try:
        if hasattr(client, 'probe_subscriptions_batch'):
            data = client.probe_subscriptions_batch(request_items, limit_per_item=limit_per_item)
        else:
            data = {'supported': False, 'items': [], 'message': 'client_missing_probe_subscriptions_batch'}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 统一订阅批量探测失败，将回退逐条查询: {e}")
        return {'enabled': True, 'supported': False, 'by_key': {}, 'message': str(e)}

    if data.get('supported') is False:
        logger.info('  ➜ [共享资源] 中心暂不支持统一订阅批量探测，将回退逐条查询。')
        return {'enabled': True, 'supported': False, 'by_key': {}, 'message': data.get('message')}

    by_key: Dict[str, Dict[str, Any]] = {}
    hit_count = gap_count = 0
    for row in data.get('items') or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get('request_key') or _subscription_probe_request_key(row.get('query') or {})).strip()
        if not key:
            continue
        row['prepared'] = context_by_key.get(key) or {}
        by_key[key] = row
        if row.get('sources'):
            hit_count += 1
        if row.get('reported_gap') or row.get('status') == 'gap_registered':
            gap_count += 1

    logger.info(
        f"  ➜ [共享资源] 统一订阅批量探测完成：提交 {len(request_items)} 个，"
        f"命中 {hit_count} 个，登记缺口 {gap_count} 个。"
    )
    return {'enabled': True, 'supported': True, 'by_key': by_key, 'raw': data}


def try_consume_preprobed_shared_resource(
    probe_row: Dict[str, Any],
    item: Dict[str, Any],
    title: str,
    tmdb_id,
    item_type: str,
    parent_tmdb_id=None,
    season_number=None,
    year='',
    exclude_share_codes: List[str] | None = None,
    force_mode: str | None = None,
) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'success': False, 'reported_gap': False}
    client = SharedCenterClient()
    if not client.ready:
        return {'enabled': True, 'success': False, 'reported_gap': False}
    probe_row = dict(probe_row or {})
    sources = [x for x in (probe_row.get('sources') or []) if isinstance(x, dict)]
    return _consume_sources_for_subscription(
        client,
        sources,
        item,
        title,
        tmdb_id,
        item_type,
        parent_tmdb_id=parent_tmdb_id,
        season_number=season_number,
        year=year,
        exclude_share_codes=exclude_share_codes,
        force_mode=force_mode,
        reported_gap=bool(probe_row.get('reported_gap') or probe_row.get('status') == 'gap_registered'),
    )
def try_consume_shared_resource(
    item: Dict[str, Any],
    title: str,
    tmdb_id,
    item_type: str,
    parent_tmdb_id=None,
    season_number=None,
    year='',
    exclude_share_codes: List[str] | None = None,
    force_mode: str | None = None,
) -> Dict[str, Any]:
    '''尝试查询并消费中心共享资源。返回结果包含是否启用共享池、是否成功消费、是否命中缺口、以及其他相关信息。'''
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
    reported = False
    try:
        data = client.search_sources(queries, limit_per_item=200)
        sources = _flatten_search_results(data)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询中心共享池失败: {e}")

    if not sources:
        try:
            client.report_gaps(queries)
            reported = True
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 中心未命中，登记缺口失败: {e}")

    return _consume_sources_for_subscription(
        client,
        sources,
        item,
        title,
        tmdb_id,
        item_type,
        parent_tmdb_id=parent_tmdb_id,
        season_number=season_number,
        year=year,
        exclude_share_codes=exclude_share_codes,
        force_mode=force_mode,
        reported_gap=reported,
    )
def consume_center_sources(source_ids: List[str], mode: str = 'permanent', context: Dict[str, Any] = None) -> Dict[str, Any]:
    """按中心 source_id 手动消费共享资源。

    虚拟入库已移除；前端“中心资源库”只允许永久转存。
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
    sources = _filter_sources_by_episode_transfer_policy(sources)
    if not sources:
        return {'enabled': True, 'success': False, 'message': '中心未返回可用资源，或已被单集转存开关过滤'}

    first = sources[0]
    ctx = dict(context or {})
    ctx.setdefault('title', first.get('title') or first.get('file_name') or '')
    ctx.setdefault('tmdb_id', first.get('tmdb_id') or '')
    ctx.setdefault('item_type', first.get('item_type') or '')
    ctx.setdefault('parent_series_tmdb_id', first.get('parent_series_tmdb_id') or first.get('series_tmdb_id') or ctx.get('parent_tmdb_id') or '')
    ctx.setdefault('parent_tmdb_id', ctx.get('parent_series_tmdb_id') or first.get('parent_series_tmdb_id') or first.get('series_tmdb_id') or '')
    ctx.setdefault('season_number', first.get('season_number'))
    ctx.setdefault('episode_number', first.get('episode_number'))
    ctx.setdefault('year', first.get('release_year'))

    selected_mode = str(mode or '').strip().lower()
    if selected_mode == 'virtual':
        return {'enabled': True, 'success': False, 'message': '虚拟入库已移除，请使用“转存”。'}

    return _consume_permanent(client, sources, ctx)
