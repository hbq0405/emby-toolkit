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


def _shared_cfg(key: str, default=None):
    return settings_db.get_shared_resource_config().get(key, default)


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


def _extract_episode_number_fallback(source: Dict[str, Any] | None = None, context: Dict[str, Any] | None = None):
    source = source or {}
    context = context or {}
    for value in (
        source.get('episode_number'),
        context.get('episode_number'),
    ):
        episode = _safe_int(value, None)
        if episode is not None:
            return episode

    text = ' '.join(
        str(v or '').strip()
        for v in (
            source.get('file_name'),
            source.get('title'),
            context.get('file_name'),
            context.get('title'),
        )
        if str(v or '').strip()
    )
    if not text:
        return None

    for pattern in (
        r'(?i)\bS\d{1,2}\s*[._ -]*E(\d{1,4})\b',
        r'第\s*(\d{1,4})\s*[集话話]',
    ):
        match = re.search(pattern, text)
        if match:
            return _safe_int(match.group(1), None)
    return None


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


def _sanitize_filename(name: str) -> str:
    name = str(name or '').strip()
    name = re.sub(r'[\\/:*?"<>|]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or 'Unknown'


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



# 虚拟入库已移除：不再生成本地 STRM 投影/sidecar，也不再写 shared_virtual_items。

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


def _backup_instruction_from_report(report_resp: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(report_resp, dict):
        return {}
    info = report_resp.get('backup_share') or report_resp.get('backup_instruction') or {}
    if isinstance(info, dict) and info.get('should_create'):
        return info
    return {}


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


def _source_backup_count(src: Dict[str, Any]) -> int:
    try:
        return int(float((src or {}).get('_backup_count') or 1))
    except Exception:
        return 1


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


def _source_season_number(src: Dict[str, Any], context: Dict[str, Any] = None):
    context = context or {}
    for value in (
        (src or {}).get('season_number'),
        context.get('season_number'),
    ):
        season = _safe_int(value, None)
        if season is not None:
            return season
    for row in _share_source_rows(src):
        season = _safe_int((row or {}).get('season_number'), None)
        if season is not None:
            return season
    return None

def _extract_created_cid(resp: Dict[str, Any]) -> str:
    if not isinstance(resp, dict):
        return ''
    data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
    return str(
        resp.get('cid')
        or resp.get('file_id')
        or resp.get('id')
        or data.get('cid')
        or data.get('file_id')
        or data.get('id')
        or ''
    ).strip()


def _node_id_from_115(node: Dict[str, Any]) -> str:
    return str(
        (node or {}).get('cid')
        or (node or {}).get('file_id')
        or (node or {}).get('fid')
        or (node or {}).get('id')
        or ''
    ).strip()


def _node_name_from_115(node: Dict[str, Any]) -> str:
    return str(
        (node or {}).get('file_name')
        or (node or {}).get('fn')
        or (node or {}).get('name')
        or (node or {}).get('n')
        or ''
    ).strip()


def _node_sha1_from_115(node: Dict[str, Any]) -> str:
    return _norm_sha1((node or {}).get('sha1') or (node or {}).get('sha') or (node or {}).get('file_sha1'))


def _node_is_dir_from_115(node: Dict[str, Any]) -> bool:
    if not isinstance(node, dict):
        return False
    for key in ('is_dir', 'is_folder', 'folder'):
        if key in node:
            return bool(node.get(key))
    fc = str(node.get('fc') or node.get('file_category') or '').strip()
    if fc == '0':
        return True
    if str(node.get('type') or '').lower() in ('folder', 'dir', 'directory'):
        return True
    return not bool(_node_sha1_from_115(node)) and not os.path.splitext(_node_name_from_115(node))[1]


def _list_115_children(client, cid: str, limit: int = 1000) -> List[Dict[str, Any]]:
    try:
        resp = client.fs_files({'cid': str(cid), 'limit': limit, 'offset': 0, 'show_dir': 1, 'record_open_time': 0, 'count_folders': 0})
        data = (resp or {}).get('data') or []
        return [x for x in data if isinstance(x, dict)]
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询 115 子节点失败: cid={cid}, err={e}")
        return []


def _season_dir_name_candidates(season_number) -> List[str]:
    season = _safe_int(season_number, None)
    if season is None:
        return []
    return [
        f"Season {season}",
        f"Season {season:02d}",
        f"S{season}",
        f"S{season:02d}",
        f"第{season}季",
    ]


def _first_source_season_number(sources: List[Dict[str, Any]]):
    for row in sources or []:
        season = _safe_int((row or {}).get('season_number'), None)
        if season is not None:
            return season
    return None


def _node_as_backup_root(node: Dict[str, Any], source: str, fallback_name: str = '') -> Dict[str, Any]:
    fid = _node_id_from_115(node)
    if not fid:
        return {}
    return {
        'fid': fid,
        'name': _node_name_from_115(node) or fallback_name or fid,
        'is_dir': _node_is_dir_from_115(node),
        'source': source,
        'node': node,
    }


def _find_season_dir_child(children: List[Dict[str, Any]], season_number) -> Dict[str, Any]:
    candidates = {x.lower() for x in _season_dir_name_candidates(season_number)}
    if not candidates:
        return {}
    for node in children or []:
        if not _node_is_dir_from_115(node):
            continue
        name = _node_name_from_115(node)
        if name.lower() in candidates:
            return _node_as_backup_root(node, 'prepared_tv_import_season_dir', name)
    return {}


def _prepare_fallback_season_dir_for_backup(client, parent_cid: str, children: List[Dict[str, Any]], season_number) -> Dict[str, Any]:
    """标准剧目录下没有季目录时，临时创建 Season xx 并把本次接收内容移进去。

    这是兜底保护：备份分享绝不直接分享标准剧目录，避免该剧后续其它季进入同目录后，
    被已有分享码一并暴露出去。
    """
    season = _safe_int(season_number, None)
    if season is None:
        return {}
    movable_ids = []
    for node in children or []:
        fid = _node_id_from_115(node)
        if fid:
            movable_ids.append(fid)
    if not movable_ids:
        return {}

    season_name = f"Season {season:02d}"
    try:
        mk_resp = client.fs_mkdir(season_name, parent_cid)
        season_cid = _extract_created_cid(mk_resp)
        if not (mk_resp and mk_resp.get('state') and season_cid):
            logger.warning(f"  ➜ [共享资源] 创建备份季目录失败，放弃自动备份分享: name={season_name}, resp={mk_resp}")
            return {}
        try:
            move_resp = client.fs_move(movable_ids, season_cid)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 移动备份季内容失败，放弃自动备份分享: season={season_name}, err={e}")
            return {}
        if move_resp and move_resp.get('state'):
            try:
                P115CacheManager.save_cid(season_cid, str(parent_cid), season_name)
            except Exception:
                pass
            logger.info(
                "  ➜ [共享资源] 已为备份分享创建季目录并移动接收内容：%s -> cid=%s, files=%s",
                season_name, season_cid, len(movable_ids),
            )
            return {
                'fid': str(season_cid),
                'name': season_name,
                'is_dir': True,
                'source': 'prepared_tv_import_created_season_dir',
                'node': {'cid': str(season_cid), 'file_id': str(season_cid), 'file_name': season_name, 'name': season_name},
            }
        logger.warning(f"  ➜ [共享资源] 移动备份季内容失败，放弃自动备份分享: season={season_name}, resp={move_resp}")
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 准备备份季目录异常，放弃自动备份分享: season={season_name}, err={e}")
    return {}


def _find_received_backup_root(client, import_resp: Dict[str, Any], target_cid: str, sources: List[Dict[str, Any]], import_container: Dict[str, Any] = None) -> Dict[str, Any]:
    """根据 share_import 返回和待整理目录定位刚转存的根节点，供备份分享使用。

    注意：v12 会在待整理目录下先创建“剧名 (年份) {tmdb=xxx}”标准剧目录，
    再把季包转存进去。这个标准剧目录只用于整理任务识别，不能作为备份分享根；
    备份分享必须仍然分享真实季目录（如 Season 1），避免把该剧目录下其它季一并分享出去。
    """
    data = (import_resp or {}).get('data') if isinstance(import_resp, dict) else {}
    data = data if isinstance(data, dict) else {}
    receive_title = str(data.get('receive_title') or data.get('title') or '').strip()
    expected_sha1s = {_norm_sha1(s.get('sha1')) for s in (sources or []) if _norm_sha1(s.get('sha1'))}

    children = _list_115_children(client, target_cid)
    wrapped_tv_import = bool((import_container or {}).get('wrapped') and (import_container or {}).get('cid'))

    # 优先按接收标题定位。对季包来说，这里通常会命中 Season 1 / S01 等真实季目录。
    if receive_title:
        for node in children:
            if _node_name_from_115(node) == receive_title:
                return _node_as_backup_root(node, 'target_children_title', receive_title)

    if wrapped_tv_import:
        # v12 以后，target_cid 是临时创建的“剧标准目录”。备份分享不能直接分享它，
        # 必须从它下面找真实季目录。
        season = _first_source_season_number(sources)
        season_root = _find_season_dir_child(children, season)
        if season_root:
            return season_root

        dir_children = [node for node in children if _node_is_dir_from_115(node)]
        if len(dir_children) == 1:
            return _node_as_backup_root(dir_children[0], 'prepared_tv_import_single_child_dir')

        # 如果接收结果被 115 直接平铺到剧目录下，兜底创建 Season xx，把本次接收内容移进去，
        # 再分享这个季目录。这样不会把同剧其它季暴露到这个备份分享里。
        prepared = _prepare_fallback_season_dir_for_backup(client, target_cid, children, season)
        if prepared:
            return prepared

        logger.warning(
            "  ➜ [共享资源] 季包备份分享未找到可分享的季目录，已放弃自动备份，避免误分享整剧目录: "
            f"target_cid={target_cid}, receive_title={receive_title or '-'}"
        )
        return {}

    # 电影文件转存时，115 可能直接落文件；按 SHA1 兜底定位。
    if expected_sha1s:
        for node in children:
            if _node_sha1_from_115(node) in expected_sha1s:
                return _node_as_backup_root(node, 'target_children_sha1')

    # 最后才使用 share_import 返回里的明确 ID。pid 在不同接口里有歧义，所以只作为兜底。
    for key in ('fid', 'file_id', 'cid', 'id', 'pid'):
        value = str(data.get(key) or '').strip()
        if value and value != str(target_cid):
            return {
                'fid': value,
                'name': receive_title or value,
                'is_dir': bool(_safe_int(data.get('recv_folder_count'), 0) > 0),
                'source': f'import_resp_{key}',
                'node': data,
            }
    return {}


def _build_backup_share_items(client, root: Dict[str, Any], sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 无法加载备份分享文件收集辅助函数: {e}")
        return []

    files = sr._collect_files_from_115(
        client,
        str(root.get('fid') or ''),
        root_name=root.get('name') or '',
        max_depth=8,
        assume_dir=bool(root.get('is_dir')),
    )
    source_by_sha1 = {_norm_sha1(s.get('sha1')): s for s in (sources or []) if _norm_sha1(s.get('sha1'))}
    for item in files or []:
        sha1 = _norm_sha1(item.get('sha1'))
        src = source_by_sha1.get(sha1) or {}
        if src:
            item['tmdb_id'] = str(src.get('tmdb_id') or item.get('tmdb_id') or '')
            item['item_type'] = src.get('item_type') or item.get('item_type')
            item['season_number'] = src.get('season_number') if src.get('season_number') not in (None, '') else item.get('season_number')
            item['episode_number'] = src.get('episode_number') if src.get('episode_number') not in (None, '') else item.get('episode_number')
            item['size'] = item.get('size') or src.get('size') or 0
            item['file_name'] = item.get('file_name') or src.get('file_name') or ''
    return files or []


def _create_backup_share_after_import(
    center_client: SharedCenterClient,
    p115,
    src: Dict[str, Any],
    import_resp: Dict[str, Any],
    target_cid: str,
    report_resp: Dict[str, Any] | None,
    context: Dict[str, Any],
    import_container: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """中心下发备份指令后，用刚转存到本机 115 的资源创建一个普通分享并登记中心。"""
    instruction = _backup_instruction_from_report(report_resp)
    if not instruction:
        return {'created': False, 'skipped': True, 'reason': 'no_instruction'}

    item_type = str(instruction.get('item_type') or src.get('item_type') or '').strip().lower()
    if item_type == 'episode':
        return {'created': False, 'skipped': True, 'reason': 'episode_no_backup'}

    group_sources = [x for x in (src.get('_group_sources') or []) if isinstance(x, dict)] or [src]
    instruction_sources = [x for x in (instruction.get('sources') or []) if isinstance(x, dict)]
    sources = instruction_sources or group_sources

    root = _find_received_backup_root(p115, import_resp, target_cid, sources, import_container=import_container)
    if not root.get('fid'):
        return {'created': False, 'skipped': False, 'reason': 'received_root_not_found', 'instruction': instruction}

    try:
        share_resp = p115.share_create([str(root['fid'])], share_duration=-1, receive_code=None)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 创建备份分享失败: root={root}, err={e}")
        return {'created': False, 'skipped': False, 'reason': 'share_create_exception', 'message': str(e)}
    if not share_resp or not share_resp.get('state'):
        logger.warning(f"  ➜ [共享资源] 创建备份分享失败: root={root}, resp={share_resp}")
        return {'created': False, 'skipped': False, 'reason': 'share_create_failed', 'response': share_resp}

    share_data = share_resp.get('data') or {}
    share_code = share_data.get('share_code') or share_resp.get('share_code')
    receive_code = share_data.get('receive_code') or ''
    share_url = share_data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
    is_season_pack = item_type == 'season'

    files = _build_backup_share_items(p115, root, sources)
    if not files:
        logger.warning(f"  ➜ [共享资源] 备份分享已创建但未能收集文件明细，暂不登记中心: share={share_code}, root={root}")
        return {'created': True, 'registered': False, 'reason': 'no_files', 'share_code': share_code, 'share_response': share_resp}

    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 备份分享无法加载登记辅助函数: {e}")
        return {'created': True, 'registered': False, 'reason': 'helper_unavailable', 'share_code': share_code}

    standard = sources[0] if sources else src
    record = shared_share_db.create_share_record({
        'share_code': share_code,
        'receive_code': receive_code,
        'share_url': share_url,
        'share_type': 'season_pack' if is_season_pack else ('movie_folder' if root.get('is_dir') else 'movie_file'),
        'root_fid': str(root.get('fid') or ''),
        'root_name': root.get('name') or standard.get('title') or standard.get('file_name') or str(root.get('fid') or ''),
        'root_is_dir': bool(root.get('is_dir')),
        'tmdb_id': str(standard.get('tmdb_id') or context.get('tmdb_id') or ''),
        'item_type': 'Season' if is_season_pack else 'Movie',
        'parent_series_tmdb_id': context.get('parent_series_tmdb_id') or context.get('parent_tmdb_id') or None,
        'season_number': standard.get('season_number') if standard.get('season_number') not in (None, '') else context.get('season_number'),
        'episode_number': None,
        'title': standard.get('title') or context.get('title') or root.get('name') or '',
        'release_year': standard.get('release_year') or context.get('year'),
        'status': 'pending_review',
        'review_status': 'pending_review',
        'center_status': 'not_reported',
        'raw_json': {
            'auto_backup_share': True,
            'backup_share': True,
            'backup_mirror': True,
            'source_provider': 'backup_mirror',
            'share_source': 'backup_mirror',
            'source_provider_label': '备份分享',
            'source_label': '备份分享',
            'backup_instruction': instruction,
            'source_share_code': src.get('share_code'),
            'import_response': import_resp,
            'backup_share_response': share_resp,
            'received_root': root,
        },
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count) or record

    # 备份分享刚创建出来时，115 可能仍处于审核中。
    # 这里不能立即上传 RAW 并登记中心，否则中心资源库会出现“本地仍审核中、中心已上线”的脏状态。
    # 正确流程是：只落本地“我的分享”记录，等待维护任务/手动检查确认 share_info=alive 后，再走既有登记中心逻辑。
    items = shared_share_db.list_share_items(record['id']) or []
    shared_share_db.update_share_record(
        record['id'],
        center_status='not_reported',
        status='pending_review',
        review_status='pending_review',
        reported_count=0,
        last_error='自动备份分享已创建，等待 115 审核通过后由维护任务登记中心',
    )
    logger.info(
        "  ➜ [共享资源] 已按中心指令创建备份分享，等待审核通过后再登记中心：share=%s, files=%s",
        share_code, len(items),
    )
    return {
        'created': True,
        'registered': False,
        'share_code': share_code,
        'record_id': record.get('id'),
        'reported': 0,
        'total': len(items),
        'center_status': 'not_reported',
        'reason': 'pending_review_wait_for_maintenance',
    }


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
            logger.info(
                f"  ➜ [共享资源] 115分享转存返回：share={share_code}, cid={import_target_cid}, "
                f"backup={alt_index}/{len(alternatives)}, resp={str(resp)[:300]}"
            )
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
                    report_resp = None
                    try:
                        report_resp = client.report_transfer(
                            src.get('source_id'),
                            'success',
                            expected_sha1=_norm_sha1(src.get('sha1')),
                            expected_size=_safe_int(src.get('size'), 0) or None,
                            message='permanent import submitted',
                        )
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源] 上报转存成功失败，跳过备份分享触发: share={share_code}, err={e}")

                    if report_resp:
                        backup_result = _create_backup_share_after_import(
                            client,
                            p115,
                            src,
                            resp,
                            import_target_cid,
                            report_resp,
                            context,
                            import_container=import_container,
                        )
                        if backup_result.get('created'):
                            logger.info(f"  ➜ [共享资源] 自动备份分享处理完成: {backup_result}")
                        elif not backup_result.get('skipped'):
                            logger.warning(f"  ➜ [共享资源] 自动备份分享未完成: {backup_result}")
                break

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
        sources = _filter_sources_by_episode_transfer_policy(sources)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询中心共享池失败: {e}")

    # =================================================================
    # 中心查询按季返回后，本地仍然必须精确过滤到当前缺失集。
    # - 单集源：必须同季同集；
    # - SUBSCRIBED Season 会带 missing_episode_numbers，只消费缺失单集；
    # - 季包/旧数据没有 episode_number：保留，后续按 SHA1/包内文件消费。
    # =================================================================
    req_s_num = season_number if season_number not in (None, '') else item.get('season_number')
    req_e_num = item.get('episode_number')
    req_missing_eps = _normalize_episode_number_list(item.get('missing_episode_numbers'))
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
            # 单集源必须在缺失列表内；季包/旧数据没有集号，保留。
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
        # parent_tmdb_id 保留给旧调用方；parent_series_tmdb_id 是新链路唯一推荐字段。
        'parent_tmdb_id': str(parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or ''),
        'parent_series_tmdb_id': str(parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or ''),
        'season_number': season_number,
        'episode_number': item.get('episode_number'), # ★ 确保 context 里有 episode_number
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



def _build_backup_search_query_from_source(src: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """前端手动选中一个中心源时，反查同资源备份分享。"""
    src = src or {}
    context = context or {}
    item_type = str(src.get('item_type') or context.get('item_type') or '').strip()
    item_type_l = item_type.lower()
    tmdb_id = str(src.get('tmdb_id') or context.get('tmdb_id') or context.get('parent_series_tmdb_id') or context.get('parent_tmdb_id') or '').strip()
    season = src.get('season_number') if src.get('season_number') not in (None, '') else context.get('season_number')
    episode = src.get('episode_number') if src.get('episode_number') not in (None, '') else context.get('episode_number')
    title = src.get('title') or context.get('title') or src.get('file_name') or ''
    year = src.get('release_year') or context.get('year')

    if item_type_l == 'movie':
        return _build_gap_item(tmdb_id=tmdb_id, item_type='Movie', title=title, year=year)
    if item_type_l == 'season':
        return _build_gap_item(tmdb_id=tmdb_id, item_type='Season', title=title, season_number=season, year=year)
    if item_type_l == 'episode':
        return _build_gap_item(tmdb_id=tmdb_id, item_type='Episode', title=title, season_number=season, episode_number=episode, year=year)
    if season not in (None, '') and episode in (None, ''):
        return _build_gap_item(tmdb_id=tmdb_id, item_type='Season', title=title, season_number=season, year=year)
    return _build_gap_item(tmdb_id=tmdb_id, item_type=item_type or 'Movie', title=title, season_number=season, episode_number=episode, year=year)


def _expand_sources_with_permanent_backups(client: SharedCenterClient, sources: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """手动永久转存也补齐同 SHA1 / 同季包指纹的所有备份候选。"""
    sources = [dict(s) for s in (sources or []) if isinstance(s, dict)]
    if not sources or not hasattr(client, 'search_sources'):
        return sources

    queries = []
    for src in sources:
        q = _build_backup_search_query_from_source(src, context or {})
        if q and q.get('tmdb_id'):
            queries.append(q)
    if not queries:
        return sources

    try:
        data = client.search_sources(queries, limit_per_item=200)
        candidates = _filter_sources_by_episode_transfer_policy(_flatten_search_results(data))
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 拉取手动转存备份候选失败，沿用已选源: {e}")
        return sources
    if not candidates:
        return sources

    selected_ids = {str(s.get('source_id') or '').strip() for s in sources if str(s.get('source_id') or '').strip()}
    selected_share_codes = {str(s.get('share_code') or '').strip() for s in sources if str(s.get('share_code') or '').strip()}
    wanted_keys = set()
    for src in candidates:
        sid = str(src.get('source_id') or '').strip()
        share_code = str(src.get('share_code') or '').strip()
        if (sid and sid in selected_ids) or (share_code and share_code in selected_share_codes):
            key = str(src.get('_resource_key') or '').strip() or _permanent_resource_key_for_rows([src], context)
            if key:
                wanted_keys.add(key)

    if not wanted_keys:
        for src in sources:
            key = _permanent_resource_key_for_rows([src], context)
            if key:
                wanted_keys.add(key)

    expanded = []
    seen = set()
    for src in candidates:
        key = str(src.get('_resource_key') or '').strip() or _permanent_resource_key_for_rows([src], context)
        if wanted_keys and key not in wanted_keys:
            continue
        dedupe_key = (str(src.get('source_id') or ''), _norm_sha1(src.get('sha1')))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        expanded.append(src)

    if expanded:
        logger.info(
            "  ➜ [共享资源] 手动永久转存已补齐备份候选：selected=%s, expanded=%s, resources=%s",
            len(sources), len(expanded), len(wanted_keys) or '-'
        )
        return expanded
    return sources

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

    sources = _expand_sources_with_permanent_backups(client, sources, ctx)
    return _consume_permanent(client, sources, ctx)
