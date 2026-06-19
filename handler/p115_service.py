# handler/p115_service.py
import logging
import requests
import os
import hashlib
import base64
import hmac    
from email.utils import formatdate 
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from gevent import spawn_later
from typing import Any, Dict
import time
import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
import handler.tmdb as tmdb
from tasks import helpers
import utils
from handler.p115_media_analyzer import P115MediaAnalyzerMixin
from handler.tg_media_candidate import candidate_to_recognition_hints, is_recognition_hint_eligible, lookup_candidate_hint_for_name, normalize_title_for_match
try:
    from p115client import P115Client
except ImportError:
    P115Client = None

logger = logging.getLogger(__name__)

from collections import OrderedDict

P115_APP_LABELS = {
    "web": "网页版",
    "tv": "安卓电视端",
    "alipaymini": "支付宝小程序",
    "wechatmini": "微信小程序",
}

def get_115_app_label(app_type):
    app_type = str(app_type or "web").strip().lower()
    return P115_APP_LABELS.get(app_type, app_type)

def get_115_ua(app_type):
    """根据 APP 类型返回对应的真实 User-Agent，防止 115 风控"""
    ua_map = {
        'web': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'mac': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2',
        'linux': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) 115Browser/25.0.3.2',
        'tv': 'Mozilla/5.0 (Linux; Android 7.1.2; 115disk Build/NHG47K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.114 Safari/537.36',
        'alipaymini': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 AlipayDefined(nt:WIFI,ws:390|844|3.0) AliApp(AP/10.5.33.8143) AlipayClient/10.5.33.8143 Language/zh-Hans Region/CN',
        'wechatmini': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.42(0x18002a2c) NetType/WIFI Language/zh_CN'
    }
    return ua_map.get(str(app_type).lower() if app_type else 'web', ua_map['web'])




def _p115_normalize_rule_path(path):
    """标准化 115 分类相对路径 / 本地 STRM 相对路径，用于无 API 前缀匹配。"""
    text = str(path or '').strip().replace('\\', '/')
    text = re.sub(r'/+', '/', text).strip('/')
    return text


def _p115_load_sorting_rule_targets():
    """读取 115 分类规则，输出可用于路径前缀匹配的分类目录。

    返回项包含：cid / category_path / rule。category_path 是相对本地 STRM 根目录的路径，
    例如“纪录片”或“电影/外语电影”。
    """
    raw_rules = settings_db.get_setting('p115_sorting_rules') or []
    if isinstance(raw_rules, str):
        try:
            raw_rules = json.loads(raw_rules)
        except Exception:
            raw_rules = []
    if not isinstance(raw_rules, list):
        return []

    targets = []
    seen = set()
    for rule in raw_rules:
        if not isinstance(rule, dict):
            continue
        if rule.get('enabled') is False:
            continue
        cid = str(rule.get('cid') or '').strip()
        if not cid or cid == '0':
            continue
        category_path = _p115_normalize_rule_path(
            rule.get('category_path')
            or rule.get('dir_name')
            or rule.get('name')
        )
        if not category_path:
            continue
        key = (cid, category_path.casefold())
        if key in seen:
            continue
        seen.add(key)
        targets.append({
            'cid': cid,
            'category_path': category_path,
            'rule': rule,
        })

    # 最长路径优先，防止“电影”抢先命中“电影/外语电影”。
    targets.sort(key=lambda x: len(x.get('category_path') or ''), reverse=True)
    return targets


def resolve_p115_sorting_target_by_local_path(local_path, *, local_root=None):
    """根据本地/相对路径推导 115 分类规则 target_cid，零 115 API 消耗。

    适用于 MP 直出：Webhook 能拿到文件绝对路径或 115_path 映射后的相对路径，
    只要它位于某个分类 category_path 下，就能反推出分类根目录 CID。
    """
    rel_path = _p115_normalize_rule_path(local_path)
    if not rel_path:
        return None

    candidates = []
    # 调用方显式传入 local_root 优先；否则使用配置中的本地 STRM 根目录。
    if local_root:
        candidates.append(local_root)
    try:
        cfg = get_config() or {}
        for key in (constants.CONFIG_OPTION_LOCAL_STRM_ROOT, 'local_strm_root', 'p115_local_strm_root', 'strm_root', 'p115_strm_root'):
            value = cfg.get(key)
            if value:
                candidates.append(value)
    except Exception:
        pass

    rel_cf = rel_path.casefold()
    for root in candidates:
        root_norm = _p115_normalize_rule_path(root)
        if not root_norm:
            continue
        root_cf = root_norm.casefold()
        if rel_cf == root_cf:
            rel_path = ''
            rel_cf = ''
            break
        if rel_cf.startswith(root_cf + '/'):
            rel_path = rel_path[len(root_norm):].strip('/')
            rel_cf = rel_path.casefold()
            break

    if not rel_path:
        return None

    for target in _p115_load_sorting_rule_targets():
        cat = _p115_normalize_rule_path(target.get('category_path'))
        if not cat:
            continue
        cat_cf = cat.casefold()
        if rel_cf == cat_cf or rel_cf.startswith(cat_cf + '/'):
            return dict(target)

    return None

def _p115_parse_sign_check_range(sign_check):
    """解析 115 upload/init 返回的 sign_check，返回闭区间 start/end。"""
    text = str(sign_check or '').strip()
    m = re.fullmatch(r'\s*(\d+)\s*-\s*(\d+)\s*', text)
    if not m:
        raise ValueError(f'非法 sign_check: {text!r}')
    start, end = int(m.group(1)), int(m.group(2))
    if start < 0 or end < start:
        raise ValueError(f'非法 sign_check 区间: {text!r}')
    return start, end


def _p115_extract_down_url(resp):
    """兼容提取 115 OpenAPI/Cookie 返回的直链 URL。"""
    if not resp:
        return ''
    if isinstance(resp, str):
        return resp
    # p115client 的 P115URL 等对象
    try:
        text = str(resp)
        if text.startswith(('http://', 'https://')):
            return text
    except Exception:
        pass
    if not isinstance(resp, dict):
        return ''
    for key in ('url', 'download_url', 'downurl', 'direct_url'):
        val = resp.get(key)
        if isinstance(val, str) and val.startswith(('http://', 'https://')):
            return val
        if isinstance(val, dict):
            nested = _p115_extract_down_url(val)
            if nested:
                return nested
    data = resp.get('data')
    if isinstance(data, dict):
        # OpenAPI downurl: data = {fid: {url: {url: ...}}}
        for val in data.values():
            nested = _p115_extract_down_url(val)
            if nested:
                return nested
        nested = _p115_extract_down_url(data)
        if nested:
            return nested
    if isinstance(data, list):
        for val in data:
            nested = _p115_extract_down_url(val)
            if nested:
                return nested
    return ''


def _p115_range_sha1_from_url(down_url, sign_check, user_agent=None, label='115', timeout=45):
    """按 sign_check 对直链执行 Range GET，返回 (sign_val, byte_len, start, end)。"""
    start, end = _p115_parse_sign_check_range(sign_check)
    expected_len = end - start + 1
    headers = {
        'Range': f'bytes={start}-{end}',
        'Accept': '*/*',
        'Connection': 'close',
    }
    if user_agent:
        headers['User-Agent'] = user_agent
    logger.debug(
        f"  ➜ [负载均衡签名] {label} 开始读取 sign_check Range: "
        f"{start}-{end}，expected={expected_len} bytes"
    )
    resp = requests.get(str(down_url), headers=headers, timeout=timeout, allow_redirects=True)
    status_code = getattr(resp, 'status_code', None)
    if status_code != 206:
        raise RuntimeError(f'Range GET HTTP={status_code}，expected=206')
    content = resp.content or b''
    if not content:
        raise RuntimeError('Range GET 返回空内容')
    if len(content) != expected_len:
        logger.warning(
            f"  ➜ [负载均衡签名] {label} Range 长度与 sign_check 不一致："
            f"got={len(content)}, expected={expected_len}，继续按实际内容计算 sign_val"
        )
    sign_val = hashlib.sha1(content).hexdigest().upper()
    logger.debug(
        f"  ➜ [负载均衡签名] {label} Range 读取完成："
        f"got={len(content)} bytes, sign_val={sign_val[:12]}..."
    )
    return sign_val, len(content), start, end


def _p115_try_local_holder_sign(*, pick_code, sign_check, downurl_getter, user_agent=None, label='本机Holder', sha1='', file_name=''):
    """最小闭环：本机作为 holder，用自己的 CK/Token 取直链并计算 sign_val。"""
    pc = str(pick_code or '').strip()
    if not pc:
        logger.warning(
            f"  ➜ [负载均衡签名] {label} 无法计算 sign_val：缺少源文件 pick_code；"
            f"sha1={str(sha1 or '')[:12]}..., file={file_name or '-'}"
        )
        return None
    logger.debug(
        f"  ➜ [负载均衡签名] {label} 收到二次校验任务："
        f"sha1={str(sha1 or '')[:12]}..., pc={pc[:8]}..., sign_check={sign_check}, file={file_name or '-'}"
    )
    down_url = downurl_getter(pc, user_agent)
    down_url = _p115_extract_down_url(down_url)
    if not down_url:
        raise RuntimeError(f'{label} 未能获取源文件直链')
    logger.debug(f"  ➜ [负载均衡签名] {label} 已获取源文件直链，准备 Range 读取：pc={pc[:8]}...")
    sign_val, byte_len, start, end = _p115_range_sha1_from_url(
        down_url, sign_check, user_agent=user_agent, label=label
    )
    return {
        'sign_val': sign_val,
        'byte_len': byte_len,
        'start': start,
        'end': end,
        'pick_code': pc,
    }


def _p115_lookup_local_holder_file_for_sign(*, sha1='', size=0, pick_code='', file_name=''):
    """按 sha1/pc 在本地 p115_filesystem_cache 找 holder 文件，不向中心暴露 CK/PC。"""
    sha1 = str(sha1 or '').strip().upper()
    pc = str(pick_code or '').strip()
    out = {'sha1': sha1, 'pick_code': pc, 'file_name': str(file_name or '').strip(), 'size': size or 0}
    cache_mgr = globals().get('P115CacheManager')
    try:
        row = None
        if pc and cache_mgr and hasattr(cache_mgr, 'get_file_cache_by_pickcode'):
            row = cache_mgr.get_file_cache_by_pickcode(pc)
        if not row and sha1 and cache_mgr and hasattr(cache_mgr, 'get_file_cache_by_sha1'):
            row = cache_mgr.get_file_cache_by_sha1(sha1)
        if row:
            row = dict(row)
            out['fid'] = str(row.get('id') or row.get('fid') or '')
            out['pick_code'] = out.get('pick_code') or str(row.get('pick_code') or '').strip()
            out['file_name'] = out.get('file_name') or str(row.get('name') or row.get('file_name') or '').strip()
            out['sha1'] = out.get('sha1') or str(row.get('sha1') or '').strip().upper()
            try:
                out['size'] = int(row.get('size') or out.get('size') or 0)
            except Exception:
                pass
            return out
    except Exception as e:
        logger.debug(f"  ➜ [负载均衡签名] 查询 P115CacheManager holder 文件失败: {e}")

    try:
        clauses, args = [], []
        if pc:
            clauses.append('pick_code=%s')
            args.append(pc)
        if sha1:
            clauses.append('UPPER(sha1)=%s')
            args.append(sha1)
        if not clauses:
            return out
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, parent_id, name, sha1, pick_code, local_path, size
                    FROM p115_filesystem_cache
                    WHERE {' OR '.join(clauses)}
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    args,
                )
                row = cur.fetchone()
                if row:
                    row = dict(row)
                    out['fid'] = str(row.get('id') or '')
                    out['pick_code'] = out.get('pick_code') or str(row.get('pick_code') or '').strip()
                    out['file_name'] = out.get('file_name') or str(row.get('name') or '').strip()
                    out['sha1'] = out.get('sha1') or str(row.get('sha1') or '').strip().upper()
                    try:
                        out['size'] = int(row.get('size') or out.get('size') or 0)
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"  ➜ [负载均衡签名] 直接查询 p115_filesystem_cache holder 文件失败: {e}")
    return out

class LimitedCache(OrderedDict):
    """带容量限制的内存缓存，防止内存泄漏撑爆服务器"""
    def __init__(self, maxsize=1000, *args, **kwds):
        self.maxsize = maxsize
        super().__init__(*args, **kwds)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            # 超过容量时，弹出最早插入的元素 (FIFO)
            self.popitem(last=False)

# 内存级缓存，防止同剧集/同系列疯狂重复请求 TMDb (限制容量)
_TMDB_METADATA_CACHE = LimitedCache(maxsize=1000)
_TMDB_SEARCH_CACHE = LimitedCache(maxsize=1000)
_AI_PARSE_CACHE = LimitedCache(maxsize=1000)
_MP_PARSE_CACHE = LimitedCache(maxsize=1000)
_RULE_PARSE_CACHE = LimitedCache(maxsize=2000)

# 全局直链缓存池，供反向代理和Web路由共享 
_DIRECT_URL_CACHE = LimitedCache(maxsize=2000)

# 全局目录缓存池
_GLOBAL_DIR_CACHE = LimitedCache(maxsize=5000)
_GLOBAL_DIR_LOCK = threading.Lock()
_TRANSFER_CONTEXTS_KEY = "p115_transfer_contexts"
_TRANSFER_CONTEXT_LIMIT = 200
_AUTHORITY_RECOGNITION_SOURCES = frozenset({
    "tg_rule_library",
    "transfer_context",
    "shared_transfer_context",
    "hdhive-share-import",
    "shared-permanent-import",
    "tg-channel-import",
})


def _normalize_transfer_context_name(name):
    normalized = re.sub(r'[\s\._\-]+', '', str(name or '').strip()).lower()
    return normalized


def _build_transfer_context_keys(*values):
    keys = []
    seen = set()
    for value in values:
        norm = _normalize_transfer_context_name(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        keys.append(norm)
    return keys


def _coerce_transfer_context_dict(payload):
    if not isinstance(payload, dict):
        return {}

    out = {}
    for key in (
        "tmdb_id",
        "year",
        "title",
        "clean_title",
        "identify_title",
        "media_type",
        "source_kind",
        "pick_code",
        "sha1",
        "root_name",
        "source",
        "authority_role",
        "conflict_reason",
        "parse_version",
    ):
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        out[key] = str(value).strip()

    season_number = payload.get("season_number")
    if season_number not in (None, ""):
        try:
            out["season_number"] = int(season_number)
        except Exception:
            pass

    episode_number = payload.get("episode_number")
    if episode_number not in (None, ""):
        try:
            out["episode_number"] = int(episode_number)
        except Exception:
            pass

    confidence = str(payload.get("confidence") or "").strip().lower()
    if confidence in ("low", "medium", "high"):
        out["confidence"] = confidence

    is_special = payload.get("is_special")
    if isinstance(is_special, bool):
        out["is_special"] = is_special
    elif str(is_special).strip().lower() in ("1", "true", "yes"):
        out["is_special"] = True

    for list_key in ("matched_rules", "evidence", "alias_titles"):
        raw_list = payload.get(list_key)
        if not isinstance(raw_list, (list, tuple, set)):
            continue
        normalized = []
        seen = set()
        for item in raw_list:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            normalized.append(text)
        if normalized:
            out[list_key] = normalized

    source_kinds = payload.get("source_kinds")
    if isinstance(source_kinds, (list, tuple, set)):
        normalized_source_kinds = []
        seen_source_kinds = set()
        for item in source_kinds:
            text = str(item or "").strip()
            if not text or text in seen_source_kinds:
                continue
            seen_source_kinds.add(text)
            normalized_source_kinds.append(text)
        if normalized_source_kinds:
            out["source_kinds"] = normalized_source_kinds
    elif payload.get("source_kind"):
        out["source_kinds"] = [str(payload.get("source_kind")).strip()]

    keys = payload.get("keys")
    if isinstance(keys, (list, tuple, set)):
        out["keys"] = _build_transfer_context_keys(*keys)
    else:
        out["keys"] = _build_transfer_context_keys(
            payload.get("root_name"),
            payload.get("title"),
        )

    if not out.get("keys"):
        return {}

    return out


def _extract_sidecar_episode_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        match = re.search(
            r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b'
            r'|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b'
            r'|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b'
            r'|第\s*\d{1,4}\s*季\s*(\d{1,4})\s*[集话話回]'
            r'|第\s*(\d{1,4})\s*[集话話回]',
            value,
            re.IGNORECASE,
        )
        if match:
            episode = match.group(2) or match.group(3) or match.group(4) or match.group(5) or match.group(6)
            if episode is not None:
                return int(episode)
    return None


def _extract_sidecar_part_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', value)
        if match:
            return int(match.group(2))
    return None


def _transfer_context_to_recognition_hints(context):
    context = _coerce_transfer_context_dict(context if isinstance(context, dict) else {})
    if not context:
        return {}

    source = str(context.get("source") or "").strip()
    source_kind = str(context.get("source_kind") or "").strip()
    if not source_kind:
        source_kind = source or "transfer_context"
    authority_role = str(context.get("authority_role") or "").strip() or "expected"
    if not source:
        source = "transfer_context"
    if source in ("shared-permanent-import",):
        normalized_source = "shared_transfer_context"
    else:
        normalized_source = source

    return {
        "tmdb_id": context.get("tmdb_id"),
        "title": context.get("title") or context.get("identify_title") or context.get("clean_title"),
        "clean_title": context.get("clean_title") or context.get("title"),
        "identify_title": context.get("identify_title") or context.get("title") or context.get("clean_title"),
        "year": context.get("year"),
        "media_type": context.get("media_type"),
        "source_kind": source_kind,
        "source_kinds": list(context.get("source_kinds") or ([source_kind] if source_kind else [])),
        "season_number": context.get("season_number"),
        "episode_number": context.get("episode_number"),
        "is_special": bool(context.get("is_special")),
        "confidence": context.get("confidence") or "high",
        "evidence": list(context.get("evidence") or []),
        "matched_rules": list(context.get("matched_rules") or []),
        "conflict_reason": context.get("conflict_reason") or "",
        "parse_version": context.get("parse_version") or "transfer-context-v1",
        "alias_titles": list(context.get("alias_titles") or []),
        "source": normalized_source,
        "authority_role": authority_role,
    }


def _is_authoritative_recognition_hint(hints):
    normalized = candidate_to_recognition_hints(hints or {})
    if not normalized:
        return False
    if normalized.get("conflict_reason"):
        return False
    if normalized.get("confidence") not in ("medium", "high"):
        return False
    source = str(normalized.get("source") or "").strip()
    source_kind = str(normalized.get("source_kind") or "").strip()
    authority_role = str(normalized.get("authority_role") or "").strip().lower()
    return (
        source in _AUTHORITY_RECOGNITION_SOURCES
        or source_kind in _AUTHORITY_RECOGNITION_SOURCES
        or authority_role in ("expected", "authoritative", "canonical")
    )


def _extract_sidecar_season_number(*texts):
    for text in texts:
        if not text:
            continue
        value = str(text)
        match = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', value, re.IGNORECASE)
        if match:
            return int(match.group(1))
        match = re.search(r'Season\s*(\d{1,4})\b', value, re.IGNORECASE)
        if match:
            return int(match.group(1))
        match = re.search(r'第\s*(\d{1,4})\s*季', value)
        if match:
            return int(match.group(1))
    return None


def _is_related_sidecar_name(video_name, other_name):
    video_name = str(video_name or '')
    other_name = str(other_name or '')
    if not video_name or not other_name:
        return False

    video_base = video_name.rsplit('.', 1)[0] if '.' in video_name else video_name
    if other_name.startswith(video_base):
        return True

    video_season = _extract_sidecar_season_number(video_name)
    other_season = _extract_sidecar_season_number(other_name)
    video_episode = _extract_sidecar_episode_number(video_name)
    other_episode = _extract_sidecar_episode_number(other_name)
    video_part = _extract_sidecar_part_number(video_name)
    other_part = _extract_sidecar_part_number(other_name)

    if video_episode is None or other_episode is None:
        return False

    if video_season is not None and other_season is not None and video_season != other_season:
        return False

    if video_part is not None and other_part is not None and video_part != other_part:
        return False

    return video_episode == other_episode

_NOISE_TOKEN_PATTERNS = [
    r'(?i)\b(?:WEB[-_. ]?DL|WEB[-_. ]?RIP|BLU[-_. ]?RAY|BDRIP|BRRIP|REMUX|DVDRIP|HDTV|UHD)\b',
    r'(?i)\b(?:HDR10\+?|HDR|DV|DOVI|DOLBY[.\s_-]*VISION|HLG)\b',
    r'(?i)\b(?:HEVC|AVC|X265|X264|H265|H264|10BIT|8BIT|AAC\d?(?:\.\d)?|DDP\d?(?:\.\d)?|DD\d?(?:\.\d)?|TRUEHD|ATMOS|DTS(?:[-_. ]?HD)?|FLAC)\b',
    r'(?i)\b(?:2160P|1080P|720P|576P|480P|4K)\b',
    r'(?i)\b(?:NF|NETFLIX|AMZN|AMAZON|DSNP|DISNEY|HMAX|MAX|ATVP|APPLE|IQIYI|YOUKU|WEB)\b',
    r'(?i)\b(?:MULTI|DUAL[-_. ]?AUDIO|DUAL|PROPER|REPACK|READNFO|EXTENDED|UNCUT|COMPLETE|FINAL)\b',
    r'(?i)\b(?:CHS|CHT|ENG|JPN|KOR|GB|BIG5|简中|繁中|中字|双语|国粤|内封|外挂|特效字幕)\b',
    r'(?i)\b(?:AAC2\.0|AAC5\.1|DDP5\.1|DD5\.1|DTS5\.1|TRUEHD7\.1|ATMOS7\.1)\b',
    r'(?i)\b(?:CAM|TS|TC|R5)\b',
]

_DATE_EPISODE_PATTERNS = [
    re.compile(r'(?<!\d)(20\d{2})[.\-_ ](0[1-9]|1[0-2])[.\-_ ]([0-3]\d)(?!\d)'),
    re.compile(r'(?<!\d)(20\d{2})(0[1-9]|1[0-2])([0-3]\d)(?!\d)'),
]

_SPECIAL_FLAG_PATTERNS = [
    re.compile(r'(?i)(?:^|[ \.\-_/\[(])(?:specials?|sp|ova|oad|oads|extra(?:s)?|ncop|nced)(?:$|[ \.\-_/)\]])'),
    re.compile(r'(?:特别篇|特別篇|番外(?:篇)?|外传|外傳|总集篇|總集篇|OVA|OAD)', re.IGNORECASE),
]

def get_115_tokens():
    """唯一真理：只从独立数据库获取 Token 和 Cookie"""
    auth_data = settings_db.get_setting('p115_auth_tokens')
    if auth_data:
        cookie = auth_data.get('cookie')
        # ★ 新增：读取 app_type，老用户默认兼容为 web
        app_type = auth_data.get('app_type', 'web')
        return auth_data.get('access_token'), auth_data.get('refresh_token'), cookie, app_type
    return None, None, None, 'web'

def save_115_tokens(access_token, refresh_token, cookie=None, app_type=None):
    """唯一真理：只写入独立数据库"""
    existing = settings_db.get_setting('p115_auth_tokens') or {}
    settings_db.save_setting('p115_auth_tokens', {
        'access_token': access_token if access_token is not None else existing.get('access_token'),
        'refresh_token': refresh_token if refresh_token is not None else existing.get('refresh_token'),
        'cookie': cookie if cookie is not None else existing.get('cookie'),
        # ★ 新增：保存 app_type
        'app_type': app_type if app_type is not None else existing.get('app_type', 'web')
    })

_refresh_lock = threading.Lock()

def refresh_115_token(failed_token=None):
    """使用 refresh_token 换取新的 access_token (纯数据库读写)"""
    with _refresh_lock:
        try:
            current_access, current_refresh, _, _ = get_115_tokens()
            if not current_refresh:
                return False
                
            # ★ 并发防御：如果数据库里的 token 已经和刚才报错的 token 不一样了，说明别的线程刚续期完，直接放行！
            if failed_token and current_access and current_access != failed_token:
                logger.info("  ➜ [115] 检测到 Token 已被其他线程续期，直接放行。")
                if P115Service._openapi_client:
                    P115Service._openapi_client.access_token = current_access
                    P115Service._openapi_client.headers["Authorization"] = f"Bearer {current_access}"
                return True

            url = "https://passportapi.115.com/open/refreshToken"
            payload = {"refresh_token": current_refresh}
            resp = requests.post(url, data=payload, timeout=10).json()
            
            if resp.get('state'):
                new_access_token = resp['data']['access_token']
                new_refresh_token = resp['data']['refresh_token']
                expires_in = resp['data'].get('expires_in', 0)
                hours = round(expires_in / 3600, 1)
                
                # 写入数据库
                save_115_tokens(new_access_token, new_refresh_token)
                
                if P115Service._openapi_client:
                    P115Service._openapi_client.access_token = new_access_token
                    P115Service._openapi_client.headers["Authorization"] = f"Bearer {new_access_token}"
                
                logger.info(f"  ➜ [115] Token 自动续期成功！有效时长 {hours} 小时。")
                return True
            else:
                logger.error(f"  ➜ Token 续期失败: {resp.get('message')}，可能需要重新扫码")
                return False
        except Exception as e:
            logger.error(f"  ➜ Token 续期请求异常: {e}")
            return False

# ======================================================================
# ★★★ 115 OpenAPI 客户端 (仅管理操作：扫描/创建目录/移动文件) ★★★
# ======================================================================
class P115OpenAPIClient:
    """使用 Access Token 进行管理操作"""
    def __init__(self, access_token):
        if not access_token:
            raise ValueError("Access Token 不能为空")
        self.access_token = access_token.strip()
        self.base_url = "https://proapi.115.com"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "Emby-toolkit/1.0 (OpenAPI)"
        }
        # ★ 核心修复：引入 Session 连接池，复用 TCP/TLS 连接，防止高并发端口耗尽
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=1)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _do_request(self, method, url, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                current_token = self.access_token 
                
                req_headers = self.headers.copy()
                if 'headers' in kwargs:
                    req_headers.update(kwargs.pop('headers'))
                    
                # ★ 核心修复：使用 self.session 发送请求
                resp = self.session.request(method, url, headers=req_headers, timeout=30, **kwargs).json()
                
                if not resp.get("state") and resp.get("code") in [40140123, 40140124, 40140125, 40140126]:
                    logger.warning("  ➜ [115] 检测到 Token 已过期，正在触发自动续期...")
                    if refresh_115_token(current_token):
                        logger.info("  ➜ [115] 续期完成，重新发送刚才失败的请求...")
                        # ★ 这里也要改用 session
                        return self.session.request(method, url, headers=self.headers, timeout=30, **kwargs).json()
                    else:
                        logger.error("  ➜ [115] 续期彻底失败，Token 已死亡，请前往 WebUI 重新扫码！")
                
                return resp
            except Exception as e:
                err_str = str(e)
                if "NameResolutionError" in err_str or "Connection" in err_str or "Timeout" in err_str:
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                return {"state": False, "error_msg": err_str}

    def get_user_info(self):
        url = f"{self.base_url}/open/user/info"
        return self._do_request("GET", url)

    def fs_files(self, payload):
        url = f"{self.base_url}/open/ufile/files"
        params = {"show_dir": 1, "limit": 1000, "offset": 0}
        if isinstance(payload, dict): params.update(payload)
        return self._do_request("GET", url, params=params)

    def fs_files_app(self, payload): 
        return self.fs_files(payload)
    
    def fs_search(self, payload):
        url = f"{self.base_url}/open/ufile/search"
        params = {"limit": 100, "offset": 0}
        if isinstance(payload, dict): params.update(payload)
        return self._do_request("GET", url, params=params)
    
    def fs_downurl(self, pick_code, user_agent=None):
        """OpenAPI 获取下载直链"""
        url = f"{self.base_url}/open/ufile/downurl"
        headers = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        return self._do_request("POST", url, data={"pick_code": str(pick_code)}, headers=headers)

    def fs_get_info(self, file_id):
        url = f"{self.base_url}/open/folder/get_info"
        return self._do_request("GET", url, params={"file_id": str(file_id)})

    def fs_mkdir(self, name, pid):
        url = f"{self.base_url}/open/folder/add"
        resp = self._do_request("POST", url, data={"pid": str(pid), "file_name": str(name)})
        if resp.get("state") and "data" in resp: 
            resp["cid"] = resp["data"].get("file_id")
        return resp

    def fs_move(self, fids, to_cid):
        url = f"{self.base_url}/open/ufile/move"
        # ★ 支持传入列表，自动用逗号拼接
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_ids": fids_str, "to_cid": str(to_cid)})

    def fs_copy(self, fids, to_cid):
        url = f"{self.base_url}/open/ufile/copy"
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_id": fids_str, "pid": str(to_cid), "nodupli": 0})

    def fs_rename(self, fid_name_tuple):
        url = f"{self.base_url}/open/ufile/update"
        return self._do_request("POST", url, data={"file_id": str(fid_name_tuple[0]), "file_name": str(fid_name_tuple[1])})

    def fs_delete(self, fids):
        url = f"{self.base_url}/open/ufile/delete"
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_ids": fids_str})

    def rb_del(self, tids=None):
        url = f"{self.base_url}/open/rb/del"
        data = {}
        if tids:
            data['tid'] = ",".join([str(t) for t in tids]) if isinstance(tids, list) else str(tids)
        return self._do_request("POST", url, data=data)
    
    def fs_upload_init(self, file_name, file_size, target_cid, sha1, preid, sign_key=None, sign_val=None):
        """文件上传初始化调度接口"""
        url = f"{self.base_url}/open/upload/init"
        data = {
            "file_name": file_name,
            "file_size": file_size,
            "target": f"U_1_{target_cid}",
            "fileid": sha1,
            "preid": preid
        }
        if sign_key and sign_val:
            data["sign_key"] = sign_key
            data["sign_val"] = sign_val
        return self._do_request("POST", url, data=data)


    def rapid_upload(self, payload=None, **kwargs):
        """按 SHA1/size 在目标目录执行 115 秒传。

        Rapid v2 只在本机使用 CK/Token，不把账号凭据上传中心。
        这里允许调用方只传 SHA1 + 目标目录；缺失 size/file_name/preid 时，
        会优先从 p115_filesystem_cache 按 sha1 / pick_code / fid 反查补齐。
        """
        payload = dict(payload or {})
        payload.update({k: v for k, v in kwargs.items() if v not in (None, '')})

        def _as_dict(value):
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                text = value.strip()
                if text:
                    try:
                        parsed = json.loads(text)
                        return parsed if isinstance(parsed, dict) else {}
                    except Exception:
                        return {}
            return {}

        rapid_meta = _as_dict(payload.get('rapid_meta_json') or payload.get('rapid_meta') or payload.get('meta'))
        source_meta = _as_dict(payload.get('source') or payload.get('source_json'))

        def _first(*values):
            for value in values:
                if value not in (None, '', [], {}):
                    return value
            return None

        def _safe_size(value):
            try:
                if value in (None, '', [], {}):
                    return 0
                if isinstance(value, (int, float)):
                    return int(value)
                text = str(value).strip().replace(',', '')
                if not text:
                    return 0
                if re.fullmatch(r'\d+(?:\.\d+)?', text):
                    return int(float(text))
                upper = text.upper()
                multiplier = 1
                if 'TB' in upper:
                    multiplier = 1024 ** 4
                elif 'GB' in upper:
                    multiplier = 1024 ** 3
                elif 'MB' in upper:
                    multiplier = 1024 ** 2
                elif 'KB' in upper:
                    multiplier = 1024
                match = re.search(r'([0-9]+(?:\.[0-9]+)?)', upper)
                return int(float(match.group(1)) * multiplier) if match else 0
            except Exception:
                return 0

        target_cid = str(_first(
            payload.get('cid'), payload.get('target_cid'), payload.get('target'), payload.get('to_cid'),
            rapid_meta.get('cid'), rapid_meta.get('target_cid'), rapid_meta.get('target'), rapid_meta.get('to_cid'),
        ) or '').strip()

        sha1 = str(_first(
            payload.get('sha1'), payload.get('fileid'), payload.get('file_sha1'),
            rapid_meta.get('sha1'), rapid_meta.get('fileid'), rapid_meta.get('file_sha1'),
            source_meta.get('sha1'), source_meta.get('file_sha1'),
        ) or '').strip().upper()

        pick_code = str(_first(
            payload.get('pick_code'), payload.get('pickcode'), payload.get('pc'),
            rapid_meta.get('pick_code'), rapid_meta.get('pickcode'), rapid_meta.get('pc'),
            source_meta.get('pick_code'), source_meta.get('pickcode'), source_meta.get('pc'),
        ) or '').strip()

        fid = str(_first(
            payload.get('fid'), payload.get('file_id'), payload.get('id'),
            rapid_meta.get('fid'), rapid_meta.get('file_id'), rapid_meta.get('id'),
            source_meta.get('fid'), source_meta.get('file_id'), source_meta.get('id'),
        ) or '').strip()

        file_name = str(_first(
            payload.get('file_name'), payload.get('filename'), payload.get('name'),
            rapid_meta.get('file_name'), rapid_meta.get('filename'), rapid_meta.get('name'),
            source_meta.get('file_name'), source_meta.get('filename'), source_meta.get('name'),
        ) or '').strip()

        size = _safe_size(_first(
            payload.get('size'), payload.get('file_size'), payload.get('filesize'), payload.get('size_bytes'),
            rapid_meta.get('size'), rapid_meta.get('file_size'), rapid_meta.get('filesize'), rapid_meta.get('size_bytes'),
            source_meta.get('size'), source_meta.get('file_size'), source_meta.get('filesize'), source_meta.get('size_bytes'),
        ))

        preid = str(_first(
            payload.get('preid'), payload.get('pre_sha1'), payload.get('pre_sha1_128k'),
            rapid_meta.get('preid'), rapid_meta.get('pre_sha1'), rapid_meta.get('pre_sha1_128k'),
            source_meta.get('preid'), source_meta.get('pre_sha1'), source_meta.get('pre_sha1_128k'),
        ) or '').strip().upper()
        sign_key = _first(payload.get('sign_key'), rapid_meta.get('sign_key'), source_meta.get('sign_key'))
        sign_val = _first(
            payload.get('sign_val'), payload.get('sign_check_value'),
            rapid_meta.get('sign_val'), rapid_meta.get('sign_check_value'),
            source_meta.get('sign_val'), source_meta.get('sign_check_value'),
        )

        cache_row = None
        cache_mgr = globals().get('P115CacheManager')
        if cache_mgr:
            try:
                if sha1 and hasattr(cache_mgr, 'get_file_cache_by_sha1'):
                    cache_row = cache_mgr.get_file_cache_by_sha1(sha1)
                if not cache_row and pick_code and hasattr(cache_mgr, 'get_file_cache_by_pickcode'):
                    cache_row = cache_mgr.get_file_cache_by_pickcode(pick_code)
                if not cache_row and fid and hasattr(cache_mgr, 'get_file_cache_by_id'):
                    cache_row = cache_mgr.get_file_cache_by_id(fid)
            except Exception as e:
                logger.debug(f"  ➜ [共享秒传] 查询 p115_filesystem_cache 失败: {e}")

        if cache_row:
            try:
                if not fid:
                    fid = str(cache_row.get('id') or cache_row.get('fid') or '').strip()
                if not pick_code:
                    pick_code = str(cache_row.get('pick_code') or cache_row.get('pc') or '').strip()
                if not sha1:
                    sha1 = str(cache_row.get('sha1') or '').strip().upper()
                if not file_name:
                    file_name = str(cache_row.get('name') or cache_row.get('file_name') or '').strip()
                if size <= 0:
                    size = _safe_size(cache_row.get('size') or cache_row.get('file_size') or cache_row.get('size_bytes'))
                if not preid:
                    preid = str(cache_row.get('preid') or '').strip().upper()
            except Exception:
                pass

        # 缓存仍缺 size 时，只要能定位 fid，就实时查 115 详情补齐。
        if (size <= 0 or not file_name or not pick_code) and not fid and pick_code:
            if cache_mgr and hasattr(cache_mgr, 'get_fid_by_pickcode'):
                try:
                    fid = str(cache_mgr.get_fid_by_pickcode(pick_code) or '').strip()
                except Exception:
                    fid = ''

        if (size <= 0 or not file_name or not pick_code or not sha1) and fid:
            try:
                info_res = self.fs_get_info(fid)
                data = info_res.get('data') if isinstance(info_res, dict) else {}
                if isinstance(data, list):
                    data = data[0] if data else {}
                if isinstance(data, dict):
                    if not sha1:
                        sha1 = str(data.get('sha1') or data.get('sha') or data.get('file_sha1') or '').strip().upper()
                    if not pick_code:
                        pick_code = str(data.get('pc') or data.get('pick_code') or data.get('pickcode') or '').strip()
                    if not file_name:
                        file_name = str(data.get('fn') or data.get('n') or data.get('file_name') or data.get('name') or '').strip()
                    if size <= 0:
                        size = _safe_size(data.get('size_byte') or data.get('fs') or data.get('size') or data.get('file_size'))

                    parent_id = data.get('parent_id') or data.get('pid') or data.get('cid')
                    if cache_mgr and fid and parent_id and file_name and hasattr(cache_mgr, 'save_file_cache'):
                        try:
                            cache_mgr.save_file_cache(
                                fid=fid,
                                parent_id=parent_id,
                                name=file_name,
                                sha1=sha1 or None,
                                pick_code=pick_code or None,
                                local_path=None,
                                size=size or 0,
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.debug(f"  ➜ [共享秒传] 实时查询 115 文件详情失败 fid={fid}: {e}")

        if not target_cid:
            return {'state': False, 'error_msg': '缺少目标目录 cid，无法秒传'}
        if not file_name:
            file_name = f'{sha1}.mkv' if sha1 else 'rapid-file.mkv'
        if not re.fullmatch(r'[A-F0-9]{40}', sha1 or ''):
            return {'state': False, 'error_msg': '缺少合法 SHA1，无法秒传'}
        if size <= 0:
            return {
                'state': False,
                'error_msg': '缺少文件大小，无法秒传：source 未携带 size，且本地 p115_filesystem_cache / 115 文件详情均未能补齐',
                'debug': {
                    'sha1': sha1,
                    'fid': fid,
                    'pick_code': pick_code,
                    'file_name': file_name,
                    'has_cache_row': bool(cache_row),
                }
            }

        if not re.fullmatch(r'[A-F0-9]{40}', preid or '') and pick_code:
            try:
                preid = P115CacheManager.ensure_file_preid({
                    'fid': fid, 'pick_code': pick_code, 'sha1': sha1, 'file_name': file_name, 'size': size,
                }) or preid
            except Exception as e:
                logger.debug(f"  ➜ [共享秒传] 秒传前补齐 preid 失败: sha1={sha1[:12]}..., err={e}")

        logger.info(
            f"  ➜ [共享秒传] 准备秒传到 CID={target_cid}: "
            f"{file_name} | sha1={sha1[:12]}... | preid={(preid[:12] + '...') if preid else '-'} | size={size}"
        )

        # 115 upload/init 要求 preid 字段；无 preid 时用 SHA1 兜底尝试。
        # 如果 115 要求二次校验，会返回 status=7，此时无法凭空计算 sign_val。
        init_res = self.fs_upload_init(file_name, size, target_cid, sha1, preid or sha1, sign_key=sign_key, sign_val=sign_val)
        if not isinstance(init_res, dict):
            return {'state': False, 'error_msg': str(init_res)}

        data = init_res.get('data') if isinstance(init_res.get('data'), dict) else {}
        status = str(data.get('status') if data.get('status') is not None else init_res.get('status') if init_res.get('status') is not None else '')

        if status in ('2', '1') and (init_res.get('state') or status == '2'):
            if status == '2':
                out = dict(init_res)
                out['state'] = True
                out['success'] = True
                out['message'] = out.get('message') or '115 秒传成功'
                out.setdefault('rapid_upload', True)
                out.setdefault('sha1', sha1)
                out.setdefault('file_name', file_name)
                out.setdefault('target_cid', target_cid)
                out.setdefault('size', size)
                return out
            return {
                'state': False,
                'error_msg': '115 返回需要普通上传(status=1)，Rapid v2 不上传明文文件',
                'response': init_res,
            }

        if status == '7':
            sign_key_text = str(data.get('sign_key') or init_res.get('sign_key') or '')
            sign_check_text = str(data.get('sign_check') or init_res.get('sign_check') or '')
            logger.warning(
                f"  ➜ [共享秒传] OpenAPI 返回 status=7，需要 holder 二次校验；"
                f"交给中心调度签名客户端："
                f"sha1={sha1[:12]}..., preid={(preid or sha1)[:12]}..., "
                f"pc={(pick_code or '-')[:8]}..., sign_check={sign_check_text or '-'}, "
                f"sign_key_prefix={sign_key_text[:12]}..., sign_key_len={len(sign_key_text)}"
            )

            stage = 'need_center_holder_sign' if sign_key_text and sign_check_text else 'missing_sign_key_or_check'
            message = (
                '115 要求二次校验(status=7)，等待中心调度 holder 签名'
                if stage == 'need_center_holder_sign'
                else '115 要求二次校验(status=7)，但返回缺少 sign_key/sign_check，无法调度 holder 签名'
            )
            return {
                'state': False,
                'error_msg': message,
                'response': init_res,
                '_rapid_sign_closed_loop': False,
                '_rapid_sign_backend': 'openapi',
                '_rapid_sign_stage': stage,
                '_rapid_sign_required': True,
                '_rapid_sign_key': sign_key_text,
                '_rapid_sign_check': sign_check_text,
                '_rapid_sign_sha1': sha1,
                '_rapid_sign_size': size,
                '_rapid_sign_file_name': file_name,
            }

        return init_res

    def fs_rapid_upload(self, target_cid, sha1, size, file_name, preid=None, **kwargs):
        payload = {'cid': target_cid, 'sha1': sha1, 'size': size, 'file_name': file_name}
        if preid:
            payload['preid'] = preid
        payload.update(kwargs)
        return self.rapid_upload(payload)

    def upload_file_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
        return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

    def fs_upload_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
        return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

    def upload_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
        return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

    def add_file_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
        return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

    def rapid_save(self, target_cid, sha1, size, file_name, **kwargs):
        return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

    def fs_upload_get_token(self):
        """获取上传凭证"""
        url = f"{self.base_url}/open/upload/get_token"
        return self._do_request("GET", url)

    def upload_file_stream(self, file_stream, file_name, target_cid):
        """
        完整的文件上传流程 (支持秒传、二次认证、OSS直传带签名与网络容错)
        """
        import urllib.parse 
        import json # ★ 确保引入 json
        
        file_data = file_stream.read()
        file_size = len(file_data)
        
        sha1_obj = hashlib.sha1()
        sha1_obj.update(file_data)
        file_sha1 = sha1_obj.hexdigest().upper()
        
        pre_sha1_obj = hashlib.sha1()
        pre_sha1_obj.update(file_data[:131072]) 
        preid = pre_sha1_obj.hexdigest().upper()
        
        init_res = self.fs_upload_init(file_name, file_size, target_cid, file_sha1, preid)
        
        if init_res.get('state') and init_res.get('data', {}).get('status') == 7:
            sign_key = init_res['data']['sign_key']
            sign_check = init_res['data']['sign_check']
            start, end = map(int, sign_check.split('-'))
            chunk = file_data[start:end+1]
            
            chunk_sha1 = hashlib.sha1()
            chunk_sha1.update(chunk)
            sign_val = chunk_sha1.hexdigest().upper()
            
            time.sleep(0.5) 
            init_res = self.fs_upload_init(file_name, file_size, target_cid, file_sha1, preid, sign_key, sign_val)
            
        if not init_res.get('state'):
            raise Exception(f"上传初始化失败: {init_res.get('message')}")
            
        status = init_res['data'].get('status')
        
        if status == 2:
            return init_res['data']
            
        if status == 1:
            time.sleep(0.5) 
            token_res = self.fs_upload_get_token()
            if not token_res.get('state'):
                raise Exception("获取上传凭证失败")
                
            t_data = token_res['data']
            
            raw_endpoint = t_data['endpoint'].replace('http://', '').replace('https://', '')
            clean_endpoint = raw_endpoint.replace('-internal', '')
            
            bucket = init_res['data']['bucket']
            object_key = init_res['data']['object'].lstrip('/')
            callback_data = init_res['data'].get('callback', {})
            
            encoded_object_key = urllib.parse.quote(object_key, safe='/')
            
            if 'aliyuncs.com' in clean_endpoint:
                upload_url = f"https://{bucket}.{clean_endpoint}/{encoded_object_key}"
            else:
                upload_url = f"https://{clean_endpoint}/{encoded_object_key}"
            
            date_gmt = formatdate(None, usegmt=True)
            content_type = "application/octet-stream"
            
            headers = {
                "Date": date_gmt,
                "Content-Type": content_type,
                "x-oss-security-token": t_data['SecurityToken'],
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            
            # ==========================================
            # ★ 核心修复：将 callback 转换为 Base64 编码
            # ==========================================
            def _encode_cb(val):
                if isinstance(val, dict):
                    val = json.dumps(val, separators=(',', ':'))
                return base64.b64encode(val.encode('utf-8') if isinstance(val, str) else val).decode('utf-8')

            if 'callback' in callback_data:
                headers["x-oss-callback"] = _encode_cb(callback_data['callback'])
            if 'callback_var' in callback_data:
                headers["x-oss-callback-var"] = _encode_cb(callback_data['callback_var'])
            
            # 计算签名
            oss_headers = {k.lower(): v for k, v in headers.items() if k.lower().startswith('x-oss-')}
            canonicalized_oss_headers = ""
            for k in sorted(oss_headers.keys()):
                canonicalized_oss_headers += f"{k}:{oss_headers[k]}\n"
                
            canonicalized_resource = f"/{bucket}/{object_key}"
            string_to_sign = f"PUT\n\n{content_type}\n{date_gmt}\n{canonicalized_oss_headers}{canonicalized_resource}"
            
            h = hmac.new(t_data['AccessKeySecret'].encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha1)
            signature = base64.b64encode(h.digest()).decode('utf-8')
            
            headers["Authorization"] = f"OSS {t_data['AccessKeyId']}:{signature}"
            
            try:
                oss_res = requests.put(upload_url, data=file_data, headers=headers, timeout=300)
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"  ➜ HTTPS 握手失败，尝试降级为 HTTP 上传... ({e})")
                upload_url_http = upload_url.replace('https://', 'http://')
                oss_res = requests.put(upload_url_http, data=file_data, headers=headers, timeout=300)
            
            try:
                oss_res_data = oss_res.json()
            except Exception:
                raise Exception(f"OSS上传失败，返回非JSON数据: {oss_res.text}")
                
            if oss_res_data.get('state') or oss_res_data.get('code') == 200:
                # 115 的 callback 返回结构可能略有不同，只要有 state=True 或 code=200 就算成功
                return oss_res_data.get('data', oss_res_data)
            else:
                raise Exception(f"OSS上传失败: {oss_res_data}")
                
        raise Exception(f"未知的上传状态: {status}")


def _p115_as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _p115_normalize_rename_pairs(rename_pairs):
    """统一批量重命名参数，输出 [(fid, new_name), ...]。"""
    pairs = []
    for item in _p115_as_list(rename_pairs):
        fid = None
        new_name = None

        if isinstance(item, dict):
            fid = item.get('fid') or item.get('file_id') or item.get('id')
            new_name = item.get('new_name') or item.get('file_name') or item.get('name')
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            fid, new_name = item[0], item[1]

        fid = str(fid).strip() if fid is not None else ''
        new_name = str(new_name).strip() if new_name is not None else ''
        if fid and new_name:
            pairs.append((fid, new_name))

    return pairs


def _p115_success(resp):
    """兼容 OpenAPI / Cookie(webapi/appapi) 的成功判断。"""
    if not isinstance(resp, dict):
        return False
    state = resp.get('state')
    if state is True or state == 1 or state == '1' or str(state).lower() == 'true':
        return True
    # 个别 Cookie 接口只返回 errno/code
    if resp.get('errno') in (0, '0') and not resp.get('error'):
        return True
    if resp.get('code') in (0, 200, '0', '200') and not (resp.get('error') or resp.get('error_msg') or resp.get('message')):
        return True
    return False


def _p115_error_text(resp):
    if isinstance(resp, dict):
        return str(resp.get('error_msg') or resp.get('error') or resp.get('message') or resp.get('msg') or resp)
    return str(resp)


def _p115_is_severe_failure(resp_or_exc):
    """405 / HTML / 非 JSON / 登录失效这类情况，允许自动切换另一套接口。"""
    text = _p115_error_text(resp_or_exc)
    lowered = text.lower()
    return any(k in lowered for k in [
        '405', 'method not allowed', '<html', '<!doctype', 'waf',
        'expecting value', 'jsondecode', 'login', '重新登录', '登录超时', '登陆超时'
    ])


def _p115_truthy_dir_flag(value):
    """115 各套接口里目录标记比较散，这里统一判断。"""
    if value is True:
        return True
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {'1', 'true', 'yes', 'y', 'folder', 'dir', 'directory'}


def _p115_normalize_item(item):
    """把 Cookie(webapi/appapi) / OpenAPI 的文件字段统一成 ETK 内部习惯字段。

    重点兼容 Cookie /files 的目录项：
    - 文件通常是 fid = 文件ID，cid = 父目录ID；
    - 目录经常是 cid = 目录自身ID，pid = 父目录ID，未必有 fid；
    如果无脑把 cid 当 parent_id，就会导致前端目录 id 为空，浏览目录点不开。
    """
    if not isinstance(item, dict):
        return item
    item = dict(item)

    # 原始字段先取出来，后面根据“是否目录”再决定 cid 到底是自身ID还是父ID。
    raw_fid = item.get('fid') or item.get('file_id') or item.get('id')
    raw_cid = item.get('cid')
    raw_pid = item.get('pid') or item.get('parent_id') or item.get('parentId')

    name = (
        item.get('fn') or item.get('n') or item.get('file_name') or
        item.get('name') or item.get('title')
    )
    pick_code = item.get('pc') or item.get('pick_code') or item.get('pickcode')
    sha1 = item.get('sha1') or item.get('sha') or item.get('file_sha1')
    size = item.get('fs') or item.get('size') or item.get('file_size') or item.get('s')

    # fc: ETK 内部约定 0=目录，1=文件。
    fc = item.get('fc')
    if fc is None:
        fc = item.get('file_category')
    if fc is None:
        fc = item.get('type')

    icon = item.get('ico') or item.get('icon') or item.get('class')
    folder_flag = (
        _p115_truthy_dir_flag(item.get('is_dir')) or
        _p115_truthy_dir_flag(item.get('is_directory')) or
        _p115_truthy_dir_flag(item.get('is_folder')) or
        _p115_truthy_dir_flag(icon)
    )

    # Cookie /files 的面包屑、目录项常见只有 cid/name/pid，没有 fid/pc/sha/size。
    cid_looks_like_folder_id = raw_cid is not None and raw_fid is None and not any([pick_code, sha1, size])

    if fc is None:
        if folder_flag or cid_looks_like_folder_id:
            fc = '0'
        elif pick_code or sha1 or size or raw_fid is not None:
            fc = '1'

    is_folder = str(fc) == '0'

    # 关键修复：目录项没有 fid 时，用 cid 作为目录自身 ID；文件项的 cid 仍然保留为父目录 ID。
    fid = raw_fid
    if fid is None and is_folder:
        fid = raw_cid

    parent_id = raw_pid
    if parent_id is None and not is_folder:
        parent_id = raw_cid

    if fid is not None:
        item.setdefault('fid', str(fid))
        item.setdefault('file_id', str(fid))
    if name is not None:
        item.setdefault('fn', name)
        item.setdefault('n', name)
        item.setdefault('file_name', name)
        item.setdefault('name', name)
    if parent_id is not None:
        item.setdefault('pid', str(parent_id))
        item.setdefault('parent_id', str(parent_id))
    if pick_code is not None:
        item.setdefault('pc', pick_code)
        item.setdefault('pick_code', pick_code)
    if sha1 is not None:
        item.setdefault('sha1', sha1)
        item.setdefault('sha', sha1)
    if size is not None:
        item.setdefault('fs', size)
        item.setdefault('size', size)
    if fc is not None:
        item['fc'] = str(fc)
        item.setdefault('file_category', str(fc))

    return item

def _p115_normalize_list_response(resp):
    """统一目录列表/搜索列表响应，保证 resp['data'] 是 list。"""
    if not isinstance(resp, dict):
        return {'state': False, 'error_msg': str(resp)}
    resp = dict(resp)

    data = resp.get('data')
    if isinstance(data, dict):
        # OpenAPI / webapi 不同接口可能把列表包在这些字段里
        for key in ('list', 'items', 'files', 'data'):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    elif data is None:
        for key in ('list', 'items', 'files'):
            if isinstance(resp.get(key), list):
                data = resp[key]
                break

    if data is None:
        data = []
    if isinstance(data, list):
        data = [_p115_normalize_item(i) for i in data]

    # Cookie /files 的 path/paths/breadcrumb 字段也顺手归一化，方便前端显示当前目录名。
    path_data = resp.get('path') or resp.get('paths') or resp.get('breadcrumb')
    if isinstance(path_data, list):
        resp['path'] = [_p115_normalize_item(i) for i in path_data]

    resp['state'] = _p115_success(resp)
    resp['data'] = data
    return resp


def _p115_normalize_info_response(resp):
    """统一文件/目录详情响应，保证 resp['data'] 是单个 dict。"""
    if not isinstance(resp, dict):
        return {'state': False, 'error_msg': str(resp)}
    resp = dict(resp)
    data = resp.get('data')

    if isinstance(data, list):
        data = data[0] if data else {}
    elif isinstance(data, dict):
        for key in ('file_info', 'info'):
            if isinstance(data.get(key), dict):
                data = data[key]
                break
            if isinstance(data.get(key), list):
                data = data[key][0] if data[key] else {}
                break
    elif data is None:
        # webapi /files/file 有些版本直接把 info 放顶层或放 file_info
        if isinstance(resp.get('file_info'), list):
            data = resp['file_info'][0] if resp['file_info'] else {}
        elif isinstance(resp.get('file_info'), dict):
            data = resp['file_info']
        else:
            data = {k: v for k, v in resp.items() if k not in ('state', 'errno', 'error', 'error_msg', 'message', 'msg')}

    data = _p115_normalize_item(data or {})
    resp['state'] = _p115_success(resp)
    resp['data'] = data
    return resp


def _p115_normalize_mkdir_response(resp):
    """统一新建目录响应，补齐 cid。"""
    resp = _p115_normalize_info_response(resp)
    data = resp.get('data') if isinstance(resp, dict) else {}
    if isinstance(data, dict):
        cid = data.get('cid') or data.get('file_id') or data.get('fid') or data.get('id')
        if cid is not None:
            resp['cid'] = str(cid)
    return resp


def _p115_normalize_common_response(resp):
    """移动/删除/重命名等只关心 state 的接口。"""
    if not isinstance(resp, dict):
        return {'state': False, 'error_msg': str(resp)}
    resp = dict(resp)
    resp['state'] = _p115_success(resp)
    return resp


def get_115_api_priority(default='openapi'):
    """
    115 API 优先级。
    当前可选值：openapi / cookie。
    """
    cfg = config_manager.APP_CONFIG or {}
    val = cfg.get(constants.CONFIG_OPTION_115_API_PRIORITY, default)
    val = str(val or default).strip().lower()
    return 'cookie' if val == 'cookie' else 'openapi'

def is_p115_mediainfo_assisted_recognition_enabled():
    """
    媒体信息辅助识别开关。
    必须同时开启：
    1. 媒体信息格式化
    2. 媒体信息辅助识别
    才允许使用 raw_ffprobe_json._etk 参与识别。
    """
    cfg = config_manager.APP_CONFIG or {}
    generate_enabled = bool(cfg.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False))
    assisted_enabled = bool(cfg.get(constants.CONFIG_OPTION_115_MEDIAINFO_ASSISTED_RECOGNITION, False))
    return generate_enabled and assisted_enabled


# ======================================================================
# ★★★ 115 Cookie 客户端 (播放 + Cookie 可承载的文件管理操作) ★★★
# ======================================================================
class P115CookieClient:
    """使用 Cookie 执行播放、目录列表、移动、重命名、删除等 webapi 操作"""
    def __init__(self, cookie_str, app_type='web'):
        if not cookie_str:
            raise ValueError("Cookie 不能为空")
        self.cookie_str = cookie_str.strip()
        self.app_type = app_type
        self.user_agent = get_115_ua(app_type) 
        self.webapi = None
        
        # ★ 核心修复：为兜底请求引入 Session
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=1)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        if P115Client:
            try:
                self.webapi = P115Client(self.cookie_str, app=self.app_type)
                try:
                    self.webapi.headers["user-agent"] = self.user_agent
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"  ➜ Cookie 客户端初始化失败: {e}")
                raise

    def download_url(self, pick_code, user_agent=None):
        """获取直链 (仅 Cookie 可用)"""
        if self.webapi:
            url_obj = self.webapi.download_url(pick_code, user_agent=user_agent)
            if url_obj: return str(url_obj)
        return None


    def rapid_upload(self, payload=None, **kwargs):
        """Cookie 侧上传初始化探测入口。

        这条链路直接调用 p115client.P115Client.upload_init，对应
        https://uplb.115.com/4.0/initupload.php。

        注意：Cookie 版 initupload 和 OpenAPI /open/upload/init 不是同一套字段：
        - Cookie: filename / filesize / fileid / target / userid / userkey / sign_key / sign_val
        - OpenAPI: file_name / file_size / fileid / preid / target

        这里只做“是否可直接秒传/复用”的探测；如果返回普通上传(status=1)，立刻停止，
        绝不继续上传明文文件。
        """
        payload = dict(payload or {})
        payload.update({k: v for k, v in kwargs.items() if v not in (None, '')})

        def _as_dict(value):
            if isinstance(value, dict):
                return value
            if isinstance(value, str) and value.strip():
                try:
                    parsed = json.loads(value)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return {}

        def _first(*values):
            for value in values:
                if value not in (None, '', [], {}):
                    return value
            return None

        def _safe_size(value):
            try:
                if value in (None, '', [], {}):
                    return 0
                if isinstance(value, (int, float)):
                    return int(value)
                text = str(value).strip().replace(',', '')
                if not text:
                    return 0
                if re.fullmatch(r'\d+(?:\.\d+)?', text):
                    return int(float(text))
                upper = text.upper()
                multiplier = 1
                if 'TB' in upper:
                    multiplier = 1024 ** 4
                elif 'GB' in upper:
                    multiplier = 1024 ** 3
                elif 'MB' in upper:
                    multiplier = 1024 ** 2
                elif 'KB' in upper:
                    multiplier = 1024
                match = re.search(r'([0-9]+(?:\.[0-9]+)?)', upper)
                return int(float(match.group(1)) * multiplier) if match else 0
            except Exception:
                return 0

        def _status_from_cookie_init(resp):
            if not isinstance(resp, dict):
                return '', {}
            data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
            status = resp.get('status') if resp.get('status') is not None else data.get('status')
            return str(status) if status is not None else '', data

        rapid_meta = _as_dict(payload.get('rapid_meta_json') or payload.get('rapid_meta') or payload.get('meta'))
        source_meta = _as_dict(payload.get('source') or payload.get('source_json'))

        target_cid = str(_first(
            payload.get('cid'), payload.get('target_cid'), payload.get('target'), payload.get('to_cid'),
            rapid_meta.get('cid'), rapid_meta.get('target_cid'), rapid_meta.get('target'), rapid_meta.get('to_cid'),
        ) or '').strip()
        sha1 = str(_first(
            payload.get('sha1'), payload.get('fileid'), payload.get('file_sha1'),
            rapid_meta.get('sha1'), rapid_meta.get('fileid'), rapid_meta.get('file_sha1'),
            source_meta.get('sha1'), source_meta.get('file_sha1'),
        ) or '').strip().upper()
        pick_code = str(_first(
            payload.get('pick_code'), payload.get('pickcode'), payload.get('pc'),
            rapid_meta.get('pick_code'), rapid_meta.get('pickcode'), rapid_meta.get('pc'),
            source_meta.get('pick_code'), source_meta.get('pickcode'), source_meta.get('pc'),
        ) or '').strip()
        file_name = str(_first(
            payload.get('file_name'), payload.get('filename'), payload.get('name'),
            rapid_meta.get('file_name'), rapid_meta.get('filename'), rapid_meta.get('name'),
            source_meta.get('file_name'), source_meta.get('filename'), source_meta.get('name'),
        ) or '').strip()
        size = _safe_size(_first(
            payload.get('size'), payload.get('file_size'), payload.get('filesize'), payload.get('size_bytes'),
            rapid_meta.get('size'), rapid_meta.get('file_size'), rapid_meta.get('filesize'), rapid_meta.get('size_bytes'),
            source_meta.get('size'), source_meta.get('file_size'), source_meta.get('filesize'), source_meta.get('size_bytes'),
        ))
        sign_key = _first(payload.get('sign_key'), rapid_meta.get('sign_key'), source_meta.get('sign_key'))
        sign_val = _first(
            payload.get('sign_val'), payload.get('sign_check_value'),
            rapid_meta.get('sign_val'), rapid_meta.get('sign_check_value'),
            source_meta.get('sign_val'), source_meta.get('sign_check_value'),
        )

        cache_mgr = globals().get('P115CacheManager')
        if cache_mgr and (not pick_code or not file_name or size <= 0):
            try:
                row = None
                if sha1 and hasattr(cache_mgr, 'get_file_cache_by_sha1'):
                    row = cache_mgr.get_file_cache_by_sha1(sha1)
                if not row and pick_code and hasattr(cache_mgr, 'get_file_cache_by_pickcode'):
                    row = cache_mgr.get_file_cache_by_pickcode(pick_code)
                if row:
                    row = dict(row)
                    pick_code = pick_code or str(row.get('pick_code') or '').strip()
                    file_name = file_name or str(row.get('name') or '').strip()
                    if size <= 0:
                        size = _safe_size(row.get('size'))
            except Exception as e:
                logger.debug(f"  ➜ [Cookie秒传] 查询 p115_filesystem_cache 失败: {e}")

        if not target_cid:
            return {'state': False, 'error_msg': 'Cookie 秒传缺少目标目录 cid', '_rapid_upload_backend': 'cookie'}
        if not re.fullmatch(r'[A-F0-9]{40}', sha1 or ''):
            return {'state': False, 'error_msg': 'Cookie 秒传缺少合法 SHA1', '_rapid_upload_backend': 'cookie'}
        if size <= 0:
            return {'state': False, 'error_msg': 'Cookie 秒传缺少文件大小', '_rapid_upload_backend': 'cookie'}
        if not file_name:
            file_name = f'{sha1}.mkv'
        if not self.webapi:
            return {'state': False, 'error_msg': 'Cookie 客户端未初始化 p115client，无法调用 Cookie 上传初始化', '_rapid_upload_backend': 'cookie'}
        if not hasattr(self.webapi, 'upload_init'):
            return {
                'state': False,
                'error_msg': '当前 p115client 不支持 P115Client.upload_init，无法测试 Cookie 上传初始化',
                '_rapid_upload_backend': 'cookie',
                '_rapid_cookie_unsupported': True,
            }

        target = target_cid if str(target_cid).startswith('U_') else f'U_1_{target_cid}'
        init_payload = {
            'filename': file_name,
            'filesize': int(size),
            'fileid': sha1,
            'target': target,
            'topupload': 'true',
        }
        if sign_key and sign_val:
            init_payload['sign_key'] = str(sign_key)
            init_payload['sign_val'] = str(sign_val).upper()

        logger.info(
            f"  ➜ [Cookie秒传] 初始化上传: {file_name} | "
        )

        try:
            resp = self.webapi.upload_init(init_payload)
        except Exception as e:
            return {
                'state': False,
                'error_msg': f'Cookie initupload 异常: {e}',
                '_rapid_upload_backend': 'cookie',
                '_rapid_cookie_payload': {'target': target, 'sha1': sha1[:12] + '...', 'size': size},
            }

        if not isinstance(resp, dict):
            return {
                'state': False,
                'error_msg': f'Cookie initupload 返回非 dict: {type(resp).__name__}',
                '_rapid_upload_backend': 'cookie',
                'response': str(resp)[:500],
            }

        out = dict(resp)
        out['_rapid_upload_backend'] = 'cookie'
        out.setdefault('sha1', sha1)
        out.setdefault('file_name', file_name)
        out.setdefault('target_cid', target_cid)
        out.setdefault('size', size)
        status, data = _status_from_cookie_init(out)
        reuse = out.get('reuse') is True or str(out.get('reuse')).lower() == 'true'

        if reuse or status in ('2', 'success', 'done'):
            out['state'] = True
            out['success'] = True
            out.setdefault('message', '115 Cookie initupload 秒传成功')
            out.setdefault('rapid_upload', True)
            logger.info(f"  ➜ [Cookie秒传] Cookie initupload 秒传成功: {file_name}")
            return out

        if status == '1':
            out['state'] = False
            out.setdefault('error_msg', 'Cookie initupload 返回普通上传(status=1)，Rapid v2 不上传明文文件')
            out['_rapid_cookie_need_plain_upload'] = True
            return out

        if status == '7':
            sign_key_text = str(out.get('sign_key') or data.get('sign_key') or '')
            sign_check_text = str(out.get('sign_check') or data.get('sign_check') or '')
            logger.warning(
                f"  ➜ [Cookie秒传] Cookie initupload 返回 status=7，需要 holder 二次校验；"
                f"交给中心调度签名客户端："
                f"sha1={sha1[:12]}..., pc={(pick_code or '-')[:8]}..., "
                f"sign_check={sign_check_text or '-'}, sign_key_prefix={sign_key_text[:12]}..., "
                f"sign_key_len={len(sign_key_text)}"
            )

            stage = 'need_center_holder_sign' if sign_key_text and sign_check_text else 'missing_sign_key_or_check'
            out['state'] = False
            out['error_msg'] = (
                'Cookie initupload 要求二次校验(status=7)，等待中心调度 holder 签名'
                if stage == 'need_center_holder_sign'
                else 'Cookie initupload 要求二次校验(status=7)，但返回缺少 sign_key/sign_check，无法调度 holder 签名'
            )
            out['_rapid_sign_closed_loop'] = False
            out['_rapid_sign_backend'] = 'cookie'
            out['_rapid_sign_stage'] = stage
            out['_rapid_sign_required'] = True
            out['_rapid_sign_key'] = sign_key_text
            out['_rapid_sign_check'] = sign_check_text
            out['_rapid_sign_sha1'] = sha1
            out['_rapid_sign_size'] = size
            out['_rapid_sign_file_name'] = file_name
            return out

        out['state'] = False
        out.setdefault('error_msg', f'Cookie initupload 未直接秒传，status={status or "unknown"}')
        return out

    def get_user_info(self):
        """获取用户信息 (仅用于验证)"""
        if self.webapi:
            try:
                # Cookie 模式获取用户信息的方式有限
                return {"state": True, "data": {"user_name": "Cookie用户"}}
            except:
                pass
        return None
    
    def request(self, url, method='GET', **kwargs):
        if self.webapi and hasattr(self.webapi, 'request'):
            return self.webapi.request(url, method=method, **kwargs)
        
        headers = {
            "User-Agent": self.user_agent, 
            "Cookie": self.cookie_str
        }
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']
        kwargs.setdefault('timeout', 30)
        # ★ 核心修复：使用 self.session
        return self.session.request(method, url, headers=headers, **kwargs)

    def _json_result(self, resp):
        if isinstance(resp, dict):
            return resp
        if hasattr(resp, 'json'):
            try:
                return resp.json()
            except Exception as e:
                text = getattr(resp, 'text', '')
                return {'state': False, 'error_msg': f'Cookie 接口返回非 JSON: {e}; {text[:200]}'}
        return {'state': False, 'error_msg': str(resp)}

    def fs_files(self, payload):
        """Cookie/webapi 获取目录列表，返回格式向 OpenAPI 对齐。"""
        params = {'aid': 1, 'show_dir': 1, 'limit': 1000, 'offset': 0, 'record_open_time': 0, 'count_folders': 0}
        if isinstance(payload, dict):
            params.update(payload)
        elif payload is not None:
            params['cid'] = payload
        if self.webapi and hasattr(self.webapi, 'fs_files'):
            try:
                return _p115_normalize_list_response(self.webapi.fs_files(params))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/files"
        r = self.request(url, method='GET', params=params)
        return _p115_normalize_list_response(self._json_result(r))

    def fs_files_app(self, payload):
        return self.fs_files(payload)

    def fs_search(self, payload):
        """Cookie/webapi 搜索文件，返回格式向 OpenAPI 对齐。"""
        params = {'aid': 1, 'cid': 0, 'show_dir': 1, 'limit': 100, 'offset': 0, 'search_value': '.'}
        if isinstance(payload, str):
            params['search_value'] = payload
        elif isinstance(payload, dict):
            params.update(payload)
        if self.webapi and hasattr(self.webapi, 'fs_search'):
            try:
                return _p115_normalize_list_response(self.webapi.fs_search(params))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/files/search"
        r = self.request(url, method='GET', params=params)
        return _p115_normalize_list_response(self._json_result(r))

    def fs_get_info(self, file_id):
        """Cookie/webapi 获取单个文件/目录信息，返回格式向 OpenAPI 对齐。"""
        payload = {'file_id': str(file_id)}
        if self.webapi and hasattr(self.webapi, 'fs_file_skim'):
            try:
                return _p115_normalize_info_response(self.webapi.fs_file_skim(payload))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/files/file"
        r = self.request(url, method='GET', params=payload)
        return _p115_normalize_info_response(self._json_result(r))

    def fs_mkdir(self, name, pid):
        payload = {'cname': str(name), 'pid': str(pid)}
        if self.webapi and hasattr(self.webapi, 'fs_mkdir'):
            try:
                return _p115_normalize_mkdir_response(self.webapi.fs_mkdir(str(name), pid=str(pid)))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/files/add"
        r = self.request(url, method='POST', data=payload)
        return _p115_normalize_mkdir_response(self._json_result(r))

    def fs_move(self, fids, to_cid):
        ids = [str(i) for i in _p115_as_list(fids) if i is not None]
        if self.webapi and hasattr(self.webapi, 'fs_move'):
            try:
                return _p115_normalize_common_response(self.webapi.fs_move(ids, pid=str(to_cid)))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        payload = {'pid': str(to_cid)}
        if len(ids) == 1:
            payload['fid'] = ids[0]
        else:
            payload.update({f'fid[{i}]': fid for i, fid in enumerate(ids)})
        url = "https://webapi.115.com/files/move"
        r = self.request(url, method='POST', data=payload)
        return _p115_normalize_common_response(self._json_result(r))

    def fs_copy(self, fids, to_cid):
        ids = [str(i) for i in _p115_as_list(fids) if i is not None]
        if self.webapi and hasattr(self.webapi, 'fs_copy'):
            try:
                return _p115_normalize_common_response(self.webapi.fs_copy(ids, pid=str(to_cid)))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        payload = {'pid': str(to_cid)}
        if len(ids) == 1:
            payload['fid'] = ids[0]
        else:
            payload.update({f'fid[{i}]': fid for i, fid in enumerate(ids)})
        url = "https://webapi.115.com/files/copy"
        r = self.request(url, method='POST', data=payload)
        return _p115_normalize_common_response(self._json_result(r))

    def fs_rename(self, fid_name_tuple):
        fid, new_name = fid_name_tuple
        if self.webapi and hasattr(self.webapi, 'fs_rename'):
            try:
                return _p115_normalize_common_response(self.webapi.fs_rename((str(fid), str(new_name))))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/files/batch_rename"
        r = self.request(url, method='POST', data={f'files_new_name[{fid}]': str(new_name)})
        return _p115_normalize_common_response(self._json_result(r))

    def fs_rename_batch(self, fid_name_tuples):
        """Cookie/webapi 批量重命名。

        115 的 /files/batch_rename 支持一次提交多个 files_new_name[fid]，
        这里只在 Cookie 优先策略下由上层调用；OpenAPI 仍保持逐条 update。
        """
        pairs = _p115_normalize_rename_pairs(fid_name_tuples)
        if not pairs:
            return {"state": True, "data": {"count": 0}, "_batch_count": 0}

        payload = {f'files_new_name[{fid}]': new_name for fid, new_name in pairs}
        url = "https://webapi.115.com/files/batch_rename"
        r = self.request(url, method='POST', data=payload)
        resp = _p115_normalize_common_response(self._json_result(r))
        if resp.get('state'):
            resp['_batch_count'] = len(pairs)
        return resp

    def fs_delete(self, fids):
        ids = [str(i) for i in _p115_as_list(fids) if i is not None]
        if self.webapi and hasattr(self.webapi, 'fs_delete'):
            try:
                return _p115_normalize_common_response(self.webapi.fs_delete(ids))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        payload = {'ignore_warn': 1}
        if len(ids) == 1:
            payload['fid'] = ids[0]
        else:
            payload.update({f'fid[{i}]': fid for i, fid in enumerate(ids)})
        url = "https://webapi.115.com/rb/delete"
        r = self.request(url, method='POST', data=payload)
        return _p115_normalize_common_response(self._json_result(r))

    def offline_add_urls(self, payload):
        if self.webapi and hasattr(self.webapi, 'offline_add_urls'):
            return self.webapi.offline_add_urls(payload)
        
        # 兜底：手动调用离线接口
        url = "https://115.com/web/lixian/?ct=lixian&ac=add_task_urls"
        r = self.request(url, method='POST', data=payload)
        return self._json_result(r)

    def share_import(self, share_code, receive_code, cid):
        # 放弃调用第三方库的 share_receive，直接使用最稳妥的官方原生 API
        # 官方接口完美支持直接传入 cid 保存到指定目录
        url = "https://webapi.115.com/share/receive"
        payload = {'share_code': share_code, 'receive_code': receive_code, 'cid': cid}
        r = self.request(url, method='POST', data=payload)
        return r.json() if hasattr(r, 'json') else r

    def history_receive_list(self, offset=0, limit=100):
        """获取 115 最近接收记录。

        该接口仅用于清理“最近接收”历史展示，不保证能解除 115 服务器侧
        对同一分享的“你已经转存过该文件”限制。
        """
        url = "https://webapi.115.com/history/receive_list"
        payload = {
            'offset': max(0, int(offset or 0)),
            'limit': max(1, min(int(limit or 100), 500)),
        }
        r = self.request(url, method='GET', params=payload)
        return self._json_result(r)

    def history_delete(self, ids):
        """删除 115 历史记录，ids 可为单个 id 或列表。"""
        ids_list = [str(i).strip() for i in _p115_as_list(ids) if str(i or '').strip()]
        if not ids_list:
            return {'state': False, 'error_msg': '缺少历史记录 id'}
        url = "https://webapi.115.com/history/delete"
        r = self.request(url, method='POST', data={'id': ','.join(ids_list)})
        return _p115_normalize_common_response(self._json_result(r))

    def share_send(self, file_ids, **kwargs):
        """创建当前账号自己的 115 分享。

        说明：这是 Cookie/webapi 能力，用于“库内资源 -> 预分享资产”。
        /share/send 只负责创建分享；有效期等配置需要再调用 /share/updateshare。
        """
        ids = [str(i).strip() for i in _p115_as_list(file_ids) if str(i or '').strip()]
        if not ids:
            return {"state": False, "error_msg": "缺少 file_id，无法创建分享"}

        payload = {
            "file_ids": ",".join(ids),
            "ignore_warn": 1,
            "is_asc": 0,
            "order": "user_ptime",
        }
        # 允许调用方覆盖 share/send 支持的额外参数，但不让空值污染 payload。
        for key, val in kwargs.items():
            if val is not None and val != "":
                payload[key] = val

        if self.webapi and hasattr(self.webapi, 'share_send'):
            try:
                return _p115_normalize_common_response(self.webapi.share_send(payload))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise

        url = "https://webapi.115.com/share/send"
        r = self.request(url, method='POST', data=payload)
        return _p115_normalize_common_response(self._json_result(r))

    def share_update(self, share_code, share_duration=-1, receive_code=None, auto_fill_recvcode=0, receive_user_limit="", action=None):
        """更新 115 分享设置。

        - share_duration=-1 表示长期有效。
        - action=cancel/delete 用于取消/删除分享。
        """
        code = str(share_code or '').strip()
        if not code:
            return {"state": False, "error_msg": "缺少 share_code，无法更新分享设置"}

        payload = {
            "share_code": code,
            "auto_fill_recvcode": auto_fill_recvcode,
            "receive_user_limit": receive_user_limit or "",
        }
        if action:
            payload["action"] = str(action).strip()
        else:
            payload["share_duration"] = share_duration
        if receive_code:
            payload["receive_code"] = str(receive_code).strip()
            payload["is_custom_code"] = 1

        if self.webapi and hasattr(self.webapi, 'share_update'):
            try:
                return _p115_normalize_common_response(self.webapi.share_update(payload))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise

        url = "https://webapi.115.com/share/updateshare"
        logger.info(f"  ➜ [115分享] 更新分享设置: share_code={code}, action={action or 'update'}, duration={share_duration}")
        r = self.request(url, method='POST', data=payload)
        resp = _p115_normalize_common_response(self._json_result(r))
        if not resp.get('state'):
            logger.warning(f"  ➜ [115分享] 更新分享设置失败: {resp}")
        return resp


    def share_create(self, file_ids, share_duration=-1, receive_code=None):
        """兼容新共享资源代码：创建分享后立即更新为长期有效。"""
        resp = self.share_send(file_ids)
        if not _p115_success(resp):
            return _p115_normalize_common_response(resp)
        data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
        share_code = data.get('share_code') or resp.get('share_code')
        if share_code:
            upd = self.share_update(share_code, share_duration=share_duration, receive_code=receive_code)
            resp.setdefault('data', data)
            resp['data']['share_duration'] = share_duration
            resp['data']['update_response'] = upd
            if receive_code:
                resp['data']['receive_code'] = receive_code
        resp['state'] = _p115_success(resp)
        return resp

    def share_update_settings(self, share_code, share_duration=-1, receive_code=None, auto_fill_recvcode=0, receive_user_limit=''):
        """兼容新共享资源代码的命名，底层仍走旧版 share_update。"""
        return self.share_update(
            share_code,
            share_duration=share_duration,
            receive_code=receive_code,
            auto_fill_recvcode=auto_fill_recvcode,
            receive_user_limit=receive_user_limit,
        )
    def share_info(self, share_code, receive_code=None, cid=0, limit=100, offset=0):
        """查询分享信息。

        - 本账号自己的分享：走 /share/shareinfo，用于审核/取消状态同步；
        - 消费别人分享快照：传 receive_code/cid/limit 时走 /share/snap，兼容旧调用。
        """
        code = str(share_code or '').strip()
        if not code:
            return {"state": False, "error_msg": "缺少 share_code，无法查询分享信息"}

        # 兼容旧版检查逻辑：带 receive_code/cid/limit 的调用读取分享快照。
        if receive_code is not None or cid not in (None, 0, '0') or int(limit or 0) != 100 or int(offset or 0) != 0:
            url = "https://webapi.115.com/share/snap"
            params = {
                "share_code": code,
                "receive_code": str(receive_code or ''),
                "cid": str(cid or 0),
                "limit": int(limit or 100),
                "offset": int(offset or 0),
            }
            r = self.request(url, method='GET', params=params)
            resp = self._json_result(r)
            resp['state'] = _p115_success(resp)
            return resp

        if self.webapi and hasattr(self.webapi, 'share_info'):
            try:
                return _p115_normalize_common_response(self.webapi.share_info({"share_code": code}))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/share/shareinfo"
        r = self.request(url, method='GET', params={"share_code": code})
        return _p115_normalize_common_response(self._json_result(r))

    def share_list(self, payload=None):
        """查询当前账号自己的分享列表，用于和 115 实际审核/取消状态对齐。"""
        params = {"limit": 100, "offset": 0, "show_cancel_share": 1, "order": "create_time", "asc": 0}
        if isinstance(payload, dict):
            params.update(payload)
        if self.webapi and hasattr(self.webapi, 'share_list'):
            try:
                return _p115_normalize_common_response(self.webapi.share_list(params))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        url = "https://webapi.115.com/share/slist"
        r = self.request(url, method='GET', params=params)
        return _p115_normalize_common_response(self._json_result(r))

    def share_cancel(self, share_code):
        """取消当前账号自己的分享。"""
        return self.share_update(share_code, action="cancel")

    def share_delete(self, share_code):
        """删除当前账号自己的分享记录；失败时调用方可回退到 cancel。"""
        return self.share_update(share_code, action="delete")

    def life_batch_delete(self, delete_data_list):
        url = "https://life.115.com/api/1.0/web/1.0/life/life_batch_delete"
        # 115 要求 delete_data 是一个 JSON 字符串
        payload = {"delete_data": json.dumps(delete_data_list)}
        r = self.request(url, method='POST', data=payload)
        return r.json() if hasattr(r, 'json') else r
    
    def life_behavior_detail(self, payload=None):
        # ★ 彻底抛弃第三方库，直接用原生 requests，加上严格的 timeout 防止卡死！
        url = "https://webapi.115.com/behavior/detail"
        params = {"limit": 100, "offset": 0}
        if isinstance(payload, dict): 
            params.update(payload)
        
        # 强制加上 timeout=15，如果 15 秒没响应直接报错，绝不卡死线程
        r = self.request(url, method='GET', params=params, timeout=15)
        return r.json() if hasattr(r, 'json') else r


# ======================================================================
# ★★★ 115 服务管理器 (分离管理/播放客户端 + 延迟初始化) ★★★
# ======================================================================
class P115Service:
    """统一管理 OpenAPI 和 Cookie 客户端"""
    _instance = None
    _lock = threading.Lock()
    _rate_limit_lock = threading.Lock() # 专用于 API 流控的锁
    _downurl_lock = threading.Lock() # 直链专用锁
    # 移动接口的绝对互斥锁
    _move_lock = threading.Lock()
    _last_move_time = 0
    
    # 客户端缓存
    _openapi_client = None
    _cookie_client = None
    _token_cache = None
    _cookie_cache = None
    
    _last_request_time = 0
    _last_downurl_time = 0 # 直链专用时间戳

    @classmethod
    def get_openapi_client(cls):
        """获取管理客户端 (OpenAPI) - 启动时初始化"""
        token, _, _, _ = get_115_tokens()
        if not token:
            return None

        with cls._lock:
            # 如果 client 不存在，或者 token 变了，重新初始化
            if cls._openapi_client is None or getattr(cls._openapi_client, 'access_token', None) != token:
                try:
                    cls._openapi_client = P115OpenAPIClient(token)
                    logger.info("  ➜ [115] OpenAPI 客户端已初始化")
                except Exception as e:
                    logger.error(f"  ➜ 115 OpenAPI 客户端初始化失败: {e}")
                    cls._openapi_client = None
            
            return cls._openapi_client

    @classmethod
    def init_cookie_client(cls):
        """初始化 Cookie 客户端 (延迟到播放请求时)"""
        # ★ 接收 app_type
        _, _, cookie, app_type = get_115_tokens() 
        cookie = (cookie or "").strip()
        
        if not cookie:
            return None

        with cls._lock:
            if cls._cookie_client is None or cookie != cls._cookie_cache:
                try:
                    # ★ 将 app_type 传给 CookieClient
                    cls._cookie_client = P115CookieClient(cookie, app_type)
                    cls._cookie_cache = cookie
                    logger.info(f"  ➜ [115] Cookie 客户端已初始化 (App: {app_type})")
                except Exception as e:
                    logger.error(f"  ➜ 115 Cookie 客户端初始化失败: {e}")
                    cls._cookie_client = None
            
            return cls._cookie_client

    @classmethod
    def get_cookie_client(cls):
        """获取播放客户端 (Cookie) - 延迟初始化，失败时重试"""
        # 如果已经初始化过，直接返回
        if cls._cookie_client is not None:
            return cls._cookie_client
        
        # 未初始化，尝试初始化（可能容器重启后首次调用）
        return cls.init_cookie_client()
    
    @classmethod
    def reset_cookie_client(cls):
        """重置 Cookie 客户端 (当检测到失效时调用)"""
        with cls._lock:
            cls._cookie_client = None
            cls._cookie_cache = None
            logger.info("  ➜ [115] Cookie 客户端已重置，下次请求将重新初始化")

    @classmethod
    def get_client(cls):
        """
        获取统一客户端：
        文件管理/整理操作 -> 按 115 API 优先级 Cookie/OpenAPI 自动切换
        清空回收站/上传初始化 -> 强制 OpenAPI
        转存/离线/生活事件 -> 强制 Cookie
        """
        openapi = cls.get_openapi_client()
        cookie = cls.get_cookie_client()
        
        if not openapi and not cookie:
            return None

        class StrictSplitClient:
            def __init__(self, openapi_client, cookie_client):
                self._openapi = openapi_client
                self._cookie = cookie_client

            @property
            def raw_client(self):
                """暴露底层原生 P115Client 供极速遍历使用"""
                if self._cookie and hasattr(self._cookie, 'webapi'):
                    return self._cookie.webapi
                return None

            def _check_openapi(self):
                if not self._openapi:
                    raise Exception("未配置 115 Token (OpenAPI)，无法执行管理操作")

            def _rate_limit(self):
                """底层统一 API 流控拦截器 (修复高并发死锁)"""
                try:
                    interval = float(get_config().get(constants.CONFIG_OPTION_115_INTERVAL, 1.5))
                    if interval < 1.5:
                        interval = 1.5
                except (ValueError, TypeError):
                    interval = 1.5
                
                sleep_time = 0
                with P115Service._rate_limit_lock:
                    current_time = time.time()
                    elapsed = current_time - P115Service._last_request_time
                    
                    if elapsed < interval:
                        import random
                        jitter = random.uniform(0.1, 0.5)
                        # 计算当前线程需要休眠的时间
                        sleep_time = (interval - elapsed) + jitter
                        # ★ 核心修复：提前预支下一次的放行时间，让后续线程基于这个未来时间计算，而不是排队死等
                        P115Service._last_request_time = current_time + sleep_time
                    else:
                        P115Service._last_request_time = current_time

                # ★ 核心修复：把 sleep 移出锁的范围！让多线程可以同时并发 sleep
                if sleep_time > 0:
                    time.sleep(sleep_time)

            def _api_order(self, force_openapi=False, force_cookie=False):
                if force_openapi:
                    return [('OpenAPI', self._openapi)]
                if force_cookie:
                    return [('Cookie', self._cookie)]
                primary = get_115_api_priority()
                if primary == 'cookie':
                    return [('Cookie', self._cookie), ('OpenAPI', self._openapi)]
                return [('OpenAPI', self._openapi), ('Cookie', self._cookie)]

            def _iter_management_clients(self, method_name):
                for label, api in self._api_order():
                    if api and hasattr(api, method_name):
                        yield label.lower(), api

            def _call_api(self, method_name, *args, normalizer=None, force_openapi=False, force_cookie=False, **kwargs):
                """按 115 API 优先级调用；失败自动切换另一个接口，并统一返回 dict。"""
                last_resp = None
                last_err = None
                attempted = []
                for label, api in self._api_order(force_openapi=force_openapi, force_cookie=force_cookie):
                    if not api or not hasattr(api, method_name):
                        continue
                    attempted.append(label)
                    try:
                        self._rate_limit()
                        resp = getattr(api, method_name)(*args, **kwargs)
                        if normalizer:
                            resp = normalizer(resp)
                        last_resp = resp
                        if _p115_success(resp):
                            if len(attempted) > 1:
                                logger.info(f"  ➜ [115] {method_name} 已自动切换到 {label} 接口成功。")
                            return resp
                        # 115 秒传返回 status=7 不是接口故障，而是需要供给方 holder 按 sign_check
                        # 读取源文件片段生成 sign_val。消费端本机通常没有源文件，切换 Cookie/OpenAPI
                        # 或尝试“本机 Holder”只会浪费请求；直接把签名需求返回给共享资源消费层，
                        # 由中心端调度真正持有该 SHA1 的客户端友情签名。
                        if method_name == 'rapid_upload' and isinstance(resp, dict) and resp.get('_rapid_sign_required'):
                            logger.warning(
                                f"  ➜ [115] {label} 接口 rapid_upload 返回 status=7，"
                                "交给中心调度签名。"
                            )
                            return resp
                        logger.warning(f"  ➜ [115] {label} 接口 {method_name} 返回失败，准备尝试备用接口: {_p115_error_text(resp)}")
                    except Exception as e:
                        last_err = e
                        logger.warning(f"  ➜ [115] {label} 接口 {method_name} 异常，准备尝试备用接口: {e}")
                        if label == 'Cookie' and _p115_is_severe_failure(e):
                            P115Service.reset_cookie_client()
                    time.sleep(0.3)

                if last_resp is not None:
                    return last_resp
                if last_err is not None:
                    return {'state': False, 'error_msg': str(last_err)}
                return {'state': False, 'error_msg': f"{method_name} 无可用 115 接口，请检查 Token/Cookie 配置"}

            def get_user_info(self):
                # 用户信息优先走 OpenAPI；没有 Token 时才尝试 Cookie。
                self._rate_limit()
                if self._openapi: return self._openapi.get_user_info()
                if self._cookie: return self._cookie.get_user_info()
                return None

            def fs_files(self, payload):
                return self._call_api('fs_files', payload, normalizer=_p115_normalize_list_response)

            def fs_files_app(self, payload):
                return self._call_api('fs_files_app', payload, normalizer=_p115_normalize_list_response)
            
            def fs_search(self, payload):
                return self._call_api('fs_search', payload, normalizer=_p115_normalize_list_response)
            
            def _fill_info_parent_from_cache(self, resp, file_id):
                """Cookie 详情接口不稳定返回父目录 ID，缺失时从本地文件系统缓存反推。"""
                if not isinstance(resp, dict) or not _p115_success(resp):
                    return resp
                data = resp.get('data')
                if isinstance(data, list):
                    data = data[0] if data else {}
                    resp['data'] = data
                if not isinstance(data, dict):
                    return resp

                if str(data.get('parent_id') or data.get('pid') or '').strip():
                    return resp

                fid = str(data.get('fid') or data.get('file_id') or data.get('id') or file_id or '').strip()
                pick_code = str(data.get('pick_code') or data.get('pc') or data.get('pickcode') or '').strip()
                sha1 = str(data.get('sha1') or data.get('sha') or data.get('file_sha1') or '').strip().upper()

                for path_key in ('path', 'paths', 'breadcrumb'):
                    path_nodes = resp.get(path_key) or data.get(path_key)
                    if not isinstance(path_nodes, list):
                        continue
                    for node in reversed(path_nodes):
                        node = _p115_normalize_item(node) if isinstance(node, dict) else {}
                        node_id = str(node.get('fid') or node.get('file_id') or node.get('id') or node.get('cid') or '').strip()
                        node_fc = str(node.get('fc') or node.get('file_category') or '').strip()
                        if node_id and node_id != fid and (not node_fc or node_fc == '0'):
                            data['parent_id'] = node_id
                            data['pid'] = node_id
                            data.setdefault('_parent_id_source', path_key)
                            return resp

                row = None
                try:
                    if fid:
                        row = P115CacheManager.get_file_cache_by_id(fid)
                    if not row and pick_code:
                        row = P115CacheManager.get_file_cache_by_pickcode(pick_code)
                    if not row and sha1:
                        row = P115CacheManager.get_file_cache_by_sha1(sha1)
                except Exception as e:
                    logger.debug(f"  ➜ [115] Cookie 文件详情父目录推导失败: fid={fid or file_id}, err={e}")
                    return resp

                if not row:
                    logger.debug(f"  ➜ [115] Cookie 文件详情缺少父目录，且本地缓存未命中: fid={fid or file_id}")
                    return resp

                parent_id = str(row.get('parent_id') or '').strip()
                if parent_id:
                    data['parent_id'] = parent_id
                    data['pid'] = parent_id
                    data.setdefault('_parent_id_source', 'p115_filesystem_cache')
                cached_id = str(row.get('id') or fid or '').strip()
                cached_name = row.get('name')
                cached_pc = row.get('pick_code')
                cached_sha1 = row.get('sha1')
                cached_size = row.get('size')
                if cached_id:
                    data.setdefault('fid', cached_id)
                    data.setdefault('file_id', cached_id)
                if cached_name:
                    data.setdefault('name', cached_name)
                    data.setdefault('file_name', cached_name)
                    data.setdefault('fn', cached_name)
                if cached_pc:
                    data.setdefault('pick_code', cached_pc)
                    data.setdefault('pc', cached_pc)
                if cached_sha1:
                    data.setdefault('sha1', cached_sha1)
                    data.setdefault('sha', cached_sha1)
                if cached_size:
                    data.setdefault('size', cached_size)
                    data.setdefault('fs', cached_size)
                return resp

            def fs_get_info(self, file_id):
                # 详情优先走 OpenAPI 获取完整父目录；访问上限/失败时用 Cookie 备用，再从本地缓存反推 parent_id。
                openapi_resp = None
                if self._openapi:
                    openapi_resp = self._call_api(
                        'fs_get_info',
                        file_id,
                        normalizer=_p115_normalize_info_response,
                        force_openapi=True,
                    )
                    if _p115_success(openapi_resp):
                        return openapi_resp

                if self._cookie:
                    cookie_resp = self._call_api(
                        'fs_get_info',
                        file_id,
                        normalizer=_p115_normalize_info_response,
                        force_cookie=True,
                    )
                    if _p115_success(cookie_resp):
                        return self._fill_info_parent_from_cache(cookie_resp, file_id)
                    return cookie_resp

                if openapi_resp is not None:
                    return openapi_resp
                return {'state': False, 'error_msg': '未配置可用的 115 OpenAPI 或 Cookie，无法查询文件详情'}

            def _is_exists_error(self, resp):
                text = json.dumps(resp, ensure_ascii=False).lower() if resp is not None else ""
                return any(k in text for k in [
                    "已存在",
                    "目录名称已存在",
                    "该目录名称已存在",
                    "already",
                    "exist",
                    "exists",
                    "same_name",
                    "文件名重复",
                    "重复"
                ])


            def _find_child_dir(self, parent_cid, name):
                """
                在 parent_cid 下精准查找同名子目录。
                用于 mkdir 返回“已存在”后的 CID 回收。
                """
                if not parent_cid or not name:
                    return None

                try:
                    search_res = self.fs_files({
                        "cid": parent_cid,
                        "search_value": name,
                        "limit": 100,
                        "show_dir": 1,
                        "record_open_time": 0,
                        "count_folders": 0
                    })

                    for item in search_res.get("data", []):
                        item_name = (
                            item.get("fn")
                            or item.get("n")
                            or item.get("file_name")
                            or item.get("name")
                        )

                        item_fc = str(
                            item.get("fc")
                            if item.get("fc") is not None
                            else item.get("type")
                        )

                        # Cookie 目录项可能 cid 才是目录自身 ID
                        item_cid = (
                            item.get("fid")
                            or item.get("file_id")
                            or item.get("id")
                            or item.get("cid")
                        )

                        if item_name == name and item_fc == "0" and item_cid:
                            return str(item_cid)

                except Exception as e:
                    logger.debug(f"  ➜ [115] mkdir 已存在后回查目录失败: parent={parent_cid}, name={name}, err={e}")

                return None


            def fs_mkdir(self, name, pid):
                """
                创建目录：
                1. 先查内存缓存
                2. 再查 DB 缓存
                3. API 创建
                4. 如果 API 返回“已存在”，不切备用接口，直接回查同级目录并写缓存
                """
                parent_cid = str(pid)
                folder_name = str(name).strip()

                if not folder_name:
                    return {"state": False, "message": "目录名称不能为空"}

                cache_key = f"{parent_cid}_{folder_name}"

                # 1. 全局内存缓存
                try:
                    with _GLOBAL_DIR_LOCK:
                        cached_cid = _GLOBAL_DIR_CACHE.get(cache_key)
                    if cached_cid and cached_cid != "FAILED":
                        return {
                            "state": True,
                            "cid": str(cached_cid),
                            "data": {"file_id": str(cached_cid), "cid": str(cached_cid)},
                            "_from_cache": "memory"
                        }
                except Exception:
                    pass

                # 2. DB 缓存
                try:
                    cached_cid = P115CacheManager.get_cid(parent_cid, folder_name)
                    if cached_cid:
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = str(cached_cid)

                        return {
                            "state": True,
                            "cid": str(cached_cid),
                            "data": {"file_id": str(cached_cid), "cid": str(cached_cid)},
                            "_from_cache": "db"
                        }
                except Exception as e:
                    logger.debug(f"  ➜ [115] mkdir 前读取目录缓存失败: parent={parent_cid}, name={folder_name}, err={e}")

                last_resp = None

                # 2.5 DB 缓存没命中时，先远程回查同级目录，避免对已存在目录执行 POST mkdir
                # 尤其是 2026 / Season 01 / 分类目录这种高复用目录，缓存可能因为同步/旧数据缺失而穿透。
                try:
                    existed_cid = self._find_child_dir(parent_cid, folder_name)
                    if existed_cid:
                        P115CacheManager.save_cid(existed_cid, parent_cid, folder_name)
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = existed_cid

                        logger.info(f"  ➜ [115整理] 本地目录缓存未命中，但远程目录已存在，已回填：{folder_name}")
                        logger.debug(f"  ➜ [115整理] 回填远程目录 CID：{existed_cid}")

                        return {
                            "state": True,
                            "cid": existed_cid,
                            "data": {
                                "file_id": existed_cid,
                                "cid": existed_cid,
                                "file_name": folder_name,
                                "name": folder_name,
                                "parent_id": parent_cid
                            },
                            "_from_remote_prefetch": True
                        }
                except Exception as e:
                    logger.debug(f"  ➜ [115] mkdir 前远程预查目录失败，继续创建流程: parent={parent_cid}, name={folder_name}, err={e}")

                # 3. 按优先级尝试接口
                for api_name, api_client in self._iter_management_clients("fs_mkdir"):
                    try:
                        self._rate_limit()

                        resp = api_client.fs_mkdir(folder_name, parent_cid)
                        last_resp = resp

                        # 创建成功
                        if resp and resp.get("state"):
                            new_cid = (
                                resp.get("cid")
                                or resp.get("file_id")
                                or resp.get("id")
                                or resp.get("data", {}).get("file_id")
                                or resp.get("data", {}).get("cid")
                                or resp.get("data", {}).get("id")
                            )

                            if new_cid:
                                new_cid = str(new_cid)
                                P115CacheManager.save_cid(new_cid, parent_cid, folder_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = new_cid

                                resp["cid"] = new_cid
                                resp.setdefault("data", {})
                                resp["data"]["file_id"] = new_cid
                                resp["data"]["cid"] = new_cid

                            return resp

                        # 已存在不是接口失败，直接回查，不要切备用接口
                        if self._is_exists_error(resp):
                            existed_cid = self._find_child_dir(parent_cid, folder_name)
                            if existed_cid:
                                P115CacheManager.save_cid(existed_cid, parent_cid, folder_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = existed_cid

                                logger.info(f"  ➜ [115整理] 目录已存在，已复用并回收本次新建目录：{folder_name}")
                                logger.debug(f"  ➜ [115整理] 已存在目录 CID：{existed_cid}")

                                return {
                                    "state": True,
                                    "cid": existed_cid,
                                    "data": {
                                        "file_id": existed_cid,
                                        "cid": existed_cid,
                                        "file_name": folder_name,
                                        "name": folder_name,
                                        "parent_id": parent_cid
                                    },
                                    "_from_exists_recovery": api_name
                                }

                            logger.warning(f"  ➜ [115] 目录已存在但暂未回查到 CID: parent={parent_cid}, name={folder_name}")
                            return resp

                        logger.warning(
                            f"  ➜ [115] {api_name} 接口 fs_mkdir 返回失败，准备尝试备用接口: "
                            f"{resp.get('message') or resp.get('error') or resp.get('error_msg') or resp}"
                        )

                    except Exception as e:
                        last_resp = {"state": False, "message": str(e)}
                        if self._is_exists_error(last_resp):
                            existed_cid = self._find_child_dir(parent_cid, folder_name)
                            if existed_cid:
                                P115CacheManager.save_cid(existed_cid, parent_cid, folder_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = existed_cid

                                return {
                                    "state": True,
                                    "cid": existed_cid,
                                    "data": {"file_id": existed_cid, "cid": existed_cid},
                                    "_from_exists_recovery": "exception"
                                }

                        logger.warning(f"  ➜ [115] {api_name} 接口 fs_mkdir 异常，准备尝试备用接口: {e}")

                return last_resp or {"state": False, "message": "创建目录失败"}

            def fs_move(self, fids, to_cid):
                return self._call_api('fs_move', fids, to_cid, normalizer=_p115_normalize_common_response)

            def fs_copy(self, fids, to_cid):
                return self._call_api('fs_copy', fids, to_cid, normalizer=_p115_normalize_common_response)

            def fs_copy_backend(self, fids, to_cid, backend=''):
                backend = str(backend or '').strip().lower()
                return self._call_api(
                    'fs_copy',
                    fids,
                    to_cid,
                    normalizer=_p115_normalize_common_response,
                    force_openapi=(backend == 'openapi'),
                    force_cookie=(backend == 'cookie'),
                )

            def fs_rename(self, fid_name_tuple):
                return self._call_api('fs_rename', fid_name_tuple, normalizer=_p115_normalize_common_response)

            def fs_rename_batch(self, fid_name_tuples):
                """按用户配置选择批量/逐条重命名。

                - 115 API 优先级为 cookie：优先调用 Cookie /files/batch_rename；
                - 其他情况：保持 OpenAPI 逐条 /open/ufile/update；
                - 批量接口失败时自动退回逐条，避免后续 STRM 使用错误文件名。
                """
                pairs = _p115_normalize_rename_pairs(fid_name_tuples)
                if not pairs:
                    return {
                        'state': True,
                        'data': {'total': 0, 'success_count': 0, 'failed_count': 0},
                        '_rename_mode': 'noop',
                        '_rename_failures': {},
                    }

                def _sequential_rename(reason=None):
                    successes = []
                    failures = {}
                    for fid, new_name in pairs:
                        resp = self.fs_rename((fid, new_name))
                        if _p115_success(resp):
                            successes.append(fid)
                        else:
                            failures[fid] = resp

                    result = {
                        'state': not failures,
                        'data': {
                            'total': len(pairs),
                            'success_count': len(successes),
                            'failed_count': len(failures),
                        },
                        '_rename_mode': 'sequential',
                        '_rename_successes': successes,
                        '_rename_failures': failures,
                    }
                    if reason:
                        result['_fallback_reason'] = reason
                    return result

                if get_115_api_priority() == 'cookie' and self._cookie and hasattr(self._cookie, 'fs_rename_batch'):
                    try:
                        self._rate_limit()
                        resp = self._cookie.fs_rename_batch(pairs)
                        resp = _p115_normalize_common_response(resp)
                        if _p115_success(resp):
                            resp.update({
                                '_rename_mode': 'cookie_batch',
                                '_rename_successes': [fid for fid, _ in pairs],
                                '_rename_failures': {},
                            })
                            resp.setdefault('data', {})
                            if isinstance(resp.get('data'), dict):
                                resp['data'].setdefault('total', len(pairs))
                                resp['data'].setdefault('success_count', len(pairs))
                                resp['data'].setdefault('failed_count', 0)
                            logger.info(f"  ➜ [批量重命名] 已通过 115 批量接口重命名 {len(pairs)} 个文件。")
                            return resp

                        logger.warning(
                            f"  ➜ [批量重命名] Cookie 批量接口失败，回退逐条重命名: "
                            f"{_p115_error_text(resp)}"
                        )
                        return _sequential_rename(reason=_p115_error_text(resp))
                    except Exception as e:
                        if _p115_is_severe_failure(e):
                            P115Service.reset_cookie_client()
                        logger.warning(f"  ➜ [批量重命名] Cookie 批量接口异常，回退逐条重命名: {e}")
                        return _sequential_rename(reason=str(e))

                return _sequential_rename(reason='openapi_priority_or_cookie_unavailable')

            def fs_delete(self, fids):
                return self._call_api('fs_delete', fids, normalizer=_p115_normalize_common_response)
            
            def rb_del(self, tids=None):
                # 清空回收站是 OpenAPI 独有，强制 OpenAPI，不参与 Cookie 优先级。
                self._check_openapi()
                return self._call_api('rb_del', tids, normalizer=_p115_normalize_common_response, force_openapi=True)
            
            def life_behavior_detail(self, payload=None):
                # 生活事件仍是 Cookie/webapi 能力。
                res = self._call_api('life_behavior_detail', payload, force_cookie=True)
                return res

            def life_batch_delete(self, delete_data_list):
                res = self._call_api('life_batch_delete', delete_data_list, force_cookie=True)
                return res
            
            def upload_file_stream(self, file_stream, file_name, target_cid):
                # 上传初始化/OSS 调度走 OpenAPI。
                self._check_openapi()
                return self._call_api('upload_file_stream', file_stream, file_name, target_cid, force_openapi=True)


            def rapid_upload(self, payload=None, **kwargs):
                """Rapid v2 秒传入口：按用户配置的 115 API 优先级尝试。

                - cookie 优先：先走 Cookie/p115client 的上传初始化探测；普通失败再退 OpenAPI。
                - openapi 优先：维持原 OpenAPI /open/upload/init 优先。
                - 任一接口返回 status=7 时直接上抛签名需求，不再切备用接口、不再做消费端本机 Holder。
                注意：这里只使用本机 CK/Token，不把账号凭据上传中心，也不恢复旧分享表。
                """
                payload = dict(payload or {})
                payload.update({k: v for k, v in kwargs.items() if v not in (None, '')})
                return self._call_api('rapid_upload', payload, normalizer=_p115_normalize_common_response)

            def rapid_sign_value(self, payload=None, **kwargs):
                """Holder 端签名任务：本机按 sha1 找 pick_code，读取 sign_check Range 计算 sign_val。

                只返回 sign_val，不上传 CK/pick_code 到中心。
                """
                payload = dict(payload or {})
                payload.update({k: v for k, v in kwargs.items() if v not in (None, '')})

                def _first(*values):
                    for value in values:
                        if value not in (None, '', [], {}):
                            return value
                    return None

                sha1 = str(_first(payload.get('sha1'), payload.get('file_sha1'), payload.get('fileid')) or '').strip().upper()
                sign_check = str(_first(payload.get('sign_check'), payload.get('range')) or '').strip()
                pick_code = str(_first(payload.get('pick_code'), payload.get('pickcode'), payload.get('pc')) or '').strip()
                file_name = str(_first(payload.get('file_name'), payload.get('filename'), payload.get('name')) or '').strip()
                try:
                    size = int(float(_first(payload.get('size'), payload.get('file_size'), payload.get('filesize')) or 0))
                except Exception:
                    size = 0

                local = _p115_lookup_local_holder_file_for_sign(sha1=sha1, size=size, pick_code=pick_code, file_name=file_name)
                pc = str(local.get('pick_code') or pick_code or '').strip()
                if not pc:
                    raise RuntimeError(f'本机不是可签名 holder：未找到 sha1={sha1[:12]}... 对应 pick_code')
                if not file_name:
                    file_name = local.get('file_name') or sha1

                try:
                    _, _, _, app_type = get_115_tokens()
                except Exception:
                    app_type = 'web'
                sign_ua = str(_first(payload.get('sign_user_agent'), payload.get('user_agent'), payload.get('ua'), get_115_ua(app_type)) or get_115_ua('web'))
                priority = get_115_api_priority()
                order = [('download_url', 'Cookie蜂群Holder'), ('openapi_downurl', 'OpenAPI蜂群Holder')] if priority == 'cookie' else [('openapi_downurl', 'OpenAPI蜂群Holder'), ('download_url', 'Cookie蜂群Holder')]
                last_error = None
                for method_name, label in order:
                    method = getattr(self, method_name, None)
                    if not callable(method):
                        continue
                    try:
                        sign_result = _p115_try_local_holder_sign(
                            pick_code=pc,
                            sign_check=sign_check,
                            downurl_getter=lambda _pc, _ua, _m=method: _m(_pc, user_agent=_ua),
                            user_agent=sign_ua,
                            label=label,
                            sha1=sha1,
                            file_name=file_name,
                        )
                        if sign_result and sign_result.get('sign_val'):
                            out = dict(sign_result)
                            out.update({'state': True, 'success': True, 'sha1': sha1, 'file_name': file_name, 'backend': label})
                            return out
                    except Exception as e:
                        last_error = e
                        logger.warning(f"  ➜ [负载均衡签名] {label} 计算 sign_val 失败，尝试备用接口: {e}")
                raise RuntimeError(f'本机 holder 计算 sign_val 失败: {last_error}')

            def fs_rapid_upload(self, target_cid, sha1, size, file_name, preid=None, **kwargs):
                payload = {'cid': target_cid, 'sha1': sha1, 'size': size, 'file_name': file_name}
                if preid:
                    payload['preid'] = preid
                payload.update(kwargs)
                return self.rapid_upload(payload)

            def upload_file_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
                return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

            def fs_upload_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
                return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

            def upload_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
                return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

            def add_file_by_sha1(self, target_cid, sha1, size, file_name, **kwargs):
                return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

            def rapid_save(self, target_cid, sha1, size, file_name, **kwargs):
                return self.fs_rapid_upload(target_cid, sha1, size, file_name, **kwargs)

            def download_url(self, pick_code, user_agent=None):
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法获取播放直链")
                
                cache_key = (pick_code, user_agent)
                now = time.time()
                
                # 1. 查缓存
                if cache_key in _DIRECT_URL_CACHE:
                    cached_data = _DIRECT_URL_CACHE[cache_key]
                    if now < cached_data['expire_at']:
                        return cached_data['url']

                with P115Service._downurl_lock:
                    if cache_key in _DIRECT_URL_CACHE and now < _DIRECT_URL_CACHE[cache_key]['expire_at']:
                        return _DIRECT_URL_CACHE[cache_key]['url']

                    current_time = time.time()
                    elapsed = current_time - P115Service._last_downurl_time
                    if elapsed < 1.5:
                        time.sleep(1.5 - elapsed)
                    
                    try:
                        # ★ 核心修复：抛弃 with 语法，防止 wait=True 导致主线程死锁！
                        from concurrent.futures import ThreadPoolExecutor, TimeoutError
                        executor = ThreadPoolExecutor(max_workers=1)
                        future = executor.submit(self._cookie.download_url, pick_code, user_agent)
                        try:
                            res = future.result(timeout=15)
                            executor.shutdown(wait=False) # 正常结束，清理线程池
                        except TimeoutError:
                            logger.error(f"  🛑 [超时拦截] 获取直链网络卡死超过 15 秒，已强制切断！")
                            P115Service._last_downurl_time = time.time()
                            executor.shutdown(wait=False) # ★ 关键：不等卡死的线程，直接跑路！
                            # ★ 终极自愈：重置 Cookie 客户端，丢弃底层卡死的 Socket 连接池
                            P115Service.reset_cookie_client()
                            return None

                        P115Service._last_downurl_time = time.time()
                        
                        if res:
                            direct_url = str(res)
                            display_name = pick_code[:8] + "..."
                            
                            # ★ 从 115 返回的直链 URL 中反向解析出真实文件名
                            try:
                                from urllib.parse import urlparse, parse_qs, unquote
                                import os
                                parsed = urlparse(direct_url)
                                qs = parse_qs(parsed.query)
                                if 'file' in qs: display_name = unquote(qs['file'][0])
                                elif 'filename' in qs: display_name = unquote(qs['filename'][0])
                                else:
                                    path_name = unquote(os.path.basename(parsed.path))
                                    if path_name: display_name = path_name
                            except: pass

                            logger.info(f"  ➜ [115直链] 已通过 Cookie 获取下载直链：{display_name}")

                            # ★ 将文件名一起存入缓存
                            _DIRECT_URL_CACHE[cache_key] = {
                                'url': direct_url,
                                'name': display_name,
                                'expire_at': time.time() + 300
                            }
                            return direct_url
                        repaired = self._repair_stale_pick_code_downurl(pick_code, user_agent, backend='cookie')
                        return repaired.get('url') if isinstance(repaired, dict) else None
                    except Exception as e:
                        err_str = str(e)
                        if '405' not in err_str and 'Method Not Allowed' not in err_str:
                            repaired = self._repair_stale_pick_code_downurl(pick_code, user_agent, backend='cookie')
                            if isinstance(repaired, dict) and repaired.get('url'):
                                return repaired['url']
                        if '405' in err_str or 'Method Not Allowed' in err_str:
                            logger.error("  🛑 [熔断] 获取直链触发 115 WAF 风控 (405)，强制休眠 10 秒...")
                            P115Service._last_downurl_time = time.time() + 10
                        else:
                            P115Service._last_downurl_time = time.time()
                        raise e
                    
            def openapi_downurl(self, pick_code, user_agent=None):
                """使用 OpenAPI 获取直链 (带缓存和 UA 透传)"""
                self._check_openapi()
                cache_key = (f"openapi_{pick_code}", user_agent)
                now = time.time()
                
                if cache_key in _DIRECT_URL_CACHE:
                    cached_data = _DIRECT_URL_CACHE[cache_key]
                    if now < cached_data['expire_at']:
                        return cached_data['url']

                with P115Service._downurl_lock:
                    if cache_key in _DIRECT_URL_CACHE and now < _DIRECT_URL_CACHE[cache_key]['expire_at']:
                        return _DIRECT_URL_CACHE[cache_key]['url']

                    self._rate_limit()
                    try:
                        res = self._openapi.fs_downurl(pick_code, user_agent)
                        if res and res.get('state') and res.get('data'):
                            data_dict = res['data']
                            file_info = next(iter(data_dict.values()), None)
                            if file_info and 'url' in file_info and 'url' in file_info['url']:
                                direct_url = file_info['url']['url']
                                display_name = file_info.get('file_name', pick_code)
                                logger.info(f"  ➜ [115直链] 已通过 OpenAPI 获取下载直链：{display_name}")
                                _DIRECT_URL_CACHE[cache_key] = {
                                    'url': direct_url,
                                    'name': display_name,
                                    'expire_at': time.time() + 300 
                                }
                                return direct_url
                        repaired = self._repair_stale_pick_code_downurl(pick_code, user_agent, backend='openapi')
                        return repaired.get('url') if isinstance(repaired, dict) else None
                    except Exception as e:
                        repaired = self._repair_stale_pick_code_downurl(pick_code, user_agent, backend='openapi')
                        if isinstance(repaired, dict) and repaired.get('url'):
                            return repaired['url']
                        logger.warning(f"  ➜ [115 OpenAPI] 获取直链异常: {e}")
                        return None

            def _repair_stale_pick_code_downurl(self, old_pick_code, user_agent=None, backend='openapi'):
                """旧 PC 取不到直链时，用本地缓存中的 FID 重新获取当前 PC 并回写。"""
                old_pc = str(old_pick_code or '').strip()
                if not old_pc:
                    return {}
                try:
                    cache_row = P115CacheManager.get_file_cache_by_pickcode(old_pc)
                    if not cache_row or not cache_row.get('id'):
                        return {}
                    fid = str(cache_row.get('id') or '').strip()
                    info_res = self.fs_get_info(fid)
                    if not info_res or not info_res.get('state') or not isinstance(info_res.get('data'), dict):
                        logger.debug(f"  ➜ [115直链] 旧 PC 补救失败：无法通过 FID={fid} 获取文件详情")
                        return {}
                    info = info_res.get('data') or {}
                    new_pc = str(info.get('pick_code') or info.get('pc') or info.get('pickcode') or '').strip()
                    if not new_pc or new_pc == old_pc:
                        return {}

                    if backend == 'cookie':
                        if not self._cookie:
                            return {}
                        from concurrent.futures import ThreadPoolExecutor, TimeoutError
                        executor = ThreadPoolExecutor(max_workers=1)
                        future = executor.submit(self._cookie.download_url, new_pc, user_agent)
                        try:
                            direct_url = _p115_extract_down_url(future.result(timeout=15))
                        except TimeoutError:
                            executor.shutdown(wait=False)
                            P115Service.reset_cookie_client()
                            return {}
                        finally:
                            try:
                                executor.shutdown(wait=False)
                            except Exception:
                                pass
                    else:
                        if not self._openapi:
                            return {}
                        direct_url = _p115_extract_down_url(self._openapi.fs_downurl(new_pc, user_agent))

                    if not direct_url:
                        return {}

                    P115CacheManager.replace_pick_code_references(
                        old_pc,
                        new_pc,
                        fid=fid,
                        info_data=info,
                        source='playback_downurl_repair',
                    )
                    display_name = info.get('name') or info.get('file_name') or cache_row.get('name') or new_pc
                    _DIRECT_URL_CACHE[(new_pc if backend == 'cookie' else f"openapi_{new_pc}", user_agent)] = {
                        'url': direct_url,
                        'name': display_name,
                        'expire_at': time.time() + 300,
                    }
                    logger.info(
                        "  ➜ [115直链] 旧 PC 已自动修复：%s -> %s，文件=%s",
                        old_pc[:8] + "...",
                        new_pc[:8] + "...",
                        display_name,
                    )
                    return {'url': direct_url, 'pick_code': new_pc, 'fid': fid}
                except Exception as e:
                    logger.warning(f"  ➜ [115直链] 旧 PC 自动修复失败: pc={old_pc[:8]}..., err={e}")
                    return {}

            def request(self, *args, **kwargs):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行网络请求")
                return self._cookie.request(*args, **kwargs)

            def offline_add_urls(self, payload):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行离线下载")
                return self._cookie.offline_add_urls(payload)

            def share_import(self, share_code, receive_code, cid):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行转存")
                return self._cookie.share_import(share_code, receive_code, cid)

            def history_receive_list(self, offset=0, limit=100):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法查询最近接收记录")
                return self._cookie.history_receive_list(offset=offset, limit=limit)

            def history_delete(self, ids):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法删除历史记录")
                return self._cookie.history_delete(ids)

            def share_send(self, file_ids, **kwargs):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法创建分享")
                return self._cookie.share_send(file_ids, **kwargs)

            def share_update(self, share_code, share_duration=-1, receive_code=None, auto_fill_recvcode=0, receive_user_limit="", action=None):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法更新分享设置")
                return self._cookie.share_update(
                    share_code,
                    share_duration=share_duration,
                    receive_code=receive_code,
                    auto_fill_recvcode=auto_fill_recvcode,
                    receive_user_limit=receive_user_limit,
                    action=action,
                )

            def share_create(self, file_ids, share_duration=-1, receive_code=None):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法创建分享")
                return self._cookie.share_create(file_ids, share_duration=share_duration, receive_code=receive_code)

            def share_update_settings(self, share_code, share_duration=-1, receive_code=None, auto_fill_recvcode=0, receive_user_limit=''):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法更新分享设置")
                return self._cookie.share_update_settings(
                    share_code,
                    share_duration=share_duration,
                    receive_code=receive_code,
                    auto_fill_recvcode=auto_fill_recvcode,
                    receive_user_limit=receive_user_limit,
                )

            def share_info(self, share_code, receive_code=None, cid=0, limit=100, offset=0):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法查询分享状态")
                return self._cookie.share_info(share_code, receive_code=receive_code, cid=cid, limit=limit, offset=offset)

            def share_list(self, payload=None):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法查询分享列表")
                return self._cookie.share_list(payload)

            def share_cancel(self, share_code):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法取消分享")
                return self._cookie.share_cancel(share_code)

            def share_delete(self, share_code):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法删除分享")
                return self._cookie.share_delete(share_code)

        return StrictSplitClient(openapi, cookie)
    
    @classmethod
    def get_cookies(cls):
        """获取 Cookie (用于直链下载等)"""
        _, _, cookie, _ = get_115_tokens()
        return cookie
    
    @classmethod
    def get_token(cls):
        """获取 Token (用于 API 调用)"""
        token, _, _, _ = get_115_tokens()
        return token


# ======================================================================
# ★★★ 115 目录树 DB 缓存管理器 ★★★
# ======================================================================
class P115CacheManager:
    _rapid_preid_hints = LimitedCache(maxsize=10000)
    _rapid_preid_hints_lock = threading.Lock()

    @staticmethod
    def get_local_path(cid):
        """从本地数据库获取已缓存的完整相对路径"""
        if not cid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT local_path FROM p115_filesystem_cache WHERE id = %s", (str(cid),))
                    row = cursor.fetchone()
                    return row['local_path'] if row else None
        except Exception:
            return None
        
    @staticmethod
    def get_fid_by_pickcode(pick_code):
        """通过 PC 码获取文件 FID"""
        if not pick_code: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE pick_code = %s LIMIT 1", (pick_code,))
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception:
            return None

    @staticmethod
    def update_local_path(cid, local_path):
        """更新数据库中的 local_path"""
        if not cid or not local_path: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE p115_filesystem_cache 
                        SET local_path = %s, updated_at = NOW() 
                        WHERE id = %s
                    """, (str(local_path), str(cid)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 更新 local_path 失败: {e}")

    @staticmethod
    def get_node_info(cid):
        """获取节点的 parent_id 和 name (查户口)"""
        if not cid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT parent_id, name FROM p115_filesystem_cache WHERE id = %s", (str(cid),))
                    return cursor.fetchone()
        except Exception:
            return None

    @staticmethod
    def get_cid(parent_cid, name):
        """从本地数据库获取 CID (毫秒级)"""
        if not parent_cid or not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id FROM p115_filesystem_cache WHERE parent_id = %s AND name = %s", 
                        (str(parent_cid), str(name))
                    )
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            logger.error(f"  ➜ 读取 115 DB 缓存失败: {e}")
            return None

    @staticmethod
    def save_cid(cid, parent_cid, name, sha1=None):
        """将 CID 和 SHA1 存入本地数据库缓存"""
        if not cid or not parent_cid or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (id, parent_id, name, sha1)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET id = EXCLUDED.id, sha1 = EXCLUDED.sha1, updated_at = NOW()
                    """, (str(cid), str(parent_cid), str(name), sha1))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 DB 缓存失败: {e}")

    @staticmethod
    def get_file_sha1(fid):
        """从本地数据库获取已缓存的文件 SHA1"""
        if not fid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT sha1 FROM p115_filesystem_cache WHERE id = %s", (str(fid),))
                    row = cursor.fetchone()
                    return row['sha1'] if row else None
        except Exception:
            return None

    @staticmethod
    def get_cid_by_name(name):
        """仅通过名称查找 CID (适用于带有 {tmdb=xxx} 的唯一主目录)"""
        if not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE name = %s LIMIT 1", (str(name),))
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            return None
        
    @staticmethod
    def get_files_by_pickcodes(pickcodes):
        """通过 PC 码批量查出文件 ID 和 父目录 ID"""
        if not pickcodes: return []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法进行数组匹配
                    cursor.execute("SELECT id, parent_id, pick_code FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (list(pickcodes),))
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"  ➜ 查询文件缓存失败: {e}")
            return []

    @staticmethod
    def delete_cid(cid):
        """从缓存中物理删除该目录及其子目录的记录"""
        if not cid: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 删除自身以及以它为父目录的子项
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s OR parent_id = %s", (str(cid), str(cid)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 DB 缓存失败: {e}")

    @staticmethod
    def _preid_hint_keys(*, sha1=None, fid=None, pick_code=None, parent_id=None, name=None, size=None):
        """生成共享秒传 preid 提示键。preid 是内容属性，SHA1 命中最可信。"""
        keys = []

        def add(key):
            key = str(key or '').strip()
            if key and key not in keys:
                keys.append(key)

        sha1 = str(sha1 or '').strip().upper()
        fid = str(fid or '').strip()
        pick_code = str(pick_code or '').strip()
        parent_id = str(parent_id or '').strip()
        name_norm = re.sub(r'\s+', ' ', str(name or '').strip()).casefold()
        try:
            size_int = int(float(size or 0))
        except Exception:
            size_int = 0

        if fid:
            add(f'fid:{fid}')
        if pick_code:
            add(f'pc:{pick_code}')
        if sha1 and re.fullmatch(r'[A-F0-9]{40}', sha1):
            add(f'sha1:{sha1}')
            if parent_id:
                add(f'parent_sha1:{parent_id}:{sha1}')
        if parent_id and name_norm and size_int > 0:
            add(f'parent_name_size:{parent_id}:{name_norm}:{size_int}')
        return keys

    @staticmethod
    def _update_preid_for_existing_cache(preid, *, fid=None, parent_id=None, name=None, sha1=None, pick_code=None):
        """把已知 preid 回填到已经存在的 p115_filesystem_cache 行，避免后续再 Range 直链。"""
        preid = P115CacheManager._norm_preid(preid)
        if not preid:
            return False
        clauses, args = [], []
        fid = str(fid or '').strip()
        parent_id = str(parent_id or '').strip()
        name = str(name or '').strip()
        pick_code = str(pick_code or '').strip()
        sha1 = str(sha1 or '').strip().upper()
        if fid:
            clauses.append('id=%s')
            args.append(fid)
        if parent_id and name:
            clauses.append('(parent_id=%s AND name=%s)')
            args.extend([parent_id, name])
        if pick_code:
            clauses.append('pick_code=%s')
            args.append(pick_code)
        if sha1 and re.fullmatch(r'[A-F0-9]{40}', sha1):
            clauses.append('UPPER(sha1)=%s')
            args.append(sha1)
        if not clauses:
            return False
        try:
            P115CacheManager._ensure_preid_column()
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE p115_filesystem_cache
                        SET preid=%s, updated_at=NOW()
                        WHERE ({' OR '.join(clauses)})
                          AND (preid IS NULL OR preid='' OR preid=%s)
                        """,
                        [preid, *args, preid],
                    )
                    changed = cursor.rowcount or 0
                conn.commit()
            return changed > 0
        except Exception as e:
            logger.debug(f"  ➜ [115缓存] 回填共享秒传 preid 提示失败: {e}")
            return False

    @staticmethod
    def register_preid_hint(file_info=None, *, sha1=None, preid=None, fid=None, pick_code=None, parent_id=None, target_cid=None, file_name=None, name=None, size=None, source=''):
        """登记已知 preid 提示。

        共享池 Rapid v2 秒传时，中心源已经携带 preid；秒传成功后 115 新文件会进入待整理扫描，
        但 /files 列表不会返回 preid。这里先按 SHA1/目标目录/文件名保存提示，整理阶段写缓存或
        ensure_file_preid 时直接复用，避免再获取直链并 Range 读取前 128KB。
        """
        item = dict(file_info or {}) if isinstance(file_info, dict) else {}
        preid = P115CacheManager._norm_preid(
            preid
            or item.get('preid')
            or item.get('pre_sha1')
            or item.get('pre_sha1_128k')
        )
        if not preid:
            return ''
        sha1 = str(sha1 or item.get('sha1') or item.get('sha') or item.get('file_sha1') or '').strip().upper()
        fid = str(fid or item.get('fid') or item.get('file_id') or item.get('id') or '').strip()
        pick_code = str(pick_code or item.get('pick_code') or item.get('pc') or item.get('pickcode') or '').strip()
        parent_id = str(parent_id or target_cid or item.get('parent_id') or item.get('pid') or item.get('cid') or item.get('target_cid') or '').strip()
        file_name = str(file_name or name or item.get('file_name') or item.get('fn') or item.get('name') or '').strip()
        if size in (None, '', [], {}):
            size = item.get('size') or item.get('fs') or item.get('file_size') or item.get('filesize') or 0
        try:
            size_int = int(float(size or 0))
        except Exception:
            size_int = 0

        keys = P115CacheManager._preid_hint_keys(
            sha1=sha1,
            fid=fid,
            pick_code=pick_code,
            parent_id=parent_id,
            name=file_name,
            size=size_int,
        )
        if not keys:
            return preid
        hint = {
            'preid': preid,
            'sha1': sha1,
            'fid': fid,
            'pick_code': pick_code,
            'parent_id': parent_id,
            'name': file_name,
            'size': size_int,
            'source': str(source or ''),
            'updated_at': time.time(),
        }
        try:
            with P115CacheManager._rapid_preid_hints_lock:
                for key in keys:
                    P115CacheManager._rapid_preid_hints[key] = dict(hint)
        except Exception:
            pass

        P115CacheManager._update_preid_for_existing_cache(
            preid,
            fid=fid,
            parent_id=parent_id,
            name=file_name,
            sha1=sha1,
            pick_code=pick_code,
        )
        logger.debug(
            f"  ➜ [115缓存] 已登记共享秒传 preid 提示: "
            f"sha1={(sha1[:12] + '...') if sha1 else '-'}, parent={parent_id or '-'}, "
            f"name={file_name or '-'}, preid={preid[:12]}..., keys={len(keys)}"
        )
        return preid

    @staticmethod
    def _lookup_preid_hint(file_info=None, *, sha1=None, fid=None, pick_code=None, parent_id=None, target_cid=None, file_name=None, name=None, size=None):
        """查找共享秒传阶段登记的 preid 提示。"""
        item = dict(file_info or {}) if isinstance(file_info, dict) else {}
        direct = P115CacheManager._norm_preid(item.get('preid') or item.get('pre_sha1') or item.get('pre_sha1_128k'))
        if direct:
            return direct
        sha1 = str(sha1 or item.get('sha1') or item.get('sha') or item.get('file_sha1') or '').strip().upper()
        fid = str(fid or item.get('fid') or item.get('file_id') or item.get('id') or '').strip()
        pick_code = str(pick_code or item.get('pick_code') or item.get('pc') or item.get('pickcode') or '').strip()
        parent_id = str(parent_id or target_cid or item.get('parent_id') or item.get('pid') or item.get('cid') or item.get('target_cid') or '').strip()
        file_name = str(file_name or name or item.get('file_name') or item.get('fn') or item.get('name') or '').strip()
        if size in (None, '', [], {}):
            size = item.get('size') or item.get('fs') or item.get('file_size') or item.get('filesize') or 0
        try:
            size_int = int(float(size or 0))
        except Exception:
            size_int = 0

        keys = P115CacheManager._preid_hint_keys(
            sha1=sha1,
            fid=fid,
            pick_code=pick_code,
            parent_id=parent_id,
            name=file_name,
            size=size_int,
        )
        for key in keys:
            try:
                with P115CacheManager._rapid_preid_hints_lock:
                    hint = P115CacheManager._rapid_preid_hints.get(key)
                if isinstance(hint, dict):
                    preid = P115CacheManager._norm_preid(hint.get('preid'))
                    if preid:
                        return preid
            except Exception:
                continue
        return ''

    @staticmethod
    def save_file_cache(
        fid, parent_id, name, sha1=None, pick_code=None, local_path=None, size=0, preid=None,
        washing_level=None, washing_snapshot_json=None,
    ):
        """专门将文件(fc=1)的 SHA1、PC码、本地相对路径、大小、preid 和洗版快照存入本地数据库缓存"""
        if not fid or not parent_id or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    P115CacheManager._ensure_preid_column()
                    P115CacheManager._ensure_washing_snapshot_columns()
                    preid = P115CacheManager._norm_preid(preid)
                    if not preid:
                        preid = P115CacheManager._lookup_preid_hint({
                            'fid': fid,
                            'parent_id': parent_id,
                            'name': name,
                            'sha1': sha1,
                            'pick_code': pick_code,
                            'size': size,
                        })
                    try:
                        washing_level = int(washing_level) if washing_level not in (None, '', [], {}) else None
                    except Exception:
                        washing_level = None
                    washing_snapshot_json = washing_snapshot_json if isinstance(washing_snapshot_json, dict) else {}
                    if not preid:
                        cursor.execute(
                            """
                            SELECT preid
                            FROM p115_filesystem_cache
                            WHERE id = %s
                               OR (parent_id = %s AND name = %s)
                               OR (%s IS NOT NULL AND pick_code = %s)
                               OR (%s IS NOT NULL AND UPPER(sha1) = UPPER(%s))
                            ORDER BY CASE WHEN preid IS NOT NULL AND preid <> '' THEN 0 ELSE 1 END,
                                     updated_at DESC NULLS LAST
                            LIMIT 1
                            """,
                            (str(fid), str(parent_id), str(name), pick_code, pick_code, sha1, sha1),
                        )
                        old_row = cursor.fetchone()
                        if old_row:
                            preid = P115CacheManager._norm_preid(old_row.get('preid'))
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s", (str(fid),))

                    from psycopg2.extras import Json
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (
                            id, parent_id, name, sha1, pick_code, local_path, size, preid,
                            washing_level, washing_snapshot_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET
                            sha1 = CASE WHEN p115_filesystem_cache.id != EXCLUDED.id THEN EXCLUDED.sha1 ELSE COALESCE(EXCLUDED.sha1, p115_filesystem_cache.sha1) END,
                            pick_code = CASE WHEN p115_filesystem_cache.id != EXCLUDED.id THEN EXCLUDED.pick_code ELSE COALESCE(EXCLUDED.pick_code, p115_filesystem_cache.pick_code) END,
                            local_path = COALESCE(EXCLUDED.local_path, p115_filesystem_cache.local_path),
                            size = CASE WHEN EXCLUDED.size > 0 THEN EXCLUDED.size ELSE p115_filesystem_cache.size END,
                            preid = CASE WHEN p115_filesystem_cache.id != EXCLUDED.id THEN COALESCE(EXCLUDED.preid, p115_filesystem_cache.preid) ELSE COALESCE(EXCLUDED.preid, p115_filesystem_cache.preid) END,
                            washing_level = CASE WHEN EXCLUDED.washing_snapshot_json IS NOT NULL THEN EXCLUDED.washing_level ELSE p115_filesystem_cache.washing_level END,
                            washing_snapshot_json = COALESCE(EXCLUDED.washing_snapshot_json, p115_filesystem_cache.washing_snapshot_json),
                            id = EXCLUDED.id,
                            updated_at = NOW()
                    """, (
                        str(fid), str(parent_id), str(name), sha1, pick_code, local_path, size, preid or None,
                        washing_level, 
                        Json(washing_snapshot_json, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)) if washing_snapshot_json else None
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 文件缓存失败: {e}")


    @staticmethod
    def _filesystem_cache_row_to_dict(row):
        """把 p115_filesystem_cache 查询结果转成普通 dict，方便任务层复用。"""
        if not row:
            return None
        try:
            return dict(row)
        except Exception:
            return row

    @staticmethod
    def get_file_cache_by_id(fid):
        """按 115 文件 FID 获取完整文件缓存行。"""
        if not fid:
            return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, parent_id, name, sha1, pick_code, local_path, size, preid, washing_level, washing_snapshot_json
                        FROM p115_filesystem_cache
                        WHERE id = %s
                        LIMIT 1
                    """, (str(fid),))
                    return P115CacheManager._filesystem_cache_row_to_dict(cursor.fetchone())
        except Exception as e:
            logger.debug(f"  ➜ 读取 115 文件缓存失败(fid={fid}): {e}")
            return None

    @staticmethod
    def get_file_cache_by_pickcode(pick_code):
        """按 115 PickCode 获取完整文件缓存行。"""
        if not pick_code:
            return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, parent_id, name, sha1, pick_code, local_path, size, preid, washing_level, washing_snapshot_json
                        FROM p115_filesystem_cache
                        WHERE pick_code = %s
                        LIMIT 1
                    """, (str(pick_code),))
                    return P115CacheManager._filesystem_cache_row_to_dict(cursor.fetchone())
        except Exception as e:
            logger.debug(f"  ➜ 读取 115 文件缓存失败(pc={pick_code}): {e}")
            return None

    @staticmethod
    def replace_pick_code_references(old_pick_code, new_pick_code, *, fid='', info_data=None, source='') -> Dict[str, Any]:
        """PC 过期后的统一回写：缓存表、整理记录、媒体元数据和 HTTP STRM。"""
        old_pc = str(old_pick_code or '').strip()
        new_pc = str(new_pick_code or '').strip()
        if not old_pc or not new_pc or old_pc == new_pc:
            return {'updated': False, 'reason': 'invalid_pick_code'}

        fid = str(fid or '').strip()
        info = info_data if isinstance(info_data, dict) else {}
        stats = {
            'updated': False,
            'filesystem_cache': 0,
            'organize_records': 0,
            'media_metadata': 0,
            'strm_files': 0,
        }

        def _first(*values):
            for value in values:
                text = str(value or '').strip()
                if text:
                    return text
            return ''

        try:
            old_row = None
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if fid:
                        cursor.execute(
                            """
                            SELECT id, parent_id, name, sha1, pick_code, local_path, size
                            FROM p115_filesystem_cache
                            WHERE id = %s OR pick_code = %s
                            ORDER BY CASE WHEN id = %s THEN 0 ELSE 1 END
                            LIMIT 1
                            """,
                            (fid, old_pc, fid),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT id, parent_id, name, sha1, pick_code, local_path, size
                            FROM p115_filesystem_cache
                            WHERE pick_code = %s
                            LIMIT 1
                            """,
                            (old_pc,),
                        )
                    old_row = P115CacheManager._filesystem_cache_row_to_dict(cursor.fetchone()) or {}

                    final_fid = _first(fid, old_row.get('id'), info.get('fid'), info.get('file_id'), info.get('id'))
                    parent_id = _first(info.get('parent_id'), info.get('pid'), info.get('cid'), old_row.get('parent_id'))
                    name = _first(info.get('name'), info.get('file_name'), info.get('fn'), old_row.get('name'))
                    sha1 = _first(info.get('sha1'), info.get('sha'), info.get('file_sha1'), old_row.get('sha1')).upper()
                    local_path = _first(old_row.get('local_path'))
                    try:
                        size = int(float(info.get('size') or info.get('fs') or info.get('file_size') or old_row.get('size') or 0))
                    except Exception:
                        size = 0

                    if final_fid:
                        cursor.execute(
                            "UPDATE p115_filesystem_cache SET pick_code = NULL, updated_at = NOW() WHERE pick_code = %s AND id <> %s",
                            (new_pc, final_fid),
                        )
                        cursor.execute(
                            """
                            UPDATE p115_filesystem_cache
                            SET parent_id = COALESCE(%s, parent_id),
                                name = COALESCE(%s, name),
                                sha1 = COALESCE(%s, sha1),
                                pick_code = %s,
                                local_path = COALESCE(%s, local_path),
                                size = CASE WHEN %s > 0 THEN %s ELSE size END,
                                updated_at = NOW()
                            WHERE id = %s OR pick_code = %s
                            """,
                            (parent_id or None, name or None, sha1 or None, new_pc, local_path or None, size, size, final_fid, old_pc),
                        )
                        stats['filesystem_cache'] = cursor.rowcount or 0

                    cursor.execute(
                        "UPDATE p115_organize_records SET pick_code = NULL WHERE pick_code = %s AND (%s = '' OR file_id <> %s)",
                        (new_pc, final_fid, final_fid),
                    )
                    cursor.execute(
                        """
                        UPDATE p115_organize_records
                        SET pick_code = %s
                        WHERE pick_code = %s OR (%s <> '' AND file_id = %s)
                        """,
                        (new_pc, old_pc, final_fid, final_fid),
                    )
                    stats['organize_records'] = cursor.rowcount or 0

                    cursor.execute(
                        """
                        UPDATE media_metadata
                        SET file_pickcode_json = (
                            SELECT COALESCE(jsonb_agg(
                                CASE WHEN elem.value = to_jsonb(%s::text) THEN to_jsonb(%s::text) ELSE elem.value END
                                ORDER BY elem.ord
                            ), '[]'::jsonb)
                            FROM jsonb_array_elements(file_pickcode_json) WITH ORDINALITY AS elem(value, ord)
                        ),
                        last_updated_at = NOW()
                        WHERE file_pickcode_json ? %s
                        """,
                        (old_pc, new_pc, old_pc),
                    )
                    stats['media_metadata'] = cursor.rowcount or 0
                conn.commit()

            stats['strm_files'] = P115CacheManager._replace_pick_code_in_strm_file(old_pc, new_pc, old_row)
            stats['updated'] = any(stats.get(k, 0) for k in ('filesystem_cache', 'organize_records', 'media_metadata', 'strm_files'))
            logger.info(
                "  ➜ [115缓存] 已回写最新 PC：%s -> %s，缓存=%s，整理记录=%s，媒体元数据=%s，STRM=%s，来源=%s",
                old_pc[:8] + "...",
                new_pc[:8] + "...",
                stats['filesystem_cache'],
                stats['organize_records'],
                stats['media_metadata'],
                stats['strm_files'],
                source or '-',
            )
            return stats
        except Exception as e:
            logger.warning(f"  ➜ [115缓存] 回写最新 PC 失败: {old_pc[:8]}... -> {new_pc[:8]}..., err={e}")
            return {**stats, 'error': str(e)}

    @staticmethod
    def _replace_pick_code_in_strm_file(old_pick_code, new_pick_code, cache_row=None) -> int:
        old_pc = str(old_pick_code or '').strip()
        new_pc = str(new_pick_code or '').strip()
        row = cache_row if isinstance(cache_row, dict) else {}
        local_path = str(row.get('local_path') or '').strip()
        if not old_pc or not new_pc or not local_path:
            return 0
        try:
            cfg = get_config() or {}
            local_root = str(cfg.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or cfg.get('local_strm_root') or '').strip()
            if not local_root:
                return 0
            rel = local_path.replace('\\', '/').lstrip('/\\')
            strm_rel = os.path.splitext(rel)[0] + '.strm'
            strm_path = os.path.join(local_root, *strm_rel.split('/'))
            if not os.path.exists(strm_path):
                return 0
            with open(strm_path, 'r', encoding='utf-8') as f:
                content = f.read()
            pattern = re.compile(r'(/api/p115/play/)' + re.escape(old_pc) + r'(?=([/?#]|$))')
            new_content, count = pattern.subn(lambda m: m.group(1) + new_pc, content)
            if count <= 0 or new_content == content:
                return 0
            emby_url = str(cfg.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').strip()
            emby_api_key = str(cfg.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
            ignore_features = None
            if emby_url and emby_api_key:
                try:
                    from handler import emby
                    ignore_features = emby.enable_strm_assistant_ignore_file_change(emby_url, emby_api_key)
                except Exception as e:
                    logger.debug(f"  ➜ [115缓存] 开启神医忽略文件变更失败: {e}")
            try:
                with open(strm_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
            finally:
                if ignore_features is not None:
                    try:
                        from handler import emby
                        emby.disable_strm_assistant_ignore_file_change(emby_url, emby_api_key, ignore_features)
                    except Exception as e:
                        logger.debug(f"  ➜ [115缓存] 恢复神医忽略文件变更失败: {e}")
            return 1
        except Exception as e:
            logger.debug(f"  ➜ [115缓存] 更新 STRM 中的 PC 失败: {e}")
            return 0

    @staticmethod
    def get_file_cache_by_sha1(sha1):
        """按 SHA1 获取完整文件缓存行。"""
        if not sha1:
            return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, parent_id, name, sha1, pick_code, local_path, size, preid, washing_level, washing_snapshot_json
                        FROM p115_filesystem_cache
                        WHERE UPPER(sha1) = UPPER(%s)
                        ORDER BY updated_at DESC NULLS LAST
                        LIMIT 1
                    """, (str(sha1),))
                    return P115CacheManager._filesystem_cache_row_to_dict(cursor.fetchone())
        except Exception as e:
            logger.debug(f"  ➜ 读取 115 文件缓存失败(sha1={str(sha1)[:12]}...): {e}")
            return None

    @staticmethod
    def get_file_cache_by_local_path(local_path):
        """按本地相对路径获取完整文件缓存行，兼容 / 与 \\ 分隔符。"""
        if not local_path:
            return None

        normalized = str(local_path).strip().replace('\\', '/')
        normalized = re.sub(r'/+', '/', normalized).strip('/')
        if not normalized:
            return None

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, parent_id, name, sha1, pick_code, local_path, size, preid, washing_level, washing_snapshot_json
                        FROM p115_filesystem_cache
                        WHERE local_path = %s
                        LIMIT 1
                    """, (normalized,))
                    row = cursor.fetchone()
                    if row:
                        return P115CacheManager._filesystem_cache_row_to_dict(row)

                    # 挂载/路径前缀不一致时的兜底：只允许“完整路径段后缀”匹配，避免单文件名误命中。
                    if '/' in normalized:
                        cursor.execute("""
                            SELECT id, parent_id, name, sha1, pick_code, local_path, size, preid, washing_level, washing_snapshot_json
                            FROM p115_filesystem_cache
                            WHERE local_path IS NOT NULL
                              AND %s LIKE '%%/' || local_path
                            ORDER BY length(local_path) DESC
                            LIMIT 1
                        """, (normalized,))
                        return P115CacheManager._filesystem_cache_row_to_dict(cursor.fetchone())
        except Exception as e:
            logger.debug(f"  ➜ 读取 115 文件缓存失败(local_path={normalized}): {e}")

        return None

    @staticmethod
    def save_transfer_context(root_name, tmdb_id, media_type, title, season_number=None, episode_number=None, *, pick_code=None, sha1=None, source='', source_kind='', source_kinds=None, confidence='high', authority_role='expected', identify_title=None, clean_title=None, matched_rules=None, evidence=None, conflict_reason='', alias_titles=None, parse_version='transfer-context-v1', is_special=None):
        payload = _coerce_transfer_context_dict({
            "root_name": root_name,
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "title": title,
            "identify_title": identify_title or title,
            "clean_title": clean_title or title,
            "season_number": season_number,
            "episode_number": episode_number,
            "pick_code": pick_code,
            "sha1": sha1,
            "source": source,
            "source_kind": source_kind,
            "source_kinds": list(source_kinds or []),
            "confidence": confidence,
            "authority_role": authority_role,
            "matched_rules": list(matched_rules or []),
            "evidence": list(evidence or []),
            "conflict_reason": conflict_reason,
            "alias_titles": list(alias_titles or []),
            "parse_version": parse_version,
            "is_special": is_special,
            "keys": [root_name, title],
        })
        if not payload:
            return False

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (_TRANSFER_CONTEXTS_KEY,))
                    row = cursor.fetchone()
                    current = row.get("value_json") if row else {}
                    contexts = current.get("contexts") if isinstance(current, dict) else {}
                    if not isinstance(contexts, dict):
                        contexts = {}

                    updated_at = int(time.time())
                    payload["updated_at"] = updated_at
                    for key in payload.get("keys") or []:
                        contexts[key] = dict(payload)

                    if len(contexts) > _TRANSFER_CONTEXT_LIMIT:
                        items = sorted(
                            contexts.items(),
                            key=lambda item: int(((item[1] or {}).get("updated_at") or 0)),
                            reverse=True,
                        )
                        contexts = {k: v for k, v in items[:_TRANSFER_CONTEXT_LIMIT]}

                    cursor.execute(
                        """
                        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (setting_key) DO UPDATE SET
                            value_json = EXCLUDED.value_json,
                            last_updated_at = NOW()
                        """,
                        (_TRANSFER_CONTEXTS_KEY, json.dumps({"contexts": contexts}, ensure_ascii=False)),
                    )
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"  ➜ 保存转存整理上下文失败: {e}", exc_info=True)
            return False

    @staticmethod
    def get_transfer_context(*names):
        keys = _build_transfer_context_keys(*names)
        if not keys:
            return None

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (_TRANSFER_CONTEXTS_KEY,))
                    row = cursor.fetchone()
                    current = row.get("value_json") if row else {}
                    contexts = current.get("contexts") if isinstance(current, dict) else {}
                    if not isinstance(contexts, dict):
                        return None

                    for key in keys:
                        payload = _coerce_transfer_context_dict(contexts.get(key))
                        if payload:
                            payload["updated_at"] = (contexts.get(key) or {}).get("updated_at")
                            return payload
        except Exception as e:
            logger.error(f"  ➜ 读取转存整理上下文失败: {e}", exc_info=True)

        return None

    @staticmethod
    def delete_transfer_context(*names):
        keys = _build_transfer_context_keys(*names)
        if not keys:
            return False

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (_TRANSFER_CONTEXTS_KEY,))
                    row = cursor.fetchone()
                    current = row.get("value_json") if row else {}
                    contexts = current.get("contexts") if isinstance(current, dict) else {}
                    if not isinstance(contexts, dict):
                        return False

                    changed = False
                    for key in keys:
                        if key in contexts:
                            contexts.pop(key, None)
                            changed = True

                    if changed:
                        cursor.execute(
                            """
                            INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (setting_key) DO UPDATE SET
                                value_json = EXCLUDED.value_json,
                                last_updated_at = NOW()
                            """,
                            (_TRANSFER_CONTEXTS_KEY, json.dumps({"contexts": contexts}, ensure_ascii=False)),
                        )
                        conn.commit()
                    return changed
        except Exception as e:
            logger.error(f"  ➜ 清理转存整理上下文失败: {e}", exc_info=True)
            return False

    @staticmethod
    def delete_files(fids):
        """批量从缓存中物理删除文件记录"""
        if not fids: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法批量删除
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(fids),))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 文件缓存失败: {e}")

    @staticmethod
    def _sanitize_raw_ffprobe_for_cache(raw_ffprobe_json):
        """移除 raw_ffprobe_json 中不适合共享的临时/账号相关信息。"""
        if not raw_ffprobe_json:
            return raw_ffprobe_json

        try:
            if isinstance(raw_ffprobe_json, str):
                raw_ffprobe_json = json.loads(raw_ffprobe_json)
        except Exception:
            return raw_ffprobe_json

        if not isinstance(raw_ffprobe_json, dict):
            return raw_ffprobe_json

        fmt = raw_ffprobe_json.get("format")
        if isinstance(fmt, dict):
            fmt.pop("filename", None)

        ctx = raw_ffprobe_json.get("_etk")
        if isinstance(ctx, dict):
            # 只保留跨账号、长期稳定的共享字段。
            # season_number / episode_number 是媒体身份的一部分，上传到中心后可避免消费端再次从文件名正则猜集号。
            allowed = {"tmdb_id", "type", "original_language", "sha1", "season_number", "episode_number"}
            raw_ffprobe_json["_etk"] = {
                k: v for k, v in ctx.items()
                if k in allowed and v not in [None, "", [], {}]
            }
            if not raw_ffprobe_json["_etk"]:
                raw_ffprobe_json.pop("_etk", None)

        return raw_ffprobe_json

    @staticmethod
    def _norm_preid(value):
        text = str(value or '').strip().upper()
        return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''

    @staticmethod
    def _ensure_preid_column():
        """兼容旧库：确保 p115_filesystem_cache 有 preid 字段。"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("ALTER TABLE p115_filesystem_cache ADD COLUMN IF NOT EXISTS preid TEXT")
                    conn.commit()
        except Exception as e:
            logger.debug(f"  ➜ [115缓存] 确认 preid 字段失败: {e}")

    @staticmethod
    def _ensure_washing_snapshot_columns():
        """兼容旧库：确保 p115_filesystem_cache 有洗版优先级快照字段。"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("ALTER TABLE p115_filesystem_cache ADD COLUMN IF NOT EXISTS washing_level INTEGER")
                    cursor.execute("ALTER TABLE p115_filesystem_cache ADD COLUMN IF NOT EXISTS washing_snapshot_json JSONB DEFAULT '{}'::jsonb")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_p115_washing_level ON p115_filesystem_cache (washing_level) WHERE washing_level IS NOT NULL")
                    conn.commit()
        except Exception as e:
            logger.debug(f"  ➜ [115缓存] 确认 washing_* 字段失败: {e}")

    @staticmethod
    def _extract_preid_range_bytes(pick_code, start=0, end=131071):
        """只读取文件前 128KB，用于计算 115 upload/init 的 preid。"""
        pick_code = str(pick_code or '').strip()
        if not pick_code:
            return b''
        try:
            client = P115Service.get_client()
            if not client:
                return b''
            try:
                _, _, _, app_type = get_115_tokens()
            except Exception:
                app_type = 'web'
            ua_candidates = []
            # 优先复用刚刚 ffprobe / STRM 生成阶段已经取过的直链 UA，避免同一个文件连续请求两次直链。
            try:
                for cache_key, cached in list(_DIRECT_URL_CACHE.items()):
                    if not isinstance(cache_key, tuple) or len(cache_key) < 2:
                        continue
                    if str(cache_key[0] or '').strip() != pick_code:
                        continue
                    if not isinstance(cached, dict) or not cached.get('url'):
                        continue
                    cached_ua = cache_key[1]
                    if cached_ua is None:
                        if None not in ua_candidates:
                            ua_candidates.append(None)
                    else:
                        cached_ua = str(cached_ua or '').strip()
                        if cached_ua and cached_ua not in ua_candidates:
                            ua_candidates.append(cached_ua)
            except Exception:
                pass
            for ua in ('Mozilla/5.0', get_115_ua(app_type or 'web'), get_115_ua('web'), get_115_ua('mac')):
                ua = str(ua or '').strip()
                if ua and ua not in ua_candidates:
                    ua_candidates.append(ua)
            if not ua_candidates:
                ua_candidates.append('Mozilla/5.0')

            priority = get_115_api_priority()
            method_order = [('download_url', 'Cookie'), ('openapi_downurl', 'OpenAPI')] if priority == 'cookie' else [('openapi_downurl', 'OpenAPI'), ('download_url', 'Cookie')]
            range_header = f'bytes={int(start)}-{int(end)}'
            expected_len = int(end) - int(start) + 1
            last_status = None

            for method_name, label in method_order:
                method = getattr(client, method_name, None)
                if not callable(method):
                    continue
                for ua in ua_candidates:
                    try:
                        down_url = _p115_extract_down_url(method(pick_code, user_agent=ua))
                    except TypeError:
                        try:
                            down_url = _p115_extract_down_url(method(pick_code, ua))
                        except Exception as e:
                            logger.debug(f"  ➜ [115缓存] 获取 preid 直链失败({label}, positional-ua): {e}")
                            down_url = ''
                    except Exception as e:
                        logger.debug(f"  ➜ [115缓存] 获取 preid 直链失败({label}): {e}")
                        down_url = ''
                    if not down_url:
                        continue
                    try:
                        headers = {
                            'Range': range_header,
                            'Accept': '*/*',
                            'Connection': 'close',
                        }
                        if ua:
                            headers['User-Agent'] = ua
                        with requests.get(down_url, headers=headers, timeout=45, allow_redirects=True, stream=True) as resp:
                            last_status = resp.status_code
                            if resp.status_code != 206:
                                logger.warning(
                                    f"  ➜ [115缓存] 读取 preid Range 失败: api={label}, "
                                    f"HTTP={resp.status_code}, range={range_header}, pc={pick_code[:8]}..."
                                )
                                continue
                            content = resp.raw.read(expected_len) or b''
                            if content:
                                logger.debug(
                                    f"  ➜ [115缓存] preid Range 读取成功: api={label}, "
                                    f"range={range_header}, bytes={len(content)}, pc={pick_code[:8]}..."
                                )
                                return content
                    except Exception as e:
                        logger.debug(
                            f"  ➜ [115缓存] Range GET 异常: api={label}, "
                            f"range={range_header}, pc={pick_code[:8]}..., err={e}"
                        )
            if last_status:
                logger.warning(f"  ➜ [115缓存] 已尝试读取 preid Range 仍失败，最后 HTTP={last_status}: pc={pick_code[:8]}...")
        except Exception as e:
            logger.debug(f"  ➜ [115缓存] 计算 preid 前置读取失败: pc={pick_code[:8]}..., err={e}")
        return b''

    @staticmethod
    def ensure_file_preid(file_info=None, *, sha1=None, fid=None, pick_code=None, file_name=None):
        """确保 p115_filesystem_cache 中对应文件有 preid，并返回 preid。

        调用场景：整理/MP直出提取媒体信息后，已经拿到 SHA1/PC/FID，顺手读取前 128KB
        计算 preid，避免后续共享登记/秒传时再单独补齐。
        """
        item = dict(file_info or {}) if isinstance(file_info, dict) else {}
        sha1 = str(sha1 or item.get('sha1') or item.get('sha') or item.get('file_sha1') or '').strip().upper()
        fid = str(fid or item.get('fid') or item.get('file_id') or item.get('id') or '').strip()
        pick_code = str(pick_code or item.get('pick_code') or item.get('pc') or item.get('pickcode') or '').strip()
        file_name = str(file_name or item.get('file_name') or item.get('fn') or item.get('name') or '').strip()
        existing = P115CacheManager._norm_preid(item.get('preid') or item.get('pre_sha1') or item.get('pre_sha1_128k'))
        if existing:
            return existing

        hinted_preid = P115CacheManager._lookup_preid_hint(
            item,
            sha1=sha1,
            fid=fid,
            pick_code=pick_code,
            file_name=file_name,
        )
        if hinted_preid:
            P115CacheManager._update_preid_for_existing_cache(
                hinted_preid,
                fid=fid,
                parent_id=item.get('parent_id') or item.get('pid') or item.get('cid'),
                name=file_name,
                sha1=sha1,
                pick_code=pick_code,
            )
            logger.debug(
                f"  ➜ [115缓存] 命中共享秒传 preid 提示，跳过直链 Range: "
                f"{file_name or sha1 or pick_code} -> {hinted_preid[:12]}..."
            )
            return hinted_preid

        P115CacheManager._ensure_preid_column()
        clauses, args = [], []
        if fid:
            clauses.append('id=%s')
            args.append(fid)
        if pick_code:
            clauses.append('pick_code=%s')
            args.append(pick_code)
        if sha1 and re.fullmatch(r'[A-F0-9]{40}', sha1):
            clauses.append('UPPER(sha1)=%s')
            args.append(sha1)
        if clauses:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f"""
                            SELECT id, name, sha1, pick_code, preid
                            FROM p115_filesystem_cache
                            WHERE {' OR '.join(clauses)}
                            ORDER BY CASE WHEN preid IS NOT NULL AND preid <> '' THEN 0 ELSE 1 END,
                                     updated_at DESC NULLS LAST
                            LIMIT 1
                            """,
                            args,
                        )
                        row = cursor.fetchone()
                if row:
                    row = dict(row)
                    found_preid = P115CacheManager._norm_preid(row.get('preid'))
                    if found_preid:
                        return found_preid
                    fid = fid or str(row.get('id') or '').strip()
                    pick_code = pick_code or str(row.get('pick_code') or '').strip()
                    sha1 = sha1 or str(row.get('sha1') or '').strip().upper()
                    file_name = file_name or str(row.get('name') or '').strip()
            except Exception as e:
                logger.debug(f"  ➜ [115缓存] 查询已有 preid 失败: {e}")

        if not pick_code:
            return ''
        chunk = P115CacheManager._extract_preid_range_bytes(pick_code, 0, 131071)
        if not chunk:
            return ''
        preid = hashlib.sha1(chunk).hexdigest().upper()
        update_clauses, update_args = [], []
        if fid:
            update_clauses.append('id=%s')
            update_args.append(fid)
        if pick_code:
            update_clauses.append('pick_code=%s')
            update_args.append(pick_code)
        if sha1 and re.fullmatch(r'[A-F0-9]{40}', sha1):
            update_clauses.append('UPPER(sha1)=%s')
            update_args.append(sha1)
        if update_clauses:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f"""
                            UPDATE p115_filesystem_cache
                            SET preid=%s, updated_at=NOW()
                            WHERE {' OR '.join(update_clauses)}
                            """,
                            [preid, *update_args],
                        )
                    conn.commit()
                logger.info(f"  ➜ [115缓存] 已缓存秒传校验片段：{file_name or sha1 or pick_code}")
            except Exception as e:
                logger.debug(f"  ➜ [115缓存] 回写 p115_filesystem_cache.preid 失败: {e}")
        return preid

    @staticmethod
    def save_mediainfo_cache(sha1, mediainfo_json, raw_ffprobe_json=None, file_info=None, *, fid=None, pick_code=None, file_name=None):
        """写入本地 p115_mediainfo_cache，结构保持 Emby MediaSourceInfo 标准格式"""
        if not sha1 or not mediainfo_json:
            return False

        try:
            from psycopg2.extras import Json

            sha1 = str(sha1).upper()
            raw_ffprobe_json = P115CacheManager._sanitize_raw_ffprobe_for_cache(raw_ffprobe_json)

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, raw_ffprobe_json, created_at, hit_count)
                        VALUES (%s, %s, %s, NOW(), 0)
                        ON CONFLICT (sha1)
                        DO UPDATE SET
                            mediainfo_json = EXCLUDED.mediainfo_json,
                            raw_ffprobe_json = COALESCE(EXCLUDED.raw_ffprobe_json, p115_mediainfo_cache.raw_ffprobe_json),
                            created_at = NOW()
                    """, (
                        sha1,
                        Json(mediainfo_json, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)) if mediainfo_json else None,
                        Json(raw_ffprobe_json, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)) if raw_ffprobe_json else None
                    ))
                    conn.commit()

            # 整理/MP直出提取媒体信息时顺手补齐 preid：
            # 只读取前 128KB，写入 p115_filesystem_cache，供后续 Rapid v2 登记/秒传直接复用。
            try:
                preid = P115CacheManager.ensure_file_preid(
                    file_info if isinstance(file_info, dict) else {'sha1': sha1},
                    sha1=sha1,
                    fid=fid,
                    pick_code=pick_code,
                    file_name=file_name,
                )
                if preid and isinstance(file_info, dict):
                    file_info['preid'] = preid
            except Exception as e_preid:
                logger.debug(f"  ➜ [媒体信息缓存] 顺手计算 preid 失败: sha1={sha1[:12]}..., err={e_preid}")

            logger.debug(f"  ➜ [媒体信息缓存] 已写入本地 p115_mediainfo_cache -> {sha1[:12]}...")
            return True

        except Exception as e:
            logger.error(f"  ➜ 写入 p115_mediainfo_cache 失败: {e}", exc_info=True)
            return False

    @staticmethod
    def get_mediainfo_cache_text(sha1):
        """从本地 p115_mediainfo_cache 读取 JSON 原文，用于直接生成 -mediainfo.json 文件"""
        if not sha1:
            return None

        try:
            sha1 = str(sha1).upper()
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT mediainfo_json::text AS mediainfo_json_text FROM p115_mediainfo_cache WHERE sha1 = %s",
                        (sha1,)
                    )
                    row = cursor.fetchone()
                    return row['mediainfo_json_text'] if row and row.get('mediainfo_json_text') else None
        except Exception as e:
            logger.error(f"  ➜ 读取 p115_mediainfo_cache 失败: {e}")
            return None
        
    @staticmethod
    def _build_etk_context_from_media_metadata(cursor, sha1):
        """
        按 SHA1 从 media_metadata 反查可共享的媒体身份。

        只返回跨账号稳定字段：
        - tmdb_id: Movie 用自身 TMDb；剧集/季/集统一用父剧 TMDb
        - type: movie / tv
        - original_language: 优先子项，兜底父剧
        - season_number / episode_number: Episode 行直接携带，供共享消费端免正则识别季集号
        - sha1
        """
        if not sha1:
            return {}

        sha1 = str(sha1).strip().upper()
        if not sha1:
            return {}

        cursor.execute(
            """
            SELECT
                m.tmdb_id,
                m.item_type,
                m.parent_series_tmdb_id,
                m.season_number,
                m.episode_number,
                COALESCE(NULLIF(m.original_language, ''), NULLIF(p.original_language, '')) AS original_language
            FROM media_metadata m
            LEFT JOIN media_metadata p
              ON p.tmdb_id = m.parent_series_tmdb_id
             AND p.item_type = 'Series'
            WHERE m.file_sha1_json ? %s
            ORDER BY
                CASE m.item_type
                    WHEN 'Movie' THEN 1
                    WHEN 'Episode' THEN 2
                    WHEN 'Season' THEN 3
                    WHEN 'Series' THEN 4
                    ELSE 9
                END,
                m.in_library DESC,
                m.last_updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (sha1,)
        )
        row = cursor.fetchone()
        if not row:
            return {}

        item_type = str(row.get('item_type') or '').strip()
        item_type_lower = item_type.lower()

        if item_type_lower == 'movie':
            tmdb_id = row.get('tmdb_id')
            media_type = 'movie'
        elif item_type_lower in ['series', 'season', 'episode']:
            tmdb_id = row.get('parent_series_tmdb_id') or row.get('tmdb_id')
            media_type = 'tv'
        else:
            return {}

        def _ctx_int(value):
            try:
                if value in (None, ''):
                    return None
                return int(float(value))
            except Exception:
                return None

        ctx = {
            'tmdb_id': str(tmdb_id).strip() if tmdb_id not in [None, ''] else None,
            'type': media_type,
            'original_language': str(row.get('original_language')).strip() if row.get('original_language') not in [None, ''] else None,
            'season_number': _ctx_int(row.get('season_number')),
            'episode_number': _ctx_int(row.get('episode_number')),
            'sha1': sha1,
        }
        return {k: v for k, v in ctx.items() if v not in [None, '', [], {}]}

    @staticmethod
    def _raw_ffprobe_has_useful_etk(raw_ffprobe_json):
        """判断 raw_ffprobe_json 顶层 _etk 是否已经具备核心身份字段。"""
        if not isinstance(raw_ffprobe_json, dict):
            return False

        ctx = raw_ffprobe_json.get('_etk')
        if not isinstance(ctx, dict):
            return False

        return bool(ctx.get('tmdb_id') and ctx.get('type'))

    @staticmethod
    def get_raw_ffprobe_cache(sha1):
        """
        从 p115_mediainfo_cache 读取 raw_ffprobe_json。

        兼容旧缓存：
        - 如果 raw_ffprobe_json 没有顶层 _etk，或 _etk 缺少核心字段；
        - 就按 SHA1 从 media_metadata 本地反查 tmdb_id / type / original_language；
        - 自动写回 p115_mediainfo_cache，避免重新拉 115 直链 ffprobe。
        """
        if not sha1:
            return None

        sha1 = str(sha1).strip().upper()
        if not sha1:
            return None

        try:
            from psycopg2.extras import Json

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT raw_ffprobe_json FROM p115_mediainfo_cache WHERE sha1 = %s",
                        (sha1,)
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None

                    raw_probe = row.get('raw_ffprobe_json')
                    if not raw_probe:
                        local_ctx = P115CacheManager._build_etk_context_from_media_metadata(cursor, sha1)
                        if not local_ctx:
                            return None

                        raw_probe = {'_etk': local_ctx}
                        cursor.execute(
                            """
                            UPDATE p115_mediainfo_cache
                            SET raw_ffprobe_json = %s
                            WHERE sha1 = %s
                            """,
                            (Json(raw_probe, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1)
                        )
                        conn.commit()
                        logger.debug(
                            f"  ➜ [raw_ffprobe缓存] raw 为空，已从 media_metadata 创建最小 _etk: "
                            f"sha1={sha1[:12]}..., tmdb={local_ctx.get('tmdb_id')}, "
                            f"type={local_ctx.get('type')}, lang={local_ctx.get('original_language')}"
                        )
                        return raw_probe

                    try:
                        if isinstance(raw_probe, str):
                            raw_probe = json.loads(raw_probe)
                    except Exception:
                        return raw_probe

                    if not isinstance(raw_probe, dict):
                        return raw_probe

                    # 顺手清理旧缓存里的 115cdn 过期直链 / PC 码等非共享字段。
                    raw_probe = P115CacheManager._sanitize_raw_ffprobe_for_cache(raw_probe)

                    ctx = raw_probe.get('_etk') if isinstance(raw_probe.get('_etk'), dict) else {}
                    need_backfill = not P115CacheManager._raw_ffprobe_has_useful_etk(raw_probe)

                    # 即使已有 _etk，也允许补齐缺失的 original_language / sha1 / 季集号。
                    if not ctx.get('original_language') or not ctx.get('sha1'):
                        need_backfill = True
                    if str(ctx.get('type') or '').strip().lower() in ('tv', 'series', 'season', 'episode'):
                        if ctx.get('season_number') in (None, '') or ctx.get('episode_number') in (None, ''):
                            need_backfill = True

                    if need_backfill:
                        local_ctx = P115CacheManager._build_etk_context_from_media_metadata(cursor, sha1)
                        if local_ctx:
                            clean_ctx = {k: v for k, v in ctx.items() if v not in [None, '', [], {}]}
                            if P115CacheManager._raw_ffprobe_has_useful_etk(raw_probe):
                                # 已有可用身份时，尊重旧 _etk，仅补齐缺失字段。
                                merged_ctx = {}
                                merged_ctx.update(local_ctx)
                                merged_ctx.update(clean_ctx)
                            else:
                                # 旧 _etk 缺少 tmdb_id/type 时，以 media_metadata 本地事实为准。
                                merged_ctx = {}
                                merged_ctx.update(clean_ctx)
                                merged_ctx.update(local_ctx)

                            # sha1 必须以当前查询值为准。
                            merged_ctx['sha1'] = sha1
                            raw_probe['_etk'] = {k: v for k, v in merged_ctx.items() if v not in [None, '', [], {}]}
                            raw_probe = P115CacheManager._sanitize_raw_ffprobe_for_cache(raw_probe)

                            cursor.execute(
                                """
                                UPDATE p115_mediainfo_cache
                                SET raw_ffprobe_json = %s
                                WHERE sha1 = %s
                                """,
                                (Json(raw_probe, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1)
                            )
                            conn.commit()

                            logger.debug(
                                f"  ➜ [raw_ffprobe缓存] 已从 media_metadata 自动补齐 _etk: "
                                f"sha1={sha1[:12]}..., tmdb={raw_probe.get('_etk', {}).get('tmdb_id')}, "
                                f"type={raw_probe.get('_etk', {}).get('type')}, "
                                f"lang={raw_probe.get('_etk', {}).get('original_language')}"
                            )

                    return raw_probe

        except Exception as e:
            logger.debug(f"  ➜ 读取 raw_ffprobe 缓存失败: {sha1[:12]}... -> {e}")
            return None


    @staticmethod
    def patch_raw_ffprobe_etk_context(
        sha1,
        *,
        tmdb_id=None,
        media_type=None,
        original_language=None,
        season_number=None,
        episode_number=None,
        force_identity=False,
    ):
        """回填/修复 raw_ffprobe_json 顶层 _etk 媒体身份。

        raw_ffprobe_json 的生成可能早于整理链路最终确认 TMDb 与季集号；
        如果第一次识别错了，_etk.tmdb_id/type 也会被污染。手动重组时需要用
        用户最终指定的 TMDb 身份强制覆盖，避免后续共享 RAW / 再识别继续吃到旧错误。
        """
        if not sha1:
            return False

        sha1 = str(sha1).strip().upper()
        if not sha1:
            return False

        def _ctx_int(value):
            try:
                if value in (None, ''):
                    return None
                return int(float(value))
            except Exception:
                return None

        def _normalize_type(value):
            text = str(value or '').strip().lower()
            if text in {'movie', 'movies', 'film', '电影'}:
                return 'movie'
            if text in {'tv', 'series', 'season', 'episode', '电视剧', '剧集', '季', '集', '分集'}:
                return 'tv'
            return None

        patch = {'sha1': sha1}

        if tmdb_id not in (None, ''):
            patch['tmdb_id'] = str(tmdb_id).strip()

        normalized_type = _normalize_type(media_type)
        if normalized_type:
            patch['type'] = normalized_type

        if original_language not in (None, ''):
            patch['original_language'] = str(original_language).strip()

        sn = _ctx_int(season_number)
        en = _ctx_int(episode_number)
        if sn is not None:
            patch['season_number'] = sn
        if en is not None:
            patch['episode_number'] = en

        # 即使只有 sha1，也允许写回 _etk.sha1。旧缓存可能已有 tmdb_id/type，
        # 但缺少 sha1；共享 RAW 上传前应把这个稳定身份字段补齐。

        try:
            from psycopg2.extras import Json

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT raw_ffprobe_json FROM p115_mediainfo_cache WHERE sha1 = %s",
                        (sha1,)
                    )
                    row = cursor.fetchone()
                    if not row:
                        return False

                    raw_probe = row.get('raw_ffprobe_json')

                    if raw_probe:
                        try:
                            if isinstance(raw_probe, str):
                                raw_probe = json.loads(raw_probe)
                        except Exception:
                            return False

                        if not isinstance(raw_probe, dict):
                            return False
                    else:
                        # 老缓存可能只有 mediainfo_json 没有 RAW；重组时至少补一个最小 _etk，
                        # 这样后续上传中心仍能携带被用户纠正后的媒体身份。
                        raw_probe = {}

                    raw_probe = P115CacheManager._sanitize_raw_ffprobe_for_cache(raw_probe)
                    ctx = raw_probe.get('_etk') if isinstance(raw_probe.get('_etk'), dict) else {}
                    ctx = {k: v for k, v in ctx.items() if v not in [None, '', [], {}]}
                    changed = False

                    def _put(key, value, *, force=False):
                        nonlocal changed
                        if value in (None, '', [], {}):
                            return
                        old_value = ctx.get(key)
                        if force or old_value in (None, '', [], {}):
                            if old_value != value:
                                ctx[key] = value
                                changed = True

                    # sha1 永远以当前缓存键为准。
                    if ctx.get('sha1') != sha1:
                        ctx['sha1'] = sha1
                        changed = True

                    # 手动重组时，用户指定的 TMDb 身份拥有最高可信度，必须覆盖旧污染。
                    _put('tmdb_id', patch.get('tmdb_id'), force=force_identity)
                    _put('type', patch.get('type'), force=force_identity)
                    _put('original_language', patch.get('original_language'), force=force_identity)

                    # 季集号属于文件级身份；整理链路落定后可以直接覆盖旧值。
                    for key in ('season_number', 'episode_number'):
                        value = patch.get(key)
                        if value is not None and ctx.get(key) != value:
                            ctx[key] = value
                            changed = True

                    if not changed:
                        # 手动重组即使本地 RAW 已经是正确身份，也可能只是之前已修过本地，
                        # 中心对象存储里仍然是旧污染 RAW；继续把对应分享项标脏，触发覆盖上传。
                        if force_identity:
                            P115CacheManager.mark_shared_raw_dirty_for_sha1(
                                sha1,
                                reason='manual_reorganize_raw_etk_verified',
                                tmdb_id=patch.get('tmdb_id'),
                                media_type=patch.get('type'),
                            )
                        return True

                    raw_probe['_etk'] = {k: v for k, v in ctx.items() if v not in [None, '', [], {}]}
                    raw_probe = P115CacheManager._sanitize_raw_ffprobe_for_cache(raw_probe)

                    cursor.execute(
                        """
                        UPDATE p115_mediainfo_cache
                        SET raw_ffprobe_json = %s
                        WHERE sha1 = %s
                        """,
                        (Json(raw_probe, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1)
                    )
                    conn.commit()

            P115CacheManager.mark_shared_raw_dirty_for_sha1(
                sha1,
                reason='raw_ffprobe_etk_context_fixed',
                tmdb_id=patch.get('tmdb_id'),
                media_type=patch.get('type'),
            )

            ctx_log = raw_probe.get('_etk', {}) if isinstance(raw_probe, dict) else {}
            logger.debug(
                f"  ➜ [raw_ffprobe缓存] 已修复 _etk: sha1={sha1[:12]}..., "
                f"tmdb={ctx_log.get('tmdb_id')}, type={ctx_log.get('type')}, "
                f"season={ctx_log.get('season_number')}, episode={ctx_log.get('episode_number')}, "
                f"force_identity={force_identity}"
            )
            return True
        except Exception as e:
            logger.debug(f"  ➜ 修复 raw_ffprobe _etk 失败: {sha1[:12]}... -> {e}")
            return False

    @staticmethod
    def mark_shared_raw_dirty_for_sha1(sha1, *, reason='raw_ffprobe_etk_context_fixed', tmdb_id=None, media_type=None):
        """本地 RAW 被修正后，标记 Rapid v2 本地共享索引需要重新上传 RAW。"""
        sha1 = str(sha1 or '').strip().upper()
        if not sha1:
            return 0

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    dirty_payload = {
                        'raw_etk_dirty': {
                            'pending': True,
                            'reason': reason,
                            'sha1': sha1,
                            'tmdb_id': str(tmdb_id or '').strip() or None,
                            'type': str(media_type or '').strip() or None,
                            'marked_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                        }
                    }
                    cursor.execute(
                        """
                        UPDATE shared_rapid_source_files
                        SET raw_ffprobe_uploaded = FALSE,
                            raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE UPPER(COALESCE(sha1, '')) = %s
                        RETURNING local_source_id
                        """,
                        (json.dumps(dirty_payload, ensure_ascii=False), sha1),
                    )
                    local_source_ids = sorted({r.get('local_source_id') for r in cursor.fetchall() if r.get('local_source_id') is not None})

                    if local_source_ids:
                        cursor.execute(
                            """
                            UPDATE shared_rapid_sources
                            SET center_status = 'dirty_raw',
                                last_error = %s,
                                raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                                updated_at = NOW()
                            WHERE id = ANY(%s)
                              AND COALESCE(status, '') NOT IN ('disabled', 'deleted')
                            """,
                            (
                                f'本地 RAW _etk 已修复，等待覆盖中心 RAW：{sha1[:12]}...',
                                json.dumps(dirty_payload, ensure_ascii=False),
                                local_source_ids,
                            ),
                        )
                    conn.commit()

            if local_source_ids:
                logger.info(
                    f"  ➜ [raw_ffprobe缓存] 已标记 {len(local_source_ids)} 个 Rapid 共享源重新上传 RAW："
                    f"sha1={sha1[:12]}..., reason={reason}"
                )
            return len(local_source_ids)
        except Exception as e:
            logger.debug(f"  ➜ 标记 Rapid 共享 RAW 重传失败: sha1={sha1[:12]}... -> {e}")
            return 0

# ======================================================================
# ★★★ 115 整理记录 DB 管理器 ★★★
# ======================================================================
class P115RecordManager:
    @staticmethod
    def add_or_update_record(file_id, original_name, status, tmdb_id=None, media_type=None, target_cid=None, category_name=None, renamed_name=None, pick_code=None, season_number=None, fail_reason=None):
        """添加或更新整理记录（基于 file_id 和 pick_code 唯一约束，智能继承原名）"""
        if not file_id or not original_name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if pick_code:
                        cursor.execute("SELECT file_id, original_name FROM p115_organize_records WHERE pick_code = %s", (pick_code,))
                        row = cursor.fetchone()
                        if row:
                            old_file_id = row['file_id']
                            original_name = row['original_name'] 
                            if str(old_file_id) != str(file_id):
                                cursor.execute("DELETE FROM p115_organize_records WHERE file_id = %s", (old_file_id,))

                    cursor.execute("""
                        INSERT INTO p115_organize_records 
                        (file_id, pick_code, original_name, status, tmdb_id, media_type, target_cid, category_name, renamed_name, processed_at, season_number, fail_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                        ON CONFLICT (file_id) 
                        DO UPDATE SET 
                            pick_code = EXCLUDED.pick_code,
                            status = EXCLUDED.status,
                            tmdb_id = EXCLUDED.tmdb_id,
                            media_type = EXCLUDED.media_type,
                            target_cid = EXCLUDED.target_cid,
                            category_name = EXCLUDED.category_name,
                            renamed_name = EXCLUDED.renamed_name,
                            processed_at = NOW(),
                            season_number = EXCLUDED.season_number,
                            fail_reason = EXCLUDED.fail_reason
                    """, (str(file_id), pick_code, str(original_name), str(status), str(tmdb_id) if tmdb_id else None, 
                          str(media_type) if media_type else None, str(target_cid) if target_cid else None, 
                          str(category_name) if category_name else None, str(renamed_name) if renamed_name else None, season_number, fail_reason))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 整理记录失败: {e}")
    @staticmethod
    def delete_records(file_ids):
        """批量删除整理记录 (用于洗版替换时清理旧记录)"""
        if not file_ids: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法批量删除
                    cursor.execute("DELETE FROM p115_organize_records WHERE file_id = ANY(%s)", (list(file_ids),))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 整理记录失败: {e}")

# ======================================================================
# ★★★ 115 全局批量删除缓冲队列 (极简暴力清理版) ★★★
# ======================================================================
class P115DeleteBuffer:
    _lock = threading.Lock()
    _fids_to_delete = set()
    _cids_to_check = set()
    _check_save_path = False # ★ 新增：是否检查待整理根目录
    _timer = None
    _last_add_time = 0

    @classmethod
    def add(cls, fids=None, base_cids=None, check_save_path=False):
        with cls._lock:
            if fids:
                cls._fids_to_delete.update(fids)
            if base_cids:
                if isinstance(base_cids, (list, set)):
                    cls._cids_to_check.update(base_cids)
                else:
                    cls._cids_to_check.add(base_cids)
            if check_save_path:
                cls._check_save_path = True

            # ★ 核心防抖：每次有新文件整理完，刷新倒计时
            cls._last_add_time = time.time()
            if cls._timer is None:
                cls._timer = spawn_later(5.0, cls._check_and_flush)

    @classmethod
    def _check_and_flush(cls):
        with cls._lock:
            now = time.time()
            # ★ 智能防抖：如果距离最后一次整理还不到 10 秒，说明大部队还在干活，继续等！
            if now - cls._last_add_time < 10.0:
                cls._timer = spawn_later(10.0 - (now - cls._last_add_time), cls._check_and_flush)
                return
            
            fids = list(cls._fids_to_delete)
            cids = list(cls._cids_to_check)
            check_save = cls._check_save_path
            
            cls._fids_to_delete.clear()
            cls._cids_to_check.clear()
            cls._check_save_path = False
            cls._timer = None

        client = P115Service.get_client()
        if not client: return

        # =================================================================
        # ★ 核心修改：直接拉取“待整理”目录下的所有一级子目录加入死刑检查名单
        # =================================================================
        config = get_config()
        if check_save:
            save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            unidentified_name = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME, "未识别")
            if save_path and str(save_path) != '0':
                try:
                    res = client.fs_files({'cid': save_path, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    for item in res.get('data', []):
                        if str(item.get('fc') or item.get('type')) == '0':
                            sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                            sub_cid = item.get('fid') or item.get('file_id')
                            # 排除“未识别”目录，其他的全部拉进去检查
                            if sub_name != unidentified_name and sub_cid:
                                cids.append(sub_cid)
                except Exception as e:
                    logger.error(f"  ➜ 获取待整理目录子项失败: {e}")

        # 去重
        cids = list(set(cids))

        if not fids and not cids:
            return

        def _safe_batch_delete(ids, is_dir=False):
            if not ids: return []
            item_type = "目录" if is_dir else "文件"
            max_retries = 3
            
            for attempt in range(max_retries):
                resp = client.fs_delete(ids)
                if resp.get('state'):
                    return ids
                
                if resp.get('code') in [770004, 990001]:
                    logger.error(f"  🛑 [触发流控] 115 API 提示达到访问上限 ({resp.get('code')})，立即终止本次删除任务！")
                    return [] 

                logger.error(f"  ➜ [批量销毁] 115 删除{item_type}失败 (第 {attempt + 1}/{max_retries} 次): {resp}")
                if attempt < max_retries - 1:
                    time.sleep(3)
            
            logger.warning(f"  ➜ [批量销毁] 批量删除彻底失败，放弃本次清理。")
            return []

        # 1. 删除明确指定的文件
        if fids:
            logger.info(f"  ➜ [批量销毁] 缓冲期结束，正在删除 {len(fids)} 个文件...")
            success_fids = _safe_batch_delete(fids, is_dir=False)
            if success_fids:
                P115CacheManager.delete_files(success_fids)

        # 2. 获取免死金牌名单
        protected_cids = {'0'}

        media_root = config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID)
        if media_root:
            protected_cids.add(str(media_root))

        save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
        if save_path:
            protected_cids.add(str(save_path))

        # ★★★ 核心修复：保护“未识别”目录 (三重保险) ★★★
        unidentified_name = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME, "未识别")
        
        # 保险 1：优先读取用户明确配置的 CID
        explicit_un_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        if explicit_un_cid and str(explicit_un_cid) != '0':
            protected_cids.add(str(explicit_un_cid))
            
        # 保险 2 & 3：如果没配置，尝试通过待整理目录推导
        if save_path and str(save_path) != '0' and unidentified_name:
            # 保险 2：查本地缓存
            unidentified_cid = P115CacheManager.get_cid(str(save_path), unidentified_name)
            if unidentified_cid:
                protected_cids.add(str(unidentified_cid))
            else:
                # 保险 3：缓存穿透时，直接查 115 API (绝对兜底)
                try:
                    search_res = client.fs_files({'cid': save_path, 'search_value': unidentified_name, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    for item in search_res.get('data', []):
                        if item.get('fn') == unidentified_name and str(item.get('fc') if item.get('fc') is not None else item.get('type')) == '0':
                            protected_cids.add(str(item.get('fid') or item.get('file_id')))
                            break
                except Exception:
                    pass

        raw_rules = settings_db.get_setting('p115_sorting_rules')
        if raw_rules:
            rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
            for rule in rules:
                if rule.get('cid'):
                    protected_cids.add(str(rule['cid']))


        def _protect_ancestors(cid):
            """把指定目录的所有父级目录加入免死名单，防止 ETK 这种管理根目录被 GC 删掉。"""
            current = str(cid or '')

            for _ in range(20):
                if not current or current == '0':
                    break

                # 1. 优先查本地缓存
                node = P115CacheManager.get_node_info(current)
                parent_id = None
                
                if node and node.get('parent_id'):
                    parent_id = str(node.get('parent_id'))
                else:
                    # 2. ★ 核心修复：缓存穿透时，直接查 115 API 溯源 (终极防线)
                    try:
                        info_res = client.fs_get_info(current)
                        if info_res and info_res.get('state') and info_res.get('data'):
                            parent_id = str(info_res['data'].get('parent_id') or info_res['data'].get('cid') or '')
                    except Exception:
                        pass

                if not parent_id or parent_id == '0':
                    break

                protected_cids.add(parent_id)
                current = parent_id


        for safe_cid in list(protected_cids):
            _protect_ancestors(safe_cid)

        # 3. 检查空目录
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        media_exts = allowed_exts | {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg', 'mp3', 'flac', 'wav', 'ape', 'm4a', 'aac', 'ogg'}

        empty_cids_to_delete = []

        for cid in cids:
            if str(cid) in protected_cids: continue
            
            media_count = 0
            def count_media(current_cid):
                nonlocal media_count
                for attempt in range(3):
                    try:
                        res = client.fs_files({'cid': current_cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                        for item in res.get('data', []):
                            if str(item.get('fc')) == '1':
                                ext = str(item.get('fn', '')).split('.')[-1].lower()
                                if ext in media_exts:
                                    item_size = _parse_115_size(item.get('fs') or item.get('size'))
                                    if item_size == 0 or item_size > 10 * 1024 * 1024:
                                        media_count += 1
                            elif str(item.get('fc')) == '0':
                                count_media(item.get('fid'))
                        return 
                    except Exception as e:
                        if attempt == 2:
                            media_count += 999 
                        time.sleep(1)

            count_media(cid)
            # ★ 只要没有媒体文件（哪怕里面有一堆 nfo 和 jpg），统统判定为空目录！
            if media_count == 0:
                empty_cids_to_delete.append(cid)
                logger.debug(f"  ➜ 判定为空目录，加入待清理队列: CID {cid}")

        # 4. 批量删除空目录
        if empty_cids_to_delete:
            logger.debug(f"  ➜ [批量清理] 正在向 115 发送批量删除空目录指令 ({len(empty_cids_to_delete)} 个)...")
            success_cids = _safe_batch_delete(empty_cids_to_delete, is_dir=True)
            if success_cids:
                for cid in success_cids:
                    P115CacheManager.delete_cid(cid)
                logger.info(f"  ➜ [批量清理] 成功删除了 {len(success_cids)} 个空目录。")

    @classmethod
    def flush(cls):
        """兼容老接口调用"""
        cls._check_and_flush()

def get_config():
    return config_manager.APP_CONFIG


class SmartOrganizer(P115MediaAnalyzerMixin):
    _P115_INVALID_NAME_CHARS_RE = re.compile(r'[\\/:*?"<>|]')

    def __init__(self, client, tmdb_id, media_type, original_title, ai_translator=None, use_ai=False, recognition_hints=None):
        self.client = client
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_title = original_title
        self.ai_translator = ai_translator # 新增
        self.use_ai = use_ai
        self.api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        self.forced_season = None
        self.studio_map = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
        self.keyword_map = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
        self.rating_map = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
        self.rating_priority = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY
        self.country_map = settings_db.get_setting('country_mapping') or utils.DEFAULT_COUNTRY_MAPPING
        self.language_map = settings_db.get_setting('language_mapping') or utils.DEFAULT_LANGUAGE_MAPPING
        self.recognition_hints = candidate_to_recognition_hints(recognition_hints or {})

        self.raw_metadata = self._fetch_raw_metadata()
        self.details = self.raw_metadata
        self.rename_config = settings_db.get_setting('p115_rename_config') or {
            "main_title_lang": "zh", "main_year_en": True, "main_tmdb_fmt": "{tmdb=ID}",
            "season_fmt": "Season {02}", "file_title_lang": "zh", "file_year_en": False,
            "file_tmdb_fmt": "none", "file_params_en": True, "file_sep": " - ",
            "strm_url_fmt": "standard"
        }
        raw_rules = settings_db.get_setting('p115_sorting_rules')
        self.rules = []
        
        if raw_rules:
            if isinstance(raw_rules, list):
                self.rules = raw_rules
            elif isinstance(raw_rules, str):
                try:
                    self.rules = json.loads(raw_rules)
                except Exception as e:
                    logger.error(f"  ➜ 解析 115 分类规则失败: {e}")
                    self.rules = []

    def _fetch_raw_metadata(self):
        """
        获取 TMDb 原始元数据 (ID/Code)，不进行任何中文转换。
        """
        if not self.api_key: return {}
        
        # 读取内存缓存
        cache_key = f"{self.media_type}_{self.tmdb_id}"
        if cache_key in _TMDB_METADATA_CACHE:
            return _TMDB_METADATA_CACHE[cache_key]

        data = {
            'genre_ids': [],
            'country_codes': [],
            'lang_code': None,
            'company_ids': [],
            'network_ids': [],
            'keyword_ids': [],
            'rating_label': '未知' # 分级是特例，必须计算出标签才能匹配
        }

        try:
            raw_details = {}
            if self.media_type == 'tv':
                raw_details = tmdb.get_tv_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,content_ratings,networks,credits,alternative_titles"
                )
            else:
                raw_details = tmdb.get_movie_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,release_dates,credits,alternative_titles"
                )

            if not raw_details: return {}

            # 1. 基础 ID/Code 提取
            data['genre_ids'] = [g.get('id') for g in raw_details.get('genres', [])]
            data['country_codes'] = [c.get('iso_3166_1') for c in raw_details.get('production_countries', [])]
            if not data['country_codes'] and raw_details.get('origin_country'):
                data['country_codes'] = raw_details.get('origin_country')

            data['lang_code'] = raw_details.get('original_language')

            data['company_ids'] = [c.get('id') for c in raw_details.get('production_companies', [])]
            data['network_ids'] = [n.get('id') for n in raw_details.get('networks', [])] if self.media_type == 'tv' else []

            # 2. 关键词 ID 提取
            kw_container = raw_details.get('keywords', {})
            raw_kw_list = kw_container.get('keywords', []) if self.media_type == 'movie' else kw_container.get('results', [])
            data['keyword_ids'] = [k.get('id') for k in raw_kw_list]

            # 3. 分级计算 
            data['rating_label'] = utils.get_rating_label(
                raw_details,
                self.media_type,
                self.rating_map,
                self.rating_priority
            )

            # 4. 演员提取
            # 只取前 3 名主演，避免客串演员乱入导致规则匹配不准确
            data['actor_ids'] = [cast.get('id') for cast in raw_details.get('credits', {}).get('cast', [])[:3]]

            # =====================================================================
            # ★★★ 5. 标题提取 (本地缓存优先 -> 隐身符清洗 -> 广告拦截 -> 别名兜底) ★★★
            # =====================================================================
            cached_title = None
            cached_original_title = None
            original_title = None
            authoritative_title_hint = None

            normalized_hints = candidate_to_recognition_hints(getattr(self, 'recognition_hints', {}) or {})
            if _is_authoritative_recognition_hint(normalized_hints):
                authoritative_title_hint = (
                    normalized_hints.get('identify_title')
                    or normalized_hints.get('clean_title')
                    or normalized_hints.get('title')
                )
            
            # 5.1 优先查询本地数据库缓存 (免疫 TMDb 后期篡改，保持网盘与 Emby 绝对一致)
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        db_item_type = 'Movie' if self.media_type == 'movie' else 'Series'
                        cursor.execute(
                            "SELECT title, original_title FROM media_metadata WHERE tmdb_id = %s AND item_type = %s",
                            (str(self.tmdb_id), db_item_type)
                        )
                        row = cursor.fetchone()
                        if row and row['title']:
                            cached_title = row['title']
                            cached_original_title = row['original_title']
            except Exception as e:
                logger.warning(f"  ➜ [115整理] 查询本地标题缓存失败: {e}")

            cached_title_conflicts_with_authority = False
            if cached_title and authoritative_title_hint:
                cached_norm = normalize_title_for_match(cached_title)
                authoritative_norm = normalize_title_for_match(authoritative_title_hint)
                cached_title_conflicts_with_authority = bool(
                    cached_norm and authoritative_norm and cached_norm != authoritative_norm
                )

            if cached_title and not cached_title_conflicts_with_authority:
                logger.info(f"  ➜ [115整理] 命中本地数据库片名: '{cached_title}'，无视 TMDb 最新变动。")
                current_title = cached_title
                original_title = cached_original_title or cached_title
            else:
                if cached_title_conflicts_with_authority:
                    logger.info(
                        f"  ➜ [115整理] 权威识别标题 '{authoritative_title_hint}' 与本地数据库片名 '{cached_title}' 冲突，"
                        f"优先使用当前 TMDb 标题。"
                    )
                # 5.2 本地无缓存 (首次入库)，走 TMDb 提取与清洗流程
                raw_title = raw_details.get('title') or raw_details.get('name')
                current_title = utils.clean_invisible_chars(raw_title)
                
                if utils.is_spam_title(current_title):
                    logger.warning(f"  ➜ [115整理] 拦截到恶意广告片名: '{current_title}'，准备寻找干净的别名...")
                    current_title = ""

                if not current_title or not utils.contains_chinese(current_title):
                    chinese_alias = None
                    alt_titles_data = raw_details.get("alternative_titles", {})
                    alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                    
                    priority_map = {"CN": 1, "SG": 2, "TW": 3, "HK": 4}
                    best_priority = 99
                    
                    for alt in alt_list:
                        alt_title = utils.clean_invisible_chars(alt.get("title", ""))
                        if utils.contains_chinese(alt_title) and not utils.is_spam_title(alt_title):
                            iso_country = alt.get("iso_3166_1", "").upper()
                            current_priority = priority_map.get(iso_country, 5) 
                            
                            if current_priority < best_priority:
                                chinese_alias = alt_title
                                best_priority = current_priority
                                
                            if best_priority == 1:
                                break 
                    
                    if chinese_alias:
                        logger.info(f"  ➜ [115整理] 发现干净的 TMDb 官方中文别名: '{chinese_alias}'")
                        current_title = chinese_alias
                    else:
                        raw_original = raw_details.get("original_title") or raw_details.get("original_name")
                        original_title = utils.clean_invisible_chars(raw_original)
                        logger.info(f"  ➜ [115整理] 未找到干净的中文别名，回退到原名: '{original_title}'")
                        current_title = original_title
                else:
                    # 如果主标题正常，提取原名
                    raw_original = raw_details.get("original_title") or raw_details.get("original_name")
                    original_title = utils.clean_invisible_chars(raw_original)

            data['title'] = current_title
            data['original_title'] = original_title

            # ★★★ 尝试提取纯英文名 (title_en) ★★★
            english_title = None
            # 如果原名本身就是英文，直接用原名
            if original_title and not utils.contains_chinese(original_title) and re.match(r'^[a-zA-Z0-9\s\-_:\.,!\?\'"&]+$', original_title):
                english_title = original_title
            else:
                # 否则去别名里找美国的别名
                alt_titles_data = raw_details.get("alternative_titles", {})
                alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                for alt in alt_list:
                    if alt.get("iso_3166_1", "").upper() == "US":
                        english_title = utils.clean_invisible_chars(alt.get("title", ""))
                        break
            
            # 存入 data 供后续调用
            data['title_en'] = english_title or original_title # 兜底用原名
            
            # 提取年份
            date_str = raw_details.get('release_date') or raw_details.get('first_air_date')
            data['date'] = date_str
            data['year'] = 0
            if date_str and len(str(date_str)) >= 4:
                try:
                    data['year'] = int(str(date_str)[:4])
                except: 
                    pass
            
            # 补充评分供规则匹配
            data['vote_average'] = raw_details.get('vote_average', 0)
            
            # ★ 补充时长供规则匹配
            if self.media_type == 'movie':
                data['runtime'] = raw_details.get('runtime', 0)
            else:
                data['episode_run_time'] = raw_details.get('episode_run_time', [])

            # ★ 补充季集数据，供动漫绝对集数推算使用
            data['seasons'] = raw_details.get('seasons', [])
            data['last_episode_to_air'] = raw_details.get('last_episode_to_air', {})

            _TMDB_METADATA_CACHE[cache_key] = data # 写入缓存

            return data

        except Exception as e:
            logger.warning(f"  ➜ [整理] 获取原始元数据失败: {e}", exc_info=True)
            return {}

    def _match_rule(self, rule):
        """
        规则匹配逻辑 (支持 AND / OR 复合匹配)
        """
        if not self.raw_metadata: return False

        # ==========================================
        # 1. 绝对前置过滤条件 (必须满足，无视 AND/OR)
        # ==========================================
        # 媒体类型 (电影/剧集) 是硬性分类，必须优先满足
        if rule.get('media_type') and rule['media_type'] != 'all':
            if rule['media_type'] != self.media_type: return False

        # ★★★ 核心重构：追剧状态的主动判定与分季隔离 ★★★
        if rule.get('watching_status') == 'watching' and self.media_type == 'tv':
            try:
                from database.watchlist_db import get_watching_tmdb_ids, get_season_watching_status
                
                season_num = getattr(self, 'forced_season', None)
                
                if season_num is not None:
                    # 1. 优先查本地数据库 (速度最快)
                    season_status = get_season_watching_status(self.tmdb_id, season_num)
                    
                    if season_status in ['Watching', 'Paused', 'Pending']:
                        # 明确在追，直接放行，无需查 TMDb
                        pass 
                    elif season_status == 'Completed':
                        # 明确完结，直接拦截
                        logger.debug(f"  🛑 [规则拦截] '第 {season_num} 季' 真实状态为 'Completed'，跳过连载规则。")
                        return False
                    else:
                        logger.info(f"  ➜ 数据库状态为 '{season_status or '空'}'，正在向 TMDb 实时查询 '第 {season_num} 季' 的连载状态...")

                        is_airing = helpers.check_series_completion(
                            self.tmdb_id,
                            self.api_key,
                            season_number=season_num,
                            series_name=getattr(self, "title", "未知剧集"),
                            mode="airing"
                            )
                        
                        if is_airing:
                            logger.info(f"  ➜ [连载判定] 确认 '第 {season_num} 季' 正在连载，命中连载规则！")
                            # 既然是连载，就让它继续往下走，命中规则
                        else:
                            logger.debug(f"  🛑 [连载判定] 确认 '第 {season_num} 季' 已完结，跳过连载规则。")
                            return False
                else:
                    # 没提取到季号，退化为查整部剧的状态
                    watching_ids = get_watching_tmdb_ids()
                    if str(self.tmdb_id) not in watching_ids:
                        return False
            except Exception as e:
                logger.warning(f"获取追剧状态失败: {e}")
                return False

        # ==========================================
        # 2. 动态条件匹配 (根据 match_mode 决定 AND 或 OR)
        # ==========================================
        match_mode = rule.get('match_mode', 'and')
        conditions_configured = 0  # 记录配置了多少个条件
        conditions_met = 0         # 记录满足了多少个条件

        def _evaluate(is_match):
            nonlocal conditions_configured, conditions_met
            conditions_configured += 1
            if is_match:
                conditions_met += 1

        # 2.1 类型 (Genres)
        if rule.get('genres'):
            rule_ids = [int(x) for x in rule['genres']]
            tmdb_genre_ids = self.raw_metadata.get('genre_ids', [])
            _evaluate(any(gid in rule_ids for gid in tmdb_genre_ids))

        # 2.2 国家 (Countries)
        if rule.get('countries'):
            target_codes = set()
            for item in rule['countries']:
                # 尝试在映射表中找中文标签
                mapping = next((m for m in self.country_map if m['label'] == item), None)
                if mapping:
                    target_codes.add(mapping['value'])
                    if 'aliases' in mapping:
                        target_codes.update(mapping['aliases'])
                else:
                    # 兼容旧规则（直接存了代码的情况）
                    target_codes.add(item)
            
            current_countries = self.raw_metadata.get('country_codes', [])
            _evaluate(any(c in target_codes for c in current_countries))

        # 2.3 语言 (Languages)
        if rule.get('languages'):
            target_codes = set()
            for item in rule['languages']:
                # 尝试在映射表中找中文标签
                mapping = next((m for m in self.language_map if m['label'] == item), None)
                if mapping:
                    target_codes.add(mapping['value'])
                    if 'aliases' in mapping:
                        target_codes.update(mapping['aliases'])
                else:
                    # 兼容旧规则（直接存了代码的情况）
                    target_codes.add(item)
                    
            _evaluate(self.raw_metadata.get('lang_code') in target_codes)

        # 2.4 工作室 (Studios)
        if rule.get('studios'):
            target_ids = set()
            for label in rule['studios']:
                config_item = next((item for item in self.studio_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('company_ids', []))
                    target_ids.update(config_item.get('network_ids', []))

            has_company = any(cid in target_ids for cid in self.raw_metadata.get('company_ids', []))
            has_network = any(nid in target_ids for nid in self.raw_metadata.get('network_ids', []))
            _evaluate(has_company or has_network)

        # 2.5 关键词 (Keywords)
        if rule.get('keywords'):
            target_ids = set()
            for label in rule['keywords']:
                config_item = next((item for item in self.keyword_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('ids', []))

            tmdb_kw_ids = [int(k) for k in self.raw_metadata.get('keyword_ids', [])]
            target_ids_int = [int(k) for k in target_ids]
            _evaluate(any(kid in target_ids_int for kid in tmdb_kw_ids))

        # 2.6 分级 (Rating)
        if rule.get('ratings'):
            _evaluate(self.raw_metadata.get('rating_label') in rule['ratings'])

        # 2.7 年份 (Year)
        year_min = rule.get('year_min')
        year_max = rule.get('year_max')
        if year_min or year_max:
            current_year = self.raw_metadata.get('year', 0)
            if current_year == 0:
                _evaluate(False)
            else:
                is_y_match = True
                if year_min and current_year < int(year_min): is_y_match = False
                if year_max and current_year > int(year_max): is_y_match = False
                _evaluate(is_y_match)

        # 2.8 时长 (Runtime)
        run_min = rule.get('runtime_min')
        run_max = rule.get('runtime_max')
        if run_min or run_max:
            current_runtime = 0
            if self.media_type == 'movie':
                current_runtime = self.details.get('runtime') or 0
            else:
                runtimes = self.details.get('episode_run_time', [])
                if runtimes and len(runtimes) > 0:
                    current_runtime = runtimes[0]

            if current_runtime == 0:
                _evaluate(False)
            else:
                is_r_match = True
                if run_min and current_runtime < int(run_min): is_r_match = False
                if run_max and current_runtime > int(run_max): is_r_match = False
                _evaluate(is_r_match)

        # 2.9 评分 (Min Rating)
        if rule.get('min_rating') and float(rule['min_rating']) > 0:
            vote_avg = self.details.get('vote_average', 0)
            _evaluate(vote_avg >= float(rule['min_rating']))

        # 2.10 演员 (Actors)
        if rule.get('actors'):
            rule_actor_ids = [int(a['id']) for a in rule['actors'] if 'id' in a]
            _evaluate(any(aid in self.raw_metadata.get('actor_ids', []) for aid in rule_actor_ids))

        # 2.11 文件扩展名
        if rule.get('file_extensions'):
            source_name = getattr(self, 'current_sorting_filename', '') or ''
            current_ext = os.path.splitext(str(source_name))[1].lower().lstrip('.')
            rule_exts = {
                str(ext).strip().lower().lstrip('.')
                for ext in rule.get('file_extensions') or []
                if str(ext).strip()
            }
            _evaluate(bool(current_ext and current_ext in rule_exts))

        # ==========================================
        # 3. 最终结果判定
        # ==========================================
        if conditions_configured == 0:
            return True # 没有配置任何条件，默认命中（兜底规则）

        if match_mode == 'or':
            # OR 模式：只要满足了任意一个条件，就算命中
            return conditions_met > 0
        else: 
            # AND 模式：必须满足所有配置的条件
            return conditions_met == conditions_configured

    def get_target_cid(self, ignore_memory=False, season_num=None):
        """获取目标 CID：优先查历史整理记录（记忆手动纠错），其次遍历规则"""
        self.is_from_memory = False # 初始化记忆标记
        # 辅助函数：校验历史 CID 是否仍在当前启用的规则中
        def _is_cid_valid_in_rules(check_cid):
            if not check_cid: return False
            for r in self.rules:
                if str(r.get('cid')) == str(check_cid) and r.get('enabled', True):
                    return True
            return False

        # 文件扩展名是文件级分类条件，必须优先于同 TMDb 的历史整理记忆。
        current_ext = os.path.splitext(str(getattr(self, 'current_sorting_filename', '') or ''))[1].lower().lstrip('.')
        if current_ext:
            for rule in self.rules:
                if not rule.get('enabled', True) or not rule.get('file_extensions'):
                    continue
                rule_exts = {
                    str(ext).strip().lower().lstrip('.')
                    for ext in rule.get('file_extensions') or []
                    if str(ext).strip()
                }
                if current_ext in rule_exts and self._match_rule(rule):
                    logger.info(
                        f"  ➜ [115整理] 命中文件扩展名规则“{rule.get('name')}”，"
                        f"扩展名：{current_ext}，目标目录：{rule.get('dir_name')}"
                    )
                    return rule.get('cid')

        # ★★★ 1. 查历史记录 (记忆功能 - 升级为分季隔离 + 规则校验版) ★★★
        if not ignore_memory:
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        if self.media_type == 'tv' and season_num is not None:
                            # 查找该剧最近的 50 条记录，寻找属于该季的专属记忆
                            cursor.execute("""
                                SELECT target_cid, category_name, renamed_name, original_name 
                                FROM p115_organize_records 
                                WHERE tmdb_id = %s AND status = 'success' 
                                ORDER BY processed_at DESC LIMIT 50
                            """, (str(self.tmdb_id),))
                            rows = cursor.fetchall()
                            import re
                            for row in rows:
                                name_to_check = row['renamed_name'] or row['original_name'] or ""
                                m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', name_to_check)
                                m2 = re.search(r'Season\s*(\d{1,4})\b', name_to_check, re.IGNORECASE)
                                m3 = re.search(r'第\s*(\d{1,4})\s*季', name_to_check)
                                s_val = None
                                if m1: s_val = int(m1.group(1))
                                elif m2: s_val = int(m2.group(1))
                                elif m3: s_val = int(m3.group(1))
                                
                                if s_val == season_num:
                                    history_cid = str(row['target_cid'])
                                    # ★ 核心修复：校验记忆是否失效
                                    if _is_cid_valid_in_rules(history_cid):
                                        logger.info(f"  ➜ [分季记忆体] 第 {season_num} 季曾整理到“{row['category_name']}”，本次沿用该分类。")
                                        logger.debug(f"  ➜ [分季记忆体] 沿用历史目录：CID={history_cid}")
                                        self.is_from_memory = True # 打上记忆命中标记
                                        return history_cid
                                    else:
                                        logger.warning(f"  ➜ [分季记忆体] 历史分类 (CID: {history_cid}) 已不在当前规则中，记忆失效，交由规则引擎重新分配。")
                                        break # 记忆失效，跳出循环走规则
                            
                            logger.debug(f"  ➜ [分季记忆体] 未找到 '第 {season_num} 季' 的有效专属记忆，将使用规则引擎进行分配。")
                        else:
                            # 电影或未提供季号的兜底逻辑
                            cursor.execute("""
                                SELECT target_cid, category_name 
                                FROM p115_organize_records 
                                WHERE tmdb_id = %s AND status = 'success' 
                                ORDER BY processed_at DESC LIMIT 1
                            """, (str(self.tmdb_id),))
                            row = cursor.fetchone()
                            if row and row['target_cid']:
                                history_cid = str(row['target_cid'])
                                # ★ 核心修复：校验记忆是否失效
                                if _is_cid_valid_in_rules(history_cid):
                                    logger.info(f"  ➜ [记忆体] 该媒体曾整理到“{row['category_name']}”，本次沿用该分类。")
                                    logger.debug(f"  ➜ [记忆体] 沿用历史目录：CID={history_cid}")
                                    self.is_from_memory = True # 打上记忆命中标记
                                    return history_cid
                                else:
                                    logger.warning(f"  ➜ [记忆体] 历史分类 (CID: {history_cid}) 已不在当前规则中，记忆失效，交由规则引擎重新分配。")
            except Exception as e:
                logger.warning(f"  ➜ 查询历史整理记录失败: {e}")

        # 2. 遍历规则
        for rule in self.rules:
            if not rule.get('enabled', True): continue
            if self._match_rule(rule):
                logger.info(f"  ➜ [115整理] 命中规则“{rule.get('name')}”，目标目录：{rule.get('dir_name')}")
                return rule.get('cid')
        return None

    @staticmethod
    def _is_special_season_name(text: str) -> bool:
        """
        判断目录名是否代表 TMDb 第 0 季 / Specials。
        注意：必须 fullmatch，避免 SPY x Family 里的 SP 被误判。
        """
        if not text:
            return False

        name = os.path.basename(str(text).replace("\\", "/")).strip()

        return bool(re.fullmatch(
            r'(?:'
            r'specials?|sp|ova|oad|extra(?:s)?|'
            r'特别篇|特別篇|番外(?:篇)?|外传|外傳|'
            r'第\s*0+\s*季|season\s*0+|s0+'
            r')',
            name,
            re.IGNORECASE
        ))

    @classmethod
    def _extract_season_from_path_or_text(cls, text: str):
        """
        从路径或文本里提取季号。
        Specials / SP / OVA / 第0季 统一返回 0。
        Season 00 / S00 / 第0季 也返回 0。
        """
        if not text:
            return None

        normalized = str(text).replace("\\", "/")

        for part in [p.strip() for p in normalized.split("/") if p.strip()]:
            if cls._is_special_season_name(part):
                return 0

        m = re.search(
            r'(?:^|[/\s\.\-_\[\(])(?:Season\s*|S|第)\s*(\d{1,4})(?:季)?(?=$|[/\s\.\-_\]\)])',
            normalized,
            re.IGNORECASE
        )
        if m:
            return int(m.group(1))

        return None

    def _build_name_from_format(self, format_array, is_tv=False, season_num=None, episode_num=None, original_title=None, video_info=None, safe_title=None):
        """解析乐高轨道生成名称 (支持目录和文件，自动过滤特殊字符)"""
        if not format_array: return ""
        
        evaluated = []
        for raw_id in format_array:
            block = raw_id.rsplit('_', 1)[0] if re.search(r'_\d+$', raw_id) else raw_id
            val = None
            is_sep = False
            
            # 标题块统一做 115 非法字符清洗，避免目录/文件名因原文标题中的引号等字符创建失败。
            if block == 'title_zh':
                raw_title = safe_title if safe_title else (self.details.get('title') or self.original_title)
                val = self._sanitize_115_name_component(raw_title)
            elif block == 'title_en':
                raw_title = self.details.get('title_en') or original_title or self.details.get('original_title') or self.original_title
                val = self._sanitize_115_name_component(raw_title)
            elif block == 'title_orig':
                raw_title = original_title or self.details.get('original_title') or self.original_title
                val = self._sanitize_115_name_component(raw_title)
            elif block == 'year': val = f"({self.details.get('date', '')[:4]})" if self.details.get('date') else None
            elif block == 'year_pure': val = self.details.get('date', '')[:4] if self.details.get('date') else None
            elif block == 'tmdb_bracket': val = f"{{tmdb={self.tmdb_id}}}"
            elif block == 'tmdb_square': val = f"[tmdbid={self.tmdb_id}]"
            elif block == 'tmdb_dash': val = f"tmdb-{self.tmdb_id}"
            elif block == 's_e' and is_tv: 
                s_val = season_num if season_num is not None else 1
                e_val = episode_num if episode_num is not None else 1
                val = f"S{s_val:02d}E{e_val:02d}" 
            elif block in ('episode_name_zh', 'episode_no_zh') and is_tv:
                e_val = episode_num if episode_num is not None else 1
                val = f"第 {e_val} 集"
            elif block in ('s_e_zh', 'season_episode_zh') and is_tv:
                s_val = season_num if season_num is not None else 1
                e_val = episode_num if episode_num is not None else 1
                val = f"第 {s_val} 季 {e_val} 集"
            elif block == 'season_name_en' and is_tv:
                val = f"Season {season_num:02d}" if season_num is not None else None
            elif block == 'season_name_en_no0' and is_tv:
                val = f"Season {season_num}" if season_num is not None else None
            elif block == 'season_name_zh' and is_tv:
                val = f"第 {season_num} 季" if season_num is not None else None
            elif block == 'season_name_s' and is_tv:
                val = f"S{season_num:02d}" if season_num is not None else None
            elif block == 'season_name_s_no0' and is_tv:
                val = f"S{season_num}" if season_num is not None else None
            elif video_info and block in video_info: val = video_info.get(block)
            elif block.startswith('sep_'):
                is_sep = True
                if block == 'sep_slash': val = '/'
                elif block.startswith('sep_dash_space'): val = ' - '
                elif block.startswith('sep_middot_space'): val = ' · '
                elif block.startswith('sep_middot'): val = '·'
                elif block.startswith('sep_dot'): val = '.'
                elif block.startswith('sep_dash'): val = '-'
                elif block.startswith('sep_underline'): val = '_'
                elif block.startswith('sep_space'): val = ' '

            if val: evaluated.append({'val': str(val).strip() if not is_sep else val, 'is_sep': is_sep})

        # 智能消除多余分隔符
        final_parts = []
        for i, item in enumerate(evaluated):
            if item['is_sep']:
                has_content_before = any(not x['is_sep'] for x in evaluated[:i])
                has_content_after = any(not x['is_sep'] for x in evaluated[i+1:])
                is_last_sep_in_group = True
                if i + 1 < len(evaluated) and evaluated[i+1]['is_sep']:
                    is_last_sep_in_group = False
                if has_content_before and has_content_after and is_last_sep_in_group:
                    final_parts.append(item['val'])
            else:
                final_parts.append(item['val'])

        return "".join(final_parts)

    @classmethod
    def _sanitize_115_name_component(cls, text):
        cleaned = utils.clean_invisible_chars(text)
        cleaned = cls._P115_INVALID_NAME_CHARS_RE.sub('', cleaned).strip()
        return cleaned

    def _get_episode_regex_rules(self):
        """懒加载自定义季集号识别规则，避免每个文件都查数据库"""
        cache_attr = '_episode_regex_rules_cache'

        if not hasattr(self, cache_attr):
            try:
                rules = settings_db.get_setting('p115_episode_regex_rules') or []
                if not isinstance(rules, list):
                    rules = []
            except Exception as e:
                logger.warning(f"  ➜ [自定义季集号识别] 读取规则失败，已忽略: {e}")
                rules = []

            setattr(self, cache_attr, rules)

        return getattr(self, cache_attr, [])
    
    def _safe_group_to_int(self, match, group_index):
        """安全获取组索引，防止组索引不存在"""
        try:
            if not group_index:
                return None
            value = match.group(int(group_index))
            if value is None:
                return None
            value = str(value).strip()
            if not value:
                return None
            return int(value)
        except Exception:
            return None
        
    def _parse_season_episode_by_custom_regex(self, original_name, rel_path=''):
        """
        返回:
            (season_num, episode_num, matched_rule_name) 或 (None, None, None)
        """
        rules = self._get_episode_regex_rules()
        if not rules or not original_name:
            return None, None, None

        for idx, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue
            if not rule.get('enabled', True):
                continue

            rule_name = str(rule.get('name') or f'规则{idx + 1}').strip()
            pattern = str(rule.get('pattern') or '').strip()
            mode = str(rule.get('mode') or 'episode_only').strip()

            if not pattern:
                continue

            try:
                match = re.search(pattern, original_name, re.IGNORECASE)
                if not match and rel_path:
                    # 可选增强：允许规则匹配相对路径，适合目录名里带季号、文件名只写 01 的情况
                    match = re.search(pattern, rel_path, re.IGNORECASE)
            except re.error as e:
                logger.warning(f"  ➜ [自定义季集号识别] 规则 '{rule_name}' 正则非法，已跳过: {e}")
                continue

            if not match:
                continue

            if mode == 'season_episode':
                season_group = int(rule.get('season_group') or 1)
                episode_group = int(rule.get('episode_group') or 2)

                season_num = self._safe_group_to_int(match, season_group)
                episode_num = self._safe_group_to_int(match, episode_group)

                if season_num is not None and episode_num is not None:
                    return season_num, episode_num, rule_name

            else:
                # episode_only
                episode_group = int(rule.get('episode_group') or 1)
                raw_default_season = rule.get('default_season')
                default_season = 1 if raw_default_season in (None, '') else int(raw_default_season)

                episode_num = self._safe_group_to_int(match, episode_group)
                if episode_num is not None:
                    return default_season, episode_num, rule_name

        return None, None, None

    def _rename_file_node(self, file_node, new_base_name, year=None, is_tv=False, original_title=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None, silent_log=False, recognition_hints=None):
        original_name = file_node.get('fn') or file_node.get('n') or file_node.get('file_name', '')
        rel_path = file_node.get('rel_path', '')
        rule_result = _build_rule_parse_result(
            filename=original_name,
            main_dir_name=os.path.basename(rel_path) if rel_path else None,
            has_season_subdirs=False,
            forced_media_type='tv' if is_tv else 'movie',
            is_folder=False,
        )
        normalized_hints = candidate_to_recognition_hints(recognition_hints or file_node.get('_recognition_hints') or {})
        
        # ★ 修复 1：无后缀文件的提前返回，补齐为 8 个返回值
        if '.' not in original_name: 
            return original_name, None, None, None, {}, False, None

        parts = original_name.rsplit('.', 1)
        name_body = parts[0]
        ext = parts[1].lower()

        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']
        lang_suffix = ""
        if is_sub:
            # ★ 核心修复：支持提取无限连击的复合语言标签 (如 .chs&eng, .zh-cn.default, _eng.forced)
            lang_keywords = r'(?:chs|cht|zh\-cn|zh\-tw|zh|cn|tw|hk|tc|sc|eng|en|jpn|jp|kor|kr|fre|spa|ara|ger|cze|dan|fin|fil|glg|heb|hin|hun|ind|ita|kan|mal|may|nob|dut|pol|por|rum|rus|swe|tam|tel|tha|tur|ukr|vie|default|forced|sdh|cc)'
            
            # 匹配结尾由分隔符(.-_&)和语言代码组成的字符串，最多允许4个组合连击
            match = re.search(rf'((?:[\.\-\_\&]+{lang_keywords}){{1,4}})$', name_body, re.IGNORECASE)
            
            if match:
                lang_suffix = match.group(1)
                # 统一将第一个分隔符替换为点，符合 Emby 规范 (例如 _chs&eng 变成 .chs&eng)
                lang_suffix = '.' + re.sub(r'^[\.\-\_\&]+', '', lang_suffix)

            # ★★★ 强制基础名注入 (专为 MP 字幕挂起等待机制设计) ★★★
            forced_base_name = file_node.get('_forced_base_name')
            if forced_base_name:
                new_name = f"{forced_base_name}{lang_suffix}.{ext}"
                season_num = file_node.get('_forced_season')
                episode_num = file_node.get('_forced_episode')
                s_name = None
                if is_tv and season_num is not None:
                    cfg = self.rename_config
                    season_format = cfg.get('season_dir_format', ['season_name_en'])
                    s_name = self._build_name_from_format(
                        season_format, 
                        is_tv=True, 
                        season_num=season_num, 
                        original_title=original_title, 
                        safe_title=new_base_name
                    )
                    if not s_name: s_name = f"Season {season_num:02d}"
                
                # ★ 修复 2：字幕文件的提前返回，补齐为 8 个返回值
                return new_name, season_num, episode_num, s_name, {}, False, None

        cfg = self.rename_config
        
        # 提取视频信息字典 (基于文件名的猜测)
        search_name = original_name
        if is_sub and lang_suffix and name_body.endswith(lang_suffix):
            search_name = f"{name_body[:-len(lang_suffix)]}.mkv"
        video_info = self._extract_video_info(search_name)

        # 基于 SHA1 获取真实参数
        real_info = None
        
        if not is_sub:
            sha1 = file_node.get('sha1') or file_node.get('sha')
            if sha1:
                real_info = self._fetch_and_parse_mediainfo(
                    sha1,
                    video_info,
                    pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo,
                    file_node=file_node,
                    silent_log=silent_log
                )
                if real_info:
                    for k, v in real_info.items():
                        video_info[k] = v
                    
        # 解析季集号
        # ★ 优先使用文件级精准数据（Webhook 强塞 / raw_ffprobe / 文件名与路径显式特征），
        #   TG Candidate hints 仅在本地证据缺失时做兜底，避免群组级集号污染整包文件。
        season_num = file_node.get('_forced_season')
        episode_num = file_node.get('_forced_episode')
        season_source = 'forced' if season_num is not None else None
        episode_source = 'forced' if episode_num is not None else None

        def _se_int(value):
            try:
                if value in (None, ''):
                    return None
                return int(float(value))
            except Exception:
                return None

        hint_season = _se_int(normalized_hints.get('season_number')) if normalized_hints else None
        hint_episode = _se_int(normalized_hints.get('episode_number')) if normalized_hints else None

        if is_tv and real_info and is_p115_mediainfo_assisted_recognition_enabled():
            raw_probe_season = _se_int(real_info.get('season_number'))
            raw_probe_episode = _se_int(real_info.get('episode_number'))
            if season_num is None:
                season_num = raw_probe_season
                if raw_probe_season is not None:
                    season_source = 'raw_ffprobe'
            if episode_num is None:
                episode_num = raw_probe_episode
                if raw_probe_episode is not None:
                    episode_source = 'raw_ffprobe'
            if (raw_probe_season is not None or raw_probe_episode is not None) and not silent_log:
                season_text = f"第 {int(raw_probe_season)} 季" if raw_probe_season is not None else "季号未知"
                episode_text = f"第 {int(raw_probe_episode)} 集" if raw_probe_episode is not None else "集号未知"
                logger.info(
                    f"  ➜ [媒体信息辅助识别] 已从媒体信息识别到 {season_text}{episode_text}：{original_name}"
                )

        if is_tv and (season_num is None or episode_num is None):

            # 0. ★ 先跑用户自定义规则，命中即优先使用
            custom_season, custom_episode, custom_rule_name = self._parse_season_episode_by_custom_regex(
                original_name=original_name,
                rel_path=rel_path
            )

            if custom_season is not None and season_num is None:
                season_num = custom_season
                season_source = 'custom_rule'
            if custom_episode is not None and episode_num is None:
                episode_num = custom_episode
                episode_source = 'custom_rule'

            if custom_rule_name and not silent_log:
                logger.info(
                    f"  ➜ [自定义季集号识别] 命中规则 '{custom_rule_name}' -> "
                    f"S{int(season_num if season_num is not None else 1):02d}E{int(episode_num if episode_num is not None else 0):02d} | {original_name}"
                )

            if rule_result.get('season_number') is not None and season_num is None:
                season_num = int(rule_result.get('season_number'))
                season_source = 'rule'
            if rule_result.get('episode_number') is not None and episode_num is None:
                episode_num = int(rule_result.get('episode_number'))
                episode_source = 'rule'
            if (
                (rule_result.get('season_number') is not None or rule_result.get('episode_number') is not None)
                and not silent_log
            ):
                logger.info(
                    f"  ➜ [规则预解析季集] 命中 evidence={','.join(rule_result.get('evidence') or []) or 'rule'} -> "
                    f"S{int(season_num if season_num is not None else 1):02d}E{int(episode_num if episode_num is not None else 0):02d} | {original_name}"
                )

            # 1. 自定义没补全，再走原有硬编码识别
            if season_num is None or episode_num is None:
                pattern = (
                    r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*?(?:e|E|p|P)(\d{1,4})\b'
                    r'|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b'
                    r'|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b'
                    r'|第\s*\d{1,4}\s*季\s*(\d{1,4})\s*[集话話回](?=$|[^\u4e00-\u9fff]|完|完结|完結)'
                    r'|第\s*(\d{1,4})\s*[集话話回](?=$|[^\u4e00-\u9fff]|完|完结|完結)'
                    r'|(?:^|[ \.\-\_\[\(])(\d{1,4})[集话話回](?=$|[^\u4e00-\u9fff]|完|完结|完結)'
                )

                match = re.search(pattern, original_name, re.IGNORECASE)
                if match:
                    s = match.group(1)
                    e = match.group(2)
                    ep_only = match.group(3)
                    e_only = match.group(4)
                    zh_ep = match.group(5) or match.group(6) or match.group(7)

                    if season_num is None:
                        season_num = int(s) if s else None
                        if season_num is not None:
                            season_source = 'filename'

                    if episode_num is None:
                        episode_num = int(e) if e else (
                            int(ep_only) if ep_only else (
                                int(e_only) if e_only else int(zh_ep)
                            )
                        )
                        if episode_num is not None:
                            episode_source = 'filename'

            # 2. 从相对路径提取季号，支持 Specials / SP / OVA / 第0季
            if season_num is None and rel_path:
                season_from_path = self._extract_season_from_path_or_text(rel_path)
                if season_from_path is not None:
                    season_num = season_from_path
                    season_source = 'path'

            # 3. ★ 纯数字 / 动漫数字兜底提取集号
            if episode_num is None:
                name_without_ext = original_name.rsplit('.', 1)[0]
                if name_without_ext.isdigit():
                    episode_num = int(name_without_ext)
                    episode_source = 'filename'
                else:
                    clean_name = re.sub(
                        r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0',
                        '',
                        name_without_ext
                    )

                    anime_match = re.search(r'(?:\s-\s+)(\d{1,4})(?:\s|$)|\[(\d{1,4})\]|【(\d{1,4})】', clean_name)
                    if anime_match:
                        ep_str = anime_match.group(1) or anime_match.group(2) or anime_match.group(3)
                        episode_num = int(ep_str)
                        episode_source = 'filename'
                    else:
                        end_match = re.search(r'(?:^|[ \.\-\_\[\(])(\d{1,4})(?:[\]\)]|\s*)$', clean_name)
                        if end_match:
                            episode_num = int(end_match.group(1))
                            episode_source = 'filename'
                        else:
                            mid_match = re.search(r'(?:^|[ \-\_\[\(])(\d{1,4})(?:[ \.\-\_\]\)]|$)', clean_name)
                    if mid_match:
                        episode_num = int(mid_match.group(1))
                        episode_source = 'filename'

            # 4. 终极兜底
            if season_num is None:
                season_num = 1
                season_source = 'default'

        if is_tv and normalized_hints:
            if season_num is None and hint_season is not None:
                season_num = hint_season
                season_source = 'hint'
            if episode_num is None and hint_episode is not None:
                episode_num = hint_episode
                episode_source = 'hint'
            if (hint_season is not None or hint_episode is not None) and not silent_log:
                logger.info(
                    f"  ➜ [TG Candidate季集] 命中 hints -> "
                    f"S{int(hint_season if hint_season is not None else 1):02d}"
                    f"E{int(hint_episode if hint_episode is not None else 0):02d} | {original_name}"
                )

        if is_tv and normalized_hints.get('is_special') and season_num is None:
            season_num = 0
            season_source = 'hint'

        if is_tv and normalized_hints.get('is_special') and season_num == 1 and episode_num is None:
            season_num = 0
            season_source = 'hint'

        if is_tv and rule_result.get('is_special') and season_num is None:
            season_num = 0
            season_source = 'rule'

        if is_tv and rule_result.get('is_special') and season_num == 1 and episode_num is None:
            season_num = 0
            season_source = 'rule'

        # ★★★ 动漫绝对集数转季号逻辑 (解决海贼王 S01E1158 的问题) ★★★
        if is_tv and episode_num is not None and episode_num > 30:
            seasons_data = self.details.get('seasons', [])
            last_ep_data = self.details.get('last_episode_to_air', {})
            
            # ★ 核心修复：容量校验。检查当前解析出的 season_num 是否真的能容纳这个 episode_num
            # 如果不能容纳 (比如 S01 只有 61 集，但文件是 E1158)，说明这是绝对集数，强制反推！
            needs_recalc = False
            if seasons_data:
                current_season_data = next((s for s in seasons_data if s.get('season_number') == season_num), None)
                if not current_season_data or current_season_data.get('episode_count', 0) < episode_num:
                    needs_recalc = True
            elif season_num == 1:
                needs_recalc = True

            if needs_recalc:
                # 捷径：如果是最新集，直接取最新季
                if last_ep_data and last_ep_data.get('episode_number') == episode_num:
                    season_num = last_ep_data.get('season_number', 1)
                    season_source = 'season_capacity_fix'
                    if not silent_log:
                        logger.info(f"  ➜ [分季修正] 命中最新集，自动修正为第 {season_num} 季")
                elif seasons_data:
                    # 累加算法：排除第 0 季(SP)，按顺序累加集数，推算所属季
                    valid_seasons = sorted([s for s in seasons_data if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])
                    cumulative = 0
                    for s in valid_seasons:
                        cumulative += s.get('episode_count', 0)
                        if episode_num <= cumulative:
                            season_num = s['season_number']
                            season_source = 'season_capacity_fix'
                            if not silent_log:
                                logger.info(f"  ➜ [分季修正] 绝对集数 {episode_num} 超出原季容量，已自动推算并修正为第 {season_num} 季！")
                            break

        if hasattr(self, 'forced_season') and self.forced_season is not None:
            # ★ 核心修复：防止批量整理时，第一个文件的季号污染后续所有不同季号的文件
            if getattr(self, 'is_manual_correct', False):
                season_num = int(self.forced_season)
            else:
                # 仅当文件名和相对路径中都没有明确的季号特征时，才使用外层推导的 forced_season 作为兜底
                has_explicit_season = False
                explicit_season_re = (
                    r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)|'
                    r'Season\s*\d{1,4}|第\s*\d{1,4}\s*季|'
                    r'(?:^|[ \.\-\_\[\(])(?:Specials?|SP|OVA|OAD|特别篇|特別篇|番外(?:篇)?|外传|外傳)(?=$|[ \.\-\_\]\)])'
                )

                if re.search(explicit_season_re, original_name, re.IGNORECASE):
                    has_explicit_season = True
                elif file_node.get('rel_path') and self._extract_season_from_path_or_text(file_node.get('rel_path')) is not None:
                    has_explicit_season = True
                    
                if not has_explicit_season:
                    season_num = int(self.forced_season)
                    season_source = 'forced'

        # ★★★ 核心升级：直接调用统一乐高引擎生成文件名 ★★★
        default_format = ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio', 'sep_middot_space', 'group']
        file_format = cfg.get('file_format', default_format)

        core_name = self._build_name_from_format(
            file_format, 
            is_tv=is_tv, 
            season_num=season_num, 
            episode_num=episode_num, 
            original_title=original_title, 
            video_info=video_info,
            safe_title=new_base_name # 传入过滤过特殊字符的标题
        )

        # 兜底：如果轨道配空了，用原名
        if not core_name: core_name = name_body

        # ★★★ 提取 Part/CD 上下集信息，符合 Emby 规范 ★★★
        part_num = None
        part_suffix = ""
        part_match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', original_name)
        if part_match:
            part_num = int(part_match.group(2))
            part_suffix = f" - pt{part_num}"

        new_name = f"{core_name}{part_suffix}{lang_suffix}.{ext}"
        
        # ★★★ 核心修复：在这里利用齐全的 video_info 生成季目录名称 ★★★
        s_name = None
        if is_tv and season_num is not None:
            season_format = cfg.get('season_dir_format', ['season_name_en'])
            s_name = self._build_name_from_format(
                season_format, 
                is_tv=True, 
                season_num=season_num, 
                original_title=original_title, 
                video_info=video_info, # ★ 关键：把视频信息传进去！
                safe_title=new_base_name
            )
            if not s_name: s_name = f"Season {season_num:02d}"

        # raw_ffprobe_json 生成早于最终识别结果；此时 TMDb 身份与季集号已经落定，
        # 反向补写本地 RAW。手动重组时强制覆盖 _etk.tmdb_id/type，避免旧错误身份继续污染共享 RAW。
        if not is_sub:
            try:
                raw_patch_sha1 = file_node.get('sha1') or file_node.get('sha')
                force_identity = bool(getattr(self, 'is_manual_correct', False))
                trusted_season = None
                trusted_episode = None

                if is_tv:
                    trusted_season = season_num if (
                        season_source not in (None, 'hint')
                        and episode_source not in ('hint',)
                    ) else None
                    trusted_episode = episode_num if episode_source not in (None, 'hint') else None

                if raw_patch_sha1 and (force_identity or trusted_season is not None or trusted_episode is not None):
                    P115CacheManager.patch_raw_ffprobe_etk_context(
                        raw_patch_sha1,
                        tmdb_id=self.tmdb_id,
                        media_type=self.media_type,
                        original_language=(self.raw_metadata or {}).get('lang_code'),
                        season_number=trusted_season,
                        episode_number=trusted_episode,
                        force_identity=force_identity,
                    )
            except Exception:
                pass

        return new_name, season_num, episode_num, s_name, video_info, bool(real_info), part_num

    def _scan_files_recursively(self, cid, depth=0, max_depth=3, current_rel_path=""):
        all_files = []
        if depth > max_depth: return []
        try:
            res = self.client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            if res.get('data'):
                for item in res['data']:
                    # 兼容 OpenAPI 键名
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if str(fc_val) == '1':
                        item['rel_path'] = current_rel_path
                        all_files.append(item)
                    elif str(fc_val) == '0':
                        sub_id = item.get('fid') or item.get('file_id')
                        sub_name = item.get('fn') or item.get('n') or item.get('file_name', '')
                        new_rel = f"{current_rel_path}/{sub_name}" if current_rel_path else sub_name
                        sub_files = self._scan_files_recursively(sub_id, depth + 1, max_depth, new_rel)
                        all_files.extend(sub_files)
        except Exception as e:
            logger.warning(f"  ➜ 扫描目录出错 (CID: {cid}): {e}")
        return all_files

    def _is_junk_file(self, filename):
        """
        检查是否为垃圾文件/样本/花絮 (基于 MP 规则)
        """
        # 垃圾文件正则列表 (合并了通用规则和你提供的 MP 规则)
        junk_patterns = [
            # 基础关键词
            r'(?i)\b(sample|trailer|featurette|bonus)\b',

            # MP 规则集
            r'(?i)Special Ending Movie',
            r'(?i)\[((TV|BD|\bBlu-ray\b)?\s*CM\s*\d{2,3})\]',
            r'(?i)\[Teaser.*?\]',
            r'(?i)\[PV.*?\]',
            r'(?i)\[NC[OPED]+.*?\]',
            r'(?i)\[S\d+\s+Recap(\s+\d+)?\]',
            r'(?i)Menu',
            r'(?i)Preview',
            r'(?i)\b(CDs|SPs|Scans|Bonus|映像特典|映像|specials|特典CD|Menu|Logo|Preview|/mv)\b',
            r'(?i)\b(NC)?(Disc|片头|OP|SP|ED|Advice|Trailer|BDMenu|片尾|PV|CM|Preview|MENU|Info|EDPV|SongSpot|BDSpot)(\d{0,2}|_ALL)\b',
            r'(?i)WiKi\.sample'
        ]

        for pattern in junk_patterns:
            if re.search(pattern, filename):
                return True
        return False
    
    def _execute_collection_breakdown(self, root_item, collection_movies, skip_gc=False):
        """内部方法：拆解并独立整理合集包内的文件 (已升级批量模式)"""
        source_root_id = root_item.get('fid') or root_item.get('file_id')
        root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
        unidentified_cid = None 
        
        # 获取或创建未识别目录 CID
        config = get_config()
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        
        if not unidentified_cid or str(unidentified_cid) == '0':
            save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            unidentified_folder_name = "未识别"
            if save_cid and str(save_cid) != '0':
                try:
                    search_res = self.client.fs_files({'cid': save_cid, 'search_value': unidentified_folder_name, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            if item.get('fn') == unidentified_folder_name and str(item.get('fc')) == '0':
                                unidentified_cid = item.get('fid')
                                break
                except: pass
                
                if not unidentified_cid:
                    try:
                        mk_res = self.client.fs_mkdir(unidentified_folder_name, save_cid)
                        if mk_res.get('state'): unidentified_cid = mk_res.get('cid')
                    except: pass

        processed_count = 0
        try:
            sub_res = self.client.fs_files({'cid': source_root_id, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            sub_items = sub_res.get('data', [])
            
            # ★ 新增：分组字典
            grouped_sub_items = {}
            unidentified_sub_fids = []
            unidentified_video_names = []
            
            for sub_item in sub_items:
                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name')
                sub_id = sub_item.get('fid') or sub_item.get('file_id')
                sub_fc_val = sub_item.get('fc') if sub_item.get('fc') is not None else sub_item.get('type')
                
                # 1. 优先看子项自己有没有带 ID / raw_ffprobe 共享身份
                sub_sha1 = sub_item.get('sha1') or sub_item.get('sha')
                if not sub_sha1 and sub_id and str(sub_fc_val) == '1':
                    try:
                        info_res = self.client.fs_get_info(sub_id)
                        if info_res.get('state') and info_res.get('data'):
                            sub_sha1 = info_res['data'].get('sha1')
                            if sub_sha1:
                                sub_item['sha1'] = sub_sha1
                    except Exception:
                        pass

                sub_hint = lookup_candidate_hint_for_name(sub_name, alt_texts=[root_name])
                tmdb_id, sub_type, sub_title = _identify_media_enhanced(
                    sub_name, 
                    ai_translator=self.ai_translator, 
                    use_ai=self.use_ai,
                    is_folder=(str(sub_fc_val) == '0'),
                    sha1=sub_sha1,
                    recognition_hints=sub_hint
                )
                
                # 2. 模糊匹配 (仅当有官方合集列表时)
                if not tmdb_id and collection_movies:
                    matched_movie = None
                    clean_sub_name = re.sub(r'[^\w\u4e00-\u9fa5]', '', sub_name).lower()
                    
                    for movie in collection_movies:
                        m_title = movie.get('title', '')
                        m_orig = movie.get('original_title', '')
                        m_year = movie.get('release_date', '')[:4] if movie.get('release_date') else ''
                        
                        clean_m_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', m_title).lower()
                        clean_m_orig = re.sub(r'[^\w\u4e00-\u9fa5]', '', m_orig).lower()
                        
                        if (clean_m_title and clean_m_title in clean_sub_name) or \
                           (clean_m_orig and clean_m_orig in clean_sub_name):
                            if m_year and m_year in sub_name:
                                matched_movie = movie
                                break
                            elif not matched_movie:
                                matched_movie = movie
                    
                    if matched_movie:
                        tmdb_id = str(matched_movie['id'])
                        sub_type = 'movie'
                        sub_title = matched_movie.get('title')
                        logger.info(f"    ├─ 官方合集匹配成功：{sub_name}，识别为《{sub_title}》。")
                        logger.debug(f"    ├─ 官方合集匹配 TMDb：{tmdb_id}")

                # 3. 终极兜底：无官方合集时的文件名暴力解析搜索
                if not tmdb_id and not collection_movies:
                    clean_name = re.sub(r'^\[.*?\]|^.*?\.com-|^.*?\.[a-z]{2,3}-', '', sub_name, flags=re.IGNORECASE)
                    match_year = re.search(r'^(.*?)(?:\.|_|-|\s|\()+(19\d{2}|20\d{2})\b', clean_name)
                    if match_year:
                        guess_title = match_year.group(1).replace('.', ' ').strip()
                        guess_year = match_year.group(2)
                        logger.info(f"    ├─ 尝试搜索: '{guess_title}' ({guess_year})")
                        try:
                            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                            results = tmdb.search_media(query=guess_title, api_key=api_key, item_type='movie', year=guess_year)
                            if results and len(results) > 0:
                                tmdb_id = str(results[0]['id'])
                                sub_type = 'movie'
                                sub_title = results[0].get('title') or results[0].get('name')
                                logger.info(f"    ├─ 搜索成功：识别为《{sub_title}》。")
                                logger.debug(f"    ├─ 搜索命中 TMDb：{tmdb_id}")
                        except Exception as e:
                            logger.debug(f"    ├─ 搜索出错: {e}")
                
                # ★ 核心修改：不再立即执行，而是加入分组字典
                if tmdb_id:
                    sub_item['_recognition_hints'] = sub_hint or {}
                    key = (tmdb_id, sub_type, sub_title)
                    if key not in grouped_sub_items:
                        grouped_sub_items[key] = {
                            "items": [],
                            "recognition_hints": sub_item.get('_recognition_hints') or {},
                        }
                    grouped_sub_items[key]["items"].append(sub_item)
                else:
                    unidentified_sub_fids.append(sub_id)
                    # ★ 检查是否为真正的视频文件
                    sub_ext = sub_name.split('.')[-1].lower() if '.' in sub_name else ''
                    if sub_ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']:
                        unidentified_video_names.append(sub_name)
            
            # ★ 核心修改：遍历分组，批量执行
            for (tmdb_id, sub_type, sub_title), group_data in grouped_sub_items.items():
                items = group_data.get("items") or []
                group_hints = group_data.get("recognition_hints") or {}
                logger.info(f"    ├─ 准备批量整理合集子项：《{sub_title}》，共 {len(items)} 个文件。")
                logger.debug(f"    ├─ 合集子项整理 TMDb：{tmdb_id}")
                try:
                    organizer = SmartOrganizer(
                        self.client,
                        tmdb_id,
                        sub_type,
                        sub_title,
                        self.ai_translator,
                        self.use_ai,
                        recognition_hints=group_hints,
                    )
                    organizer.recognition_hints = group_hints
                    target_cid_for_sub = organizer.get_target_cid()
                    if organizer.execute(items, target_cid_for_sub):
                        processed_count += len(items)
                except Exception as e:
                    logger.error(f"    ➜ 批量处理子项失败: {e}")
            
            # ★ 核心修改：批量移入未识别
            if unidentified_sub_fids and unidentified_cid:
                logger.warning(f"    ➜ 无法识别合集子项 {len(unidentified_sub_fids)} 个，批量移入未识别。")
                try: 
                    self.client.fs_move(unidentified_sub_fids, unidentified_cid)
                    # ★★★ 核心修复：只有当存在真正的视频文件时，才发送通知 ★★★
                    if unidentified_video_names:
                        from handler.telegram import send_unrecognized_notification
                        send_unrecognized_notification(f"合集包 [{root_name}] 内的 {len(unidentified_video_names)} 个视频文件", reason="合集拆解时无法匹配到 TMDb 数据")
                except Exception as e: 
                    logger.error(f"    ➜ 移入未识别失败: {e}")
            
            if not skip_gc:
                from handler.p115_service import P115DeleteBuffer
                P115DeleteBuffer.add(check_save_path=True)
                logger.info(f"  ➜ [清理空目录] 已将拆解完毕的合集包交由垃圾回收器检查: {root_name}")
            
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"  ➜ 拆解合集包失败: {e}")
            return False

    def execute(self, root_item_or_items, target_cid, progress_callback=None, skip_gc=False):
        # 判断传入的是单个文件还是批量文件列表
        is_batch = isinstance(root_item_or_items, list)
        
        if is_batch:
            if not root_item_or_items: return True 
            root_item = root_item_or_items[0]      
            root_name = "批量文件"
            parse_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '') 
            source_root_id = root_item.get('pid') or root_item.get('parent_id')
            is_source_file = True
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else source_root_id
        else:
            root_item = root_item_or_items
            root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
            parse_name = root_name 
            source_root_id = root_item.get('fid') or root_item.get('file_id')
            fc_val = root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type')
            is_source_file = str(fc_val) == '1'
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))

        # =================================================================
        # 1. 拦截合集包 (Collection Breakdown) - 仅限单项传入时触发
        # =================================================================
        if not is_batch and not is_source_file and re.search(r'(合集|部曲|系列|Collection|Pack|Trilogy|Quadrilogy|\d+-\d+)', root_name, re.IGNORECASE):
            logger.info(f"  ➜ 检测到疑似合集包: {root_name}，正在验证...")
            collection_movies = []
            try:
                res_c = tmdb.get_collection_details(int(self.tmdb_id), self.api_key, skip_fallback=True)
                if res_c and 'parts' in res_c: collection_movies = res_c['parts']
            except: pass
            
            if not collection_movies and self.media_type == 'movie':
                try:
                    c_id = None
                    if hasattr(self, 'raw_metadata') and self.raw_metadata and self.raw_metadata.get('belongs_to_collection'):
                        c_id = self.raw_metadata['belongs_to_collection']['id']
                    else:
                        res_m = tmdb.get_movie_details(int(self.tmdb_id), self.api_key)
                        if res_m and res_m.get('belongs_to_collection'):
                            c_id = res_m['belongs_to_collection']['id']
                    if c_id:
                        res_c = tmdb.get_collection_details(int(c_id), self.api_key, skip_fallback=True)
                        if res_c and 'parts' in res_c: collection_movies = res_c['parts']
                except: pass

            if collection_movies:
                logger.info(f"  ➜ 确认为官方合集包，包含 {len(collection_movies)} 部电影，启动精确拆解模式...")
            else:
                logger.info(f"  ➜ 未找到官方合集信息 (可能是民间自制包)，启动基于文件名的暴力拆解模式...")
            return self._execute_collection_breakdown(root_item, collection_movies, skip_gc=skip_gc)

        # =================================================================
        # 2. 提前获取候选文件列表 (支持批量合并)
        # =================================================================
        candidates = []
        if is_batch:
            for item in root_item_or_items:
                fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                if str(fc_val) == '1':
                    candidates.append(item)
                else:
                    candidates.extend(self._scan_files_recursively(item.get('fid') or item.get('file_id'), max_depth=3))
        else:
            if is_source_file:
                candidates.append(root_item)
            else:
                candidates = self._scan_files_recursively(source_root_id, max_depth=3)

        if not candidates: return True

        # =================================================================
        # ★★★ 3. 核心重构：提前提取物理视频流信息 (替代原有的冗余嗅探逻辑) ★★★
        # =================================================================
        # 无论是否配置了时长规则，我们都提前抓取第一个视频的真实媒体信息。
        # 这样不仅代码更简洁，而且能确保后续所有的分类规则判定都基于最准确的数据。
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        first_video = next((c for c in candidates if (c.get('fn') or c.get('n') or c.get('file_name') or '').split('.')[-1].lower() in known_video_exts), None)

        # 媒体信息缓存以 p115_mediainfo_cache 数据库为唯一真理。
        # 这里保留变量仅兼容旧函数签名，不再使用内存预取字典。
        pre_fetched_mediainfo = None
        local_pre_fetched_mediainfo = None
        if first_video and not getattr(self, 'is_manual_correct', False) and not getattr(self, 'is_from_memory', False):
            v_sha1 = first_video.get('sha1') or first_video.get('sha')
            v_fid = first_video.get('fid') or first_video.get('file_id')
            
            if not v_sha1 and v_fid:
                try:
                    info_res = self.client.fs_get_info(v_fid)
                    if info_res.get('state') and info_res.get('data'):
                        v_sha1 = info_res['data'].get('sha1')
                        first_video['sha1'] = v_sha1
                except: pass

            if v_sha1 or v_fid:
                # 提前解析媒体信息。内部会直读本地 DB；DB 没有时才 ffprobe。
                self._fetch_and_parse_mediainfo(
                    v_sha1,
                    guessed_info={},
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                    file_node=first_video,
                    silent_log=True
                )
                
                # 尝试补齐 TMDb 缺失的时长
                if v_sha1:
                    cached_text = P115CacheManager.get_mediainfo_cache_text(v_sha1)
                    if cached_text:
                        try:
                            mi_json = json.loads(cached_text)
                            ticks = 0
                            if isinstance(mi_json, list) and len(mi_json) > 0:
                                ticks = mi_json[0].get("MediaSourceInfo", {}).get("RunTimeTicks", 0)
                            elif isinstance(mi_json, dict):
                                ticks = mi_json.get("MediaSourceInfo", {}).get("RunTimeTicks", 0)

                            if ticks > 0:
                                physical_runtime = int(ticks / 10000000 / 60)
                                if self.media_type == 'movie':
                                    if not self.details.get('runtime'):
                                        self.details['runtime'] = physical_runtime
                                        logger.info(f"  ➜ [提前解析] 成功补齐电影物理时长: {physical_runtime} 分钟")
                                else:
                                    runtimes = self.details.get('episode_run_time', [])
                                    if not runtimes or runtimes[0] == 0:
                                        self.details['episode_run_time'] = [physical_runtime]
                                        logger.info(f"  ➜ [提前解析] 成功补齐剧集物理时长: {physical_runtime} 分钟")
                        except Exception:
                            pass

        # =================================================================
        # ★★★ 4. 智能类型纠错嗅探 (Movie -> TV) ★★★
        # =================================================================
        if self.media_type == 'movie' and not getattr(self, 'is_manual_correct', False):
            is_actually_tv = False
            for c in candidates:
                c_name = c.get('fn') or c.get('n') or c.get('file_name', '')
                rel_path = c.get('rel_path', '')
                
                if self._extract_season_from_path_or_text(rel_path) is not None:
                    is_actually_tv = True
                    break
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第\s*\d{1,4}\s*季\s*\d{1,4}\s*[集话話回]|第\s*\d{1,4}\s*[集话話回]', c_name, re.IGNORECASE):
                    is_actually_tv = True
                    break
                
                clean_c_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', c_name)
                if re.search(r'(?:\s-\s+)(\d{2,4})(?:\s|$)|\[(\d{2,4})\]|【(\d{2,4})】', clean_c_name): 
                    is_actually_tv = True
                    break
            
            if is_actually_tv:
                logger.warning(f"  🕵️‍♂️ [智能纠错] 发现文件包含明显的剧集特征(如季目录/EP01)，但当前被错误识别为电影。正在尝试自动纠错...")
                try:
                    self.media_type = 'tv'
                    cache_key = f"tv_{self.tmdb_id}"
                    if cache_key in _TMDB_METADATA_CACHE:
                        del _TMDB_METADATA_CACHE[cache_key]
                        
                    self.raw_metadata = self._fetch_raw_metadata()
                    
                    if self.raw_metadata and self.raw_metadata.get('title'):
                        self.details = self.raw_metadata
                        logger.info(f"  ➜ [智能纠错] 成功保留原 ID ({self.tmdb_id}) 并切换为剧集: {self.details.get('title')}")
                    else:
                        logger.warning(f"  ➜ [智能纠错] 原 ID ({self.tmdb_id}) 作为剧集查询失败，尝试用名称重新搜索...")
                        search_title = self.original_title
                        clean_title = re.sub(r'\(\d{4}\)', '', search_title).strip()
                        results = tmdb.search_media(query=clean_title, api_key=self.api_key, item_type='tv')
                        
                        if results and len(results) > 0:
                            new_tmdb_id = str(results[0]['id'])
                            logger.info(f"  ➜ [智能纠错] 已重新识别为剧集：《{results[0].get('name')}》。")
                            logger.debug(f"  ➜ [智能纠错] 新 TMDb：{new_tmdb_id}")
                            self.tmdb_id = new_tmdb_id
                            self.raw_metadata = self._fetch_raw_metadata()
                            self.details = self.raw_metadata
                        else:
                            logger.warning(f"  ➜ [智能纠错] 未能在 TMDb 找到对应的剧集，将强制按剧集格式重命名以防冲突。")
                except Exception as e:
                    logger.error(f"  ➜ [智能纠错] 纠错失败: {e}")

        # =================================================================
        # ★★★ 5. 提取季号并统一计算最终 Target CID ★★★
        # =================================================================
        if self.media_type == 'tv' and getattr(self, 'forced_season', None) is None:
            extracted_season = self._extract_season_from_path_or_text(parse_name)

            if extracted_season is None:
                m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', parse_name, re.IGNORECASE)
                m2 = re.search(r'Season\s*(\d{1,4})\b', parse_name, re.IGNORECASE)
                m3 = re.search(r'第\s*(\d{1,4})\s*季', parse_name)

                if m1:
                    extracted_season = int(m1.group(1))
                elif m2:
                    extracted_season = int(m2.group(1))
                elif m3:
                    extracted_season = int(m3.group(1))
            else:
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b|第\s*(\d{1,4})\s*[集话話回]', parse_name, re.IGNORECASE):
                    extracted_season = 1
            
            if extracted_season is not None:
                self.forced_season = extracted_season

        # 同一 TMDb 批次里可能同时有 mkv / iso 等不同文件级分类。
        # 这种情况必须先按目标分类拆批，否则会被后续电影洗版逻辑当成同目录多版本互相淘汰。
        if is_batch and not getattr(self, '_disable_extension_batch_split', False):
            split_groups = {}
            original_sorting_filename = getattr(self, 'current_sorting_filename', '')
            original_memory_flag = getattr(self, 'is_from_memory', False)
            try:
                for item in candidates:
                    item_name = item.get('fn') or item.get('n') or item.get('file_name', '')
                    self.current_sorting_filename = item_name
                    item_target_cid = self.get_target_cid(season_num=getattr(self, 'forced_season', None)) or target_cid
                    group_key = str(item_target_cid or '')
                    split_groups.setdefault(group_key, {'target_cid': item_target_cid, 'items': []})['items'].append(item)
            finally:
                self.current_sorting_filename = original_sorting_filename
                self.is_from_memory = original_memory_flag

            if len(split_groups) > 1:
                logger.info(f"  ➜ [智能分类] 同批文件命中 {len(split_groups)} 个不同分类，按分类拆分整理。")
                ok = True
                self._disable_extension_batch_split = True
                try:
                    for group in split_groups.values():
                        ok = self.execute(
                            group['items'],
                            group['target_cid'],
                            progress_callback=progress_callback,
                            skip_gc=skip_gc
                        ) and ok
                finally:
                    self._disable_extension_batch_split = False
                return ok

        # ★ 统一在这里获取最终的 target_cid！(因为 details 已经补齐了时长，media_type 也可能被纠错了，season 也提取了)
        if not getattr(self, 'is_manual_correct', False):
            self.current_sorting_filename = parse_name
            new_target_cid = self.get_target_cid(season_num=getattr(self, 'forced_season', None))
            if new_target_cid and str(new_target_cid) != str(target_cid):
                logger.info(f"  ➜ [智能分类] 目标目录已根据最新元数据(时长/类型/连载状态)修正！")
                target_cid = new_target_cid
                dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else source_root_id

        # =================================================================
        # 6. 计算最终的目录名称和路径 (支持 / 多级目录)
        # =================================================================
        title = self.details.get('title') or self.original_title
        original_title = self.details.get('original_title') or title
        date_str = self.details.get('date') or ''
        year = date_str[:4] if date_str else ''

        cfg = self.rename_config
        keep_original = cfg.get('keep_original_name', False)
        
        # ★ 必须保留 safe_title 的计算，供后续文件重命名使用
        base_title = original_title if cfg.get('main_title_lang', 'zh') == 'original' else title
        safe_title = self._sanitize_115_name_component(base_title)

        # ★ 保留原名只影响文件名，不影响主目录
        # batch 模式 root_name 可能是“批量文件”，绝不能拿它当目标主目录
        main_format = cfg.get('main_dir_format', ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'])
        std_root_name = self._build_name_from_format(
            main_format,
            is_tv=(self.media_type == 'tv'),
            original_title=original_title,
            safe_title=safe_title
        )

        # 兜底防空
        if not std_root_name:
            std_root_name = safe_title

        config = get_config()
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        min_size_mb = int(config.get(constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE, 10))
        MIN_VIDEO_SIZE = min_size_mb * 1024 * 1024

        # 获取“未识别”目录的 CID
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        if not unidentified_cid or str(unidentified_cid) == '0':
            save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            if save_cid and str(save_cid) != '0':
                try:
                    search_res = self.client.fs_files({'cid': save_cid, 'search_value': '未识别', 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            if item.get('fn') == '未识别' and str(item.get('fc')) == '0':
                                unidentified_cid = item.get('fid')
                                break
                except: pass

        logger.info(f"  ➜ [115整理] 开始整理：{root_name}，目标目录：{std_root_name}")

        final_home_cid = None
        current_parent_cid = dest_parent_cid
        
        # ★★★ 核心升级：支持 / 分层创建多级目录 ★★★
        dir_parts = [p.strip() for p in std_root_name.split('/') if p.strip()]
        
        # 提前计算基础相对路径，用于逐级修复 local_path
        category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
        base_rel_path = category_rule.get('category_path') or category_rule.get('dir_name', '未识别') if category_rule else "未识别"
        
        for attempt in range(2):
            success_chain = True
            temp_parent_cid = current_parent_cid
            
            # 逐级检查/创建目录
            for part_name in dir_parts:
                cache_key = f"{temp_parent_cid}_{part_name}"
                
                # 1. 优先查全局内存缓存 (抵抗并发)
                with _GLOBAL_DIR_LOCK:
                    part_cid = _GLOBAL_DIR_CACHE.get(cache_key)
                
                # 2. 查数据库缓存
                if not part_cid:
                    part_cid = P115CacheManager.get_cid(temp_parent_cid, part_name)
                    if part_cid:
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = part_cid

                # 缓存自愈检查
                if part_cid and str(part_cid) == str(source_root_id) and str(temp_parent_cid) != str(root_item.get('pid') or root_item.get('parent_id')):
                    P115CacheManager.delete_cid(part_cid)
                    with _GLOBAL_DIR_LOCK:
                        _GLOBAL_DIR_CACHE.pop(cache_key, None)
                    part_cid = None

                if not part_cid:
                    mk_res = self.client.fs_mkdir(part_name, temp_parent_cid)
                    if mk_res.get('state'):
                        part_cid = mk_res.get('cid')
                        P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = part_cid
                    else:
                        err_text = json.dumps(mk_res, ensure_ascii=False)
                        should_search_after_mkdir_fail = any(
                            kw in err_text.lower()
                            for kw in ['exist', 'exists', 'already', '重复', '已存在', 'same_name', '文件名重复']
                        )

                        if should_search_after_mkdir_fail:
                            try:
                                # ★ 核心修复：使用 fs_files + search_value 精准定位！
                                # 既突破了 1000 条限制，又不会触发全局 search 的 WAF 风控
                                search_res = self.client.fs_files({
                                    'cid': temp_parent_cid,
                                    'search_value': part_name,
                                    'limit': 100,
                                    'show_dir': 1,
                                    'record_open_time': 0
                                })
                                for item in search_res.get('data', []):
                                    item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                    item_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                                    item_cid = (
                                        item.get('fid')
                                        or item.get('file_id')
                                        or item.get('id')
                                        or item.get('cid')
                                    )
                                    
                                    if item_fc == '0' and item_name == part_name and item_cid:
                                        part_cid = item_cid
                                        P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                                        with _GLOBAL_DIR_LOCK:
                                            _GLOBAL_DIR_CACHE[cache_key] = part_cid
                                        break
                            except Exception as e:
                                logger.debug(f"  ➜ 目录精准定位失败: {e}")
                
                if part_cid:
                    temp_parent_cid = part_cid
                    # ★ 核心修复：逐级累加路径并更新 DB，彻底解决年份目录 local_path 为 NULL 的问题！
                    base_rel_path = f"{base_rel_path}/{part_name}"
                    P115CacheManager.update_local_path(part_cid, base_rel_path)
                else:
                    success_chain = False
                    break
            
            if success_chain:
                final_home_cid = temp_parent_cid
                break # 成功获取最终层级，跳出重试循环
                
            # 失败回退逻辑
            if attempt == 0:
                fallback_cid = self.get_target_cid(ignore_memory=True)
                if fallback_cid and str(fallback_cid) != str(current_parent_cid):
                    P115CacheManager.delete_cid(current_parent_cid)
                    current_parent_cid = fallback_cid
                    target_cid = fallback_cid 
                else:
                    break

        if not final_home_cid:
            logger.error(f"  ➜ 无法获取或创建目标目录链 (已尝试所有手段)")
            return False
        
        if not candidates: return True

        moved_count = 0
        move_groups = {}
        unrecognized_fids = [] # ★ 终极垃圾桶：收集所有不符合要求的文件
        unqualified_items = [] # ★ 质检不合格垃圾桶
        
        # ★ 新增：用于记录本批次已经生成的目标文件名，防止同名冲突
        seen_new_filenames = set()

        # 媒体信息缓存以 p115_mediainfo_cache 数据库为唯一真理。
        # 不再批量查询中心服务器，也不再维护本轮内存媒体信息字典。
        pre_fetched_mediainfo = None
        local_pre_fetched_mediainfo = None

        # 仅补齐候选视频缺失的 SHA1，供后续 _fetch_and_parse_mediainfo 直读 DB / ffprobe 使用。
        for file_item in candidates:
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            if ext in known_video_exts:
                sha1 = file_item.get('sha1') or file_item.get('sha')
                if not sha1:
                    fid = file_item.get('fid') or file_item.get('file_id')
                    if fid:
                        try:
                            info_res = self.client.fs_get_info(fid)
                            if info_res.get('state') and info_res.get('data'):
                                sha1 = info_res['data'].get('sha1')
                                if sha1:
                                    file_item['sha1'] = sha1
                        except Exception:
                            pass

        # 确保 allowed_exts 有兜底，防止用户清空列表导致报错
        if not allowed_exts:
            allowed_exts = known_video_exts | {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

        # =================================================================
        # ★★★ 同批次字幕完美对齐视频命名 (解决 MP 单文件上传分离问题) ★★★
        # =================================================================
        batch_video_names = {} # key: (season, episode, part) -> base_name
        if is_batch:
            # 1. 预扫描视频，生成标准命名
            for file_item in candidates:
                fn = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                ext = fn.split('.')[-1].lower() if '.' in fn else ''
                if ext in known_video_exts:
                    # 临时调用重命名获取名字
                    v_name, v_s, v_e, _, _, _, v_part = self._rename_file_node(
                        file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), 
                        original_title=original_title, pre_fetched_mediainfo=pre_fetched_mediainfo, 
                        local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                        silent_log=True,  # ★ 开启静默，防止预扫描时重复打印日志
                        recognition_hints=self.recognition_hints,
                    )
                    video_base_name = fn.rsplit('.', 1)[0]
                    if not keep_original:
                        video_base_name = v_name.rsplit('.', 1)[0]
                    key = (v_s, v_e, v_part) if self.media_type == 'tv' else ('movie', v_part)
                    # 电影只保留第一个视频作为基准 (通常电影只有一个正片)
                    if key not in batch_video_names:
                        batch_video_names[key] = video_base_name
            
            # 2. 将视频基础名注入到同批次的字幕中
            if batch_video_names:
                for file_item in candidates:
                    fn = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                    ext = fn.split('.')[-1].lower() if '.' in fn else ''
                    if ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']:
                        s_num = file_item.get('_forced_season')
                        e_num = file_item.get('_forced_episode')
                        
                        # 提取字幕的 Part 信息
                        sub_part_num = None
                        sub_part_match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', fn)
                        if sub_part_match:
                            sub_part_num = int(sub_part_match.group(2))
                        
                        # ★ 电影无脑匹配逻辑
                        if self.media_type == 'movie':
                            m_key = ('movie', sub_part_num)
                            if m_key in batch_video_names:
                                file_item['_forced_base_name'] = batch_video_names[m_key]
                            elif ('movie', None) in batch_video_names:
                                file_item['_forced_base_name'] = batch_video_names[('movie', None)]
                            continue

                        # ★ 剧集匹配逻辑：使用强大的正则和纯数字兜底提取集号
                        if self.media_type == 'tv' and (s_num is None or e_num is None):
                            # 1. 标准特征匹配
                            match = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b|第\s*\d{1,4}\s*季\s*(\d{1,4})\s*[集话話回]|第\s*(\d{1,4})\s*[集话話回]', fn, re.IGNORECASE)
                            if match:
                                s = match.group(1)
                                e = match.group(2)
                                ep_only = match.group(3)
                                e_only = match.group(4)
                                zh_ep = match.group(5) or match.group(6)
                                if s_num is None: s_num = int(s) if s else None
                                if e_num is None: e_num = int(e) if e else (int(ep_only) if ep_only else (int(e_only) if e_only else int(zh_ep)))
                            
                            # 2. 纯数字兜底 (针对 01.srt, 02.ass 这种)
                            if e_num is None:
                                name_without_ext = fn.rsplit('.', 1)[0]
                                if name_without_ext.isdigit():
                                    e_num = int(name_without_ext)
                                else:
                                    clean_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', name_without_ext)
                                    anime_match = re.search(r'(?:\s-\s+)(\d{1,4})(?:\s|$)|\[(\d{1,4})\]|【(\d{1,4})】', clean_name)
                                    if anime_match:
                                        ep_str = anime_match.group(1) or anime_match.group(2) or anime_match.group(3)
                                        e_num = int(ep_str)
                                    else:
                                        end_match = re.search(r'(?:^|[ \.\-\_\[\(])(\d{1,4})(?:[\]\)]|\s*)$', clean_name)
                                        if end_match:
                                            e_num = int(end_match.group(1))
                                        else:
                                            mid_match = re.search(r'(?:^|[ \-\_\[\(])(\d{1,4})(?:[ \.\-\_\]\)]|$)', clean_name)
                                            if mid_match:
                                                e_num = int(mid_match.group(1))
                            
                            # 3. 季号兜底
                            if s_num is None:
                                s_num = getattr(self, 'forced_season', 1)
                        
                        key = (s_num, e_num, sub_part_num)
                        fallback_key = (s_num, e_num, None)
                        
                        if key in batch_video_names:
                            file_item['_forced_base_name'] = batch_video_names[key]
                            file_item['_forced_season'] = s_num
                            file_item['_forced_episode'] = e_num
                            logger.debug(f"  ➜ [字幕对齐] 剧集精准绑定: 字幕 '{fn}' -> 视频 '{batch_video_names[key]}'")
                        elif fallback_key in batch_video_names:
                            file_item['_forced_base_name'] = batch_video_names[fallback_key]
                            file_item['_forced_season'] = s_num
                            file_item['_forced_episode'] = e_num
                            logger.debug(f"  ➜ [字幕对齐] 剧集降级绑定: 字幕 '{fn}' -> 视频 '{batch_video_names[fallback_key]}'")
                        else:
                            logger.warning(f"  ➜ [字幕对齐] 警告：字幕 '{fn}' 提取到 S{s_num}E{e_num}，但未找到对应的视频文件！")

        # =================================================================
        # ★★★ 核心性能修复：内存级目录缓存 ★★★
        # 解决超大季/超多集整理时，频繁查询本地DB和请求115 API导致的严重卡死问题
        # =================================================================
        memory_dir_cache = {}
        
        # 提前拉取目标主目录下的现有文件夹，填充到内存缓存中 (★ 优化：直接查本地数据库，零 API 消耗)
        if final_home_cid:
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 直接拉取该目录下的所有缓存项，利用 parent_id 索引极速返回
                        cursor.execute("SELECT id, name FROM p115_filesystem_cache WHERE parent_id = %s", (str(final_home_cid),))
                        for row in cursor.fetchall():
                            d_name = row['name']
                            d_id = str(row['id'])
                            if d_name and d_id:
                                memory_dir_cache[f"{final_home_cid}_{d_name}"] = d_id
            except Exception as e:
                pass

        for file_item in candidates:
            # 兼容 OpenAPI 键名
            fid = file_item.get('fid') or file_item.get('file_id')
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))

            # 每个文件先初始化通用字段，防止 keep_original 分支漏赋值
            new_filename = file_name
            season_num = None
            episode_num = None
            s_name = None
            has_real_info = False
            video_info = {}
            part_num = None
            
            # 1. 扩展名绝对白名单校验 (最高优先级)
            if ext not in allowed_exts:
                logger.debug(f"  ➜ 扩展名 .{ext} 不在允许列表中，打入未识别: {file_name}")
                if fid: unrecognized_fids.append(fid)
                if progress_callback: progress_callback()
                continue

            # 2. 垃圾/花絮/样本校验 (仅针对视频)
            if ext in known_video_exts:
                if self._is_junk_file(file_name) or (0 < file_size < MIN_VIDEO_SIZE):
                    logger.debug(f"  ➜ 判定为花絮或体积过小，打入未识别: {file_name}")
                    if fid: unrecognized_fids.append(fid)
                    if progress_callback: progress_callback()
                    continue

            # 在重命名和查缓存前，如果缺失 SHA1，主动请求详情补齐 
            file_sha1 = file_item.get('sha1') or file_item.get('sha')
            if not file_sha1 and fid and ext in known_video_exts:
                try:
                    info_res = self.client.fs_get_info(fid)
                    if info_res.get('state') and info_res.get('data'):
                        fetched_sha1 = info_res['data'].get('sha1')
                        if fetched_sha1:
                            file_item['sha1'] = fetched_sha1 
                except Exception:
                    pass

            # =================================================================
            # ★ 保留原名模式：只保留文件名，不跳过内部解析
            # =================================================================
            if keep_original:
                # 调用统一命名解析器，只取内部结构化字段，不使用它生成的新文件名
                parsed_filename, season_num, episode_num, s_name, video_info, has_real_info, part_num = self._rename_file_node(
                    file_item,
                    safe_title,
                    year=year,
                    is_tv=(self.media_type == 'tv'),
                    original_title=original_title,
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                    silent_log=True,
                    recognition_hints=self.recognition_hints,
                )

                # ★ 核心：保留原名只对主视频生效。
                # 外挂字幕若已绑定到同批视频，仍需跟随视频基名改名，避免 Emby/Jellyfin 挂载失配。
                if ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'] and file_item.get('_forced_base_name'):
                    new_filename = parsed_filename
                else:
                    new_filename = file_name

                # ★ 目录仍走标准逻辑
                real_target_cid = final_home_cid

                # 剧集仍然进入标准季目录
                if self.media_type == 'tv' and season_num is not None and s_name:
                    cache_key = f"{final_home_cid}_{s_name}"

                    with _GLOBAL_DIR_LOCK:
                        s_cid = _GLOBAL_DIR_CACHE.get(cache_key)

                    if s_cid == 'FAILED':
                        real_target_cid = final_home_cid
                    else:
                        if not s_cid:
                            s_cid = P115CacheManager.get_cid(final_home_cid, s_name)

                        if s_cid:
                            real_target_cid = s_cid
                            with _GLOBAL_DIR_LOCK:
                                _GLOBAL_DIR_CACHE[cache_key] = s_cid
                        else:
                            s_mk = self.client.fs_mkdir(s_name, final_home_cid)
                            s_cid = s_mk.get('cid') if s_mk.get('state') else None

                            if not s_cid:
                                try:
                                    s_search = self.client.fs_files({
                                        'cid': final_home_cid,
                                        'search_value': s_name,
                                        'limit': 100,
                                        'show_dir': 1,
                                        'record_open_time': 0
                                    })
                                    for item in s_search.get('data', []):
                                        item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                        item_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                                        item_cid = (
                                            item.get('fid')
                                            or item.get('file_id')
                                            or item.get('id')
                                            or item.get('cid')
                                        )

                                        if item_fc == '0' and item_name == s_name and item_cid:
                                            s_cid = item_cid
                                            break
                                except Exception:
                                    pass

                            if s_cid:
                                P115CacheManager.save_cid(s_cid, final_home_cid, s_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = s_cid
                                real_target_cid = s_cid

                                season_rel_path = f"{base_rel_path}/{s_name}"
                                P115CacheManager.update_local_path(s_cid, season_rel_path)
                            else:
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = 'FAILED'
                                real_target_cid = final_home_cid

            else:
                new_filename, season_num, episode_num, s_name, video_info, has_real_info, part_num = self._rename_file_node(
                    file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), original_title=original_title,
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                    recognition_hints=self.recognition_hints,
                )

                real_target_cid = final_home_cid
                
                # ★ 直接使用返回的 s_name 创建/查找季目录
                if self.media_type == 'tv' and season_num is not None and s_name:
                    cache_key = f"{final_home_cid}_{s_name}"
                    
                    with _GLOBAL_DIR_LOCK:
                        s_cid = _GLOBAL_DIR_CACHE.get(cache_key)
                    
                    if s_cid == 'FAILED':
                        real_target_cid = final_home_cid
                    else:
                        if not s_cid:
                            s_cid = P115CacheManager.get_cid(final_home_cid, s_name)
                        
                        if s_cid:
                            real_target_cid = s_cid
                            with _GLOBAL_DIR_LOCK:
                                _GLOBAL_DIR_CACHE[cache_key] = s_cid
                        else:
                            s_mk = self.client.fs_mkdir(s_name, final_home_cid)
                            s_cid = s_mk.get('cid') if s_mk.get('state') else None
                            
                            if not s_cid: 
                                try:
                                    # ★ 核心修复：使用 fs_files + search_value 精准定位
                                    s_search = self.client.fs_files({
                                        'cid': final_home_cid, 
                                        'search_value': s_name,
                                        'limit': 100, 
                                        'show_dir': 1,
                                        'record_open_time': 0
                                    })
                                    for item in s_search.get('data', []):
                                        item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                        item_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                                        item_cid = (
                                            item.get('fid')
                                            or item.get('file_id')
                                            or item.get('id')
                                            or item.get('cid')
                                        )
                                        
                                        if item_fc == '0' and item_name == s_name and item_cid:
                                            s_cid = item_cid
                                            break
                                except Exception: pass
                            
                            if s_cid:
                                P115CacheManager.save_cid(s_cid, final_home_cid, s_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = s_cid
                                real_target_cid = s_cid
                                
                                # ★ 同步更新季目录的 local_path
                                season_rel_path = f"{base_rel_path}/{s_name}"
                                P115CacheManager.update_local_path(s_cid, season_rel_path)
                            else:
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = 'FAILED'
                                real_target_cid = final_home_cid

            # =================================================================
            # ★★★ 核心修复：严格去重逻辑 (防多版本/洗版残留冲突) ★★★
            # =================================================================
            if new_filename in seen_new_filenames:
                logger.warning(f"  ➜ [去重丢弃] 发现重复版本: '{file_name}' -> 目标名 '{new_filename}' 已被占用，当作垃圾打入未识别！")
                if fid: unrecognized_fids.append(fid)
                continue # 直接跳过，绝不重命名，绝不移动，绝不生成 STRM！
            
            # 记录已占用的文件名
            seen_new_filenames.add(new_filename)

            # 暂存入分组字典
            file_item['_new_filename'] = new_filename
            file_item['_season_num'] = season_num
            file_item['_episode_num'] = episode_num
            file_item['_s_name'] = s_name
            file_item['_video_info'] = video_info
            
            if real_target_cid not in move_groups:
                move_groups[real_target_cid] = []
            move_groups[real_target_cid].append(file_item)

        # =================================================================
        # ★★★ 执行批量移动与后续 STRM 生成 ★★★
        # =================================================================
        conflict_mode = cfg.get('conflict_mode', 'replace') # 获取覆盖模式，默认洗版替换
        
        # ★★★ 洗版特权检测 (细化到单集) ★★★
        active_washing_eps = set()
        movie_active_washing = False
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if self.media_type == 'tv':
                        # 查出该剧所有带有特权的分集
                        cursor.execute("SELECT season_number, episode_number FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND active_washing = TRUE", (str(self.tmdb_id),))
                        for row in cursor.fetchall():
                            active_washing_eps.add((row['season_number'], row['episode_number']))
                    else:
                        cursor.execute("SELECT active_washing FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Movie'", (str(self.tmdb_id),))
                        row = cursor.fetchone()
                        if row and row.get('active_washing'):
                            movie_active_washing = True
        except Exception as e:
            pass

        if active_washing_eps or movie_active_washing:
            logger.info(f"  ➜ [洗版特权] 检测到当前媒体存在洗版特权标记，命中特权的文件将强制替换旧版！")
        
        for batch_target_cid, items in move_groups.items():
            # -----------------------------------------------------------
            # ★ 1. 移动前：拉取目标目录现有文件，进行冲突检测 (保持不变)
            # -----------------------------------------------------------
            existing_names = {}      
            existing_tv_eps = {}     
            existing_movie_vids = [] 
            
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT id, name FROM p115_filesystem_cache WHERE parent_id = %s", (str(batch_target_cid),))
                        for row in cursor.fetchall():
                            e_name = row['name']
                            e_fid = str(row['id'])
                            e_ext = e_name.split('.')[-1].lower() if '.' in e_name else ''
                            
                            if e_ext in known_video_exts:
                                existing_names[e_name] = e_fid
                                if self.media_type == 'tv':
                                    match = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b', e_name, re.IGNORECASE)
                                    if match:
                                        s, e = int(match.group(1)), int(match.group(2))
                                        if (s, e) not in existing_tv_eps: existing_tv_eps[(s, e)] = []
                                        existing_tv_eps[(s, e)].append(e_fid)
                                else:
                                    existing_movie_vids.append(e_fid)
            except Exception as e:
                logger.warning(f"  ➜ [冲突检测] 查询本地缓存失败: {e}")

            # =================================================================
            # ★★★ 核心修复 1：视频优先排序 ★★★
            # =================================================================
            items.sort(key=lambda x: 0 if (x['_new_filename'].split('.')[-1].lower() in known_video_exts) else 1)
            known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

            # =================================================================
            # ★★★ 核心修复 4：外挂字幕豁免机制 ★★★
            # 扫描本批次，记录哪些视频带有外挂字幕 (支持电影和剧集)
            # =================================================================
            episodes_with_ext_subs = set()
            for item in items:
                temp_name = item['_new_filename']
                temp_ext = temp_name.split('.')[-1].lower() if '.' in temp_name else ''
                if temp_ext in known_sub_exts:
                    episodes_with_ext_subs.add((item.get('_season_num'), item.get('_episode_num')))

            valid_items = []
            fids_to_delete = set()
            rejected_episodes = set()

            from handler.resubscribe_service import WashingService
            original_lang = (self.raw_metadata or {}).get('lang_code')

            # 同一批待整理里可能同时出现同一电影/同一集的多个视频版本。
            # decide_washing_action() 只能和“已入库旧版”比较；批内第一个候选尚未入库，
            # 后续候选不会自然触发“旧版对比”，因此这里额外做一次批内 PK，防止多版本同时入库。
            batch_washing_best = {}

            def _batch_washing_identity_key(_item, _is_video):
                if not _is_video:
                    return None
                if self.media_type == 'movie':
                    return ('movie', str(self.tmdb_id))
                _s = _item.get('_season_num')
                _e = _item.get('_episode_num')
                if _s is None or _e is None:
                    return None
                try:
                    return ('episode', int(_s), int(_e))
                except Exception:
                    return ('episode', str(_s), str(_e))

            def _batch_washing_level_from_reason(_reason):
                text = str(_reason or '')
                m = re.search(r'优先级\s*([0-9]+)', text)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        pass
                # 手动重组/特权替换可能没有优先级文本；这种情况下不按规则等级压制，
                # 只在同批候选之间用体积做兜底。
                return 9999

            def _batch_washing_score(_level, _file_size):
                try:
                    size_int = int(_file_size or 0)
                except Exception:
                    size_int = 0
                # level 越小越好；同级体积越大越优。
                return (int(_level or 9999), -size_int)

            def _build_washing_snapshot(_item, _new_name, _reason='', _file_size=0, _has_ext_sub=False):
                """给 p115_filesystem_cache 写入洗版优先级快照。失败只返回空快照，不影响整理。"""
                try:
                    file_sha1 = _item.get('sha1') or _item.get('sha')
                    level = _batch_washing_level_from_reason(_reason)
                    level_reason = str(_reason or '').strip()

                    # reason 里没有优先级时，现场按 RAW 再算一次，保证非 replace/手动重组也能留下快照。
                    if level >= 9999 and file_sha1:
                        raw_info = WashingService._get_raw_info_by_sha1(file_sha1)
                        if isinstance(raw_info, list) and raw_info:
                            new_info = dict(raw_info[0])
                        elif isinstance(raw_info, dict):
                            new_info = dict(raw_info)
                        else:
                            new_info = {}
                        if new_info:
                            new_info['filename'] = _new_name
                            new_info['_file_size'] = _file_size
                            new_info['_original_lang'] = original_lang
                            new_info['has_external_subtitle'] = bool(_has_ext_sub)
                            new_info['_media_type'] = self.media_type
                            new_info['_tmdb_id'] = self.tmdb_id
                            new_info['_season_num'] = _item.get('_season_num')
                            new_info['_episode_num'] = _item.get('_episode_num')
                            db_media_type = 'Movie' if self.media_type == 'movie' else 'Series'
                            priorities = WashingService._load_priorities(db_media_type, str(target_cid))
                            if priorities:
                                new_info['_need_clean_version_check'] = WashingService._priorities_need_clean_version(priorities)
                                norm_new = WashingService._normalize_info(new_info)
                                level, level_reason = WashingService.get_level(norm_new, priorities)

                    if level >= 9999:
                        return {}

                    identity = {
                        'tmdb_id': str(self.tmdb_id or ''),
                        'media_type': self.media_type,
                    }
                    if self.media_type == 'tv':
                        identity.update({
                            'season_number': _item.get('_season_num'),
                            'episode_number': _item.get('_episode_num'),
                        })
                    from datetime import datetime, timezone
                    return {
                        'washing_level': int(level),
                        'washing_snapshot_json': {
                            'reason': level_reason or f'优先级 {level}',
                            'target_cid': str(target_cid or ''),
                            'media_type': 'movie' if self.media_type == 'movie' else 'series',
                            'identity': identity,
                            'evaluated_at': datetime.now(timezone.utc).isoformat()
                        }
                    }
                except Exception as e:
                    logger.debug(f"  ➜ [洗版快照] 计算失败: {_new_name} -> {e}")
                    return {}

            def _register_batch_washing_candidate(_item, _new_name, _action, _reason, _file_size):
                key = _batch_washing_identity_key(_item, True)
                if key is None:
                    return True

                level = _batch_washing_level_from_reason(_reason)
                score = _batch_washing_score(level, _file_size)
                current = batch_washing_best.get(key)

                if not current:
                    batch_washing_best[key] = {
                        'item': _item,
                        'name': _new_name,
                        'level': level,
                        'score': score,
                        'action': _action,
                        'reason': _reason,
                    }
                    return True

                old_item = current.get('item')
                old_name = current.get('name')
                old_level = current.get('level')
                old_score = current.get('score')

                if score < old_score:
                    try:
                        valid_items.remove(old_item)
                    except ValueError:
                        pass
                    old_fid = old_item.get('fid') or old_item.get('file_id') if isinstance(old_item, dict) else None
                    if old_fid:
                        unrecognized_fids.append(old_fid)
                    batch_washing_best[key] = {
                        'item': _item,
                        'name': _new_name,
                        'level': level,
                        'score': score,
                        'action': _action,
                        'reason': _reason,
                    }
                    logger.info(
                        f"  ➜ [批内洗版淘汰] {old_name} -> 同批候选 {_new_name} 更优/同级更大，"
                        f"old_level={old_level}, new_level={level}"
                    )
                    return True

                fid = _item.get('fid') or _item.get('file_id')
                if fid:
                    unrecognized_fids.append(fid)
                logger.info(
                    f"  ➜ [批内洗版跳过] {_new_name} -> 同批已有更优/同级候选 {old_name}，"
                    f"old_level={old_level}, new_level={level}"
                )
                return False
            
            for item in items:
                new_name = item['_new_filename']
                s_num = item.get('_season_num')
                e_num = item.get('_episode_num')
                ext = new_name.split('.')[-1].lower() if '.' in new_name else ''
                is_vid = ext in known_video_exts
                file_size = _parse_115_size(item.get('fs') or item.get('size'))
                
                # 检查带头大哥是否已经挂了。仅剧集允许按 S/E 株连字幕/关联文件；
                # 电影的 s_num/e_num 都是 None，不能把 (None, None) 放进黑名单，
                # 否则一个电影版本失败会误杀后续所有电影视频。
                if (
                    self.media_type == 'tv'
                    and s_num is not None
                    and e_num is not None
                    and (s_num, e_num) in rejected_episodes
                ):
                    logger.info(f"  ➜ [关联跳过] 同集视频已被拦截/跳过，同步忽略关联文件: {new_name}")
                    unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                    continue

                # 判断当前文件是否享有洗版特权
                is_ep_active_washing = False
                if self.media_type == 'tv' and s_num is not None and e_num is not None:
                    is_ep_active_washing = (s_num, e_num) in active_washing_eps
                elif self.media_type == 'movie':
                    is_ep_active_washing = movie_active_washing
                
                effective_conflict_mode = 'replace' if is_ep_active_washing else conflict_mode

                # ★ 判断是否享有外挂字幕豁免权
                has_ext_sub = (s_num, e_num) in episodes_with_ext_subs

                # 调用阶梯洗版优先级服务
                if is_vid and effective_conflict_mode == 'replace':
                    # ★ 核心修复：手动重组拥有最高特权，无视洗版规则直接放行！
                    if getattr(self, 'is_manual_correct', False):
                        action = 'REPLACE'
                        reason = '手动重组，无视洗版规则强制放行'
                    else:
                        logger.debug(f"  ➜ [覆盖模式:洗版] 正在调用洗版规则评估文件: {new_name}")
                        
                        video_info = item.get('_video_info') or self._extract_video_info(new_name)
                        file_sha1 = item.get('sha1') or item.get('sha')
                        
                        action, reason = WashingService.decide_washing_action(
                            sha1=file_sha1,
                            file_name=new_name,
                            file_size=file_size,
                            target_cid=target_cid,
                            media_type=self.media_type,
                            tmdb_id=self.tmdb_id,
                            season_num=s_num,
                            episode_num=e_num,
                            original_lang=original_lang,
                            is_active_washing=is_ep_active_washing,
                            has_external_subtitle=has_ext_sub # ★★★ 传入外挂字幕豁免标志 ★★★
                        )
                    
                    if action == 'REJECT':
                        logger.warning(f"  ➜ [洗版拦截] {new_name} -> {reason}")
                        unqualified_items.append({
                            'fid': item.get('fid') or item.get('file_id'), 'name': item.get('fn') or item.get('file_name'), 
                            'reason': reason, 'pc': item.get('pc') or item.get('pick_code'), 'season_num': s_num
                        })
                        # 剧集才按 S/E 记黑名单，电影不能写入 (None, None)。
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            rejected_episodes.add((s_num, e_num))
                        continue
                    elif action == 'SKIP':
                        logger.info(f"  ➜ [洗版跳过] {new_name}，原因：{reason}")
                        unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                        # 剧集才按 S/E 记黑名单，电影不能写入 (None, None)。
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            rejected_episodes.add((s_num, e_num))
                        continue
                    elif action == 'REPLACE':
                        logger.info(f"  ➜ [洗版替换] {new_name}，原因：{reason}")
                        item['_washing_snapshot'] = _build_washing_snapshot(item, new_name, reason, file_size, has_ext_sub)
                        if not _register_batch_washing_candidate(item, new_name, action, reason, file_size):
                            continue
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            fids_to_delete.update(existing_tv_eps.get((s_num, e_num), []))
                        else:
                            fids_to_delete.update(existing_movie_vids)
                        valid_items.append(item)
                    elif action == 'ACCEPT':
                        logger.info(f"  ➜ [洗版入库] {new_name}，原因：{reason}")
                        item['_washing_snapshot'] = _build_washing_snapshot(item, new_name, reason, file_size, has_ext_sub)
                        if not _register_batch_washing_candidate(item, new_name, action, reason, file_size):
                            continue
                        if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                        valid_items.append(item)
                else:
                    # 非视频文件，或非替换模式
                    is_conflict = False
                    conflict_old_fids = []
                    if is_vid:
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            if (s_num, e_num) in existing_tv_eps:
                                is_conflict = True
                                conflict_old_fids = existing_tv_eps[(s_num, e_num)]
                        elif self.media_type == 'movie':
                            if existing_movie_vids:
                                is_conflict = True
                                conflict_old_fids = existing_movie_vids
                    
                    if is_conflict:
                        if conflict_mode == 'skip':
                            logger.info(f"  ➜ [覆盖模式:跳过] 目标目录已存在同集/同电影，放弃处理: {new_name}")
                            unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                            continue 
                        elif conflict_mode == 'keep_both':
                            logger.info(f"  ➜ [覆盖模式:共存] 目标目录已存在同集/同电影，保留两者: {new_name}")
                            if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                            valid_items.append(item)
                    else:
                        if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                        valid_items.append(item)
            
            if not valid_items:
                continue # 这批全被 skip/reject 了
                
            # -----------------------------------------------------------
            # ★ 2. 执行删除旧文件 (洗版/同名覆盖 + 完美擦屁股)
            # -----------------------------------------------------------
            if fids_to_delete:
                logger.warning(f"  ➜ [版本控制] 正在删除 {len(fids_to_delete)} 个被替换的旧版本文件...")

                # === 共享资源主动下架：洗版窗口期内先通知中心端禁用旧源 ===
                # 维护任务会兜底清理“本地已换版/删除”的共享源，但维护窗口内中心仍可能
                # 派发 holder 签名任务。这里在物理删除旧 115 文件前精准下架，避免旧源
                # 已删除后继续参与签名导致贡献点被平白扣除。
                try:
                    from tasks import shared_resource_tasks
                    disable_res = shared_resource_tasks.disable_shared_sources_for_deleted_fids(
                        list(fids_to_delete),
                        reason='washing_replaced_old_version',
                        message='local file replaced by washing; disable old shared source before deleting old version',
                    )
                    if disable_res.get('matched'):
                        logger.info(
                            "  ➜ [版本控制] 洗版旧共享源主动下架完成：匹配 %s，成功 %s，失败 %s",
                            disable_res.get('matched', 0),
                            disable_res.get('disabled', 0),
                            disable_res.get('failed', 0),
                        )
                except Exception as e:
                    # 下架失败不能阻塞洗版删除；失败源会保留本地 active/reported 锚点，
                    # 后续共享资源维护任务会继续尝试中心下架。
                    logger.warning(f"  ➜ [版本控制] 洗版旧共享源主动下架异常，继续删除旧版并等待维护任务兜底: {e}")
                
                # === 本地擦屁股逻辑 ===
                local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
                old_strm_paths_for_emby = []
                old_dirs_to_check = set() # ★ 新增：记录需要检查是否为空的旧目录
                
                if local_root and os.path.exists(local_root):
                    try:
                        from database.connection import get_db_connection
                        with get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                # 从缓存查出旧文件的本地路径
                                cursor.execute("SELECT local_path FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(fids_to_delete),))
                                for row in cursor.fetchall():
                                    old_file_rel_path = row['local_path']
                                    if not old_file_rel_path: continue
                                    
                                    old_file_rel_path = str(old_file_rel_path).lstrip('\\/')
                                    old_strm_rel_path = os.path.splitext(old_file_rel_path)[0] + ".strm"
                                    old_strm_full_path = os.path.join(local_root, old_strm_rel_path)
                                    
                                    old_strm_paths_for_emby.append(old_strm_full_path)
                                    
                                    # 1. 删除 STRM
                                    if os.path.exists(old_strm_full_path):
                                        os.remove(old_strm_full_path)
                                        logger.debug(f"  ➜ 删除本地旧 STRM: {old_strm_full_path}")
                                        
                                    # 2. 删除 mediainfo.json
                                    old_mi_full_path = os.path.splitext(old_file_rel_path)[0] + "-mediainfo.json"
                                    if os.path.exists(old_mi_full_path):
                                        os.remove(old_mi_full_path)
                                        
                                    # 3. 删除关联字幕和专属 NFO
                                    old_dir_full_path = os.path.dirname(old_strm_full_path)
                                    old_base_name = os.path.splitext(os.path.basename(old_file_rel_path))[0]
                                    if os.path.exists(old_dir_full_path):
                                        old_dirs_to_check.add(old_dir_full_path) # ★ 记录旧目录
                                        for f in os.listdir(old_dir_full_path):
                                            # ★ 核心修复：把 nfo 和图片也加进去，连同字幕一起删，彻底擦干净屁股
                                            if f.startswith(old_base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup', 'nfo', 'jpg', 'png', 'jpeg', 'bif']:
                                                sub_to_del = os.path.join(old_dir_full_path, f)
                                                try:
                                                    os.remove(sub_to_del)
                                                    logger.debug(f"  ➜ 删除本地旧附属文件: {sub_to_del}")
                                                except: pass
                                                
                        # ★ 4. 向上递归清理本地空目录 (连锅端海报和tvshow.nfo)
                        if old_dirs_to_check:
                            import shutil
                            protected_dirs = {os.path.abspath(local_root)}
                            for rule in self.rules:
                                cat_path = rule.get('category_path') or rule.get('dir_name')
                                if cat_path:
                                    protected_dirs.add(os.path.abspath(os.path.join(local_root, cat_path.lstrip('\\/'))))
                            protected_dirs.add(os.path.abspath(os.path.join(local_root, "未识别")))

                            for old_dir in list(old_dirs_to_check):
                                curr_dir = old_dir
                                while curr_dir and curr_dir not in protected_dirs:
                                    if os.path.exists(curr_dir):
                                        has_media = False
                                        for root_dir, _, files in os.walk(curr_dir):
                                            for f in files:
                                                ext = f.split('.')[-1].lower()
                                                if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov'}:
                                                    has_media = True
                                                    break
                                            if has_media: break

                                        if not has_media:
                                            shutil.rmtree(curr_dir)
                                            logger.info(f"  ➜ 本地旧目录已无媒体文件，连目录删除: {curr_dir}")
                                            curr_dir = os.path.dirname(curr_dir)
                                        else:
                                            break
                                    else:
                                        break
                    except Exception as e:
                        logger.warning(f"  ➜ 清理本地旧文件失败: {e}")
                
                # === 执行网盘删除和缓存清理 ===
                self.client.fs_delete(list(fids_to_delete))
                P115CacheManager.delete_files(list(fids_to_delete))
                P115RecordManager.delete_records(list(fids_to_delete))

                # === 通知 Emby 立即扫描旧路径，清理被删除的旧版本条目 ===
                if old_strm_paths_for_emby:
                    emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
                    emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
                    if emby_url and emby_api_key:
                        try:
                            from handler import emby
                            logger.info(
                                f"  ➜ [版本控制] 已删除旧版本，通知 Emby 极速扫描 {len(old_strm_paths_for_emby)} 个旧 STRM 路径..."
                            )
                            emby.notify_emby_file_changes(
                                old_strm_paths_for_emby,
                                emby_url,
                                emby_api_key,
                                update_type="Deleted",
                            )
                        except Exception as e:
                            logger.warning(f"  ➜ [版本控制] 通知 Emby 扫描旧版本路径失败: {e}")
                
            # -----------------------------------------------------------
            # ★ 3. 执行移动新文件
            # -----------------------------------------------------------
            move_fids = [item.get('fid') or item.get('file_id') for item in valid_items]
            move_res = self.client.fs_move(move_fids, batch_target_cid)
            
            if move_res.get('state'):
                display_target = std_root_name
                if valid_items and valid_items[0].get('_s_name'):
                    display_target = f"{std_root_name} - {valid_items[0]['_s_name']}"
                logger.info(f"  ➜ [批量移动] 已将 {len(move_fids)} 个文件移动到：{display_target}")
                
                # -----------------------------------------------------------
                # ★ 4. 执行重命名
                # -----------------------------------------------------------
                rename_items = []
                for item in valid_items:
                    fid = str(item.get('fid') or item.get('file_id') or '').strip()
                    old_name = item.get('fn') or item.get('n') or item.get('file_name')
                    new_name = item['_new_filename']
                    if fid and old_name != new_name:
                        rename_items.append({
                            'fid': fid,
                            'old_name': old_name,
                            'new_name': new_name,
                            'item': item,
                        })

                if rename_items:
                    rename_pairs = [(x['fid'], x['new_name']) for x in rename_items]
                    if hasattr(self.client, 'fs_rename_batch'):
                        ren_res = self.client.fs_rename_batch(rename_pairs)
                    else:
                        # 极旧客户端兜底：保持原有逐条行为。
                        failures = {}
                        successes = []
                        for fid, new_name in rename_pairs:
                            one_res = self.client.fs_rename((fid, new_name))
                            if one_res.get('state'):
                                successes.append(fid)
                            else:
                                failures[fid] = one_res
                        ren_res = {
                            'state': not failures,
                            '_rename_mode': 'legacy_sequential',
                            '_rename_successes': successes,
                            '_rename_failures': failures,
                            'data': {
                                'total': len(rename_pairs),
                                'success_count': len(successes),
                                'failed_count': len(failures),
                            }
                        }

                    failures = ren_res.get('_rename_failures') or {}
                    mode = ren_res.get('_rename_mode') or 'unknown'
                    success_count = len(rename_items) - len(failures)

                    if success_count > 0:
                        if mode == 'cookie_batch':
                            logger.info(f"  ➜ [批量重命名] 重命名完成：成功 {success_count} 个，共 {len(rename_items)} 个。")
                        else:
                            logger.info(f"  ➜ [逐条重命名] 重命名完成：成功 {success_count} 个，共 {len(rename_items)} 个。")

                    for x in rename_items:
                        fail_res = failures.get(x['fid'])
                        if fail_res:
                            logger.warning(
                                f"  ➜ [重命名失败] {x['old_name']} -> {x['new_name']}, "
                                f"原因: {_p115_error_text(fail_res)}"
                            )
                            # 如果 115 API 重命名失败，强制退回原名，确保后续生成的 STRM 指向正确文件。
                            x['item']['_new_filename'] = x['old_name']
                        else:
                            logger.debug(f"  ➜ [重命名] {x['old_name']} -> {x['new_name']}")
                
                # -----------------------------------------------------------
                # ★ 5. 生成 STRM 和记录日志
                # -----------------------------------------------------------
                processed_episodes_for_flag = set() # ★ 新增：记录成功处理的集数
                
                for file_item in valid_items:
                    fid = file_item.get('fid') or file_item.get('file_id')
                    file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                    new_filename = file_item['_new_filename']
                    season_num = file_item['_season_num']
                    s_name = file_item['_s_name']
                    
                    moved_count += 1
                    
                    # ★ 记录成功处理的集数，用于后续精准核销特权
                    if self.media_type == 'tv' and season_num is not None and file_item.get('_episode_num') is not None:
                        processed_episodes_for_flag.add((season_num, file_item.get('_episode_num')))
                    pick_code = file_item.get('pc') or file_item.get('pick_code') 
                    file_sha1 = file_item.get('sha1') or file_item.get('sha')
                    ext = new_filename.split('.')[-1].lower() if '.' in new_filename else ''
                    
                    # 整理日志
                    if ext in known_video_exts:
                        try:
                            category_name = "未识别"
                            for rule in self.rules:
                                if str(rule.get('cid')) == str(target_cid):
                                    category_name = rule.get('dir_name', '未识别')
                                    break
                            P115RecordManager.add_or_update_record(
                                file_id=fid,
                                original_name=file_name,
                                status='success',
                                tmdb_id=self.tmdb_id,
                                media_type=self.media_type,
                                target_cid=target_cid,
                                category_name=category_name,
                                renamed_name=new_filename,
                                pick_code=pick_code,
                                season_number=season_num 
                            )
                        except Exception as e:
                            logger.error(f"  ➜ 记录文件整理日志失败: {e}")

                    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
                    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "http://127.0.0.1:5257").rstrip('/')
                    
                    if pick_code and local_root and os.path.exists(local_root):
                        if not etk_url.startswith('http'):
                            logger.error("  ➜ 请配置 http(s) 开头的 ETK 访问地址。")
                            return False
                        try:
                            category_name = None
                            for rule in self.rules:
                                if rule.get('cid') == str(target_cid):
                                    category_name = rule.get('dir_name', '未识别')
                                    break
                            if not category_name: category_name = "未识别"

                            category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
                            relative_category_path = "未识别"
                            
                            if category_rule:
                                if 'category_path' in category_rule and category_rule['category_path']:
                                    relative_category_path = category_rule['category_path']
                                else:
                                    media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
                                    try:
                                        dir_info = self.client.fs_files({'cid': target_cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                                        path_nodes = dir_info.get('path', [])
                                        start_idx = 0
                                        found_root = False
                                        
                                        if media_root_cid == '0':
                                            if str(target_cid) == '0': start_idx = 0
                                            else: start_idx = 1 
                                            found_root = True
                                        else:
                                            for i, node in enumerate(path_nodes):
                                                if str(node.get('cid') or node.get('file_id')) == media_root_cid:
                                                    start_idx = i + 1
                                                    found_root = True
                                                    break
                                        
                                        if found_root and start_idx < len(path_nodes):
                                            rel_segments = [str(n.get('file_name') or n.get('fn') or n.get('name') or n.get('n')).strip() for n in path_nodes[start_idx:] if (n.get('file_name') or n.get('fn') or n.get('name') or n.get('n'))]
                                            relative_category_path = "/".join(rel_segments) if rel_segments else category_rule.get('dir_name', '未识别')
                                        else:
                                            relative_category_path = category_rule.get('dir_name', '未识别')
                                            
                                        category_rule['category_path'] = relative_category_path
                                        settings_db.save_setting('p115_sorting_rules', self.rules)
                                        
                                    except Exception as e:
                                        relative_category_path = category_rule.get('dir_name', '未识别')

                            # ★ 保留原名只影响文件名，本地目录仍走标准结构
                            if self.media_type == 'tv' and season_num is not None and s_name:
                                local_dir = os.path.join(local_root, relative_category_path, std_root_name, s_name)
                            else:
                                local_dir = os.path.join(local_root, relative_category_path, std_root_name)
                            
                            os.makedirs(local_dir, exist_ok=True) 

                            try:
                                main_folder_path = os.path.join(relative_category_path, std_root_name)
                                P115CacheManager.update_local_path(final_home_cid, main_folder_path)
                                # ★ 保留原名不再复刻原始子目录，只按标准季目录更新缓存
                                if self.media_type == 'tv' and season_num is not None and s_name:
                                    P115CacheManager.update_local_path(batch_target_cid, os.path.join(main_folder_path, s_name))
                            except Exception: pass 

                            is_video = ext in known_video_exts
                            is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']

                            if is_video:
                                strm_filename = os.path.splitext(new_filename)[0] + ".strm"
                                strm_filepath = os.path.join(local_dir, strm_filename)
                                strm_content = f"{etk_url}/api/p115/play/{pick_code}"
                                if cfg.get('strm_url_fmt') == 'with_name':
                                    strm_content = f"{strm_content}/{new_filename}"
                                
                                with open(strm_filepath, 'w', encoding='utf-8') as f:
                                    f.write(strm_content)
                                logger.info(f"  ➜ 已生成 STRM：{strm_filename}")
                                
                                try:
                                    from monitor_service import enqueue_file_actively
                                    enqueue_file_actively(strm_filepath)
                                except Exception: pass

                                if not file_sha1 and fid:
                                    try:
                                        info_res = self.client.fs_get_info(fid)
                                        if info_res.get('state') and info_res.get('data'):
                                            file_sha1 = info_res['data'].get('sha1')
                                    except Exception: pass

                                if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                                    try:
                                        mediainfo_filename = os.path.splitext(new_filename)[0] + "-mediainfo.json"
                                        mediainfo_filepath = os.path.join(local_dir, mediainfo_filename)
                                        mediainfo_text = P115CacheManager.get_mediainfo_cache_text(file_sha1) if file_sha1 else None
                                        if mediainfo_text:
                                            with open(mediainfo_filepath, 'w', encoding='utf-8') as f:
                                                f.write(mediainfo_text)
                                            logger.info(f"  ➜ 已生成媒体信息文件：{mediainfo_filename}")
                                        else:
                                            logger.debug(f"  ➜ 跳过媒体信息文件生成，未命中本地缓存: {new_filename}")
                                    except Exception as e:
                                        logger.error(f"  ➜ 生成媒体信息文件失败: {e}")

                                # ★ 保留原名只影响文件名，缓存路径仍走标准目录
                                if self.media_type == 'tv' and season_num is not None and s_name:
                                    file_local_path = os.path.join(relative_category_path, std_root_name, s_name, new_filename)
                                else:
                                    file_local_path = os.path.join(relative_category_path, std_root_name, new_filename)
                                
                                file_local_path = file_local_path.replace('\\', '/')
                                file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))

                                washing_snapshot = file_item.get('_washing_snapshot') if isinstance(file_item, dict) else {}
                                if is_video and not washing_snapshot:
                                    washing_snapshot = _build_washing_snapshot(file_item, new_filename, '', file_size, False)

                                if pick_code and fid:
                                    P115CacheManager.save_file_cache(
                                        fid, batch_target_cid, new_filename, 
                                        sha1=file_sha1, pick_code=pick_code, 
                                        local_path=file_local_path, size=file_size,
                                        preid=file_item.get('preid'),
                                        **(washing_snapshot or {})
                                    )
                                    # 负载均衡分享资产记录钩子：
                                    # 对影巢/TG 等外部分享导入后再整理的资源，只有这里拿到的
                                    # fid / pick_code / sha1 才是 STRM 实际播放文件身份。
                                    share_ctx = getattr(self, 'external_share_context', None) or getattr(self, '_external_share_context', None)
                                    if share_ctx:
                                        try:
                                            from database import p115_pool_db
                                            p115_pool_db.record_share_asset_file_from_organize_context(share_ctx, {
                                                'source_file_id': fid,
                                                'source_pick_code': pick_code,
                                                'sha1': file_sha1,
                                                'file_name': new_filename,
                                                'file_size': file_size,
                                                'parent_cid': batch_target_cid,
                                                'season_number': season_num,
                                                'episode_number': file_item.get('_episode_num'),
                                            })
                                        except Exception as e:
                                            logger.debug(f"  ➜ [负载均衡] 整理钩子记录分享文件失败: {e}")
                                    
                            elif is_sub:
                                if config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True):
                                    sub_filepath = os.path.join(local_dir, new_filename)
                                    if not os.path.exists(sub_filepath):
                                        try:
                                            url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                                            if url_obj:
                                                import requests
                                                headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                                resp = requests.get(str(url_obj), stream=True, timeout=30, headers=headers)
                                                resp.raise_for_status()
                                                with open(sub_filepath, 'wb') as f:
                                                    for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
                                                logger.info(f"  ➜ [字幕下载] {new_filename} 下载完成！")
                                        except Exception as e:
                                            logger.error(f"  ➜ 下载字幕失败: {e}")
                            
                        except Exception as e:
                            logger.error(f"  ➜ 生成 STRM 文件失败: {e}", exc_info=True)
                    if progress_callback:
                        progress_callback()
            else:
                raw_err_msg = str(move_res.get('error_msg', move_res))
                if (
                    'Expecting value: line 1 column 1 (char 0)' in raw_err_msg
                    or 'JSONDecodeError' in raw_err_msg
                    or '<html' in raw_err_msg.lower()
                    or '<!doctype html' in raw_err_msg.lower()
                ):
                    err_msg = '该片无法整理，请手动重命名移动后增量生成STRM。'
                else:
                    err_msg = raw_err_msg

                logger.error(f"  ➜ [批量移动失败] 目标CID:{batch_target_cid}, 包含 {len(move_fids)} 个文件, 原因: {err_msg}")
                
                if '不存在' in raw_err_msg or move_res.get('code') in [20004, 70004]:
                    logger.warning(f"  ➜ 检测到目标目录在网盘中已不存在，正在清理失效缓存: CID {batch_target_cid}")
                    P115CacheManager.delete_cid(batch_target_cid)
                if progress_callback:
                    for _ in valid_items:
                        progress_callback()

        # =================================================================
        # ★★★ 终极清理：将所有不合规文件移入未识别目录 ★★★
        # =================================================================
        if unrecognized_fids and unidentified_cid:
            logger.info(f"  ➜ 发现 {len(unrecognized_fids)} 个不合规文件(扩展名不符/花絮/样本/广告)，正在移入未识别目录...")
            # 同样传入列表，防止 115 API 报错
            self.client.fs_move(unrecognized_fids, unidentified_cid)
            
        if unqualified_items and unidentified_cid:
            logger.info(f"  ➜ 发现 {len(unqualified_items)} 个质检不合格文件，正在移入未识别目录...")
            unq_fids = [item['fid'] for item in unqualified_items if item['fid']]
            self.client.fs_move(unq_fids, unidentified_cid)
            
            for item in unqualified_items:
                P115RecordManager.add_or_update_record(
                    file_id=item['fid'],
                    original_name=item['name'],
                    status='unqualified',
                    tmdb_id=self.tmdb_id,
                    media_type=self.media_type,
                    target_cid=target_cid,
                    category_name="质检不合格",
                    renamed_name=None,
                    pick_code=item['pc'],
                    season_number=item['season_num'],
                    fail_reason=item['reason']
                )
                
            # ★★★ 触发 TG 拦截通知 (聚合版) ★★★
            try:
                from handler.telegram import send_intercept_notification
                grouped_unqualified = {}
                for item in unqualified_items:
                    reason = item['reason']
                    if reason not in grouped_unqualified:
                        grouped_unqualified[reason] = []
                    grouped_unqualified[reason].append(item['name'])
                    
                for reason, names in grouped_unqualified.items():
                    send_intercept_notification(names, reason)
            except Exception as e:
                logger.error(f"  ➜ 触发拦截通知失败: {e}")

        # =================================================================
        # ★ 极简垃圾回收：直接通知缓冲队列检查“待整理”目录
        # =================================================================
        if not skip_gc:
            if not (not is_batch and root_item.get('_skip_gc')):
                logger.info(f"  ➜ [清理空目录] 整理完毕，已通知全局垃圾回收器检查待整理目录...")
                from handler.p115_service import P115DeleteBuffer
                P115DeleteBuffer.add(check_save_path=True)
            else:
                logger.info("  ➜ [MP上传] 单文件跳过垃圾回收检查。")
        else:
            logger.debug("  ➜ [清理空目录] 批量任务模式，跳过单次垃圾回收检查，等待统一清理。")

        # --- active_washing 状态收口 ---
        # 整理模块只读取 active_washing 来获得洗版替换特权，不再在单集入库后核销它。
        # 完结洗版可能分多批入库；如果这里按单集清理，会导致追剧模块误以为洗版事务已结束，
        # 进而在 Completed -> Completed 刷新中失去事务锁。active_washing 的开启/清理统一由
        # watchlist_processor 的完结质量门禁决定：一致性通过后整季清理，超时维护再兜底清理。
        if moved_count > 0 and (active_washing_eps or movie_active_washing):
            logger.debug("  ➜ [洗版特权] 本次整理使用了洗版特权。")

        return True
    
    def execute_mp_passthrough(self, file_nodes):
        """
        MP直出模式 (终极优化版)：
        完全信任 115 现有的目录结构和文件名 (直接从 Webhook 传来的 115_path 提取)。
        跳过整理、归类、移动、重命名。
        直接在本地 1:1 映射生成 STRM 和 -mediainfo.json。
        """
        config = get_config()
        local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
        etk_url = (config.get(constants.CONFIG_OPTION_ETK_SERVER_URL) or "").rstrip("/")
        media_root_name = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_NAME) or "").strip("/")

        if not local_root or not etk_url or not etk_url.startswith("http"):
            logger.warning("  ➜ [MP直出] 请配置 http(s) 开头的 ETK 访问地址。")
            return False

        os.makedirs(local_root, exist_ok=True)

        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

        for file_item in file_nodes:
            original_name = file_item.get("fn") or file_item.get("file_name") or ""
            if "." not in original_name:
                continue

            # ★ 修复：使用 [-1] 提取后缀，防止文件名中包含多个点导致报错
            ext = original_name.rsplit(".", 1)[-1].lower()
            fid = file_item.get("fid") or file_item.get("file_id")
            parent_id = file_item.get("pid") or file_item.get("parent_id")
            pick_code = file_item.get("pc") or file_item.get("pick_code")
            sha1 = file_item.get("sha1") or file_item.get("sha")
            full_115_path = file_item.get("115_path")

            is_video = ext in known_video_exts
            is_sub = ext in sub_exts
            
            if not is_video and not is_sub:
                continue

            # ==========================================================
            # ★ 核心优化：直接从 Webhook 传来的 115_path 提取相对路径，0 API 消耗！
            # 彻底砍掉画蛇添足的 Season 拼接逻辑，完全 1:1 映射 115 物理路径
            # ==========================================================
            parent_rel_path = ""
            if full_115_path:
                path_parts = [p for p in full_115_path.split('/') if p]
                
                start_idx = 0
                if media_root_name and media_root_name in path_parts:
                    # 如果配置了根目录名称，从根目录的下一级开始截取
                    start_idx = path_parts.index(media_root_name) + 1
                elif len(path_parts) > 1:
                    # 兜底：如果没有配置，默认剥离第一层目录 (如 /影视待整理/)
                    start_idx = 1
                    
                if len(path_parts) > start_idx:
                    # 剥离根目录，并去掉最后的文件名，剩下的就是纯净的相对目录！
                    # 例如：['影视待整理', '虾路相逢', 'Season 01', 'S01E01.mkv'] -> '虾路相逢/Season 01'
                    parent_rel_path = "/".join(path_parts[start_idx:-1])
            else:
                logger.warning(f"  ➜ [MP直出] 缺少 115_path 参数，无法映射目录结构: {original_name}")
                continue

            # 根据本地相对路径反推真实分类目录 CID。
            # MP 直出不移动文件，parent_id 只是文件真实父目录/季目录，不能作为洗版规则 target_cid。
            # 这里用 p115_sorting_rules.category_path 做前缀匹配，0 API 消耗。
            inferred_sorting_target = resolve_p115_sorting_target_by_local_path(parent_rel_path or original_name, local_root=local_root)
            inferred_target_cid = str((inferred_sorting_target or {}).get('cid') or '').strip()
            inferred_category_path = str((inferred_sorting_target or {}).get('category_path') or '').strip()

            if inferred_target_cid:
                logger.debug(
                    f"  ➜ [MP直出] 已通过路径匹配分类目录: {parent_rel_path or original_name} "
                    f"-> CID={inferred_target_cid} ({inferred_category_path or '-'})"
                )
            else:
                logger.debug(f"  ➜ [MP直出] 未能通过路径匹配分类目录，暂沿用父目录CID: {parent_rel_path or original_name}")

            # 确定本地落盘目录
            local_dir = os.path.join(local_root, parent_rel_path) if parent_rel_path else local_root
            os.makedirs(local_dir, exist_ok=True)

            # 补齐 SHA1 (仅视频需要，用于缓存 mediainfo)
            if is_video and not sha1 and fid:
                try:
                    info_res = self.client.fs_get_info(fid)
                    if info_res.get('state') and info_res.get('data'):
                        fetched_sha1 = info_res['data'].get('sha1')
                        if fetched_sha1:
                            sha1 = str(fetched_sha1).upper()
                            file_item['sha1'] = sha1
                except Exception:
                    pass

            # 1. 处理视频 (STRM + Mediainfo)
            if is_video and pick_code:
                # 生成 STRM
                strm_filename = os.path.splitext(original_name)[0] + ".strm"
                strm_filepath = os.path.join(local_dir, strm_filename)

                strm_content = f"{etk_url}/api/p115/play/{pick_code}/{original_name}"

                with open(strm_filepath, "w", encoding="utf-8") as f:
                    f.write(strm_content)
                logger.info(f"  ➜ [MP直出] 已生成 STRM：{strm_filename}")

                # ★★★ 主动推送给实时监控队列，防止底层文件系统事件丢失 ★★★
                try:
                    from monitor_service import enqueue_file_actively
                    enqueue_file_actively(strm_filepath)
                except Exception: 
                    pass

                # 生成 Mediainfo
                if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                    try:
                        mediainfo_text = None
                        if sha1:
                            mediainfo_text = P115CacheManager.get_mediainfo_cache_text(sha1)

                        if not mediainfo_text:
                            emby_obj, raw_ffprobe = self._probe_mediainfo_with_ffprobe(file_item, sha1=sha1, silent_log=False) or (None, None)
                            if emby_obj:
                                probe_sha1 = sha1 or file_item.get('sha1') or file_item.get('sha')
                                if probe_sha1:
                                    probe_sha1 = str(probe_sha1).upper()
                                    P115CacheManager.save_mediainfo_cache(probe_sha1, emby_obj, raw_ffprobe)
                                    try:
                                        cached_preid = P115CacheManager.ensure_file_preid(file_item, sha1=probe_sha1, fid=fid, pick_code=pick_code, file_name=original_name)
                                        if cached_preid:
                                            file_item['preid'] = cached_preid
                                    except Exception as e_preid:
                                        logger.debug(f"  ➜ [MP直出] 媒体信息提取后计算 preid 失败: {original_name} -> {e_preid}")
                                    sha1 = probe_sha1
                                    file_item['sha1'] = probe_sha1
                                mediainfo_text = json.dumps(emby_obj, ensure_ascii=False, indent=2)

                        if mediainfo_text:
                            mediainfo_filename = os.path.splitext(original_name)[0] + "-mediainfo.json"
                            mediainfo_filepath = os.path.join(local_dir, mediainfo_filename)
                            with open(mediainfo_filepath, "w", encoding="utf-8") as f:
                                f.write(mediainfo_text)
                            logger.info(f"  ➜ [MP直出] 已生成媒体信息文件：{mediainfo_filename}")
                    except Exception as e:
                        logger.error(f"  ➜ [MP直出] 生成媒体信息失败: {e}")

            # 2. 处理字幕 (直接下载)
            elif is_sub and config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True) and pick_code:
                try:
                    sub_filepath = os.path.join(local_dir, original_name)
                    if not os.path.exists(sub_filepath):
                        url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                        if url_obj:
                            import requests
                            headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                            resp = requests.get(str(url_obj), stream=True, timeout=30, headers=headers)
                            resp.raise_for_status()
                            with open(sub_filepath, "wb") as f:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            logger.info(f"  ➜ [MP直出] 字幕已下载：{original_name}")
                except Exception as e:
                    logger.error(f"  ➜ [MP直出] 下载字幕失败: {e}")

            # 3. 写入数据库缓存 (保持 Emby 扫库和后续删除的闭环)
            try:
                file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            except Exception:
                file_size = 0

            file_local_path = os.path.join(parent_rel_path, original_name).replace("\\", "/") if parent_rel_path else original_name

            if fid and pick_code:
                P115CacheManager.save_file_cache(
                    fid=fid,
                    parent_id=parent_id,
                    name=original_name,
                    sha1=sha1,
                    pick_code=pick_code,
                    local_path=file_local_path,
                    size=file_size,
                    preid=file_item.get('preid')
                )

                P115RecordManager.add_or_update_record(
                    file_id=fid,
                    pick_code=pick_code,
                    original_name=original_name,
                    status="success",
                    tmdb_id=self.tmdb_id,
                    media_type=self.media_type,
                    # MP直出的真实分类目录来自本地路径匹配；失败时才退回父目录，避免旧逻辑断链。
                    target_cid=inferred_target_cid or parent_id,
                    category_name=(f"MP直出/{inferred_category_path}" if inferred_category_path else "MP直出"),
                    renamed_name=original_name,
                    season_number=file_item.get('_forced_season')
                )

        return True

def _parse_115_size(size_val):
    """
    统一解析 115 返回的文件大小为字节(Int)
    支持: 12345(int), "12345"(str), "1.2GB", "500KB"
    """
    try:
        if size_val is None: return 0

        # 1. 如果已经是数值 (115 API 's' 字段通常是 int)
        if isinstance(size_val, (int, float)):
            return int(size_val)

        # 2. 如果是字符串
        if isinstance(size_val, str):
            s = size_val.strip()
            if not s: return 0
            # 纯数字字符串
            if s.isdigit():
                return int(s)

            s_upper = s.upper().replace(',', '')
            mult = 1
            if 'TB' in s_upper: mult = 1024**4
            elif 'GB' in s_upper: mult = 1024**3
            elif 'MB' in s_upper: mult = 1024**2
            elif 'KB' in s_upper: mult = 1024

            match = re.search(r'([\d\.]+)', s_upper)
            if match:
                return int(float(match.group(1)) * mult)
    except Exception:
        pass
    return 0

def _extract_raw_ffprobe_identity(raw_ffprobe_json):
    """从 p115_mediainfo_cache.raw_ffprobe_json 顶层 _etk 提取可复用媒体身份。"""
    if not raw_ffprobe_json:
        return None

    try:
        if isinstance(raw_ffprobe_json, str):
            raw_ffprobe_json = json.loads(raw_ffprobe_json)
    except Exception:
        return None

    if not isinstance(raw_ffprobe_json, dict):
        return None

    ctx = raw_ffprobe_json.get("_etk")
    if not isinstance(ctx, dict):
        return None

    tmdb_id = ctx.get("tmdb_id") or ctx.get("tmdbid") or ctx.get("tmdb")
    media_type = ctx.get("type") or ctx.get("media_type") or ctx.get("item_type")
    original_language = ctx.get("original_language")
    sha1 = ctx.get("sha1")

    def _identity_int(*values):
        for value in values:
            try:
                if value in (None, ''):
                    continue
                return int(float(value))
            except Exception:
                continue
        return None

    season_number = _identity_int(ctx.get("season_number"), ctx.get("season"), ctx.get("s"))
    episode_number = _identity_int(ctx.get("episode_number"), ctx.get("episode"), ctx.get("e"))

    if not tmdb_id or not media_type:
        return None

    media_type_text = str(media_type).strip().lower()
    if media_type_text in ["movie", "movies", "film", "电影"]:
        normalized_type = "movie"
    elif media_type_text in ["tv", "series", "season", "episode", "电视剧", "剧集", "季", "集", "分集"]:
        normalized_type = "tv"
    else:
        return None

    identity = {
        "tmdb_id": str(tmdb_id).strip(),
        "media_type": normalized_type,
        "original_language": str(original_language).strip() if original_language not in [None, ""] else None,
        "season_number": season_number,
        "episode_number": episode_number,
        "sha1": str(sha1).strip().upper() if sha1 not in [None, ""] else None,
    }
    return {k: v for k, v in identity.items() if v not in [None, "", [], {}]}


def _get_raw_ffprobe_identity_by_sha1(sha1):
    """按 SHA1 从 p115_mediainfo_cache 读取 raw_ffprobe_json 的 ETK 身份信息。"""
    if not sha1:
        return None
    try:
        raw_probe = P115CacheManager.get_raw_ffprobe_cache(str(sha1).upper())
        return _extract_raw_ffprobe_identity(raw_probe)
    except Exception:
        return None


def _normalize_rule_media_type(value):
    text = str(value or '').strip().lower()
    if text in ['movie', 'movies', 'film', '电影']:
        return 'movie'
    if text in ['tv', 'series', 'episode', 'season', 'show', '电视剧', '剧集', '番剧', '动漫']:
        return 'tv'
    return None


def _parse_rule_cn_number(value):
    text = str(value or '').strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)

    digit_map = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    unit_map = {'十': 10, '百': 100}
    total = 0
    current = 0
    for ch in text:
        if ch in digit_map:
            current = digit_map[ch]
        elif ch in unit_map:
            unit = unit_map[ch]
            if current == 0:
                current = 1
            total += current * unit
            current = 0
    total += current
    return total or None


def _clean_rule_title(text):
    if not text:
        return ''

    value = str(text).replace('\\', '/')
    value = os.path.basename(value.strip())
    value = os.path.splitext(value)[0]
    value = re.sub(r'[？?！!：:；;，,、]+', ' ', value)

    for pattern in _NOISE_TOKEN_PATTERNS:
        value = re.sub(pattern, ' ', value)

    value = re.sub(r'(?i)\b(?:s\d{1,4}[ .\-_]*e\d{1,4}|season[ .\-_]*\d{1,4}|ep(?:isode)?[ .\-_]*\d{1,4}|第\s*[一二三四五六七八九十百零\d]+\s*[季集话話回])\b', ' ', value)
    value = re.sub(r'(?i)\b(?:part|pt|cd)[ .\-_]*\d{1,2}\b', ' ', value)
    value = re.sub(r'(?i)\b(?:tmdb|tmdbid)[=\-_ ]*\d+\b', ' ', value)
    value = re.sub(r'(?<!\d)(?:19|20)\d{2}(?!\d)', ' ', value)
    value = re.sub(r'(?i)\b(?:specials?|ova|oad|sp|extra(?:s)?|collection|complete)\b', ' ', value)
    value = re.sub(r'(?i)\b(?:h[ ._-]?26[45]|x26[45])\b', ' ', value)
    value = re.sub(r'(?i)\b(?:flac|aac|ddp|dd|dts|truehd|atmos)[ ._-]*\d(?:\.\d)?\b', ' ', value)
    value = re.sub(r'(?i)(?:[-_. ]+)?[A-Za-z0-9][A-Za-z0-9._-]{1,20}@[A-Za-z0-9][A-Za-z0-9._-]{1,20}$', ' ', value)
    value = re.sub(r'(?<!\w)\d\.\d(?!\w)', ' ', value)
    value = re.sub(r'[\[\]\(\)\{\}]', ' ', value)
    value = re.sub(r'[._]+', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip(' -_./')
    return value.strip()


def _extract_rule_year(text):
    if not text:
        return None
    matches = list(re.finditer(r'(?<!\d)((?:19|20)\d{2})(?!\d)', str(text)))
    if not matches:
        return None
    try:
        return int(matches[-1].group(1))
    except Exception:
        return None


def _rule_detect_special(text):
    if not text:
        return False
    for pattern in _SPECIAL_FLAG_PATTERNS:
        if pattern.search(str(text)):
            return True
    return False


def _rule_extract_tmdb_id(*texts):
    tmdb_regex = re.compile(r'(?i)(?:tmdb|tmdbid)[=\-_ ]*(\d{2,10})')
    for text in texts:
        if not text:
            continue
        match = tmdb_regex.search(str(text))
        if match:
            return match.group(1)
    return None


def _rule_extract_date_episode(text):
    if not text:
        return None
    for pattern in _DATE_EPISODE_PATTERNS:
        match = pattern.search(str(text))
        if match:
            year, month, day = match.groups()
            try:
                return int(f"{year}{month}{day}")
            except Exception:
                return None
    return None


def _rule_extract_season_episode_from_text(text):
    if not text:
        return None, None, []

    evidence = []
    normalized = str(text).replace('\\', '/')
    lower_text = normalized.lower()

    match = re.search(r'(?i)(?:^|[ \.\-_/\[(])s(\d{1,4})[ \.\-_]*[eEpP](\d{1,4})(?:$|[ \.\-_/)\]])', normalized)
    if match:
        evidence.append('sxe')
        return int(match.group(1)), int(match.group(2)), evidence

    match = re.search(r'(?i)(?:^|[ \.\-_/\[(])season[ \.\-_]*(\d{1,4})(?:$|[ \.\-_/)\]])', normalized)
    season_num = int(match.group(1)) if match else None
    if match:
        evidence.append('season_word')

    match = re.search(r'第\s*([一二三四五六七八九十百零\d]+)\s*季', normalized)
    if match and season_num is None:
        cn_number = _parse_rule_cn_number(match.group(1))
        if cn_number is not None:
            season_num = int(cn_number)
            evidence.append('season_zh')

    match = re.search(r'(?i)(?:^|[ \.\-_/\[(])(?:ep|episode)[ \.\-_]*(\d{1,4})(?:$|[ \.\-_/)\]])', normalized)
    episode_num = int(match.group(1)) if match else None
    if match:
        evidence.append('episode_word')

    match = re.search(r'(?i)(?:^|[ \.\-_/\[(])e(\d{1,4})(?:$|[ \.\-_/)\]])', normalized)
    if match and episode_num is None:
        episode_num = int(match.group(1))
        evidence.append('episode_e')

    match = re.search(r'第\s*([一二三四五六七八九十百零\d]+)\s*[集话話回]', normalized)
    if match and episode_num is None:
        cn_number = _parse_rule_cn_number(match.group(1))
        if cn_number is not None:
            episode_num = int(cn_number)
            evidence.append('episode_zh')

    if episode_num is None:
        date_ep = _rule_extract_date_episode(normalized)
        if date_ep is not None:
            episode_num = date_ep
            evidence.append('episode_date')

    if _rule_detect_special(normalized):
        if season_num is None:
            season_num = 0
        evidence.append('special')

    if episode_num is None:
        match = re.search(r'(?i)(?:^|[ \.\-_/\[(])(?:sp|ova|oad)(\d{1,4})(?:$|[ \.\-_/)\]])', normalized)
        if match:
            episode_num = int(match.group(1))
            evidence.extend(['special', 'episode_special_code'])

    if season_num is None and episode_num is not None and any(flag in lower_text for flag in ['ep', 'episode', '第', '话', '話', '回']):
        season_num = 1

    return season_num, episode_num, evidence


def _build_rule_parse_result(filename, main_dir_name=None, has_season_subdirs=False, forced_media_type=None, is_folder=False):
    cache_key = json.dumps({
        'filename': filename or '',
        'main_dir_name': main_dir_name or '',
        'has_season_subdirs': bool(has_season_subdirs),
        'forced_media_type': forced_media_type or '',
        'is_folder': bool(is_folder),
    }, ensure_ascii=False, sort_keys=True)
    if cache_key in _RULE_PARSE_CACHE:
        return _RULE_PARSE_CACHE[cache_key]

    source_texts = [str(x) for x in [filename, main_dir_name] if x]
    combined_text = ' / '.join(source_texts)
    result = {
        'tmdb_id': None,
        'media_type': _normalize_rule_media_type(forced_media_type),
        'title': None,
        'year': None,
        'season_number': None,
        'episode_number': None,
        'is_special': False,
        'confidence': 'low',
        'evidence': [],
        'source': 'rules',
    }

    explicit_tmdb_id = _rule_extract_tmdb_id(filename, main_dir_name)
    if explicit_tmdb_id:
        result['tmdb_id'] = explicit_tmdb_id
        result['confidence'] = 'high'
        result['evidence'].append('explicit_tmdb')

    season_num = None
    episode_num = None
    season_evidence = []
    for text in source_texts:
        s_val, e_val, evi = _rule_extract_season_episode_from_text(text)
        if season_num is None and s_val is not None:
            season_num = s_val
        if episode_num is None and e_val is not None:
            episode_num = e_val
        season_evidence.extend(evi)
        if season_num is not None and episode_num is not None and 'special' not in season_evidence:
            break

    if has_season_subdirs and season_num is None:
        season_num = 1
        season_evidence.append('season_subdir')

    result['season_number'] = season_num
    result['episode_number'] = episode_num
    result['is_special'] = 'special' in season_evidence or _rule_detect_special(combined_text)
    result['evidence'].extend([e for e in season_evidence if e not in result['evidence']])

    title_candidates = []
    generic_dir = bool(main_dir_name and re.search(r'(?i)\b(collection|合集|系列|pack|misc|unknown)\b', str(main_dir_name)))
    if filename:
        title_candidates.append(filename)
    if main_dir_name and str(main_dir_name).strip() and str(main_dir_name) != str(filename) and not generic_dir:
        title_candidates.insert(0, main_dir_name)

    for candidate in title_candidates:
        clean_title = _clean_rule_title(candidate)
        if clean_title:
            result['title'] = clean_title
            break

    if not result['title'] and main_dir_name:
        result['title'] = _clean_rule_title(main_dir_name)

    for candidate in title_candidates:
        year_val = _extract_rule_year(candidate)
        if year_val:
            result['year'] = year_val
            result['evidence'].append('year')
            break

    if result['media_type'] is None:
        if result['season_number'] is not None or has_season_subdirs:
            result['media_type'] = 'tv'
            result['evidence'].append('tv_structure')
        elif result['episode_number'] is not None and result['season_number'] is not None:
            result['media_type'] = 'tv'
            result['evidence'].append('tv_structure')
        elif result['tmdb_id']:
            result['media_type'] = 'movie'
        elif result['title'] and result['year']:
            result['media_type'] = 'movie'

    if result['is_special'] and result['season_number'] is None:
        result['season_number'] = 0

    if result['tmdb_id']:
        result['confidence'] = 'high'
    elif result['title'] and (result['year'] or result['media_type'] or result['episode_number'] is not None):
        result['confidence'] = 'medium'
    elif result['title']:
        result['confidence'] = 'low'

    _RULE_PARSE_CACHE[cache_key] = result
    return result


def _identify_media_enhanced(filename, main_dir_name=None, has_season_subdirs=False, forced_media_type=None, ai_translator=None, use_ai=False, is_folder=False, sha1=None, raw_ffprobe_json=None, recognition_hints=None):
    """
    【绝对正确版】识别逻辑：
    1. 先定类型：综合主目录、子目录特征、文件名，判断是 Movie 还是 TV。
    2. 再提 ID：优先从主目录/文件名提取 {tmdb=xxx}。
    3. 目录拦截：如果是目录且没有显式 ID，直接返回 None，强制深入扫描子目录！
    4. 定向查询：用确定的类型 + 提取的 Title (Year) 向 TMDb 发起查询。
    """
    tmdb_id = None
    media_type = 'movie' # 默认兜底
    title = filename
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    
    is_same_name = (main_dir_name == filename)
    rule_result = _build_rule_parse_result(
        filename=filename,
        main_dir_name=main_dir_name,
        has_season_subdirs=has_season_subdirs,
        forced_media_type=forced_media_type,
        is_folder=is_folder,
    )
    normalized_hints = candidate_to_recognition_hints(recognition_hints or {})
    eligible_hints = normalized_hints if is_recognition_hint_eligible(normalized_hints) else {}
    authoritative_hints = normalized_hints if _is_authoritative_recognition_hint(normalized_hints) else {}
    if normalized_hints:
        if normalized_hints.get('media_type') and not forced_media_type:
            media_type = normalized_hints.get('media_type')
        if normalized_hints.get('identify_title') or normalized_hints.get('clean_title') or normalized_hints.get('title'):
            title = normalized_hints.get('identify_title') or normalized_hints.get('clean_title') or normalized_hints.get('title')

    # =================================================================
    # ★ 第一步：铁腕判定媒体类型 (Movie or TV)
    # =================================================================
    if forced_media_type:
        media_type = forced_media_type
    else:
        if rule_result.get('media_type'):
            media_type = rule_result.get('media_type')
        else:
            # 将主目录名和文件名拼在一起，寻找剧集特征
            combined_text = f"{main_dir_name or ''} {filename}"
            if has_season_subdirs or re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第\s*[一二三四五六七八九十\d]+\s*季|Season', combined_text, re.IGNORECASE):
                media_type = 'tv'

    if rule_result.get('title'):
        title = rule_result.get('title')

    if rule_result.get('confidence') in ('medium', 'high') and rule_result.get('title'):
        evidence_text = ','.join(rule_result.get('evidence') or []) or 'rule'
        logger.debug(
            f"  ➜ [规则预解析] 命中预解析: title='{rule_result.get('title')}', "
            f"year={rule_result.get('year')}, type={rule_result.get('media_type')}, "
            f"season={rule_result.get('season_number')}, episode={rule_result.get('episode_number')}, "
            f"special={rule_result.get('is_special')} (confidence={rule_result.get('confidence')}, evidence={evidence_text})"
        )

    if normalized_hints:
        logger.debug(
            f"  ➜ [TG Candidate] 命中识别 hints: title='{normalized_hints.get('identify_title') or normalized_hints.get('clean_title') or normalized_hints.get('title')}', "
            f"year={normalized_hints.get('year')}, type={normalized_hints.get('media_type')}, "
            f"season={normalized_hints.get('season_number')}, episode={normalized_hints.get('episode_number')}, "
            f"special={normalized_hints.get('is_special')} (confidence={normalized_hints.get('confidence')}, source={normalized_hints.get('source')}, authority={normalized_hints.get('authority_role')})"
        )

    # 辅助函数：用已锁定的类型去 TMDb 查官方标题
    def _fetch_title_by_id(ext_id, m_type):
        if not api_key: return None
        try:
            if m_type == 'tv':
                res = tmdb.get_tv_details(ext_id, api_key)
                if res: return res.get('name') or res.get('original_name')
            else:
                res = tmdb.get_movie_details(ext_id, api_key)
                if res: return res.get('title') or res.get('original_title')
        except Exception:
            pass
        return None

    # =================================================================
    # ★ 优先级 0：共享媒体信息缓存身份命中
    # raw_ffprobe_json 顶层 _etk 来自 p115_mediainfo_cache，跨账号仍可复用。
    # =================================================================
    if is_p115_mediainfo_assisted_recognition_enabled():
        probe_identity = _extract_raw_ffprobe_identity(raw_ffprobe_json)
        if not probe_identity and sha1:
            probe_identity = _get_raw_ffprobe_identity_by_sha1(sha1)

        if probe_identity:
            tmdb_id = probe_identity.get("tmdb_id")
            probe_type = probe_identity.get("media_type")

            # forced_media_type 仍然拥有最终约束权，但不允许与缓存类型冲突时静默误判。
            if forced_media_type and forced_media_type != probe_type:
                logger.debug(
                    f"  ➜ [媒体信息辅助识别] 命中 TMDb:{tmdb_id} 类型:{probe_type}，"
                    f"但当前强制类型为 {forced_media_type}，跳过缓存身份。"
                )
            else:
                media_type = probe_type
                official_title = _fetch_title_by_id(tmdb_id, media_type)
                probe_type_text = '剧集' if probe_type == 'tv' else '电影' if probe_type == 'movie' else str(probe_type or '未知类型')
                se_parts = []
                if probe_identity.get('season_number') not in (None, ''):
                    se_parts.append(f"第 {int(probe_identity.get('season_number'))} 季")
                if probe_identity.get('episode_number') not in (None, ''):
                    se_parts.append(f"第 {int(probe_identity.get('episode_number'))} 集")
                se_text = "，" + "".join(se_parts) if se_parts else ""
                logger.info(
                    f"  ➜ [媒体信息辅助识别] 命中共享媒体信息缓存：TMDb {tmdb_id}，类型：{probe_type_text}{se_text}"
                )
                if probe_identity.get('original_language'):
                    logger.debug(f"  ➜ [媒体信息辅助识别] 原始语言：{probe_identity.get('original_language')}")
                return tmdb_id, media_type, official_title or filename

    if normalized_hints.get('tmdb_id') and normalized_hints.get('confidence') == 'high':
        hinted_type = normalized_hints.get('media_type') or media_type
        official_title = _fetch_title_by_id(normalized_hints.get('tmdb_id'), hinted_type)
        logger.info(
            f"  ➜ [TG Candidate] 命中高置信显式 TMDb ID: {normalized_hints.get('tmdb_id')} "
            f"(evidence={','.join(normalized_hints.get('evidence') or []) or 'candidate'})"
        )
        return normalized_hints.get('tmdb_id'), hinted_type, official_title or title or filename

    if rule_result.get('tmdb_id') and rule_result.get('confidence') == 'high':
        official_title = _fetch_title_by_id(rule_result.get('tmdb_id'), media_type)
        logger.info(
            f"  ➜ [规则预解析] 命中高置信显式 TMDb ID: {rule_result.get('tmdb_id')} "
            f"(evidence={','.join(rule_result.get('evidence') or []) or 'rule'})"
        )
        return rule_result.get('tmdb_id'), media_type, official_title or title or filename

    # =================================================================
    # ★ 第二步：按优先级提取信息并定向查询
    # =================================================================
    
    # 优先级 1: 显式 TMDb ID (最高优先级，绝不误判)
    tmdb_regex = r'(?:tmdb|tmdbid)[=\-_]*(\d+)'
    
    # 1.1 优先从 filename 提取
    match_id_file = re.search(tmdb_regex, filename, re.IGNORECASE)
    if match_id_file:
        tmdb_id = match_id_file.group(1)
        clean_name = re.sub(r'\[.*?\]|\{.*?\}|\(.*?\)', '', filename).strip()
        official_title = _fetch_title_by_id(tmdb_id, media_type)
        return tmdb_id, media_type, official_title or clean_name or filename

    # 1.2 其次从 main_dir_name 提取
    if main_dir_name:
        match_id_dir = re.search(tmdb_regex, main_dir_name, re.IGNORECASE)
        if match_id_dir:
            tmdb_id = match_id_dir.group(1)
            clean_name = re.sub(r'\[.*?\]|\{.*?\}|\(.*?\)', '', main_dir_name).strip()
            official_title = _fetch_title_by_id(tmdb_id, media_type)
            return tmdb_id, media_type, official_title or clean_name or main_dir_name

    # ★★★ 核心拦截：如果是目录，且没有显式 ID，直接返回 None，强制深入扫描子目录！★★★
    if is_folder:
        return None, None, None

    # 优先级 2: 提取 Title (Year) 进行搜索 (仅限文件)
    def _search_by_title_year(text, query_override=None, year_override=None, type_override=None):
        # 剔除 S01E01 等干扰字符 (连同前面的点和下划线一起剔除，防止留下 "The.Crown.")
        clean_text = re.sub(r'(?i)[\.\s\-_]*s\d{1,4}(?:e\d{1,4})?\b.*$', '', text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*ep?\d{1,4}\b.*$', '', clean_text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*season\s*\d{1,4}\b.*$', '', clean_text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*第\s*[一二三四五六七八九十\d]+\s*季.*$', '', clean_text).strip()

        # 尝试提取年份 (不再强制要求必须有年份)
        name_part = clean_text
        year_part = None
        match_std = re.search(r'[\(\[\.\s_-](\d{4})(?:[\)\]\.\s_-]|$)', clean_text)
        if match_std:
            year_part = match_std.group(1)
            # 把年份从名字里剔除
            name_part = clean_text[:match_std.start()].strip()

        if query_override:
            name_part = str(query_override).strip()
        if year_override not in [None, '']:
            year_part = str(year_override).strip()

        # 清理名字里的点和下划线
        name_part = name_part.replace('.', ' ').replace('_', ' ').strip()

        if not name_part: return None

        try:
            if api_key:
                search_media_type = type_override or media_type
                search_key = f"{name_part}_{year_part}_{search_media_type}"
                if search_key in _TMDB_SEARCH_CACHE:
                    results = _TMDB_SEARCH_CACHE[search_key]
                else:
                    # 严格按照锁定的 media_type 搜索
                    results = tmdb.search_media(query=name_part, api_key=api_key, item_type=search_media_type, year=year_part)
                    _TMDB_SEARCH_CACHE[search_key] = results

                if results and len(results) > 0:
                    best = results[0]
                    # ★★★ 核心修复：精准匹配，防止 TMDb 瞎给结果 ★★★
                    name_lower = name_part.lower()
                    name_parts = [p for p in name_lower.split() if p]
                    
                    for res in results:
                        res_title = (res.get('title') or res.get('name') or '').lower()
                        res_orig = (res.get('original_title') or res.get('original_name') or '').lower()
                        
                        if name_lower == res_title or name_lower == res_orig:
                            best = res
                            break
                            
                        part_match = False
                        for part in name_parts:
                            if part == res_title or part == res_orig:
                                best = res
                                part_match = True
                                break
                        if part_match:
                            break
                            
                    return str(best['id']), search_media_type, (best.get('title') or best.get('name'))
        except Exception:
            pass
        return None

    if authoritative_hints.get('tmdb_id'):
        hinted_type = authoritative_hints.get('media_type') or media_type
        official_title = _fetch_title_by_id(authoritative_hints.get('tmdb_id'), hinted_type)
        logger.info(
            f"  ➜ [Authority识别] 命中权威身份: TMDb:{authoritative_hints.get('tmdb_id')} "
            f"type:{hinted_type} source:{authoritative_hints.get('source')}"
        )
        return authoritative_hints.get('tmdb_id'), hinted_type, official_title or title or filename

    if rule_result.get('title') and rule_result.get('confidence') in ('medium', 'high'):
        res = _search_by_title_year(
            filename,
            query_override=rule_result.get('title'),
            year_override=rule_result.get('year'),
            type_override=rule_result.get('media_type') or media_type
        )
        if res:
            logger.info(
                f"  ➜ [规则预解析] 规则命中后 TMDb 搜索成功: "
                f"title='{rule_result.get('title')}', year={rule_result.get('year')}, type={rule_result.get('media_type') or media_type}"
            )
            return res

    if normalized_hints.get('identify_title') and normalized_hints.get('confidence') in ('medium', 'high'):
        res = _search_by_title_year(
            filename,
            query_override=normalized_hints.get('identify_title') or normalized_hints.get('clean_title'),
            year_override=normalized_hints.get('year'),
            type_override=normalized_hints.get('media_type') or media_type,
        )
        if res:
            logger.info(
                f"  ➜ [TG Candidate] hints 命中后 TMDb 搜索成功: "
                f"title='{normalized_hints.get('identify_title') or normalized_hints.get('clean_title')}', year={normalized_hints.get('year')}, "
                f"type={normalized_hints.get('media_type') or media_type}"
            )
            return res

    # 2.1 优先从 filename 搜索
    res = _search_by_title_year(filename)
    if res: return res

    # 2.2 其次从 main_dir_name 搜索
    if main_dir_name and not is_same_name:
        res = _search_by_title_year(main_dir_name)
        if res: return res

    # =================================================================
    # ★ 第三步：MoviePilot 辅助识别 (免费、快速、高准确率)
    # =================================================================
    mp_config = settings_db.get_setting('mp_config') or {}
    use_mp_recognition = mp_config.get('moviepilot_recognition', False)
    if use_mp_recognition:
        import handler.moviepilot as mp
        target_mp_name = main_dir_name if main_dir_name else filename
        
        def _do_mp_search(target_name):
            if target_name in _MP_PARSE_CACHE:
                return _MP_PARSE_CACHE[target_name]
                
            logger.debug(f"  ➜ 本地正则失败，尝试调用 MoviePilot 辅助识别: {target_name}")
            mp_res = mp.recognize_media_from_candidate(
                eligible_hints if eligible_hints else rule_result,
                fallback_title=target_name,
                config=config_manager.APP_CONFIG
            )
            
            if mp_res:
                logger.info(f"  ➜ [MP辅助识别] 已识别为《{mp_res[2]}》。")
                logger.debug(f"  ➜ [MP辅助识别] 命中详情：TMDb={mp_res[0]}")
                _MP_PARSE_CACHE[target_name] = mp_res
                return mp_res
            
            _MP_PARSE_CACHE[target_name] = None
            return None

        # 优先尝试主目录
        res = _do_mp_search(target_mp_name)
        if res: return res
        
        # 如果主目录失败，且当前是文件，尝试解析文件名
        if main_dir_name and not is_same_name:
            res_file = _do_mp_search(filename)
            if res_file: return res_file

    # =================================================================
    # ★ 第四步：AI 辅助识别 (终极兜底 + 记忆体优化)
    # =================================================================
    if use_ai and ai_translator:
        target_ai_name = main_dir_name if main_dir_name else filename
        
        def _do_ai_search(target_name):
            # 1. 查 AI 记忆体
            if target_name in _AI_PARSE_CACHE:
                ai_result = _AI_PARSE_CACHE[target_name]
            else:
                logger.info(f"  🤖 常规识别失败，消耗 Token 请求 AI 解析: {target_name}")
                try:
                    ai_result = ai_translator.parse_media_filename(target_name)
                    _AI_PARSE_CACHE[target_name] = ai_result # 写入记忆体
                except Exception as e:
                    logger.error(f"  ➜ AI 解析出错: {e}")
                    return None

            # 2. 查 TMDb 记忆体
            if ai_result and ai_result.get('title'):
                ai_title = ai_result.get('title')
                ai_year = ai_result.get('year')
                ai_type = forced_media_type or ai_result.get('type') or media_type
                if rule_result.get('title') and not ai_title:
                    ai_title = rule_result.get('title')
                if rule_result.get('year') and not ai_year:
                    ai_year = rule_result.get('year')
                if rule_result.get('media_type') and not forced_media_type:
                    ai_type = rule_result.get('media_type')
                
                if api_key:
                    search_key = f"AI_{ai_title}_{ai_year}_{ai_type}"
                    if search_key in _TMDB_SEARCH_CACHE:
                        results = _TMDB_SEARCH_CACHE[search_key]
                    else:
                        results = tmdb.search_media(query=ai_title, api_key=api_key, item_type=ai_type, year=ai_year)
                        _TMDB_SEARCH_CACHE[search_key] = results

                    if results and len(results) > 0:
                        best = results[0]
                        # ★★★ 核心修复：精准匹配 ★★★
                        ai_title_lower = ai_title.lower()
                        ai_title_parts = [p for p in ai_title_lower.split() if p]
                        
                        for res in results:
                            res_title = (res.get('title') or res.get('name') or '').lower()
                            res_orig = (res.get('original_title') or res.get('original_name') or '').lower()
                            
                            if ai_title_lower == res_title or ai_title_lower == res_orig:
                                best = res
                                break
                                
                            part_match = False
                            for part in ai_title_parts:
                                if part == res_title or part == res_orig:
                                    best = res
                                    part_match = True
                                    break
                            if part_match:
                                break
                                
                        return str(best['id']), ai_type, (best.get('title') or best.get('name'))
                    else:
                        logger.debug(f"  🤖 AI 提取了标题 '{ai_title}'，但在 TMDb 未搜索到结果。")
            return None

        # 优先尝试主目录
        res = _do_ai_search(target_ai_name)
        if res: return res
        
        # 如果主目录彻底没救了，且当前是文件，才尝试解析文件名
        if main_dir_name and not is_same_name:
            res_file = _do_ai_search(filename)
            if res_file: return res_file

    return None, None, None

# ======================================================================
# ★★★ Webhook 深度删除缓冲队列 (实现并发删除请求的批量合并) ★★★
# ======================================================================
class WebhookDeleteBuffer:
    _lock = threading.Lock()
    _pickcodes = set()
    _timer = None

    @classmethod
    def add(cls, pickcodes):
        if not pickcodes: return
        with cls._lock:
            cls._pickcodes.update(pickcodes)
            
            # 如果有新任务进来，重置定时器
            if cls._timer is not None:
                cls._timer.kill()
            
            from gevent import spawn_later
            # 延迟 3 秒，足以收集一键去重/批量删除瞬间发来的所有 Webhook
            cls._timer = spawn_later(3.0, cls._execute_all)

    @classmethod
    def _execute_all(cls):
        with cls._lock:
            pickcodes = list(cls._pickcodes)
            cls._pickcodes.clear()
            cls._timer = None

        if not pickcodes: return
        
        from gevent import spawn
        spawn(cls._process_batch, pickcodes)

    @classmethod
    def _process_batch(cls, pickcodes):
        client = P115Service.get_client()
        if not client: return

        try:
            # 1. 获取免死金牌名单 (绝对不能删的根目录)
            config = get_config()
            protected_cids = {'0'}
            media_root = config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID)
            if media_root: protected_cids.add(str(media_root))
            save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            if save_path: protected_cids.add(str(save_path))

            raw_rules = settings_db.get_setting('p115_sorting_rules')
            if raw_rules:
                rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
                for rule in rules:
                    if rule.get('cid'): protected_cids.add(str(rule['cid']))

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # =================================================================
                    # 第一步：通过 PC 码从本地缓存锁定初始文件 (FID) 和 父目录 (PID)
                    # =================================================================
                    cursor.execute("SELECT id, parent_id FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (list(pickcodes),))
                    initial_files = cursor.fetchall()

                    if not initial_files:
                        logger.warning(f"  ➜ [深度删除] 本地缓存未找到对应 PC 码的文件，无法执行本地推导，任务终止。")
                        return

                    deleted_nodes = set()       # 记录所有被判死刑的节点 (文件 + 变空的目录)
                    nodes_to_check = set()      # 待检查是否变空的父目录
                    node_parent_map = {}        # 缓存节点关系 (id -> parent_id)，用于最后提炼顶级节点

                    for row in initial_files:
                        fid = str(row['id'])
                        pid = str(row['parent_id'])
                        deleted_nodes.add(fid)
                        node_parent_map[fid] = pid
                        if pid and pid not in protected_cids:
                            nodes_to_check.add(pid)

                    # =================================================================
                    # 第二步：自下而上溯源，本地计算空目录 (季目录 -> 剧目录)
                    # =================================================================
                    while nodes_to_check:
                        current_pid = nodes_to_check.pop()
                        if current_pid in protected_cids:
                            continue

                        # 查当前目录下的所有子节点
                        cursor.execute("SELECT id FROM p115_filesystem_cache WHERE parent_id = %s", (current_pid,))
                        children = {str(r['id']) for r in cursor.fetchall()}

                        # ★ 核心逻辑：如果该目录下的所有子节点都在死刑名单里，说明该目录将被掏空！
                        if children and children.issubset(deleted_nodes):
                            deleted_nodes.add(current_pid) # 目录本身加入死刑名单
                            
                            # 查当前目录的父目录，继续向上溯源 (比如季目录空了，继续查剧目录)
                            cursor.execute("SELECT parent_id FROM p115_filesystem_cache WHERE id = %s", (current_pid,))
                            parent_row = cursor.fetchone()
                            if parent_row and parent_row['parent_id']:
                                grand_pid = str(parent_row['parent_id'])
                                node_parent_map[current_pid] = grand_pid
                                if grand_pid not in protected_cids:
                                    nodes_to_check.add(grand_pid)

                    # =================================================================
                    # 第三步：提炼最终需要发送给 115 API 的顶级节点
                    # =================================================================
                    final_api_ids = []
                    for node in deleted_nodes:
                        parent_id = node_parent_map.get(node)
                        # 如果缓存 map 里没有，去库里查一下兜底
                        if not parent_id:
                            cursor.execute("SELECT parent_id FROM p115_filesystem_cache WHERE id = %s", (node,))
                            p_row = cursor.fetchone()
                            parent_id = str(p_row['parent_id']) if p_row else None

                        # ★ 核心优化：如果一个节点的父节点也在死刑名单里，说明它会被连锅端，不需要单独发 API！
                        if parent_id not in deleted_nodes:
                            final_api_ids.append(node)

                    # =================================================================
                    # 第四步：执行唯一一次 115 API 删除调用
                    # =================================================================
                    if final_api_ids:
                        logger.info(f"  ➜ [深度删除] 本地推导完毕！向 115 发送批量删除指令 (共 {len(final_api_ids)} 个顶级节点)...")
                        resp = client.fs_delete(final_api_ids)
                        
                        if resp.get('state'):
                            logger.info(f"  ➜ [深度删除] 115 网盘文件/空目录物理销毁成功！")
                        else:
                            logger.error(f"  ➜ [深度删除] 115 API 删除失败: {resp}")
                            return # API 失败则不清理本地库，保持一致性

                    # =================================================================
                    # 第五步：清理本地数据库记录 (缓存表 + 整理记录表)
                    # =================================================================
                    if deleted_nodes:
                        # 1. 清理目录树缓存
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(deleted_nodes),))
                        deleted_cache_count = cursor.rowcount

                        # 2. 清理整理记录
                        cursor.execute("DELETE FROM p115_organize_records WHERE pick_code = ANY(%s)", (list(pickcodes),))
                        deleted_record_count = cursor.rowcount

                        conn.commit()
                        logger.info(f"  ➜ [深度删除] 本地数据清理完毕: 缓存表移除 {deleted_cache_count} 条, 记录表移除 {deleted_record_count} 条。")

        except Exception as e:
            logger.error(f"  ➜ [深度删除] 执行异常: {e}", exc_info=True)

def delete_115_files_by_webhook(item_path, pickcodes):
    """
    【V6 终极缓冲版】接收神医 Webhook 传来的提取码，加入缓冲队列。
    """
    if not pickcodes: return
    WebhookDeleteBuffer.add(pickcodes)

# ======================================================================
# ★★★ 手动纠错后共享 RAW 覆盖上传触发器 ★★★
# ======================================================================
_SHARED_RAW_REUPLOAD_LOCK = threading.Lock()
_LAST_SHARED_RAW_REUPLOAD_AT = 0

def _kick_shared_raw_reupload_detached(reason: str = '', delay: float = 8.0):
    """手动重组修复 RAW 后，异步触发一次 Rapid 共享维护。

    Rapid v2 已废弃 115 分享状态同步；这里只唤醒共享维护线程，避免重组接口
    被中心网络波动拖死。后续需要强制重登记时，由共享资源维护任务扫描 dirty_raw。
    """
    global _LAST_SHARED_RAW_REUPLOAD_AT

    now = time.time()
    with _SHARED_RAW_REUPLOAD_LOCK:
        if now - _LAST_SHARED_RAW_REUPLOAD_AT < 20:
            return {'started': False, 'message': '共享 RAW 重传同步刚触发过，本次不重复启动'}
        _LAST_SHARED_RAW_REUPLOAD_AT = now

    def _runner():
        if delay and delay > 0:
            time.sleep(delay)
        try:
            from tasks.shared_resource_tasks import task_shared_resource_maintenance
            logger.info(f"  ➜ [批量重组] 异步触发 Rapid 共享维护：{reason or 'manual-reorganize-raw-fix'}")
            task_shared_resource_maintenance(maintenance_silent=True)
        except Exception as e:
            logger.warning(f"  ➜ [批量重组] 触发 Rapid 共享维护失败: {e}", exc_info=True)

    threading.Thread(
        target=_runner,
        name='shared-rapid-maintenance-after-manual-correct',
        daemon=True,
    ).start()
    return {'started': True, 'message': '已异步触发 Rapid 共享维护'}

# ======================================================================
# ★★★ 手动纠错缓冲队列 (实现批量重组与一次性刷新) ★★★
# ======================================================================
class ManualCorrectTaskQueue:
    _lock = threading.Lock()
    _tasks = {}  # 结构: {(tmdb_id, media_type, target_cid, season_num): [record_id1, record_id2, ...]}
    _timer = None

    @classmethod
    def add(cls, record_id, tmdb_id, media_type, target_cid, season_num):
        with cls._lock:
            key = (tmdb_id, media_type, target_cid, season_num)
            if key not in cls._tasks:
                cls._tasks[key] = []
            cls._tasks[key].append(record_id)

            if cls._timer is not None:
                cls._timer.kill()
            from gevent import spawn_later
            # 延迟 2 秒，收集前端并发发来的所有同批次请求
            cls._timer = spawn_later(2.0, cls._execute_all)

    @classmethod
    def _execute_all(cls):
        with cls._lock:
            tasks = cls._tasks.copy()
            cls._tasks.clear()
            cls._timer = None

        from gevent import spawn
        for key, record_ids in tasks.items():
            spawn(cls._process_batch, key, record_ids)

    @classmethod
    def _process_batch(cls, key, record_ids):
        """旧接口兼容层：收集到同批请求后，提交给统一 media 任务队列执行。"""
        tmdb_id, media_type, target_cid, season_num = key
        record_ids = list(record_ids or [])
        if not record_ids:
            return

        try:
            import task_manager

            def _queued_manual_correct_task(processor=None):
                return _batch_manual_correct(record_ids, tmdb_id, media_type, target_cid, season_num)

            success = task_manager.submit_task(
                task_function=_queued_manual_correct_task,
                task_name=f"手动重组整理记录({len(record_ids)}条)",
                processor_type='media',
            )
            if success:
                logger.info(
                    f"  ➜ [批量重组] 已提交到媒体任务队列：{len(record_ids)} 条记录 -> "
                    f"ID:{tmdb_id}, type={media_type}, target={target_cid}, season={season_num or '-'}"
                )
            else:
                logger.warning(
                    f"  ➜ [批量重组] 提交媒体任务队列失败，可能已有任务正在运行："
                    f"{len(record_ids)} 条记录 -> ID:{tmdb_id}"
                )
        except Exception as e:
            logger.error(f"  ➜ 批量重组提交任务队列失败: {e}", exc_info=True)


def manual_correct_organize_record(record_id, tmdb_id, media_type, target_cid, season_num=None):
    """手动纠错兼容入口：保留旧 API 的 2 秒合批，但最终必须进入媒体任务队列。"""
    ManualCorrectTaskQueue.add(record_id, tmdb_id, media_type, target_cid, season_num)
    return True


def _batch_manual_correct(record_ids, tmdb_id, media_type, target_cid, season_num=None):
    """真正的批量执行逻辑"""
    client = P115Service.get_client()
    if not client: return

    # 1. 批量获取数据库记录
    records = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, file_id, original_name FROM p115_organize_records WHERE id = ANY(%s)", (list(record_ids),))
                records = cursor.fetchall()
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return

    if not records: return

    # 2. 批量获取旧缓存
    old_caches = {}
    file_ids = [str(r['file_id']) for r in records]
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, parent_id, pick_code, sha1, local_path FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(file_ids),))
                for row in cursor.fetchall():
                    old_caches[str(row['id'])] = row
    except: pass

    root_items = []
    old_pids = set()
    old_cids_to_check = set()
    refresh_target_dirs = set()
    config = get_config()
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)

    def _remember_old_cid_chain(start_cid, info_data=None):
        """手动重组移动后，需要检查旧目录及其媒体主目录是否已空。"""
        current = str(start_cid or '').strip()
        seen = set()

        def _remember_path_nodes(payload):
            for node in (payload or {}).get('paths') or (payload or {}).get('path') or []:
                if not isinstance(node, dict):
                    continue
                cid_val = str(node.get('file_id') or node.get('cid') or node.get('fid') or '').strip()
                if cid_val and cid_val != '0':
                    old_cids_to_check.add(cid_val)

        def _parent_from_info_payload(payload):
            payload = payload if isinstance(payload, dict) else {}
            parent = str(payload.get('parent_id') or payload.get('pid') or '').strip()
            if parent:
                return parent
            path_nodes = payload.get('paths') or payload.get('path') or []
            if isinstance(path_nodes, list):
                for node in reversed(path_nodes):
                    if not isinstance(node, dict):
                        continue
                    cid_val = str(node.get('file_id') or node.get('cid') or node.get('fid') or '').strip()
                    if cid_val and cid_val != '0':
                        return cid_val
            return ''

        _remember_path_nodes(info_data)
        for _ in range(20):
            if not current or current == '0' or current in seen:
                break
            seen.add(current)
            old_cids_to_check.add(current)
            node = P115CacheManager.get_node_info(current)
            parent_id = str((node or {}).get('parent_id') or '').strip() if node else ''
            if not parent_id:
                try:
                    info_res = client.fs_get_info(current)
                    if info_res and info_res.get('state') and isinstance(info_res.get('data'), dict):
                        remote_info = info_res.get('data') or {}
                        _remember_path_nodes(remote_info)
                        parent_id = _parent_from_info_payload(remote_info)
                except Exception as e:
                    logger.debug(f"  ➜ [批量重组] 回查旧目录父级失败: cid={current}, err={e}")
            if not parent_id or parent_id == '0':
                break
            current = parent_id

    for r in records:
        file_id = str(r['file_id'])
        original_name = r['original_name']
        old_cache = old_caches.get(file_id)

        old_pid = None
        pick_code = None
        sha1 = None
        info_data = {}

        # ★ 核心提速：优先使用本地缓存，彻底干掉 1.5 秒/次的 API 延迟！
        if old_cache and old_cache.get('parent_id') and old_cache.get('pick_code'):
            old_pid = old_cache['parent_id']
            pick_code = old_cache['pick_code']
            sha1 = old_cache.get('sha1')
            info_data = {
                'file_id': file_id, 
                'file_name': original_name, 
                'file_category': '1', 
                'parent_id': old_pid, 
                'pick_code': pick_code, 
                'sha1': sha1
            }
        else:
            # 只有当缓存丢失时，才迫不得已去请求 115 API
            info_res = client.fs_get_info(file_id)
            if not info_res or not info_res.get('state') or not info_res.get('data'):
                logger.warning(f"无法在 115 中定位到该文件(ID:{file_id})，可能已被删除。")
                continue
            info_data = info_res['data']
            old_pid = info_data.get('parent_id') or info_data.get('cid')
            pick_code = info_data.get('pick_code')
            sha1 = info_data.get('sha1')

        if old_pid:
            old_pids.add(str(old_pid))
            _remember_old_cid_chain(old_pid, info_data)

        root_items.append({
            'fid': file_id,
            'file_id': file_id,
            'fn': original_name,
            'fc': str(info_data.get('file_category', '1')),
            'pid': old_pid,
            'pc': pick_code,
            'pick_code': pick_code,
            'sha1': sha1,
            '_record_id': r['id'],
            '_old_cache': old_cache,
            '_info_data': info_data
        })

        # 收集需要刷新的本地旧目录
        if local_root and old_cache and old_cache.get('local_path'):
            old_file_rel_path = str(old_cache['local_path']).lstrip('\\/')
            old_dir = os.path.abspath(os.path.dirname(os.path.join(local_root, old_file_rel_path)))
            refresh_target_dirs.add(old_dir)

    if not root_items: return

    title = root_items[0]['fn']
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    try:
        import handler.tmdb as tmdb
        if media_type == 'tv': details = tmdb.get_tv_details(tmdb_id, api_key)
        else: details = tmdb.get_movie_details(tmdb_id, api_key)
        if details: title = details.get('title') or details.get('name') or title
    except: pass

    logger.info(f"  ➜ [批量重组] 开始定向整理《{title}》，共 {len(root_items)} 个文件。")
    logger.debug(f"  ➜ [批量重组] 定向整理详情：TMDb={tmdb_id}")

    organizer = SmartOrganizer(client, tmdb_id, media_type, title, None, False)
    organizer.is_manual_correct = True
    if season_num is not None and str(season_num).strip():
        organizer.forced_season = int(season_num)
        logger.info(f"  ➜ [批量重组] 已指定整理到第 {organizer.forced_season} 季。")

    # ★ 核心：将列表直接传给 execute，底层会自动打包成一次 115 API 移动请求！
    success = organizer.execute(root_items, target_cid)
    if not success:
        logger.error("执行批量重组失败。")
        return

    # ★ 手动重组后，立即把已分享过的同 SHA1 RAW 标脏，并异步触发中心 RAW 覆盖上传。
    try:
        dirty_count = 0
        for raw_sha1 in sorted({str(item.get('sha1') or '').strip().upper() for item in root_items if str(item.get('sha1') or '').strip()}):
            dirty_count += P115CacheManager.mark_shared_raw_dirty_for_sha1(
                raw_sha1,
                reason='manual_reorganize_raw_etk_fixed',
                tmdb_id=tmdb_id,
                media_type=media_type,
            )
        if dirty_count > 0:
            kick = _kick_shared_raw_reupload_detached(
                reason=f'手动重组修复 RAW {dirty_count} 个共享记录',
                delay=8.0,
            )
            logger.info(f"  ➜ [批量重组] 已触发 {dirty_count} 条共享媒体信息重传。")
            logger.debug(f"  ➜ [批量重组] 共享媒体信息重传触发结果：{kick}")
    except Exception as e:
        logger.debug(f"  ➜ [批量重组] 标记共享 RAW 重传失败: {e}")

    # ★ 查找并重组关联字幕 (批量)
    sub_items = []
    for old_pid in old_pids:
        if str(old_pid) == '0': continue
        try:
            sub_res = client.fs_files({'cid': old_pid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            for item in sub_res.get('data', []):
                if str(item.get('fc', '0')) == '1':
                    sub_name = item.get('fn') or item.get('n') or item.get('file_name', '')
                    ext = sub_name.split('.')[-1].lower() if '.' in sub_name else ''
                    if ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']:
                        # 检查是否匹配任何一个视频的基础名
                        for r_item in root_items:
                            v_name = r_item['_info_data'].get('file_name') or r_item['fn']
                            if _is_related_sidecar_name(v_name, sub_name):
                                sub_items.append(item)
                                break
        except Exception as e:
            logger.warning(f"  ➜ 查找关联字幕失败: {e}")

    if sub_items:
        if root_items:
            subtitle_video_names = {}
            for r_item in root_items:
                info_name = (
                    r_item.get('_new_filename')
                    or (r_item.get('_info_data') or {}).get('file_name')
                    or r_item.get('fn')
                    or r_item.get('file_name')
                    or ''
                )
                if not info_name:
                    continue
                season_key = _extract_sidecar_season_number(info_name)
                episode_key = _extract_sidecar_episode_number(info_name)
                part_key = _extract_sidecar_part_number(info_name)
                if episode_key is None:
                    continue
                if season_key is None:
                    season_key = organizer.forced_season or 1
                subtitle_video_names[(int(season_key), int(episode_key), part_key)] = info_name.rsplit('.', 1)[0]

            for sub_item in sub_items:
                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name', '')
                season_key = _extract_sidecar_season_number(sub_name)
                episode_key = _extract_sidecar_episode_number(sub_name)
                part_key = _extract_sidecar_part_number(sub_name)
                if episode_key is None:
                    continue
                if season_key is None:
                    season_key = organizer.forced_season or 1
                forced_base_name = subtitle_video_names.get((int(season_key), int(episode_key), part_key))
                if not forced_base_name:
                    forced_base_name = subtitle_video_names.get((int(season_key), int(episode_key), None))
                if forced_base_name:
                    sub_item['_forced_base_name'] = forced_base_name
                    sub_item['_forced_season'] = int(season_key)
                    sub_item['_forced_episode'] = int(episode_key)
        logger.info(f"  🔤 [批量重组] 发现 {len(sub_items)} 个关联字幕，跟随重组...")
        organizer.execute(sub_items, target_cid)

    # ★ 本地擦屁股：精准删除旧的本地 STRM 和空目录
    if local_root:
        import shutil
        protected_dirs = {os.path.abspath(local_root)}
        for rule in organizer.rules:
            cat_path = rule.get('category_path') or rule.get('dir_name')
            if cat_path:
                protected_dirs.add(os.path.abspath(os.path.join(local_root, cat_path.lstrip('\\/'))))
        protected_dirs.add(os.path.abspath(os.path.join(local_root, "未识别")))

        old_strm_paths_for_emby = [] # ★ 新增：收集旧路径用于极速扫描

        for r_item in root_items:
            old_cache = r_item['_old_cache']
            if not old_cache or not old_cache.get('local_path'): continue

            old_file_rel_path = str(old_cache['local_path']).lstrip('\\/')
            old_strm_rel_path = os.path.splitext(old_file_rel_path)[0] + ".strm"
            old_strm_full_path = os.path.join(local_root, old_strm_rel_path)

            old_strm_paths_for_emby.append(old_strm_full_path) # ★ 收集路径

            if os.path.exists(old_strm_full_path):
                os.remove(old_strm_full_path)
                logger.debug(f"  ➜ 删除本地旧 STRM: {old_strm_full_path}")

            old_mi_full_path = os.path.splitext(old_file_rel_path)[0] + "-mediainfo.json"
            if os.path.exists(old_mi_full_path):
                os.remove(old_mi_full_path)

            old_dir_full_path = os.path.dirname(old_strm_full_path)
            old_base_name = os.path.splitext(os.path.basename(old_file_rel_path))[0]
            if os.path.exists(old_dir_full_path):
                for f in os.listdir(old_dir_full_path):
                    if f.startswith(old_base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup', 'nfo', 'jpg', 'png', 'jpeg', 'bif']:
                        sub_to_del = os.path.join(old_dir_full_path, f)
                        try:
                            os.remove(sub_to_del)
                        except: pass

        # 向上递归清理本地空目录
        for old_dir in list(refresh_target_dirs):
            curr_dir = old_dir
            while curr_dir and curr_dir not in protected_dirs:
                if os.path.exists(curr_dir):
                    has_media = False
                    for root, _, files in os.walk(curr_dir):
                        for f in files:
                            ext = f.split('.')[-1].lower()
                            if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov'}:
                                has_media = True
                                break
                        if has_media: break

                    if not has_media:
                        shutil.rmtree(curr_dir)
                        logger.info(f"  ➜ 本地旧目录已无媒体文件，执行删除: {curr_dir}")
                        curr_dir = os.path.dirname(curr_dir)
                    else:
                        break
                else:
                    break

        # =================================================================
        # ★ 核心优化：调用极速扫描接口，秒级清理 Emby 中的失效旧条目
        # =================================================================
        emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        if emby_url and emby_api_key and old_strm_paths_for_emby:
            from handler import emby
            logger.info(f"  ➜ 正在通知 Emby 极速扫描旧路径以清理失效媒体项...")
            try:
                # 传入 update_type="Deleted"，复用我们刚写的极速向上寻根扫描逻辑
                emby.notify_emby_file_changes(old_strm_paths_for_emby, emby_url, emby_api_key, update_type="Deleted")
            except Exception as e:
                logger.warning(f"  ➜ 通知 Emby 极速扫描旧路径失败: {e}")

    # ★ 网盘擦屁股：直接移交全局垃圾回收器
    for r_item in root_items:
        info_data = r_item['_info_data']
        pid = str(r_item.get('pid') or r_item.get('parent_id') or '')
        if pid and pid != '0':
            old_cids_to_check.add(pid)
        else:
            paths = info_data.get('paths') or []
            if paths:
                # paths 通常是从根到当前目录的链条，只取最后一级
                leaf = paths[-1]
                cid_val = str(leaf.get('file_id') or leaf.get('cid') or '')
                if cid_val and cid_val != '0':
                    old_cids_to_check.add(cid_val)

    if old_cids_to_check:
        from handler.p115_service import P115DeleteBuffer
        logger.info(f"  ➜ 已将网盘旧目录链条 ({len(old_cids_to_check)}个层级) 加入全局清理队列，稍后执行清理...")
        P115DeleteBuffer.add(fids=[], base_cids=list(old_cids_to_check))

    # ★ 更新记录状态
    try:
        category_name = "未识别"
        for rule in organizer.rules:
            if str(rule.get('cid')) == str(target_cid):
                category_name = rule.get('dir_name', '未识别')
                break
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE p115_organize_records 
                    SET status = 'success', tmdb_id = %s, media_type = %s, target_cid = %s, category_name = %s
                    WHERE id = ANY(%s)
                """, (tmdb_id, media_type, target_cid, category_name, list(record_ids)))
                conn.commit()
    except Exception as e: pass

    logger.info(f"  ➜ [批量重组] {len(root_items)} 个文件处理完成！")
