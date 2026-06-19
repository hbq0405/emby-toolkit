# handler/shared_center_client.py
# ETK 共享资源中心客户端（Rapid v2）：中心只保存资源索引/manifest，不保存 CK、不创建 115 分享。
import base64
import hashlib
import json
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

import config_manager
import constants
from database import settings_db

logger = logging.getLogger(__name__)

_CENTER_HTTP = requests.Session()
_CENTER_HTTP.mount('http://', requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=128))
_CENTER_HTTP.mount('https://', requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=128))




def _normalize_pro_tier(value: str = '') -> str:
    text = str(value or '').strip().upper()
    if text in {'M', 'MONTH', 'MONTHLY', '月卡'}:
        return 'M'
    if text in {'Y', 'YEAR', 'YEARLY', '年卡'}:
        return 'Y'
    if text in {'L', 'LIFETIME', 'FOREVER', '永久', '终身'}:
        return 'L'
    return ''


def _sha256_or_empty(value: str = '') -> str:
    text = str(value or '').strip()
    return hashlib.sha256(text.encode('utf-8')).hexdigest() if text else ''


def _current_server_id_hash() -> str:
    """返回当前 Emby ServerID 的 hash；共享中心设备身份只认 ServerID。"""
    server_id = ''
    try:
        import extensions
        server_id = str(getattr(extensions, 'EMBY_SERVER_ID', '') or '').strip()
    except Exception:
        server_id = ''
    if not server_id:
        server_id = str((config_manager.APP_CONFIG or {}).get('emby_server_id') or '').strip()
    return _sha256_or_empty(server_id)


def _infer_pro_tier_from_license(value: str = '') -> str:
    text = str(value or '').strip().upper()
    import re
    match = re.search(r'ETK[-_]?([MYL])(?:[-_]|$)', text)
    if match:
        return match.group(1)
    return _normalize_pro_tier(text)


def _parse_pro_expire_time(value: str = ''):
    text = str(value or '').strip()
    if not text:
        return None
    try:
        if text.startswith('2099'):
            return datetime.max.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(text.replace('Z', '+00:00'))
    except Exception:
        return None


def _pro_expire_state(expire_time: str = '') -> str:
    parsed = _parse_pro_expire_time(expire_time)
    if not parsed:
        return 'unknown'
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return 'active' if parsed > datetime.now(timezone.utc) else 'expired'


def _setting_text(key: str) -> str:
    try:
        return str(settings_db.get_setting(key) or '').strip()
    except Exception:
        return ''


def build_current_pro_quota_report_payload() -> Dict[str, Any]:
    """把客户端当前 Pro 认证状态整理成上报中心的 payload；只上报 hash，不传卡密/ServerID 明文。"""
    app_cfg = config_manager.APP_CONFIG or {}
    cfg = _shared_cfg()
    license_key = ''
    for key in ('pro_license_key', 'license_key'):
        license_key = _setting_text(key)
        if license_key:
            break
    tier = _normalize_pro_tier(app_cfg.get('pro_tier') or app_cfg.get('pro_level') or app_cfg.get('pro_card_type')) or _infer_pro_tier_from_license(license_key)
    app_is_pro_active = bool(app_cfg.get('is_pro_active', False))
    pro_expire_time = str(app_cfg.get('pro_expire_time') or '').strip() or _setting_text('pro_expire_time')
    expire_state = _pro_expire_state(pro_expire_time)
    effective_is_pro_active = app_is_pro_active
    auth_state = 'active' if app_is_pro_active else 'inactive'
    auth_grace = False

    # 共享中心的 Pro 额度是累计权益，不能因为本机到认证服务的短暂网络抖动
    # 把 APP_CONFIG.is_pro_active 瞬时 false 上报成“明确失效”。只要本地仍有
    # 未过期卡密记录，就按宽限认证上报；真正过期/无卡密才让中心停用额度。
    if not effective_is_pro_active and license_key and expire_state == 'active':
        effective_is_pro_active = True
        auth_state = 'grace_local_unverified'
        auth_grace = True
    elif app_is_pro_active and expire_state == 'expired':
        effective_is_pro_active = False
        auth_state = 'expired_local'

    try:
        import extensions
        server_id = str(getattr(extensions, 'EMBY_SERVER_ID', '') or '').strip()
    except Exception:
        server_id = ''
    return {
        'is_pro_active': effective_is_pro_active,
        'pro_tier': tier,
        'pro_expire_time': pro_expire_time,
        'pro_license_hash': _sha256_or_empty(license_key),
        'pro_server_id_hash': _sha256_or_empty(server_id),
        'client_version': _app_version(),
        'auth_source': 'client_local_license_grace' if auth_grace else 'client_app_config',
        'auth_state': auth_state,
        'auth_grace': auth_grace,
        'local_app_is_pro_active': app_is_pro_active,
        'pro_expire_state': expire_state,
    }

def _app_version() -> str:
    return str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0').strip() or '0.0.0'


def _client_user_agent() -> str:
    return f"ETK/{_app_version()} RapidV2"


def _raise_for_center_error(resp):
    if resp.ok:
        return
    try:
        body = resp.json() if resp.text else {}
    except Exception:
        body = {}
    if resp.status_code == 426:
        min_version = body.get('min_client_version') if isinstance(body, dict) else ''
        client_version = body.get('client_version') if isinstance(body, dict) else ''
        message = body.get('message') if isinstance(body, dict) else ''
        raise RuntimeError(message or f"共享中心拒绝服务：当前客户端版本 {client_version or _app_version()} 低于中心要求 {min_version or '未知'}")
    detail = body.get('detail') or body.get('message') if isinstance(body, dict) else ''
    raise RuntimeError(f"共享中心请求失败: {resp.status_code} {detail or resp.text[:300]}")


def _request_kwargs(timeout: int) -> Dict[str, Any]:
    kwargs = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    return kwargs


def _shared_cfg() -> Dict[str, Any]:
    return settings_db.get_shared_resource_config() or {}


def shared_center_enabled() -> bool:
    cfg = _shared_cfg()
    return bool(cfg.get('p115_shared_resource_enabled')) and bool(cfg.get('p115_shared_center_url'))


def shared_resource_mode() -> str:
    return 'rapid'


def _safe_int_or_none(value):
    try:
        if value in (None, ''):
            return None
        return int(float(value))
    except Exception:
        return None


def _canonical_item_type(value: str) -> str:
    text = str(value or '').strip().lower()
    if text in ('movie', 'movie_file', 'movie_folder', 'film'):
        return 'Movie'
    if text in ('episode', 'episode_file', 'single'):
        return 'Episode'
    if text in ('season', 'season_pack', 'tv_pack'):
        return 'Season'
    if text in ('series', 'show', 'tv'):
        return 'Series'
    return str(value or '').strip() or 'Movie'


def _normalize_gap_item_for_center(item: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(item or {})
    item_type = _canonical_item_type(item.get('item_type') or item.get('target_type'))
    season = _safe_int_or_none(item.get('season_number'))
    episode = _safe_int_or_none(item.get('episode_number'))
    # Rapid v2：中心缺口仍按电影/季聚合。分集只是追更池资源，不把 wanted_gaps 打爆。
    if item_type == 'Episode' and season is not None:
        item_type = 'Season'
        episode = None
    if item_type in ('Movie', 'Series'):
        episode = None
        if item_type == 'Movie':
            season = None
    if item_type == 'Season':
        episode = None
    return {
        'tmdb_id': str(item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or item.get('tmdb_id') or '').strip(),
        'item_type': item_type,
        'season_number': season,
        'episode_number': episode,
        'title': item.get('title') or item.get('name') or item.get('file_name') or None,
        'release_year': _safe_int_or_none(item.get('release_year') or item.get('year')),
    }


def _dedupe_gap_items_for_center(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        item = _normalize_gap_item_for_center(raw)
        if not item.get('tmdb_id') or not item.get('item_type'):
            continue
        key = (item.get('tmdb_id'), item.get('item_type'), item.get('season_number'), item.get('episode_number'))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out




def _encode_raw_ffprobe_zstd_b64(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把 RAW JSON 压成 zstd+base64 作为上传传输格式。

    中心端仍会解压、sanitize、校验 summary_json 后再落盘；这里仅减少
    HTTP 请求体体积。zstandard 不可用或压缩失败时返回空 dict，调用方
    自动回退旧的 raw_ffprobe_json 直传。
    """
    if not isinstance(raw, dict) or not raw:
        return {}
    try:
        import zstandard as zstd
        raw_bytes = json.dumps(raw, ensure_ascii=False, separators=(',', ':'), default=str).encode('utf-8')
        compressed = zstd.ZstdCompressor(level=3).compress(raw_bytes)
        return {
            'encoding': 'zstd_base64',
            'raw_zstd_b64': base64.b64encode(compressed).decode('ascii'),
            'raw_bytes': len(raw_bytes),
            'compressed_bytes': len(compressed),
        }
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] RAW zstd 压缩失败，回退 JSON 直传: {e}")
        return {}


def _raw_upload_payload_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    encoded = _encode_raw_ffprobe_zstd_b64(raw)
    if encoded:
        return encoded
    return {'raw_ffprobe_json': raw or {}}


def _raw_upload_legacy_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {'raw_ffprobe_json': raw or {}}


def _raw_upload_used_compression(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get('raw_zstd_b64') or payload.get('raw_zstd_base64') or payload.get('raw_compressed_b64'):
        return True
    for item in payload.get('items') or []:
        if isinstance(item, dict) and (item.get('raw_zstd_b64') or item.get('raw_zstd_base64') or item.get('raw_compressed_b64')):
            return True
    return False

class SharedCenterClient:
    def __init__(self):
        cfg = _shared_cfg()
        self.base_url = str(cfg.get('p115_shared_center_url') or 'https://shared.55565576.xyz').rstrip('/')
        self.device_token = str(cfg.get('p115_shared_device_token') or '').strip()

    @property
    def ready(self) -> bool:
        return bool(self.base_url and self.device_token)

    def _headers(self) -> Dict[str, str]:
        version = _app_version()
        return {
            'X-Device-Token': self.device_token,
            'X-Client-Version': version,
            'X-ETK-Version': version,
            'Content-Type': 'application/json',
            'User-Agent': _client_user_agent(),
        }

    def _post(self, path: str, payload: Dict[str, Any] | None = None, timeout: int = 20) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = _CENTER_HTTP.post(url, headers=self._headers(), json=payload or {}, **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def _get(self, path: str, params: Dict[str, Any] | None = None, timeout: int = 15) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = _CENTER_HTTP.get(url, headers=self._headers(), params=params or {}, **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def register_device(self, name: str = '') -> Dict[str, Any]:
        if not self.base_url:
            raise RuntimeError('共享中心地址未配置')
        server_id_hash = _current_server_id_hash()
        if not server_id_hash:
            raise RuntimeError('无法读取 Emby ServerID，不能注册共享中心设备')
        payload = {
            'name': str(name or '').strip() or 'ETK Device',
            'server_id_hash': server_id_hash,
        }
        headers = {'X-Client-Version': _app_version(), 'X-ETK-Version': _app_version(), 'Content-Type': 'application/json', 'User-Agent': _client_user_agent()}
        resp = _CENTER_HTTP.post(f"{self.base_url}/api/v1/devices/register", headers=headers, json=payload, **_request_kwargs(20))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def me(self) -> Dict[str, Any]:
        return self._get('/api/v1/me', timeout=12)

    def stats(self) -> Dict[str, Any]:
        return self._get('/api/v1/stats', timeout=12)

    def pro_quota(self) -> Dict[str, Any]:
        return self._get('/api/v1/pro-quota/me', timeout=12)

    def report_pro_quota_auth(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self._post('/api/v1/pro-quota/report', payload or build_current_pro_quota_report_payload(), timeout=15)

    def report_current_pro_quota_auth(self) -> Dict[str, Any]:
        return self.report_pro_quota_auth(build_current_pro_quota_report_payload())

    def credit_ledger(self, limit: int = 200) -> Dict[str, Any]:
        return self._get('/api/v1/credit/ledger', {'limit': max(1, min(int(limit or 200), 1000))}, timeout=15)

    def report_gaps(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        items = _dedupe_gap_items_for_center(items)
        if not items:
            return {'count': 0, 'items': []}
        return self._post('/api/v1/gaps/batch', {'items': items}, timeout=20)

    def list_open_gaps(self, limit: int = 200) -> Dict[str, Any]:
        return self._get('/api/v1/gaps/open', {'limit': max(1, min(int(limit or 200), 1000))}, timeout=15)

    def list_sources(self, *, q: str = '', status: str = 'alive,available,updating,inconsistent', mine_only: bool = False,
                     source_kind: str = '', item_type: str = '', tmdb_id: str = '', source_id: str = '',
                     source_ids: List[str] | None = None, limit: int = 200, offset: int = 0,
                     **_ignored) -> Dict[str, Any]:
        ids = []
        for value in source_ids or []:
            text = str(value or '').strip()
            if text and text not in ids:
                ids.append(text)
        return self._get('/api/v1/sources/list', {
            'q': q or '',
            'status': status or 'alive,available,updating,inconsistent',
            'mine_only': 1 if mine_only else 0,
            'source_kind': source_kind or '',
            'item_type': item_type or '',
            'tmdb_id': tmdb_id or '',
            'source_id': source_id or '',
            'source_ids': ','.join(ids),
            'limit': max(1, min(int(limit or 200), 1000)),
            'offset': max(0, int(offset or 0)),
        }, timeout=25)

    def list_display_sources(self, *, q: str = '', status: str = 'alive,available,updating,inconsistent,incomplete',
                             item_type: str = '', tmdb_id: str = '', order_by: str = 'latest',
                             limit: int = 200, offset: int = 0, force_refresh: bool = False, **_ignored) -> Dict[str, Any]:
        """中心资源库展示口径：由中心端分页、筛选、聚合。

        默认只返回电影和季容器；连载季返回公共 season_hub，单集只作为 children/pack_items。
        force_refresh=True 时绕过中心端展示缓存并重建缓存。
        """
        return self._get('/api/v1/sources/display-list', {
            'q': q or '',
            'status': status or 'alive,available,updating,inconsistent,incomplete',
            'item_type': item_type or '',
            'tmdb_id': tmdb_id or '',
            'order_by': order_by or 'latest',
            'limit': max(1, min(int(limit or 200), 1000)),
            'offset': max(0, int(offset or 0)),
            'force_refresh': 1 if force_refresh else 0,
        }, timeout=30)

    def list_cloud_search_sources(self, *, q: str = '', status: str = 'alive,available,updating,inconsistent,incomplete',
                                  item_type: str = '', tmdb_id: str = '', order_by: str = 'latest',
                                  limit: int = 200, offset: int = 0, **_ignored) -> Dict[str, Any]:
        """云资源搜索专用口径：返回可挑选的电影/季包行，不影响中心资源库剧卡聚合。"""
        return self._get('/api/v1/sources/cloud-search', {
            'q': q or '',
            'status': status or 'alive,available,updating,inconsistent,incomplete',
            'item_type': item_type or '',
            'tmdb_id': tmdb_id or '',
            'order_by': order_by or 'latest',
            'limit': max(1, min(int(limit or 200), 1000)),
            'offset': max(0, int(offset or 0)),
        }, timeout=30)

    def list_display_home(self, *, limit_per_section: int = 10, force_refresh: bool = False, **_ignored) -> Dict[str, Any]:
        return self._get('/api/v1/sources/display-home', {
            'limit_per_section': max(1, min(int(limit_per_section or 10), 20)),
            'force_refresh': 1 if force_refresh else 0,
        }, timeout=15)


    def list_display_children(self, *, source_kind: str = '', source_id: str = '', source_ids: List[str] | None = None,
                              hub_id: str = '', limit: int = 5000, offset: int = 0, **_ignored) -> Dict[str, Any]:
        """中心资源库懒加载子项：展开季包时再取 children / pack_items。"""
        ids = []
        for value in source_ids or []:
            text = str(value or '').strip()
            if text and text not in ids:
                ids.append(text)
        if source_id and source_id not in ids:
            ids.insert(0, str(source_id).strip())
        return self._get('/api/v1/sources/display-children', {
            'source_kind': source_kind or '',
            'source_id': source_id or '',
            'source_ids': ','.join(ids),
            'hub_id': hub_id or '',
            'limit': max(1, min(int(limit or 5000), 20000)),
            'offset': max(0, int(offset or 0)),
        }, timeout=30)


    def display_detail(self, *, source_kind: str = '', source_id: str = '', hub_id: str = '',
                       tmdb_id: str = '', item_type: str = '', season_number=None,
                       limit: int = 200, include_people: bool = False, **_ignored) -> Dict[str, Any]:
        """中心资源库卡片详情：点开卡片后取展示元数据、演职员和资源列表。"""
        params = {
            'source_kind': source_kind or '',
            'source_id': source_id or '',
            'hub_id': hub_id or '',
            'tmdb_id': tmdb_id or '',
            'item_type': item_type or '',
            'limit': max(1, min(int(limit or 200), 1000)),
            'include_people': 1 if include_people else 0,
        }
        if season_number not in (None, ''):
            params['season_number'] = season_number
        return self._get('/api/v1/sources/display-detail', params, timeout=30)

    def list_hubs(self, *, q: str = '', status: str = '', tmdb_id: str = '', limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        return self._get('/api/v1/hubs/list', {'q': q or '', 'status': status or '', 'tmdb_id': tmdb_id or '', 'limit': limit, 'offset': offset}, timeout=20)

    def search_sources(self, items: List[Dict[str, Any]], limit_per_item: int = 20) -> Dict[str, Any]:
        # Rapid v2：订阅命中也走中心展示口径。
        # Season 查询返回“完结客户端包”或“公共连载季包”，而不是把单集散铺给客户端。
        results = []
        for item in _dedupe_gap_items_for_center(items):
            kind = str(item.get('item_type') or '').strip()
            if kind == 'Season':
                resp = self.list_display_sources(
                    tmdb_id=item.get('tmdb_id') or '',
                    item_type='Pack',
                    status='alive,available,updating,inconsistent,incomplete',
                    limit=limit_per_item,
                )
            elif kind == 'Movie':
                resp = self.list_display_sources(
                    tmdb_id=item.get('tmdb_id') or '',
                    item_type='Movie',
                    status='alive,available',
                    limit=limit_per_item,
                )
            else:
                resp = self.list_sources(tmdb_id=item.get('tmdb_id') or '', item_type=kind, limit=limit_per_item)
            sources = resp.get('items') or []
            if kind == 'Season' and item.get('season_number') is not None:
                sn = int(item.get('season_number'))
                sources = [s for s in sources if int(s.get('season_number') or -999) == sn]
            results.append({'query': item, 'sources': sources})
        return {'results': results}

    def probe_subscriptions_batch(self, items: List[Dict[str, Any]], limit_per_item: int = 200) -> Dict[str, Any]:
        results = []
        hit_count = 0
        gap_count = 0
        for item in _dedupe_gap_items_for_center(items):
            search = self.search_sources([item], limit_per_item=limit_per_item)
            sources = ((search.get('results') or [{}])[0].get('sources') or [])
            if sources:
                hit_count += 1
            else:
                self.report_gaps([item])
                gap_count += 1
            results.append({'query': item, 'sources': sources, 'hit': bool(sources)})
        return {'supported': True, 'items': results, 'hit_count': hit_count, 'gap_count': gap_count}

    def upload_raw_ffprobe(self, sha1: str, raw_ffprobe_json: Dict[str, Any], size: int | None = None, summary_json: Dict[str, Any] | None = None) -> Dict[str, Any]:
        raw = raw_ffprobe_json or {}
        payload = {'sha1': sha1, 'size': size}
        payload.update(_raw_upload_payload_fields(raw))
        if isinstance(summary_json, dict) and summary_json:
            payload['summary_json'] = summary_json

        # 兼容未升级中心端：压缩上传失败时自动回退旧 JSON 直传。
        try:
            return self._post('/api/v1/rawffprobe/upload', payload, timeout=35)
        except RuntimeError:
            if not _raw_upload_used_compression(payload):
                raise
            legacy_payload = {'sha1': sha1, 'size': size}
            legacy_payload.update(_raw_upload_legacy_fields(raw))
            if isinstance(summary_json, dict) and summary_json:
                legacy_payload['summary_json'] = summary_json
            logger.warning('  ➜ [共享资源] RAW 压缩上传失败，已回退 JSON 直传：%s...', str(sha1 or '')[:12])
            return self._post('/api/v1/rawffprobe/upload', legacy_payload, timeout=60)

    def upload_raw_ffprobe_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload_items = []
        legacy_items = []
        raw_bytes_total = 0
        compressed_bytes_total = 0
        compressed_count = 0

        for item in items or []:
            if not isinstance(item, dict):
                continue
            sha1 = str(item.get('sha1') or '').strip().upper()
            raw = item.get('raw_ffprobe_json') or item.get('raw') or {}
            if not sha1 or not isinstance(raw, dict):
                continue

            common = {'sha1': sha1, 'size': item.get('size')}
            entry = dict(common)
            entry.update(_raw_upload_payload_fields(raw))
            legacy_entry = dict(common)
            legacy_entry.update(_raw_upload_legacy_fields(raw))

            summary = item.get('summary_json') or item.get('summary') or {}
            if isinstance(summary, dict) and summary:
                entry['summary_json'] = summary
                legacy_entry['summary_json'] = summary

            if entry.get('raw_zstd_b64'):
                compressed_count += 1
                raw_bytes_total += int(entry.get('raw_bytes') or 0)
                compressed_bytes_total += int(entry.get('compressed_bytes') or 0)

            payload_items.append(entry)
            legacy_items.append(legacy_entry)

        if not payload_items:
            return {'ok': True, 'items': [], 'errors': [], 'count': 0}

        payload = {'items': payload_items}
        timeout = max(60, min(300, 20 + len(payload_items) * 4))
        if compressed_count and raw_bytes_total and compressed_bytes_total:
            ratio = compressed_bytes_total / max(raw_bytes_total, 1)
            logger.info(
                f"  ➜ [共享资源] 媒体信息已压缩，准备批量上传："
                f"{compressed_count}/{len(payload_items)} 个文件，"
                f"{raw_bytes_total / 1024 / 1024:.1f}MB 压缩到 {compressed_bytes_total / 1024 / 1024:.1f}MB，"
                f"压缩后约为原来的 {ratio:.0%}。"
            )

        try:
            return self._post('/api/v1/rawffprobe/upload-batch', payload, timeout=timeout)
        except RuntimeError:
            if not _raw_upload_used_compression(payload):
                raise
            logger.warning('  ➜ [共享资源] RAW 批量压缩上传失败，已回退 JSON 直传。')
            return self._post('/api/v1/rawffprobe/upload-batch', {'items': legacy_items}, timeout=max(timeout, 120))

    def raw_batch(self, sha1_list: List[str]) -> Dict[str, Any]:
        return self._post('/api/v1/rawffprobe/batch', {'sha1_list': list(sha1_list or [])}, timeout=25)

    def get_raw_ffprobe(self, sha1: str) -> Dict[str, Any]:
        return self._get(f"/api/v1/rawffprobe/{urllib.parse.quote(str(sha1 or '').strip())}", timeout=25)

    def register_movie_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/sources/movie/register', payload or {}, timeout=35)

    def register_episode_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/sources/episode/register', payload or {}, timeout=35)

    def disable_source(self, source_kind: str, source_id: str, message: str = '') -> Dict[str, Any]:
        source_kind = str(source_kind or '').strip()
        source_id = str(source_id or '').strip()
        return self._post(
            f"/api/v1/sources/{urllib.parse.quote(source_kind)}/{urllib.parse.quote(source_id)}/disable",
            {'message': message},
            timeout=25,
        )

    def logical_season_manifest(self, group_id: str) -> Dict[str, Any]:
        return self._get(f"/api/v1/logical-seasons/{urllib.parse.quote(str(group_id))}/manifest", timeout=30)

    def dispatch_logical_season_share(self, group_id: str, *, force: bool = False, reason: str = '') -> Dict[str, Any]:
        return self._post(
            f"/api/v1/logical-seasons/{urllib.parse.quote(str(group_id))}/share/dispatch",
            {'force': bool(force), 'reason': reason or ''},
            timeout=20,
        )

    def report_logical_season_share(self, group_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(
            f"/api/v1/logical-seasons/{urllib.parse.quote(str(group_id))}/share/report",
            payload or {},
            timeout=25,
        )

    def update_logical_season_share_status(self, channel_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(
            f"/api/v1/logical-season-share-channels/{urllib.parse.quote(str(channel_id))}/status",
            payload or {},
            timeout=20,
        )

    def get_logical_season_share_channel(self, group_id: str) -> Dict[str, Any]:
        return self._get(
            f"/api/v1/logical-seasons/{urllib.parse.quote(str(group_id))}/share-channel",
            timeout=15,
        )

    def share_sync_heartbeat(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/devices/share-sync/heartbeat', payload or {}, timeout=20)


    def acquire_transfer_lease(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/transfers/lease', payload or {}, timeout=30)

    def report_transfer(self, source_kind: str, source_id: str, result: str, **kwargs) -> Dict[str, Any]:
        payload = {'source_kind': source_kind, 'source_id': source_id, 'result': result}
        payload.update({k: v for k, v in kwargs.items() if v is not None})

        if not payload.get('transfer_mode'):
            msg = str(payload.get('message') or '')
            if str(source_kind or '').strip() in ('logical_season',) and result == 'success' and (
                '分享转存' in msg or '115 分享' in msg or 'share_import' in msg or 'transfer_mode=share' in msg
            ):
                payload['transfer_mode'] = 'share'
                import re
                match = re.search(r'channel=([A-Za-z0-9_:\-.]+)', msg)
                if match and not payload.get('share_channel_id'):
                    payload['share_channel_id'] = match.group(1)
        return self._post('/api/v1/transfers/report', payload, timeout=20)

    def register_rapid_sign_holder(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/rapid-sign/holders/register', payload or {}, timeout=15)

    def create_rapid_sign_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/rapid-sign/jobs', payload or {}, timeout=15)

    def wait_rapid_sign_job(self, job_id: str, timeout: int = 75) -> Dict[str, Any]:
        # 等待时间必须大于中心端 pending/claimed 重新分配阈值，
        # 否则 holder 短暂离线时请求端会先超时返回，中心来不及判失败/扣分/换 holder。
        wait_timeout = max(1, min(int(timeout or 75), 80))
        return self._get(
            f"/api/v1/rapid-sign/jobs/{urllib.parse.quote(str(job_id))}/wait",
            {'timeout': wait_timeout},
            timeout=max(10, wait_timeout + 10),
        )

    def poll_rapid_sign_jobs(self, *, timeout: int = 1, limit: int = 3) -> Dict[str, Any]:
        return self._get('/api/v1/rapid-sign/jobs/poll', {'timeout': max(0, min(int(timeout or 1), 55)), 'limit': max(1, min(int(limit or 3), 20))}, timeout=max(8, int(timeout or 1) + 8))

    def submit_rapid_sign_job(self, job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(f"/api/v1/rapid-sign/jobs/{urllib.parse.quote(str(job_id))}/submit", payload or {}, timeout=15)

    def poll_device_events(self, *, timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
        return self._get('/api/v1/device-events/poll', {'timeout': max(1, min(int(timeout or 25), 55)), 'limit': max(1, min(int(limit or 5), 50))}, timeout=max(10, int(timeout or 25) + 10))

    def ack_device_events(self, event_ids: List[str], result: str = 'ok', message: str = '') -> Dict[str, Any]:
        return self._post('/api/v1/device-events/ack', {'event_ids': event_ids or [], 'result': result or 'ok', 'message': message or ''}, timeout=15)

    # Rapid v2 已移除 115 分享、小黑屋、分享撤销、求共享中心端接口。保留空实现，避免旧调用点炸进程。
    def cancel_sources(self, *args, **kwargs):
        return {'ok': True, 'skipped': True, 'message': 'Rapid v2 无 115 分享源需要撤销'}

    def check_resource_blacklist(self, *args, **kwargs):
        return {'ok': True, 'blacklisted': False, 'items': []}

    def report_resource_blacklist(self, *args, **kwargs):
        return {'ok': True, 'skipped': True, 'message': 'Rapid v2 不使用中心资源黑名单'}

    def quote_share_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/share-requests/quote', payload or {}, timeout=15)

    def create_share_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/share-requests', payload or {}, timeout=20)

    def list_share_requests(self, *, keyword: str = '', status: str = 'open', media_type: str = '',
                            target_type: str = '', limit: int = 100, offset: int = 0, **_ignored) -> Dict[str, Any]:
        return self._get('/api/v1/share-requests', {
            'keyword': keyword or '',
            'status': status or 'open',
            'media_type': media_type or '',
            'target_type': target_type or '',
            'limit': max(1, min(int(limit or 100), 500)),
            'offset': max(0, int(offset or 0)),
        }, timeout=20)

    def co_request_share_request(self, group_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self._post(f"/api/v1/share-requests/{urllib.parse.quote(str(group_id or '').strip())}/co-request", payload or {}, timeout=20)

    def cancel_share_request(self, group_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self._post(f"/api/v1/share-requests/{urllib.parse.quote(str(group_id or '').strip())}/cancel", payload or {}, timeout=20)
    
    def get_raw_ffprobe_batch(self, sha1_list, return_compressed=True):
        """批量拉取中心 RAW。

        v2 默认请求中心直接返回 zstd 压缩对象（base64），客户端本地解压，
        避免中心端把 RAW 全部解压并序列化成巨型 JSON。电影/单集/季包都走
        同一个接口：files=1 时就是单文件批量，files=N 时分片批量。
        """
        sha1s = []
        seen = set()
        for value in sha1_list or []:
            text = str(value or '').strip().upper()
            if len(text) == 40 and all(c in '0123456789ABCDEF' for c in text) and text not in seen:
                seen.add(text)
                sha1s.append(text)
        if not sha1s:
            return {'items': [], 'missing': [], 'encoding': 'zstd_base64'}

        return self._post(
            '/api/v1/rawffprobe/fetch-batch',
            {
                'sha1_list': sha1s,
                'return_compressed': bool(return_compressed),
            },
            timeout=max(60, min(180, 15 + len(sha1s) * 2)),
        )
