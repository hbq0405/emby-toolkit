# handler/shared_center_client.py
# ETK 共享资源中心客户端（Rapid v2）：中心只保存资源索引/manifest，不保存 CK、不创建 115 分享。
import logging
import urllib.parse
from typing import Any, Dict, List

import requests

import config_manager
import constants
from database import settings_db

logger = logging.getLogger(__name__)


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
        resp = requests.post(url, headers=self._headers(), json=payload or {}, **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def _get(self, path: str, params: Dict[str, Any] | None = None, timeout: int = 15) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), params=params or {}, **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def register_device(self, name: str = '', install_id: str = '', admin_token: str = '') -> Dict[str, Any]:
        if not self.base_url:
            raise RuntimeError('共享中心地址未配置')
        payload = {'name': str(name or '').strip() or 'ETK Device', 'install_id': str(install_id or '').strip()}
        headers = {'X-Client-Version': _app_version(), 'X-ETK-Version': _app_version(), 'Content-Type': 'application/json', 'User-Agent': _client_user_agent()}
        resp = requests.post(f"{self.base_url}/api/v1/devices/register", headers=headers, json=payload, **_request_kwargs(20))
        if resp.status_code == 404 and admin_token:
            admin_headers = dict(headers)
            admin_headers['X-Admin-Token'] = str(admin_token)
            resp = requests.post(f"{self.base_url}/api/v1/admin/devices/register", headers=admin_headers, json={'name': payload['name']}, **_request_kwargs(20))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def me(self) -> Dict[str, Any]:
        return self._get('/api/v1/me', timeout=12)

    def stats(self) -> Dict[str, Any]:
        return self._get('/api/v1/stats', timeout=12)

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
                     source_kind: str = '', item_type: str = '', tmdb_id: str = '', limit: int = 200, offset: int = 0,
                     **_ignored) -> Dict[str, Any]:
        return self._get('/api/v1/sources/list', {
            'q': q or '',
            'status': status or 'alive,available,updating,inconsistent',
            'mine_only': 1 if mine_only else 0,
            'source_kind': source_kind or '',
            'item_type': item_type or '',
            'tmdb_id': tmdb_id or '',
            'limit': max(1, min(int(limit or 200), 1000)),
            'offset': max(0, int(offset or 0)),
        }, timeout=25)

    def list_display_sources(self, *, q: str = '', status: str = 'alive,available,updating,inconsistent,incomplete',
                             item_type: str = '', tmdb_id: str = '', order_by: str = 'latest',
                             limit: int = 200, offset: int = 0, **_ignored) -> Dict[str, Any]:
        """中心资源库展示口径：由中心端分页、筛选、聚合。

        默认只返回电影和季容器；连载季返回公共 season_hub，单集只作为 children/pack_items。
        """
        return self._get('/api/v1/sources/display-list', {
            'q': q or '',
            'status': status or 'alive,available,updating,inconsistent,incomplete',
            'item_type': item_type or '',
            'tmdb_id': tmdb_id or '',
            'order_by': order_by or 'latest',
            'limit': max(1, min(int(limit or 200), 1000)),
            'offset': max(0, int(offset or 0)),
        }, timeout=30)

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
        payload = {'sha1': sha1, 'size': size, 'raw_ffprobe_json': raw_ffprobe_json or {}}
        if isinstance(summary_json, dict) and summary_json:
            payload['summary_json'] = summary_json
        return self._post('/api/v1/rawffprobe/upload', payload, timeout=35)

    def upload_raw_ffprobe_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload_items = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            sha1 = str(item.get('sha1') or '').strip().upper()
            raw = item.get('raw_ffprobe_json') or item.get('raw') or {}
            if not sha1 or not isinstance(raw, dict):
                continue
            entry = {'sha1': sha1, 'size': item.get('size'), 'raw_ffprobe_json': raw}
            summary = item.get('summary_json') or item.get('summary') or {}
            if isinstance(summary, dict) and summary:
                entry['summary_json'] = summary
            payload_items.append(entry)
        if not payload_items:
            return {'ok': True, 'items': [], 'errors': [], 'count': 0}
        return self._post('/api/v1/rawffprobe/upload-batch', {'items': payload_items}, timeout=max(60, min(300, 20 + len(payload_items) * 4)))

    def raw_batch(self, sha1_list: List[str]) -> Dict[str, Any]:
        return self._post('/api/v1/rawffprobe/batch', {'sha1_list': list(sha1_list or [])}, timeout=25)

    def get_raw_ffprobe(self, sha1: str) -> Dict[str, Any]:
        return self._get(f"/api/v1/rawffprobe/{urllib.parse.quote(str(sha1 or '').strip())}", timeout=25)

    def register_movie_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/sources/movie/register', payload or {}, timeout=35)

    def register_episode_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/sources/episode/register', payload or {}, timeout=35)

    def register_completed_season_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/sources/completed-season/register', payload or {}, timeout=60)

    def update_completed_season_status(self, source_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(f"/api/v1/sources/completed-season/{urllib.parse.quote(str(source_id))}/status", payload or {}, timeout=25)

    def disable_source(self, source_kind: str, source_id: str, message: str = '') -> Dict[str, Any]:
        source_kind = str(source_kind or '').strip()
        source_id = str(source_id or '').strip()
        return self._post(f"/api/v1/sources/{urllib.parse.quote(source_kind)}/{urllib.parse.quote(source_id)}/disable", {'message': message}, timeout=25)

    def completed_season_manifest(self, source_id: str) -> Dict[str, Any]:
        return self._get(f"/api/v1/sources/completed-season/{urllib.parse.quote(str(source_id))}/manifest", timeout=30)

    def report_transfer(self, source_kind: str, source_id: str, result: str, **kwargs) -> Dict[str, Any]:
        payload = {'source_kind': source_kind, 'source_id': source_id, 'result': result}
        payload.update({k: v for k, v in kwargs.items() if v is not None})
        return self._post('/api/v1/transfers/report', payload, timeout=20)

    def register_rapid_sign_holder(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/rapid-sign/holders/register', payload or {}, timeout=15)

    def create_rapid_sign_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post('/api/v1/rapid-sign/jobs', payload or {}, timeout=15)

    def wait_rapid_sign_job(self, job_id: str, timeout: int = 45) -> Dict[str, Any]:
        return self._get(f"/api/v1/rapid-sign/jobs/{urllib.parse.quote(str(job_id))}/wait", {'timeout': max(1, min(int(timeout or 45), 55))}, timeout=max(10, int(timeout or 45) + 10))

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
