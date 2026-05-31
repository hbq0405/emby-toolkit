# handler/shared_center_client.py
# ETK 共享资源中心客户端：缺口登记、共享源查询、raw_ffprobe 批量拉取、转存结果上报。
import logging
from typing import Any, Dict, List, Optional

import requests

import config_manager
import constants
from database import settings_db

logger = logging.getLogger(__name__)


def _app_version() -> str:
    return str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0').strip() or '0.0.0'


def _client_user_agent() -> str:
    return f"ETK/{_app_version()}"


def _raise_for_center_error(resp):
    if resp.ok:
        return
    if resp.status_code == 426:
        try:
            body = resp.json()
        except Exception:
            body = {}
        min_version = body.get('min_client_version') if isinstance(body, dict) else ''
        client_version = body.get('client_version') if isinstance(body, dict) else ''
        message = body.get('message') if isinstance(body, dict) else ''
        raise RuntimeError(
            message or f"共享中心拒绝服务：当前客户端版本 {client_version or _app_version()} 低于中心要求 {min_version or '未知'}，请升级 ETK 后再使用共享资源。"
        )
    raise RuntimeError(f"共享中心请求失败: {resp.status_code} {resp.text[:200]}")


def _request_kwargs(timeout: int) -> Dict[str, Any]:
    """共享中心 HTTP 请求参数。

    复用全局 Network 代理配置，只影响共享中心相关 requests。
    未开启代理时不传 proxies，保持原行为。
    """
    kwargs = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    return kwargs


def _shared_cfg() -> Dict[str, Any]:
    return settings_db.get_shared_resource_config()


def shared_center_enabled() -> bool:
    return bool(_shared_cfg().get('p115_shared_resource_enabled'))


def shared_resource_mode() -> str:
    # 虚拟入库已移除，共享池消费统一走永久转存。
    return 'permanent'


def _safe_int_or_none(value):
    try:
        if value in (None, ''):
            return None
        return int(float(value))
    except Exception:
        return None


def _normalize_gap_item_for_center(item: Dict[str, Any]) -> Dict[str, Any]:
    """归一化中心缺口粒度。

    普通共享池只登记 Movie / Season / Series 缺口；Episode 只作为客户端本地
    缺失明细存在。这样长篇动漫不会因为几百上千集把中心 wanted_gaps 撑爆。
    """
    item = dict(item or {})
    item_type = str(item.get('item_type') or '').strip()
    season = _safe_int_or_none(item.get('season_number'))
    episode = _safe_int_or_none(item.get('episode_number'))
    if item_type.lower() in ('episode', 'episode_file', 'single') and season is not None:
        item['item_type'] = 'Season'
        item['season_number'] = season
        item['episode_number'] = None
    elif item_type.lower() in ('season', 'season_pack', 'tv_pack'):
        item['item_type'] = 'Season'
        item['season_number'] = season
        item['episode_number'] = None
    elif item_type.lower() in ('movie', 'movie_file', 'movie_folder'):
        item['item_type'] = 'Movie'
        item['episode_number'] = None
    elif item_type.lower() in ('series', 'show', 'tv'):
        item['item_type'] = 'Series'
        item['episode_number'] = None
    return item


def _dedupe_gap_items_for_center(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        item = _normalize_gap_item_for_center(raw)
        if not item.get('tmdb_id') or not item.get('item_type'):
            continue
        key = (
            str(item.get('tmdb_id') or ''),
            str(item.get('item_type') or ''),
            _safe_int_or_none(item.get('season_number')),
            _safe_int_or_none(item.get('episode_number')),
        )
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

    def _post(self, path: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self._headers(), json=payload, **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}

    def _get(self, path: str, timeout: int = 15) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), **_request_kwargs(timeout))
        _raise_for_center_error(resp)
        return resp.json() if resp.text else {}


    def register_device(self, name: str = '', install_id: str = '', admin_token: str = '') -> Dict[str, Any]:
        """向共享中心注册本机设备，返回 device_id / device_token。

        首选公开自助注册接口 /api/v1/devices/register。
        如果中心尚未升级且传入 admin_token，则回退到旧的管理员注册接口。
        注意：该方法不依赖现有 device_token，专门用于首次生成共享中心 device_token。
        """
        if not self.base_url:
            raise RuntimeError('共享中心地址未配置')
        payload = {
            'name': str(name or '').strip() or 'ETK Device',
            'install_id': str(install_id or '').strip(),
        }
        headers = {
            'X-Client-Version': _app_version(),
            'X-ETK-Version': _app_version(),
            'Content-Type': 'application/json',
            'User-Agent': _client_user_agent(),
        }
        url = f"{self.base_url}/api/v1/devices/register"
        resp = requests.post(url, headers=headers, json=payload, **_request_kwargs(20))
        if resp.status_code == 404 and admin_token:
            # 兼容未升级的私有中心：使用管理员接口注册，但这种方式无法按 install_id 幂等。
            admin_url = f"{self.base_url}/api/v1/admin/devices/register"
            admin_headers = dict(headers)
            admin_headers['X-Admin-Token'] = str(admin_token)
            resp = requests.post(
                admin_url,
                headers=admin_headers,
                json={'name': payload['name']},
                **_request_kwargs(20),
            )
        if not resp.ok:
            if resp.status_code == 426:
                _raise_for_center_error(resp)
            raise RuntimeError(f"共享中心设备注册失败: {resp.status_code} {resp.text[:300]}")
        return resp.json() if resp.text else {}

    def report_gaps(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        items = _dedupe_gap_items_for_center(items)
        if not items:
            return {'count': 0, 'items': []}
        return self._post('/api/v1/gaps/batch', {'items': items}, timeout=20)

    def search_sources(self, items: List[Dict[str, Any]], limit_per_item: int = 20) -> Dict[str, Any]:
        if not items:
            return {'results': []}
        return self._post('/api/v1/sources/search', {'items': items, 'limit_per_item': limit_per_item}, timeout=25)

    def probe_share_needed(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """询问中心端本机刚入库的资源是否需要创建共享。

        新中心会实现 /api/v1/share/probe-needed；旧中心返回 404 时，
        调用方可回退到 open gaps + sources/search 的本地判断。
        """
        try:
            return self._post('/api/v1/share/probe-needed', {'item': item or {}}, timeout=20)
        except RuntimeError as e:
            text = str(e)
            if '404' in text or 'Not Found' in text:
                return {'supported': False, 'need_share': False, 'message': 'center_probe_endpoint_not_supported'}
            raise



    def list_share_requests(self, *, status: str = 'open', keyword: str = '', media_type: str = '', target_type: str = '', limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """拉取共享中心求分享列表。维护任务用于自动响应别人发布的求分享。"""
        import urllib.parse
        params = {
            'status': status or 'open',
            'keyword': keyword or '',
            'media_type': media_type or '',
            'target_type': target_type or '',
            'limit': max(1, min(int(limit or 100), 200)),
            'offset': max(0, int(offset or 0)),
        }
        return self._get(f"/api/v1/share-requests?{urllib.parse.urlencode(params)}", timeout=25)

    def poll_device_events(self, *, timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
        """长轮询领取中心按 device_id 下发的通用事件。"""
        import urllib.parse
        timeout = max(1, min(int(timeout or 25), 55))
        limit = max(1, min(int(limit or 5), 20))
        query = urllib.parse.urlencode({'timeout': timeout, 'limit': limit})
        try:
            resp = self._get(f'/api/v1/device-events/poll?{query}', timeout=timeout + 10)
            resp['supported'] = True
            return resp
        except RuntimeError as e:
            text = str(e)
            if '404' in text or 'Not Found' in text:
                return {'supported': False, 'items': [], 'message': 'center_device_events_not_supported'}
            raise

    def ack_device_event(self, event_id: str, result: str = 'success', message: str = '') -> Dict[str, Any]:
        event_id = str(event_id or '').strip()
        if not event_id:
            return {'ok': False, 'message': 'missing event_id'}
        return self._post(
            f'/api/v1/device-events/{event_id}/ack',
            {'result': result or 'success', 'message': message or ''},
            timeout=15,
        )

    def poll_share_request_events(self, *, timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
        """兼容旧中心：长轮询领取求分享命中事件。新中心请使用 poll_device_events。"""
        import urllib.parse
        timeout = max(1, min(int(timeout or 25), 55))
        limit = max(1, min(int(limit or 5), 20))
        query = urllib.parse.urlencode({'timeout': timeout, 'limit': limit})
        return self._get(f'/api/v1/share-requests/events/poll?{query}', timeout=timeout + 10)

    def ack_share_request_event(self, event_id: str, result: str = 'success', message: str = '') -> Dict[str, Any]:
        event_id = str(event_id or '').strip()
        if not event_id:
            return {'ok': False, 'message': 'missing event_id'}
        return self._post(
            f'/api/v1/share-requests/events/{event_id}/ack',
            {'result': result or 'success', 'message': message or ''},
            timeout=15,
        )

    def list_open_gaps(self, limit: int = 100) -> Dict[str, Any]:
        limit = max(1, min(int(limit or 100), 500))
        return self._get(f'/api/v1/gaps/open?limit={limit}', timeout=20)

    def list_sources(self, *, q: str = '', tmdb_id: str = '', item_type: str = '', status: str = 'alive,pending',
                     source_ids: List[str] = None, mine_only: bool = False,
                     order_by: str = 'latest', limit: int = 100, offset: int = 0, include_raw: bool = True) -> Dict[str, Any]:
        """列出中心已有共享源。用于前端展示版本列表，也用于按 source_id 手动入库。"""
        import urllib.parse
        source_ids = [str(x).strip() for x in (source_ids or []) if str(x or '').strip()]
        params = {
            'q': q or '',
            'tmdb_id': tmdb_id or '',
            'item_type': item_type or '',
            'status': status or '',
            'mine_only': '1' if mine_only else '0',
            'order_by': order_by or 'latest',
            'limit': max(1, min(int(limit or 100), 500)),
            'offset': max(0, int(offset or 0)),
            'include_raw': '1' if include_raw else '0',
        }
        if source_ids:
            params['source_ids'] = ','.join(source_ids)
        query = urllib.parse.urlencode(params)
        return self._get(f'/api/v1/sources/list?{query}', timeout=60 if include_raw else 25)
    def fetch_raw_ffprobe_batch(self, sha1_list: List[str]) -> Dict[str, Any]:
        sha1_list = [str(x or '').strip().upper() for x in sha1_list if x]
        if not sha1_list:
            return {'items': []}
        return self._post('/api/v1/rawffprobe/batch', {'sha1_list': sha1_list}, timeout=60)



    def upload_raw_ffprobe(self, sha1: str, raw_ffprobe_json: Dict[str, Any], size=None, summary_json: Dict[str, Any] = None) -> Dict[str, Any]:
        """上传 raw_ffprobe_json 到共享中心，供其他设备复用媒体信息。

        完整 RAW 仍然上传保存；summary_json 只给中心资源库列表页减小传输体积。
        """
        sha1 = str(sha1 or '').strip().upper()
        if not sha1 or not raw_ffprobe_json:
            return {'ok': False, 'message': '缺少 sha1 或 raw_ffprobe_json'}
        payload = {
            'sha1': sha1,
            'size': size,
            'raw_ffprobe_json': raw_ffprobe_json,
            'summary_json': summary_json or None,
        }
        return self._post('/api/v1/rawffprobe/upload', payload, timeout=60)

    def upload_raw_ffprobe_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量上传 raw_ffprobe_json。用于季包；失败项由调用方单条重传。"""
        payload_items = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            sha1 = str(item.get('sha1') or '').strip().upper()
            raw = item.get('raw_ffprobe_json')
            if not sha1 or not raw:
                continue
            payload_items.append({
                'sha1': sha1,
                'size': item.get('size'),
                'raw_ffprobe_json': raw,
                'summary_json': item.get('summary_json') or None,
            })
        if not payload_items:
            return {'ok': True, 'ok_count': 0, 'fail_count': 0, 'items': []}
        return self._post('/api/v1/rawffprobe/upload-batch', {'items': payload_items}, timeout=120)

    def register_source(self, *, tmdb_id, item_type, sha1, file_name, share_code,
                        receive_code='', season_number=None, episode_number=None,
                        title='', release_year=None, size=None, quality='',
                        has_raw_ffprobe=True, source_provider='user_share',
                        share_request_group_id: str = '') -> Dict[str, Any]:
        """登记一个可被共享中心消费的 115 分享源。

        中心端按“当前设备 + SHA1”幂等计分：首次登记 +1，重复登记只更新分享码/元数据。
        """
        payload = {
            'tmdb_id': str(tmdb_id or ''),
            'item_type': str(item_type or ''),
            'season_number': season_number,
            'episode_number': episode_number,
            'title': title or None,
            'release_year': release_year,
            'sha1': str(sha1 or '').strip().upper(),
            'size': size,
            'file_name': file_name or '',
            'quality': quality or '',
            'source_provider': str(source_provider or 'user_share').strip(),
            'share_code': str(share_code or '').strip(),
            'receive_code': str(receive_code or '').strip() or None,
            'has_raw_ffprobe': bool(has_raw_ffprobe),
        }
        if str(share_request_group_id or '').strip():
            payload['share_request_group_id'] = str(share_request_group_id).strip()
        return self._post('/api/v1/sources/register', payload, timeout=25)

    def register_sources_batch(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """批量登记共享源。中心端任一条失败会整包回滚。"""
        payload_items = [dict(x) for x in (items or []) if isinstance(x, dict)]
        if not payload_items:
            return {'ok': True, 'ok_count': 0, 'fail_count': 0, 'items': []}
        return self._post('/api/v1/sources/register-batch', {'items': payload_items}, timeout=90)

    def cancel_sources(self, share_code: str = '', source_ids: List[str] = None, sha1_list: List[str] = None, reason: str = 'share_cancelled', delete_raw_ffprobe: bool = True) -> Dict[str, Any]:
        """从共享中心撤销当前设备登记的共享源，并同步删除对应媒体信息。

        中心端按当前 device_token 校验归属，只会删除本设备贡献的 source。
        share_code 可一次撤销同一个 115 分享包下登记的所有源；source_ids 用于精确兜底；
        sha1_list 用于清理“源已删但 raw_ffprobe 仍残留”的历史媒体信息。
        """
        source_ids = [str(x).strip() for x in (source_ids or []) if str(x or '').strip()]
        sha1_list = [str(x).strip().upper() for x in (sha1_list or []) if str(x or '').strip()]
        payload = {
            'share_code': str(share_code or '').strip() or None,
            'source_ids': source_ids,
            'sha1_list': sha1_list,
            'delete_raw_ffprobe': bool(delete_raw_ffprobe),
            'reason': reason or 'share_cancelled',
        }
        return self._post('/api/v1/sources/cancel', payload, timeout=25)

    def report_transfer(self, source_id: str, result: str, expected_sha1: str = '', actual_sha1: str = '', expected_size=None, actual_size=None, message: str = ''):
        if not source_id:
            return None
        payload = {
            'source_id': source_id,
            'result': result,
            'expected_sha1': expected_sha1 or None,
            'actual_sha1': actual_sha1 or None,
            'expected_size': expected_size,
            'actual_size': actual_size,
            'message': message or None,
        }
        return self._post('/api/v1/transfers/report', payload, timeout=20)
