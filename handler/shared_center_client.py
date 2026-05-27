# handler/shared_center_client.py
# ETK 共享资源中心客户端：缺口登记、共享源查询、raw_ffprobe 批量拉取、转存结果上报。
import logging
from typing import Any, Dict, List, Optional

import requests

import config_manager
import constants

logger = logging.getLogger(__name__)

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


def _cfg_const(name: str, fallback: str, default=None):
    key = getattr(constants, name, fallback)
    return (config_manager.APP_CONFIG or {}).get(key, default)


def shared_center_enabled() -> bool:
    value = _cfg_const('CONFIG_OPTION_115_SHARED_RESOURCE_ENABLED', 'p115_shared_resource_enabled', False)
    if isinstance(value, str):
        value = value.strip().lower() in ('1', 'true', 'yes', 'on', '启用')
    return bool(value)


def shared_resource_mode() -> str:
    mode = str(_cfg_const('CONFIG_OPTION_115_SHARED_RESOURCE_MODE', 'p115_shared_resource_mode', 'permanent') or 'permanent').strip().lower()
    return 'virtual' if mode == 'virtual' else 'permanent'


class SharedCenterClient:
    def __init__(self):
        self.base_url = str(_cfg_const('CONFIG_OPTION_115_SHARED_CENTER_URL', 'p115_shared_center_url', 'https://shared.55565576.xyz') or '').rstrip('/')
        self.device_token = str(_cfg_const('CONFIG_OPTION_115_SHARED_DEVICE_TOKEN', 'p115_shared_device_token', '') or '').strip()

    @property
    def ready(self) -> bool:
        return bool(self.base_url and self.device_token)

    def _headers(self) -> Dict[str, str]:
        return {
            'X-Device-Token': self.device_token,
            'Content-Type': 'application/json',
        }

    def _post(self, path: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = requests.post(url, headers=self._headers(), json=payload, **_request_kwargs(timeout))
        if not resp.ok:
            raise RuntimeError(f"共享中心请求失败: {resp.status_code} {resp.text[:200]}")
        return resp.json() if resp.text else {}

    def _get(self, path: str, timeout: int = 15) -> Dict[str, Any]:
        if not self.ready:
            raise RuntimeError('共享中心地址或 device_token 未配置')
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self._headers(), **_request_kwargs(timeout))
        if not resp.ok:
            raise RuntimeError(f"共享中心请求失败: {resp.status_code} {resp.text[:200]}")
        return resp.json() if resp.text else {}


    def register_device(self, name: str = '', install_id: str = '', admin_token: str = '') -> Dict[str, Any]:
        """向共享中心注册本机设备，返回 device_id / device_token。

        首选公开自助注册接口 /api/v1/devices/register。
        如果中心尚未升级且传入 admin_token，则回退到旧的管理员注册接口。
        注意：该方法不依赖现有 device_token，专门用于首次生成 p115_shared_device_token。
        """
        if not self.base_url:
            raise RuntimeError('共享中心地址未配置')
        payload = {
            'name': str(name or '').strip() or 'ETK Device',
            'install_id': str(install_id or '').strip(),
        }
        url = f"{self.base_url}/api/v1/devices/register"
        resp = requests.post(url, json=payload, **_request_kwargs(20))
        if resp.status_code == 404 and admin_token:
            # 兼容未升级的私有中心：使用管理员接口注册，但这种方式无法按 install_id 幂等。
            admin_url = f"{self.base_url}/api/v1/admin/devices/register"
            resp = requests.post(
                admin_url,
                headers={'X-Admin-Token': str(admin_token)},
                json={'name': payload['name']},
                **_request_kwargs(20),
            )
        if not resp.ok:
            raise RuntimeError(f"共享中心设备注册失败: {resp.status_code} {resp.text[:300]}")
        return resp.json() if resp.text else {}

    def report_gaps(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not items:
            return {'count': 0, 'items': []}
        return self._post('/api/v1/gaps/batch', {'items': items}, timeout=20)

    def search_sources(self, items: List[Dict[str, Any]], limit_per_item: int = 20) -> Dict[str, Any]:
        if not items:
            return {'results': []}
        return self._post('/api/v1/sources/search', {'items': items, 'limit_per_item': limit_per_item}, timeout=25)



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



    def upload_raw_ffprobe(self, sha1: str, raw_ffprobe_json: Dict[str, Any], size=None) -> Dict[str, Any]:
        """上传 raw_ffprobe_json 到共享中心，供其他设备复用媒体信息。"""
        sha1 = str(sha1 or '').strip().upper()
        if not sha1 or not raw_ffprobe_json:
            return {'ok': False, 'message': '缺少 sha1 或 raw_ffprobe_json'}
        payload = {
            'sha1': sha1,
            'size': size,
            'raw_ffprobe_json': raw_ffprobe_json,
        }
        return self._post('/api/v1/rawffprobe/upload', payload, timeout=60)

    def register_source(self, *, tmdb_id, item_type, sha1, file_name, share_code,
                        receive_code='', season_number=None, episode_number=None,
                        title='', release_year=None, size=None, quality='',
                        has_raw_ffprobe=True, source_provider='user_share') -> Dict[str, Any]:
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
        return self._post('/api/v1/sources/register', payload, timeout=25)

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
