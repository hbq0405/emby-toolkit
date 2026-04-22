# handler/hdhive_client.py
import time
import requests
import logging
import re
from collections import deque
import config_manager

logger = logging.getLogger(__name__)

class HDHiveClient:
    BASE_URL = "https://hdhive.com/api/open"

    def __init__(self, api_key):
        self.api_key = api_key.strip() if api_key else ""
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }
        self.proxies = config_manager.get_proxies_for_requests()
        # 用于记录最近3次解锁请求的时间戳，以应对 3次/分钟 的限制
        self.unlock_timestamps = deque(maxlen=3)

    def _handle_error(self, e, context="请求"):
        """统一处理 HTTP 错误，说人话"""
        if isinstance(e, requests.exceptions.HTTPError):
            status = e.response.status_code
            if status == 401:
                logger.error(f"  ➜ 影巢 {context} 失败: API Key 无效或未关联用户 (401)！请检查 Key 是否正确。")
            elif status == 403:
                logger.error(f"  ➜ 影巢 {context} 失败: 权限不足 (403)！该接口可能需要 Premium 会员。")
            elif status == 404:
                logger.error(f"  ➜ 影巢 {context} 失败: 找不到资源 (404)。")
            elif status == 429:
                logger.error(f"  ➜ 影巢 {context} 失败: 请求过于频繁 (429)！触发了接口频率限制。")
            else:
                logger.error(f"  ➜ 影巢 {context} 失败: HTTP {status} 错误。")
        else:
            logger.error(f"  ➜ 影巢 {context} 失败 (网络或代理解析异常): {e}")

    def _check_unlock_rate_limit(self):
        """检查并处理解锁频率限制：1分钟最多3次"""
        if len(self.unlock_timestamps) == 3:
            elapsed = time.time() - self.unlock_timestamps[0]
            if elapsed < 60.0:
                wait_time = 60.0 - elapsed + 1.0  # 补齐60秒并加1秒缓冲
                logger.info(f"  ➜ 触发影巢解锁频率限制 (3次/分钟)，主动等待 {wait_time:.1f} 秒...")
                time.sleep(wait_time)
        
        # 记录本次请求的时间戳
        self.unlock_timestamps.append(time.time())

    def ping(self):
        """测试 API Key 是否有效"""
        try:
            res = requests.get(f"{self.BASE_URL}/ping", headers=self.headers, proxies=self.proxies, timeout=10)
            res.raise_for_status()
            return res.json().get("success") is True
        except Exception as e:
            self._handle_error(e, "Ping 测试")
            return False

    def get_user_info(self):
        """获取当前用户信息"""
        try:
            res = requests.get(f"{self.BASE_URL}/me", headers=self.headers, proxies=self.proxies, timeout=10)
            
            # 记录异常状态码，方便排错
            if res.status_code != 200:
                logger.warning(f"  ➜ 影巢获取用户信息异常: HTTP {res.status_code} - {res.text}")
                
            if res.status_code == 403:
                return {"nickname": "普通用户", "user_meta": {"points": "未知 (需Premium)"}}
                
            res.raise_for_status()
            data = res.json()
            
            if data.get("success"):
                return data.get("data")
            else:
                logger.error(f"  ➜ 影巢获取用户信息失败: {data.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"  ➜ 影巢获取用户信息发生异常: {e}")
            return None

    def get_quota(self):
        """获取每日 API 配额"""
        try:
            res = requests.get(f"{self.BASE_URL}/quota", headers=self.headers, proxies=self.proxies, timeout=10)
            res.raise_for_status()
            data = res.json()
            return data.get("data") if data.get("success") else None
        except Exception as e:
            self._handle_error(e, "获取配额")
            return None

    def get_resources(self, tmdb_id, media_type, target_season=None):
        """根据 TMDB ID 获取资源列表"""
        try:
            res = requests.get(f"{self.BASE_URL}/resources/{media_type}/{tmdb_id}", headers=self.headers, proxies=self.proxies, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            if not data.get("success"):
                return []
                
            resources = [r for r in data.get("data", []) if r.get("pan_type") == "115" or r.get("pan_type") is None]
            if media_type == 'movie' or target_season is None:
                return resources
                
            filtered_resources = []
            target_s_str = f"S{int(target_season):02d}" 
            target_s_num = str(int(target_season))      
            zh_num_map = {1:"一", 2:"二", 3:"三", 4:"四", 5:"五", 6:"六", 7:"七", 8:"八", 9:"九", 10:"十"}
            zh_season = f"第{zh_num_map.get(int(target_season), target_s_num)}季"
            
            for r in resources:
                title = r.get("title", "").upper()
                remark = r.get("remark", "").upper()
                combined_text = f"{title} {remark}"
                
                is_match = False
                if target_s_str in combined_text or f"S{target_s_num}" in combined_text or zh_season in combined_text:
                    is_match = True
                else:
                    range_match = re.search(r'S(\d{1,2})\s*-\s*S?(\d{1,2})', combined_text)
                    if range_match and int(range_match.group(1)) <= int(target_season) <= int(range_match.group(2)):
                        is_match = True
                            
                if is_match:
                    filtered_resources.append(r)
            return filtered_resources
            
        except Exception as e:
            self._handle_error(e, "获取资源列表")
            return []

    def unlock_resource(self, slug, max_retries=3, timeout=15):
        """解锁资源（带网络异常重试及频率限制）"""
        payload = {"slug": slug}
        url = f"{self.BASE_URL}/resources/unlock"

        # 只对这些“偶发网络类异常”重试
        retryable_exceptions = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
        )

        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                # 发起请求前，检查并遵守 3次/分钟 的频率限制
                self._check_unlock_rate_limit()

                res = requests.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    proxies=self.proxies,
                    timeout=timeout
                )

                # 5xx 可视为服务端临时异常，允许重试
                if 500 <= res.status_code < 600:
                    raise requests.exceptions.HTTPError(response=res)

                # 4xx 直接按业务/鉴权失败处理，不重试 (429 除外，在下面单独捕获)
                res.raise_for_status()

                data = res.json()
                if data.get("success"):
                    return data.get("data")

                # 接口正常返回，但业务失败：不重试
                logger.error(f"  ➜ 影巢解锁失败: {data.get('message')}")
                return None

            except retryable_exceptions as e:
                last_exception = e
                if attempt < max_retries:
                    wait_seconds = 2 ** (attempt - 1)  # 1s, 2s, 4s
                    logger.warning(
                        f"  ➜ 影巢解锁请求异常，第 {attempt}/{max_retries} 次失败: {e}，"
                        f"{wait_seconds}s 后重试..."
                    )
                    time.sleep(wait_seconds)
                    continue

                self._handle_error(e, "解锁资源")
                return None

            except requests.exceptions.HTTPError as e:
                last_exception = e
                status = e.response.status_code if e.response is not None else None

                # 针对 429 (Too Many Requests) 的特殊重试处理
                if status == 429 and attempt < max_retries:
                    wait_seconds = 60  # 被服务端限流时，直接等待1分钟
                    logger.warning(
                        f"  ➜ 影巢解锁触发 429 限制，第 {attempt}/{max_retries} 次失败，"
                        f"强制等待 {wait_seconds}s 后重试..."
                    )
                    time.sleep(wait_seconds)
                    continue

                # 仅 5xx 重试，4xx 不重试
                if status and 500 <= status < 600 and attempt < max_retries:
                    wait_seconds = 2 ** (attempt - 1)
                    logger.warning(
                        f"  ➜ 影巢解锁请求异常: HTTP {status}，"
                        f"第 {attempt}/{max_retries} 次失败，{wait_seconds}s 后重试..."
                    )
                    time.sleep(wait_seconds)
                    continue

                self._handle_error(e, "解锁资源")
                return None

            except Exception as e:
                # 未知异常不重试，避免掩盖真实 bug
                self._handle_error(e, "解锁资源")
                return None

        if last_exception:
            self._handle_error(last_exception, "解锁资源")
        return None

    def checkin(self, is_gambler=False):
        """每日签到"""
        try:
            payload = {"is_gambler": is_gambler}
            res = requests.post(f"{self.BASE_URL}/checkin", headers=self.headers, json=payload, proxies=self.proxies, timeout=10)
            return res.json()
        except Exception as e:
            self._handle_error(e, "签到")
            return {"success": False, "message": "网络或鉴权异常，请查看日志"}