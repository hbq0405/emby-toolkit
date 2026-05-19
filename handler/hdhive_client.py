# handler/hdhive_client.py
import time
import requests
import logging
import threading
from collections import deque
from urllib.parse import quote

import config_manager
from database import settings_db

logger = logging.getLogger(__name__)


class HDHiveClient:
    """
    影巢客户端。

    支持两种模式：
    1. relay 模式（推荐）：本机 EmbyToolKit -> VPS 授权中转 -> HDHive
       配置项：
         hdhive_config.relay_base_url = https://hdhive.847977.xyz
         hdhive_config.relay_secret   = VPS 中转 ETK_SHARED_SECRET

    2. direct 模式（兼容旧版）：本机 EmbyToolKit -> HDHive
       配置项：
         hdhive_config.api_key = 个人 API Key / 绑定用户的应用 Key
    """
    BASE_URL = "https://hdhive.com/api/open"

    _unlock_timestamps = deque()
    _rate_limit_lock = threading.Lock()

    def __init__(self, api_key=None):
        hdhive_config = settings_db.get_setting("hdhive_config") or {}
        if not isinstance(hdhive_config, dict):
            hdhive_config = {}

        # 兼容旧调用：外部仍可能传 api_key
        self.api_key = (api_key or hdhive_config.get("api_key") or "").strip()

        self.relay_base_url = (
            hdhive_config.get("relay_base_url")
            or hdhive_config.get("auth_relay_url")
            or hdhive_config.get("proxy_base_url")
            or ""
        ).strip().rstrip("/")

        self.relay_secret = (
            hdhive_config.get("relay_secret")
            or hdhive_config.get("auth_relay_secret")
            or hdhive_config.get("proxy_secret")
            or ""
        ).strip()

        self.use_relay = bool(self.relay_base_url and self.relay_secret)

        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.relay_headers = {
            "X-ETK-Secret": self.relay_secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.proxies = config_manager.get_proxies_for_requests()

        unlock_limit = hdhive_config.get("unlock_limit") or {}
        self.limit_count = int(unlock_limit.get("count", 3))
        self.limit_window = int(unlock_limit.get("window", 60))

    def is_configured(self):
        return self.use_relay or bool(self.api_key)

    def mode(self):
        return "relay" if self.use_relay else "direct"

    def authorize_url(self):
        if not self.relay_base_url:
            return ""
        return f"{self.relay_base_url}/hdhive/authorize"

    # ----------------------------
    # 通用 HTTP / 错误处理
    # ----------------------------
    def _safe_json(self, res):
        try:
            return res.json()
        except Exception:
            return {
                "success": False,
                "code": str(getattr(res, "status_code", "")),
                "message": getattr(res, "text", "")[:500],
            }

    def _get_retry_after(self, res, default=60):
        data = self._safe_json(res)
        value = (
            res.headers.get("Retry-After")
            or data.get("retry_after_seconds")
            or default
        )
        try:
            return int(float(value))
        except Exception:
            return int(default)

    def _log_response_error(self, res, context="请求"):
        data = self._safe_json(res)
        logger.error(
            "  ➜ 影巢%s失败: mode=%s HTTP=%s code=%s message=%s description=%s "
            "limit_scope=%s retry_after=%s",
            context,
            self.mode(),
            getattr(res, "status_code", None),
            data.get("code"),
            data.get("message"),
            data.get("description"),
            data.get("limit_scope"),
            data.get("retry_after_seconds") or res.headers.get("Retry-After"),
        )
        return data

    def _handle_error(self, e, context="请求"):
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            self._log_response_error(e.response, context)
        else:
            logger.error("  ➜ 影巢%s失败: mode=%s 网络或代理异常: %s", context, self.mode(), e)

    def _request(self, method, path, *, json_body=None, timeout=15, use_relay_path=True):
        """
        use_relay_path=True:
          relay 模式下 path 是 VPS 中转路径；direct 模式下 path 是 HDHive /api/open 相对路径。
        """
        if self.use_relay:
            url = f"{self.relay_base_url}{path}"
            headers = self.relay_headers
            proxies = None  # VPS 中转一般不需要本机代理，避免绕路
        else:
            if not self.api_key:
                raise RuntimeError("未配置影巢 API Key，也未配置授权中转 relay_base_url/relay_secret")
            url = f"{self.BASE_URL}{path}"
            headers = self.headers
            proxies = self.proxies

        res = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=json_body,
            proxies=proxies,
            timeout=timeout,
        )
        return res

    # ----------------------------
    # 本地解锁频率限制
    # ----------------------------
    def _check_unlock_rate_limit(self):
        if self.limit_count <= 0:
            return

        with self._rate_limit_lock:
            if self.__class__._unlock_timestamps.maxlen != self.limit_count:
                items = list(self.__class__._unlock_timestamps)[-self.limit_count:] if self.__class__._unlock_timestamps else []
                self.__class__._unlock_timestamps = deque(items, maxlen=self.limit_count)

            if len(self.__class__._unlock_timestamps) == self.limit_count:
                elapsed = time.time() - self.__class__._unlock_timestamps[0]
                if elapsed < self.limit_window:
                    wait_time = self.limit_window - elapsed + 1.0
                    logger.info(
                        "  ➜ 触发影巢本地解锁频率限制 (%s次/%s秒)，主动等待 %.1f 秒...",
                        self.limit_count,
                        self.limit_window,
                        wait_time,
                    )
                    time.sleep(wait_time)

            self.__class__._unlock_timestamps.append(time.time())

    # ----------------------------
    # 对外方法
    # ----------------------------
    def ping(self):
        """
        relay 模式：检查 VPS 中转是否已有 access_token。
        direct 模式：检查 HDHive API Key。
        """
        try:
            if self.use_relay:
                res = self._request("GET", "/hdhive/status", timeout=10)
                data = self._safe_json(res)
                if res.status_code != 200 or not data.get("success"):
                    self._log_response_error(res, "中转状态检查")
                    return False
                return bool(data.get("has_access_token"))

            res = self._request("GET", "/ping", timeout=10)
            res.raise_for_status()
            data = res.json()
            return data.get("success") is True

        except Exception as e:
            self._handle_error(e, "Ping 测试")
            return False

    def get_relay_status(self):
        if not self.use_relay:
            return None
        try:
            res = self._request("GET", "/hdhive/status", timeout=10)
            data = self._safe_json(res)
            return data if res.status_code == 200 else None
        except Exception as e:
            self._handle_error(e, "中转状态检查")
            return None

    def get_user_info(self):
        try:
            path = "/api/hdhive/me" if self.use_relay else "/me"
            res = self._request("GET", path, timeout=15)
            data = self._safe_json(res)

            if res.status_code != 200:
                err = self._log_response_error(res, "获取用户信息")
                if err.get("code") == "VIP_REQUIRED":
                    return {"nickname": "普通用户", "user_meta": {"points": "未知 (需 Premium)"}}
                return None

            if data.get("success"):
                return data.get("data")

            logger.error("  ➜ 影巢获取用户信息失败: %s", data.get("message"))
            return None

        except Exception as e:
            self._handle_error(e, "获取用户信息")
            return None

    def get_quota(self):
        """
        你的 VPS 最小中转版未实现 quota 时，relay 模式返回 None。
        如果后面在 VPS 增加 /api/hdhive/quota，这里会自动读取。
        """
        try:
            path = "/api/hdhive/quota" if self.use_relay else "/quota"
            res = self._request("GET", path, timeout=10)
            data = self._safe_json(res)

            if res.status_code == 404 and self.use_relay:
                logger.debug("  ➜ 影巢中转未实现 quota 接口，跳过配额展示。")
                return None

            if res.status_code != 200:
                self._log_response_error(res, "获取配额")
                return None

            return data.get("data") if data.get("success") else None

        except Exception as e:
            self._handle_error(e, "获取配额")
            return None

    def get_resources(self, tmdb_id, media_type, target_season=None):
        """
        根据 TMDB ID 获取资源列表。
        relay 模式会请求：
          GET {relay_base_url}/api/hdhive/resources/{movie|tv}/{tmdb_id}
        direct 模式会请求：
          GET https://hdhive.com/api/open/resources/{movie|tv}/{tmdb_id}
        """
        try:
            media_type = str(media_type or "").strip()
            tmdb_id = str(tmdb_id or "").strip()

            if not media_type or not tmdb_id:
                return []

            if self.use_relay:
                path = f"/api/hdhive/resources/{quote(media_type, safe='')}/{quote(tmdb_id, safe='')}"
            else:
                path = f"/resources/{quote(media_type, safe='')}/{quote(tmdb_id, safe='')}"

            res = self._request("GET", path, timeout=30)
            data = self._safe_json(res)

            if res.status_code != 200:
                self._log_response_error(res, "获取资源列表")
                return []

            if not data.get("success"):
                logger.error("  ➜ 影巢获取资源列表失败: %s", data.get("message"))
                return []

            allowed_types = {"115", "magnet", "ed2k", "bt"}
            raw_resources = data.get("data", []) or []

            resources = [
                r for r in raw_resources
                if r.get("pan_type") is None
                or str(r.get("pan_type")).lower() in allowed_types
            ]

            if media_type == "tv" and target_season is not None:
                try:
                    season_text = f"S{int(target_season):02d}"
                except Exception:
                    season_text = str(target_season)

                logger.info(
                    "  ➜ 影巢资源接口返回 %s 条，保留可处理类型 %s 条；目标季 %s 不在请求阶段过滤，仅用于本地排序。",
                    len(raw_resources),
                    len(resources),
                    season_text,
                )
            else:
                logger.info(
                    "  ➜ 影巢资源接口返回 %s 条，保留可处理类型 %s 条。",
                    len(raw_resources),
                    len(resources),
                )

            return resources

        except Exception as e:
            self._handle_error(e, "获取资源列表")
            return []

    def unlock_resource(self, slug, max_retries=3, timeout=60):
        payload = {"slug": slug}
        path = "/api/hdhive/unlock" if self.use_relay else "/resources/unlock"

        retryable_exceptions = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
        )

        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                self._check_unlock_rate_limit()

                res = self._request("POST", path, json_body=payload, timeout=timeout)
                data = self._safe_json(res)

                if res.status_code == 429 and attempt < max_retries:
                    wait_seconds = self._get_retry_after(res, default=self.limit_window or 60)
                    logger.warning(
                        "  ➜ 影巢解锁触发 429 限制，mode=%s，第 %s/%s 次失败，等待 %ss 后重试...",
                        self.mode(),
                        attempt,
                        max_retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue

                if res.status_code in (400, 401, 402, 403, 404):
                    self._log_response_error(res, "解锁资源")
                    return None

                if 500 <= res.status_code < 600:
                    if attempt < max_retries:
                        wait_seconds = 2 ** (attempt - 1)
                        logger.warning(
                            "  ➜ 影巢解锁请求异常: HTTP %s，第 %s/%s 次失败，%ss 后重试...",
                            res.status_code,
                            attempt,
                            max_retries,
                            wait_seconds,
                        )
                        time.sleep(wait_seconds)
                        continue
                    self._log_response_error(res, "解锁资源")
                    return None

                if data.get("success"):
                    return data.get("data")

                logger.error("  ➜ 影巢解锁失败: %s", data.get("message"))
                return None

            except retryable_exceptions as e:
                last_exception = e
                if attempt < max_retries:
                    wait_seconds = 2 ** (attempt - 1)
                    logger.warning(
                        "  ➜ 影巢解锁请求异常，第 %s/%s 次失败: %s，%ss 后重试...",
                        attempt,
                        max_retries,
                        e,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue

                self._handle_error(e, "解锁资源")
                return None

            except Exception as e:
                self._handle_error(e, "解锁资源")
                return None

        if last_exception:
            self._handle_error(last_exception, "解锁资源")
        return None

    def checkin(self, is_gambler=False):
        try:
            payload = {"is_gambler": bool(is_gambler)}
            path = "/api/hdhive/checkin" if self.use_relay else "/checkin"

            res = self._request("POST", path, json_body=payload, timeout=30)
            data = self._safe_json(res)

            if res.status_code != 200:
                self._log_response_error(res, "签到")
                return {
                    "success": False,
                    "message": data.get("message") or data.get("description") or "签到失败",
                }

            return data

        except Exception as e:
            self._handle_error(e, "签到")
            return {"success": False, "message": "网络或鉴权异常，请查看日志"}
