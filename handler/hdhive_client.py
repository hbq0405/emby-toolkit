# handler/hdhive_client.py
import os
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
    面向普通用户的影巢客户端。

    用户不需要填写 API Key / 中转地址 / 中转密钥。
    ETK 会自动注册一个本地实例 ID，并通过项目方统一的 HDHive Relay 完成 OAuth 授权和业务请求。
    """

    DEFAULT_RELAY_BASE_URL = os.getenv("HDHIVE_RELAY_BASE_URL", "https://hdhive.55565576.xyz").rstrip("/")
    REGISTER_TOKEN = os.getenv("HDHIVE_RELAY_REGISTER_TOKEN", "").strip()

    _unlock_timestamps = deque()
    _rate_limit_lock = threading.Lock()

    def __init__(self, api_key=None):
        # api_key 参数仅为兼容旧调用签名，不再使用。
        cfg = settings_db.get_setting("hdhive_config") or {}
        if not isinstance(cfg, dict):
            cfg = {}

        self.cfg = cfg
        self.relay_base_url = (
            os.getenv("HDHIVE_RELAY_BASE_URL")
            or cfg.get("relay_base_url")
            or self.DEFAULT_RELAY_BASE_URL
        ).strip().rstrip("/")

        self.instance_id = (cfg.get("relay_instance_id") or cfg.get("instance_id") or "").strip()
        self.instance_secret = (cfg.get("relay_instance_secret") or cfg.get("instance_secret") or "").strip()

        self.proxies = config_manager.get_proxies_for_requests()

        unlock_limit = cfg.get("unlock_limit") or {}
        self.limit_count = int(unlock_limit.get("count", 3))
        self.limit_window = int(unlock_limit.get("window", 60))

    # -------------------- 配置与注册 --------------------

    def _save_cfg(self):
        settings_db.save_setting("hdhive_config", self.cfg)

    def ensure_registered(self):
        if self.instance_id and self.instance_secret:
            return True

        headers = {"Accept": "application/json"}
        if self.REGISTER_TOKEN:
            headers["X-ETK-Register-Token"] = self.REGISTER_TOKEN

        try:
            res = requests.post(
                f"{self.relay_base_url}/api/etk/register",
                headers=headers,
                proxies=self.proxies,
                timeout=20,
            )
            data = self._safe_json(res)
            if res.status_code != 200 or not data.get("success"):
                logger.error("  ➜ 影巢中转实例注册失败: HTTP %s %s", res.status_code, data)
                return False

            item = data.get("data") or {}
            self.instance_id = item.get("instance_id") or ""
            self.instance_secret = item.get("instance_secret") or ""
            if not self.instance_id or not self.instance_secret:
                logger.error("  ➜ 影巢中转实例注册失败: 返回缺少 instance_id/instance_secret")
                return False

            self.cfg["relay_base_url"] = self.relay_base_url
            self.cfg["relay_instance_id"] = self.instance_id
            self.cfg["relay_instance_secret"] = self.instance_secret
            self._save_cfg()
            logger.info("  ➜ 影巢中转实例注册成功: %s", self.instance_id)
            return True

        except Exception as e:
            logger.error("  ➜ 影巢中转实例注册异常: %s", e)
            return False

    def _instance_headers(self):
        if not self.ensure_registered():
            raise RuntimeError("HDHive relay instance registration failed")

        return {
            "X-ETK-Instance-ID": self.instance_id,
            "X-ETK-Instance-Secret": self.instance_secret,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # -------------------- 通用请求与错误处理 --------------------

    def _safe_json(self, res):
        try:
            return res.json()
        except Exception:
            return {"success": False, "code": str(getattr(res, "status_code", "")), "message": getattr(res, "text", "")[:500]}

    def _log_response_error(self, res, context="请求"):
        data = self._safe_json(res)
        retry_after = res.headers.get("Retry-After") or data.get("retry_after_seconds")
        logger.error(
            "  ➜ 影巢%s失败: HTTP %s, code=%s, message=%s, description=%s, limit_scope=%s, retry_after=%s",
            context,
            res.status_code,
            data.get("code"),
            data.get("message"),
            data.get("description"),
            data.get("limit_scope"),
            retry_after,
        )
        return data

    def _handle_error(self, e, context="请求"):
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            self._log_response_error(e.response, context)
        else:
            logger.error("  ➜ 影巢%s失败: %s", context, e)

    def _get_retry_after(self, res, default=60):
        data = self._safe_json(res)
        raw = res.headers.get("Retry-After") or data.get("retry_after_seconds") or default
        try:
            return int(float(raw))
        except Exception:
            return int(default)

    def _request(self, method, path, json_body=None, timeout=30):
        url = self.relay_base_url + path
        return requests.request(
            method=method,
            url=url,
            headers=self._instance_headers(),
            json=json_body,
            proxies=self.proxies,
            timeout=timeout,
        )

    # -------------------- 授权与状态 --------------------

    def authorize_url(self):
        try:
            res = self._request("POST", "/api/etk/authorize-url", timeout=15)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                return ((data.get("data") or {}).get("authorize_url") or "")
            self._log_response_error(res, "生成授权链接")
            return ""
        except Exception as e:
            self._handle_error(e, "生成授权链接")
            return ""

    def get_relay_status(self):
        try:
            res = self._request("GET", "/api/hdhive/status", timeout=10)
            data = self._safe_json(res)
            return data if res.status_code == 200 else None
        except Exception as e:
            self._handle_error(e, "中转状态检查")
            return None

    def ping(self):
        status = self.get_relay_status() or {}
        return bool(status.get("success") and status.get("has_access_token"))


    def clear_authorization(self):
        """清除当前本地实例在 relay 上保存的影巢用户授权，保留 instance_id / instance_secret。"""
        try:
            res = self._request("DELETE", "/api/hdhive/authorization", timeout=20)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                logger.info("  ➜ 影巢授权已清除")
                return data

            self._log_response_error(res, "清除授权")
            return data
        except Exception as e:
            self._handle_error(e, "清除授权")
            return {"success": False, "message": "清除授权失败，请查看日志"}

    # -------------------- 业务接口 --------------------

    def get_quota(self):
        try:
            res = self._request("GET", "/api/hdhive/quota", timeout=15)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                return data.get("data")
            self._log_response_error(res, "获取配额")
            return None
        except Exception as e:
            self._handle_error(e, "获取配额")
            return None
        
    def get_vip_entitlements(self):
        try:
            res = self._request("GET", "/api/hdhive/vip/entitlements", timeout=15)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                return data.get("data")
            
            # ★ 静默处理：如果是普通用户，官方会返回 403 VIP_REQUIRED，这是正常现象，不打印错误日志
            if res.status_code == 403 and data.get("code") == "VIP_REQUIRED":
                return None
                
            self._log_response_error(res, "获取VIP权益")
            return None
        except Exception as e:
            self._handle_error(e, "获取VIP权益")
            return None

    def get_usage_today(self):
        try:
            # 对应影巢开放平台的 /api/open/usage/today
            res = self._request("GET", "/api/hdhive/usage/today", timeout=15)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                return data.get("data")
            self._log_response_error(res, "获取今日用量")
            return None
        except Exception as e:
            self._handle_error(e, "获取今日用量")
            return None

    def get_user_info(self):
        try:
            res = self._request("GET", "/api/hdhive/me", timeout=15)
            data = self._safe_json(res)
            if res.status_code == 200 and data.get("success"):
                return data.get("data")

            self._log_response_error(res, "获取用户信息")
            return None
        except Exception as e:
            self._handle_error(e, "获取用户信息")
            return None


    def _normalize_media_type(self, media_type):
        """影巢 OpenAPI 只接受 movie / tv。
        ETK 内部可能传 Movie/Series/Season/tvshow 等，这里统一归一化。
        """
        raw = str(media_type or "").strip().lower()
        if raw in {"movie", "movies", "film", "films"}:
            return "movie"
        if raw in {"tv", "series", "season", "episode", "show", "shows", "tvshow", "tvshows", "电视剧", "剧集", "季", "集"}:
            return "tv"
        # 默认按 tv 处理比直接把 Series/Season 传给影巢更安全；
        # 但空值仍回退 movie，避免电影入口缺字段时误查剧集。
        return "movie" if not raw else ("tv" if raw != "movie" else "movie")

    def get_resources(self, tmdb_id, media_type, target_season=None):
        try:
            normalized_media_type = self._normalize_media_type(media_type)
            media_type_for_url = quote(normalized_media_type, safe="")
            tmdb_id_for_url = quote(str(tmdb_id), safe="")
            logger.debug(
                "  ➜ 影巢资源查询参数: raw_type=%s, normalized_type=%s, tmdb_id=%s, season=%s",
                media_type, normalized_media_type, tmdb_id, target_season
            )
            res = self._request("GET", f"/api/hdhive/resources/{media_type_for_url}/{tmdb_id_for_url}", timeout=30)
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
                if r.get("pan_type") is None or str(r.get("pan_type")).lower() in allowed_types
            ]
            logger.info("  ➜ 影巢资源接口返回 %s 条，保留可处理类型 %s 条。", len(raw_resources), len(resources))
            return resources
        except Exception as e:
            self._handle_error(e, "获取资源列表")
            return []

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
                    logger.info("  ➜ 触发本地影巢解锁频率限制，主动等待 %.1f 秒...", wait_time)
                    time.sleep(wait_time)
            self.__class__._unlock_timestamps.append(time.time())

    def unlock_resource(self, slug, max_retries=3, timeout=60):
        last_exception = None
        for attempt in range(1, max_retries + 1):
            try:
                self._check_unlock_rate_limit()
                res = self._request("POST", "/api/hdhive/unlock", {"slug": slug}, timeout=timeout)

                if res.status_code == 429 and attempt < max_retries:
                    wait_seconds = self._get_retry_after(res, default=self.limit_window or 60)
                    logger.warning("  ➜ 影巢解锁触发 429，第 %s/%s 次失败，等待 %ss 后重试...", attempt, max_retries, wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                if 500 <= res.status_code < 600 and attempt < max_retries:
                    wait_seconds = 2 ** (attempt - 1)
                    logger.warning("  ➜ 影巢解锁 HTTP %s，第 %s/%s 次失败，%ss 后重试...", res.status_code, attempt, max_retries, wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                data = self._safe_json(res)
                if res.status_code == 200 and data.get("success"):
                    return data.get("data")

                self._log_response_error(res, "解锁资源")
                return None

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
                last_exception = e
                if attempt < max_retries:
                    wait_seconds = 2 ** (attempt - 1)
                    logger.warning("  ➜ 影巢解锁网络异常，第 %s/%s 次失败: %s，%ss 后重试...", attempt, max_retries, e, wait_seconds)
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
            res = self._request("POST", "/api/hdhive/checkin", {"is_gambler": bool(is_gambler)}, timeout=30)
            return self._safe_json(res)
        except Exception as e:
            self._handle_error(e, "签到")
            return {"success": False, "message": "网络或鉴权异常，请查看日志"}
