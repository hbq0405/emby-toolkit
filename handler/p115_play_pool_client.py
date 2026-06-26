import json
import logging
import re

import requests

from handler.p115_service import (
    P115CacheManager,
    P115Client,
    get_115_ua,
    _p115_as_list,
    _p115_is_severe_failure,
    _p115_normalize_common_response,
    _p115_normalize_list_response,
    _p115_normalize_mkdir_response,
)

logger = logging.getLogger(__name__)


class P115PlayPoolClient:
    """小号播放池专用 Cookie 客户端。

    这套客户端只服务小号播放，不走 P115Service 混合代理，避免和主号/共享资源
    链路共用客户端状态。主号仍只在外层负责 holder 签名。
    """

    def __init__(self, cookie_str, app_type="alipaymini"):
        if not cookie_str:
            raise ValueError("Cookie 不能为空")
        self.cookie_str = str(cookie_str).strip()
        self.app_type = str(app_type or "alipaymini").strip() or "alipaymini"
        self.user_agent = get_115_ua(self.app_type)
        self.webapi = None
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if P115Client:
            self.webapi = P115Client(self.cookie_str, app=self.app_type)
            try:
                self.webapi.headers["user-agent"] = self.user_agent
            except Exception:
                pass

    def download_url(self, pick_code, user_agent=None):
        if self.webapi:
            url_obj = self.webapi.download_url(pick_code, user_agent=user_agent)
            return str(url_obj) if url_obj else None
        return None

    def request(self, url, method="GET", **kwargs):
        headers = {
            "User-Agent": self.user_agent,
            "Cookie": self.cookie_str,
        }
        headers.update(kwargs.pop("headers", {}) or {})
        kwargs.setdefault("timeout", 30)
        return self.session.request(method, url, headers=headers, **kwargs)

    def _json_result(self, resp):
        if isinstance(resp, dict):
            return resp
        if hasattr(resp, "json"):
            try:
                return resp.json()
            except Exception as e:
                return {"state": False, "error_msg": f"小号专用接口返回非 JSON: {e}; {getattr(resp, 'text', '')[:200]}"}
        return {"state": False, "error_msg": str(resp)}

    def fs_files(self, payload):
        params = {"aid": 1, "show_dir": 1, "limit": 1000, "offset": 0, "record_open_time": 0, "count_folders": 0}
        if isinstance(payload, dict):
            params.update(payload)
        elif payload is not None:
            params["cid"] = payload
        if self.webapi and hasattr(self.webapi, "fs_files"):
            try:
                return _p115_normalize_list_response(self.webapi.fs_files(params))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        resp = self.request("https://webapi.115.com/files", method="GET", params=params)
        return _p115_normalize_list_response(self._json_result(resp))

    def fs_mkdir(self, name, pid):
        payload = {"cname": str(name), "pid": str(pid)}
        if self.webapi and hasattr(self.webapi, "fs_mkdir"):
            try:
                return _p115_normalize_mkdir_response(self.webapi.fs_mkdir(str(name), pid=str(pid)))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        resp = self.request("https://webapi.115.com/files/add", method="POST", data=payload)
        return _p115_normalize_mkdir_response(self._json_result(resp))

    def fs_delete(self, fids):
        ids = [str(i) for i in _p115_as_list(fids) if i is not None]
        if self.webapi and hasattr(self.webapi, "fs_delete"):
            try:
                return _p115_normalize_common_response(self.webapi.fs_delete(ids))
            except Exception as e:
                if not _p115_is_severe_failure(e):
                    raise
        payload = {"ignore_warn": 1}
        if len(ids) == 1:
            payload["fid"] = ids[0]
        else:
            payload.update({f"fid[{i}]": fid for i, fid in enumerate(ids)})
        resp = self.request("https://webapi.115.com/rb/delete", method="POST", data=payload)
        return _p115_normalize_common_response(self._json_result(resp))

    def rapid_upload(self, payload=None, **kwargs):
        payload = dict(payload or {})
        payload.update({k: v for k, v in kwargs.items() if v not in (None, "")})

        target_cid = str(_first(payload.get("cid"), payload.get("target_cid"), payload.get("target")) or "").strip()
        sha1 = str(_first(payload.get("sha1"), payload.get("fileid"), payload.get("file_sha1")) or "").strip().upper()
        pick_code = str(_first(payload.get("pick_code"), payload.get("pickcode"), payload.get("pc")) or "").strip()
        file_name = str(_first(payload.get("file_name"), payload.get("filename"), payload.get("name")) or "").strip()
        size = _safe_size(_first(payload.get("size"), payload.get("file_size"), payload.get("filesize")))
        preid = str(_first(payload.get("preid"), payload.get("pre_sha1"), payload.get("pre_sha1_128k")) or "").strip().upper()
        sign_key = _first(payload.get("sign_key"), payload.get("sign_check_key"))
        sign_val = _first(payload.get("sign_val"), payload.get("sign_check_value"))

        if (not pick_code or not file_name or size <= 0) and sha1:
            try:
                row = P115CacheManager.get_file_cache_by_sha1(sha1)
                if row:
                    pick_code = pick_code or str(row.get("pick_code") or "").strip()
                    file_name = file_name or str(row.get("name") or "").strip()
                    if size <= 0:
                        size = _safe_size(row.get("size"))
            except Exception as e:
                logger.debug("  ➜ [小号专用秒传] 查询缓存失败: %s", e)

        if not target_cid:
            return {"state": False, "error_msg": "小号专用秒传缺少目标目录 cid", "_rapid_upload_backend": "play_pool_cookie"}
        if not re.fullmatch(r"[A-F0-9]{40}", sha1 or ""):
            return {"state": False, "error_msg": "小号专用秒传缺少合法 SHA1", "_rapid_upload_backend": "play_pool_cookie"}
        if size <= 0:
            return {"state": False, "error_msg": "小号专用秒传缺少文件大小", "_rapid_upload_backend": "play_pool_cookie"}
        if not file_name:
            file_name = f"{sha1}.mkv"
        if not self.webapi or not hasattr(self.webapi, "upload_init"):
            return {"state": False, "error_msg": "小号专用客户端未初始化 upload_init", "_rapid_upload_backend": "play_pool_cookie"}

        target = target_cid if str(target_cid).startswith("U_") else f"U_1_{target_cid}"
        init_payload = {
            "filename": file_name,
            "filesize": int(size),
            "fileid": sha1,
            "target": target,
            "topupload": "true",
        }
        if re.fullmatch(r"[A-F0-9]{40}", preid or ""):
            init_payload["preid"] = preid
        if sign_key and sign_val:
            init_payload["sign_key"] = str(sign_key)
            init_payload["sign_val"] = str(sign_val).upper()

        logger.debug(
            "  ➜ [小号专用秒传] 初始化上传: %s | sha1=%s... | preid=%s | size=%s | app=%s",
            file_name,
            sha1[:12],
            (preid[:12] + "...") if preid else "-",
            size,
            self.app_type,
        )

        try:
            resp = self.webapi.upload_init(init_payload)
        except Exception as e:
            logger.warning(
                "  ➜ [小号专用秒传] upload_init 异常: %s | payload=%s",
                e,
                {"target": target, "sha1": sha1[:12] + "...", "size": size, "has_preid": bool(preid), "has_sign": bool(sign_key and sign_val)},
            )
            return {
                "state": False,
                "error_msg": f"小号专用 initupload 异常: {e}",
                "_rapid_upload_backend": "play_pool_cookie",
                "_rapid_cookie_payload": {"target": target, "sha1": sha1[:12] + "...", "size": size},
            }

        return _normalize_upload_init_response(resp, sha1, size, file_name, target_cid, pick_code)


def _first(*values):
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _safe_size(value):
    try:
        if value in (None, "", [], {}):
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(",", "")
        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            return int(float(text))
    except Exception:
        return 0
    return 0


def _status_from_cookie_init(resp):
    if not isinstance(resp, dict):
        return "", {}
    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
    status = resp.get("status") if resp.get("status") is not None else data.get("status")
    return str(status) if status is not None else "", data


def _normalize_upload_init_response(resp, sha1, size, file_name, target_cid, pick_code):
    if not isinstance(resp, dict):
        return {
            "state": False,
            "error_msg": f"小号专用 initupload 返回非 dict: {type(resp).__name__}",
            "_rapid_upload_backend": "play_pool_cookie",
            "response": str(resp)[:500],
        }

    out = dict(resp)
    out["_rapid_upload_backend"] = "play_pool_cookie"
    out.setdefault("sha1", sha1)
    out.setdefault("file_name", file_name)
    out.setdefault("target_cid", target_cid)
    out.setdefault("size", size)
    status, data = _status_from_cookie_init(out)
    reuse = out.get("reuse") is True or str(out.get("reuse")).lower() == "true"

    if reuse or status in ("2", "success", "done"):
        out["state"] = True
        out["success"] = True
        out.setdefault("message", "小号专用 initupload 秒传成功")
        out.setdefault("rapid_upload", True)
        logger.debug("  ➜ [小号专用秒传] initupload 秒传成功: %s", file_name)
        return out

    if status == "1":
        out["state"] = False
        out.setdefault("error_msg", "小号专用 initupload 返回普通上传(status=1)，Rapid v2 不上传明文文件")
        out["_rapid_cookie_need_plain_upload"] = True
        return out

    if status == "7":
        sign_key_text = str(out.get("sign_key") or data.get("sign_key") or "")
        sign_check_text = str(out.get("sign_check") or data.get("sign_check") or "")
        logger.debug(
            "  ➜ [小号专用秒传] initupload 返回 status=7：sha1=%s..., pc=%s..., sign_check=%s, sign_key_prefix=%s..., sign_key_len=%s",
            sha1[:12],
            (pick_code or "-")[:8],
            sign_check_text or "-",
            sign_key_text[:12],
            len(sign_key_text),
        )
        out["state"] = False
        out["error_msg"] = (
            "小号专用 initupload 要求二次校验(status=7)，等待主号签名"
            if sign_key_text and sign_check_text
            else "小号专用 initupload 要求二次校验(status=7)，但缺少 sign_key/sign_check"
        )
        out["_rapid_sign_required"] = True
        out["_rapid_sign_backend"] = "play_pool_cookie"
        out["_rapid_sign_key"] = sign_key_text
        out["_rapid_sign_check"] = sign_check_text
        out["_rapid_sign_sha1"] = sha1
        out["_rapid_sign_size"] = size
        out["_rapid_sign_file_name"] = file_name
        return out

    out["state"] = False
    out.setdefault("error_msg", f"小号专用 initupload 未直接秒传，status={status or 'unknown'}")
    return out
