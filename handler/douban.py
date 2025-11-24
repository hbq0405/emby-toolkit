# handler/douban.py

import requests # type: ignore
from typing import Optional, Dict, Any, List
import logging
# --- 标准库导入 ---
import json
import re
import base64
import hashlib
import hmac
import time
from utils import clean_character_name_static
from urllib import parse
from datetime import datetime
from random import choice
import threading
# --- 标准库导入结束 ---

logger = logging.getLogger(__name__)


class DoubanApi:
    _session: Optional[requests.Session] = None
    _session_lock = threading.Lock()
    # --- ✨ 新增的冷却相关属性 ✨ ---
    _cooldown_seconds: float = 1.5  # 默认冷却时间（秒），可以设置一个安全值
    _last_request_time: float = 0.0 # 上次请求的时间戳
    _cooldown_lock = threading.Lock() # 用于冷却计时的线程锁，防止多线程下计时错乱
    # --- ✨ 新增属性结束 ✨ ---
    _user_cookie: Optional[str] = None

    _urls = {
        "search": "/search/weixin", "imdbid": "/movie/imdb/%s",
        "movie_detail": "/movie/", "tv_detail": "/tv/",
        "movie_celebrities": "/movie/%s/celebrities", "tv_celebrities": "/tv/%s/celebrities",
        "celebrity_detail": "/celebrity/%s",
    }
    _user_agents = [
        "api-client/1 com.douban.frodo/7.22.0.beta9(231) Android/23 product/Mate 40 vendor/HUAWEI model/Mate 40 brand/HUAWEI  rom/android  network/wifi  platform/AndroidPad",
        "api-client/1 com.douban.frodo/7.18.0(230) Android/22 product/MI 9 vendor/Xiaomi model/MI 9 brand/Android  rom/miui6  network/wifi  platform/mobile nd/1",
    ]
    _api_secret_key = "bf7dddc7c9cfe6f7"
    _api_key = "0dad551ec0f84ed02907ff5c42e8ec70"
    _api_key2 = "0ab215a8b1977939201640fa14c66bab"
    _base_url = "https://frodo.douban.com/api/v2"
    _api_url = "https://api.douban.com/v2"
    _default_timeout = 15 # 稍微增加超时
    

    def __init__(self, cooldown_seconds: Optional[float] = None, user_cookie: Optional[str] = None):
        if DoubanApi._session is None:
            with DoubanApi._session_lock:
                if DoubanApi._session is None:
                    DoubanApi._session = requests.Session()
                    logger.trace("DoubanApi requests.Session 已初始化。")
        
        if cooldown_seconds is not None and cooldown_seconds > 0:
            DoubanApi._cooldown_seconds = cooldown_seconds
            logger.trace(f"豆瓣Api 已设置请求冷却时间为: {DoubanApi._cooldown_seconds} 秒。")
        if user_cookie:
            DoubanApi._user_cookie = user_cookie
            logger.trace("DoubanApi 已加载用户登录 Cookie。")
    @classmethod
    def _apply_cooldown(cls):
        """在每次API请求前应用冷却等待，线程安全。"""
        with cls._cooldown_lock:
            now = time.time()
            elapsed = now - cls._last_request_time
            
            if elapsed < cls._cooldown_seconds:
                wait_time = cls._cooldown_seconds - elapsed
                logger.trace(f"豆瓣 API 冷却中... 等待 {wait_time:.2f} 秒。")
                time.sleep(wait_time)
            
            # 无论是否等待，都更新最后请求时间为当前时间
            cls._last_request_time = time.time()

    @classmethod
    def _ensure_session(cls):
        """确保 requests.Session 已初始化。线程安全。"""
        if cls._session is None:
            with cls._session_lock: # 加锁确保只有一个线程创建 session
                if cls._session is None: # 双重检查锁定模式
                    cls._session = requests.Session()
                    logger.trace("DoubanApi: requests.Session 已重新初始化 (ensure_session)。")

    @classmethod
    def _sign(cls, url: str, ts: str, method='GET') -> str:
        url_path = parse.urlparse(url).path
        raw_sign = '&'.join([method.upper(), parse.quote(url_path, safe=''), ts])
        return base64.b64encode(hmac.new(cls._api_secret_key.encode(), raw_sign.encode(), hashlib.sha1).digest()).decode()

    def _make_error_dict(self, error_code: str, message: str, original_response: Optional[Dict]=None) -> Dict[str, Any]:
        """辅助函数，创建统一的错误返回字典"""
        err_dict = {"error": error_code, "message": message}
        if original_response and isinstance(original_response, dict) and original_response.get("code"):
            err_dict["douban_code"] = original_response.get("code")
        return err_dict

    def __invoke(self, url: str, **kwargs) -> Dict[str, Any]:
        DoubanApi._apply_cooldown()
        DoubanApi._ensure_session() # <--- 在每次请求前确保 session 存在
        if DoubanApi._session is None: return self._make_error_dict("session_not_initialized", "Session未初始化")
        req_url = DoubanApi._base_url + url
        params: Dict[str, Any] = {'apiKey': DoubanApi._api_key, **kwargs}
        ts = params.pop('_ts', datetime.strftime(datetime.now(), '%Y%m%d'))
        params.update({'os_rom': 'android', '_ts': ts, '_sig': DoubanApi._sign(url=req_url, ts=ts)})
        headers = {'User-Agent': choice(DoubanApi._user_agents)}
        if DoubanApi._user_cookie:
            headers['Cookie'] = DoubanApi._user_cookie
        resp = None
        try:
            resp = DoubanApi._session.get(req_url, params=params, headers=headers, timeout=DoubanApi._default_timeout)
            resp.raise_for_status()
            response_json = resp.json()
            if response_json.get("code") == 1080:
                msg = response_json.get('msg', "豆瓣API速率限制")
                logger.warning(f"GET触发豆瓣速率限制: {msg}")
                return self._make_error_dict("rate_limit", msg, response_json)
            return response_json
        except requests.exceptions.HTTPError as e:
            msg = str(e)
            if e.response is not None:
                try:
                    error_json = e.response.json()
                    # 专门处理 need_login 错误
                    if error_json.get("code") == 1001 or "need_login" in error_json.get("msg", ""):
                        msg = "need_login"
                        logger.error(f"豆瓣API请求失败: 需要登录。请在设置中配置有效的豆瓣Cookie。")
                    else:
                        msg = error_json.get("msg", str(e))
                except json.JSONDecodeError:
                    msg = f"{str(e)} (响应非JSON: {e.response.text[:100]})"
            logger.error(f"HTTP error on GET {req_url}: {msg}", exc_info=False) # exc_info=False 避免need_login刷屏
            return self._make_error_dict("http_error", msg, getattr(e.response, 'json', lambda: None)())
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on GET {req_url}: {e}", exc_info=True)
            return self._make_error_dict("request_exception", str(e))
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError on GET {req_url}: {e}. Response text: {resp.text[:200] if resp else 'N/A'}", exc_info=True)
            return self._make_error_dict("json_decode_error", "无效的JSON响应")

    def __post(self, url: str, **kwargs) -> Dict[str, Any]:
        DoubanApi._apply_cooldown()
        DoubanApi._ensure_session() # <--- 在每次请求前确保 session 存在
        if DoubanApi._session is None: return self._make_error_dict("session_not_initialized", "Session未初始化")
        req_url = DoubanApi._api_url + url
        data_payload: Dict[str, Any] = {'apikey': DoubanApi._api_key2, **kwargs}
        if '_ts' in data_payload: data_payload.pop('_ts')
        headers = {'User-Agent': choice(DoubanApi._user_agents), "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"}
        if DoubanApi._user_cookie:
            headers['Cookie'] = DoubanApi._user_cookie
        resp = None
        try:
            resp = DoubanApi._session.post(req_url, data=data_payload, headers=headers, timeout=DoubanApi._default_timeout)
            resp.raise_for_status()
            response_json = resp.json()
            if response_json.get("code") == 1080:
                msg = response_json.get('msg', "豆瓣API速率限制")
                logger.warning(f"POST触发豆瓣速率限制: {msg}")
                return self._make_error_dict("rate_limit", msg, response_json)
            return response_json
        except requests.exceptions.HTTPError as e:
            # ▼▼▼ 核心修改在这里 ▼▼▼
            if e.response is not None and e.response.status_code == 404:
                # 1. 这是一个预期的“未找到”情况，使用 WARNING 级别日志，而不是 ERROR
                # 2. 日志内容更友好，明确指出是资源未找到
                # 3. 最关键：不使用 exc_info=True，这样就不会打印 traceback
                logger.warning(f"请求的资源未找到 (404 Not Found)，URL: {req_url}")
                
                # 4. 返回一个特定的错误字典，方便上层调用者判断具体错误类型
                #    这里的 "movie_not_found" 对应了日志中的 "movie_not_found"
                return self._make_error_dict("movie_not_found", f"IMDb ID a la que se consulta no encontrada en Douban")
            msg = str(e)
            if e.response is not None:
                try:
                    error_json = e.response.json()
                    if error_json.get("code") == 1001 or "need_login" in error_json.get("msg", ""):
                        msg = "need_login"
                        logger.error(f"豆瓣API请求失败: 需要登录。请在设置中配置有效的豆瓣Cookie。")
                    else:
                        msg = error_json.get("msg", str(e))
                except json.JSONDecodeError:
                    msg = f"{str(e)} (响应非JSON: {e.response.text[:100]})"
            logger.error(f"HTTP error on POST {req_url}: {msg}", exc_info=True)
            return self._make_error_dict("http_error", msg, getattr(e.response, 'json', lambda: None)())
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on POST {req_url}: {e}", exc_info=True)
            return self._make_error_dict("request_exception", str(e))
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError on POST {req_url}: {e}. Response text: {resp.text[:200] if resp else 'N/A'}", exc_info=True)
            return self._make_error_dict("json_decode_error", "无效的JSON响应 (POST)")

    def imdbid(self, imdbid: str, ts: Optional[str] = None) -> Dict[str, Any]:
        params = {}
        if ts: params['_ts'] = ts
        return self.__post(DoubanApi._urls["imdbid"] % imdbid, **params)

    def search(self, keyword: str, start: int = 0, count: int = 20, ts: Optional[str] = None) -> Dict[str, Any]:
        if ts is None: ts = datetime.strftime(datetime.now(), '%Y%m%d')
        return self.__invoke(DoubanApi._urls["search"], q=keyword, start=start, count=count, _ts=ts)

    def _get_subject_details(self, subject_id: str, subject_type: str = "movie") -> Dict[str, Any]:
        if not subject_id or not str(subject_id).isdigit():
            return self._make_error_dict("invalid_param", f"无效的豆瓣 subject_id: {subject_id}")
        url_key = f"{subject_type}_detail"
        if url_key not in DoubanApi._urls:
            return self._make_error_dict("invalid_param", f"未知的 subject_type for detail: {subject_type}")
        detail_url = DoubanApi._urls[url_key] + subject_id
        logger.info(f"  ➜ 通过豆瓣ID获取详情: {detail_url}")
        details = self.__invoke(detail_url)
        if details.get("error"): # __invoke 返回了错误
            logger.warning(f"获取豆瓣ID {subject_id} ({subject_type}) 详情失败: {details.get('message')}")
        return details # 直接返回 __invoke 的结果 (成功或错误字典)

    def match_info(self, name: str, imdbid: Optional[str] = None, mtype: Optional[str] = None,
               year: Optional[str] = None, season: Optional[int] = None) -> Dict[str, Any]:
        if imdbid and imdbid.strip().startswith("tt"):
            actual_imdbid = imdbid.strip()
            logger.trace(f"尝试通过IMDBID {actual_imdbid} (使用统一接口) 查询豆瓣信息...")
            
            # 1. 调用唯一的、简单的 imdbid 函数
            result_from_imdb = self.imdbid(actual_imdbid)
            
            if result_from_imdb.get("error"):
                logger.warning(f"IMDBID {actual_imdbid} 查询失败: {result_from_imdb.get('message')}")
            elif result_from_imdb.get("id"):
                douban_id_url = str(result_from_imdb.get("id"))
                match = re.search(r'/(movie|tv)/(\d+)/?$', douban_id_url)
                if match:
                    # 2. 从API结果中提取ID，但忽略它返回的类型
                    _, actual_douban_id = match.groups()
                    
                    # 3. ✨✨✨ 核心修正：直接使用从 Emby 传入的 mtype 作为最终类型 ✨✨✨
                    final_mtype = 'tv' if mtype and mtype.lower() in ['series', 'tv'] else 'movie'
                    
                    logger.trace(f"IMDBID '{actual_imdbid}' -> 豆瓣ID: {actual_douban_id}。将使用传入的类型: '{final_mtype}'")
                    
                    title = result_from_imdb.get("title", result_from_imdb.get("alt_title", name))
                    original_title = result_from_imdb.get("original_title")
                    year_from_api = str(result_from_imdb.get("year", "")).strip()
                    
                    return {"id": actual_douban_id, "title": title, "original_title": original_title,
                            "year": year_from_api or year, "type": final_mtype, "source": "imdb_lookup"}
                else:
                    logger.warning(f"IMDBID {actual_imdbid} 查询到的豆瓣ID URL '{douban_id_url}' 无法解析。")
            else:
                logger.warning(f"IMDBID {actual_imdbid} 查询结果无效或无ID。")

        # 如果IMDb查询失败或无IMDbID，则进行名称搜索
        logger.info(f"  ➜ IMDb查询失败或未提供ID，回退到名称搜索: '{name}'")
        return self._search_by_name_for_match_info(name, mtype, year, season)

    def _search_by_name_for_match_info(self, name: str, mtype: Optional[str],
                                       year: Optional[str] = None, season: Optional[int] = None) -> Dict[str, Any]:
        logger.info(f"  ➜ 开始使用名称 '{name}'{(', 年份: '+year) if year else ''}{(', 类型: '+mtype) if mtype else ''} 匹配豆瓣信息 ...")
        
        # 规范化 mtype，将 'Series' 视为 'tv'
        normalized_mtype = mtype
        if mtype and mtype.lower() == 'series':
            normalized_mtype = 'tv'
            logger.trace(f"  ➜ 将传入的媒体类型 'Series' 规范化为 'tv'。")

        # 改进 search_query 的构建，避免重复年份
        effective_year_in_query = year
        if year:
            # 检查 name 中是否已经包含年份
            year_pattern = re.compile(r'\((\d{4})\)')
            match = year_pattern.search(name)
            if match and match.group(1) == year:
                effective_year_in_query = '' # 如果 name 中已包含年份，则不在 query 中重复
                logger.debug(f"  ➜ 名称 '{name}' 中已包含年份 '{year}'，搜索查询中将不重复年份。")

        search_query = f"{name} {effective_year_in_query or ''}".strip()
        
        if not search_query: return self._make_error_dict("invalid_param", "搜索关键词为空")

        logger.trace(f"  ➜ 最终豆瓣搜索查询: '{search_query}'")
        search_result = self.search(search_query)
        logger.trace(f"  ➜ 名称搜索 '{search_query}' 原始结果: {search_result}")

        if search_result.get("error"):
            logger.warning(f"  ➜ 豆瓣名称搜索 '{search_query}' 返回错误: {search_result.get('message')}")
            return search_result # 直接返回搜索的错误

        items = search_result.get("items")
        if not items or not isinstance(items, list):
            logger.warning(f"  ➜ 豆瓣名称搜索 '{search_query}' 未找到条目或格式错误。")
            return self._make_error_dict("no_items_found", f"豆瓣名称搜索 '{search_query}' 未找到条目或格式错误。")

        candidates = []
        exact_match = None
        for item_obj in items:
            if not isinstance(item_obj, dict):
                logger.debug(f"  ➜ 跳过无效的搜索结果条目 (非字典): {item_obj}")
                continue
            target = item_obj.get("target", {})
            if not isinstance(target, dict):
                logger.debug(f"  ➜ 跳过无效的搜索结果条目 (target 非字典): {item_obj}")
                continue

            api_item_type = item_obj.get("target_type") # 从 item_obj 中获取 target_type
            if api_item_type not in ["movie", "tv"]:
                logger.debug(f"  ➜ 跳过不相关的类型 '{api_item_type}' for item: {target.get('title')}")
                continue
            
            # 使用规范化后的类型进行比较
            if normalized_mtype and normalized_mtype != api_item_type:
                logger.debug(f"  ➜ 跳过类型不匹配的条目。请求类型: '{normalized_mtype}', API类型: '{api_item_type}' for item: {target.get('title')}")
                continue

            title_from_api = target.get("title")
            douban_id = str(target.get("id", "")).strip()

            if not isinstance(title_from_api, str) or not title_from_api.strip() or not douban_id.isdigit():
                logger.trace(f"_search_by_name_for_match_info: 跳过无效条目，title='{title_from_api}' (类型: {type(title_from_api).__name__}), douban_id='{douban_id}'")
                continue

            title_str = title_from_api
            api_item_year = str(target.get("year", "")).strip()
            
            # 详细记录年份匹配过程
            year_match_status = "N/A"
            if not year:
                year_match = True
                year_match_status = "请求未提供年份，默认匹配"
            elif api_item_year and api_item_year.isdigit():
                try:
                    year_diff = abs(int(api_item_year) - int(year))
                    year_match = year_diff <= 1
                    year_match_status = f"请求年份: {year}, API年份: {api_item_year}, 差异: {year_diff}, 匹配: {year_match}"
                except ValueError:
                    year_match = False
                    year_match_status = f"API年份 '{api_item_year}' 或请求年份 '{year}' 无效，无法比较。"
            else:
                year_match = False
                year_match_status = f"API未提供年份或年份无效 ('{api_item_year}')。"
            
            logger.trace(f"处理条目 '{title_str}' ({api_item_year}, ID: {douban_id}, 类型: {api_item_type}). 年份匹配状态: {year_match_status}")

            if year_match:
                candidate_info = {"id": douban_id, "title": title_str, "original_title": target.get("original_title"),
                                  "year": api_item_year, "type": api_item_type, "source": "name_search_candidate"}
                
                name_to_compare = str(name).strip()

                if title_str.lower().strip() == name_to_compare.lower() and (not year or api_item_year == year):
                    exact_match = candidate_info
                    exact_match["source"] = "name_search_exact"
                    logger.debug(f"  ➜ 找到精确匹配: {exact_match}")
                    break
                candidates.append(candidate_info)
                logger.debug(f"  ➜ 添加候选匹配项: {candidate_info}")

        if exact_match: return exact_match
        if candidates:
            if len(candidates) == 1:
                logger.info(f"  ➜ 找到唯一候选匹配项: {candidates[0]}")
                return candidates[0]
            
            # 多个候选，尝试根据年份精确度排序
            if year:
                # 优先选择年份完全匹配的
                year_exact_candidates = [c for c in candidates if c.get("year") == year]
                if year_exact_candidates:
                    logger.info(f"  ➜ 找到多个年份精确匹配的候选，返回第一个。Candidates: {year_exact_candidates}")
                    return year_exact_candidates[0]
            
            logger.info(f"  ➜ 找到多个候选匹配项 for '{name}', 返回第一个。 Candidates: {candidates}")
            return candidates[0]

        logger.warning(f"  ➜ 豆瓣名称搜索未能为 '{name}' 找到合适的匹配项。")
        return self._make_error_dict("no_suitable_match", f"豆瓣名称搜索未能为 '{name}' 找到合适的匹配项。")

    def get_acting(self, name: str, imdbid: Optional[str] = None, mtype: Optional[str] = None,
                   year: Optional[str] = None, season: Optional[int] = None,
                   douban_id_override: Optional[str] = None) -> Dict[str, Any]:
        douban_subject_id = None
        final_mtype = mtype

        if douban_id_override and str(douban_id_override).isdigit():
            douban_subject_id = str(douban_id_override)
            logger.info(f"  ➜ 使用提供的豆瓣ID覆盖: {douban_subject_id}")
            if not final_mtype: # 尝试推断类型
                details_movie = self._get_subject_details(douban_subject_id, "movie")
                if details_movie and not details_movie.get("error") and details_movie.get("type"): final_mtype = details_movie.get("type")
                else:
                    details_tv = self._get_subject_details(douban_subject_id, "tv")
                    if details_tv and not details_tv.get("error") and details_tv.get("type"): final_mtype = details_tv.get("type")
                if final_mtype: logger.debug(f"推断豆瓣ID {douban_subject_id} 类型为: {final_mtype}")
                else: return self._make_error_dict("type_inference_failed", f"无法为豆瓣ID '{douban_subject_id}' 推断媒体类型", {"cast": []})
        else:
            match_info_result = self.match_info(name=name, imdbid=imdbid, mtype=mtype, year=year, season=season)
            if match_info_result.get("error"):
                return {**match_info_result, "cast": []} # 合并错误信息并添加空cast
            if match_info_result.get("id") and str(match_info_result.get("id")).isdigit():
                douban_subject_id = str(match_info_result.get("id"))
                if not final_mtype and match_info_result.get("type"): final_mtype = match_info_result.get("type")
            # elif match_info_result.get("search_candidates"): # 如果 match_info 返回候选列表
            #     return {**match_info_result, "cast": []} # 透传并加空cast
            else: # 未找到ID
                return self._make_error_dict("no_match_id_found", f"未能为 '{name}' 匹配到豆瓣ID", {"cast": []})

        if not douban_subject_id or not final_mtype:
            return self._make_error_dict("missing_id_or_type", f"获取演职员信息前豆瓣ID或类型无效 (ID: {douban_subject_id}, Type: {final_mtype})", {"cast": []})

        logger.info(f"  ➜ 获取豆瓣ID '{douban_subject_id}' (类型: {final_mtype}) 的演职员信息...")
        response = None
        if final_mtype == "tv": response = self.tv_celebrities(douban_subject_id)
        elif final_mtype == "movie": response = self.movie_celebrities(douban_subject_id)
        else: return self._make_error_dict("unknown_media_type", f"未知的媒体类型 '{final_mtype}'", {"cast": []})

        if not response or response.get("error"): # 检查错误
            err_msg = response.get("message", "获取演职员信息失败") if response else "获取演职员信息无响应"
            return self._make_error_dict(response.get("error", "api_error") if response else "no_response", err_msg, {"cast": []})

        data: Dict[str, List[Dict[str, Any]]] = {"cast": []}
        actors_list = response.get("celebrities", response.get("actors", []))
        if actors_list is None: actors_list = []

        for idx, item in enumerate(actors_list):
            if not isinstance(item, dict): continue
            character_str_raw = item.get("character", "")
            if not character_str_raw and item.get("attrs", {}).get("role"):
                roles = item.get("attrs").get("role")
                if isinstance(roles, list) and roles: character_str_raw = " / ".join(r for r in roles if isinstance(r, str))
            
            cleaned_char_name = clean_character_name_static(character_str_raw)
            actor_id_str = str(item.get("id", "")).strip()
            actor_id_int = int(actor_id_str) if actor_id_str.isdigit() else None
            profile_img_obj = item.get("avatar", item.get("cover_url"))
            profile_path_val = None
            if isinstance(profile_img_obj, dict): profile_path_val = profile_img_obj.get("large", profile_img_obj.get("normal"))
            elif isinstance(profile_img_obj, str): profile_path_val = profile_img_obj

            data["cast"].append({
                "name": item.get("name"), "character": cleaned_char_name, "id": actor_id_int,
                "original_name": item.get("latin_name", item.get("name_en")),
                "profile_path": profile_path_val, "order": item.get("rank", idx)
                # 可以添加更多TMDb兼容的字段，如果豆瓣API提供或可以转换
            })
        return data # 成功时返回包含 cast 的字典

    def movie_celebrities(self, subject_id: str) -> Dict[str, Any]:
        return self.__invoke(DoubanApi._urls["movie_celebrities"] % subject_id)

    def tv_celebrities(self, subject_id: str) -> Dict[str, Any]:
        return self.__invoke(DoubanApi._urls["tv_celebrities"] % subject_id)

    def close(self):
        with DoubanApi._session_lock: # 关闭时也加锁
            if DoubanApi._session:
                try: DoubanApi._session.close(); logger.trace("DoubanApi requests.Session 已关闭。")
                except Exception as e: logger.error(f"关闭 DoubanApi session 时出错: {e}")
                finally: DoubanApi._session = None

    # ✨✨✨ 获取演员详细信息方法 ✨✨✨
    def celebrity_details(self, celebrity_id: str) -> Dict[str, Any]:
        """获取单个名人（演员/导演）的详细信息。"""
        if not celebrity_id or not str(celebrity_id).isdigit():
            return self._make_error_dict("invalid_param", f"无效的名人 celebrity_id: {celebrity_id}")
        
        detail_url = DoubanApi._urls["celebrity_detail"] % celebrity_id
        logger.debug(f"  ➜ 获取豆瓣演员详情: {detail_url}")
        details = self.__invoke(detail_url)
        return details
    
    # ▼▼▼ 通过豆瓣链接获取其对应的IMDb ID ▼▼▼
    def get_details_from_douban_link(self, douban_link: str, mtype: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        【V3 - 增强版】专门通过豆瓣链接获取其完整的详情信息字典。
        """
        logger.debug(f"  ➜ 专用函数：尝试从豆瓣链接 '{douban_link}' 获取完整详情...")
        match = re.search(r'/(?:movie|tv|subject)/(\d+)', douban_link)
        if not match:
            logger.warning(f"  ➜ 无法从链接中解析出豆瓣ID。")
            return None

        douban_id = match.group(1)
        
        primary_type = 'tv' if mtype and mtype.lower() in ['series', 'tv'] else 'movie'
        secondary_type = 'movie' if primary_type == 'tv' else 'tv'

        details = self._get_subject_details(douban_id, primary_type)
        if details.get("error"):
            logger.trace(f"  ➜ 使用主类型 '{primary_type}' 获取详情失败，尝试备用类型 '{secondary_type}'...")
            details = self._get_subject_details(douban_id, secondary_type)

        if details.get("error"):
            logger.error(f"  ➜ 无法获取豆瓣ID '{douban_id}' 的详情: {details.get('message')}")
            return None # 如果有错误，返回 None
        return details # 成功时返回完整的 details 字典

if __name__ == '__main__':
    # 测试代码现在不再需要创建数据库文件
    try:
        api = DoubanApi(cooldown_seconds=2.0)
        
        # 测试API调用 (需要网络)
        # search_res = api.search("你好，李焕英")
        # logger.info(f"搜索结果: {json.dumps(search_res, indent=2, ensure_ascii=False)}")
        
    except Exception as e:
        logger.error(f"DoubanApi 测试异常: {e}", exc_info=True)
    finally:
        if 'api' in locals() and api:
            api.close()
        logger.info("--- DoubanApi 测试结束 ---")
