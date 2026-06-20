# handler/tmdb.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import concurrent.futures
import re
from utils import contains_chinese, normalize_name_for_matching
from typing import Optional, List, Dict, Any, Callable
import logging
import config_manager
import constants
import threading
logger = logging.getLogger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
# ★★★ 自定义的重试类，用于输出更友好的日志 ★★★
class LoggedRetry(Retry):
    """
    一个继承自 urllib3.Retry 的自定义类，
    用于在每次重试时记录一条更清晰、更友好的日志消息。
    """
    def increment(self, method, url, response=None, error=None, _pool=None, _stacktrace=None):
        # ★ 修复：在调用父类 increment 之前计算真实的原始总次数
        # self.total 是剩余次数，len(self.history) 是已失败次数
        original_total = (len(self.history) + self.total) if self.total is not None else 0
        attempt_number = len(self.history) + 1
        backoff_time = self.get_backoff_time()

        if response:
            reason = f"不成功的状态码: {response.status}"
        elif error:
            reason = f"连接错误: {error.__class__.__name__}"
        else:
            reason = "未知错误"

        logger.warning(
            f"  ➜ TMDb API 请求失败 ({reason})。将在 {backoff_time:.2f} 秒后重试... (第 {attempt_number}/{original_total} 次)"
        )

        # 调用父类方法，它会返回一个新的 Retry 对象用于下一次请求
        return super().increment(method, url, response, error, _pool, _stacktrace)

# ★★★ 创建带重试功能的 Session (已修改为使用 LoggedRetry) ★★★
def requests_retry_session(
    retries=3,
    backoff_factor=2,
    status_forcelist=(500, 502, 503, 504, 429),
    session=None,
):
    session = session or requests.Session()
    retry = LoggedRetry(
        total=retries,
        read=retries,
        connect=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

_tmdb_session_local = threading.local()

def get_tmdb_session() -> requests.Session:
    """为每个线程复用独立 Session，避免多线程共享 requests.Session 卡住连接池。"""
    session = getattr(_tmdb_session_local, "session", None)
    if session is None:
        session = requests_retry_session()
        _tmdb_session_local.session = session
    return session

def get_tmdb_api_base_url() -> str:
    """
    从配置管理器获取TMDb API基础URL，如果未配置则使用默认值
    """
    return config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_BASE_URL, "https://api.themoviedb.org/3")

# 默认语言设置
DEFAULT_LANGUAGE = "zh-CN"
DEFAULT_REGION = "CN"

def _config_bool(key: str, default: bool = False) -> bool:
    """兼容 bool / str / int 的配置布尔值读取。"""
    value = config_manager.APP_CONFIG.get(key, default)

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value != 0

    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on", "enabled", "开启", "启用")

    return bool(value)


def _get_ai_translation_flags() -> Dict[str, bool]:
    """集中读取 AI 翻译相关开关，避免到处散落 APP_CONFIG 读取逻辑。"""
    return {
        "title": _config_bool(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False),
        "overview": _config_bool(constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW, False),
        "episode_overview": _config_bool(constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW, False),
    }

def _sanitize_text(text: str) -> str:
    """隐藏文本中的 api_key，防止日志泄露"""
    if not text:
        return str(text)
    # 将 api_key= 后面的字母数字替换为 ***
    return re.sub(r'api_key=[a-zA-Z0-9]+', 'api_key=***', str(text))

# --- 通用的 TMDb 请求函数 ---
def _tmdb_request(endpoint: str, api_key: str, params: Optional[Dict[str, Any]] = None, use_default_language: bool = True) -> Optional[Dict[str, Any]]:
    """【V2.1 - 最终驱魔版】增加了 use_default_language 开关，用于控制是否添加默认语言参数。"""
    if not api_key:
        logger.error("TMDb API Key 未提供，无法发起请求。")
        return None

    tmdb_base_url = get_tmdb_api_base_url()
    full_url = f"{tmdb_base_url}{endpoint}"
    base_params = {
        "api_key": api_key,
    }
    # 只有当开启 use_default_language 时，才添加默认语言参数
    if use_default_language:
        base_params["language"] = DEFAULT_LANGUAGE
    if params:
        base_params.update(params)

    try:
        proxies = config_manager.get_proxies_for_requests()
        response = get_tmdb_session().get(full_url, params=base_params, timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as e:
        error_details = ""
        try:
            error_data = e.response.json()
            error_details = error_data.get("status_message", str(e))
        except json.JSONDecodeError:
            error_details = str(e)
            
        safe_error_details = _sanitize_text(error_details)
        logger.error(f"  ➜ 所有重试后 TMDb API HTTP 出现错误: {e.response.status_code} - {safe_error_details}. URL: {full_url}", exc_info=False)
        return None
    except requests.exceptions.RequestException as e:
        safe_e = _sanitize_text(str(e))
        logger.error(f"  ➜ 所有重试后 TMDb API 请求均出现错误: {safe_e}. URL: {full_url}", exc_info=False)
        return None
    except json.JSONDecodeError as e:
        safe_e = _sanitize_text(str(e))
        safe_response = _sanitize_text(response.text[:200]) if response else 'N/A'
        logger.error(f"  ➜ TMDb API JSON 解码错误: {safe_e}. URL: {full_url}. Response: {safe_response}", exc_info=False)
        return None

# --- 获取电影的详细信息 ---
def get_movie_details(movie_id: int, api_key: str, append_to_response: Optional[str] = "credits,videos,images,keywords,external_ids,translations,release_dates,alternative_titles", language: Optional[str] = None, include_image_language: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    【新增】获取电影的详细信息。
    增加 include_image_language 参数支持自定义图片语言筛选。
    """
    endpoint = f"/movie/{movie_id}"
    
    # 默认的图片语言列表
    default_img_lang = "zh-CN,zh-TW,zh,en,null,ja,ko"
    
    params = {
        "language": language or DEFAULT_LANGUAGE, 
        "append_to_response": append_to_response or "",
        # 优先使用传入的参数，否则使用默认值
        "include_image_language": include_image_language if include_image_language is not None else default_img_lang
    }
    logger.trace(f"  ➜ TMDb: 获取电影详情 (ID: {movie_id})")
    details = _tmdb_request(endpoint, api_key, params)
    
    # ... (保留原本的英文标题补充逻辑) ...
    if details and details.get("original_language") != "en" and DEFAULT_LANGUAGE.startswith("zh"):
        if "translations" in (append_to_response or "") and details.get("translations", {}).get("translations"):
            for trans in details["translations"]["translations"]:
                if trans.get("iso_639_1") == "en" and trans.get("data", {}).get("title"):
                    details["english_title"] = trans["data"]["title"]
                    logger.trace(f"  从translations补充电影英文名: {details['english_title']}")
                    break
        if not details.get("english_title"):
            logger.trace(f"  ➜ 尝试获取电影 {movie_id} 的英文名...")
            en_params = {"language": "en-US"}
            en_details = _tmdb_request(f"/movie/{movie_id}", api_key, en_params)
            if en_details and en_details.get("title"):
                details["english_title"] = en_details.get("title")
                logger.trace(f"  ➜ 通过请求英文版补充电影英文名: {details['english_title']}")
    elif details and details.get("original_language") == "en":
        details["english_title"] = details.get("original_title")

    return details

# --- 获取电视剧的详细信息 ---
def get_tv_details(
    tv_id: int,
    api_key: str,
    append_to_response: Optional[str] = "credits,videos,images,keywords,external_ids,translations,content_ratings,alternative_titles",
    language: Optional[str] = None,
    include_image_language: Optional[str] = None,
    allow_english_fallback: Optional[bool] = None
) -> Optional[Dict[str, Any]]:
    """
    获取电视剧的详细信息。
    allow_english_fallback:
      - None: 自动按 AI 翻译标题/简介开关决定
      - True: 允许请求英文版兜底
      - False: 禁止额外英文兜底请求
    """
    endpoint = f"/tv/{tv_id}"
    default_img_lang = "zh-CN,zh-TW,zh,en,null,ja,ko"

    params = {
        "language": language or DEFAULT_LANGUAGE,
        "append_to_response": append_to_response or "",
        "include_image_language": include_image_language if include_image_language is not None else default_img_lang
    }

    logger.trace(f"  ➜ TMDb: 获取电视剧详情 (ID: {tv_id})")
    details = _tmdb_request(endpoint, api_key, params)

    if not details:
        return None

    flags = _get_ai_translation_flags()
    if allow_english_fallback is None:
        allow_english_fallback = flags["title"] or flags["overview"]

    if details.get("original_language") == "en":
        details["english_name"] = details.get("original_name")
        return details

    if not DEFAULT_LANGUAGE.startswith("zh"):
        return details

    # 先从 translations 里拿英文名，不额外增加请求
    if "translations" in (append_to_response or "") and details.get("translations", {}).get("translations"):
        for trans in details["translations"]["translations"]:
            if trans.get("iso_639_1") == "en" and trans.get("data", {}).get("name"):
                details["english_name"] = trans["data"]["name"]
                logger.trace(f"  从 translations 补充剧集英文名: {details['english_name']}")
                break

    # 没开 AI 标题/简介翻译，就不要为了兜底多请求英文版
    if not allow_english_fallback:
        return details

    need_en_title = flags["title"] and not details.get("english_name")
    need_en_overview = flags["overview"] and (not details.get("overview") or len(details.get("overview", "")) < 2)

    if need_en_title or need_en_overview:
        logger.trace(f"  ➜ 剧集 {tv_id} 缺失 AI 翻译源文本，尝试请求英文版兜底...")
        en_details = _tmdb_request(f"/tv/{tv_id}", api_key, {"language": "en-US"})

        if en_details:
            if need_en_title and en_details.get("name"):
                details["english_name"] = en_details.get("name")
                logger.trace(f"  ➜ 通过英文版补充剧集英文名: {details['english_name']}")

            if need_en_overview and en_details.get("overview"):
                details["overview"] = en_details.get("overview")
                logger.trace("  ➜ 通过英文版补充剧集简介源文本")

    return details

# --- 获取电视剧某一季的详细信息 ---
def get_season_details_tmdb(
    tv_id: int,
    season_number: int,
    api_key: str,
    append_to_response: Optional[str] = "credits",
    item_name: Optional[str] = None,
    language: Optional[str] = None,
    include_image_language: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    endpoint = f"/tv/{tv_id}/season/{season_number}"

    params = {
        "language": language or DEFAULT_LANGUAGE
    }

    if append_to_response:
        params["append_to_response"] = append_to_response

    if include_image_language is not None:
        params["include_image_language"] = include_image_language

    item_name_for_log = f"'{item_name}' " if item_name else ""

    if language and language != DEFAULT_LANGUAGE:
        logger.debug(f"  ➜ TMDb API: 获取电视剧 {item_name_for_log}(ID: {tv_id}) 第 {season_number} 季的详情 (语言: {language})...")
    else:
        logger.debug(f"  ➜ TMDb API: 获取电视剧 {item_name_for_log}(ID: {tv_id}) 第 {season_number} 季的详情...")

    return _tmdb_request(endpoint, api_key, params)

# --- 获取电视剧某一季的集总数 ---
def get_season_episode_count(api_key: str, tmdb_id: int, season_number: int) -> int:
    """
    通过 TMDb ID 和季度号获取该季的剧集总数。
    """
    if not api_key or not tmdb_id:
        return 0
    
    # 构造请求端点：/tv/{series_id}/season/{season_number}
    endpoint = f"/tv/{tmdb_id}/season/{season_number}"
    try:
        data = _tmdb_request(endpoint, api_key, {"language": "zh-CN"})
        if data and "episodes" in data:
            return len(data["episodes"])
    except Exception as e:
        logger.error(f"TMDb: 获取剧集数量失败 (ID: {tmdb_id}, S{season_number}): {e}")
    
    return 0

# --- 获取电视剧某一季的详细信息，简化调用版 ---
def get_tv_season_details(tv_id: int, season_number: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    获取电视剧某一季的详细信息。
    这是 get_season_details_tmdb 的一个更简洁的别名，用于简化调用并获取海报。
    """
    # 直接调用已有的、功能更全的函数。
    # 我们不需要 'credits' 等附加信息，所以 append_to_response 传 None，这样请求更轻量。
    return get_season_details_tmdb(
        tv_id=tv_id,
        season_number=season_number,
        api_key=api_key,
        append_to_response=None
    )

# --- 并发获取剧集详情 ---
def aggregate_full_series_data_from_tmdb(
    tv_id: int,
    api_key: str,
    max_workers: int = 5
) -> Optional[Dict[str, Any]]:
    """
    【V4 - 智能补全版】
    通过并发请求获取每一季的详情。
    ★ 新增特性：如果检测到分集简介为空（TMDb未返回中文），会自动请求英文版数据进行补全，
    确保 core_processor 的 AI 翻译功能有源文本可译。
    """
    if not tv_id or not api_key:
        return None

    logger.info(f"  ➜ 开始为剧集 ID {tv_id} 并发聚合 TMDB 数据 (并发数: {max_workers})...")

    ai_flags = _get_ai_translation_flags()
    allow_series_english_fallback = ai_flags["title"] or ai_flags["overview"]
    allow_episode_english_fallback = ai_flags["title"] or ai_flags["episode_overview"]
    
    # --- 步骤 1: 获取顶层剧集详情 ---
    series_details = get_tv_details(
        tv_id,
        api_key,
        append_to_response="credits,aggregate_credits,keywords,external_ids,content_ratings,alternative_titles,translations",
        allow_english_fallback=allow_series_english_fallback
    )
    
    if not series_details:
        logger.error(f"  ➜ 聚合失败：无法获取顶层剧集 {tv_id} 的详情。")
        return None
    
    if series_details.get('aggregate_credits'):
        agg_cast = series_details['aggregate_credits'].get('cast', [])
        mapped_cast = []
        for actor in agg_cast:
            new_actor = actor.copy()
            roles = actor.get('roles', [])
            if roles and 'character' in roles[0]:
                new_actor['character'] = roles[0]['character']
            mapped_cast.append(new_actor)
        if mapped_cast:
            if 'credits' not in series_details: series_details['credits'] = {}
            series_details['credits']['cast'] = mapped_cast

    logger.info(f"  ➜ 成功获取剧集 '{series_details.get('name')}' 的顶层信息，共 {len(series_details.get('seasons', []))} 季。")

    orig_lang = series_details.get("original_language", "en")
    lang_pref = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE, 'zh')
    
    if lang_pref == 'zh':
        img_lang_param = "zh-CN,zh-TW,zh,en,null"
    else:
        if orig_lang != 'en':
            img_lang_param = f"{orig_lang},en,null,zh-CN,zh-TW,zh"
        else:
            img_lang_param = "en,null,zh-CN,zh-TW,zh"

    # --- 步骤 2: 定义智能获取函数 ---
    def _fetch_season_smart(tvid, s_num):
        """内部函数：获取季数据，如果简介缺失则自动获取英文版补全"""
        # 1. 获取默认语言 (通常是中文)，★ 附加 images 请求
        data_zh = get_season_details_tmdb(
            tvid, s_num, api_key, 
            append_to_response="credits,images", 
            include_image_language=img_lang_param
        )
        if not data_zh: 
            return None
            
        if "images" in data_zh and "posters" in data_zh["images"] and data_zh["images"]["posters"]:
            best_poster = data_zh["images"]["posters"][0]["file_path"]
            data_zh["poster_path"] = best_poster
        
        # 2. 按 AI 翻译开关决定是否请求英文版兜底
        # 兜底的目的只是给 AI 翻译提供源文本；没开对应翻译，就不浪费请求。
        if DEFAULT_LANGUAGE.startswith("zh") and allow_episode_english_fallback:
            episodes = data_zh.get("episodes", [])

            need_fallback_indices = []

            for i, ep in enumerate(episodes):
                overview_missing = not ep.get("overview") or len(ep.get("overview", "")) < 2

                current_title = ep.get("name", "")
                title_generic = bool(re.match(
                    r'^(第\s*\d+\s*集|Episode\s*\d+)$',
                    current_title,
                    re.IGNORECASE
                ))
                title_missing_or_weak = not current_title or title_generic or not contains_chinese(current_title)

                need_overview_fallback = ai_flags["episode_overview"] and overview_missing
                need_title_fallback = ai_flags["title"] and title_missing_or_weak

                if need_overview_fallback or need_title_fallback:
                    need_fallback_indices.append(i)

            if need_fallback_indices:
                logger.debug(
                    f"  ➜ 第 {s_num} 季有 {len(need_fallback_indices)} 集缺失 AI 翻译源文本，正在请求英文版兜底..."
                )

                try:
                    data_en = get_season_details_tmdb(tvid, s_num, api_key, language="en-US")

                    if data_en:
                        episodes_en = data_en.get("episodes", [])
                        en_ep_map = {e.get("episode_number"): e for e in episodes_en}

                        filled_overview_count = 0
                        filled_title_count = 0

                        for idx in need_fallback_indices:
                            target_ep = episodes[idx]
                            ep_num = target_ep.get("episode_number")

                            if ep_num not in en_ep_map:
                                continue

                            en_data_item = en_ep_map[ep_num]

                            # A. 分集简介兜底：只在“翻译集简介”开启时执行
                            if ai_flags["episode_overview"]:
                                current_overview = target_ep.get("overview", "")
                                if not current_overview or len(current_overview) < 2:
                                    en_overview = en_data_item.get("overview")
                                    if en_overview:
                                        target_ep["overview"] = en_overview
                                        filled_overview_count += 1

                            # B. 分集标题兜底：只在“翻译标题”开启时执行
                            if ai_flags["title"]:
                                en_title = en_data_item.get("name")
                                current_title = target_ep.get("name", "")

                                if en_title:
                                    is_en_generic = bool(re.match(
                                        r'^Episode\s*\d+$',
                                        en_title,
                                        re.IGNORECASE
                                    ))

                                    is_current_generic = bool(re.match(
                                        r'^(第\s*\d+\s*集|Episode\s*\d+)$',
                                        current_title,
                                        re.IGNORECASE
                                    ))

                                    if not is_en_generic:
                                        if not current_title or is_current_generic or not contains_chinese(current_title):
                                            target_ep["name"] = en_title
                                            filled_title_count += 1

                        if filled_overview_count or filled_title_count:
                            logger.debug(
                                f"  ➜ 第 {s_num} 季英文兜底完成：简介 {filled_overview_count} 条，标题 {filled_title_count} 条。"
                            )

                except Exception as e:
                    logger.warning(f"  ➜ 补全英文分集源文本失败: {e}")

        return data_zh

    # --- 步骤 3: 构建任务 ---
    tasks = []
    for season in series_details.get("seasons", []):
        season_number = season.get("season_number")
        if season_number is not None and season_number > 0:
            tasks.append(("season", tv_id, season_number))

    if not tasks:
        return {"series_details": series_details, "seasons_details": [], "episodes_details": {}}

    # --- 步骤 4: 并发执行 (使用 _fetch_season_smart) ---
    results = {}
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    timed_out = False
    try:
        future_to_task = {}
        for task in tasks:
            _, tvid, s_num = task
            # ★★★ 这里提交的是 _fetch_season_smart ★★★
            future = executor.submit(_fetch_season_smart, tvid, s_num)
            future_to_task[future] = f"S{s_num}"

        done_count = 0
        aggregate_timeout = min(300, max(60, len(tasks) * 45))
        try:
            completed_futures = concurrent.futures.as_completed(future_to_task, timeout=aggregate_timeout)
            for future in completed_futures:
                done_count += 1
                task_key = future_to_task[future]
                try:
                    result_data = future.result()
                    if result_data:
                        results[task_key] = result_data
                    logger.trace(f"    ({done_count}/{len(tasks)}) 季数据 {task_key} 获取完成。")
                except Exception as exc:
                    logger.error(f"    任务 {task_key} 执行时产生错误: {exc}")
        except concurrent.futures.TimeoutError:
            timed_out = True
            pending = [task_key for future, task_key in future_to_task.items() if not future.done()]
            for future in future_to_task:
                if not future.done():
                    future.cancel()
            logger.error(
                f"  ➜ TMDb 聚合等待超时 ({aggregate_timeout}s)，已完成 {done_count}/{len(tasks)}，"
                f"未完成: {', '.join(pending[:10])}{'...' if len(pending) > 10 else ''}"
            )
    finally:
        executor.shutdown(wait=not timed_out, cancel_futures=timed_out)

    # --- 步骤 5: 聚合数据与结构清洗 (保持不变) ---
    final_aggregated_data = {
        "series_details": series_details,
        "seasons_details": [], 
        "episodes_details": {} 
    }

    temp_seasons = []

    for key, season_data in results.items():
        if not season_data: continue
        
        temp_seasons.append(season_data)
        
        episodes_list = season_data.get("episodes", [])
        season_num = season_data.get("season_number")
        
        for ep in episodes_list:
            ep_num = ep.get("episode_number")
            if season_num is not None and ep_num is not None:
                if 'credits' not in ep:
                    ep['credits'] = {
                        'cast': ep.get('cast', []),
                        'guest_stars': ep.get('guest_stars', []),
                        'crew': ep.get('crew', [])
                    }
                
                ep_key = f"S{season_num}E{ep_num}"
                final_aggregated_data["episodes_details"][ep_key] = ep

    temp_seasons.sort(key=lambda x: x.get("season_number", 0))
    final_aggregated_data["seasons_details"] = temp_seasons
            
    logger.info(f"  ➜ 聚合完成。获取了 {len(temp_seasons)} 个季详情，提取并清洗了 {len(final_aggregated_data['episodes_details'])} 个集详情。")
    
    return final_aggregated_data

# --- 通过外部ID (如 IMDb ID) 在 TMDb 上查找人物 ---
def find_person_by_external_id(external_id: str, api_key: str, source: str = "imdb_id",
                               names_for_verification: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """
    【V5 - 精确匹配版】通过外部ID查找TMDb名人信息。
    只使用最可靠的外文名 (original_name) 进行精确匹配验证。
    """
    if not all([external_id, api_key, source]):
        return None
    tmdb_base_url = get_tmdb_api_base_url()
    api_url = f"{tmdb_base_url}/find/{external_id}"
    params = {"api_key": api_key, "external_source": source, "language": "en-US"}
    logger.debug(f"  ➜ TMDb: 正在通过 {source} '{external_id}' 查找人物...")
    try:
        proxies = config_manager.get_proxies_for_requests()
        response = get_tmdb_session().get(api_url, params=params, timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        person_results = data.get("person_results", [])
        if not person_results:
            logger.debug(f"  ➜ 未能通过 {source} '{external_id}' 找到任何人物。")
            return None

        person_found = person_results[0]
        tmdb_name = person_found.get('name')
        logger.debug(f"  ➜ 查找成功: 找到了 '{tmdb_name}' (TMDb ID: {person_found.get('id')})")

        if names_for_verification:
            # 1. 标准化 TMDb 返回的英文名
            normalized_tmdb_name = normalize_name_for_matching(tmdb_name)
            
            # 2. 获取我们期望的外文名 (通常来自豆瓣的 OriginalName)
            expected_original_name = names_for_verification.get("original_name")
            
            # 3. 只有在期望的外文名存在时，才进行验证
            if expected_original_name:
                normalized_expected_name = normalize_name_for_matching(expected_original_name)
                
                # 4. 进行精确比较
                if normalized_tmdb_name == normalized_expected_name:
                    logger.debug(f"  ➜ [验证成功 - 精确匹配] TMDb name '{tmdb_name}' 与期望的 original_name '{expected_original_name}' 匹配。")
                else:
                    # 如果不匹配，检查一下姓和名颠倒的情况
                    parts = expected_original_name.split()
                    if len(parts) > 1:
                        reversed_name = " ".join(reversed(parts))
                        if normalize_name_for_matching(reversed_name) == normalized_tmdb_name:
                            logger.debug(f"  ➜ [验证成功 - 精确匹配] 名字为颠倒顺序匹配。")
                            return person_found # 颠倒匹配也算成功

                    # 如果精确匹配和颠倒匹配都失败，则拒绝
                    logger.error(f"  ➜ [验证失败] TMDb返回的名字 '{tmdb_name}' 与期望的 '{expected_original_name}' 不符。拒绝此结果！")
                    return None
            else:
                # 如果豆瓣没有提供外文名，我们无法进行精确验证，可以选择信任或拒绝
                # 当前选择信任，但打印一条警告
                logger.warning(f"  ➜ [验证跳过] 未提供用于精确匹配的 original_name，将直接接受TMDb结果。")
        
        return person_found

    except requests.exceptions.RequestException as e:
        logger.error(f"TMDb: 通过外部ID查找时发生网络错误: {e}")
        return None

# --- 获取合集的详细信息 ---
def get_collection_details(collection_id: int, api_key: str, skip_fallback: bool = False) -> Optional[Dict[str, Any]]:
    """
    【V3 - 极致性能版】获取指定 TMDb 合集的详细信息。
    增加 skip_fallback 参数，允许调用方在不需要简介时跳过英文兜底请求。
    """
    if not collection_id or not api_key:
        return None
        
    endpoint = f"/collection/{collection_id}"
    params = {"language": DEFAULT_LANGUAGE}
    
    logger.debug(f"  ➜ TMDb API: 获取合集详情 (ID: {collection_id})...")
    data_zh = _tmdb_request(endpoint, api_key, params)
    
    if not data_zh:
        return None

    # ★ 核心优化：如果调用方明确表示不需要兜底，直接返回
    if skip_fallback:
        return data_zh

    # 检查简介是否缺失，如果缺失则请求英文兜底
    overview = data_zh.get("overview", "")
    if not overview or len(overview) < 2:
        if DEFAULT_LANGUAGE.startswith("zh"):
            logger.debug(f"    ➜ 合集 (ID: {collection_id}) 缺失中文简介，正在请求英文版补全...")
            try:
                params_en = {"language": "en-US"}
                data_en = _tmdb_request(endpoint, api_key, params_en)
                
                if data_en:
                    en_overview = data_en.get("overview")
                    if en_overview:
                        data_zh["overview"] = en_overview
                        logger.debug(f"    ➜ 成功补全合集英文简介源文本。")
                    
                    if not data_zh.get("name") and data_en.get("name"):
                        data_zh["name"] = data_en.get("name")
                        
            except Exception as e:
                logger.warning(f"    ➜ 补全合集英文简介失败: {e}")

    return data_zh

# --- 搜索媒体 ---
def search_media(query: str, api_key: str, item_type: str = 'movie', year: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    【V3 - 年份感知版】通过名字在 TMDb 上搜索媒体（电影、电视剧、演员），支持年份筛选。
    """
    if not query or not api_key:
        return None
    
    # 根据 item_type 决定 API 的端点
    endpoint_map = {
        'movie': '/search/movie',
        'tv': '/search/tv',
        'series': '/search/tv', # series 是 tv 的别名
        'person': '/search/person'
    }
    endpoint = endpoint_map.get(item_type.lower())
    
    if not endpoint:
        logger.error(f"不支持的搜索类型: '{item_type}'")
        return None

    params = {
        "query": query,
        "include_adult": "true", # 电影搜索通常需要包含成人内容
        "language": DEFAULT_LANGUAGE
    }
    
    # 新增：如果提供了年份，则添加到请求参数中
    if year:
        item_type_lower = item_type.lower()
        if item_type_lower == 'movie':
            params['year'] = year
        elif item_type_lower in ['tv', 'series']:
            params['first_air_date_year'] = year

    year_info = f" (年份: {year})" if year else ""
    logger.debug(f"  ➜ TMDb: 正在搜索 {item_type}: '{query}'{year_info}")
    data = _tmdb_request(endpoint, api_key, params)
    
    # 如果中文搜索不到，可以尝试用英文再搜一次
    if data and not data.get("results") and params['language'].startswith("zh"):
        logger.debug(f"  ➜ TMDb: 中文搜索 '{query}'{year_info} 未找到结果，尝试使用英文再次搜索...")
        params['language'] = 'en-US'
        data = _tmdb_request(endpoint, api_key, params)

    return data.get("results") if data else None


# --- 多类型搜索媒体（Telegram 交互搜索使用） ---
def search_multi_media(query: str, api_key: str, page: int = 1) -> Optional[Dict[str, Any]]:
    """
    通过 TMDb /search/multi 搜索电影和电视剧，返回完整响应对象。
    仅保留 movie / tv 两类结果，过滤 person 等不适合转存的结果。
    """
    if not query or not api_key:
        return None

    endpoint = "/search/multi"
    params = {
        "query": query,
        "include_adult": "true",
        "language": DEFAULT_LANGUAGE,
        "page": page,
    }

    logger.debug(f"  ➜ TMDb: 正在多类型搜索: '{query}' at page {page}")
    data = _tmdb_request(endpoint, api_key, params)

    if data and not data.get("results") and params["language"].startswith("zh"):
        logger.debug(f"  ➜ TMDb: 中文多类型搜索 '{query}' 未找到结果，尝试英文再次搜索...")
        params["language"] = "en-US"
        data = _tmdb_request(endpoint, api_key, params)

    if data:
        results = data.get("results") or []
        data["results"] = [
            item for item in results
            if item.get("media_type") in {"movie", "tv"}
        ]

    return data

# --- 搜索媒体 (为探索页面定制) ---
def search_media_for_discover(query: str, api_key: str, item_type: str = 'movie', year: Optional[str] = None, page: int = 1) -> Optional[Dict[str, Any]]:
    """
    【新】为探索页面的搜索功能定制，返回完整的TMDb响应对象。
    """
    if not query or not api_key:
        return None
    
    endpoint_map = {
        'movie': '/search/movie',
        'tv': '/search/tv',
        'series': '/search/tv',
        'person': '/search/person'
    }
    endpoint = endpoint_map.get(item_type.lower())
    
    if not endpoint:
        logger.error(f"不支持的搜索类型: '{item_type}'")
        return None

    params = {
        "query": query,
        "include_adult": "true",
        "language": DEFAULT_LANGUAGE,
        "page": page
    }
    
    if year:
        if item_type.lower() == 'movie':
            params['year'] = year
        elif item_type.lower() in ['tv', 'series']:
            params['first_air_date_year'] = year

    year_info = f" (年份: {year})" if year else ""
    logger.debug(f"  ➜ TMDb: 正在搜索 {item_type}: '{query}'{year_info} at page {page}")
    data = _tmdb_request(endpoint, api_key, params)
    
    if data and not data.get("results") and params['language'].startswith("zh"):
        logger.debug(f"  ➜ TMDb: 中文搜索 '{query}'{year_info} 未找到结果，尝试使用英文再次搜索...")
        params['language'] = 'en-US'
        data = _tmdb_request(endpoint, api_key, params)

    return data

# --- 搜索电视剧 ---
def search_tv_shows(query: str, api_key: str, year: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    【新增】通过名字在 TMDb 上搜索电视剧。
    这是 search_media 的一个便捷封装。
    """
    return search_media(query=query, api_key=api_key, item_type='tv', year=year)

# --- 搜索演员 ---
def search_person_tmdb(query: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    【新】通过名字在 TMDb 上搜索演员。
    """
    if not query or not api_key:
        return None
    endpoint = "/search/person"
    # 我们可以添加一些参数来优化搜索，比如只搜索非成人内容，并优先中文结果
    params = {
        "query": query,
        "include_adult": "false",
        "language": DEFAULT_LANGUAGE # 使用模块内定义的默认语言
    }
    logger.debug(f"  ➜ TMDb: 正在搜索演员: '{query}'")
    data = _tmdb_request(endpoint, api_key, params)
    return data.get("results") if data else None

# --- 获取演员详情 ---
def get_person_details_tmdb(person_id: int, api_key: str, append_to_response: Optional[str] = "movie_credits,tv_credits,images,external_ids,translations") -> Optional[Dict[str, Any]]:
    endpoint = f"/person/{person_id}"
    params = {
        "language": DEFAULT_LANGUAGE,
        "append_to_response": append_to_response
    }
    details = _tmdb_request(endpoint, api_key, params)

    # 尝试补充英文名，如果主语言是中文且original_name不是英文 (TMDb人物的original_name通常是其母语名)
    if details and details.get("name") != details.get("original_name") and DEFAULT_LANGUAGE.startswith("zh"):
        # 检查 translations 是否包含英文名
        if "translations" in (append_to_response or "") and details.get("translations", {}).get("translations"):
            for trans in details["translations"]["translations"]:
                if trans.get("iso_639_1") == "en" and trans.get("data", {}).get("name"):
                    details["english_name_from_translations"] = trans["data"]["name"]
                    logger.trace(f"  从translations补充人物英文名: {details['english_name_from_translations']}")
                    break
        # 如果 original_name 本身是英文，也可以用 (需要判断 original_name 的语言，较复杂)
        # 简单处理：如果 original_name 和 name 不同，且 name 是中文，可以认为 original_name 可能是外文名
        if details.get("original_name") and not contains_chinese(details.get("original_name", "")): # 假设 contains_chinese 在这里可用
             details["foreign_name_from_original"] = details.get("original_name")


    return details

# --- 获取演员的所有影视作品 ---
def get_person_credits_tmdb(person_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    【新】获取一个演员参与的所有电影和电视剧作品。
    使用 append_to_response 来一次性获取 movie_credits 和 tv_credits。
    """
    if not person_id or not api_key:
        return None
    
    endpoint = f"/person/{person_id}"
    # ★★★ 关键：一次请求同时获取电影和电视剧作品 ★★★
    params = {
        "append_to_response": "movie_credits,tv_credits"
    }
    logger.trace(f"TMDb: 正在获取演员 (ID: {person_id}) 的所有作品...")
    
    # 这里我们直接调用 get_person_details_tmdb，因为它内部已经包含了 _tmdb_request 的逻辑
    # 并且我们不需要它的其他附加信息，所以第三个参数传我们自己的 append_to_response
    details = get_person_details_tmdb(person_id, api_key, append_to_response="movie_credits,tv_credits")

    return details

# --- 通过 IMDb ID 获取 TMDb ID ---
def get_tmdb_id_by_imdb_id(imdb_id: str, api_key: str, media_type: str) -> Optional[int]:
    """
    通过 TMDb API v3 /find/{imdb_id} 方式获取TMDb ID。
    media_type: 'movie' 或 'tv'
    """
    tmdb_base_url = get_tmdb_api_base_url()
    url = f"{tmdb_base_url}/find/{imdb_id}"
    params = {
        "api_key": api_key,
        "external_source": "imdb_id"
    }
    
    try:
        # ➜ 修复：获取代理配置
        proxies = config_manager.get_proxies_for_requests()
        # ➜ 修复：使用全局 session (带重试功能) 并传入 proxies
        resp = get_tmdb_session().get(url, params=params, proxies=proxies, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            if media_type.lower() == 'movie' and data.get('movie_results'):
                return data['movie_results'][0].get('id')
            elif media_type.lower() in ['series', 'tv']:
                if data.get('tv_results'):
                    return data['tv_results'][0].get('id')
    except Exception as e:
        logger.error(f"通过 IMDb ID 获取 TMDb ID 失败: {e}")
        
    return None

# --- 获取片单的详细信息 ---
def get_list_details_tmdb(list_id: int, api_key: str, page: int = 1) -> Optional[Dict[str, Any]]:
    """
    【新】获取指定 TMDb 片单的详细信息，支持分页。
    """
    if not list_id or not api_key:
        return None
        
    endpoint = f"/list/{list_id}"
    params = {
        "language": DEFAULT_LANGUAGE,
        "page": page
    }
    
    logger.debug(f"TMDb: 获取片单详情 (ID: {list_id}, Page: {page})")
    return _tmdb_request(endpoint, api_key, params)

# --- 探索电影 ---
def discover_movie_tmdb(api_key: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """ 通过筛选条件发现电影。"""
    if not api_key:
        return None
    endpoint = "/discover/movie"
    logger.debug(f"  ➜ TMDb: 发现电影 (条件: {params})")
    return _tmdb_request(endpoint, api_key, params, use_default_language=True)

# --- 探索电视剧 ---
def discover_tv_tmdb(api_key: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """ 通过筛选条件发现电视剧。"""
    if not api_key:
        return None
    endpoint = "/discover/tv"
    logger.debug(f"  ➜ TMDb: 发现电视剧 (条件: {params})")
    return _tmdb_request(endpoint, api_key, params, use_default_language=True)

# --- 获取电影类型列表 ---
def get_movie_genres_tmdb(api_key: str) -> Optional[List[Dict[str, Any]]]:
    """【新】获取TMDb所有电影类型的官方列表。"""
    endpoint = "/genre/movie/list"
    data = _tmdb_request(endpoint, api_key, {"language": DEFAULT_LANGUAGE})
    return data.get("genres") if data else None

# --- 获取电视剧类型列表 ---
def get_tv_genres_tmdb(api_key: str) -> Optional[List[Dict[str, Any]]]:
    """【新】获取TMDb所有电视剧类型的官方列表。"""
    endpoint = "/genre/tv/list"
    data = _tmdb_request(endpoint, api_key, {"language": DEFAULT_LANGUAGE})
    return data.get("genres") if data else None

# --- 搜索 TMDb 电影公司 ---
def search_companies_tmdb(api_key: str, query: str) -> Optional[List[Dict[str, Any]]]:
    """【新】根据文本搜索TMDb电影公司，返回ID和名称。"""
    endpoint = "/search/company"
    params = {"query": query}
    data = _tmdb_request(endpoint, api_key, params)
    return data.get("results") if data else None


# --- 获取 TMDb 公司详情 ---
def get_company_details_tmdb(company_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    根据 TMDb company_id 获取制作公司详情，用于读取 logo_path。
    """
    if not company_id or not api_key:
        return None

    try:
        cid = int(company_id)
    except Exception:
        logger.warning(f"TMDb Company ID 非法: {company_id}")
        return None

    endpoint = f"/company/{cid}"
    logger.debug(f"  ➜ TMDb: 获取公司详情 (ID: {cid})")
    return _tmdb_request(endpoint, api_key, {"language": DEFAULT_LANGUAGE}, use_default_language=True)


# --- 获取 TMDb 电视网详情 ---
def get_network_details_tmdb(network_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    根据 TMDb network_id 获取电视网 / 播出平台详情，用于读取 logo_path。
    """
    if not network_id or not api_key:
        return None

    try:
        nid = int(network_id)
    except Exception:
        logger.warning(f"TMDb Network ID 非法: {network_id}")
        return None

    endpoint = f"/network/{nid}"
    logger.debug(f"  ➜ TMDb: 获取电视网详情 (ID: {nid})")
    return _tmdb_request(endpoint, api_key, {"language": DEFAULT_LANGUAGE}, use_default_language=True)


# --- 构造 TMDb 图片地址 ---
def build_tmdb_image_url(file_path: str, size: str = "original") -> Optional[str]:
    """
    把 TMDb 返回的 /xxx.png 形式 logo_path 转成可下载 URL。
    """
    if not file_path:
        return None
    if str(file_path).startswith("http://") or str(file_path).startswith("https://"):
        return str(file_path)
    return f"https://image.tmdb.org/t/p/{size}{file_path}"


# --- 探索 TMDb 热门电影 ---
def get_popular_movies_tmdb(api_key: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    获取 TMDb 上的热门电影列表，支持分页等参数。
    这是“每日推荐”功能的核心数据源。
    """
    if not api_key:
        return None
    endpoint = "/movie/popular"
    logger.debug(f"TMDb: 获取热门电影 (参数: {params})")
    return _tmdb_request(endpoint, api_key, params, use_default_language=True)

# --- 搜索电视剧，返回完整响应 ---
def search_tv_tmdb(api_key: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    搜索电视剧，返回完整响应（包含 results 列表）。
    用于映射管理中“搜代表剧集”功能。
    """
    query = params.get('query')
    if not query:
        return None
    # 复用现有的 search_media_for_discover，它返回完整的 dict
    return search_media_for_discover(query=query, api_key=api_key, item_type='tv')
