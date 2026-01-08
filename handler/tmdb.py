# handler/tmdb.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import concurrent.futures
from utils import contains_chinese, normalize_name_for_matching
from typing import Optional, List, Dict, Any, Callable
import logging
import config_manager
import constants
import threading
logger = logging.getLogger(__name__)

# ★★★ 自定义的重试类，用于输出更友好的日志 ★★★
class LoggedRetry(Retry):
    """
    一个继承自 urllib3.Retry 的自定义类，
    用于在每次重试时记录一条更清晰、更友好的日志消息。
    """
    def increment(self, method, url, response=None, error=None, _pool=None, _stacktrace=None):
        # 首先，调用父类的 increment 方法。
        # 如果不应该重试了（例如，达到最大次数），它会抛出异常，
        # 这样我们的日志代码就不会执行。
        new_retry = super().increment(method, url, response, error, _pool, _stacktrace)

        # 如果代码能执行到这里，说明即将进行一次重试。
        
        # 确定失败原因
        if response:
            reason = f"不成功的状态码: {response.status}"
        elif error:
            reason = f"连接错误: {error.__class__.__name__}"
        else:
            reason = "未知错误"

        # 获取下一次重试的等待时间
        backoff_time = self.get_backoff_time()
        # 计算当前是第几次重试
        attempt_number = len(self.history) + 1
        
        # 记录一条警告级别的日志，这样既能引起注意又不会像错误一样吓人
        logger.warning(
            f"  ➜ TMDb API 请求失败 ({reason})。将在 {backoff_time:.2f} 秒后重试... (第 {attempt_number}/{self.total} 次)"
        )

        return new_retry


# ★★★ 创建带重试功能的 Session (已修改为使用 LoggedRetry) ★★★
def requests_retry_session(
    retries=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    session=None,
):
    """创建一个配置了重试策略的 requests.Session 对象"""
    session = session or requests.Session()
    retry = LoggedRetry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['HEAD', 'GET', 'PUT', 'DELETE', 'OPTIONS', 'TRACE', 'POST']),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# 创建一个全局的、可复用的、带重试功能的 session 实例
# 整个程序将通过这个实例来请求 TMDB API
tmdb_session = requests_retry_session()

def get_tmdb_api_base_url() -> str:
    """
    从配置管理器获取TMDb API基础URL，如果未配置则使用默认值
    """
    return config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_BASE_URL, "https://api.themoviedb.org/3")

# 默认语言设置
DEFAULT_LANGUAGE = "zh-CN"
DEFAULT_REGION = "CN"


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
        response = tmdb_session.get(full_url, params=base_params, timeout=15, proxies=proxies)
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
        logger.error(f"  ➜ 所有重试后 TMDb API HTTP 出现错误: {e.response.status_code} - {error_details}. URL: {full_url}", exc_info=False)
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"  ➜ 所有重试后 TMDb API 请求均出现错误: {e}. URL: {full_url}", exc_info=False)
        return None
    except json.JSONDecodeError as e:
        logger.error(f"  ➜ TMDb API JSON 解码错误: {e}. URL: {full_url}. Response: {response.text[:200] if response else 'N/A'}", exc_info=False)
        return None
# --- 获取电影的详细信息 ---
def get_movie_details(movie_id: int, api_key: str, append_to_response: Optional[str] = "credits,videos,images,keywords,external_ids,translations,release_dates") -> Optional[Dict[str, Any]]:
    """
    【新增】获取电影的详细信息。
    """
    endpoint = f"/movie/{movie_id}"
    params = {
        "language": DEFAULT_LANGUAGE,
        "append_to_response": append_to_response or "",
        "include_image_language": "zh,en,null"
    }
    logger.trace(f"TMDb: 获取电影详情 (ID: {movie_id})")
    details = _tmdb_request(endpoint, api_key, params)
    
    # 同样为电影补充英文标题，保持逻辑一致性
    if details and details.get("original_language") != "en" and DEFAULT_LANGUAGE.startswith("zh"):
        # 优先从 translations 获取
        if "translations" in (append_to_response or "") and details.get("translations", {}).get("translations"):
            for trans in details["translations"]["translations"]:
                if trans.get("iso_639_1") == "en" and trans.get("data", {}).get("title"):
                    details["english_title"] = trans["data"]["title"]
                    logger.trace(f"  从translations补充电影英文名: {details['english_title']}")
                    break
        # 如果没有，再单独请求一次英文版
        if not details.get("english_title"):
            logger.trace(f"  尝试获取电影 {movie_id} 的英文名...")
            en_params = {"language": "en-US"}
            en_details = _tmdb_request(f"/movie/{movie_id}", api_key, en_params)
            if en_details and en_details.get("title"):
                details["english_title"] = en_details.get("title")
                logger.trace(f"  通过请求英文版补充电影英文名: {details['english_title']}")
    elif details and details.get("original_language") == "en":
        details["english_title"] = details.get("original_title")

    return details
# --- 获取电视剧的详细信息 ---
def get_tv_details(tv_id: int, api_key: str, append_to_response: Optional[str] = "credits,videos,images,keywords,external_ids,translations,content_ratings") -> Optional[Dict[str, Any]]:
    """
    【已升级】获取电视剧的详细信息。
    """
    endpoint = f"/tv/{tv_id}"
    params = {
        "language": DEFAULT_LANGUAGE,
        # ★★★ 确保 append_to_response 不为空，即使外部没传 ★★★
        "append_to_response": append_to_response or "",
        "include_image_language": "zh,en,null" 
    }
    logger.trace(f"TMDb: 获取电视剧详情 (ID: {tv_id})")
    details = _tmdb_request(endpoint, api_key, params)
    
    # 同样可以为剧集补充英文标题
    if details and details.get("original_language") != "en" and DEFAULT_LANGUAGE.startswith("zh"):
        if "translations" in (append_to_response or "") and details.get("translations", {}).get("translations"):
            for trans in details["translations"]["translations"]:
                if trans.get("iso_639_1") == "en" and trans.get("data", {}).get("name"):
                    details["english_name"] = trans["data"]["name"]
                    logger.trace(f"  从translations补充剧集英文名: {details['english_name']}")
                    break
        if not details.get("english_name"):
            logger.trace(f"  尝试获取剧集 {tv_id} 的英文名...")
            en_params = {"language": "en-US"}
            en_details = _tmdb_request(f"/tv/{tv_id}", api_key, en_params)
            if en_details and en_details.get("name"):
                details["english_name"] = en_details.get("name")
                logger.trace(f"  通过请求英文版补充剧集英文名: {details['english_name']}")
    elif details and details.get("original_language") == "en":
        details["english_name"] = details.get("original_name")

    return details
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
# --- 获取电视剧某一季的详细信息 ---
def get_season_details_tmdb(tv_id: int, season_number: int, api_key: str, append_to_response: Optional[str] = "credits", item_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    【已升级】获取电视剧某一季的详细信息，并支持 item_name 用于日志。
    """
    endpoint = f"/tv/{tv_id}/season/{season_number}"
    params = {
        "language": DEFAULT_LANGUAGE,
        "append_to_response": append_to_response
    }
    
    item_name_for_log = f"'{item_name}' " if item_name else ""
    logger.debug(f"  ➜ TMDb API: 获取电视剧 {item_name_for_log}(ID: {tv_id}) 第 {season_number} 季的详情...")
    
    return _tmdb_request(endpoint, api_key, params)
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
# --- 接收一个剧集 TMDB ID 列表，并发地获取所有这些剧集的完整子项（季、集）信息 ---
def batch_get_full_series_details_tmdb(
    series_tmdb_ids: List[str], 
    api_key: str, 
    max_workers: int = 5, # <-- 将默认并发数从10降低到5，更加安全
    progress_callback: Optional[Callable] = None
) -> Dict[str, List[Dict]]:
    """
    【V1.2 - 稳定防崩溃版】
    接收一个剧集 TMDB ID 列表，并发地获取所有这些剧集的完整子项（季、集）信息。
    通过降低默认并发数和增加请求间隔，防止触发TMDb的速率限制。
    """
    if not series_tmdb_ids or not api_key:
        return {}

    logger.info(f"  ➜ 开始为 {len(series_tmdb_ids)} 部剧集并发聚合 TMDB 子项数据 (并发数: {max_workers})...")
    
    all_children_data = {}
    completed_count = 0
    lock = threading.Lock()
    total_tasks = len(series_tmdb_ids)

    def _fetch_one_series_all_children(tv_id: str) -> Optional[List[Dict]]:
        """线程工作函数：获取单部剧集的所有子项"""
        try:
            series_details = get_tv_details(tv_id, api_key, append_to_response="seasons")
            if not series_details or not series_details.get("seasons"):
                logger.warning(f"    ➜ 无法获取剧集 {tv_id} 的季列表，跳过。")
                return None

            children = []
            for season_summary in series_details.get("seasons", []):
                season_num = season_summary.get("season_number")
                if season_num is None or season_num == 0:
                    continue
                
                children.append(season_summary)
                season_details = get_season_details_tmdb(tv_id, season_num, api_key)
                if season_details and season_details.get("episodes"):
                    children.extend(season_details["episodes"])
                
                # ★★★ 核心修复 2/2：在这里加入一个微小的延时 ★★★
                # 这会极大地增加程序的稳定性，避免被服务器拒绝连接。
                time.sleep(0.05) # 暂停 50 毫秒

            return children
        except Exception as e:
            logger.error(f"    ➜ 获取剧集 {tv_id} 的子项时出错: {e}", exc_info=False)
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {executor.submit(_fetch_one_series_all_children, tv_id): tv_id for tv_id in series_tmdb_ids}
        
        for future in concurrent.futures.as_completed(future_to_id):
            tv_id = future_to_id[future]
            try:
                result = future.result()
                if result:
                    all_children_data[tv_id] = result
            except Exception as exc:
                logger.error(f"    剧集 {tv_id} 的并发任务执行时产生异常: {exc}")
            finally:
                if progress_callback:
                    with lock:
                        completed_count += 1
                    
                    progress_percent = 15 + (completed_count / total_tasks) * 25
                    message = f"正在从TMDb并发获取... ({completed_count}/{total_tasks})"
                    progress_callback(int(progress_percent), message)

    logger.info(f"  ➜ 成功为 {len(all_children_data)} 部剧集获取了完整的子项元数据。")
    return all_children_data
# --- 并发获取剧集详情 ---
def aggregate_full_series_data_from_tmdb(
    tv_id: int,
    api_key: str,
    max_workers: int = 5  # ★★★ 并发数，可以从外部配置传入 ★★★
) -> Optional[Dict[str, Any]]:
    """
    【V1 - 并发聚合版】
    通过并发请求，从 TMDB API 高效地聚合一部剧集的完整元数据（剧集、所有季、所有集）。
    """
    if not tv_id or not api_key:
        return None

    logger.info(f"  ➜ 开始为剧集 ID {tv_id} 并发聚合 TMDB 数据 (并发数: {max_workers})...")
    
    # --- 步骤 1: 获取顶层剧集详情，这是所有后续操作的基础 ---
    series_details = get_tv_details(tv_id, api_key)
    if not series_details:
        logger.error(f"  ➜ 聚合失败：无法获取顶层剧集 {tv_id} 的详情。")
        return None
    
    logger.info(f"  ➜ 成功获取剧集 '{series_details.get('name')}' 的顶层信息，共 {len(series_details.get('seasons', []))} 季。")

    # --- 步骤 2: 构建所有需要并发执行的“任务” ---
    tasks = []
    # 添加获取每一季详情的任务
    for season in series_details.get("seasons", []):
        season_number = season.get("season_number")
        if season_number is not None:
            # (任务类型, tv_id, season_number)
            tasks.append(("season", tv_id, season_number))
            
            # 添加获取该季下每一集详情的任务
            episode_count = season.get("episode_count", 0)
            for episode_number in range(1, episode_count + 1):
                # (任务类型, tv_id, season_number, episode_number)
                tasks.append(("episode", tv_id, season_number, episode_number))

    if not tasks:
        logger.warning("  ➜ 未找到任何季或集需要获取，聚合结束。")
        return {"series_details": series_details, "seasons_details": {}, "episodes_details": {}}

    logger.info(f"  ➜ 共构建了 {len(tasks)} 个并发任务 (获取所有季和集的详情)。")

    # --- 步骤 3: 使用线程池并发执行所有任务 ---
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 使用字典来映射 future 和它的任务描述，方便后续处理
        future_to_task = {}
        for task in tasks:
            if task[0] == "season":
                _, tvid, s_num = task
                future = executor.submit(get_season_details_tmdb, tvid, s_num, api_key)
                future_to_task[future] = f"S{s_num}"
            elif task[0] == "episode":
                _, tvid, s_num, e_num = task
                future = executor.submit(get_episode_details_tmdb, tvid, s_num, e_num, api_key)
                future_to_task[future] = f"S{s_num}E{e_num}"

        # 收集结果
        for i, future in enumerate(concurrent.futures.as_completed(future_to_task)):
            task_key = future_to_task[future]
            try:
                result_data = future.result()
                if result_data:
                    results[task_key] = result_data
                logger.trace(f"    ({i+1}/{len(tasks)}) 任务 {task_key} 完成。")
            except Exception as exc:
                logger.error(f"    任务 {task_key} 执行时产生错误: {exc}")

    # --- 步骤 4: 将所有结果智能地聚合到一个大字典中 ---
    final_aggregated_data = {
        "series_details": series_details,
        "seasons_details": {}, # 临时使用字典，方便按季号归位
        "episodes_details": {} # key 是 "S1E1", "S1E2", ...
    }

    for key, data in results.items():
        if key.startswith("S") and "E" not in key: # 是季
            season_num = int(key[1:])
            final_aggregated_data["seasons_details"][season_num] = data
        elif key.startswith("S") and "E" in key: # 是集
            final_aggregated_data["episodes_details"][key] = data
            
    logger.info(f"  ➜ 成功获取 {len(final_aggregated_data['seasons_details'])} 季和 {len(final_aggregated_data['episodes_details'])} 集的详情。")
    seasons_list = list(final_aggregated_data["seasons_details"].values())
    # 为了美观和逻辑顺畅，按季号排序
    seasons_list.sort(key=lambda x: x.get("season_number", 0))
    
    # 覆盖回字典
    final_aggregated_data["seasons_details"] = seasons_list
    return final_aggregated_data
# +++ 获取集详情 +++
def get_episode_details_tmdb(tv_id: int, season_number: int, episode_number: int, api_key: str, append_to_response: Optional[str] = "credits,videos,images,external_ids") -> Optional[Dict[str, Any]]:
    """
    获取电视剧某一集的详细信息。
    """
    endpoint = f"/tv/{tv_id}/season/{season_number}/episode/{episode_number}"
    params = {
        "language": DEFAULT_LANGUAGE,
        "append_to_response": append_to_response
    }
    logger.trace(f"  ➜ TMDb API: 获取电视剧 (ID: {tv_id}) S{season_number}E{episode_number} 的详情...")
    return _tmdb_request(endpoint, api_key, params)
def get_tv_episode_details(tv_id: int, season_number: int, episode_number: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    获取电视剧某一集的详细信息。
    这是 get_episode_details_tmdb 的一个更简洁的别名，用于简化调用。
    """
    return get_episode_details_tmdb(
        tv_id=tv_id,
        season_number=season_number,
        episode_number=episode_number,
        api_key=api_key,
        append_to_response=None # 仅获取基础信息，不需要 credits 等额外数据，提高速度
    )
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
        response = tmdb_session.get(api_url, params=params, timeout=15, proxies=proxies)
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
def get_collection_details(collection_id: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    【新】获取指定 TMDb 合集的详细信息，包含其所有影片部分。
    """
    if not collection_id or not api_key:
        return None
        
    endpoint = f"/collection/{collection_id}"
    params = {"language": DEFAULT_LANGUAGE}
    
    logger.debug(f"TMDb: 获取合集详情 (ID: {collection_id})")
    return _tmdb_request(endpoint, api_key, params)
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
    logger.debug(f"TMDb: 正在搜索 {item_type}: '{query}'{year_info}")
    data = _tmdb_request(endpoint, api_key, params)
    
    # 如果中文搜索不到，可以尝试用英文再搜一次
    if data and not data.get("results") and params['language'].startswith("zh"):
        logger.debug(f"中文搜索 '{query}'{year_info} 未找到结果，尝试使用英文再次搜索...")
        params['language'] = 'en-US'
        data = _tmdb_request(endpoint, api_key, params)

    return data.get("results") if data else None
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
    logger.debug(f"TMDb: 正在搜索 {item_type}: '{query}'{year_info} at page {page}")
    data = _tmdb_request(endpoint, api_key, params)
    
    if data and not data.get("results") and params['language'].startswith("zh"):
        logger.debug(f"中文搜索 '{query}'{year_info} 未找到结果，尝试使用英文再次搜索...")
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
    logger.debug(f"TMDb: 正在搜索演员: '{query}'")
    data = _tmdb_request(endpoint, api_key, params)
    return data.get("results") if data else None
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

# --- 通过 TMDb API v3 /find/{imdb_id} 方式获取TMDb ID ---
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
        # ✅ 修复：获取代理配置
        proxies = config_manager.get_proxies_for_requests()
        # ✅ 修复：使用全局 session (带重试功能) 并传入 proxies
        resp = tmdb_session.get(url, params=params, proxies=proxies, timeout=15)
        
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
    logger.debug(f"TMDb: 发现电影 (条件: {params})")
    return _tmdb_request(endpoint, api_key, params, use_default_language=True)

# --- 探索电视剧 ---
def discover_tv_tmdb(api_key: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """ 通过筛选条件发现电视剧。"""
    if not api_key:
        return None
    endpoint = "/discover/tv"
    logger.debug(f"TMDb: 发现电视剧 (条件: {params})")
    return _tmdb_request(endpoint, api_key, params, use_default_language=True)

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