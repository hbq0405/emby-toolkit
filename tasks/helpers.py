# tasks/helpers.py
# 跨模块共享的辅助函数

import os
from typing import Optional, Dict, Any
import logging
from datetime import datetime, timedelta

# 假设您的项目结构允许这样导入，如果 helpers.py 和 tmdb.py 在不同的父文件夹下
# (例如 tasks/ 和 handler/)，您可能需要调整 Python 的 sys.path 或使用相对导入
# 例如: from ..handler.tmdb import get_movie_details
from handler.tmdb import get_movie_details
import constants

logger = logging.getLogger(__name__)


# --- 从文件名和视频流信息中提取并标准化特效标签，支持杜比视界Profile ---
def _get_standardized_effect(path_lower: str, video_stream: Optional[Dict]) -> str:
    """
    【V9 - 全局·智能文件名识别增强版】
    - 这是一个全局函数，可被项目中所有需要特效识别的地方共享调用。
    - 增强了文件名识别逻辑：当文件名同时包含 "dovi" 和 "hdr" 时，智能判断为 davi_p8。
    - 调整了判断顺序，确保更精确的规则优先执行。
    """
    
    # 1. 优先从文件名判断 (逻辑增强)
    if ("dovi" in path_lower or "dolbyvision" in path_lower or "dv" in path_lower) and "hdr" in path_lower:
        return "dovi_p8"
    if any(s in path_lower for s in ["dovi p7", "dovi.p7", "dv.p7", "profile 7", "profile7"]):
        return "dovi_p7"
    if any(s in path_lower for s in ["dovi p5", "dovi.p5", "dv.p5", "profile 5", "profile5"]):
        return "dovi_p5"
    if ("dovi" in path_lower or "dolbyvision" in path_lower) and "hdr" in path_lower:
        return "dovi_p8"
    if "dovi" in path_lower or "dolbyvision" in path_lower:
        return "dovi_other"
    if "hdr10+" in path_lower or "hdr10plus" in path_lower:
        return "hdr10+"
    if "hdr" in path_lower:
        return "hdr"

    # 2. 如果文件名没有信息，再对视频流进行精确分析
    if video_stream and isinstance(video_stream, dict):
        all_stream_info = []
        for key, value in video_stream.items():
            all_stream_info.append(str(key).lower())
            if isinstance(value, str):
                all_stream_info.append(value.lower())
        combined_info = " ".join(all_stream_info)

        if "doviprofile81" in combined_info: return "dovi_p8"
        if "doviprofile76" in combined_info: return "dovi_p7"
        if "doviprofile5" in combined_info: return "dovi_p5"
        if any(s in combined_info for s in ["dvhe.08", "dvh1.08"]): return "dovi_p8"
        if any(s in combined_info for s in ["dvhe.07", "dvh1.07"]): return "dovi_p7"
        if any(s in combined_info for s in ["dvhe.05", "dvh1.05"]): return "dovi_p5"
        if "dovi" in combined_info or "dolby" in combined_info or "dolbyvision" in combined_info: return "dovi_other"
        if "hdr10+" in combined_info or "hdr10plus" in combined_info: return "hdr10+"
        if "hdr" in combined_info: return "hdr"

    # 3. 默认是SDR
    return "sdr"

# ★★★ 智能从文件名提取质量标签的辅助函数 ★★★
def _extract_quality_tag_from_filename(filename_lower: str, video_stream: dict) -> str:
    """
    根据预定义的优先级，从文件名中提取最高级的质量标签。
    如果找不到任何标签，则回退到使用视频编码作为备用方案。
    """
    # 定义质量标签的优先级，越靠前越高级
    QUALITY_HIERARCHY = [
        'remux',
        'bluray',
        'blu-ray', # 兼容写法
        'web-dl',
        'webdl',   # 兼容写法
        'webrip',
        'hdtv',
        'dvdrip'
    ]
    
    for tag in QUALITY_HIERARCHY:
        # 为了更精确匹配，我们检查被点、空格或短横线包围的标签
        if f".{tag}." in filename_lower or f" {tag} " in filename_lower or f"-{tag}-" in filename_lower:
            # 返回大写的、更美观的标签
            return tag.replace('-', '').upper()

    # 如果循环结束都没找到，提供一个备用值
    return (video_stream.get('Codec', '未知') if video_stream else '未知').upper()

# +++ 判断电影是否满足订阅条件 +++
def is_movie_subscribable(movie_id: int, api_key: str, config: dict) -> tuple[bool, Optional[dict]]:
    """
    【V2 - 返回详情版】
    检查一部电影是否适合订阅。
    返回一个元组 (can_subscribe, movie_details)，其中 movie_details 可能是 None。
    """
    if not api_key:
        logger.error("TMDb API Key 未提供，无法检查电影是否可订阅。")
        return False, None

    delay_days = config.get(constants.CONFIG_OPTION_MOVIE_SUBSCRIPTION_DELAY_DAYS, 30)
    logger.debug(f"检查电影 (ID: {movie_id}) 是否适合订阅 (延迟天数: {delay_days})...")

    details = get_movie_details(
        movie_id=movie_id,
        api_key=api_key,
        append_to_response="release_dates"
    )

    log_identifier = f"《{details.get('title')}》" if details and details.get('title') else f"(ID: {movie_id})"

    if not details:
        logger.warning(f"无法获取电影 {log_identifier} 的详情，默认其不适合订阅。")
        return False, None

    release_info = details.get("release_dates", {}).get("results", [])
    if not release_info:
        logger.warning(f"电影 {log_identifier} 未找到任何地区的发行日期信息，默认其不适合订阅。")
        # ★★★ 核心修改 1: 即使失败，也返回已获取的详情 ★★★
        return False, details

    earliest_theatrical_date = None
    today = datetime.now().date()

    for country_releases in release_info:
        for release in country_releases.get("release_dates", []):
            if release.get("type") == 4:
                logger.info(f"  ➜ 成功: 电影 {log_identifier} 已有数字版发行记录，适合订阅。")
                # ★★★ 核心修改 2: 成功时，也返回详情 ★★★
                return True, details
            if release.get("type") == 3:
                try:
                    release_date_str = release.get("release_date", "").split("T")[0]
                    if release_date_str:
                        current_release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
                        if earliest_theatrical_date is None or current_release_date < earliest_theatrical_date:
                            earliest_theatrical_date = current_release_date
                except (ValueError, TypeError):
                    logger.warning(f"解析电影 {log_identifier} 的上映日期 '{release.get('release_date')}' 时出错。")
                    continue

    if earliest_theatrical_date:
        days_since_release = (today - earliest_theatrical_date).days
        if days_since_release >= delay_days:
            logger.info(f"  ➜ 成功: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，已超过配置的 {delay_days} 天，适合订阅。")
            # ★★★ 核心修改 3: 成功时，也返回详情 ★★★
            return True, details
        else:
            logger.info(f"  ➜ 失败: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，未满配置的 {delay_days} 天，不适合订阅。")
            # ★★★ 核心修改 4: 失败时，也返回详情 ★★★
            return False, details

    logger.warning(f"电影 {log_identifier} 未找到数字版或任何有效的影院上映日期，默认其不适合订阅。")
    # ★★★ 核心修改 5: 最终失败时，也返回详情 ★★★
    return False, details