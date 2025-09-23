# tasks/helpers.py
# 跨模块共享的辅助函数

import os
from typing import Optional, Dict

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