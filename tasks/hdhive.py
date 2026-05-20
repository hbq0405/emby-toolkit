# handler/hdhive.py
import re
import time
import logging
from handler.hdhive_client import HDHiveClient
from handler.p115_service import P115Service, SmartOrganizer, get_config
from handler.telegram import send_hdhive_checkin_notification
from database import settings_db
import task_manager
import constants
import config_manager

logger = logging.getLogger(__name__)

def task_download_from_hdhive(api_key, slug, tmdb_id, media_type, title):
    """
    核心任务：从影巢解锁 -> 转存 115 -> 搜索真实ID -> 精准整理
    """
    logger.info(f"=== ➜ 开始从影巢获取资源: {title} (TMDB: {tmdb_id}) ===")
    
    # api_key 参数保留兼容旧调用签名；新版影巢授权通过统一 Relay 完成。
    hdhive = HDHiveClient(api_key)
    unlock_data = hdhive.unlock_resource(slug)
    
    if not unlock_data:
        logger.error("  ➜ 影巢资源解锁失败，可能积分不足或资源已失效。")
        return False
        
    share_url = unlock_data.get("url") or ""
    full_url = unlock_data.get("full_url") or ""
    
    client = P115Service.get_client()
    if not client:
        logger.error("  ➜ 115 客户端未初始化，无法处理任务。")
        return False

    config = get_config()
    save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID) 
    if not save_cid or str(save_cid) == '0':
        logger.error("  ➜ 未配置 115 待整理目录，无法处理任务。")
        return False

    # ==========================================
    # ★ 分支 1：处理磁力链 / ED2K (提交离线下载)
    # ==========================================
    def _to_str(val):
        return "\n".join(val) if isinstance(val, list) else str(val or "")
        
    combined_url = _to_str(share_url) + "\n" + _to_str(full_url)
    magnet_ed2k_links = re.findall(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', combined_url, re.IGNORECASE)

    if magnet_ed2k_links:
        logger.info(f"  ➜ 检测到 {len(magnet_ed2k_links)} 个磁力/ED2K链接，准备批量提交离线下载...")
        
        # 1. 提交前：获取目标目录前 50 个文件的快照
        existing_fids = set()
        try:
            res_before = client.fs_files({'cid': save_cid, 'limit': 50})
            if res_before and res_before.get('data'):
                existing_fids = {str(item.get('fid') or item.get('file_id')) for item in res_before.get('data', [])}
        except Exception as e:
            logger.debug(f"  ➜ 获取目录快照失败: {e}")

        # 2. 批量构造 payload
        payload = {"wp_path_id": save_cid}
        for i, u in enumerate(magnet_ed2k_links):
            payload[f"url[{i}]"] = u
            
        res = client.offline_add_urls(payload)
        
        if res and res.get('state'):
            logger.info(f"=== 🎉 影巢磁力/ED2K 离线下载任务提交成功！(共 {len(magnet_ed2k_links)} 个任务) ===")
            
            # 3. 等待 5 秒钟让 115 服务器反应一下
            logger.info("  ➜ 等待 5 秒后检查是否已秒传...")
            time.sleep(5)
            
            # 4. 提交后：再次获取目录比对
            has_new_file = False
            try:
                res_after = client.fs_files({'cid': save_cid, 'limit': 50})
                if res_after and res_after.get('data'):
                    current_fids = {str(item.get('fid') or item.get('file_id')) for item in res_after.get('data', [])}
                    new_fids = current_fids - existing_fids
                    if new_fids:
                        has_new_file = True
                        logger.info(f"  ➜ 发现 {len(new_fids)} 个新文件/目录，离线秒传成功！")
            except Exception as e:
                logger.debug(f"  ➜ 检查新文件失败: {e}")

            # 5. 踢一脚整理任务
            if has_new_file:
                try:
                    import threading
                    threading.Timer(1.0, task_manager.trigger_115_organize_task).start()
                    logger.info("  ➜ 已成功唤醒 115 智能整理任务！")
                except Exception as e:
                    logger.error(f"  ➜ 唤醒整理任务失败: {e}")
            else:
                logger.info("  ➜ 暂未发现新文件，可能仍在缓慢下载中，将由系统定时任务兜底处理。")

            return True
        else:
            err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
            logger.error(f"  ➜ 离线下载提交失败: {err}")
            return False

    # ==========================================
    # ★ 分支 2：处理 115 官方分享链接 (秒传转存)
    # ==========================================
    access_code = unlock_data.get("access_code")
    
    # ★★★ 提取码兜底逻辑 ★★★
    if not access_code:
        pwd_match = re.search(r'(?:pwd|password|code)=([a-zA-Z0-9]+)', full_url + "&" + share_url, re.IGNORECASE)
        if pwd_match:
            access_code = pwd_match.group(1)
            
    access_code = access_code or ""
    
    # 提取分享码
    match = re.search(r'(?:115\.com|115cdn\.com|anxia\.com)/s/([a-zA-Z0-9]+)', combined_url)
    if not match:
        logger.error(f"  ➜ 无法从链接中提取 115 分享码，且不是磁力链接: {share_url} | {full_url}")
        return False
        
    share_code = match.group(1)
    logger.info(f"  ➜ 成功获取 115 分享码: {share_code}, 提取码: {access_code}")
    logger.info(f"  ➜ 正在转存到 115 待整理目录 (CID: {save_cid})...")
    try:
        import_res = client.share_import(share_code, access_code, save_cid)
        
        # ★ 1：兼容 state 为 True (布尔值) 或 1 (数字/字符串) 的情况
        if not import_res or not import_res.get('state'):
            logger.error(f"  ➜ 115 转存失败: {import_res.get('error_msg', import_res)}")
            return False
            
        receive_title = import_res.get('data', {}).get('receive_title')
        logger.info(f"  ➜ 115 转存成功！文件/目录名: {receive_title}")
        
        if not receive_title:
            logger.warning("  ➜ 转存成功但未返回文件名，交由全局定时扫描任务处理。")
            return True

        # ★ 新增：检查智能整理总开关，未开启则直接下班回家
        enable_organize = config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)
        if str(enable_organize).lower() != 'true' and enable_organize is not True:
            logger.info("  ➜ 智能整理功能未开启，转存任务结束！")
            return True

        # ★ 2：去待整理目录搜索刚刚转存的文件，获取真实的 file_id (增加重试逻辑)
        logger.info(f"  ➜ 正在定位转存文件，准备执行精准整理...")
        
        target_item = None
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            # 递增等待时间：2秒, 4秒, 6秒
            wait_time = attempt * 2
            logger.debug(f"  ➜ 等待 {wait_time} 秒后进行第 {attempt}/{max_retries} 次搜索...")
            time.sleep(wait_time)
            
            try:
                search_res = client.fs_files({'cid': save_cid, 'search_value': receive_title, 'limit': 10})
                saved_items = search_res.get('data', [])
                
                for item in saved_items:
                    name = item.get('fn') or item.get('file_name') or item.get('n')
                    if name == receive_title:
                        target_item = item
                        break
                        
                if target_item:
                    logger.info(f"  ➜ 第 {attempt} 次搜索成功定位到文件！")
                    break
                else:
                    logger.debug(f"  ➜ 第 {attempt} 次搜索未找到文件，115 索引可能尚未同步。")
            except Exception as e:
                logger.warning(f"  ➜ 第 {attempt} 次搜索请求发生异常: {e}")
                
        if not target_item:
            logger.warning(f"  ➜ 经过 {max_retries} 次重试，仍未能精准定位到文件 '{receive_title}'，将交由全局定时扫描任务处理。")
            return True

        # 构造 root_item 触发 SmartOrganizer (补全 pc, sha1, fs 等生成 STRM 必需的关键信息)
        root_item = {
            'fid': target_item.get('fid') or target_item.get('file_id'),
            'fn': receive_title,
            'fc': target_item.get('fc') if target_item.get('fc') is not None else target_item.get('file_category', '1'),
            'pid': save_cid,
            'pc': target_item.get('pc') or target_item.get('pick_code'),
            'sha1': target_item.get('sha1') or target_item.get('sha'),
            'fs': target_item.get('fs') or target_item.get('size')
        }
        
        organizer = SmartOrganizer(
            client=client, 
            tmdb_id=tmdb_id, 
            media_type=media_type, 
            original_title=title,
            use_ai=False 
        )
        
        target_cid = organizer.get_target_cid()
        organizer.execute(root_item, target_cid)
            
        logger.info("=== 🎉 影巢资源极速秒传并精准整理完毕！ ===")
        return True

    except Exception as e:
        logger.error(f"  ➜ 转存或整理过程中发生异常: {e}", exc_info=True)
        return False
    
def task_hdhive_auto_checkin(processor):
    """
    后台任务：影巢自动签到
    """
    logger.info("--- 开始执行影巢自动签到任务 ---")
    task_manager.update_status_from_thread(0, "正在读取影巢配置...")

    # 新版影巢使用统一 Relay OAuth 授权，不再依赖个人 API Key。
    client = HDHiveClient()
    if not client.ping():
        logger.info("  ➜ 影巢尚未完成授权，跳过自动签到。")
        task_manager.update_status_from_thread(100, "影巢未授权，跳过")
        return
    task_manager.update_status_from_thread(50, "正在发送签到请求...")

    try:
        # 1. 根据配置选择签到方式
        hdhive_config = settings_db.get_setting("hdhive_config") or {}
        checkin_mode = hdhive_config.get("checkin_mode") if isinstance(hdhive_config, dict) else "normal"
        is_gambler = checkin_mode == "gambler"
        logger.info(f"  ➜ 影巢自动签到方式: {'赌狗签到' if is_gambler else '普通签到'}")

        res = client.checkin(is_gambler=is_gambler)
        
        # 2. 签到完顺便拉取一下最新的用户信息，为了在通知里展示最新积分
        user_info = client.get_user_info() or {}

        if res.get("success"):
            res_data = res.get("data", {})
            real_message = res_data.get("message") or res.get("message", "签到请求成功")

            if res_data.get("checked_in") is False:
                logger.info(f"  ➜ 影巢签到: {real_message}")
                task_manager.update_status_from_thread(100, f"已签到: {real_message}")
            else:
                logger.info(f"  ➜ 影巢签到成功: {real_message}")
                task_manager.update_status_from_thread(100, f"签到成功: {real_message}")
        else:
            error_msg = res.get("message", "签到失败")
            logger.warning(f"  ➜ 影巢签到失败: {error_msg}")
            task_manager.update_status_from_thread(-1, f"签到失败: {error_msg}")

        # 3. 触发 Telegram 通知
        notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, [])
        if 'hdhive_checkin' in notify_types:
            try:
                send_hdhive_checkin_notification(res, is_gambler, user_info)
            except Exception as e:
                logger.error(f"  ➜ 发送影巢签到通知失败: {e}")
        else:
            logger.debug("  ➜ 影巢签到通知已在设置中关闭，跳过推送。")

    except Exception as e:
        logger.error(f"  ➜ 影巢自动签到发生异常: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "签到异常")

HDHIVE_FILTER_DEFAULTS = {
    "free_only": False,
    "max_points": 10,
    "max_size_gb": 120,
    "resolution": "All",
    "zh_sub_only": True,
    "exclude_iso": False,
}


def get_hdhive_filter_config():
    """
    读取影巢资源筛选配置。
    统一来源：app_settings.hdhive_config.value_json.filter
    """
    cfg = settings_db.get_setting("hdhive_config") or {}

    if not isinstance(cfg, dict):
        cfg = {}

    filter_cfg = cfg.get("filter") or {}
    if not isinstance(filter_cfg, dict):
        filter_cfg = {}

    merged = {
        **HDHIVE_FILTER_DEFAULTS,
        **filter_cfg
    }

    return {
        "free_only": bool(merged.get("free_only", False)),
        "max_points": int(merged.get("max_points", 10)),
        "max_size_gb": float(merged.get("max_size_gb", 120)),
        "resolution": merged.get("resolution") or "All",
        "zh_sub_only": bool(merged.get("zh_sub_only", True)),
        "exclude_iso": bool(merged.get("exclude_iso", False)),
    }


def _resource_size_to_gb(resource: dict) -> float | None:
    """从资源字典中提取大小信息，并转换为 GB 单位的浮点数。"""
    raw = (
        resource.get("share_size")
        or resource.get("size")
        or resource.get("file_size")
        or resource.get("total_size")
    )

    if raw is None:
        return None

    if isinstance(raw, (int, float)):
        # 大数字按 bytes，小数字按 GB
        return float(raw) / 1024 / 1024 / 1024 if raw > 10000 else float(raw)

    text = str(raw).strip().upper().replace(",", "")
    m = re.search(r"([\d.]+)\s*(TB|GB|MB|KB|B)?", text)
    if not m:
        return None

    value = float(m.group(1))
    unit = m.group(2) or "GB"

    if unit == "TB":
        return value * 1024
    if unit == "GB":
        return value
    if unit == "MB":
        return value / 1024
    if unit == "KB":
        return value / 1024 / 1024
    if unit == "B":
        return value / 1024 / 1024 / 1024

    return value


def _resource_text(resource: dict) -> str:
    """从资源字典中提取多个文本字段，并合并成一个字符串，供后续关键词/季号匹配使用。"""
    if not isinstance(resource, dict):
        return ""

    parts = [
        resource.get("title"),
        resource.get("name"),
        resource.get("remark"),
        resource.get("description"),
        resource.get("summary"),
        resource.get("filename"),
        resource.get("file_name"),
        resource.get("resource_name"),
        resource.get("share_name"),
        resource.get("subtitle"),
        resource.get("subtitles"),
        resource.get("audio"),
        resource.get("source"),
        resource.get("video_resolution"),
        resource.get("video_codec"),
        resource.get("format"),
        resource.get("tags"),
    ]

    text_parts = []

    def _append(value):
        if value is None:
            return
        if isinstance(value, dict):
            for v in value.values():
                _append(v)
            return
        if isinstance(value, (list, tuple, set)):
            for v in value:
                _append(v)
            return
        value = str(value).strip()
        if value:
            text_parts.append(value)

    for part in parts:
        _append(part)

    return " ".join(text_parts)


def _resource_has_zh_sub(resource: dict) -> bool:
    """检查资源是否包含中文相关字幕信息。英文短码必须边界匹配，避免裸 zh 误伤。"""
    text = _resource_text(resource).lower()

    zh_words = [
        "中字", "中文字幕", "简中", "繁中", "简体", "繁体", "简繁",
        "双语", "中英", "中日", "中韩", "国配中字", "内封中字", "内嵌中字",
        "官方中字", "中文字幕", "中文", "粤字", "中字特效",
    ]
    if any(k in text for k in zh_words):
        return True

    zh_tokens = [
        "chs", "cht", "chi", "zho", "zh-cn", "zh-tw", "zh-hk",
        "zh-hans", "zh-hant", "chinese", "mandarin", "cantonese",
    ]
    return any(
        re.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", text)
        for k in zh_tokens
    )


def _resource_resolution_match(resource: dict, expected: str) -> bool:
    """检查资源分辨率；字段缺失时回退到 title/remark 等全文匹配。"""
    if not expected or expected == "All":
        return True

    values = resource.get("video_resolution") or []
    if not isinstance(values, list):
        values = [values]

    text = " ".join(str(v) for v in values if v).strip()
    if not text:
        text = _resource_text(resource)

    text = text.lower()

    if expected == "4K":
        return "4k" in text or "2160" in text or "uhd" in text

    if expected == "1080p":
        return "1080" in text

    if expected == "720p":
        return "720" in text

    return True


def _zh_num_to_int(text: str) -> int | None:
    """把 一/二/十一/二十三 这类中文数字转成 int。"""
    if text is None:
        return None

    text = str(text).strip()
    if text.isdigit():
        return int(text)

    num_map = {
        "零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
        "五": 5, "六": 6, "七": 7, "八": 8, "九": 9
    }

    if text == "十":
        return 10

    if "十" in text:
        left, _, right = text.partition("十")
        tens = num_map.get(left, 1) if left else 1
        ones = num_map.get(right, 0) if right else 0
        return tens * 10 + ones

    return num_map.get(text)


def _int_to_zh_num(num: int) -> str:
    """把 int 转成中文数字，支持常见季号。"""
    num_map = {
        0: "零", 1: "一", 2: "二", 3: "三", 4: "四",
        5: "五", 6: "六", 7: "七", 8: "八", 9: "九"
    }

    if num <= 10:
        return "十" if num == 10 else num_map.get(num, str(num))

    if num < 20:
        return "十" + num_map.get(num % 10, "")

    if num < 100:
        tens = num // 10
        ones = num % 10
        return num_map.get(tens, str(tens)) + "十" + (num_map.get(ones, "") if ones else "")

    return str(num)


def _season_between(start, end, target: int) -> bool:
    try:
        start = int(start)
        end = int(end)
        if start > end:
            start, end = end, start
        return start <= target <= end
    except Exception:
        return False


def _resource_season_match_level(resource: dict, target_season) -> int:
    """
    判断资源是否覆盖目标季。

    返回值：
      30 = 明确命中目标季，例如 S03 / Season 3 / 第三季 / 第3季
      20 = 范围覆盖目标季，例如 S01-S05 / Season 1-5 / 第1-5季 / 全五季
      15 = 全季/全集/合集/Complete Series 这类合集资源
       5 = 没写季号，弱保留
      -1 = 明确写了其他季，且不覆盖目标季
    """
    if target_season is None:
        return 0

    try:
        target = int(target_season)
    except Exception:
        return 0

    text = _resource_text(resource)
    if not text:
        return 5

    upper_text = text.upper()
    zh_target = _int_to_zh_num(target)

    # 1. 明确命中当前季：S03 / S3 / Season 3 / 第三季 / 第3季 / 3季
    #    S05E01-S05E06 也会被 S05 命中。
    exact_patterns = [
        rf"(?<![A-Z0-9])S0?{target}(?!\d)",
        rf"\bSEASONS?\s*0?{target}\b",
        rf"第\s*(?:0?{target}|{zh_target})\s*季",
        rf"(?<!\d)0?{target}\s*季",
    ]

    if any(re.search(p, upper_text, re.IGNORECASE) for p in exact_patterns):
        return 30

    # 2. S01-S05 / S1-5 / Season 1-5 / Seasons 1 to 5
    range_patterns = [
        r"(?<![A-Z0-9])S0?(\d{1,2})\s*(?:-|~|–|—|至|到|TO)\s*S?0?(\d{1,2})(?!\d)",
        r"\bSEASONS?\s*0?(\d{1,2})\s*(?:-|~|–|—|TO|至|到)\s*(?:SEASONS?\s*)?0?(\d{1,2})\b",
    ]

    for pattern in range_patterns:
        for m in re.finditer(pattern, upper_text, re.IGNORECASE):
            return 20 if _season_between(m.group(1), m.group(2), target) else -1

    # 3. 第1-5季 / 一至五季 / 1-5季
    zh_range_pattern = r"(?:第)?\s*([一二两三四五六七八九十\d]{1,3})\s*(?:-|~|–|—|至|到)\s*([一二两三四五六七八九十\d]{1,3})\s*季"
    for m in re.finditer(zh_range_pattern, text, re.IGNORECASE):
        start = _zh_num_to_int(m.group(1))
        end = _zh_num_to_int(m.group(2))
        if start is not None and end is not None:
            return 20 if _season_between(start, end, target) else -1

    # 4. 全5季 / 全五季
    full_with_count_patterns = [
        r"全\s*(\d{1,2})\s*季",
        r"全\s*([一二两三四五六七八九十]{1,3})\s*季",
    ]

    for pattern in full_with_count_patterns:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            total = _zh_num_to_int(m.group(1))
            if total is not None:
                return 20 if target <= total else -1

    # 5. Complete Season N / S04 Complete：这种不是合集，必须按具体季号判断。
    complete_season_patterns = [
        r"\bCOMPLETE\s+SEASONS?\s*0?(\d{1,2})\b",
        r"\bSEASONS?\s*0?(\d{1,2})\s+COMPLETE\b",
        r"(?<![A-Z0-9])S0?(\d{1,2})(?!\d).*?\bCOMPLETE\b",
    ]

    for pattern in complete_season_patterns:
        for m in re.finditer(pattern, upper_text, re.IGNORECASE):
            try:
                return 30 if int(m.group(1)) == target else -1
            except Exception:
                continue

    # 6. 全季 / 全集 / 合集 / Complete Series / Pack。
    #    注意：不要用裸 COMPLETE，否则 S04 Complete 会误认为合集。
    pack_patterns = [
        r"全季", r"全集", r"合集", r"全套",
        r"COMPLETE\s+SERIES",
        r"\bSERIES\s+PACK\b",
        r"\bSEASONS?\s*PACK\b",
        r"\bPACK\b",
    ]

    if any(re.search(p, upper_text, re.IGNORECASE) for p in pack_patterns):
        return 15

    # 7. 明确有季号，但不是目标季，也不是覆盖范围：排除。
    explicit_other_season_patterns = [
        r"(?<![A-Z0-9])S\d{1,2}(?!\d)",
        r"\bSEASONS?\s*\d{1,2}\b",
        r"第\s*[一二两三四五六七八九十\d]{1,3}\s*季",
        r"(?<!\d)\d{1,2}\s*季",
    ]

    if any(re.search(p, upper_text, re.IGNORECASE) for p in explicit_other_season_patterns):
        return -1

    # 8. 没写季号：同一个 tv tmdb_id 下弱保留，不直接杀，避免影巢备注缺失导致全军覆没。
    return 5


def _season_match_label(level: int) -> str:
    if level >= 30:
        return "明确命中目标季"
    if level >= 20:
        return "范围/全季覆盖目标季"
    if level >= 15:
        return "合集资源"
    if level >= 5:
        return "未标明季号，弱保留"
    return "明确错季，已排除"


def _resource_completion_level(resource: dict) -> int:
    """
    判断影巢资源完整度。

    返回值：
      30 = 明确完整：全集 / 全结 / 完结 / 38集全 / 全38集 / Complete Series
      10 = 明确分段：S01E01-E10 / E01-E10 / 第01-10集
       0 = 未知

    注意：完整度只参与排序，不覆盖季号排除。
    如果资源明确写了其他季，仍然由 _resource_season_match_level 返回 -1 排除。
    """
    text = _resource_text(resource)
    if not text:
        return 0

    upper_text = text.upper()

    complete_patterns = [
        r"全集",
        r"全套",
        r"全结",
        r"全劇",
        r"全剧",
        r"完结",
        r"完結",
        r"已完结",
        r"已完結",
        r"\d{1,4}\s*集\s*(?:全|完|全结|完结|全劇|全剧|完結)",
        r"全\s*\d{1,4}\s*集",
        r"COMPLETE\s+SERIES",
        r"\bSERIES\s+COMPLETE\b",
    ]

    if any(re.search(p, upper_text, re.IGNORECASE) for p in complete_patterns):
        return 30

    partial_patterns = [
        r"S\d{1,2}\s*E\d{1,3}\s*(?:-|~|–|—|至|到|TO)\s*E?\d{1,3}",
        r"E\d{1,3}\s*(?:-|~|–|—|至|到|TO)\s*E?\d{1,3}",
        r"第\s*\d{1,3}\s*(?:-|~|–|—|至|到)\s*\d{1,3}\s*集",
        r"\d{1,3}\s*(?:-|~|–|—|至|到)\s*\d{1,3}\s*集",
    ]

    if any(re.search(p, upper_text, re.IGNORECASE) for p in partial_patterns):
        return 10

    return 0


def _completion_label(level: int) -> str:
    if level >= 30:
        return "完整资源"
    if level >= 10:
        return "分段资源"
    return "完整度未知"


def _rank_hdhive_resources_for_season(resources: list[dict], target_season) -> list[dict]:
    """
    按目标季对影巢资源排序。

    核心原则：
    - 明确错季仍然排除。
    - 完整资源优先于分段资源，解决“未标季全结”被“S01E01-E10”压住的问题。
    - 同完整度下，再按季号命中强度、积分、115、体积排序。
    """
    ranked = []

    for resource in resources or []:
        season_level = _resource_season_match_level(resource, target_season)

        resource["_season_match_level"] = season_level
        resource["_season_match_label"] = _season_match_label(season_level)

        if season_level < 0:
            logger.debug(
                f"  ➜ [影巢季过滤] 排除明确错季资源: "
                f"{resource.get('title') or resource.get('remark') or resource.get('slug')}"
            )
            continue

        completion_level = _resource_completion_level(resource)
        resource["_completion_level"] = completion_level
        resource["_completion_label"] = _completion_label(completion_level)

        ranked.append(resource)

    ranked.sort(
        key=lambda r: (
            -int(r.get("_completion_level", 0)),
            -int(r.get("_season_match_level", 0)),
            int(r.get("_effective_points", 0)),
            0 if str(r.get("pan_type") or "115").lower() == "115" else 1,
            -float(r.get("_size_gb", 0) or 0),
        )
    )

    return ranked

def filter_hdhive_resources(
        resources: list[dict],
        config: dict | None = None,
        target_season=None,
        media_type: str | None = None,
        require_complete: bool = False
    ) -> list[dict]:
    """
    根据配置过滤影巢资源列表，返回符合条件的资源。

    require_complete 仅用于剧集资源：
    - True：只保留“全集 / 全结 / 完结 / 全N集 / N集全”这类完整包。
      用于 TMDb 判定已完结的剧集/季，避免转存 S01E01-E10 这种残缺包。
      第一季已完结时，调用方会忽略“明确 S01”的排序优先级，但这里仍会排除明确错季资源。
    - False：不强制完整包。用于未完结剧集，逮到可用资源就转存，后续交给智能追剧补全。
    """
    cfg = config or get_hdhive_filter_config()

    filtered = []

    for resource in resources or []:
        points = resource.get("unlock_points")
        points = 0 if points is None else int(points or 0)
        already_owned = bool(resource.get("already_owned"))

        if cfg.get("free_only"):
            if not already_owned and points > 0:
                continue
        else:
            max_points = int(cfg.get("max_points", 10))
            if not already_owned and points > max_points:
                continue

        max_size_gb = float(cfg.get("max_size_gb", 120))
        size_gb = _resource_size_to_gb(resource)
        if size_gb is not None and size_gb > max_size_gb:
            continue

        if not _resource_resolution_match(resource, cfg.get("resolution", "All")):
            continue

        if cfg.get("zh_sub_only") and not _resource_has_zh_sub(resource):
            continue

        if cfg.get("exclude_iso"):
            text = _resource_text(resource).upper()
            # 1. 如果包含纯原盘格式，直接排除
            if any(k in text for k in ["ISO", "BDISO", "BDMV"]):
                continue
            # 2. 如果包含"原盘"，但【不包含】"REMUX"，则排除 (防止误杀"蓝光原盘REMUX")
            if "原盘" in text and "REMUX" not in text:
                continue

        resource['_effective_points'] = 0 if already_owned else points
        resource['_size_gb'] = size_gb or 0

        # 剧集资源统一标注完整度。即使没有目标季，也能用于“已完结只转存完结包”。
        if media_type == "tv":
            completion_level = _resource_completion_level(resource)
            resource["_completion_level"] = completion_level
            resource["_completion_label"] = _completion_label(completion_level)

        filtered.append(resource)

    if media_type == "tv" and target_season is not None:
        before_count = len(filtered)
        filtered = _rank_hdhive_resources_for_season(filtered, target_season)
        try:
            season_text = f"S{int(target_season):02d}"
        except Exception:
            season_text = str(target_season)

        logger.info(
            f"  ➜ [影巢季排序] 目标季 {season_text}: "
            f"{before_count} 条候选资源 -> {len(filtered)} 条可用资源。"
        )

    if media_type == "tv" and require_complete:
        before_complete_count = len(filtered)
        filtered = [r for r in filtered if int(r.get("_completion_level") or 0) >= 30]
        logger.info(
            f"  ➜ [影巢完结包过滤] 已完结剧集/季只保留完整包: "
            f"{before_complete_count} 条候选资源 -> {len(filtered)} 条完整资源。"
        )

    return filtered
