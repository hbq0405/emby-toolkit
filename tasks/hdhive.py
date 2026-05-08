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

    hdhive_config = settings_db.get_setting("hdhive_config") or {}
    api_key = hdhive_config.get("api_key")
    if not api_key:
        logger.info("  ➜ 未配置影巢 API Key，跳过自动签到。")
        task_manager.update_status_from_thread(100, "未配置 API Key，跳过")
        return

    client = HDHiveClient(api_key)
    task_manager.update_status_from_thread(50, "正在发送签到请求...")

    try:
        # 1. 发送签到请求
        res = client.checkin(is_gambler=False)
        
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
                send_hdhive_checkin_notification(res, user_info)
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
    """从资源字典中提取多个文本字段，并合并成一个字符串，供后续关键词匹配使用。"""
    parts = [
        resource.get("title"),
        resource.get("remark"),
        resource.get("description"),
        resource.get("subtitle"),
        resource.get("subtitles"),
        resource.get("audio"),
        resource.get("source"),
        resource.get("video_resolution"),
    ]

    text_parts = []
    for p in parts:
        if isinstance(p, list):
            text_parts.extend([str(x) for x in p])
        elif p:
            text_parts.append(str(p))

    return " ".join(text_parts)


def _resource_has_zh_sub(resource: dict) -> bool:
    """检查资源是否包含中文相关的字幕信息。"""
    text = _resource_text(resource).lower()

    zh_keywords = [
        "中字", "中文字幕", "简中", "繁中", "简体", "繁体", "简繁",
        "双语", "中英", "国配中字", "内封中字",
        "chs", "cht", "chi", "zh", "zh-cn", "zh-tw", "chinese"
    ]

    return any(k.lower() in text for k in zh_keywords)


def _resource_resolution_match(resource: dict, expected: str) -> bool:
    """检查资源的分辨率信息是否符合预期。"""
    if not expected or expected == "All":
        return True

    values = resource.get("video_resolution") or []
    if not isinstance(values, list):
        values = [values]

    text = " ".join(str(v) for v in values).lower()

    if expected == "4K":
        return "4k" in text or "2160" in text or "uhd" in text

    if expected == "1080p":
        return "1080" in text

    return True


def filter_hdhive_resources(resources: list[dict], config: dict | None = None) -> list[dict]:
    """根据配置过滤影巢资源列表，返回符合条件的资源。"""
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
            if any(k in text for k in ["ISO", "BDISO", "BDMV", "原盘"]):
                continue

        resource['_effective_points'] = 0 if already_owned else points
        resource['_size_gb'] = size_gb or 0
        filtered.append(resource)

    return filtered