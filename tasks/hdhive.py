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
    access_code = unlock_data.get("access_code")
    
    # ★★★ 提取码兜底逻辑 ★★★
    # 如果 API 没有直接返回 access_code，尝试从 URL 参数中正则提取 (如 ?password=n832 或 ?pwd=n832)
    if not access_code:
        pwd_match = re.search(r'(?:pwd|password|code)=([a-zA-Z0-9]+)', full_url + "&" + share_url, re.IGNORECASE)
        if pwd_match:
            access_code = pwd_match.group(1)
            
    # 确保 access_code 不是 None，防止后续报错
    access_code = access_code or ""
    
    # 提取分享码 (同时从 share_url 和 full_url 中找，增加容错率)
    match = re.search(r'(?:115\.com|115cdn\.com|anxia\.com)/s/([a-zA-Z0-9]+)', share_url + " " + full_url)
    if not match:
        logger.error(f"  ➜ 无法从链接中提取 115 分享码: {share_url} | {full_url}")
        return False
        
    share_code = match.group(1)
    logger.info(f"  ➜ 成功获取 115 分享码: {share_code}, 提取码: {access_code}")

    client = P115Service.get_client()
    if not client:
        logger.error("  ➜ 115 客户端未初始化，无法转存。")
        return False

    config = get_config()
    save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID) 
    if not save_cid or str(save_cid) == '0':
        logger.error("  ➜ 未配置 115 待整理目录，无法转存。")
        return False

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

    api_key = settings_db.get_setting('hdhive_api_key')
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