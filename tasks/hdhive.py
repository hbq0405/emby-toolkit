# handler/hdhive.py
import re
import time
import logging
from handler.hdhive_client import HDHiveClient
from handler.p115_service import P115Service, SmartOrganizer, get_config
import constants

logger = logging.getLogger(__name__)

def task_download_from_hdhive(api_key, slug, tmdb_id, media_type, title):
    """
    核心任务：从影巢解锁 -> 转存 115 -> 搜索真实ID -> 精准整理
    """
    logger.info(f"=== 🚀 开始从影巢获取资源: {title} (TMDB: {tmdb_id}) ===")
    
    hdhive = HDHiveClient(api_key)
    unlock_data = hdhive.unlock_resource(slug)
    
    if not unlock_data:
        logger.error("  ❌ 影巢资源解锁失败，可能积分不足或资源已失效。")
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
        logger.error(f"  ❌ 无法从链接中提取 115 分享码: {share_url} | {full_url}")
        return False
        
    share_code = match.group(1)
    logger.info(f"  🔗 成功获取 115 分享码: {share_code}, 提取码: {access_code}")

    client = P115Service.get_client()
    if not client:
        logger.error("  ❌ 115 客户端未初始化，无法转存。")
        return False

    config = get_config()
    save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID) 
    if not save_cid or str(save_cid) == '0':
        logger.error("  ❌ 未配置 115 待整理目录，无法转存。")
        return False

    logger.info(f"  📥 正在转存到 115 待整理目录 (CID: {save_cid})...")
    try:
        import_res = client.share_import(share_code, access_code, save_cid)
        
        # ★ 修复 1：兼容 state 为 True (布尔值) 或 1 (数字/字符串) 的情况
        if not import_res or not import_res.get('state'):
            logger.error(f"  ❌ 115 转存失败: {import_res.get('error_msg', import_res)}")
            return False
            
        receive_title = import_res.get('data', {}).get('receive_title')
        logger.info(f"  ✅ 115 转存成功！文件/目录名: {receive_title}")
        
        if not receive_title:
            logger.warning("  ⚠️ 转存成功但未返回文件名，交由全局定时扫描任务处理。")
            return True

        # ★ 修复 2：去待整理目录搜索刚刚转存的文件，获取真实的 file_id
        logger.info(f"  🔍 正在定位转存文件，准备执行精准整理...")
        time.sleep(2) # 稍微等2秒，防止 115 后端数据没同步
        
        search_res = client.fs_files({'cid': save_cid, 'search_value': receive_title, 'limit': 10})
        saved_items = search_res.get('data', [])
        
        target_item = None
        for item in saved_items:
            name = item.get('fn') or item.get('file_name') or item.get('n')
            if name == receive_title:
                target_item = item
                break
                
        if not target_item:
            logger.warning(f"  ⚠️ 未能精准定位到文件 '{receive_title}'，将交由全局定时扫描任务处理。")
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
        logger.error(f"  ❌ 转存或整理过程中发生异常: {e}", exc_info=True)
        return False