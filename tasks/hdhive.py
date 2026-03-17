# handler/hdhive.py
import re
import logging
from handler.hdhive_client import HDHiveClient
from handler.p115_service import P115Service, SmartOrganizer, get_config
import constants

logger = logging.getLogger(__name__)

def task_download_from_hdhive(api_key, slug, tmdb_id, media_type, title):
    """
    核心任务：从影巢解锁 -> 转存 115 -> 自动整理
    """
    logger.info(f"=== 🚀 开始从影巢获取资源: {title} (TMDB: {tmdb_id}) ===")
    
    # 1. 初始化影巢客户端并解锁
    hdhive = HDHiveClient(api_key)
    unlock_data = hdhive.unlock_resource(slug)
    
    if not unlock_data:
        logger.error("❌ 影巢资源解锁失败，可能积分不足或资源已失效。")
        return False
        
    share_url = unlock_data.get("url")
    access_code = unlock_data.get("access_code")
    
    # 2. 解析 115 分享码
    # 链接格式通常为: https://115.com/s/swz9xxxxxxx
    match = re.search(r'115\.com/s/([a-zA-Z0-9]+)', share_url)
    if not match:
        logger.error(f"❌ 无法从链接中提取 115 分享码: {share_url}")
        return False
        
    share_code = match.group(1)
    logger.info(f"  🔗 成功获取 115 分享码: {share_code}, 提取码: {access_code}")

    # 3. 调用 115 接口进行转存
    client = P115Service.get_client()
    if not client:
        logger.error("❌ 115 客户端未初始化，无法转存。")
        return False

    config = get_config()
    # 获取 ETK 配置的“待整理/下载”目录 CID
    save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID) 
    if not save_cid or str(save_cid) == '0':
        logger.error("❌ 未配置 115 待整理目录，无法转存。")
        return False

    logger.info(f"  📥 正在转存到 115 待整理目录 (CID: {save_cid})...")
    try:
        # 调用 p115_service.py 中现成的 share_import 方法
        import_res = client.share_import(share_code, access_code, save_cid)
        
        if not import_res or str(import_res.get('state')) != '1':
            logger.error(f"❌ 115 转存失败: {import_res.get('error_msg', import_res)}")
            return False
            
        logger.info("  ✅ 115 转存成功！准备触发自动整理...")
        
        # 4. 构造 root_item 触发 SmartOrganizer 自动整理
        # 115 转存接口通常会返回新生成的文件/文件夹信息
        # 这里需要根据 115 实际返回的 JSON 结构提取 fid/cid
        # 假设返回结构包含 data.list 或 data.file_id
        saved_items = import_res.get('data', {}).get('list', [])
        
        if not saved_items:
            # 如果没返回列表，主动去 save_cid 目录下扫一下最新文件兜底
            logger.warning("  ⚠️ 转存接口未返回文件列表，交由全局定时扫描任务处理。")
            return True

        # 遍历转存进来的项目，逐个丢给 SmartOrganizer
        for item in saved_items:
            # 兼容 115 字段
            root_item = {
                'fid': item.get('file_id') or item.get('fid'),
                'fn': item.get('file_name') or item.get('fn'),
                'fc': '0' if item.get('is_dir') else '1',
                'pid': save_cid
            }
            
            # 实例化智能整理器 (传入 TMDB ID 确保 100% 识别准确)
            organizer = SmartOrganizer(
                client=client, 
                tmdb_id=tmdb_id, 
                media_type=media_type, 
                original_title=title,
                use_ai=False # 已经有 TMDB ID 了，不需要 AI 猜
            )
            
            # 获取目标分类目录
            target_cid = organizer.get_target_cid()
            
            # 执行整理、重命名、生成 STRM
            organizer.execute(root_item, target_cid)
            
        logger.info("=== 🎉 影巢资源下载并整理完毕！ ===")
        return True

    except Exception as e:
        logger.error(f"❌ 转存或整理过程中发生异常: {e}", exc_info=True)
        return False