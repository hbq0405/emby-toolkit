# tasks/covers.py
# 封面生成与资产同步任务模块

import logging

# 导入需要的底层模块和共享实例
import emby_handler
import task_manager
from database import settings_db, collection_db
from services.cover_generator import CoverGeneratorService
from .collections import _get_cover_badge_text_for_collection

logger = logging.getLogger(__name__)

# ★★★ 同步覆盖缓存的任务函数 ★★★
def task_full_image_sync(processor, force_full_update: bool = False):
    """
    后台任务：调用 processor 的方法来同步所有图片。
    新增 force_full_update 参数以支持深度模式。
    """
    # 直接把回调函数和新参数传进去
    processor.sync_all_media_assets(
        update_status_callback=task_manager.update_status_from_thread,
        force_full_update=force_full_update
    )

# ★★★ 立即生成所有媒体库封面的后台任务 ★★★
def task_generate_all_covers(processor):
    """
    后台任务：为所有（未被忽略的）媒体库生成封面。
    """
    task_name = "一键生成所有媒体库封面"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # 1. 读取配置
        cover_config = settings_db.get_setting('cover_generator_config') or {}

        if not cover_config:
            # 如果数据库里连配置都没有，可以认为功能未配置
            task_manager.update_status_from_thread(-1, "错误：未找到封面生成器配置，请先在设置页面保存一次。")
            return

        if not cover_config.get("enabled"):
            task_manager.update_status_from_thread(100, "任务跳过：封面生成器未启用。")
            return

        # 2. 获取媒体库列表
        task_manager.update_status_from_thread(5, "正在获取所有媒体库列表...")
        all_libraries = emby_handler.get_emby_libraries(
            emby_server_url=processor.emby_url,
            emby_api_key=processor.emby_api_key,
            user_id=processor.emby_user_id
        )
        if not all_libraries:
            task_manager.update_status_from_thread(-1, "错误：未能从Emby获取到任何媒体库。")
            return
        
        # 3. 筛选媒体库
        # ★★★ 核心修复：直接使用原始ID进行比较 ★★★
        exclude_ids = set(cover_config.get("exclude_libraries", []))
        # 允许处理的媒体库类型列表，增加了 'audiobooks'
        ALLOWED_COLLECTION_TYPES = ['movies', 'tvshows', 'boxsets', 'mixed', 'music', 'audiobooks']

        libraries_to_process = [
            lib for lib in all_libraries 
            if lib.get('Id') not in exclude_ids
            and (
                # 条件1：满足常规的 CollectionType
                lib.get('CollectionType') in ALLOWED_COLLECTION_TYPES
                # 条件2：或者，是“混合库测试”这种特殊的 CollectionFolder
                or lib.get('Type') == 'CollectionFolder' 
            )
        ]
        
        total = len(libraries_to_process)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：没有需要处理的媒体库。")
            return
            
        logger.info(f"  -> 将为 {total} 个媒体库生成封面: {[lib['Name'] for lib in libraries_to_process]}")
        
        # 4. 实例化服务并循环处理
        cover_service = CoverGeneratorService(config=cover_config)
        
        TYPE_MAP = {
            'movies': 'Movie', 
            'tvshows': 'Series', 
            'music': 'MusicAlbum',
            'boxsets': 'BoxSet', 
            'mixed': 'Movie,Series',
            'audiobooks': 'AudioBook'  # <-- 增加有声读物的映射
        }

        for i, library in enumerate(libraries_to_process):
            if processor.is_stop_requested(): break
            
            progress = 10 + int((i / total) * 90)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total}) 正在处理: {library.get('Name')}")
            
            try:
                library_id = library.get('Id')
                collection_type = library.get('CollectionType')
                item_type_to_query = None # 先重置

                # --- ★★★ 核心修复 3：使用更精确的 if/elif 逻辑判断查询类型 ★★★ ---
                # 优先使用 CollectionType 进行判断，这是最准确的
                if collection_type:
                    item_type_to_query = TYPE_MAP.get(collection_type)
                
                # 如果 CollectionType 不存在，再使用 Type == 'CollectionFolder' 作为备用方案
                # 这专门用于处理像“混合库测试”那样的特殊库
                elif library.get('Type') == 'CollectionFolder':
                    logger.info(f"媒体库 '{library.get('Name')}' 是一个特殊的 CollectionFolder，将查询电影和剧集。")
                    item_type_to_query = 'Movie,Series'
                # --- 修复结束 ---

                item_count = 0
                if library_id and item_type_to_query:
                    item_count = emby_handler.get_item_count(
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id,
                        parent_id=library_id,
                        item_type=item_type_to_query
                    ) or 0

                cover_service.generate_for_library(
                    emby_server_id='main_emby', # 这里的 server_id 只是一个占位符，不影响忽略逻辑
                    library=library,
                    item_count=item_count
                )
            except Exception as e_gen:
                logger.error(f"为媒体库 '{library.get('Name')}' 生成封面时发生错误: {e_gen}", exc_info=True)
                continue
        
        final_message = "所有媒体库封面已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 只为所有自建合集生成封面的后台任务 ★★★
def task_generate_all_custom_collection_covers(processor):
    """
    后台任务：为所有已启用、且已在Emby中创建的自定义合集生成封面。
    """
    task_name = "一键生成所有自建合集封面"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # 1. 读取封面生成器的配置
        cover_config = settings_db.get_setting('cover_generator_config') or {}
        if not cover_config.get("enabled"):
            task_manager.update_status_from_thread(100, "任务跳过：封面生成器未启用。")
            return

        # 2. 从数据库获取所有已启用的自定义合集
        task_manager.update_status_from_thread(5, "正在获取所有已启用的自建合集...")
        all_active_collections = collection_db.get_all_active_custom_collections()
        
        # 3. 筛选出那些已经在Emby中成功创建的合集
        collections_to_process = [
            c for c in all_active_collections if c.get('emby_collection_id')
        ]
        
        total = len(collections_to_process)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：没有找到已在Emby中创建的自建合集。")
            return
            
        logger.info(f"  -> 将为 {total} 个自建合集生成封面。")
        
        # 4. 实例化服务并循环处理
        cover_service = CoverGeneratorService(config=cover_config)
        
        for i, collection_db_info in enumerate(collections_to_process):
            if processor.is_stop_requested(): break
            
            collection_name = collection_db_info.get('name')
            emby_collection_id = collection_db_info.get('emby_collection_id')
            
            progress = 10 + int((i / total) * 90)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total}) 正在处理: {collection_name}")
            
            try:
                # a. 获取完整的Emby合集详情，这是封面生成器需要的
                emby_collection_details = emby_handler.get_emby_item_details(
                    emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id
                )
                if not emby_collection_details:
                    logger.warning(f"无法获取合集 '{collection_name}' (Emby ID: {emby_collection_id}) 的详情，跳过。")
                    continue

                # 1. 从数据库记录中获取合集定义
                definition = collection_db_info.get('definition_json', {})
                content_types = definition.get('item_type', ['Movie'])

                # 2. 直接将当前循环中的合集信息传递给辅助函数
                item_count_to_pass = _get_cover_badge_text_for_collection(collection_db_info)

                # 3. 调用封面生成服务
                cover_service.generate_for_library(
                    emby_server_id='main_emby',
                    library=emby_collection_details,
                    item_count=item_count_to_pass, # <-- 使用计算好的角标参数
                    content_types=content_types
                )
            except Exception as e_gen:
                logger.error(f"为自建合集 '{collection_name}' 生成封面时发生错误: {e_gen}", exc_info=True)
                continue
        
        final_message = "所有自建合集封面已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")