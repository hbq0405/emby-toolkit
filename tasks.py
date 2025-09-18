# tasks.py

import time
import os
import json
import psycopg2
import pytz
import collections
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
import logging
from typing import Dict, Any, Tuple, List
import threading
from datetime import datetime, date, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed 
import concurrent.futures
import gevent
# 导入类型提示
from typing import Optional, List
from core_processor import MediaProcessor
from watchlist_processor import WatchlistProcessor
from actor_subscription_processor import ActorSubscriptionProcessor
from custom_collection_handler import ListImporter, FilterEngine

# 导入需要的底层模块和共享实例
import db_handler
import emby_handler
import tmdb_handler
import moviepilot_handler
import config_manager
import constants
import extensions
import task_manager
from actor_utils import enrich_all_actor_aliases_task
from actor_sync_handler import UnifiedSyncHandler
from extensions import TASK_REGISTRY
from custom_collection_handler import ListImporter, FilterEngine
from core_processor import _read_local_json
from services.cover_generator import CoverGeneratorService
import utils
from utils import get_country_translation_map, translate_country_list, get_unified_rating

logger = logging.getLogger(__name__)

EFFECT_KEYWORD_MAP = {
    "杜比视界": ["dolby vision", "dovi"],
    "HDR": ["hdr", "hdr10", "hdr10+", "hlg"]
}

AUDIO_SUBTITLE_KEYWORD_MAP = {
    # 音轨关键词
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    # 字幕关键词 (可以和音轨共用，也可以分开定义)
    "sub_chi": ["CHS", "CHT", "中字", "简中", "繁中", "简", "繁"],
    "sub_eng": ["ENG", "英字"],
}

# ★★★ 全量处理任务 ★★★
def task_run_full_scan(processor: MediaProcessor, force_reprocess: bool = False):
    """
    根据传入的 force_reprocess 参数，决定是执行标准扫描还是强制扫描。
    """
    # 1. 根据参数决定日志信息
    if force_reprocess:
        logger.warning("即将执行【强制】全量处理，将处理所有媒体项...")
    else:
        logger.info("即将执行【标准】全量处理，将跳过已处理项...")


    # 3. 调用核心处理函数，并将 force_reprocess 参数透传下去
    processor.process_full_library(
        update_status_callback=task_manager.update_status_from_thread,
        force_reprocess_all=force_reprocess,
        force_fetch_from_tmdb=force_reprocess
    )

# --- 同步演员映射表 ---
def task_sync_person_map(processor):
    """
    【最终兼容版】任务：同步演员映射表。
    接收 processor 和 is_full_sync 以匹配通用任务执行器，
    但内部逻辑已统一，不再使用 is_full_sync。
    """
    task_name = "同步演员映射"
    # 我们不再需要根据 is_full_sync 来改变任务名了，因为逻辑已经统一
    
    logger.trace(f"开始执行 '{task_name}'...")
    
    try:
        # ★★★ 从传入的 processor 对象中获取 config 字典 ★★★
        config = processor.config
        
        sync_handler = UnifiedSyncHandler(
            emby_url=config.get("emby_server_url"),
            emby_api_key=config.get("emby_api_key"),
            emby_user_id=config.get("emby_user_id"),
            tmdb_api_key=config.get("tmdb_api_key", "")
        )
        
        # 调用同步方法，不再需要传递 is_full_sync
        sync_handler.sync_emby_person_map_to_db(
            update_status_callback=task_manager.update_status_from_thread
        )
        
        logger.trace(f"'{task_name}' 成功完成。")

    except Exception as e:
        logger.error(f"'{task_name}' 执行过程中发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误：同步失败 ({str(e)[:50]}...)")
# ✨✨✨ 演员数据补充函数 ✨✨✨
def task_enrich_aliases(processor: MediaProcessor, force_full_update: bool = False):
    """
    【V4 - 支持深度模式】演员数据补充任务的入口点。
    - 标准模式 (force_full_update=False): 使用30天冷却期，只处理过期或不完整的演员。
    - 深度模式 (force_full_update=True): 无视冷却期 (设置为0)，全量处理所有需要补充数据的演员。
    """
    # 根据模式确定任务名和冷却时间
    if force_full_update:
        task_name = "演员数据补充 (全量)"
        cooldown_days = 0  # 深度模式：冷却时间为0，即无视冷却期
        logger.info(f"后台任务 '{task_name}' 开始执行，将全量处理所有演员...")
    else:
        task_name = "演员数据补充 (增量)"
        cooldown_days = 30 # 标准模式：使用固定的30天冷却期
        logger.info(f"后台任务 '{task_name}' 开始执行...")

    try:
        # 从传入的 processor 对象中获取配置字典
        config = processor.config
        
        # 获取必要的配置项
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)

        if not tmdb_api_key:
            logger.error(f"任务 '{task_name}' 中止：未在配置中找到 TMDb API Key。")
            task_manager.update_status_from_thread(-1, "错误：缺少TMDb API Key")
            return

        # 运行时长硬编码为0，代表“不限制时长”
        duration_minutes = 0
        
        logger.trace(f"演员数据补充任务将使用 {cooldown_days} 天作为同步冷却期。")

        # 调用核心函数，并传递计算好的冷却时间
        enrich_all_actor_aliases_task(
            tmdb_api_key=tmdb_api_key,
            run_duration_minutes=duration_minutes,
            sync_interval_days=cooldown_days, # <--- 核心修改点
            stop_event=processor.get_stop_event(),
            update_status_callback=task_manager.update_status_from_thread,
            force_full_update=force_full_update
        )
        
        logger.info(f"--- '{task_name}' 任务执行完毕。 ---")
        task_manager.update_status_from_thread(100, f"{task_name}完成。")

    except Exception as e:
        logger.error(f"'{task_name}' 执行过程中发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误：任务失败 ({str(e)[:50]}...)")
# --- 使用手动编辑的结果处理媒体项 ---
def task_manual_update(processor: MediaProcessor, item_id: str, manual_cast_list: list, item_name: str):
    """任务：使用手动编辑的结果处理媒体项"""
    processor.process_item_with_manual_cast(
        item_id=item_id,
        manual_cast_list=manual_cast_list,
        item_name=item_name
    )
# --- 扫描单个演员订阅的所有作品 ---
def task_scan_actor_media(processor: ActorSubscriptionProcessor, subscription_id: int):
    """【新】后台任务：扫描单个演员订阅的所有作品。"""
    logger.trace(f"手动刷新任务(ID: {subscription_id})：开始准备Emby媒体库数据...")
    
    # 在调用核心扫描函数前，必须先获取Emby数据
    emby_tmdb_ids = set()
    try:
        # 从 processor 或全局配置中获取 Emby 连接信息
        config = processor.config # 假设 processor 对象中存有配置
        emby_url = config.get('emby_server_url')
        emby_api_key = config.get('emby_api_key')
        emby_user_id = config.get('emby_user_id')

        all_libraries = emby_handler.get_emby_libraries(emby_url, emby_api_key, emby_user_id)
        library_ids_to_scan = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['movies', 'tvshows']]
        emby_items = emby_handler.get_emby_library_items(base_url=emby_url, api_key=emby_api_key, user_id=emby_user_id, library_ids=library_ids_to_scan, media_type_filter="Movie,Series")
        
        emby_tmdb_ids = {item['ProviderIds'].get('Tmdb') for item in emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        logger.debug(f"手动刷新任务：已从 Emby 获取 {len(emby_tmdb_ids)} 个媒体ID。")

    except Exception as e:
        logger.error(f"手动刷新任务：在获取Emby媒体库信息时失败: {e}", exc_info=True)
        # 获取失败时，可以传递一个空集合，让扫描逻辑继续（但可能不准确），或者直接返回
        # 这里选择继续，让用户至少能更新TMDb信息

    # 现在，带着准备好的 emby_tmdb_ids 调用函数
    processor.run_full_scan_for_actor(subscription_id, emby_tmdb_ids)
# --- 演员订阅 ---
def task_process_actor_subscriptions(processor: ActorSubscriptionProcessor):
    """【新】后台任务：执行所有启用的演员订阅扫描。"""
    processor.run_scheduled_task(update_status_callback=task_manager.update_status_from_thread)
# ★★★ 处理webhook、用于编排任务的函数 ★★★
def webhook_processing_task(processor: MediaProcessor, item_id: str, force_reprocess: bool):
    """
    【V5 - 媒体库限制修复版】
    - 调整了代码顺序，确保在匹配自定义合集时，能将媒体项所属的库ID传递给筛选引擎。
    - 修复了筛选类合集的媒体库限制在实时匹配时无效的BUG。
    """
    item_details = emby_handler.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  -> 无法获取项目 {item_id} 的详情，任务中止。")
        return

    processor.check_and_add_to_watchlist(item_details)

    processed_successfully = processor.process_single_item(item_id, force_reprocess_this_item=force_reprocess)
    
    if not processed_successfully:
        logger.warning(f"  -> 项目 {item_id} 的元数据处理未成功完成，跳过自定义合集匹配。")
        return

    try:
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name", f"ID:{item_id}")
        if not tmdb_id:
            logger.debug("  -> 媒体项缺少TMDb ID，无法进行自定义合集匹配。")
            return

        item_metadata = db_handler.get_media_metadata_by_tmdb_id(tmdb_id)
        if not item_metadata:
            logger.warning(f"  -> 无法从本地缓存中找到TMDb ID为 {tmdb_id} 的元数据，无法匹配合集。")
            return

        # ▼▼▼ 步骤 1: 将获取媒体库信息的逻辑提前 ▼▼▼
        library_info = emby_handler.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not library_info:
            logger.warning(f"  -> 无法为项目 {item_id} 定位到其所属的媒体库根，将无法进行基于媒体库的合集匹配。")
            # 注意：这里我们只记录警告，不中止任务，因为可能还有不限制媒体库的合集需要匹配
            media_library_id = None
        else:
            media_library_id = library_info.get("Id")
        # ▲▲▲ 步骤 1: 完成 ▲▲▲

        # --- 匹配 Filter (筛选) 类型的合集 ---
        engine = FilterEngine()
        
        # 【关键修改】在这里将获取到的 media_library_id 传递给 find_matching_collections
        matching_filter_collections = engine.find_matching_collections(item_metadata, media_library_id=media_library_id)

        if matching_filter_collections:
            logger.info(f"  -> 《{item_name}》匹配到 {len(matching_filter_collections)} 个筛选类合集，正在追加...")
            for collection in matching_filter_collections:
                # 步骤 1: 更新 Emby 实体合集
                emby_handler.append_item_to_collection(
                    collection_id=collection['emby_collection_id'],
                    item_emby_id=item_id,
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
                
                # ★★★ 核心修复：同步更新我们自己的数据库缓存 ★★★
                db_handler.append_item_to_filter_collection_db(
                    collection_id=collection['id'],
                    new_item_tmdb_id=tmdb_id,
                    new_item_emby_id=item_id
                )
        else:
            logger.info(f"  -> 《{item_name}》没有匹配到任何筛选类合集。")

        # --- 匹配 List (榜单) 类型的合集 ---
        # (这部分逻辑不变)
        updated_list_collections = db_handler.match_and_update_list_collections_on_item_add(
            new_item_tmdb_id=tmdb_id,
            new_item_emby_id=item_id,
            new_item_name=item_name
        )
        
        if updated_list_collections:
            logger.info(f"  -> 《{item_name}》匹配到 {len(updated_list_collections)} 个榜单类合集，正在追加...")
            for collection_info in updated_list_collections:
                emby_handler.append_item_to_collection(
                    collection_id=collection_info['emby_collection_id'],
                    item_emby_id=item_id,
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
        else:
             logger.info(f"  -> 《{item_name}》没有匹配到任何需要更新状态的榜单类合集。")

    except Exception as e:
        logger.error(f"  -> 为新入库项目 {item_id} 匹配自定义合集时发生意外错误: {e}", exc_info=True)

    # --- 封面生成逻辑 (保持不变) ---
    try:
        cover_config = db_handler.get_setting('cover_generator_config') or {}

        if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
            logger.info(f"  -> 检测到 '{item_details.get('Name')}' 入库，将为其所属媒体库生成新封面...")
            
            # ▼▼▼ 步骤 2: 复用已获取的 library_info，无需重复获取 ▼▼▼
            if not library_info:
                logger.warning(f"  -> (封面生成) 无法为项目 {item_id} 定位到其所属的媒体库根，跳过封面生成。")
                return
            # ▲▲▲ 步骤 2: 完成 ▲▲▲

            library_id = library_info.get("Id") # library_id 变量在这里被重新赋值，但不影响上面的逻辑
            library_name = library_info.get("Name", library_id)
            
            if library_info.get('CollectionType') not in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                logger.debug(f"  -> 父级 '{library_name}' 不是一个常规媒体库，跳过封面生成。")
                return

            server_id = 'main_emby'
            library_unique_id = f"{server_id}-{library_id}"
            if library_unique_id in cover_config.get("exclude_libraries", []):
                logger.info(f"  -> 媒体库 '{library_name}' 在忽略列表中，跳过。")
                return
            
            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
            collection_type = library_info.get('CollectionType')
            item_type_to_query = TYPE_MAP.get(collection_type)
            
            item_count = 0
            if library_id and item_type_to_query:
                item_count = emby_handler.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0
            
            logger.info(f"  -> 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
            cover_service = CoverGeneratorService(config=cover_config)
            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)
        else:
            logger.debug("  -> 封面生成器或入库监控未启用，跳过封面生成。")

    except Exception as e:
        logger.error(f"  -> 在新入库后执行精准封面生成时发生错误: {e}", exc_info=True)

    logger.trace(f"  -> Webhook 任务及所有后续流程完成: {item_id}")
# --- 追剧 ---    
def task_process_watchlist(processor: WatchlistProcessor, item_id: Optional[str] = None):
    """
    【V9 - 启动器】
    调用处理器实例来执行追剧任务，并处理UI状态更新。
    """
    # 定义一个可以传递给处理器的回调函数
    def progress_updater(progress, message):
        # 这里的 task_manager.update_status_from_thread 是你项目中用于更新UI的函数
        task_manager.update_status_from_thread(progress, message)

    try:
        # 直接调用 processor 实例的方法，并将回调函数传入
        processor.run_regular_processing_task_concurrent(progress_callback=progress_updater, item_id=item_id)

    except Exception as e:
        task_name = "追剧列表更新"
        if item_id:
            task_name = f"单项追剧更新 (ID: {item_id})"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")
# ★★★ 只更新追剧列表中的一个特定项目 ★★★
def task_refresh_single_watchlist_item(processor: WatchlistProcessor, item_id: str):
    """
    【V11 - 新增】后台任务：只刷新追剧列表中的一个特定项目。
    这是一个职责更明确的函数，专门用于手动触发。
    """
    # 定义一个可以传递给处理器的回调函数
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 直接调用处理器的主方法，并将 item_id 传入
        # 这会执行完整的元数据刷新、状态检查和数据库更新流程
        processor.run_regular_processing_task_concurrent(progress_callback=progress_updater, item_id=item_id)

    except Exception as e:
        task_name = f"单项追剧刷新 (ID: {item_id})"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")
# ★★★ 低频任务 - 检查已完结剧集是否复活 ★★★
def task_run_revival_check(processor: WatchlistProcessor):
    """
    【低频任务】后台任务入口：检查所有已完结剧集是否“复活”。
    """
    # 定义一个可以传递给处理器的回调函数
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 直接调用 processor 实例的方法，并将回调函数传入
        processor.run_revival_check_task(progress_callback=progress_updater)

    except Exception as e:
        task_name = "已完结剧集复活检查"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")
# --- 辅助函数 1: 数据清洗与准备 ---
def _prepare_data_for_insert(table_name: str, table_data: List[Dict[str, Any]]) -> tuple[List[str], List[tuple]]:
    """
    一个极简的数据准备函数，专为 PG-to-PG 流程设计。
    - 它只做一件事：将需要存入 JSONB 列的数据包装成 psycopg2 的 Json 对象。
    """
    # 定义哪些列是 JSONB 类型，需要特殊处理
    JSONB_COLUMNS = {
        'actor_subscriptions': {
            'config_genres_include_json', 'config_genres_exclude_json', 
            'config_tags_include_json', 'config_tags_exclude_json'
        },
        'custom_collections': {'definition_json', 'generated_media_info_json'},
        'media_metadata': {
            'genres_json', 'actors_json', 'directors_json', 
            'studios_json', 'countries_json', 'tags_json'
        },
        'watchlist': {'next_episode_to_air_json', 'missing_info_json'},
        'collections_info': {'missing_movies_json'},
        'resubscribe_rules': { # Add resubscribe_rules as it has JSONB fields
            'target_library_ids', 'resubscribe_audio_missing_languages',
            'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
            'resubscribe_effect_include'
        },
        'media_cleanup_tasks': { # Add media_cleanup_tasks as it has JSONB fields
            'versions_info_json'
        },
        'app_settings': { # Add app_settings as it has JSONB fields
            'value_json'
        }
    }

    # Add specific non-JSONB columns that might be lists and need string conversion
    LIST_TO_STRING_COLUMNS = {
        'actor_subscriptions': {'config_media_types'}
    }

    if not table_data:
        return [], []

    columns = list(table_data[0].keys())
    # 使用小写表名来匹配规则
    table_json_rules = JSONB_COLUMNS.get(table_name.lower(), set())
    table_list_to_string_rules = LIST_TO_STRING_COLUMNS.get(table_name.lower(), set())
    
    prepared_rows = []
    for row_dict in table_data:
        row_values = []
        for col_name in columns:
            value = row_dict.get(col_name)
            
            # ★ 核心逻辑: 如果列是 JSONB 类型且值非空，使用 Json 适配器包装 ★
            if col_name in table_json_rules and value is not None:
                # Json() 会告诉 psycopg2: "请将这个 Python 对象作为 JSON 处理"
                value = Json(value)
            elif col_name in table_list_to_string_rules and isinstance(value, list):
                # 如果是需要转换为字符串的列表，则进行转换
                value = ','.join(map(str, value))
            
            row_values.append(value)
        prepared_rows.append(tuple(row_values))
        
    return columns, prepared_rows

# --- 辅助函数 2: 数据库覆盖操作 (保持不变，但现在更可靠) ---
def _overwrite_table_data(cursor, table_name: str, columns: List[str], data: List[tuple]):
    """安全地清空并批量插入数据。"""
    db_table_name = table_name.lower()

    logger.warning(f"执行覆盖模式：将清空表 '{db_table_name}' 中的所有数据！")
    truncate_query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(
        table=sql.Identifier(db_table_name)
    )
    cursor.execute(truncate_query)

    insert_query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
        table=sql.Identifier(db_table_name),
        cols=sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    execute_values(cursor, insert_query, data, page_size=500)
    logger.info(f"成功向表 '{db_table_name}' 插入 {len(data)} 条记录。")

def task_sync_metadata_cache(processor: MediaProcessor, item_id: str, item_name: str):
    """
    任务：为单个媒体项同步元数据到 media_metadata 数据库表。
    """
    logger.info(f"任务开始：同步媒体元数据缓存 for '{item_name}' (ID: {item_id})")
    try:
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name)
        logger.info(f"任务成功：同步媒体元数据缓存 for '{item_name}'")
    except Exception as e:
        logger.error(f"任务失败：同步媒体元数据缓存 for '{item_name}' 时发生错误: {e}", exc_info=True)
        # 根据需要，可以决定是否要重新抛出异常以标记任务失败
        raise

def task_sync_assets(processor: MediaProcessor, item_id: str, update_description: str, sync_timestamp_iso: str):
    """
    任务：为单个媒体项同步图片和元数据文件到本地 override 目录。
    """
    logger.info(f"任务开始：同步资源文件 for ID: {item_id} (原因: {update_description})")
    try:
        # 注意：这里我们不再需要冷却检查，因为任务队列本身就防止了并发
        processor.sync_single_item_assets(item_id, update_description, sync_timestamp_iso)
        logger.info(f"任务成功：同步资源文件 for ID: {item_id}")
    except Exception as e:
        logger.error(f"任务失败：同步资源文件 for ID: {item_id} 时发生错误: {e}", exc_info=True)
        raise
# --- 主任务函数 (V4 - 纯PG重构版) ---
def task_import_database(processor, file_content: str, tables_to_import: List[str]):
    """
    【V4 - 纯净重构版】
    - 移除所有为兼容旧 SQLite 备份而设的数据清洗逻辑。
    - 假设备份文件源自本系统的 PostgreSQL 数据库，数据类型是干净的。
    - 使用 psycopg2.extras.Json 适配器专业地处理 JSONB 字段的插入。
    """
    task_name = "数据库恢复 (覆盖模式)"
    logger.info(f"后台任务开始：{task_name}，将恢复表: {tables_to_import}。")
    TABLE_TRANSLATIONS = {
        'person_identity_map': '演员映射表', 'actor_metadata': '演员元数据',
        'translation_cache': '翻译缓存', 'watchlist': '智能追剧列表',
        'actor_subscriptions': '演员订阅配置', 'tracked_actor_media': '已追踪的演员作品',
        'collections_info': '电影合集信息', 'processed_log': '已处理列表',
        'failed_log': '待复核列表', 'users': '用户账户',
        'custom_collections': '自建合集', 'media_metadata': '媒体元数据',
    }
    summary_lines = []
    conn = None
    try:
        backup = json.loads(file_content)
        backup_data = backup.get("data", {})

        # --- 新增的逻辑: 强制排序 tables_to_import ---
        # 定义表的依赖顺序。排在前面的表是父表或没有依赖的表。
        # 这里只列出明确需要优先处理的表。
        # 未列出的表将保持其在原始列表中的相对顺序，但会排在已定义依赖的表之后。
        # 例如：person_identity_map 必须在 actor_metadata 之前
        # 你可以根据实际情况添加更多依赖关系
        
        # 建立一个排序键函数
        def get_table_sort_key(table_name):
            table_name_lower = table_name.lower()
            if table_name_lower == 'person_identity_map':
                return 0  # 演员身份映射表，是 actor_metadata 和 actor_subscriptions 的基础
            elif table_name_lower == 'users':
                return 1  # 用户表，通常也是基础
            elif table_name_lower == 'actor_subscriptions':
                return 10 # 演员订阅配置，依赖于 person_identity_map (语义上)，被 tracked_actor_media 依赖
            elif table_name_lower == 'actor_metadata':
                return 11 # 演员元数据，依赖于 person_identity_map (外键)
            elif table_name_lower == 'tracked_actor_media':
                return 20 # 已追踪的演员作品，依赖于 actor_subscriptions (外键)
            # 其他表，目前没有明确的外键依赖，可以放在后面
            # 它们的相对顺序将由原始 tables_to_import 列表决定，如果它们有相同的默认权重
            elif table_name_lower in [
                'processed_log', 'failed_log', 'translation_cache',
                'collections_info', 'custom_collections', 'media_metadata', 'watchlist'
            ]:
                return 100 # 默认权重
            else:
                return 999 # 未知表，确保它排在最后，以防万一

        # 核心排序逻辑
        actual_tables_to_import = [
            t for t in tables_to_import if t in backup_data
        ]
        
        sorted_tables_to_import = sorted(actual_tables_to_import, key=get_table_sort_key)
        
        logger.info(f"调整后的导入顺序：{sorted_tables_to_import}")
        # --- 结束更新逻辑 ---

        with db_handler.get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("数据库事务已开始。")
                # 使用排序后的列表进行迭代
                for table_name in sorted_tables_to_import:
                    cn_name = TABLE_TRANSLATIONS.get(table_name.lower(), table_name)
                    table_data = backup_data.get(table_name, [])
                    if not table_data:
                        logger.debug(f"表 '{cn_name}' 在备份中没有数据，跳过。")
                        summary_lines.append(f"  - 表 '{cn_name}': 跳过 (备份中无数据)。")
                        continue

                    logger.info(f"正在处理表: '{cn_name}'，共 {len(table_data)} 行。")
                    columns, prepared_data = _prepare_data_for_insert(table_name, table_data)
                    _overwrite_table_data(cursor, table_name, columns, prepared_data)
                    summary_lines.append(f"  - 表 '{cn_name}': 成功恢复 {len(prepared_data)} 条记录。")
                
                logger.info("="*11 + " 数据库恢复摘要 " + "="*11)
                for line in summary_lines: logger.info(line)
                logger.info("="*36)
                conn.commit()
                logger.info("✅ 数据库事务已成功提交！所有选择的表已恢复。")
    except Exception as e:
        logger.error(f"数据库恢复任务发生严重错误，所有更改将回滚: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                logger.warning("数据库事务已回滚。")
            except Exception as rollback_e:
                logger.error(f"尝试回滚事务时发生额外错误: {rollback_e}")
# ★★★ 重新处理单个项目 ★★★
def task_reprocess_single_item(processor: MediaProcessor, item_id: str, item_name_for_ui: str):
    """
    【最终版 - 职责分离】后台任务。
    此版本负责在任务开始时设置“正在处理”的状态，并执行核心逻辑。
    """
    logger.debug(f"--- 后台任务开始执行 ({item_name_for_ui}) ---")
    
    try:
        # ✨ 关键修改：任务一开始，就用“正在处理”的状态覆盖掉旧状态
        task_manager.update_status_from_thread(0, f"正在处理: {item_name_for_ui}")

        # 现在才开始真正的工作
        processor.process_single_item(
            item_id, 
            force_reprocess_this_item=True,
            force_fetch_from_tmdb=True
        )
        # 任务成功完成后的状态更新会自动由任务队列处理，我们无需关心
        logger.debug(f"--- 后台任务完成 ({item_name_for_ui}) ---")

    except Exception as e:
        logger.error(f"后台任务处理 '{item_name_for_ui}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"处理失败: {item_name_for_ui}")
# --- 翻译演员任务 ---
def task_actor_translation_cleanup(processor):
    """
    【V3.2 - 并发写入版】
    1.  第一阶段：完整扫描一次Emby，将所有需要翻译的演员名和信息聚合到内存中。
    2.  第二阶段：将聚合好的列表按固定大小（50个）分批，依次进行“翻译 -> 并发写回”操作。
    -   使用 ThreadPoolExecutor 并发更新 Emby 演员信息，大幅提升写回速度。
    """
    task_name = "中文化演员名"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ======================================================================
        # 阶段 1: 扫描并聚合所有需要翻译的演员 (此部分逻辑不变)
        # ======================================================================
        task_manager.update_status_from_thread(0, "阶段 1/2: 正在扫描 Emby，收集所有待翻译演员...")
        
        names_to_translate = set()
        name_to_persons_map = {}
        
        person_generator = emby_handler.get_all_persons_from_emby(
            base_url=processor.emby_url,
            api_key=processor.emby_api_key,
            user_id=processor.emby_user_id,
            stop_event=processor.get_stop_event(),
            batch_size=500
        )

        total_scanned = 0
        for person_batch in person_generator:
            if processor.is_stop_requested():
                logger.info("任务在扫描阶段被用户中断。")
                task_manager.update_status_from_thread(100, "任务已中止。")
                return

            for person in person_batch:
                name = person.get("Name")
                if name and not utils.contains_chinese(name):
                    names_to_translate.add(name)
                    if name not in name_to_persons_map:
                        name_to_persons_map[name] = []
                    name_to_persons_map[name].append(person)
            
            total_scanned += len(person_batch)
            task_manager.update_status_from_thread(5, f"阶段 1/2: 已扫描 {total_scanned} 名演员...")

        if not names_to_translate:
            logger.info("扫描完成，没有发现需要翻译的演员名。")
            task_manager.update_status_from_thread(100, "任务完成，所有演员名都无需翻译。")
            return

        logger.info(f"扫描完成！共发现 {len(names_to_translate)} 个外文名需要翻译。")

        # ======================================================================
        # 阶段 2: 将聚合列表分批，依次进行“翻译 -> 并发写回”
        # ======================================================================
        all_names_list = list(names_to_translate)
        TRANSLATION_BATCH_SIZE = 50
        total_names_to_process = len(all_names_list)
        total_batches = (total_names_to_process + TRANSLATION_BATCH_SIZE - 1) // TRANSLATION_BATCH_SIZE
        
        total_updated_count = 0

        for i in range(0, total_names_to_process, TRANSLATION_BATCH_SIZE):
            if processor.is_stop_requested():
                logger.info("任务在翻译阶段被用户中断。")
                break

            current_batch_names = all_names_list[i:i + TRANSLATION_BATCH_SIZE]
            batch_num = (i // TRANSLATION_BATCH_SIZE) + 1
            
            progress = int(10 + (i / total_names_to_process) * 90)
            task_manager.update_status_from_thread(
                progress, 
                f"阶段 2/2: 正在翻译批次 {batch_num}/{total_batches} (已成功 {total_updated_count} 个)"
            )
            
            try:
                translation_map = processor.ai_translator.batch_translate(
                    texts=current_batch_names, mode="fast"
                )
            except Exception as e_trans:
                logger.error(f"翻译批次 {batch_num} 时发生错误: {e_trans}，将跳过此批次。")
                continue

            if not translation_map:
                logger.warning(f"翻译批次 {batch_num} 未能返回任何结果。")
                continue

            # ★★★ 核心修改：使用线程池并发写回当前批次的结果 ★★★
            batch_updated_count = 0
            
            # 1. 准备好所有需要更新的任务
            update_tasks = []
            for original_name, translated_name in translation_map.items():
                if not translated_name or original_name == translated_name: continue
                persons_to_update = name_to_persons_map.get(original_name, [])
                for person in persons_to_update:
                    update_tasks.append((person.get("Id"), translated_name))

            if not update_tasks:
                continue

            logger.info(f"  -> 批次 {batch_num}/{total_batches}: 翻译完成，准备并发写入 {len(update_tasks)} 个更新...")
            
            # 2. 使用 ThreadPoolExecutor 执行并发更新
            with ThreadPoolExecutor(max_workers=10) as executor:
                # 提交所有更新任务
                future_to_task = {
                    executor.submit(
                        emby_handler.update_person_details,
                        person_id=task[0],
                        new_data={"Name": task[1]},
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    ): task for task in update_tasks
                }

                # 收集结果
                for future in as_completed(future_to_task):
                    if processor.is_stop_requested():
                        # 如果任务被中止，我们可以尝试取消未完成的 future，但最简单的是直接跳出
                        break
                    
                    try:
                        success = future.result()
                        if success:
                            batch_updated_count += 1
                    except Exception as exc:
                        task_info = future_to_task[future]
                        logger.error(f"并发更新演员 (ID: {task_info[0]}) 时线程内发生错误: {exc}")

            total_updated_count += batch_updated_count
            
            if batch_updated_count > 0:
                logger.info(f"  -> ✅ 批次 {batch_num}/{total_batches} 并发写回完成，成功更新 {batch_updated_count} 个演员名。")
        
        # ======================================================================
        # 阶段 3: 任务结束 (此部分逻辑不变)
        # ======================================================================
        final_message = f"任务完成！共成功翻译并更新了 {total_updated_count} 个演员名。"
        if processor.is_stop_requested():
            final_message = f"任务已中断。本次运行成功翻译并更新了 {total_updated_count} 个演员名。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行演员翻译任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
# ★★★ 重新处理所有待复核项 ★★★
def task_reprocess_all_review_items(processor: MediaProcessor):
    """
    【已升级】后台任务：遍历所有待复核项并逐一以“强制在线获取”模式重新处理。
    """
    logger.trace("--- 开始执行“重新处理所有待复核项”任务 [强制在线获取模式] ---")
    try:
        # +++ 核心修改 1：同时查询 item_id 和 item_name +++
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            # 从 failed_log 中同时获取 ID 和 Name
            cursor.execute("SELECT item_id, item_name FROM failed_log")
            # 将结果保存为一个字典列表，方便后续使用
            all_items = [{'id': row['item_id'], 'name': row['item_name']} for row in cursor.fetchall()]
        
        total = len(all_items)
        if total == 0:
            logger.info("待复核列表中没有项目，任务结束。")
            task_manager.update_status_from_thread(100, "待复核列表为空。")
            return

        logger.info(f"共找到 {total} 个待复核项需要以“强制在线获取”模式重新处理。")

        # +++ 核心修改 2：在循环中解包 item_id 和 item_name +++
        for i, item in enumerate(all_items):
            if processor.is_stop_requested():
                logger.info("任务被中止。")
                break
            
            item_id = item['id']
            item_name = item['name'] or f"ItemID: {item_id}" # 如果名字为空，提供一个备用名

            task_manager.update_status_from_thread(int((i/total)*100), f"正在重新处理 {i+1}/{total}: {item_name}")
            
            # +++ 核心修改 3：传递所有必需的参数 +++
            task_reprocess_single_item(processor, item_id, item_name)
            
            # 每个项目之间稍作停顿
            time.sleep(2) 

    except Exception as e:
        logger.error(f"重新处理所有待复核项时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")
# ★★★ 同步覆盖缓存的任务函数 ★★★
def task_full_image_sync(processor: MediaProcessor, force_full_update: bool = False):
    """
    后台任务：调用 processor 的方法来同步所有图片。
    新增 force_full_update 参数以支持深度模式。
    """
    # 直接把回调函数和新参数传进去
    processor.sync_all_media_assets(
        update_status_callback=task_manager.update_status_from_thread,
        force_full_update=force_full_update
    )
# ✨ 辅助函数，并发刷新合集使用
def _process_single_collection_concurrently(collection_data: dict, tmdb_api_key: str) -> dict:
    """
    【V5 - 逻辑与类型双重修复版】
    在单个线程中处理单个电影合集的所有逻辑。
    """
    collection_id = collection_data['Id']
    collection_name = collection_data.get('Name', '')
    today_str = datetime.now().strftime('%Y-%m-%d')
    item_type = 'Movie'
    
    # ★★★ 核心修复 1/2: 强制将所有来自Emby的ID转换为字符串集合，确保类型统一 ★★★
    emby_movie_tmdb_ids = {str(id) for id in collection_data.get("ExistingMovieTmdbIds", [])}
    
    in_library_count = len(emby_movie_tmdb_ids)
    status, has_missing = "ok", False
    provider_ids = collection_data.get("ProviderIds", {})
    all_movies_with_status = []
    
    tmdb_id = provider_ids.get("TmdbCollection") or provider_ids.get("TmdbCollectionId") or provider_ids.get("Tmdb")

    if not tmdb_id:
        status = "unlinked"
    else:
        details = tmdb_handler.get_collection_details_tmdb(int(tmdb_id), tmdb_api_key)
        if not details or "parts" not in details:
            status = "tmdb_error"
        else:
            # ★★★ 核心修复 2/2: 修正数据库读取逻辑 ★★★
            previous_movies_map = {}
            with db_handler.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = %s", (collection_id,))
                row = cursor.fetchone()
                # 1. 使用字典键 'missing_movies_json' 访问，而不是索引 [0]
                # 2. psycopg2 已经自动解析了 JSONB 字段，无需再 json.loads
                if row and row.get('missing_movies_json'):
                    try:
                        previous_movies_map = {str(m['tmdb_id']): m for m in row['missing_movies_json']}
                    except (TypeError, KeyError): 
                        logger.warning(f"解析合集 '{collection_name}' 的历史数据时格式不兼容，将忽略。")
            
            for movie in details.get("parts", []):
                # 确保 TMDB ID 也为字符串，与上面创建的集合类型一致
                movie_tmdb_id = str(movie.get("id"))
                
                # 跳过没有发布日期的电影，它们通常是未完成的项目
                if not movie.get("release_date"): 
                    continue

                movie_status = "unknown"
                if movie_tmdb_id in emby_movie_tmdb_ids:
                    movie_status = "in_library"
                elif movie.get("release_date", '') > today_str:
                    movie_status = "unreleased"
                elif previous_movies_map.get(movie_tmdb_id, {}).get('status') == 'subscribed':
                    movie_status = "subscribed"
                else:
                    movie_status = "missing"

                all_movies_with_status.append({
                    "tmdb_id": movie_tmdb_id, "title": movie.get("title", ""), 
                    "release_date": movie.get("release_date"), "poster_path": movie.get("poster_path"), 
                    "status": movie_status
                })
            
            if any(m['status'] == 'missing' for m in all_movies_with_status):
                has_missing = True
                status = "has_missing"

    image_tag = collection_data.get("ImageTags", {}).get("Primary")
    poster_path = f"/Items/{collection_id}/Images/Primary?tag={image_tag}" if image_tag else None

    return {
        "emby_collection_id": collection_id, "name": collection_name, 
        "tmdb_collection_id": tmdb_id, "item_type": item_type,
        "status": status, "has_missing": has_missing, 
        "missing_movies_json": json.dumps(all_movies_with_status, ensure_ascii=False), 
        "last_checked_at": datetime.now(timezone.utc), 
        "poster_path": poster_path, 
        "in_library_count": in_library_count
    }
# ★★★ 刷新合集的后台任务函数 ★★★
def task_refresh_collections(processor: MediaProcessor):
    """
    【V2 - PG语法修正版】
    - 修复了数据库批量写入时使用 SQLite 特有语法 INSERT OR REPLACE 的问题。
    - 改为使用 PostgreSQL 标准的 ON CONFLICT ... DO UPDATE 语法，确保数据能被正确地插入或更新。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    task_manager.update_status_from_thread(0, "正在获取 Emby 合集列表...")
    try:
        emby_collections = emby_handler.get_all_collections_with_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id
        )
        if emby_collections is None: raise RuntimeError("从 Emby 获取合集列表失败")

        total = len(emby_collections)
        task_manager.update_status_from_thread(5, f"共找到 {total} 个合集，准备开始并发处理...")

        # 清理数据库中已不存在的合集
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            emby_current_ids = {c['Id'] for c in emby_collections}
            # ★★★ 语法修正：PostgreSQL 的 cursor.fetchall() 返回字典列表，需要正确提取 ★★★
            cursor.execute("SELECT emby_collection_id FROM collections_info")
            db_known_ids = {row['emby_collection_id'] for row in cursor.fetchall()}
            deleted_ids = db_known_ids - emby_current_ids
            if deleted_ids:
                # executemany 需要一个元组列表
                cursor.executemany("DELETE FROM collections_info WHERE emby_collection_id = %s", [(id,) for id in deleted_ids])
            conn.commit()

        tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        if not tmdb_api_key: raise RuntimeError("未配置 TMDb API Key")

        processed_count = 0
        all_results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_process_single_collection_concurrently, collection, tmdb_api_key): collection for collection in emby_collections}
            
            for future in as_completed(futures):
                if processor.is_stop_requested():
                    for f in futures: f.cancel()
                    break
                
                collection_name = futures[future].get('Name', '未知合集')
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"处理合集 '{collection_name}' 时线程内发生错误: {e}", exc_info=True)
                
                processed_count += 1
                progress = 10 + int((processed_count / total) * 90)
                task_manager.update_status_from_thread(progress, f"处理中: {collection_name[:20]}... ({processed_count}/{total})")

        if processor.is_stop_requested():
            logger.warning("任务被用户中断，部分数据可能未被处理。")
        
        if all_results:
            logger.info(f"  -> 并发处理完成，准备将 {len(all_results)} 条结果写入数据库...")
            with db_handler.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION;")
                try:
                    # ★★★ 核心修复：将 INSERT OR REPLACE 改为 ON CONFLICT ... DO UPDATE ★★★
                    # 1. 定义所有列和占位符
                    cols = all_results[0].keys()
                    cols_str = ", ".join(cols)
                    placeholders_str = ", ".join([f"%({k})s" for k in cols]) # 使用 %(key)s 格式
                    
                    # 2. 定义冲突时的更新规则
                    update_cols = [f"{col} = EXCLUDED.{col}" for col in cols if col != 'emby_collection_id']
                    update_str = ", ".join(update_cols)
                    
                    # 3. 构建最终的SQL
                    sql = f"""
                        INSERT INTO collections_info ({cols_str})
                        VALUES ({placeholders_str})
                        ON CONFLICT (emby_collection_id) DO UPDATE SET {update_str}
                    """
                    
                    # 4. 使用 executemany 执行
                    cursor.executemany(sql, all_results)
                    conn.commit()
                    logger.info("  -> ✅ 数据库写入成功！")
                except Exception as e_db:
                    logger.error(f"数据库批量写入时发生错误: {e_db}", exc_info=True)
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"刷新合集任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")
# ★★★ 带智能预判的自动订阅任务 ★★★
def task_auto_subscribe(processor: MediaProcessor):
    """
    【V6 - 全局配额终极版】
    - 将所有订阅行为（电影、剧集、自定义合集）都纳入了全局每日配额管理。
    - 在每次订阅前检查配额，并在订阅成功后消耗配额。
    - 当配额用尽时，任务会提前、安全地结束。
    """
    task_name = "智能订阅缺失"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (全局配额模式) ---")
    
    task_manager.update_status_from_thread(0, "正在启动智能订阅任务...")
    
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("智能订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        today = date.today()
        task_manager.update_status_from_thread(10, "智能订阅已启动...")
        successfully_subscribed_items = []
        quota_exhausted = False # 新增一个标志，用于记录配额是否用尽

        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()

            # ★★★ 1. 处理原生电影合集 (collections_info) ★★★
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(20, "正在检查原生电影合集...")
                sql_query_native_movies = "SELECT * FROM collections_info WHERE status = 'has_missing' AND missing_movies_json IS NOT NULL AND missing_movies_json != '[]'"
                cursor.execute(sql_query_native_movies)
                native_collections_to_check = cursor.fetchall()
                logger.info(f"  -> 找到 {len(native_collections_to_check)} 个有缺失影片的原生合集。")
                
                for collection in native_collections_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    
                    movies_to_keep = []
                    all_movies = collection['missing_movies_json']
                    movies_changed = False
                    
                    for movie in all_movies:
                        if processor.is_stop_requested(): break
                        
                        if movie.get('status') == 'missing':
                            release_date_str = movie.get('release_date')
                            if not release_date_str:
                                movies_to_keep.append(movie)
                                continue
                            try:
                                release_date = datetime.strptime(release_date_str.strip(), '%Y-%m-%d').date()
                            except (ValueError, TypeError):
                                movies_to_keep.append(movie)
                                continue

                            if release_date <= today:
                                # ★★★ 核心修改 1/3: 在订阅前检查配额 ★★★
                                current_quota = db_handler.get_subscription_quota()
                                if current_quota <= 0:
                                    quota_exhausted = True
                                    logger.warning("每日订阅配额已用尽，原生合集检查提前结束。")
                                    movies_to_keep.append(movie) # 把当前未处理的电影加回去
                                    break # 跳出内层循环

                                if moviepilot_handler.subscribe_movie_to_moviepilot(movie, config_manager.APP_CONFIG):
                                    db_handler.decrement_subscription_quota() # 消耗配额
                                    successfully_subscribed_items.append(f"电影《{movie['title']}》")
                                    movies_changed = True
                                    movie['status'] = 'subscribed'
                                movies_to_keep.append(movie)
                            else:
                                movies_to_keep.append(movie)
                        else:
                            movies_to_keep.append(movie)
                            
                    if movies_changed:
                        new_missing_json = json.dumps(movies_to_keep)
                        new_status = 'ok' if not any(m.get('status') == 'missing' for m in movies_to_keep) else 'has_missing'
                        cursor.execute("UPDATE collections_info SET missing_movies_json = %s, status = %s WHERE emby_collection_id = %s", (new_missing_json, new_status, collection['emby_collection_id']))

            # --- 2. 处理智能追剧 ---
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(60, "正在检查缺失的剧集...")
                sql_query = "SELECT * FROM watchlist WHERE status IN ('Watching', 'Paused') AND missing_info_json IS NOT NULL AND missing_info_json != '[]'"
                cursor.execute(sql_query)
                series_to_check = cursor.fetchall()
                
                for series in series_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    series_name = series['item_name']
                    logger.info(f"  -> 正在检查: 《{series_name}》")
                    try:
                        missing_info = series['missing_info_json']
                        missing_seasons = missing_info.get('missing_seasons', [])
                        if not missing_seasons: continue
                        
                        seasons_to_keep = []
                        seasons_changed = False
                        for season in missing_seasons:
                            if processor.is_stop_requested(): break
                            
                            air_date_str = season.get('air_date')
                            if not air_date_str:
                                seasons_to_keep.append(season)
                                continue
                            try:
                                season_date = datetime.strptime(air_date_str.strip(), '%Y-%m-%d').date()
                            except (ValueError, TypeError):
                                seasons_to_keep.append(season)
                                continue

                            if season_date <= today:
                                # ★★★ 核心修改 2/3: 在订阅前检查配额 ★★★
                                current_quota = db_handler.get_subscription_quota()
                                if current_quota <= 0:
                                    quota_exhausted = True
                                    logger.warning("每日订阅配额已用尽，追剧检查提前结束。")
                                    seasons_to_keep.append(season)
                                    break

                                success = moviepilot_handler.subscribe_series_to_moviepilot(dict(series), season['season_number'], config_manager.APP_CONFIG)
                                if success:
                                    db_handler.decrement_subscription_quota() # 消耗配额
                                    successfully_subscribed_items.append(f"《{series['item_name']}》第 {season['season_number']} 季")
                                    seasons_changed = True
                                else:
                                    seasons_to_keep.append(season)
                            else:
                                seasons_to_keep.append(season)
                                
                        if seasons_changed:
                            missing_info['missing_seasons'] = seasons_to_keep
                            cursor.execute("UPDATE watchlist SET missing_info_json = %s WHERE item_id = %s", (json.dumps(missing_info), series['item_id']))
                    except Exception as e_series:
                        logger.error(f"【智能订阅-剧集】处理剧集 '{series['item_name']}' 时出错: {e_series}")

            # ★★★ 3. 处理自定义合集 (custom_collections) ★★★
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(70, "正在检查自定义榜单合集...")
                sql_query_custom_collections = "SELECT * FROM custom_collections WHERE type = 'list' AND health_status = 'has_missing' AND generated_media_info_json IS NOT NULL AND generated_media_info_json != '[]'"
                cursor.execute(sql_query_custom_collections)
                custom_collections_to_check = cursor.fetchall()
                
                for collection in custom_collections_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    collection_id = collection['id']
                    collection_name = collection['name']
                    try:
                        definition = collection['definition_json']
                        all_media = collection['generated_media_info_json']
                        
                        item_type_from_db = definition.get('item_type', 'Movie')
                        authoritative_type = 'Movie'
                        if isinstance(item_type_from_db, list) and item_type_from_db:
                            authoritative_type = item_type_from_db[0]
                        elif isinstance(item_type_from_db, str):
                            authoritative_type = item_type_from_db
                        if authoritative_type not in ['Movie', 'Series']:
                            authoritative_type = 'Movie'
                            
                        media_to_keep = []
                        media_changed = False
                        for media_item in all_media:
                            if processor.is_stop_requested(): break
                            
                            if media_item.get('status') == 'missing':
                                release_date_str = media_item.get('release_date')
                                if not release_date_str:
                                    media_to_keep.append(media_item)
                                    continue
                                try:
                                    release_date = datetime.strptime(release_date_str.strip(), '%Y-%m-%d').date()
                                except (ValueError, TypeError):
                                    media_to_keep.append(media_item)
                                    continue

                                if release_date <= today:
                                    # ★★★ 核心修改 3/3: 在订阅前检查配额 ★★★
                                    current_quota = db_handler.get_subscription_quota()
                                    if current_quota <= 0:
                                        quota_exhausted = True
                                        logger.warning("每日订阅配额已用尽，自定义合集检查提前结束。")
                                        media_to_keep.append(media_item)
                                        break
                                        
                                    success = False
                                    media_title = media_item.get('title', '未知标题')
                                    if authoritative_type == 'Movie':
                                        success = moviepilot_handler.subscribe_movie_to_moviepilot(media_item, config_manager.APP_CONFIG)
                                    elif authoritative_type == 'Series':
                                        series_info = { "item_name": media_title, "tmdb_id": media_item.get('tmdb_id') }
                                        success = moviepilot_handler.subscribe_series_to_moviepilot(series_info, season_number=None, config=config_manager.APP_CONFIG)
                                    
                                    if success:
                                        db_handler.decrement_subscription_quota() # 消耗配额
                                        successfully_subscribed_items.append(f"{authoritative_type}《{media_title}》")
                                        media_changed = True
                                        media_item['status'] = 'subscribed'
                                    media_to_keep.append(media_item)
                                else:
                                    media_to_keep.append(media_item)
                            else:
                                media_to_keep.append(media_item)
                                
                        if media_changed:
                            new_missing_json = json.dumps(media_to_keep, ensure_ascii=False)
                            new_missing_count = sum(1 for m in media_to_keep if m.get('status') == 'missing')
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            cursor.execute(
                                "UPDATE custom_collections SET generated_media_info_json = %s, health_status = %s, missing_count = %s WHERE id = %s", 
                                (new_missing_json, new_health_status, new_missing_count, collection_id)
                            )
                    except Exception as e_coll:
                        logger.error(f"  -> 处理自定义合集 '{collection_name}' 时发生错误: {e_coll}", exc_info=True)

            # ★★★ 4. 处理演员订阅 (tracked_actor_media) ★★★
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(80, "正在检查演员订阅的缺失作品...")
                sql_query_actors = "SELECT * FROM tracked_actor_media WHERE status = 'MISSING'"
                cursor.execute(sql_query_actors)
                actor_media_to_check = cursor.fetchall()
                
                logger.info(f"  -> 找到 {len(actor_media_to_check)} 个来自演员订阅的缺失作品。")

                for media_item in actor_media_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    
                    # 再次确认发布日期，以防万一
                    release_date = media_item.get('release_date')
                    if not release_date or release_date > today:
                        continue

                    # 检查配额
                    current_quota = db_handler.get_subscription_quota()
                    if current_quota <= 0:
                        quota_exhausted = True
                        logger.warning("每日订阅配额已用尽，演员订阅检查提前结束。")
                        break
                    
                    success = False
                    media_title = media_item.get('title', '未知标题')
                    media_tmdb_id = media_item.get('tmdb_media_id')
                    
                    if media_item['media_type'] == 'Movie':
                        movie_info = {'title': media_title, 'tmdb_id': media_tmdb_id}
                        success = moviepilot_handler.subscribe_movie_to_moviepilot(movie_info, config_manager.APP_CONFIG)
                    elif media_item['media_type'] == 'Series':
                        series_info = {"item_name": media_title, "tmdb_id": media_tmdb_id}
                        success = moviepilot_handler.subscribe_series_to_moviepilot(series_info, season_number=None, config=config_manager.APP_CONFIG)
                    
                    if success:
                        db_handler.decrement_subscription_quota()
                        successfully_subscribed_items.append(f"演员作品《{media_title}》")
                        # 直接更新状态
                        cursor.execute("UPDATE tracked_actor_media SET status = 'SUBSCRIBED' WHERE id = %s", (media_item['id'],))

            conn.commit()

        summary = ""
        if successfully_subscribed_items:
            summary = "✅ 任务完成！已自动订阅: " + ", ".join(successfully_subscribed_items)
        else:
            summary = "任务完成：本次运行没有发现符合自动订阅条件的媒体。"
        
        if quota_exhausted:
            summary += " (注意：每日订阅配额已用尽，部分项目可能未处理)"

        logger.info(summary)
        task_manager.update_status_from_thread(100, summary)

    except Exception as e:
        logger.error(f"智能订阅任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")
# ✨✨✨ 一键添加所有剧集到追剧列表的任务 ✨✨✨
def task_add_all_series_to_watchlist(processor: MediaProcessor):
    """
    【V3 - 并发获取与批量写入 PG 版】
    - 使用5个并发线程，分别从不同的媒体库获取剧集，提升 Emby 数据拉取速度。
    - 将数据库操作改为单次批量写入（execute_values），大幅提升数据库性能。
    - 使用 RETURNING 子句精确统计实际新增的剧集数量。
    """
    task_name = "一键扫描全库剧集 (并发版)"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        emby_url = processor.emby_url
        emby_api_key = processor.emby_api_key
        emby_user_id = processor.emby_user_id
        
        library_ids_to_process = config_manager.APP_CONFIG.get('emby_libraries_to_process', [])
        
        if not library_ids_to_process:
            logger.info("未在配置中指定媒体库，将自动扫描所有媒体库...")
            all_libraries = emby_handler.get_emby_libraries(emby_url, emby_api_key, emby_user_id)
            if all_libraries:
                library_ids_to_process = [
                    lib['Id'] for lib in all_libraries 
                    if lib.get('CollectionType') in ['tvshows', 'mixed']
                ]
                logger.info(f"将扫描以下剧集库: {[lib['Name'] for lib in all_libraries if lib.get('CollectionType') in ['tvshows', 'mixed']]}")
            else:
                logger.warning("未能从 Emby 获取到任何媒体库。")
        
        if not library_ids_to_process:
            task_manager.update_status_from_thread(100, "任务完成：没有找到可供扫描的剧集媒体库。")
            return

        # --- 并发获取 Emby 剧集 ---
        task_manager.update_status_from_thread(10, f"正在从 {len(library_ids_to_process)} 个媒体库并发获取剧集...")
        all_series = []
        
        def fetch_series_from_library(library_id: str) -> List[Dict[str, Any]]:
            """线程工作函数：从单个媒体库获取剧集"""
            try:
                items = emby_handler.get_emby_library_items(
                    base_url=emby_url, api_key=emby_api_key, user_id=emby_user_id,
                    library_ids=[library_id], media_type_filter="Series"
                )
                return items if items is not None else []
            except Exception as e:
                logger.error(f"从媒体库 {library_id} 获取数据时出错: {e}")
                return []

        # 使用线程池并发执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有任务
            future_to_library = {executor.submit(fetch_series_from_library, lib_id): lib_id for lib_id in library_ids_to_process}
            for future in concurrent.futures.as_completed(future_to_library):
                try:
                    result = future.result()
                    all_series.extend(result)
                except Exception as exc:
                    library_id = future_to_library[future]
                    logger.error(f"媒体库 {library_id} 的任务在执行中产生异常: {exc}")

        if not all_series:
            raise RuntimeError("从 Emby 获取剧集列表失败，请检查网络和配置。")

        total = len(all_series)
        task_manager.update_status_from_thread(30, f"共找到 {total} 部剧集，正在筛选...")
        
        series_to_insert = []
        for series in all_series:
            tmdb_id = series.get("ProviderIds", {}).get("Tmdb")
            item_name = series.get("Name")
            item_id = series.get("Id")
            if tmdb_id and item_name and item_id:
                # 准备元组用于批量插入
                series_to_insert.append(
                    (item_id, tmdb_id, item_name, "Series", 'Watching')
                )

        if not series_to_insert:
            task_manager.update_status_from_thread(100, "任务完成：找到的剧集均缺少TMDb ID，无法添加。")
            return

        added_count = 0
        total_to_add = len(series_to_insert)
        task_manager.update_status_from_thread(60, f"筛选出 {total_to_add} 部有效剧集，准备批量写入数据库...")
        
        # --- 高效批量写入数据库 ---
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                # ★★★ 核心修复：使用 execute_values 进行高效批量插入 ★★★
                sql_insert = """
                    INSERT INTO watchlist (item_id, tmdb_id, item_name, item_type, status)
                    VALUES %s
                    ON CONFLICT (item_id) DO NOTHING
                    RETURNING item_id
                """
                # execute_values 会自动将数据列表转换成 (v1,v2), (v1,v2) 的形式
                inserted_ids = execute_values(
                    cursor, sql_insert, series_to_insert, 
                    template=None, page_size=1000, fetch=True
                )
                added_count = len(inserted_ids)
                conn.commit()
            except Exception as e_db:
                conn.rollback()
                raise RuntimeError(f"数据库批量写入时发生错误: {e_db}")

        scan_complete_message = f"扫描完成！共发现 {total} 部剧集，新增 {added_count} 部。"
        logger.info(scan_complete_message)
        
        # --- 后续任务链逻辑 (保持不变) ---
        if added_count > 0:
            logger.info("--- 任务链：即将自动触发【检查所有在追剧集】任务 ---")
            task_manager.update_status_from_thread(99, "扫描完成，正在启动追剧检查...")
            time.sleep(2)
            try:
                watchlist_proc = extensions.watchlist_processor_instance
                if watchlist_proc:
                    watchlist_proc.run_regular_processing_task_concurrent(
                        progress_callback=task_manager.update_status_from_thread,
                        item_id=None
                    )
                    final_message = "自动化流程完成：扫描与追剧检查均已结束。"
                    task_manager.update_status_from_thread(100, final_message)
                else:
                    raise RuntimeError("WatchlistProcessor 未初始化，无法执行链式任务。")
            except Exception as e_chain:
                 logger.error(f"执行链式任务【检查所有在追剧集】时失败: {e_chain}", exc_info=True)
                 task_manager.update_status_from_thread(-1, f"链式任务失败: {e_chain}")
        else:
            final_message = f"任务完成！共扫描到 {total} 部剧集，没有发现可新增的剧集。"
            logger.info(final_message)
            task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
# --- 任务链 ---
def task_run_chain(processor: MediaProcessor, task_sequence: list):
    """
    【V5 - 及时刹车版】
    - 使用一个独立的计时器线程来触发全局停止信号，实现对耗时子任务的及时中断。
    """
    task_name = "自动化任务链"
    total_tasks = len(task_sequence)
    logger.info(f"--- '{task_name}' 已启动，共包含 {total_tasks} 个子任务 ---")
    task_manager.update_status_from_thread(0, f"任务链启动，共 {total_tasks} 个任务。")

    # --- 准备计时器和停止信号 ---
    max_runtime_minutes = processor.config.get(constants.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES, 0)
    timeout_seconds = max_runtime_minutes * 60 if max_runtime_minutes > 0 else None
    
    processor.clear_stop_signal()
    timeout_triggered = threading.Event()

    def timeout_watcher():
        if timeout_seconds:
            logger.info(f"任务链运行时长限制为 {max_runtime_minutes} 分钟，计时器已启动。")
            time.sleep(timeout_seconds)
            
            if not processor.is_stop_requested():
                logger.warning(f"任务链达到 {max_runtime_minutes} 分钟的运行时长限制，将发送停止信号...")
                timeout_triggered.set()
                processor.signal_stop()

    # 启动计时器线程
    timer_thread = threading.Thread(target=timeout_watcher, daemon=True)
    timer_thread.start()

    try:
        # --- 主任务循环 ---
        registry = get_task_registry()
        for i, task_key in enumerate(task_sequence):
            if processor.is_stop_requested():
                if not timeout_triggered.is_set():
                    logger.warning(f"'{task_name}' 被用户手动中止。")
                break

            task_info = registry.get(task_key)
            if not task_info:
                logger.error(f"任务链警告：在注册表中未找到任务 '{task_key}'，已跳过。")
                continue

            try:
                # ▼▼▼ 核心修复 1/2：我们不再需要 processor_type 了 ▼▼▼
                task_function, task_description, _ = task_info
            except ValueError:
                logger.error(f"任务链错误：任务 '{task_key}' 的注册信息格式不正确，已跳过。")
                continue

            progress = int((i / total_tasks) * 100)
            status_message = f"({i+1}/{total_tasks}) 正在执行: {task_description}"
            logger.info(f"--- {status_message} ---")
            task_manager.update_status_from_thread(progress, status_message)

            try:
                # ▼▼▼ 核心修复 2/2：直接将 task_run_chain 收到的 processor 实例传递给子任务函数 ▼▼▼
                # 所有子任务函数（如 task_run_full_scan）的第一个参数都是 processor
                task_function(processor)
                time.sleep(1)

            except Exception as e:
                # 检查异常是否是由于我们的“刹车”引起的
                if isinstance(e, InterruptedError):
                    logger.info(f"子任务 '{task_description}' 响应停止信号，已中断。")
                    # 不需要再做什么，外层循环会处理
                else:
                    error_message = f"任务链中的子任务 '{task_description}' 执行失败: {e}"
                    logger.error(error_message, exc_info=True)
                    task_manager.update_status_from_thread(progress, f"子任务'{task_description}'失败，继续...")
                    time.sleep(3)
                continue

    finally:
        # --- 任务结束后的清理和状态报告 ---
        final_message = f"'{task_name}' 执行完毕。"
        if processor.is_stop_requested():
            if timeout_triggered.is_set():
                final_message = f"'{task_name}' 已达最长运行时限，自动结束。"
            else:
                final_message = f"'{task_name}' 已被用户手动中止。"
        
        logger.info(f"--- {final_message} ---")
        task_manager.update_status_from_thread(100, final_message)
        
        # 确保在任务链结束后，清除停止信号，以免影响下一个手动任务
        processor.clear_stop_signal()
# --- 任务注册表 ---
def get_task_registry(context: str = 'all'):
    """
    【V4 - 最终完整版】
    返回一个包含所有可执行任务的字典。
    每个任务的定义现在是一个四元组：(函数, 描述, 处理器类型, 是否适合任务链)。
    """
    # 完整的任务注册表
    # 格式: 任务Key: (任务函数, 任务描述, 处理器类型, 是否适合在任务链中运行)
    full_registry = {
        'task-chain': (task_run_chain, "自动化任务链", 'media', False), # 任务链本身不能嵌套

        # --- 适合任务链的常规任务 ---
        'sync-person-map': (task_sync_person_map, "同步演员数据", 'media', True),
        'enrich-aliases': (task_enrich_aliases, "演员数据补充", 'media', True),
        'populate-metadata': (task_populate_metadata_cache, "同步媒体数据", 'media', True),
        'full-scan': (task_run_full_scan, "中文化角色名", 'media', True),
        'actor-cleanup': (task_actor_translation_cleanup, "中文化演员名", 'media', True),
        'process-watchlist': (task_process_watchlist, "刷新智能追剧", 'watchlist', True),
        'refresh-collections': (task_refresh_collections, "刷新原生合集", 'media', True),
        'custom-collections': (task_process_all_custom_collections, "刷新自建合集", 'media', True),
        'update-resubscribe-cache': (task_update_resubscribe_cache, "刷新洗版状态", 'media', True),
        'actor-tracking': (task_process_actor_subscriptions, "演员订阅扫描", 'actor', True),
        'auto-subscribe': (task_auto_subscribe, "智能订阅缺失", 'media', True),
        'sync-images-map': (task_full_image_sync, "覆盖缓存备份", 'media', True),
        'resubscribe-library': (task_resubscribe_library, "媒体洗版订阅", 'media', True),
        'generate-all-covers': (task_generate_all_covers, "生成原生封面", 'media', True),
        'generate-custom-collection-covers': (task_generate_all_custom_collection_covers, "生成合集封面", 'media', True),
        'purge-ghost-actors': (task_purge_ghost_actors, "删除幽灵演员", 'media', True),
        'sync-all-user-data': (task_sync_all_user_data, "同步用户数据", 'media', True),
        

        # --- 不适合任务链的、需要特定参数的任务 ---
        'process_all_custom_collections': (task_process_all_custom_collections, "生成所有自建合集", 'media', False),
        'process-single-custom-collection': (task_process_custom_collection, "生成单个自建合集", 'media', False),
        'scan-cleanup-issues': (task_scan_for_cleanup_issues, "扫描媒体重复项", 'media', False),
        'revival-check': (task_run_revival_check, "检查剧集复活", 'watchlist', False),
        'task_apply_main_cast_to_episodes': (task_apply_main_cast_to_episodes, "轻量化同步分集演员表", 'media', False),
    }

    if context == 'chain':
        # ★★★ 核心修复 1/2：使用第四个元素 (布尔值) 来进行过滤 ★★★
        # 这将完美恢复您原来的功能
        return {
            key: (info[0], info[1]) 
            for key, info in full_registry.items() 
            if info[3]  # info[3] 就是那个 True/False 标志
        }
    
    # ★★★ 核心修复 2/2：默认情况下，返回前三个元素 ★★★
    # 这确保了“万用插座”API (/api/tasks/run) 能够正确解包，无需修改
    return {
        key: (info[0], info[1], info[2]) 
        for key, info in full_registry.items()
    }

# ★★★ 用于统一处理自定义合集的角标逻辑 ★★★
def _get_cover_badge_text_for_collection(collection_db_info: Dict[str, Any]) -> Any:
    """
    根据自定义合集的数据库信息，智能判断并返回用于封面角标的参数。
    - 如果是特定类型的榜单，返回对应的中文字符串。
    - 否则，返回该合集在媒体库中实际包含的项目数量。
    """
    # 默认情况下，角标就是媒体库中的项目数量
    item_count_to_pass = collection_db_info.get('in_library_count', 0)
    
    collection_type = collection_db_info.get('type')
    definition = collection_db_info.get('definition_json', {})

    # 只有榜单(list)类型才需要特殊处理角标文字
    if collection_type == 'list':
        url = definition.get('url', '')
        # 根据URL或其他特征判断榜单来源
        if url.startswith('maoyan://'):
            return '猫眼'
        elif 'douban.com/doulist' in url:
            return '豆列'
        elif 'themoviedb.org/discover/' in url:
            return '探索'
        else:
            # 对于其他所有榜单类型，统一显示为'榜单'
            return '榜单'
            
    # 如果不是榜单类型，或者榜单类型不匹配任何特殊规则，则返回数字角标
    return item_count_to_pass

# ★★★ 一键生成所有合集的后台任务 ★★★
def task_process_all_custom_collections(processor: MediaProcessor):
    """
    【V7 - 榜单类型识别 & 精确封面参数】
    """
    task_name = "生成所有自建合集"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # ... (前面的代码都不变，直到 for 循环) ...
        task_manager.update_status_from_thread(0, "正在获取所有启用的合集定义...")
        active_collections = db_handler.get_all_active_custom_collections()
        if not active_collections:
            logger.info("  -> 没有找到任何已启用的自定义合集，任务结束。")
            task_manager.update_status_from_thread(100, "没有已启用的合集。")
            return
        
        total = len(active_collections)
        logger.info(f"  -> 共找到 {total} 个已启用的自定义合集需要处理。")

        task_manager.update_status_from_thread(2, "正在从Emby获取全库媒体数据...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids: raise ValueError("未在配置中指定要处理的媒体库。")
        
        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        logger.info(f"  -> 已从Emby获取 {len(all_emby_items)} 个媒体项目，并创建了TMDB->Emby映射。")

        task_manager.update_status_from_thread(5, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby_handler.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}
        logger.info(f"  -> 已预加载 {len(prefetched_collection_map)} 个现有合集的信息。")

        cover_service = None
        cover_config = {}
        try:
            cover_config = db_handler.get_setting('cover_generator_config') or {}
            
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
                logger.info("  -> 封面生成器已启用，将在每个合集处理后尝试生成封面。")
        except Exception as e_cover_init:
            logger.error(f"初始化封面生成器时失败: {e_cover_init}", exc_info=True)


        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break

            collection_id = collection['id']
            collection_name = collection['name']
            collection_type = collection['type']
            definition = collection['definition_json']
            
            progress = 10 + int((i / total) * 90)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total}) 正在处理: {collection_name}")

            try:
                item_types_for_collection = definition.get('item_type', ['Movie'])
                tmdb_items = []
                source_type = 'filter' # 默认

                if collection_type == 'list':
                    url = definition.get('url', '')
                    if url.startswith('maoyan://'):
                        source_type = 'list_maoyan'
                        importer = ListImporter(processor.tmdb_api_key)
                        greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
                        tmdb_items = greenlet.get()
                    else:
                        importer = ListImporter(processor.tmdb_api_key)
                        tmdb_items, source_type = importer.process(definition)
                elif collection_type == 'filter':
                    engine = FilterEngine()
                    tmdb_items = engine.execute_filter(definition)
                
                # ... (后续代码直到封面生成部分) ...
                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，跳过。")
                    db_handler.update_custom_collection_after_sync(collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]", "generated_emby_ids_json": "[]"})
                    continue

                ordered_emby_ids_in_library = [
                    tmdb_to_emby_item_map[item['id']]['Id'] 
                    for item in tmdb_items if item['id'] in tmdb_to_emby_item_map
                ]

                emby_collection_id = None # 先初始化为 None
                if not ordered_emby_ids_in_library:
                    logger.warning(f"榜单 '{collection_name}' 解析成功，但在您的媒体库中未找到任何匹配项目。将只更新数据库，不创建Emby合集。")
                else:
                    emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                        collection_name=collection_name, 
                        emby_ids_in_library=ordered_emby_ids_in_library, 
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key, 
                        user_id=processor.emby_user_id,
                        prefetched_collection_map=prefetched_collection_map
                    )
                    if not emby_collection_id:
                        raise RuntimeError("在Emby中创建或更新合集失败，请检查Emby日志。")
                
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc)
                }

                if collection_type == 'list':
                    # ... (这部分健康度检查逻辑不变) ...
                    previous_media_map = {}
                    try:
                        previous_media_list = collection.get('generated_media_info_json') or []
                        previous_media_map = {str(m.get('tmdb_id')): m for m in previous_media_list}
                    except TypeError:
                        logger.warning(f"解析合集 {collection_name} 的旧媒体JSON失败...")
                    
                    image_tag = None
                    if emby_collection_id:
                        emby_collection_details = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                        image_tag = emby_collection_details.get("ImageTags", {}).get("Primary")
                    
                    all_media_details_unordered = []
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        future_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details_tmdb, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                        for future in as_completed(future_to_item):
                            try:
                                detail = future.result()
                                if detail: all_media_details_unordered.append(detail)
                            except Exception as exc:
                                logger.error(f"获取TMDb详情时线程内出错: {exc}")
                    
                    details_map = {str(d.get("id")): d for d in all_media_details_unordered}
                    all_media_details_ordered = [details_map[item['id']] for item in tmdb_items if item['id'] in details_map]

                    tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
                    all_media_with_status, has_missing, missing_count = [], False, 0
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    
                    for media in all_media_details_ordered:
                        media_tmdb_id = str(media.get("id"))
                        emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                        
                        release_date = media.get("release_date") or media.get("first_air_date", '')
                        media_status = "unknown"
                        if emby_item: media_status = "in_library"
                        elif previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed': media_status = "subscribed"
                        elif release_date and release_date > today_str: media_status = "unreleased"
                        else: media_status, has_missing, missing_count = "missing", True, missing_count + 1
                        
                        final_media_item = {
                            "tmdb_id": media_tmdb_id,
                            "emby_id": emby_item.get('Id') if emby_item else None,
                            "title": media.get("title") or media.get("name"),
                            "release_date": release_date,
                            "poster_path": media.get("poster_path"),
                            "status": media_status
                        }

                        season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                        if season_number is not None:
                            final_media_item['season'] = season_number
                            final_media_item['title'] = f"{final_media_item['title']} 第 {season_number} 季"
                        
                        all_media_with_status.append(final_media_item)

                    update_data.update({
                        "health_status": "has_missing" if has_missing else "ok",
                        "in_library_count": len(ordered_emby_ids_in_library),
                        "missing_count": missing_count,
                        "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False),
                        "poster_path": f"/Items/{emby_collection_id}/Images/Primary?tag={image_tag}" if image_tag and emby_collection_id else None
                    })
                else: 
                    all_media_with_status = [
                        {
                            'tmdb_id': item['id'],
                            'emby_id': tmdb_to_emby_item_map.get(item['id'], {}).get('Id')
                        }
                        for item in tmdb_items
                    ]
                    update_data.update({
                        "health_status": "ok", 
                        "in_library_count": len(ordered_emby_ids_in_library),
                        "missing_count": 0, 
                        "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False), 
                        "poster_path": None
                    })
                
                db_handler.update_custom_collection_after_sync(collection_id, update_data)
                logger.info(f"  -> ✅ 合集 '{collection_name}' 处理完成，并已更新数据库状态。")

                if cover_service and emby_collection_id:
                    logger.info(f"  -> 正在为合集 '{collection_name}' 生成封面...")
                    # 1. 获取最新的 Emby 合集详情
                    library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                    if library_info:
                        # 2. 准备封面生成器需要的其他参数
                        item_types_for_collection = definition.get('item_type', ['Movie'])
                        
                        # 3. 将数据库中最新的合集信息（包含in_library_count）传递给辅助函数
                        #    我们使用 db_handler 重新获取一次，确保拿到的是刚刚更新过的最新数据
                        latest_collection_info = db_handler.get_custom_collection_by_id(collection_id)
                        item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                        
                        # 4. 调用封面生成服务
                        cover_service.generate_for_library(
                            emby_server_id='main_emby',
                            library=library_info,
                            item_count=item_count_to_pass, # <-- 使用计算好的角标参数
                            content_types=item_types_for_collection
                        )
            except Exception as e_coll:
                logger.error(f"处理合集 '{collection_name}' (ID: {collection_id}) 时发生错误: {e_coll}", exc_info=True)
                continue
        
        final_message = "所有启用的自定义合集均已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")


# --- 处理单个自定义合集的核心任务 ---
def task_process_custom_collection(processor: MediaProcessor, custom_collection_id: int):
    """
    【V12 - 榜单类型识别 & 精确封面参数】
    """
    task_name = f"处理自定义合集 (ID: {custom_collection_id})"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ... (前面的代码都不变，直到 tmdb_items = [] ) ...
        task_manager.update_status_from_thread(0, "正在读取合集定义...")
        collection = db_handler.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        
        collection_name = collection['name']
        collection_type = collection['type']
        definition = collection['definition_json']
        
        item_types_for_collection = definition.get('item_type', ['Movie'])
        
        tmdb_items = []
        source_type = 'filter' # 默认

        if collection_type == 'list':
            url = definition.get('url', '')
            if url.startswith('maoyan://'):
                source_type = 'list_maoyan'
                logger.info(f"检测到猫眼榜单 '{collection_name}'，将启动异步后台任务...")
                task_manager.update_status_from_thread(10, f"正在后台获取猫眼榜单: {collection_name}...")
                importer = ListImporter(processor.tmdb_api_key)
                greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
                tmdb_items = greenlet.get()
            else:
                importer = ListImporter(processor.tmdb_api_key)
                tmdb_items, source_type = importer.process(definition)
        elif collection_type == 'filter':
            engine = FilterEngine()
            tmdb_items = engine.execute_filter(definition)
        
        # ... (后续代码直到封面生成部分) ...
        if not tmdb_items:
            logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，任务结束。")
            db_handler.update_custom_collection_after_sync(custom_collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]"})
            return

        task_manager.update_status_from_thread(70, f"已生成 {len(tmdb_items)} 个ID，正在Emby中创建/更新合集...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])

        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter=",".join(item_types_for_collection), library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        ordered_emby_ids_in_library = [tmdb_to_emby_item_map[item['id']]['Id'] for item in tmdb_items if item['id'] in tmdb_to_emby_item_map]

        if not ordered_emby_ids_in_library:
            logger.warning(f"榜单 '{collection_name}' 解析成功，但在您的媒体库中未找到任何匹配项目。将只更新数据库，不创建Emby合集。")
            emby_collection_id = None 
        else:
            emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                collection_name=collection_name, 
                emby_ids_in_library=ordered_emby_ids_in_library, 
                base_url=processor.emby_url,
                api_key=processor.emby_api_key, 
                user_id=processor.emby_user_id
            )
            if not emby_collection_id:
                raise RuntimeError("在Emby中创建或更新合集失败。")
        
        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(item_types_for_collection),
            "last_synced_at": datetime.now(pytz.utc)
        }

        if collection_type == 'list':
            # ... (这部分健康度检查逻辑不变) ...
            task_manager.update_status_from_thread(90, "榜单合集已同步，正在并行获取详情...")
            
            previous_media_map = {}
            try:
                previous_media_list = collection.get('generated_media_info_json') or []
                previous_media_map = {str(m.get('tmdb_id')): m for m in previous_media_list}
            except TypeError:
                logger.warning(f"解析合集 {collection_name} 的旧媒体JSON失败...")

            image_tag = None
            if emby_collection_id:
                emby_collection_details = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                image_tag = emby_collection_details.get("ImageTags", {}).get("Primary")
            
            all_media_details_unordered = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_item = {executor.submit(tmdb_handler.get_movie_details if item['type'] != 'Series' else tmdb_handler.get_tv_details_tmdb, item['id'], processor.tmdb_api_key): item for item in tmdb_items}
                for future in as_completed(future_to_item):
                    try:
                        detail = future.result()
                        if detail: all_media_details_unordered.append(detail)
                    except Exception as exc:
                        logger.error(f"获取TMDb详情时线程内出错: {exc}")

            details_map = {str(d.get("id")): d for d in all_media_details_unordered}
            all_media_details_ordered = [details_map[item['id']] for item in tmdb_items if item['id'] in details_map]
            
            tmdb_id_to_season_map = {str(item['id']): item.get('season') for item in tmdb_items if item.get('type') == 'Series' and item.get('season') is not None}
            all_media_with_status, has_missing, missing_count = [], False, 0
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            for media in all_media_details_ordered:
                media_tmdb_id = str(media.get("id"))
                emby_item = tmdb_to_emby_item_map.get(media_tmdb_id)
                
                release_date = media.get("release_date") or media.get("first_air_date", '')
                media_status = "unknown"
                if emby_item: media_status = "in_library"
                elif previous_media_map.get(media_tmdb_id, {}).get('status') == 'subscribed': media_status = "subscribed"
                elif release_date and release_date > today_str: media_status = "unreleased"
                else: media_status, has_missing, missing_count = "missing", True, missing_count + 1
                
                final_media_item = {
                    "tmdb_id": media_tmdb_id,
                    "emby_id": emby_item.get('Id') if emby_item else None,
                    "title": media.get("title") or media.get("name"),
                    "release_date": release_date,
                    "poster_path": media.get("poster_path"),
                    "status": media_status
                }

                season_number = tmdb_id_to_season_map.get(media_tmdb_id)
                if season_number is not None:
                    final_media_item['season'] = season_number
                    final_media_item['title'] = f"{final_media_item['title']} 第 {season_number} 季"
                
                all_media_with_status.append(final_media_item)

            update_data.update({
                "health_status": "has_missing" if has_missing else "ok",
                "in_library_count": len(ordered_emby_ids_in_library),
                "missing_count": missing_count,
                "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False),
                "poster_path": f"/Items/{emby_collection_id}/Images/Primary?tag={image_tag}" if image_tag and emby_collection_id else None
            })
            logger.info(f"  -> 已为RSS合集 '{collection_name}' 分析健康状态。")
        else: 
            task_manager.update_status_from_thread(95, "筛选合集已生成，跳过缺失分析。")
            all_media_with_status = [{'tmdb_id': item['id'], 'emby_id': tmdb_to_emby_item_map.get(item['id'], {}).get('Id')} for item in tmdb_items]
            update_data.update({
                "health_status": "ok", "in_library_count": len(ordered_emby_ids_in_library),
                "missing_count": 0, 
                "generated_media_info_json": json.dumps(all_media_with_status, ensure_ascii=False), 
                "poster_path": None
            })

        db_handler.update_custom_collection_after_sync(custom_collection_id, update_data)
        logger.info(f"  -> 已更新自定义合集 '{collection_name}' (ID: {custom_collection_id}) 的同步状态和健康信息。")

        try:
            cover_config = db_handler.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and emby_collection_id:
                logger.info(f"  -> 检测到封面生成器已启用，将为合集 '{collection_name}' 生成封面...")
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    # ▼▼▼ 核心修改点 ▼▼▼
                    # 1. 获取最新的合集信息
                    latest_collection_info = db_handler.get_custom_collection_by_id(custom_collection_id)
                    
                    # 2. 调用辅助函数获取正确的角标参数
                    item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                        
                    # 3. 调用封面生成服务
                    cover_service.generate_for_library(
                        emby_server_id='main_emby',
                        library=library_info,
                        item_count=item_count_to_pass, # <-- 使用计算好的角标参数
                        content_types=item_types_for_collection
                    )
                else:
                    logger.warning(f"无法获取 Emby 合集 {emby_collection_id} 的详情，跳过封面生成。")
        except Exception as e:
            logger.error(f"为合集 '{collection_name}' 生成封面时发生错误: {e}", exc_info=True)

        task_manager.update_status_from_thread(100, "自定义合集同步并分析完成！")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 轻量级的元数据缓存填充任务 ★★★
def task_populate_metadata_cache(processor: 'MediaProcessor', batch_size: int = 50, force_full_update: bool = False):
    """
    【V4 - 增量与全量同步版】
    - 移除了基于时间戳的差异对比逻辑。
    - 快速模式: 只同步 Emby 中新增的、本地数据库不存在的媒体项。
    - 深度模式: 强制同步 Emby 媒体库中的所有媒体项，覆盖本地数据。
    - 保留了高效的分批处理、并发获取和带 SAVEPOINT 的健壮数据库写入机制。
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    try:
        # ======================================================================
        # 步骤 1: 计算差异 (逻辑已简化)
        # ======================================================================
        task_manager.update_status_from_thread(0, f"阶段1/2: 计算媒体库差异 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        emby_items_index = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            fields="ProviderIds,Type,DateCreated,Name,ProductionYear,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,ProductionLocations,People,Tags,DateModified,OfficialRating"
        ) or []
        
        emby_items_map = {
            item.get("ProviderIds", {}).get("Tmdb"): item 
            for item in emby_items_index if item.get("ProviderIds", {}).get("Tmdb")
        }
        emby_tmdb_ids = set(emby_items_map.keys())
        logger.info(f"  -> 从 Emby 获取到 {len(emby_tmdb_ids)} 个有效的媒体项。")

        if processor.is_stop_requested():
            logger.info("任务在获取 Emby 媒体项后被中止。")
            return

        db_tmdb_ids = set()
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata")
            db_tmdb_ids = {row["tmdb_id"] for row in cursor.fetchall()}
        logger.info(f"  -> 从本地数据库 media_metadata 表中获取到 {len(db_tmdb_ids)} 个媒体项。")

        if processor.is_stop_requested():
            logger.info("任务在获取本地数据库媒体项后被中止。")
            return

        # --- 核心逻辑修改 ---
        ids_to_process: set
        items_to_delete_tmdb_ids = db_tmdb_ids - emby_tmdb_ids
        
        if force_full_update:
            logger.info("  -> 深度同步模式：将处理 Emby 中的所有项目。")
            ids_to_process = emby_tmdb_ids
            logger.info(f"  -> 计算差异完成：处理 {len(ids_to_process)} 项, 删除 {len(items_to_delete_tmdb_ids)} 项。")
        else:
            logger.info("  -> 快速同步模式：仅处理 Emby 中新增的项目。")
            ids_to_process = emby_tmdb_ids - db_tmdb_ids
            logger.info(f"  -> 计算差异完成：新增 {len(ids_to_process)} 项, 删除 {len(items_to_delete_tmdb_ids)} 项。")

        if items_to_delete_tmdb_ids:
            logger.info(f"  -> 正在从数据库中删除 {len(items_to_delete_tmdb_ids)} 个已不存在的媒体项...")
            with db_handler.get_db_connection() as conn:
                cursor = conn.cursor()
                ids_to_delete_list = list(items_to_delete_tmdb_ids)
                for i in range(0, len(ids_to_delete_list), 500):
                    if processor.is_stop_requested():
                        logger.info("任务在删除冗余数据时被中止。")
                        break
                    batch_ids = ids_to_delete_list[i:i+500]
                    sql = "DELETE FROM media_metadata WHERE tmdb_id = ANY(%s)"
                    cursor.execute(sql, (batch_ids,))
                conn.commit()
            logger.info("  -> 冗余数据清理完成。")

        if processor.is_stop_requested():
            logger.info("任务在冗余数据清理后被中止。")
            return

        items_to_process = [emby_items_map[tmdb_id] for tmdb_id in ids_to_process]
        
        total_to_process = len(items_to_process)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "数据库已是最新，无需同步。")
            return

        logger.info(f"  -> 总共需要处理 {total_to_process} 项，将分 { (total_to_process + batch_size - 1) // batch_size } 个批次。")

        # ======================================================================
        # 步骤 2: 分批循环处理需要新增/更新的媒体项
        # ======================================================================
        
        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested():
                logger.info("任务在批次处理前被中止。")
                break

            batch_items = items_to_process[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (total_to_process + batch_size - 1) // batch_size
            
            logger.info(f"--- 开始处理批次 {batch_number}/{total_batches} (包含 {len(batch_items)} 个项目) ---")
            task_manager.update_status_from_thread(
                10 + int((processed_count / total_to_process) * 90), 
                f"处理批次 {batch_number}/{total_batches}..."
            )

            batch_people_to_enrich = [p for item in batch_items for p in item.get("People", [])]
            enriched_people_list = processor._enrich_cast_from_db_and_api(batch_people_to_enrich)
            enriched_people_map = {str(p.get("Id")): p for p in enriched_people_list}

            if processor.is_stop_requested():
                logger.info("任务在演员数据补充后被中止。")
                break

            logger.info(f"  -> 开始从Tmdb补充导演/国家数据...")
            tmdb_details_map = {}
            def fetch_tmdb_details(item):
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                item_type = item.get("Type")
                if not tmdb_id: return None, None
                details = None
                if item_type == 'Movie':
                    details = tmdb_handler.get_movie_details(tmdb_id, processor.tmdb_api_key)
                elif item_type == 'Series':
                    details = tmdb_handler.get_tv_details_tmdb(tmdb_id, processor.tmdb_api_key)
                return tmdb_id, details

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_tmdb_id = {executor.submit(fetch_tmdb_details, item): item.get("ProviderIds", {}).get("Tmdb") for item in batch_items}
                for future in concurrent.futures.as_completed(future_to_tmdb_id):
                    if processor.is_stop_requested():
                        logger.info("任务在并发获取 TMDb 详情时被中止。")
                        break
                    tmdb_id, details = future.result()
                    if tmdb_id and details:
                        tmdb_details_map[tmdb_id] = details
            
            if processor.is_stop_requested():
                logger.info("任务在 TMDb 详情获取后被中止。")
                break

            metadata_batch = []
            for item in batch_items:
                if processor.is_stop_requested():
                    logger.info("任务在处理单个媒体项时被中止。")
                    break
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not tmdb_id: continue

                full_details_emby = item
                tmdb_details = tmdb_details_map.get(tmdb_id)

                actors = []
                for person in full_details_emby.get("People", []):
                    person_id = str(person.get("Id"))
                    enriched_person = enriched_people_map.get(person_id)
                    if enriched_person and enriched_person.get("ProviderIds", {}).get("Tmdb"):
                        actors.append({'id': enriched_person["ProviderIds"]["Tmdb"], 'name': enriched_person.get('Name')})
                
                directors, countries = [], []
                if tmdb_details:
                    item_type = full_details_emby.get("Type")
                    if item_type == 'Movie':
                        credits_data = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                        if credits_data:
                            directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        countries = translate_country_list([c['name'] for c in tmdb_details.get('production_countries', [])])
                    elif item_type == 'Series':
                        credits_data = tmdb_details.get("credits", {})
                        if credits_data:
                            directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        if not directors: directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                        countries = translate_country_list(tmdb_details.get('origin_country', []))

                studios = [s['Name'] for s in full_details_emby.get('Studios', []) if s.get('Name')]
                tags = [tag['Name'] for tag in full_details_emby.get('TagItems', []) if tag.get('Name')]
                
                # ★★★ 修复 1/2: 修正日期处理逻辑 ★★★
                # 如果日期字符串存在，则取 'T' 之前的部分；如果不存在，则直接为 None
                premiere_date_str = full_details_emby.get('PremiereDate')
                release_date = premiere_date_str.split('T')[0] if premiere_date_str else None
                
                date_created_str = full_details_emby.get('DateCreated')
                date_added = date_created_str.split('T')[0] if date_created_str else None

                official_rating = full_details_emby.get('OfficialRating') # 获取原始分级，可能为 None
                unified_rating = get_unified_rating(official_rating)    # 即使 official_rating 是 None，函数也能处理

                metadata_to_save = {
                    "tmdb_id": tmdb_id, "item_type": full_details_emby.get("Type"),
                    "title": full_details_emby.get('Name'), "original_title": full_details_emby.get('OriginalTitle'),
                    "release_year": full_details_emby.get('ProductionYear'), "rating": full_details_emby.get('CommunityRating'),
                    "official_rating": official_rating, # 保留原始值用于调试
                    "unified_rating": unified_rating,   # 存入计算后的统一分级
                    "release_date": release_date,
                    "date_added": date_added,
                    "genres_json": json.dumps(full_details_emby.get('Genres', []), ensure_ascii=False),
                    "actors_json": json.dumps(actors, ensure_ascii=False),
                    "directors_json": json.dumps(directors, ensure_ascii=False),
                    "studios_json": json.dumps(studios, ensure_ascii=False),
                    "countries_json": json.dumps(countries, ensure_ascii=False),
                    "tags_json": json.dumps(tags, ensure_ascii=False),
                }
                metadata_batch.append(metadata_to_save)

            if processor.is_stop_requested():
                logger.info("任务在构建元数据批次后被中止。")
                break

            if metadata_batch:
                with db_handler.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # ★★★ 修复 2/2: 改进事务处理逻辑 ★★★
                    # 开启一个总事务
                    cursor.execute("BEGIN;")
                    for idx, metadata in enumerate(metadata_batch):
                        if processor.is_stop_requested():
                            logger.info("任务在数据库写入循环中被中止。")
                            break
                        savepoint_name = f"sp_{idx}"
                        try:
                            # 为每个条目创建一个保存点
                            cursor.execute(f"SAVEPOINT {savepoint_name};")
                            
                            columns = list(metadata.keys())
                            columns_str = ', '.join(columns)
                            placeholders_str = ', '.join(['%s'] * len(columns))
                            
                            update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns]
                            update_clauses.append("last_synced_at = EXCLUDED.last_synced_at")
                            update_str = ', '.join(update_clauses)

                            sql = f"""
                                INSERT INTO media_metadata ({columns_str}, last_synced_at)
                                VALUES ({placeholders_str}, %s)
                                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update_str}
                            """
                            sync_time = datetime.now(timezone.utc).isoformat()
                            cursor.execute(sql, tuple(metadata.values()) + (sync_time,))
                        except psycopg2.Error as e:
                            # 如果发生错误，记录它，并回滚到这个条目之前的状态
                            logger.error(f"写入 TMDB ID {metadata.get('tmdb_id')} 的元数据时发生数据库错误: {e}")
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                    
                    # 提交整个事务（所有成功的条目）
                    conn.commit()
                logger.info(f"--- 批次 {batch_number}/{total_batches} 已成功写入数据库。---")
            
            processed_count += len(batch_items)

        final_message = f"同步完成！本次处理 {processed_count}/{total_to_process} 项, 删除 {len(items_to_delete_tmdb_ids)} 项。"
        if processor.is_stop_requested():
            final_message = "任务已中止，部分数据可能未处理。"
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 立即生成所有媒体库封面的后台任务 ★★★
def task_generate_all_covers(processor: MediaProcessor):
    """
    后台任务：为所有（未被忽略的）媒体库生成封面。
    """
    task_name = "一键生成所有媒体库封面"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # 1. 读取配置
        cover_config = db_handler.get_setting('cover_generator_config') or {}

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
def task_generate_all_custom_collection_covers(processor: MediaProcessor):
    """
    后台任务：为所有已启用、且已在Emby中创建的自定义合集生成封面。
    """
    task_name = "一键生成所有自建合集封面"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # 1. 读取封面生成器的配置
        cover_config = db_handler.get_setting('cover_generator_config') or {}
        if not cover_config.get("enabled"):
            task_manager.update_status_from_thread(100, "任务跳过：封面生成器未启用。")
            return

        # 2. 从数据库获取所有已启用的自定义合集
        task_manager.update_status_from_thread(5, "正在获取所有已启用的自建合集...")
        all_active_collections = db_handler.get_all_active_custom_collections()
        
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

# ★★★ 媒体洗版任务 (基于精确API模型重构) ★★★
def _build_resubscribe_payload(item_details: dict, rule: Optional[dict]) -> Optional[dict]:
    """
    【V4 - 实战命名·最终版】
    - 根据PT站点的实际命名约定，优化了杜比视界Profile 8的正则表达式。
    - 现在，当订阅 Profile 8 时，会生成一个匹配 "dovi" 和 "hdr" 两个关键词同时存在的正则，
      这完美符合了现实世界中的文件命名习惯。
    """
    item_name = item_details.get('Name') or item_details.get('item_name')
    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb") or item_details.get('tmdb_id')
    item_type = item_details.get("Type") or item_details.get('item_type')

    if not all([item_name, tmdb_id, item_type]):
        logger.error(f"构建Payload失败：缺少核心媒体信息 {item_details}")
        return None

    payload = {
        "name": item_name, "tmdbid": int(tmdb_id),
        "type": "电影" if item_type == "Movie" else "电视剧",
        "best_version": 1
    }

    use_custom_subscribe = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_USE_CUSTOM_RESUBSCRIBE, False)
    if not use_custom_subscribe or not rule:
        log_reason = "自定义洗版未开启" if not use_custom_subscribe else "未匹配到规则"
        logger.info(f"  -> 《{item_name}》将使用全局洗版 ({log_reason})。")
        return payload

    rule_name = rule.get('name', '未知规则')
    final_include_lookaheads = []

    # --- 分辨率、质量 (逻辑不变) ---
    if rule.get("resubscribe_resolution_enabled"):
        threshold = rule.get("resubscribe_resolution_threshold")
        target_resolution = None
        if threshold == 3840: target_resolution = "4k"
        elif threshold == 1920: target_resolution = "1080p"
        if target_resolution:
            payload['resolution'] = target_resolution
            logger.info(f"  -> 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 分辨率: {target_resolution}")
    if rule.get("resubscribe_quality_enabled"):
        quality_list = rule.get("resubscribe_quality_include")
        if isinstance(quality_list, list) and quality_list:
            payload['quality'] = ",".join(quality_list)
            logger.info(f"  -> 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 质量: {payload['quality']}")
    
    # --- 特效订阅逻辑 (实战优化) ---
    if rule.get("resubscribe_effect_enabled"):
        effect_list = rule.get("resubscribe_effect_include", [])
        if isinstance(effect_list, list) and effect_list:
            simple_effects_for_payload = set()
            
            EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
            # ★★★ 核心修改：将 "dv" 加入正则 ★★★
            EFFECT_PARAM_MAP = {
                "dovi_p8": ("(?=.*(dovi|dolby|dv))(?=.*hdr)", "dovi"),
                "dovi_p7": ("(?=.*(dovi|dolby|dv))(?=.*(p7|profile.?7))", "dovi"),
                "dovi_p5": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "dovi_other": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "hdr10+": ("(?=.*(hdr10\+|hdr10plus))", "hdr10+"),
                "hdr": ("(?=.*hdr)", "hdr")
            }
            OLD_EFFECT_MAP = {"杜比视界": "dovi_other", "HDR": "hdr"}

            highest_req_priority = 999
            best_effect_choice = None
            for choice in effect_list:
                normalized_choice = OLD_EFFECT_MAP.get(choice, choice)
                try:
                    priority = EFFECT_HIERARCHY.index(normalized_choice)
                    if priority < highest_req_priority:
                        highest_req_priority = priority
                        best_effect_choice = normalized_choice
                except ValueError: continue
            
            if best_effect_choice:
                regex_pattern, simple_effect = EFFECT_PARAM_MAP.get(best_effect_choice, (None, None))
                if regex_pattern:
                    final_include_lookaheads.append(regex_pattern)
                if simple_effect:
                    simple_effects_for_payload.add(simple_effect)

            if simple_effects_for_payload:
                 payload['effect'] = ",".join(simple_effects_for_payload)

    # --- 音轨、字幕处理 (逻辑不变) ---
    if rule.get("resubscribe_audio_enabled"):
        audio_langs = rule.get("resubscribe_audio_missing_languages", [])
        if isinstance(audio_langs, list) and audio_langs:
            audio_keywords = [k for lang in audio_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(lang, [])]
            if audio_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(audio_keywords)), key=len, reverse=True))}))")

    if rule.get("resubscribe_subtitle_effect_only"):
        final_include_lookaheads.append("(?=.*特效)")
    elif rule.get("resubscribe_subtitle_enabled"):
        subtitle_langs = rule.get("resubscribe_subtitle_missing_languages", [])
        if isinstance(subtitle_langs, list) and subtitle_langs:
            subtitle_keywords = [k for lang in subtitle_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(f"sub_{lang}", [])]
            if subtitle_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(subtitle_keywords)), key=len, reverse=True))}))")

    if final_include_lookaheads:
        payload['include'] = "".join(final_include_lookaheads)
        logger.info(f"  -> 《{item_name}》按规则 '{rule_name}' 生成的 AND 正则过滤器(精筛): {payload['include']}")

    return payload

def _item_needs_resubscribe(item_details: dict, config: dict, media_metadata: Optional[dict] = None) -> tuple[bool, str]:
    """
    【V12 - 功能完整·最终版】
    - 恢复了所有检查逻辑，包括：分辨率、质量、特效、音轨和字幕。
    - 此版本调用全局的、最新的 _get_standardized_effect 函数来做决策。
    """
    item_name = item_details.get('Name', '未知项目')
    logger.trace(f"  -> 开始为《{item_name}》检查洗版需求 ---")
    
    media_streams = item_details.get('MediaStreams', [])
    file_path = item_details.get('Path', '')
    file_name_lower = os.path.basename(file_path).lower() if file_path else ""

    reasons = []
    video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)

    CHINESE_LANG_CODES = {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn', 'yue'}
    CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}

    # 1. 分辨率检查
    try:
        if config.get("resubscribe_resolution_enabled"):
            if not video_stream:
                reasons.append("无视频流信息")
            else:
                threshold = int(config.get("resubscribe_resolution_threshold") or 1920)
                current_width = int(video_stream.get('Width') or 0)
                if 0 < current_width < threshold:
                    threshold_name = {3840: "4K", 1920: "1080p", 1280: "720p"}.get(threshold, "未知")
                    reasons.append(f"分辨率低于{threshold_name}")
    except (ValueError, TypeError) as e:
        logger.warning(f"  -> [分辨率检查] 处理时发生类型错误: {e}")

    # 2. 质量检查
    try:
        if config.get("resubscribe_quality_enabled"):
            required_list = config.get("resubscribe_quality_include", [])
            if isinstance(required_list, list) and required_list:
                required_list_lower = [str(q).lower() for q in required_list]
                if not any(term in file_name_lower for term in required_list_lower):
                    reasons.append("质量不达标")
    except Exception as e:
        logger.warning(f"  -> [质量检查] 处理时发生未知错误: {e}")

    # 3. 特效检查 (调用最新的全局函数)
    try:
        if config.get("resubscribe_effect_enabled"):
            user_choices = config.get("resubscribe_effect_include", [])
            if isinstance(user_choices, list) and user_choices:
                EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
                OLD_EFFECT_MAP = {"杜比视界": "dovi_other", "HDR": "hdr"}
                highest_req_priority = 999
                for choice in user_choices:
                    normalized_choice = OLD_EFFECT_MAP.get(choice, choice)
                    try:
                        priority = EFFECT_HIERARCHY.index(normalized_choice)
                        if priority < highest_req_priority:
                            highest_req_priority = priority
                    except ValueError:
                        continue
                
                if highest_req_priority < 999:
                    current_effect = _get_standardized_effect(file_name_lower, video_stream)
                    current_priority = EFFECT_HIERARCHY.index(current_effect)
                    if current_priority > highest_req_priority:
                        reasons.append("特效不达标")
    except Exception as e:
        logger.warning(f"  -> [特效检查] 处理时发生未知错误: {e}")

    # 4. & 5. 音轨和字幕检查
    def _is_exempted_from_chinese_check() -> bool:
        present_audio_langs = {str(s.get('Language', '')).lower() for s in media_streams if s.get('Type') == 'Audio' and s.get('Language')}
        if not present_audio_langs.isdisjoint(CHINESE_LANG_CODES):
            return True
        if 'und' in present_audio_langs or not present_audio_langs:
            if media_metadata and media_metadata.get('countries_json'):
                if not set(media_metadata['countries_json']).isdisjoint(CHINESE_SPEAKING_REGIONS):
                    return True
        return False

    is_exempted = _is_exempted_from_chinese_check()
    
    try:
        if config.get("resubscribe_audio_enabled") and not is_exempted:
            required_langs = set(config.get("resubscribe_audio_missing_languages", []))
            if 'chi' in required_langs or 'yue' in required_langs:
                present_langs = {str(s.get('Language', '')).lower() for s in media_streams if s.get('Type') == 'Audio' and s.get('Language')}
                if present_langs.isdisjoint(CHINESE_LANG_CODES):
                    reasons.append("缺中文音轨")
    except Exception as e:
        logger.warning(f"  -> [音轨检查] 处理时发生未知错误: {e}")

    try:
        if config.get("resubscribe_subtitle_enabled") and not is_exempted:
            required_langs = set(config.get("resubscribe_subtitle_missing_languages", []))
            if 'chi' in required_langs:
                present_sub_langs = {str(s.get('Language', '')).lower() for s in media_streams if s.get('Type') == 'Subtitle' and s.get('Language')}
                if present_sub_langs.isdisjoint(CHINESE_LANG_CODES):
                    reasons.append("缺中文字幕")
    except Exception as e:
        logger.warning(f"  -> [字幕检查] 处理时发生未知错误: {e}")
                 
    if reasons:
        final_reason = "; ".join(sorted(list(set(reasons))))
        logger.info(f"  -> 《{item_name}》需要洗版。原因: {final_reason}")
        return True, final_reason
    else:
        logger.debug(f"  -> 《{item_name}》质量达标。")
        return False, ""

# ★★★ 精准批量订阅的后台任务 ★★★
def task_resubscribe_batch(processor: MediaProcessor, item_ids: List[str]):
    """【精准批量版】后台任务：只订阅列表中指定的一批媒体项。"""
    task_name = "批量媒体洗版"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_subscribe = []
    
    try:
        # 1. 从数据库中精确获取需要处理的项目
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)"
            cursor.execute(sql, (item_ids,))
            items_to_subscribe = cursor.fetchall()

        total_to_process = len(items_to_subscribe)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有需要订阅的项。")
            return

        logger.info(f"  -> 精准任务：共找到 {total_to_process} 个项目待处理，将开始订阅...")
        
        # 2. 后续的订阅、删除、配额检查逻辑和“一键洗版”完全一致
        all_rules = db_handler.get_all_resubscribe_rules()
        config = processor.config
        delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))
        resubscribed_count = 0
        deleted_count = 0

        for i, item in enumerate(items_to_subscribe):
            if processor.is_stop_requested():
                logger.info("  -> 任务被用户中止。")
                break
            
            current_quota = db_handler.get_subscription_quota()
            if current_quota <= 0:
                logger.warning("  -> 每日订阅配额已用尽，任务提前结束。")
                break

            item_id = item.get('item_id')
            item_name = item.get('item_name')
            task_manager.update_status_from_thread(
                int((i / total_to_process) * 100), 
                f"({i+1}/{total_to_process}) [配额:{current_quota}] 正在订阅: {item_name}"
            )

            # 1. 获取当前项目匹配的规则
            matched_rule_id = item.get('matched_rule_id')
            rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

            # 2. 让“智能荷官”配牌 (item 字典本身就包含了需要的信息)
            payload = _build_resubscribe_payload(item, rule)

            if not payload:
                logger.warning(f"为《{item.get('item_name')}》构建订阅Payload失败，已跳过。")
                continue # 跳过这个项目，继续下一个

            # 3. 发送订阅
            success = moviepilot_handler.subscribe_with_custom_payload(payload, config)
            
            if success:
                db_handler.decrement_subscription_quota()
                resubscribed_count += 1
                
                matched_rule_id = item.get('matched_rule_id')
                rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

                if rule and rule.get('delete_after_resubscribe'):
                    delete_success = emby_handler.delete_item(
                        item_id=item_id, emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                    )
                    if delete_success:
                        db_handler.delete_resubscribe_cache_item(item_id)
                        deleted_count += 1
                    else:
                        db_handler.update_resubscribe_item_status(item_id, 'subscribed')
                else:
                    db_handler.update_resubscribe_item_status(item_id, 'subscribed')
                
                if i < total_to_process - 1: time.sleep(delay)

        final_message = f"批量任务完成！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_resubscribe_library(processor: MediaProcessor):
    """ 后台任务：订阅成功后，根据规则删除或更新缓存。"""
    task_name = "媒体洗版"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    config = processor.config
    
    try:
        all_rules = db_handler.get_all_resubscribe_rules()
        delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))

        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache WHERE status = 'needed'")
            items_to_resubscribe = cursor.fetchall()

        total_needed = len(items_to_resubscribe)
        if total_needed == 0:
            task_manager.update_status_from_thread(100, "任务完成：没有发现需要洗版的项目。")
            return

        logger.info(f"  -> 共找到 {total_needed} 个项目待处理，将开始订阅...")
        resubscribed_count = 0
        deleted_count = 0

        for i, item in enumerate(items_to_resubscribe):
            if processor.is_stop_requested(): break
            
            current_quota = db_handler.get_subscription_quota()
            if current_quota <= 0:
                logger.warning("  -> 每日订阅配额已用尽，任务提前结束。")
                break

            item_name = item.get('item_name')
            item_id = item.get('item_id')
            task_manager.update_status_from_thread(
                int((i / total_needed) * 100), 
                f"({i+1}/{total_needed}) [配额:{current_quota}] 正在订阅: {item_name}"
            )

            # 1. 获取当前项目匹配的规则
            matched_rule_id = item.get('matched_rule_id')
            rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

            # 2. 让“智能荷官”配牌 (item 字典本身就包含了需要的信息)
            payload = _build_resubscribe_payload(item, rule)

            if not payload:
                logger.warning(f"为《{item.get('item_name')}》构建订阅Payload失败，已跳过。")
                continue # 跳过这个项目，继续下一个

            # 3. 发送订阅
            success = moviepilot_handler.subscribe_with_custom_payload(payload, config)
            
            if success:
                db_handler.decrement_subscription_quota()
                resubscribed_count += 1
                
                matched_rule_id = item.get('matched_rule_id')
                rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

                # --- ★★★ 核心逻辑改造：根据规则决定是“删除”还是“更新” ★★★ ---
                if rule and rule.get('delete_after_resubscribe'):
                    logger.warning(f"规则 '{rule['name']}' 要求删除源文件，正在删除 Emby 项目: {item_name} (ID: {item_id})")
                    delete_success = emby_handler.delete_item(
                        item_id=item_id, emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                    )
                    if delete_success:
                        # 如果 Emby 项删除成功，就从我们的缓存里也删除
                        db_handler.delete_resubscribe_cache_item(item_id)
                        deleted_count += 1
                    else:
                        # 如果 Emby 项删除失败，那我们只更新状态，让用户知道订阅成功了但删除失败
                        db_handler.update_resubscribe_item_status(item_id, 'subscribed')
                else:
                    # 如果没有删除规则，就正常更新状态
                    db_handler.update_resubscribe_item_status(item_id, 'subscribed')
                
                if i < total_needed - 1: time.sleep(delay)

        final_message = f"任务完成！成功提交 {resubscribed_count} 个订阅，并根据规则删除了 {deleted_count} 个媒体项。"
        if not processor.is_stop_requested() and current_quota <= 0:
             final_message = f"配额用尽！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 精准批量删除的后台任务 ★★★
def task_delete_batch(processor: MediaProcessor, item_ids: List[str]):
    """【精准批量版】后台任务：只删除列表中指定的一批媒体项。"""
    task_name = "批量删除媒体"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_delete = []
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)"
            cursor.execute(sql, (item_ids,))
            items_to_delete = cursor.fetchall()

        total_to_process = len(items_to_delete)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有可删除的项。")
            return

        logger.info(f"  -> 精准删除：共找到 {total_to_process} 个项目待处理...")
        deleted_count = 0

        for i, item in enumerate(items_to_delete):
            if processor.is_stop_requested(): break
            
            item_id = item.get('item_id')
            item_name = item.get('item_name')
            task_manager.update_status_from_thread(
                int((i / total_to_process) * 100), 
                f"({i+1}/{total_to_process}) 正在删除: {item_name}"
            )
            
            delete_success = emby_handler.delete_item(
                item_id=item_id, emby_server_url=processor.emby_url,
                emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
            )
            if delete_success:
                db_handler.delete_resubscribe_cache_item(item_id)
                deleted_count += 1
            
            time.sleep(0.5) # 避免请求过快

        final_message = f"批量删除任务完成！成功删除了 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_update_resubscribe_cache(processor: MediaProcessor):
    """
    【V-Final Pro - 架构恢复最终版】
    - 恢复了简洁的函数结构，所有业务逻辑都通过调用正确的全局辅助函数完成。
    """
    task_name = "刷新洗版状态 (架构恢复最终版)"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        task_manager.update_status_from_thread(0, "正在加载规则并确定扫描范围...")
        all_enabled_rules = [rule for rule in db_handler.get_all_resubscribe_rules() if rule.get('enabled')]
        library_ids_to_scan = set()
        for rule in all_enabled_rules:
            target_libs = rule.get('target_library_ids')
            if isinstance(target_libs, list):
                library_ids_to_scan.update(target_libs)
        libs_to_process_ids = list(library_ids_to_scan)

        if not libs_to_process_ids:
            task_manager.update_status_from_thread(100, "任务跳过：没有规则指定媒体库")
            return
        
        task_manager.update_status_from_thread(10, f"正在从 {len(libs_to_process_ids)} 个目标库中获取项目...")
        all_items_base_info = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            fields="ProviderIds,Name,Type,ChildCount,_SourceLibraryId"
        ) or []
        
        current_db_status_map = {item['item_id']: item['status'] for item in db_handler.get_all_resubscribe_cache()}
        total = len(all_items_base_info)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：在目标媒体库中未找到任何项目。")
            return

        logger.info(f"  -> 将为 {total} 个媒体项目获取详情并按规则检查洗版状态...")
        cache_update_batch = []
        processed_count = 0
        library_to_rule_map = {}
        for rule in reversed(all_enabled_rules):
            target_libs = rule.get('target_library_ids')
            if isinstance(target_libs, list):
                for lib_id in target_libs:
                    library_to_rule_map[lib_id] = rule

        def process_item_for_cache(item_base_info):
            item_id = item_base_info.get('Id')
            item_name = item_base_info.get('Name')
            source_lib_id = item_base_info.get('_SourceLibraryId')

            if current_db_status_map.get(item_id) == 'ignored': return None
        
            try:
                applicable_rule = library_to_rule_map.get(source_lib_id)
                if not applicable_rule:
                    return { "item_id": item_id, "status": 'ok', "reason": "无匹配规则" }
                
                item_details = emby_handler.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if not item_details: return None
                
                tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                media_metadata = db_handler.get_media_metadata_by_tmdb_id(tmdb_id) if tmdb_id else None
                item_type = item_details.get('Type')
                if item_type == 'Series' and item_details.get('ChildCount', 0) > 0:
                    first_episode_list = emby_handler.get_series_children(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, "Episode", "MediaStreams,Path")
                    if first_episode_list:
                        item_details['MediaStreams'] = first_episode_list[0].get('MediaStreams', [])
                        item_details['Path'] = first_episode_list[0].get('Path', '')
                
                needs_resubscribe, reason = _item_needs_resubscribe(item_details, applicable_rule, media_metadata)
                old_status = current_db_status_map.get(item_id)
                new_status = 'ok' if not needs_resubscribe else ('subscribed' if old_status == 'subscribed' else 'needed')
                
                media_streams = item_details.get('MediaStreams', [])
                video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)
                file_name_lower = os.path.basename(item_details.get('Path', '')).lower()
                
                raw_effect_tag = _get_standardized_effect(file_name_lower, video_stream)
                
                EFFECT_DISPLAY_MAP = {'dovi_p8': 'DoVi P8', 'dovi_p7': 'DoVi P7', 'dovi_p5': 'DoVi P5', 'dovi_other': 'DoVi (Other)', 'hdr10+': 'HDR10+', 'hdr': 'HDR', 'sdr': 'SDR'}
                effect_str = EFFECT_DISPLAY_MAP.get(raw_effect_tag, raw_effect_tag.upper())

                resolution_str = "未知"
                if video_stream and video_stream.get('Width'):
                    width = video_stream.get('Width')
                    if width >= 3840: resolution_str = "4K"
                    elif width >= 1920: resolution_str = "1080p"
                    else: resolution_str = f"{width}p"
                quality_str = _extract_quality_tag_from_filename(file_name_lower, video_stream)
                
                AUDIO_LANG_MAP = {'chi': '国语', 'zho': '国语', 'yue': '粤语', 'eng': '英语'}
                CHINESE_AUDIO_CODES = {'chi', 'zho', 'yue'}
                audio_langs_raw = list(set(s.get('Language') for s in media_streams if s.get('Type') == 'Audio' and s.get('Language')))
                display_audio_langs = []
                has_other_audio = False
                for lang in audio_langs_raw:
                    if lang in CHINESE_AUDIO_CODES or lang == 'eng':
                        display_audio_langs.append(AUDIO_LANG_MAP.get(lang, lang))
                    else:
                        has_other_audio = True
                display_audio_langs = sorted(list(set(display_audio_langs)))
                if has_other_audio: display_audio_langs.append('...')
                audio_str = ', '.join(display_audio_langs) or '无'

                SUBTITLE_LANG_MAP = {'chi': '中字', 'zho': '中字', 'eng': '英文'}
                CHINESE_SUB_CODES = {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn'}
                subtitle_langs_raw = list(set(s.get('Language') for s in media_streams if s.get('Type') == 'Subtitle' and s.get('Language')))
                display_subtitle_langs = []
                has_other_subtitle = False
                for lang in subtitle_langs_raw:
                    lang_lower = str(lang).lower()
                    if lang_lower in CHINESE_SUB_CODES:
                        display_subtitle_langs.append(SUBTITLE_LANG_MAP.get('chi'))
                    elif lang_lower == 'eng':
                        display_subtitle_langs.append(SUBTITLE_LANG_MAP.get('eng'))
                    else:
                        has_other_subtitle = True
                display_subtitle_langs = sorted(list(set(display_subtitle_langs)))
                if has_other_subtitle: display_subtitle_langs.append('...')
                subtitle_str = ', '.join(display_subtitle_langs) or '无'
                
                return {
                    "item_id": item_id, "item_name": item_details.get('Name'), "tmdb_id": tmdb_id, "item_type": item_type, "status": new_status, 
                    "reason": reason, "resolution_display": resolution_str, "quality_display": quality_str, "effect_display": effect_str,
                    "audio_display": audio_str, "subtitle_display": subtitle_str,
                    "audio_languages_raw": audio_langs_raw, "subtitle_languages_raw": subtitle_langs_raw,
                    "matched_rule_id": applicable_rule.get('id'), "matched_rule_name": applicable_rule.get('name'), "source_library_id": source_lib_id
                }
            except Exception as e:
                logger.error(f"处理项目 '{item_name}' (ID: {item_id}) 时线程内发生错误: {e}", exc_info=True)
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(process_item_for_cache, item): item for item in all_items_base_info}
            for future in as_completed(future_to_item):
                if processor.is_stop_requested(): break
                result = future.result()
                if result: cache_update_batch.append(result)
                processed_count += 1
                progress = int(20 + (processed_count / (total or 1)) * 80)
                task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {future_to_item[future].get('Name')}")

        if cache_update_batch:
            logger.info(f"分析完成，正在将 {len(cache_update_batch)} 条记录写入缓存表...")
            db_handler.upsert_resubscribe_cache_batch(cache_update_batch)
            
            task_manager.update_status_from_thread(99, "缓存写入完成，即将刷新...")
            time.sleep(1) # 给前端一点反应时间，确保信号被接收

        final_message = "媒体洗版状态刷新完成！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

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
# ======================================================================
# ★★★ 媒体去重模块 (Media Cleanup Module) - 新增 ★★★
# ======================================================================

def _get_version_properties(version: Optional[Dict]) -> Dict:
    """【V4 - 杜比Profile细分版】从单个版本信息中提取并计算属性，增加特效标准化。"""
    if not version or not isinstance(version, dict):
        return {
            'id': 'unknown_or_invalid', 'path': '', 'quality': 'unknown',
            'resolution': 'unknown', 'effect': 'sdr', 'filesize': 0
        }

    path_lower = version.get("Path", "").lower()
    
    # --- 质量标准化 (逻辑不变) ---
    QUALITY_ALIASES = {
        "remux": "remux", "bluray": "blu-ray", "blu-ray": "blu-ray",
        "web-dl": "web-dl", "webdl": "web-dl", "webrip": "webrip",
        "hdtv": "hdtv", "dvdrip": "dvdrip"
    }
    QUALITY_HIERARCHY = ["remux", "blu-ray", "web-dl", "webrip", "hdtv", "dvdrip"]
    quality = "unknown"
    for alias, official_name in QUALITY_ALIASES.items():
        if (f".{alias}." in path_lower or f" {alias} " in path_lower or
            f"-{alias}-" in path_lower or f"·{alias}·" in path_lower):
            current_priority = QUALITY_HIERARCHY.index(quality) if quality in QUALITY_HIERARCHY else 999
            new_priority = QUALITY_HIERARCHY.index(official_name)
            if new_priority < current_priority:
                quality = official_name

    # --- 分辨率标准化 (逻辑不变) ---
    resolution_tag = "unknown"
    resolution_wh = version.get("resolution_wh", (0, 0))
    width = resolution_wh[0]
    if width >= 3840: resolution_tag = "2160p"
    elif width >= 1920: resolution_tag = "1080p"
    elif width >= 1280: resolution_tag = "720p"

    # --- ★★★ 核心修改：调用新的辅助函数来标准化特效 ★★★ ---
    video_stream = version.get("video_stream")
    effect_tag = _get_standardized_effect(path_lower, video_stream)

    return {
        "id": version.get("id"),
        "quality": quality,
        "resolution": resolution_tag,
        "effect": effect_tag, # <-- 使用新字段
        "filesize": version.get("filesize", 0)
    }

def _determine_best_version_by_rules(versions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """【V6 - 杜比Profile细分版】"""
    
    rules = db_handler.get_setting('media_cleanup_rules')
    if not rules:
        # 更新默认规则，加入 effect
        rules = [
            {"id": "quality", "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            {"id": "resolution", "priority": ["2160p", "1080p", "720p"]},
            # ★★★ 核心修改：更新默认特效规则，细化杜比视界 ★★★
            {"id": "effect", "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]},
            {"id": "filesize", "priority": "desc"}
        ]

    processed_rules = []
    for rule in rules:
        new_rule = rule.copy()
        if rule.get("id") == "quality" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            normalized_priority = []
            for p in new_rule["priority"]:
                p_lower = str(p).lower()
                if p_lower == "bluray": p_lower = "blu-ray"
                if p_lower == "webdl": p_lower = "web-dl"
                normalized_priority.append(p_lower)
            new_rule["priority"] = normalized_priority
        # ★★★ 新增：对特效规则也进行标准化处理 ★★★
        elif rule.get("id") == "effect" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            new_rule["priority"] = [str(p).lower().replace(" ", "_") for p in new_rule["priority"]]

        processed_rules.append(new_rule)
    
    version_properties = [_get_version_properties(v) for v in versions if v is not None]

    from functools import cmp_to_key
    def compare_versions(item1_props, item2_props):
        for rule in processed_rules:
            if not rule.get("enabled", True): continue
            
            rule_id = rule.get("id")
            val1 = item1_props.get(rule_id)
            val2 = item2_props.get(rule_id)

            if rule_id == "filesize":
                if val1 > val2: return -1
                if val1 < val2: return 1
                continue

            priority_list = rule.get("priority", [])
            try:
                index1 = priority_list.index(val1) if val1 in priority_list else 999
                index2 = priority_list.index(val2) if val2 in priority_list else 999
                
                if index1 < index2: return -1
                if index1 > index2: return 1
            except (ValueError, TypeError):
                continue
        return 0

    sorted_versions = sorted(version_properties, key=cmp_to_key(compare_versions))
    
    best_version_id = sorted_versions[0]['id'] if sorted_versions else None
    
    # 返回原始版本信息和最佳ID
    return versions, best_version_id

# ★★★ 核心修改 2/2: 更新 _determine_best_version_by_rules 函数 ★★★
def _determine_best_version_by_rules(versions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """【V5 - 特效支持最终版】"""
    
    rules = db_handler.get_setting('media_cleanup_rules')
    if not rules:
        # 更新默认规则，加入 effect
        rules = [
            {"id": "quality", "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            {"id": "resolution", "priority": ["2160p", "1080p", "720p"]},
            {"id": "effect", "priority": ["dovi", "hdr10+", "hdr", "sdr"]}, # <-- 新增默认特效规则
            {"id": "filesize", "priority": "desc"}
        ]

    processed_rules = []
    for rule in rules:
        new_rule = rule.copy()
        if rule.get("id") == "quality" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            normalized_priority = []
            for p in new_rule["priority"]:
                p_lower = str(p).lower()
                if p_lower == "bluray": p_lower = "blu-ray"
                if p_lower == "webdl": p_lower = "web-dl"
                normalized_priority.append(p_lower)
            new_rule["priority"] = normalized_priority
        # ★★★ 新增：对特效规则也进行标准化处理 ★★★
        elif rule.get("id") == "effect" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            new_rule["priority"] = [str(p).lower() for p in new_rule["priority"]]

        processed_rules.append(new_rule)
    
    version_properties = [_get_version_properties(v) for v in versions if v is not None]

    from functools import cmp_to_key
    def compare_versions(item1_props, item2_props):
        for rule in processed_rules:
            if not rule.get("enabled", True): continue
            
            rule_id = rule.get("id")
            val1 = item1_props.get(rule_id)
            val2 = item2_props.get(rule_id)

            if rule_id == "filesize":
                if val1 > val2: return -1
                if val1 < val2: return 1
                continue

            priority_list = rule.get("priority", [])
            try:
                index1 = priority_list.index(val1) if val1 in priority_list else 999
                index2 = priority_list.index(val2) if val2 in priority_list else 999
                
                if index1 < index2: return -1
                if index1 > index2: return 1
            except (ValueError, TypeError):
                continue
        return 0

    sorted_versions = sorted(version_properties, key=cmp_to_key(compare_versions))
    
    best_version_id = sorted_versions[0]['id'] if sorted_versions else None
    
    # 返回原始版本信息和最佳ID
    return versions, best_version_id

def task_scan_for_cleanup_issues(processor: MediaProcessor):
    """
    【V15 - 特效支持版】
    在构造 versions_info 时，将 video_stream 传递下去。
    """
    task_name = "扫描媒体库重复项"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (特效支持模式) ---")
    task_manager.update_status_from_thread(0, "正在准备扫描媒体库...")

    try:
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        task_manager.update_status_from_thread(5, f"正在从 {len(libs_to_process_ids)} 个媒体库获取项目...")
        all_emby_items = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            # 确保请求了 MediaStreams
            fields="ProviderIds,Name,Type,MediaSources,Path,ProductionYear,MediaStreams"
        ) or []

        if not all_emby_items:
            task_manager.update_status_from_thread(100, "任务完成：在指定媒体库中未找到任何项目。")
            return

        task_manager.update_status_from_thread(30, f"已获取 {len(all_emby_items)} 个项目，正在分析...")
        
        media_map = collections.defaultdict(list)
        for item in all_emby_items:
            tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
            item_type = item.get("Type")
            if tmdb_id and item_type:
                media_map[(tmdb_id, item_type)].append(item)

        duplicate_tasks = []
        for (tmdb_id, item_type), items in media_map.items():
            if len(items) > 1:
                logger.info(f"  -> [发现重复] TMDB ID {tmdb_id} (类型: {item_type}) 关联了 {len(items)} 个独立的媒体项。")
                versions_info = []
                for item in items:
                    source = item.get("MediaSources", [{}])[0]
                    video_stream = next((s for s in source.get("MediaStreams", []) if s.get("Type") == "Video"), None)
                    versions_info.append({
                        "id": item.get("Id"),
                        "path": source.get("Path") or item.get("Path") or "",
                        "size": source.get("Size", 0),
                        "resolution_wh": (video_stream.get("Width", 0), video_stream.get("Height", 0)) if video_stream else (0, 0),
                        # ★★★ 核心修改：把 video_stream 整个传下去 ★★★
                        "video_stream": video_stream
                    })
                
                analyzed_versions, best_id = _determine_best_version_by_rules(versions_info)
                best_item_name = next((item.get("Name") for item in items if item.get("Id") == best_id), items[0].get("Name"))
                
                duplicate_tasks.append({
                    "task_type": "duplicate",
                    "tmdb_id": tmdb_id,
                    "item_type": item_type,
                    "item_name": best_item_name,
                    "versions_info_json": analyzed_versions, "best_version_id": best_id
                })

        task_manager.update_status_from_thread(90, f"分析完成，正在将 {len(duplicate_tasks)} 组重复项写入数据库...")
        db_handler.batch_insert_cleanup_tasks(duplicate_tasks)

        final_message = f"扫描完成！共发现 {len(duplicate_tasks)} 组重复项，待清理。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_purge_ghost_actors(processor: MediaProcessor):
    """
    【高危 V4 - 增强日志版】
    - 增加了更详细的统计日志，明确报告每一步处理了多少演员，以及最终筛选出多少幽灵演员。
    - 修复了在没有发现幽灵演员时日志过于简单的问题。
    """
    task_name = "清理幽灵演员"
    logger.warning(f"--- !!! 开始执行高危任务: '{task_name}' !!! ---")
    
    task_manager.update_status_from_thread(0, "正在读取媒体库配置...")

    try:
        # 1. 读取并验证媒体库配置
        config = processor.config
        library_ids_to_process = config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])

        if not library_ids_to_process:
            logger.error("任务中止：未在设置中选择任何要处理的媒体库。")
            task_manager.update_status_from_thread(-1, "任务失败：未选择媒体库")
            return

        logger.info(f"将只扫描 {len(library_ids_to_process)} 个选定媒体库中的演员...")
        task_manager.update_status_from_thread(10, f"正在从 {len(library_ids_to_process)} 个媒体库中获取所有媒体...")

        # 2. 获取指定媒体库中的所有电影和剧集
        all_media_items = emby_handler.get_emby_library_items(
            base_url=processor.emby_url,
            api_key=processor.emby_api_key,
            user_id=processor.emby_user_id,
            library_ids=library_ids_to_process,
            media_type_filter="Movie,Series",
            fields="People"
        )
        if not all_media_items:
            task_manager.update_status_from_thread(100, "任务完成：在选定的媒体库中未找到任何媒体项。")
            return

        # 3. 从媒体项中提取所有唯一的演员ID
        task_manager.update_status_from_thread(30, "正在从媒体项中提取唯一的演员ID...")
        unique_person_ids = set()
        for item in all_media_items:
            for person in item.get("People", []):
                if person_id := person.get("Id"):
                    unique_person_ids.add(person_id)
        
        person_ids_to_fetch = list(unique_person_ids)
        logger.info(f"在选定媒体库中，共识别出 {len(person_ids_to_fetch)} 位独立演员。")

        if not person_ids_to_fetch:
            task_manager.update_status_from_thread(100, "任务完成：未在媒体项中找到任何演员。")
            return

        # 4. 分批获取这些演员的完整详情
        task_manager.update_status_from_thread(50, f"正在分批获取 {len(person_ids_to_fetch)} 位演员的完整详情...")
        all_people_in_scope_details = []
        batch_size = 500
        for i in range(0, len(person_ids_to_fetch), batch_size):
            if processor.is_stop_requested():
                logger.info("在分批获取演员详情阶段，任务被中止。")
                break
            
            batch_ids = person_ids_to_fetch[i:i + batch_size]
            logger.debug(f"  -> 正在获取批次 {i//batch_size + 1} 的演员详情 ({len(batch_ids)} 个)...")

            person_details_batch = emby_handler.get_emby_items_by_id(
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                user_id=processor.emby_user_id,
                item_ids=batch_ids,
                fields="ProviderIds,Name"
            )
            if person_details_batch:
                all_people_in_scope_details.extend(person_details_batch)

        if processor.is_stop_requested():
            logger.warning("任务已中止。")
            task_manager.update_status_from_thread(100, "任务已中止。")
            return
        
        # ★★★ 新增：详细的获取结果统计日志 ★★★
        logger.info(f"详情获取完成：成功获取到 {len(all_people_in_scope_details)} 位演员的完整详情。")

        # 5. 基于完整的详情，筛选出真正的“幽灵”演员
        ghosts_to_delete = [
            p for p in all_people_in_scope_details 
            if not p.get("ProviderIds", {}).get("Tmdb")
        ]
        total_to_delete = len(ghosts_to_delete)

        # ★★★ 新增：核心的筛选结果统计日志 ★★★
        logger.info(f"筛选完成：在 {len(all_people_in_scope_details)} 位演员中，发现 {total_to_delete} 个没有TMDb ID的幽灵演员。")

        if total_to_delete == 0:
            # ★★★ 优化：更清晰的完成日志 ★★★
            logger.info("扫描完成，在选定媒体库中未发现需要清理的幽灵演员。")
            task_manager.update_status_from_thread(100, "扫描完成，未发现无TMDb ID的演员。")
            return
        
        logger.warning(f"共发现 {total_to_delete} 个幽灵演员，即将开始删除...")
        deleted_count = 0

        # 6. 执行删除
        for i, person in enumerate(ghosts_to_delete):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break
            
            person_id = person.get("Id")
            person_name = person.get("Name")
            
            progress = 60 + int((i / total_to_delete) * 40)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_to_delete}) 正在删除: {person_name}")

            success = emby_handler.delete_person_custom_api(
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                person_id=person_id
            )
            
            if success:
                deleted_count += 1
            
            time.sleep(0.2)

        final_message = f"清理完成！共找到 {total_to_delete} 个目标，成功删除了 {deleted_count} 个。"
        if processor.is_stop_requested():
            final_message = f"任务已中止。共删除了 {deleted_count} 个演员。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_execute_cleanup(processor: MediaProcessor, task_ids: List[int], **kwargs):
    """
    后台任务：执行指定的一批媒体去重任务（删除多余文件）。
    这是一个高危的写操作。
    """
    # ★★★ 核心修复：这个函数签名现在可以正确接收 processor 和 task_ids 两个位置参数，
    # 同时用 **kwargs 忽略掉 task_manager 传来的其他所有参数。

    if not task_ids or not isinstance(task_ids, list):
        logger.error("执行媒体去重任务失败：缺少有效的 'task_ids' 参数。")
        task_manager.update_status_from_thread(-1, "任务失败：缺少任务ID")
        return

    task_name = "执行媒体去重"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (任务ID: {task_ids}) ---")
    
    try:
        tasks_to_execute = db_handler.get_cleanup_tasks_by_ids(task_ids)
        total = len(tasks_to_execute)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：未找到指定的清理任务。")
            return

        deleted_count = 0
        for i, task in enumerate(tasks_to_execute):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break
            
            task_id = task['id']
            item_name = task['item_name']
            best_version_id = task['best_version_id']
            versions = task['versions_info_json']

            task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在清理: {item_name}")

            for version in versions:
                version_id_to_check = version.get('id')
                if version_id_to_check != best_version_id:
                    logger.warning(f"  -> 准备删除劣质版本: {version.get('path')}")
                    
                    id_to_delete = version_id_to_check
                    
                    success = emby_handler.delete_item(
                        item_id=id_to_delete,
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    )
                    if success:
                        deleted_count += 1
                        logger.info(f"    -> 成功删除 ID: {id_to_delete}")
                    else:
                        logger.error(f"    -> 删除 ID: {id_to_delete} 失败！")
            
            db_handler.batch_update_cleanup_task_status([task_id], 'processed')

        final_message = f"清理完成！共处理 {total} 个任务，删除了 {deleted_count} 个多余版本/文件。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
# ★★★ 用户数据全量同步任务 ★★★
def task_sync_all_user_data(processor: MediaProcessor):
    """
    用户数据全量同步任务
    """
    task_name = "同步用户数据"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ... (获取 all_users 的逻辑不变) ...
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户...")
        emby_url = processor.emby_url
        emby_key = processor.emby_api_key
        all_users = emby_handler.get_all_emby_users_from_server(emby_url, emby_key)
        if not all_users:
            task_manager.update_status_from_thread(100, "任务完成：未能从Emby获取到任何用户。")
            return
        db_handler.upsert_emby_users_batch(all_users)
        total_users = len(all_users)
        logger.info(f"  -> 共找到 {total_users} 个Emby用户，将逐一同步其数据...")

        for i, user in enumerate(all_users):
            user_id = user.get('Id')
            user_name = user.get('Name')
            if not user_id: continue
            if processor.is_stop_requested(): break

            progress = int((i / total_users) * 100)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_users}) 正在同步用户: {user_name}")

            user_items_with_data = emby_handler.get_all_user_view_data(user_id, emby_url, emby_key)
            if not user_items_with_data:
                continue
            
            final_data_map = {}

            # 聚合与去重逻辑保持不变
            for item in user_items_with_data:
                item_type = item.get('Type')
                item_id = item.get('Id')
                target_id = item_id if item_type in ['Movie', 'Series'] else item.get('SeriesId')
                if not target_id: continue

                if target_id not in final_data_map:
                    final_data_map[target_id] = item
                    if item_type == 'Episode':
                        final_data_map[target_id]['Id'] = target_id
                else:
                    existing_item = final_data_map[target_id]
                    new_user_data = item.get('UserData', {})
                    if 'PlaybackPositionTicks' in new_user_data:
                        existing_item['UserData']['PlaybackPositionTicks'] = new_user_data['PlaybackPositionTicks']
                    if 'Played' in new_user_data:
                        existing_item['UserData']['Played'] = new_user_data['Played']
            
            final_data_to_upsert = list(final_data_map.values())
            
            # ★★★ 核心改造：调用一个新的、不包含 last_played_date 的数据库函数 ★★★
            db_handler.upsert_user_media_data_batch_no_date(user_id, final_data_to_upsert)
            
            logger.info(f"  -> 成功为用户 '{user_name}' 同步了 {len(final_data_to_upsert)} 条媒体状态。")

        final_message = f"任务完成！已成功为 {total_users} 个用户同步数据。"
        if processor.is_stop_requested(): final_message = "任务已中断。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_apply_main_cast_to_episodes(processor: MediaProcessor, series_id: str, episode_ids: List[str]):
    """
    【新增】轻量级任务：将剧集主项目的演员表应用到指定的新增分集。
    """
    try:
        # 1. 获取剧集主项目的演员表和名字
        series_details = emby_handler.get_emby_item_details(
            series_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
            fields="People,Name"
        )
        if not series_details:
            logger.error(f"轻量化任务失败：无法获取剧集 {series_id} 的详情。")
            return

        series_name = series_details.get("Name", f"ID:{series_id}")

        # 2. 准备好 Emby Handler 需要的演员表格式
        cast_for_handler = []
        # 智能筛选，只同步类型为 'Actor' 的人员
        for person in series_details.get("People", []):
             if person.get("Type") == "Actor":
                cast_for_handler.append({
                    "name": person.get("Name"),
                    "character": person.get("Role"),
                    "emby_person_id": person.get("Id")
                    # ProviderIds 不是必须的，因为 emby_person_id 是精确匹配
                })
        
        if not cast_for_handler:
            logger.warning(f"轻量化任务：剧集 '{series_name}' 主项目没有演员信息，无需同步。")
            return

        # 3. 遍历并逐个更新指定的分集
        logger.info(f"轻量化任务：开始为《{series_name}》的 {len(episode_ids)} 个新分集同步演员表...")
        for episode_id in episode_ids:
            # 检查停止信号，以便任务可以被中止
            if processor.is_stop_requested():
                logger.warning("轻量化同步任务被中止。")
                break

            emby_handler.update_emby_item_cast(
                item_id=episode_id,
                new_cast_list_for_handler=cast_for_handler,
                emby_server_url=processor.emby_url,
                emby_api_key=processor.emby_api_key,
                user_id=processor.emby_user_id
            )
            time.sleep(0.2) # 加入微小延迟，避免请求过于密集
        
        logger.info(f"轻量化任务完成：已为《{series_name}》的新分集同步了演员表。")
    except Exception as e:
        logger.error(f"执行轻量化分集同步任务时发生错误: {e}", exc_info=True)