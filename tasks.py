# tasks.py

import time
import os
import json
import psycopg2
import pytz
import threading
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
import logging
from typing import Dict, Any
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
    """
    task_name = "同步演员映射"
    logger.trace(f"开始执行 '{task_name}'...")
    
    try:
        # ★★★ 核心修改：将整个 processor.config 传递给 UnifiedSyncHandler ★★★
        config = processor.config
        
        sync_handler = UnifiedSyncHandler(
            emby_url=config.get("emby_server_url"),
            emby_api_key=config.get("emby_api_key"),
            emby_user_id=config.get("emby_user_id"),
            tmdb_api_key=config.get("tmdb_api_key", ""),
            config=config  # <--- 把完整的配置传进去
        )
        
        # 假设您的 task_manager 可以处理 stop_event
        stop_event = threading.Event()
        # task_manager.register_stop_handler(stop_event.set) # 示例

        sync_handler.sync_emby_person_map_to_db(
            update_status_callback=task_manager.update_status_from_thread,
            stop_event=stop_event
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
    【V4 - 规则库实时同步修复版】
    - 在将新媒体添加到规则类合集后，同步更新数据库中的JSON缓存，确保虚拟库实时刷新。
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

        # --- 匹配 Filter (筛选) 类型的合集 ---
        engine = FilterEngine()
        matching_filter_collections = engine.find_matching_collections(item_metadata)

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
            
            library_info = emby_handler.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
            
            if not library_info:
                logger.warning(f"  -> 无法为项目 {item_id} 定位到其所属的媒体库根，跳过封面生成。")
                return

            library_id = library_info.get("Id")
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
    }

    if not table_data:
        return [], []

    columns = list(table_data[0].keys())
    # 使用小写表名来匹配规则
    table_json_rules = JSONB_COLUMNS.get(table_name.lower(), set())
    
    prepared_rows = []
    for row_dict in table_data:
        row_values = []
        for col_name in columns:
            value = row_dict.get(col_name)
            
            # ★ 核心逻辑: 如果列是 JSONB 类型且值非空，使用 Json 适配器包装 ★
            if col_name in table_json_rules and value is not None:
                # Json() 会告诉 psycopg2: "请将这个 Python 对象作为 JSON 处理"
                value = Json(value)
            
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
    【最终修正版】执行演员名翻译的查漏补缺工作，并使用正确的全局状态更新函数。
    """
    try:
        # ✨✨✨ 修正：直接调用全局函数，而不是processor的方法 ✨✨✨
        task_manager.update_status_from_thread(5, "正在准备需要翻译的演员数据...")
        
        # 1. 调用数据准备函数
        translation_map, name_to_persons_map = emby_handler.prepare_actor_translation_data(
            emby_url=processor.emby_url,
            emby_api_key=processor.emby_api_key,
            user_id=processor.emby_user_id,
            ai_translator=processor.ai_translator,
            stop_event=processor.get_stop_event()
        )

        if not translation_map:
            task_manager.update_status_from_thread(100, "任务完成，没有需要翻译的演员。")
            return

        total_to_update = len(translation_map)
        task_manager.update_status_from_thread(50, f"数据准备完毕，开始更新 {total_to_update} 个演员名...")
        
        update_count = 0
        processed_count = 0

        # 2. 主循环
        for original_name, translated_name in translation_map.items():
            processed_count += 1
            if processor.is_stop_requested():
                logger.info("演员翻译任务被用户中断。")
                break
            
            if not translated_name or original_name == translated_name:
                continue

            persons_to_update = name_to_persons_map.get(original_name, [])
            for person in persons_to_update:
                # 3. 更新单个条目
                success = emby_handler.update_person_details(
                    person_id=person.get("Id"),
                    new_data={"Name": translated_name},
                    emby_server_url=processor.emby_url,
                    emby_api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
                if success:
                    update_count += 1
                    time.sleep(0.2)

            # 4. 更新进度
            progress = int(50 + (processed_count / total_to_update) * 50)
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total_to_update}) 正在更新: {original_name} -> {translated_name}")

        # 任务结束时，也直接调用全局函数
        final_message = f"任务完成！共更新了 {update_count} 个演员名。"
        if processor.is_stop_requested():
            final_message = "任务已中断。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行演员翻译任务时出错: {e}", exc_info=True)
        # 在异常处理中也直接调用全局函数
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
    【V5.1 - PG 兼容版】
    - 修复了因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    """
    task_manager.update_status_from_thread(0, "正在启动智能订阅任务...")
    
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("智能订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        today = date.today()
        task_manager.update_status_from_thread(10, "智能订阅已启动...")
        successfully_subscribed_items = []

        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()

            # ★★★ 1. 处理原生电影合集 (collections_info) ★★★
            if not processor.is_stop_requested():
                # ... (这部分逻辑无需修改，因为 db_handler.py 中的 get_all_collections 已经处理了JSON) ...
                task_manager.update_status_from_thread(20, "正在检查原生电影合集...")
                sql_query_native_movies = "SELECT * FROM collections_info WHERE status = 'has_missing' AND missing_movies_json IS NOT NULL AND missing_movies_json != '[]'"
                cursor.execute(sql_query_native_movies)
                native_collections_to_check = cursor.fetchall()
                logger.info(f"  -> 找到 {len(native_collections_to_check)} 个有缺失影片的原生合集。")
                for collection in native_collections_to_check:
                    # ... (内部循环逻辑保持不变) ...
                    movies_to_keep = []
                    # ★★★ 核心修复：直接使用已经是列表的 missing_movies_json 字段 ★★★
                    all_movies = collection['missing_movies_json']
                    movies_changed = False
                    for movie in all_movies:
                        # ... (订阅逻辑不变) ...
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
                                if moviepilot_handler.subscribe_movie_to_moviepilot(movie, config_manager.APP_CONFIG):
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
            if not processor.is_stop_requested():
                # ... (这部分逻辑也需要修复) ...
                task_manager.update_status_from_thread(60, "正在检查缺失的剧集...")
                sql_query = "SELECT * FROM watchlist WHERE status IN ('Watching', 'Paused') AND missing_info_json IS NOT NULL AND missing_info_json != '[]'"
                cursor.execute(sql_query)
                series_to_check = cursor.fetchall()
                for series in series_to_check:
                    if processor.is_stop_requested(): break
                    series_name = series['item_name']
                    logger.info(f"  -> 正在检查: 《{series_name}》")
                    try:
                        # ★★★ 核心修复：直接使用已经是字典的 missing_info_json 字段 ★★★
                        missing_info = series['missing_info_json']
                        missing_seasons = missing_info.get('missing_seasons', [])
                        # ... (内部循环逻辑保持不变) ...
                        if not missing_seasons: continue
                        seasons_to_keep = []
                        seasons_changed = False
                        for season in missing_seasons:
                            # ... (订阅逻辑不变) ...
                            season_num = season.get('season_number')
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
                                success = moviepilot_handler.subscribe_series_to_moviepilot(dict(series), season['season_number'], config_manager.APP_CONFIG)
                                if success:
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
            if not processor.is_stop_requested():
                # ... (这部分逻辑也需要修复) ...
                task_manager.update_status_from_thread(70, "正在检查自定义榜单合集...")
                sql_query_custom_collections = "SELECT * FROM custom_collections WHERE type = 'list' AND health_status = 'has_missing' AND generated_media_info_json IS NOT NULL AND generated_media_info_json != '[]'"
                cursor.execute(sql_query_custom_collections)
                custom_collections_to_check = cursor.fetchall()
                for collection in custom_collections_to_check:
                    if processor.is_stop_requested(): break
                    collection_id = collection['id']
                    collection_name = collection['name']
                    try:
                        # ★★★ 核心修复：直接使用已经是字典/列表的 _json 字段 ★★★
                        definition = collection['definition_json']
                        all_media = collection['generated_media_info_json']
                        # ... (内部循环和订阅逻辑保持不变) ...
                        item_type_from_db = definition.get('item_type', 'Movie')
                        authoritative_type = None
                        if isinstance(item_type_from_db, list) and item_type_from_db:
                            authoritative_type = item_type_from_db[0]
                        elif isinstance(item_type_from_db, str):
                            authoritative_type = item_type_from_db
                        if authoritative_type not in ['Movie', 'Series']:
                            authoritative_type = 'Movie'
                        media_to_keep = []
                        media_changed = False
                        for media_item in all_media:
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
                                    success = False
                                    media_title = media_item.get('title', '未知标题')
                                    if authoritative_type == 'Movie':
                                        success = moviepilot_handler.subscribe_movie_to_moviepilot(media_item, config_manager.APP_CONFIG)
                                    elif authoritative_type == 'Series':
                                        series_info = { "item_name": media_title, "tmdb_id": media_item.get('tmdb_id') }
                                        success = moviepilot_handler.subscribe_series_to_moviepilot(series_info, season_number=None, config=config_manager.APP_CONFIG)
                                    if success:
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

            conn.commit()

        if successfully_subscribed_items:
            summary = "  -> ✅ 任务完成！已自动订阅: " + ", ".join(successfully_subscribed_items)
            logger.info(summary)
            task_manager.update_status_from_thread(100, summary)
        else:
            task_manager.update_status_from_thread(100, "任务完成：本次运行没有发现符合自动订阅条件的媒体。")

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
    【V3 - 精确调度最终版】自动化任务链。
    - 修复了因任务注册表升级为三元组而导致的解包错误。
    - 现在能为链中的每一个子任务，精确地选择并传递正确的处理器。
    """
    task_name = "自动化任务链"
    total_tasks = len(task_sequence)
    logger.info(f"--- '{task_name}' 已启动，共包含 {total_tasks} 个子任务 ---")
    task_manager.update_status_from_thread(0, f"任务链启动，共 {total_tasks} 个任务。")

    # ★★★ 核心修复 1/3：获取完整的、包含处理器类型的注册表 ★★★
    registry = get_task_registry()
    
    # ★★★ 核心修复 2/3：创建一个处理器查找表，用于动态选择 ★★★
    processor_map = {
        'media': extensions.media_processor_instance,
        'watchlist': extensions.watchlist_processor_instance,
        'actor': extensions.actor_subscription_processor_instance
    }
    
    for i, task_key in enumerate(task_sequence):
        if processor.is_stop_requested():
            logger.warning(f"'{task_name}' 被用户中止。")
            break

        task_info = registry.get(task_key)
        if not task_info:
            logger.error(f"任务链警告：在注册表中未找到任务 '{task_key}'，已跳过。")
            continue

        # ★★★ 核心修复 3/3：正确解包三元组，并动态选择处理器 ★★★
        try:
            task_function, task_description, processor_type = task_info
        except ValueError:
            logger.error(f"任务链错误：任务 '{task_key}' 的注册信息格式不正确，已跳过。")
            continue

        progress = int((i / total_tasks) * 100)
        status_message = f"({i+1}/{total_tasks}) 正在执行: {task_description}"
        logger.info(f"--- {status_message} ---")
        task_manager.update_status_from_thread(progress, status_message)

        try:
            actual_processor_to_use = processor_map.get(processor_type)
            if not actual_processor_to_use:
                logger.error(f"任务链中的子任务 '{task_description}' 无法执行：类型为 '{processor_type}' 的处理器未初始化，已跳过。")
                continue

            # 使用我们为这个子任务精确选择的处理器来执行它
            task_function(actual_processor_to_use)
            time.sleep(1)

        except Exception as e:
            error_message = f"任务链中的子任务 '{task_description}' 执行失败: {e}"
            logger.error(error_message, exc_info=True)
            task_manager.update_status_from_thread(progress, f"子任务'{task_description}'失败，继续...")
            time.sleep(3)
            continue

    final_message = f"'{task_name}' 执行完毕。"
    if processor.is_stop_requested():
        final_message = f"'{task_name}' 已中止。"
    
    logger.info(f"--- {final_message} ---")
    task_manager.update_status_from_thread(100, "任务链已全部执行完毕。")
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
        'sync-person-map': (task_sync_person_map, "同步演员映射", 'media', True),
        'enrich-aliases': (task_enrich_aliases, "演员数据补充", 'media', True),
        'populate-metadata': (task_populate_metadata_cache, "同步媒体数据", 'media', True),
        'full-scan': (task_run_full_scan, "中文化角色名", 'media', True),
        'actor-cleanup': (task_actor_translation_cleanup, "中文化演员名", 'media', True),
        'process-watchlist': (task_process_watchlist, "智能追剧更新", 'watchlist', True),
        'refresh-collections': (task_refresh_collections, "原生合集刷新", 'media', True),
        'custom-collections': (task_process_all_custom_collections, "自建合集刷新", 'media', True),
        'actor-tracking': (task_process_actor_subscriptions, "演员订阅扫描", 'actor', True),
        'generate-all-covers': (task_generate_all_covers, "生成所有封面", 'media', True),
        'auto-subscribe': (task_auto_subscribe, "智能订阅缺失", 'media', True),
        'sync-images-map': (task_full_image_sync, "覆盖缓存备份", 'media', True),

        # --- 不适合任务链的、需要特定参数的任务 ---
        'process_all_custom_collections': (task_process_all_custom_collections, "生成所有自建合集", 'media', False),
        'process-single-custom-collection': (task_process_custom_collection, "生成单个自建合集", 'media', False),
        'revival-check': (task_run_revival_check, "检查剧集复活", 'watchlist', False),
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

# ★★★ 一键生成所有合集的后台任务，核心优化在于只获取一次Emby媒体库 ★★★
def task_process_all_custom_collections(processor: MediaProcessor):
    """
    【V6 - Emby ID 内嵌 & 排序保持最终版】
    - 在循环外一次性获取全库媒体数据，提高效率。
    - 严格确保榜单的原始排序被存入数据库并同步到Emby。
    - 将媒体项的 Emby ID 一并存入 generated_media_info_json。
    """
    task_name = "生成所有自建合集"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
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
                if collection_type == 'list' and definition.get('url', '').startswith('maoyan://'):
                    importer = ListImporter(processor.tmdb_api_key)
                    greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
                    tmdb_items = greenlet.get()
                else:
                    if collection_type == 'list':
                        importer = ListImporter(processor.tmdb_api_key)
                        tmdb_items = importer.process(definition)
                    elif collection_type == 'filter':
                        engine = FilterEngine()
                        tmdb_items = engine.execute_filter(definition)
                
                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，跳过。")
                    db_handler.update_custom_collection_after_sync(collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]", "generated_emby_ids_json": "[]"})
                    continue

                ordered_emby_ids_in_library = [
                    tmdb_to_emby_item_map[item['id']]['Id'] 
                    for item in tmdb_items if item['id'] in tmdb_to_emby_item_map
                ]

                emby_collection_id = emby_handler.create_or_update_collection_with_emby_ids(
                    collection_name=collection_name, 
                    emby_ids_in_library=ordered_emby_ids_in_library, 
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key, 
                    user_id=processor.emby_user_id,
                    prefetched_collection_map=prefetched_collection_map
                )
                
                if not emby_collection_id:
                    raise RuntimeError("在Emby中创建或更新合集失败。")
                
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc)
                }

                if collection_type == 'list':
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
                    # ★★★ 核心修复 1: 为规则筛选类合集生成精简版JSON ★★★
                    logger.debug(f"  -> 为规则筛选合集 '{collection_name}' 生成精简版媒体信息JSON...")
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
                    library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                    if library_info:
                        item_count_to_pass = update_data.get('in_library_count', 0)
                        if collection_type == 'list': item_count_to_pass = '榜单'
                        
                        # ★★★ 核心修复 2: 调用封面生成器时，传入内容类型 ★★★
                        cover_service.generate_for_library(
                            emby_server_id='main_emby',
                            library=library_info,
                            item_count=item_count_to_pass,
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
    【V11.1 - 变量未定义修复版】
    - 修复了因缺少封面配置加载逻辑导致的 "cover_config" 未定义错误。
    """
    task_name = f"处理自定义合集 (ID: {custom_collection_id})"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        task_manager.update_status_from_thread(0, "正在读取合集定义...")
        collection = db_handler.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        
        collection_name = collection['name']
        collection_type = collection['type']
        definition = collection['definition_json']
        
        item_types_for_collection = definition.get('item_type', ['Movie'])
        
        tmdb_items = []
        if collection_type == 'list' and definition.get('url', '').startswith('maoyan://'):
            logger.info(f"检测到猫眼榜单 '{collection_name}'，将启动异步后台任务...")
            task_manager.update_status_from_thread(10, f"正在后台获取猫眼榜单: {collection_name}...")
            importer = ListImporter(processor.tmdb_api_key)
            greenlet = gevent.spawn(importer._execute_maoyan_fetch, definition)
            tmdb_items = greenlet.get()
        else:
            if collection_type == 'list':
                importer = ListImporter(processor.tmdb_api_key)
                tmdb_items = importer.process(definition)
            elif collection_type == 'filter':
                engine = FilterEngine()
                tmdb_items = engine.execute_filter(definition)
        
        if not tmdb_items:
            logger.warning(f"合集 '{collection_name}' 未能生成任何媒体ID，任务结束。")
            db_handler.update_custom_collection_after_sync(custom_collection_id, {"emby_collection_id": None, "generated_media_info_json": "[]"})
            return

        task_manager.update_status_from_thread(70, f"已生成 {len(tmdb_items)} 个ID，正在Emby中创建/更新合集...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])

        all_emby_items = emby_handler.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter=",".join(item_types_for_collection), library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        ordered_emby_ids_in_library = [tmdb_to_emby_item_map[item['id']]['Id'] for item in tmdb_items if item['id'] in tmdb_to_emby_item_map]

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

        # ★★★ 核心修复：在这里添加缺失的封面配置加载逻辑 ★★★
        try:
            cover_config = db_handler.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and emby_collection_id:
                logger.info(f"  -> 检测到封面生成器已启用，将为合集 '{collection_name}' 生成封面...")
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby_handler.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    in_library_count = update_data.get('in_library_count', 0)
                    item_count_to_pass = in_library_count
                    if collection_type == 'list':
                        item_count_to_pass = '榜单'
                    cover_service.generate_for_library(
                        emby_server_id='main_emby',
                        library=library_info,
                        item_count=item_count_to_pass,
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
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ 新增：轻量级的元数据缓存填充任务 ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
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

        db_tmdb_ids = set()
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM media_metadata")
            db_tmdb_ids = {row["tmdb_id"] for row in cursor.fetchall()}
        logger.info(f"  -> 从本地数据库 media_metadata 表中获取到 {len(db_tmdb_ids)} 个媒体项。")

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
                    batch_ids = ids_to_delete_list[i:i+500]
                    sql = "DELETE FROM media_metadata WHERE tmdb_id = ANY(%s)"
                    cursor.execute(sql, (batch_ids,))
                conn.commit()
            logger.info("  -> 冗余数据清理完成。")

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
                    tmdb_id, details = future.result()
                    if tmdb_id and details:
                        tmdb_details_map[tmdb_id] = details
            
            metadata_batch = []
            for item in batch_items:
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

            if metadata_batch:
                with db_handler.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # ★★★ 修复 2/2: 改进事务处理逻辑 ★★★
                    # 开启一个总事务
                    cursor.execute("BEGIN;")
                    for idx, metadata in enumerate(metadata_batch):
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
        task_manager.update_status_from_thread(100, final_message)
        logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
        

# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ 新增：立即生成所有媒体库封面的后台任务 ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
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
