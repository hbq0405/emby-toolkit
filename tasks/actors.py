# tasks/actors.py
# 演员相关任务模块

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入需要的底层模块和共享实例
from database.connection import get_db_connection
import constants
import emby_handler
import task_manager
import utils
from actor_utils import enrich_all_actor_aliases_task
from actor_sync_handler import UnifiedSyncHandler

logger = logging.getLogger(__name__)

# --- 同步演员映射表 ---
def task_sync_person_map(processor):
    """
    【V2 - 支持进度反馈】任务：同步演员映射表。
    """
    task_name = "同步演员映射"
    logger.trace(f"开始执行 '{task_name}'...")
    
    try:
        config = processor.config
        
        sync_handler = UnifiedSyncHandler(
            emby_url=config.get("emby_server_url"),
            emby_api_key=config.get("emby_api_key"),
            emby_user_id=config.get("emby_user_id"),
            tmdb_api_key=config.get("tmdb_api_key", "")
        )
        
        # ### 修改点：将任务管理器的回调函数传递给处理器 ###
        sync_handler.sync_emby_person_map_to_db(
            update_status_callback=task_manager.update_status_from_thread
        )
        
        logger.trace(f"'{task_name}' 成功完成。")

    except Exception as e:
        logger.error(f"'{task_name}' 执行过程中发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误：同步失败 ({str(e)[:50]}...)")

# ✨✨✨ 演员数据补充函数 ✨✨✨
def task_enrich_aliases(processor, force_full_update: bool = False):
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

# --- 扫描单个演员订阅的所有作品 ---
def task_scan_actor_media(processor, subscription_id: int):
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
def task_process_actor_subscriptions(processor):
    """【新】后台任务：执行所有启用的刷新演员订阅。"""
    processor.run_scheduled_task(update_status_callback=task_manager.update_status_from_thread)

# --- 翻译演员任务 ---
def task_actor_translation_cleanup(processor):
    """
    【V4.0 - 智能原料版】
    - 扫描时，同时获取演员的TMDb ID。
    - 翻译前，利用TMDb ID从本地数据库缓存的 actor_metadata 表中反查最权威的 original_name。
    - 优先使用 original_name 进行翻译，大幅提升对非英语演员名的翻译准确率。
    - 整个过程无新增API调用，性能卓越。
    """
    task_name = "中文化演员名 (智能版)"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ======================================================================
        # 阶段 1: 扫描并聚合所有需要翻译的演员 (智能数据采集)
        # ======================================================================
        task_manager.update_status_from_thread(0, "阶段 1/3: 正在扫描 Emby，收集所有待翻译演员...")
        
        # ★★★ 核心修改 1: 准备新的数据结构 ★★★
        # 我们需要存储 Emby Name -> [Emby Person 列表] 的映射
        name_to_persons_map = {}
        # 同时，我们需要一个列表来存储需要获取 original_name 的演员信息
        actors_to_enrich = []

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
                    tmdb_id = person.get("ProviderIds", {}).get("Tmdb")
                    # 只有在有 TMDb ID 时，我们才有机会获取 original_name
                    if tmdb_id:
                        actors_to_enrich.append({"name": name, "tmdb_id": tmdb_id})
                    
                    if name not in name_to_persons_map:
                        name_to_persons_map[name] = []
                    name_to_persons_map[name].append(person)
            
            total_scanned += len(person_batch)
            task_manager.update_status_from_thread(5, f"阶段 1/3: 已扫描 {total_scanned} 名演员...")

        if not name_to_persons_map:
            logger.info("扫描完成，没有发现需要翻译的演员名。")
            task_manager.update_status_from_thread(100, "任务完成，所有演员名都无需翻译。")
            return

        logger.info(f"扫描完成！共发现 {len(name_to_persons_map)} 个外文名需要翻译。")

        # ======================================================================
        # ★★★ 新增阶段 2: 从本地数据库获取 Original Name ★★★
        # ======================================================================
        task_manager.update_status_from_thread(10, "阶段 2/3: 正在从本地缓存获取演员原始名...")
        
        # original_name -> emby_name 的映射，用于后续回填
        original_to_emby_name_map = {}
        texts_to_translate = set()
        
        tmdb_ids_to_query = list(set([int(actor['tmdb_id']) for actor in actors_to_enrich if actor.get('tmdb_id')]))

        if tmdb_ids_to_query:
            tmdb_id_to_original_name = {}
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY(%s) 进行高效批量查询
                    query = "SELECT tmdb_id, original_name FROM actor_metadata WHERE tmdb_id = ANY(%s)"
                    cursor.execute(query, (tmdb_ids_to_query,))
                    for row in cursor.fetchall():
                        tmdb_id_to_original_name[str(row['tmdb_id'])] = row['original_name']
            
            logger.info(f"成功从本地数据库为 {len(tmdb_id_to_original_name)} 个TMDb ID找到了original_name。")

            # 构建最终待翻译列表
            for actor in actors_to_enrich:
                emby_name = actor['name']
                tmdb_id = actor['tmdb_id']
                original_name = tmdb_id_to_original_name.get(str(tmdb_id))
                
                # 优先使用 original_name，如果没有，则用 emby_name 作为后备
                text_for_translation = original_name if original_name and not utils.contains_chinese(original_name) else emby_name
                
                texts_to_translate.add(text_for_translation)
                # 记录映射关系，以便翻译后能找到对应的 Emby 演员
                original_to_emby_name_map[text_for_translation] = emby_name

        # 对于那些没有 TMDb ID 的演员，直接将他们的 Emby Name 加入翻译列表
        emby_names_with_tmdb_id = {actor['name'] for actor in actors_to_enrich}
        for emby_name in name_to_persons_map.keys():
            if emby_name not in emby_names_with_tmdb_id:
                texts_to_translate.add(emby_name)
                original_to_emby_name_map[emby_name] = emby_name

        # ======================================================================
        # 阶段 3: 分批翻译并并发写回 (逻辑与原版类似，但使用新的数据)
        # ======================================================================
        all_names_list = list(texts_to_translate)
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
            
            progress = int(20 + (i / total_names_to_process) * 80)
            task_manager.update_status_from_thread(
                progress, 
                f"阶段 3/3: 正在翻译批次 {batch_num}/{total_batches} (已成功 {total_updated_count} 个)"
            )
            
            try:
                # 使用 "音译" 模式，因为它对人名更友好
                translation_map = processor.ai_translator.batch_translate(
                    texts=current_batch_names, mode="transliterate"
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

def task_purge_ghost_actors(processor):
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
            logger.error("  -> 任务中止：未在设置中选择任何要处理的媒体库。")
            task_manager.update_status_from_thread(-1, "任务失败：未选择媒体库")
            return

        logger.info(f"  -> 将只扫描 {len(library_ids_to_process)} 个选定媒体库中的演员...")
        task_manager.update_status_from_thread(10, f"  -> 正在从 {len(library_ids_to_process)} 个媒体库中获取所有媒体...")

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
            task_manager.update_status_from_thread(100, "  -> 任务完成：在选定的媒体库中未找到任何媒体项。")
            return

        # 3. 从媒体项中提取所有唯一的演员ID
        task_manager.update_status_from_thread(30, "  -> 正在从媒体项中提取唯一的演员ID...")
        unique_person_ids = set()
        for item in all_media_items:
            for person in item.get("People", []):
                if person_id := person.get("Id"):
                    unique_person_ids.add(person_id)
        
        person_ids_to_fetch = list(unique_person_ids)
        logger.info(f"  -> 在选定媒体库中，共识别出 {len(person_ids_to_fetch)} 位独立演员。")

        if not person_ids_to_fetch:
            task_manager.update_status_from_thread(100, "  -> 任务完成：未在媒体项中找到任何演员。")
            return

        # 4. 分批获取这些演员的完整详情
        task_manager.update_status_from_thread(50, f"  -> 正在分批获取 {len(person_ids_to_fetch)} 位演员的完整详情...")
        all_people_in_scope_details = []
        batch_size = 500
        for i in range(0, len(person_ids_to_fetch), batch_size):
            if processor.is_stop_requested():
                logger.info("  -> 在分批获取演员详情阶段，任务被中止。")
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
        logger.info(f"  -> 详情获取完成：成功获取到 {len(all_people_in_scope_details)} 位演员的完整详情。")

        # 5. 基于完整的详情，筛选出真正的“幽灵”演员
        ghosts_to_delete = [
            p for p in all_people_in_scope_details 
            if not p.get("ProviderIds", {}).get("Tmdb")
        ]
        total_to_delete = len(ghosts_to_delete)

        # ★★★ 新增：核心的筛选结果统计日志 ★★★
        logger.info(f"  -> 筛选完成：在 {len(all_people_in_scope_details)} 位演员中，发现 {total_to_delete} 个没有TMDb ID的幽灵演员。")

        if total_to_delete == 0:
            # ★★★ 优化：更清晰的完成日志 ★★★
            logger.info("  -> 扫描完成，在选定媒体库中未发现需要清理的幽灵演员。")
            task_manager.update_status_from_thread(100, "  -> 扫描完成，未发现无TMDb ID的演员。")
            return
        
        logger.warning(f"  -> 共发现 {total_to_delete} 个幽灵演员，即将开始删除...")
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

                #  如果 Emby 删除成功，则从本地数据库同步删除 
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "DELETE FROM person_identity_map WHERE emby_person_id = %s",
                                (person_id,)
                            )
                            # 记录数据库操作结果
                            if cursor.rowcount > 0:
                                logger.info(f"  -> 同步成功: 已从 person_identity_map 中移除 ID '{person_id}'。")
                            else:
                                logger.info(f"  -> 同步提醒: 在 person_identity_map 中未找到 ID '{person_id}'，无需删除。")
                except Exception as db_exc:
                    logger.error(f"      -> 同步失败: 尝试从 person_identity_map 删除 ID '{person_id}' 时出错: {db_exc}")
            
            time.sleep(0.2)

        final_message = f"清理完成！共找到 {total_to_delete} 个目标，成功删除了 {deleted_count} 个。"
        if processor.is_stop_requested():
            final_message = f"任务已中止。共删除了 {deleted_count} 个演员。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")