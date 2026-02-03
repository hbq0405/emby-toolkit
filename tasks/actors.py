# tasks/actors.py
# 演员相关任务模块

import time
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入需要的底层模块和共享实例
from database.connection import get_db_connection
from database import actor_db
import constants
import handler.emby as emby
import task_manager
import utils
from actor_utils import enrich_all_actor_aliases_task
from handler.actor_sync import UnifiedSyncHandler

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
            logger.error(f"  🚫 任务 '{task_name}' 中止：未在配置中找到 TMDb API Key。")
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
    """
    手动触发对单个演员订阅进行全量作品扫描的任务。
    """
     # --- 步骤 1: 获取演员名，用于日志和前端状态显示 ---
    actor_name_for_log = f"订阅ID {subscription_id}"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT actor_name FROM actor_subscriptions WHERE id = %s", (subscription_id,))
                result = cursor.fetchone()
                if result:
                    actor_name_for_log = result['actor_name']
    except Exception as e:
        logger.warning(f"获取订阅 {subscription_id} 的演员名失败: {e}")

    logger.info(f"--- 开始为演员 '{actor_name_for_log}' 执行手动刷新任务 ---")
    
    try:
        # --- 步骤 2: 从 processor 中获取 ActorSubscriptionProcessor 实例 ---
        # processor 参数实际上是 extensions.actor_subscription_processor_instance
        sub_processor = processor
        if not sub_processor:
            raise RuntimeError("ActorSubscriptionProcessor 实例未初始化。")

        # --- 步骤 3: 从本地数据库（数据中台）一次性加载所有需要的 Emby 媒体信息 ---
        task_manager.update_status_from_thread(10, "正在从本地数据库缓存媒体信息...")
        logger.info("  ➜ 正在从 media_metadata 表一次性获取全量在库媒体及剧集结构数据...")
        
        try:
            (emby_media_map, 
             emby_series_seasons_map, 
             emby_series_name_to_tmdb_id_map) = actor_db.get_all_in_library_media_for_actor_sync()
            logger.info(f"  ➜ 从数据库成功加载 {len(emby_media_map)} 个媒体映射，{len(emby_series_seasons_map)} 个剧集季结构。")
        except Exception as e:
            logger.error(f"  ➜ 手动刷新任务：从 media_metadata 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            task_manager.update_status_from_thread(-1, "错误：读取本地数据库失败。")
            return

        # --- 步骤 4: 调用核心扫描函数，传入所有必需的参数 ---
        # 这和 run_scheduled_task 中的调用逻辑完全一致。
        task_manager.update_status_from_thread(30, f"正在扫描演员 '{actor_name_for_log}' 的作品...")
        
        sub_processor.run_full_scan_for_actor(
            subscription_id=subscription_id,
            emby_media_map=emby_media_map
        )
        
        task_manager.update_status_from_thread(100, "扫描完成。")
        logger.info(f"--- 演员 '{actor_name_for_log}' 的手动刷新任务执行完毕 ---")

    except Exception as e:
        logger.error(f"手动刷新任务 '{actor_name_for_log}' 在执行时失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# --- 演员订阅 ---
def task_process_actor_subscriptions(processor):
    """【新】后台任务：执行所有启用的刷新演员订阅。"""
    processor.run_scheduled_task(update_status_callback=task_manager.update_status_from_thread)

# --- 翻译演员任务 ---
def task_actor_translation(processor):
    """
    【V4.1 - 详细日志版】
    - 增加详细日志：明确打印翻译跳过原因（结果为空/结果相同）以及 Emby API 更新失败的原因。
    """
    task_name = "中文化演员名 (智能版)"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    actor = processor.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE)

    if not actor:
        logger.info("  🚫 AI翻译功能未启用，跳过任务。")
        return
    
    try:
        # ======================================================================
        # 阶段 1: 扫描并聚合所有需要翻译的演员 (智能数据采集)
        # ======================================================================
        task_manager.update_status_from_thread(0, "阶段 1/3: 正在扫描 Emby，收集所有待翻译演员...")
        
        name_to_persons_map = {}
        actors_to_enrich = []

        person_generator = emby.get_all_persons_from_emby(
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
                    if tmdb_id:
                        actors_to_enrich.append({"name": name, "tmdb_id": tmdb_id})
                    
                    if name not in name_to_persons_map:
                        name_to_persons_map[name] = []
                    name_to_persons_map[name].append(person)
            
            total_scanned += len(person_batch)
            task_manager.update_status_from_thread(5, f"阶段 1/3: 已扫描 {total_scanned} 名演员...")

        if not name_to_persons_map:
            logger.info("  ➜ 扫描完成，没有发现需要翻译的演员名。")
            task_manager.update_status_from_thread(100, "任务完成，所有演员名都无需翻译。")
            return

        logger.info(f"  ➜ 扫描完成！共发现 {len(name_to_persons_map)} 个外文名需要翻译。")

        # ======================================================================
        # 阶段 2: 从本地数据库获取 Original Name
        # ======================================================================
        task_manager.update_status_from_thread(10, "阶段 2/3: 正在从本地缓存获取演员原始名...")
        
        original_to_emby_name_map = {}
        texts_to_translate = set()
        
        tmdb_ids_to_query = list(set([int(actor['tmdb_id']) for actor in actors_to_enrich if actor.get('tmdb_id')]))

        if tmdb_ids_to_query:
            tmdb_id_to_original_name = {}
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    query = "SELECT tmdb_id, original_name FROM actor_metadata WHERE tmdb_id = ANY(%s)"
                    cursor.execute(query, (tmdb_ids_to_query,))
                    for row in cursor.fetchall():
                        tmdb_id_to_original_name[str(row['tmdb_id'])] = row['original_name']
            
            logger.trace(f"  ➜ 成功从本地数据库为 {len(tmdb_id_to_original_name)} 个TMDb ID找到了original_name。")

            for actor in actors_to_enrich:
                emby_name = actor['name']
                tmdb_id = actor['tmdb_id']
                original_name = tmdb_id_to_original_name.get(str(tmdb_id))
                
                text_for_translation = original_name if original_name and not utils.contains_chinese(original_name) else emby_name
                
                texts_to_translate.add(text_for_translation)
                original_to_emby_name_map[text_for_translation] = emby_name

        emby_names_with_tmdb_id = {actor['name'] for actor in actors_to_enrich}
        for emby_name in name_to_persons_map.keys():
            if emby_name not in emby_names_with_tmdb_id:
                texts_to_translate.add(emby_name)
                original_to_emby_name_map[emby_name] = emby_name

        # ======================================================================
        # 阶段 3: 分批翻译并并发写回
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
                translation_map = processor.ai_translator.batch_translate(
                    texts=current_batch_names, mode="transliterate"
                )
            except Exception as e_trans:
                logger.error(f"翻译批次 {batch_num} 时发生错误: {e_trans}，将跳过此批次。")
                continue

            if not translation_map:
                logger.warning(f"翻译批次 {batch_num} 未能返回任何结果。")
                continue

            batch_updated_count = 0
            
            # 1. 准备好所有需要更新的任务
            update_tasks = []
            for original_name, translated_name in translation_map.items():
                # --- [新增日志] 详细记录跳过原因 ---
                if not translated_name:
                    logger.warning(f"    - ⚠️ [跳过] 原名: '{original_name}' -> 翻译结果为空")
                    continue
                
                if original_name == translated_name:
                    # 如果翻译结果和原文一样，说明AI认为不需要翻译，或者翻译失败
                    logger.info(f"    - ℹ️ [跳过] 原名: '{original_name}' -> 结果与原文相同 (未变)")
                    continue
                # -----------------------------------

                persons_to_update = name_to_persons_map.get(original_name, [])
                for person in persons_to_update:
                    update_tasks.append((person.get("Id"), translated_name))

            if not update_tasks:
                logger.info(f"  ➜ 批次 {batch_num}: 翻译结果经比对后无有效变更，跳过写入。")
                continue

            logger.info(f"  ➜ 批次 {batch_num}/{total_batches}: 准备并发写入 {len(update_tasks)} 个更新...")
            
            # 2. 使用 ThreadPoolExecutor 执行并发更新
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_task = {
                    executor.submit(
                        emby.update_person_details,
                        person_id=task[0],
                        new_data={"Name": task[1]},
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    ): task for task in update_tasks
                }

                for future in as_completed(future_to_task):
                    if processor.is_stop_requested():
                        break
                    
                    task_info = future_to_task[future] # (person_id, new_name)
                    try:
                        success = future.result()
                        if success:
                            batch_updated_count += 1
                        else:
                            # --- [新增日志] 记录API调用失败 ---
                            logger.warning(f"    - ❌ [更新失败] Emby API 拒绝更新演员 ID: {task_info[0]} -> '{task_info[1]}'")
                    except Exception as exc:
                        logger.error(f"    - ❌ [异常] 更新演员 (ID: {task_info[0]}) 时发生错误: {exc}")

            total_updated_count += batch_updated_count
            
            if batch_updated_count > 0:
                logger.info(f"  ➜ 批次 {batch_num}/{total_batches} 完成，成功更新 {batch_updated_count} 个演员名。")
        
        # ======================================================================
        # 阶段 3: 任务结束
        # ======================================================================
        final_message = f"  ✅ 任务完成！共成功翻译并更新了 {total_updated_count} 个演员名。"
        if processor.is_stop_requested():
            final_message = f"任务已中断。本次运行成功翻译并更新了 {total_updated_count} 个演员名。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行演员翻译任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_merge_duplicate_actors(processor):
    """
    【高危 V2.1 - 修正版】
    - 扫描 Emby 中所有演员，找出拥有相同 TMDb ID 的“分身”演员。
    - 自动计算每个分身演员关联的媒体项列表。
    - 根据“保大删小”原则（保留关联媒体最多的），确定唯一的“主号”。
    - 【核心】在删除“小号”前，将其参演的所有媒体项中的演员替换为“主号”，实现无缝合并。
    - 最后才删除“小号”演员，并【修正】数据库映射关系。
    """
    task_name = "合并分身演员"
    logger.warning(f"--- !!! 开始执行高危任务: '{task_name}' !!! ---")
    
    task_manager.update_status_from_thread(0, "准备开始...")

    try:
        config = processor.config
        library_ids_to_process = config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])

        if not library_ids_to_process:
            logger.error("  🚫 任务中止：未在设置中选择任何要处理的媒体库。")
            task_manager.update_status_from_thread(-1, "任务失败：未选择媒体库")
            return

        # ======================================================================
        # 阶段 1: 扫描媒体库，建立演员到媒体项的映射
        # ======================================================================
        logger.info(f"  ➜ 将扫描 {len(library_ids_to_process)} 个选定媒体库来建立演员-媒体映射...")
        task_manager.update_status_from_thread(5, f"阶段 1/4: 扫描媒体库，建立演员-媒体映射...")

        all_media_items = emby.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            library_ids=library_ids_to_process, media_type_filter="Movie,Series", fields="People"
        )
        if not all_media_items:
            task_manager.update_status_from_thread(100, "任务完成：在选定的媒体库中未找到任何媒体项。")
            return

        actor_media_map = defaultdict(set)
        for item in all_media_items:
            for person in item.get("People", []):
                if person_id := person.get("Id"):
                    actor_media_map[person_id].add(item['Id'])
        
        logger.info(f"  ➜ 演员-媒体映射建立完成，共统计了 {len(actor_media_map)} 位演员的媒体关联。")

        # ======================================================================
        # 阶段 2: 扫描所有演员，并按 TMDb ID 分组
        # ======================================================================
        task_manager.update_status_from_thread(25, "阶段 2/4: 扫描所有演员，按TMDb ID分组...")
        
        tmdb_id_to_persons_map = defaultdict(list)
        person_generator = emby.get_all_persons_from_emby(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            stop_event=processor.get_stop_event(),
            force_full_scan=True
        )

        total_scanned = 0
        for person_batch in person_generator:
            if processor.is_stop_requested():
                logger.info("任务在扫描演员阶段被用户中断。")
                task_manager.update_status_from_thread(100, "任务已中止。")
                return
            
            for person in person_batch:
                if tmdb_id := person.get("ProviderIds", {}).get("Tmdb"):
                    tmdb_id_to_persons_map[tmdb_id].append(person)
            
            total_scanned += len(person_batch)
            task_manager.update_status_from_thread(25, f"阶段 2/4: 已扫描 {total_scanned} 名演员...")

        # ======================================================================
        # 阶段 3: 识别分身演员并制定合并计划
        # ======================================================================
        task_manager.update_status_from_thread(50, "阶段 3/4: 识别分身并制定合并计划...")
        
        duplicate_groups = {k: v for k, v in tmdb_id_to_persons_map.items() if len(v) > 1}
        
        if not duplicate_groups:
            logger.info("扫描完成，没有发现任何拥有相同TMDb ID的分身演员。")
            task_manager.update_status_from_thread(100, "任务完成，未发现分身演员。")
            return

        logger.warning(f"  ➜ 发现 {len(duplicate_groups)} 组共用TMDb ID的分身演员，开始应用“保大删小”策略...")
        
        merge_plan = []
        for tmdb_id, persons in duplicate_groups.items():
            if processor.is_stop_requested(): break

            keeper = None
            max_refs = -1
            
            for person in persons:
                person_id = person['Id']
                ref_count = len(actor_media_map.get(person_id, set()))
                
                if ref_count > max_refs:
                    max_refs = ref_count
                    keeper = person
                elif ref_count == max_refs and keeper and int(person_id) < int(keeper['Id']):
                    keeper = person
            
            if not keeper: keeper = persons[0]

            person_details_log = [f"'{p['Name']}' (ID: {p['Id']}, 作品数: {len(actor_media_map.get(p['Id'], set()))})" for p in persons]
            logger.info(f"  ➜ [TMDb ID: {tmdb_id}] 决策:")
            logger.info(f"     - 分身列表: {', '.join(person_details_log)}")
            logger.info(f"     - ✅ 保留 (主号): '{keeper['Name']}' (ID: {keeper['Id']})")

            for person in persons:
                if person['Id'] != keeper['Id']:
                    # 将TMDb ID也加入计划，以便后续数据库操作
                    merge_plan.append({'keeper': keeper, 'deletee': person, 'tmdb_id': tmdb_id})
                    logger.warning(f"     - ❌ 合并并删除 (小号): '{person['Name']}' (ID: {person['Id']})")

        # ======================================================================
        # 阶段 4: 执行合并与删除
        # ======================================================================
        if processor.is_stop_requested():
            logger.warning("  🚫 任务已中止，未执行任何合并或删除操作。")
            task_manager.update_status_from_thread(100, "任务已中止。")
            return

        total_to_process = len(merge_plan)
        if total_to_process == 0:
            logger.info("所有分身组合并分析完成，无需操作。")
            task_manager.update_status_from_thread(100, "任务完成，无需操作。")
            return

        logger.warning(f"  ➜ 合并计划制定完成，共需处理 {total_to_process} 个“小号”演员。")
        deleted_count = 0
        merged_item_count = 0

        for i, plan in enumerate(merge_plan):
            if processor.is_stop_requested():
                logger.warning("  🚫 合并操作被用户中止。")
                break
            
            keeper = plan['keeper']
            deletee = plan['deletee']
            tmdb_id = plan['tmdb_id']
            
            progress = 60 + int((i / total_to_process) * 40)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_to_process}) 正在合并: {deletee.get('Name')} -> {keeper.get('Name')}")

            media_ids_to_update = actor_media_map.get(deletee['Id'], set())
            all_media_updates_succeeded = True

            if media_ids_to_update:
                logger.info(f"  ➜ 正在将 '{deletee['Name']}' 的 {len(media_ids_to_update)} 个作品转移给 '{keeper['Name']}'...")
                for media_id in media_ids_to_update:
                    item_details = emby.get_emby_item_details(media_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                    if not item_details:
                        logger.error(f"    - 获取媒体项 {media_id} 详情失败，跳过此项的合并。")
                        all_media_updates_succeeded = False
                        continue
                    
                    old_people = item_details.get("People", [])
                    role_from_deletee = "Actor"
                    for p in old_people:
                        if p.get("Id") == deletee['Id']:
                            role_from_deletee = p.get("Role", "Actor")
                            break
                    
                    new_people = [p for p in old_people if p.get("Id") != deletee['Id']]
                    
                    keeper_exists = any(p.get("Id") == keeper['Id'] for p in new_people)
                    if not keeper_exists:
                        new_people.append({
                            "Id": keeper['Id'], "Name": keeper['Name'],
                            "Type": "Actor", "Role": role_from_deletee
                        })
                    
                    update_success = emby.update_emby_item_details(
                        item_id=media_id, new_data={"People": new_people},
                        emby_server_url=processor.emby_url, emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    )

                    if update_success:
                        merged_item_count += 1
                        logger.debug(f"    - ✅ 成功更新媒体项 '{item_details.get('Name')}' 的演员列表。")
                    else:
                        all_media_updates_succeeded = False
                        logger.error(f"    - ❌ 更新媒体项 '{item_details.get('Name')}' 失败！")

            if all_media_updates_succeeded:
                logger.info(f"  ➜ 所有媒体项已成功转移，准备删除“小号”演员 '{deletee['Name']}' (ID: {deletee['Id']})...")
                delete_success = emby.delete_person_custom_api(
                    base_url=processor.emby_url, api_key=processor.emby_api_key, person_id=deletee['Id']
                )
                if delete_success:
                    deleted_count += 1
                    try:
                        with get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                # ★★★ 修正 2/2: 更新映射表，而不是删除 ★★★
                                cursor.execute(
                                    "UPDATE person_identity_map SET emby_person_id = %s WHERE tmdb_person_id = %s",
                                    (keeper['Id'], tmdb_id)
                                )
                                if cursor.rowcount > 0:
                                    logger.info(f"  ➜ 同步成功: 已将数据库中 TMDb ID '{tmdb_id}' 的映射更新为 Emby ID '{keeper['Id']}'。")
                                else:
                                    logger.warning(f"  ➜ 同步提醒: 在 person_identity_map 中未找到 TMDb ID '{tmdb_id}'，无法更新。")
                    except Exception as db_exc:
                        logger.error(f"  ➜ 同步失败: 尝试更新 TMDb ID '{tmdb_id}' 的映射时出错: {db_exc}")
            else:
                logger.error(f"  ➜ 由于媒体项更新失败，演员 '{deletee['Name']}' (ID: {deletee['Id']}) 将被跳过，不予删除，以保证数据安全。")
            
            time.sleep(0.2)
        
        final_message = f"合并完成！共处理 {total_to_process} 个分身，成功合并 {merged_item_count} 个媒体项，并删除了 {deleted_count} 个多余演员。"
        if processor.is_stop_requested():
            final_message = f"任务已中止。本次运行成功合并 {merged_item_count} 个媒体项并删除 {deleted_count} 个分身演员。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_purge_ghost_actors(processor):
    """
    【高危 V2 - 命名修正版】
    - 精准打击在整个Emby服务器范围内，没有任何媒体项关联的“幽灵”演员。
    - 此任务无视用户在设置中选择的媒体库，始终对整个服务器进行操作。
    """
    task_name = "删除幽灵演员" 
    logger.warning(f"--- !!! 开始执行高危任务: '{task_name}' !!! ---")
    logger.warning("  ➜ 此任务将扫描您整个服务器的媒体和演员，以找出并删除任何未被使用的演员条目。")
    
    task_manager.update_status_from_thread(0, "准备开始全局扫描...")

    try:
        # ======================================================================
        # 阶段 1: 全局扫描所有媒体库，获取所有关联的人物ID (白名单)
        # ======================================================================
        task_manager.update_status_from_thread(0, "准备阶段: 正在扫描所有媒体库...")
        
        # 1.1 获取服务器上所有可见的媒体库ID
        all_libraries = emby.get_emby_libraries(processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not all_libraries:
            task_manager.update_status_from_thread(100, "任务中止：无法获取服务器媒体库列表。")
            return
        
        all_library_ids = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['movies', 'tvshows', 'homevideos', 'musicvideos']]
        logger.info(f"  ➜ 将扫描服务器上的 {len(all_library_ids)} 个媒体库...")

        # 1.2 获取所有媒体项
        all_media_items = emby.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            library_ids=all_library_ids, media_type_filter="Movie,Series", fields="People"
        )
        if not all_media_items:
            task_manager.update_status_from_thread(100, "任务完成：服务器中未找到任何媒体项。")
            return

        # 1.3 建立白名单
        whitelist_person_ids = set()
        for item in all_media_items:
            if processor.is_stop_requested():
                logger.info("任务在建立白名单阶段被用户中断。")
                return
            for person in item.get("People", []):
                if person_id := person.get("Id"):
                    whitelist_person_ids.add(person_id)
        
        logger.info(f"  ➜ 白名单建立完成，服务器中共有 {len(whitelist_person_ids)} 位被引用的演员/职员。")

        # ======================================================================
        # 阶段 2: 全局扫描所有 Person 条目，并找出孤儿
        # ======================================================================
        task_manager.update_status_from_thread(0, "准备阶段: 白名单建立完成，正在扫描演员...")
        
        all_person_items = []
        person_generator = emby.get_all_persons_from_emby(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            stop_event=processor.get_stop_event(), force_full_scan=True
        )

        total_scanned = 0
        for person_batch in person_generator:
            if processor.is_stop_requested():
                logger.info("任务在扫描演员阶段被用户中断。")
                return
            all_person_items.extend(person_batch)
            total_scanned += len(person_batch)
            task_manager.update_status_from_thread(0, f"准备阶段: 已扫描 {total_scanned} 名演员...")

        all_person_ids = {p['Id'] for p in all_person_items}
        orphan_person_ids = all_person_ids - whitelist_person_ids
        
        orphans_to_delete = [p for p in all_person_items if p['Id'] in orphan_person_ids]
        total_to_delete = len(orphans_to_delete)

        if total_to_delete == 0:
            logger.info("  ➜ 扫描完成，未发现任何未被引用的“幽灵演员”。")
            task_manager.update_status_from_thread(100, "扫描完成，服务器演员数据很干净！")
            return

        # ======================================================================
        # 阶段 3: 执行删除
        # ======================================================================
        logger.warning(f"  ➜ 筛选完成：...发现 {total_to_delete} 个幽灵演员，即将开始删除...")
        task_manager.update_status_from_thread(0, f"准备阶段: 识别完成，发现 {total_to_delete} 个幽灵演员。")
        deleted_count = 0

        for i, person in enumerate(orphans_to_delete):
            if processor.is_stop_requested():
                logger.warning("  🚫 删除操作被用户中止。")
                break
            
            person_id = person.get("Id")
            person_name = person.get("Name")
            
            progress = int(((i + 1) / total_to_delete) * 100)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_to_delete}) 正在删除幽灵: {person.get('Name')}")

            success = emby.delete_person_custom_api(
                base_url=processor.emby_url, api_key=processor.emby_api_key, person_id=person_id
            )
            
            if success:
                deleted_count += 1
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("DELETE FROM person_identity_map WHERE emby_person_id = %s", (person_id,))
                            if cursor.rowcount > 0:
                                logger.info(f"  ➜ 同步成功: 已从本地数据库移除 ID '{person_id}'。")
                except Exception as db_exc:
                    logger.error(f"  ➜ 同步失败: 尝试从本地数据库删除 ID '{person_id}' 时出错: {db_exc}")
            
            time.sleep(0.2)

        final_message = f"“幽灵演员”清理完成！共找到 {total_to_delete} 个目标，成功删除了 {deleted_count} 个。"
        if processor.is_stop_requested():
            final_message = f"任务已中止。本次运行成功删除了 {deleted_count} 个“幽灵演员”。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_purge_unregistered_actors(processor):
    """
    【高危 V5 - 命名修正版】
    - 清理那些有关联媒体，但没有TMDb ID的“黑户”演员。
    - 此任务只在你选定的媒体库范围内生效。
    """
    task_name = "删除黑户演员" 
    logger.warning(f"--- !!! 开始执行高危任务: '{task_name}' !!! ---")

    try:
        # 1. 读取并验证媒体库配置
        config = processor.config
        library_ids_to_process = config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])

        if not library_ids_to_process:
            logger.error("  🚫 任务中止：未在设置中选择任何要处理的媒体库。")
            task_manager.update_status_from_thread(-1, "任务失败：未选择媒体库")
            return

        logger.info(f"  ➜ 将只扫描 {len(library_ids_to_process)} 个选定媒体库中的演员...")
        task_manager.update_status_from_thread(10, f"  ➜ 正在从 {len(library_ids_to_process)} 个媒体库中获取所有媒体...")

        # 2. 获取指定媒体库中的所有电影和剧集
        all_media_items = emby.get_emby_library_items(
            base_url=processor.emby_url,
            api_key=processor.emby_api_key,
            user_id=processor.emby_user_id,
            library_ids=library_ids_to_process,
            media_type_filter="Movie,Series",
            fields="People"
        )
        if not all_media_items:
            task_manager.update_status_from_thread(100, "  ➜ 任务完成：在选定的媒体库中未找到任何媒体项。")
            return

        # 3. 从媒体项中提取所有唯一的演员ID
        task_manager.update_status_from_thread(30, "  ➜ 正在从媒体项中提取唯一的演员ID...")
        unique_person_ids = set()
        for item in all_media_items:
            for person in item.get("People", []):
                if person_id := person.get("Id"):
                    unique_person_ids.add(person_id)
        
        person_ids_to_fetch = list(unique_person_ids)
        logger.info(f"  ➜ 在选定媒体库中，共识别出 {len(person_ids_to_fetch)} 位独立演员。")

        if not person_ids_to_fetch:
            task_manager.update_status_from_thread(100, "  ➜ 任务完成：未在媒体项中找到任何演员。")
            return

        # 4. 分批获取这些演员的完整详情
        task_manager.update_status_from_thread(50, f"  ➜ 正在分批获取 {len(person_ids_to_fetch)} 位演员的完整详情...")
        all_people_in_scope_details = []
        batch_size = 500
        for i in range(0, len(person_ids_to_fetch), batch_size):
            if processor.is_stop_requested():
                logger.info("  🚫 在分批获取演员详情阶段，任务被中止。")
                break
            
            batch_ids = person_ids_to_fetch[i:i + batch_size]
            logger.debug(f"  ➜ 正在获取批次 {i//batch_size + 1} 的演员详情 ({len(batch_ids)} 个)...")

            person_details_batch = emby.get_emby_items_by_id(
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                user_id=processor.emby_user_id,
                item_ids=batch_ids,
                fields="ProviderIds,Name"
            )
            if person_details_batch:
                all_people_in_scope_details.extend(person_details_batch)

        if processor.is_stop_requested():
            logger.warning("  🚫 任务已中止。")
            task_manager.update_status_from_thread(100, "任务已中止。")
            return
        
        # ★★★ 新增：详细的获取结果统计日志 ★★★
        logger.info(f"  ➜ 详情获取完成：成功获取到 {len(all_people_in_scope_details)} 位演员的完整详情。")

        # 5. 基于完整的详情，筛选出真正的“幽灵”演员
        ghosts_to_delete = [
            p for p in all_people_in_scope_details 
            if not p.get("ProviderIds", {}).get("Tmdb")
        ]
        total_to_delete = len(ghosts_to_delete)

        # ★★★ 新增：核心的筛选结果统计日志 ★★★
        logger.info(f"  ➜ 筛选完成：在 {len(all_people_in_scope_details)} 位演员中，发现 {total_to_delete} 个没有TMDb ID的“黑户演员”。")

        if total_to_delete == 0:
            # ★★★ 优化：更清晰的完成日志 ★★★
            logger.info("  ➜ 扫描完成，在选定媒体库中未发现需要清理的“黑户演员”。")
            task_manager.update_status_from_thread(100, "  ➜ 扫描完成，未发现无TMDb ID的演员。")
            return
        
        logger.warning(f"  ➜ 共发现 {total_to_delete} 个“黑户演员”，即将开始删除...")
        deleted_count = 0

        # 6. 执行删除
        for i, person in enumerate(ghosts_to_delete):
            if processor.is_stop_requested():
                logger.warning("  🚫 任务被用户中止。")
                break
            
            person_id = person.get("Id")
            person_name = person.get("Name")
            
            progress = 60 + int((i / total_to_delete) * 40)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_to_delete}) 正在删除: {person_name}")

            success = emby.delete_person_custom_api(
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
                                logger.info(f"  ➜ 同步成功: 已从 person_identity_map 中移除 ID '{person_id}'。")
                            else:
                                logger.info(f"  ➜ 同步提醒: 在 person_identity_map 中未找到 ID '{person_id}'，无需删除。")
                except Exception as db_exc:
                    logger.error(f"      ➜ 同步失败: 尝试从 person_identity_map 删除 ID '{person_id}' 时出错: {db_exc}")
            
            time.sleep(0.2)

        final_message = f"清理完成！共找到 {total_to_delete} 个目标，成功删除了 {deleted_count} 个。"
        if processor.is_stop_requested():
            final_message = f"任务已中止。共删除了 {deleted_count} 个“黑户演员”。"
        
        logger.info(final_message)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")