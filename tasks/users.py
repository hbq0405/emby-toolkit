# tasks/users.py
# 用户数据同步与管理任务模块

import time
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入需要的底层模块和共享实例
import handler.emby as emby
import task_manager
from database import connection, user_db
from extensions import SYSTEM_UPDATE_MARKERS, SYSTEM_UPDATE_LOCK

logger = logging.getLogger(__name__)

# ★★★ 用户数据全量同步任务 ★★★
def task_sync_all_user_data(processor):
    """
    【V2 - 双向同步版】用户数据全量同步任务
    - 新增逻辑：在同步开始时，清理掉本地数据库中存在、但 Emby 服务器上已不存在的用户。
    """
    task_name = "同步用户数据"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户...")
        emby_url = processor.emby_url
        emby_key = processor.emby_api_key
        
        # 步骤 1: 从 Emby 获取当前所有用户的权威列表
        all_users_basic = emby.get_all_emby_users_from_server(emby_url, emby_key)
        if all_users_basic is None: # API 调用失败
            task_manager.update_status_from_thread(-1, "任务失败：无法从Emby获取用户列表。")
            return
        
        # --- 【新增开始】并发获取完整详情（含 Policy） ---
        task_manager.update_status_from_thread(2, "正在获取用户详细权限策略...")
        all_users_full = []
        
        def fetch_user_detail(u):
            # 调用 emby.get_user_details 获取包含 Policy 的完整对象
            return emby.get_user_details(u['Id'], emby_url, emby_key)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_user_detail, u) for u in all_users_basic]
            for future in as_completed(futures):
                try:
                    user_detail = future.result()
                    if user_detail:
                        all_users_full.append(user_detail)
                except Exception:
                    pass

        # 步骤 2: ★★★ 新增：执行清理逻辑 ★★★
        task_manager.update_status_from_thread(5, "正在比对本地与Emby用户差异...")
        
        # a. 获取 Emby 上所有用户的 ID 集合
        emby_user_ids = {user['Id'] for user in all_users_basic}
        
        # b. 获取本地数据库中所有用户的 ID 集合
        local_user_ids = user_db.get_all_local_emby_user_ids()
        
        # c. 计算出需要删除的 ID (存在于本地，但不存在于 Emby)
        ids_to_delete = list(local_user_ids - emby_user_ids)
        
        if ids_to_delete:
            logger.warning(f"  ➜ 发现 {len(ids_to_delete)} 个用户已在Emby中被删除，将从本地数据库清理...")
            task_manager.update_status_from_thread(8, f"正在清理 {len(ids_to_delete)} 个陈旧用户...")
            user_db.delete_emby_users_by_ids(ids_to_delete)
        else:
            logger.info("  ➜ 本地用户与Emby用户一致，无需清理。")

        # 步骤 3: 更新或插入最新的用户信息到本地缓存 (此逻辑保持不变)
        if not all_users_basic:
            task_manager.update_status_from_thread(100, "任务完成：Emby中没有任何用户。")
            return
        
        # 3.1 同步基础信息 (ID, Name, IsAdmin...)
        user_db.upsert_emby_users_batch(all_users_basic)

        # 3.2 ★★★ 新增：同步扩展信息 (Registration Date, 确保有记录) ★★★
        task_manager.update_status_from_thread(8, "正在同步用户注册时间与扩展状态...")
        user_db.upsert_emby_users_extended_batch_sync(all_users_basic)
        
        # 步骤 4: 循环同步每个用户的媒体播放状态
        total_users = len(all_users_basic)
        logger.info(f"  ➜ 共找到 {total_users} 个Emby用户，将逐一同步其数据...")

        for i, user in enumerate(all_users_basic):
            user_id = user.get('Id')
            user_name = user.get('Name')
            if not user_id: continue
            if processor.is_stop_requested(): break

            progress = 10 + int((i / total_users) * 90)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_users}) 正在同步用户: {user_name}")

            user_items_with_data = emby.get_all_user_view_data(user_id, emby_url, emby_key)
            if not user_items_with_data:
                continue
            
            final_data_map = {}
            for item in user_items_with_data:
                item_type = item.get('Type')
                item_id = item.get('Id')
                # 聚合逻辑：电影用自身ID，分集聚合到剧集ID
                target_id = item_id if item_type in ['Movie', 'Series'] else item.get('SeriesId')
                if not target_id: continue

                new_user_data = item.get('UserData', {})
                
                # =========================================================
                # ★★★ 核心修复：播放次数兜底逻辑 ★★★
                # =========================================================
                raw_play_count = new_user_data.get('PlayCount', 0)
                is_played = new_user_data.get('Played', False)
                
                # 如果 Emby 说“已播放”但次数是 0，强制修正为 1
                if is_played and raw_play_count == 0:
                    current_play_count = 1
                else:
                    current_play_count = raw_play_count
                # =========================================================

                if target_id not in final_data_map:
                    # --- 初始化新条目 ---
                    final_data_map[target_id] = item
                    if item_type == 'Episode':
                        final_data_map[target_id]['Id'] = target_id
                    
                    # 确保 UserData 字典存在
                    if 'UserData' not in final_data_map[target_id]:
                        final_data_map[target_id]['UserData'] = {}
                    
                    # 初始化 PlayCount
                    final_data_map[target_id]['UserData']['PlayCount'] = current_play_count
                    
                    # 初始化 Played 状态 (如果是电影，直接用自己的；如果是分集，稍后聚合)
                    if 'Played' not in final_data_map[target_id]['UserData']:
                         final_data_map[target_id]['UserData']['Played'] = is_played

                else:
                    # --- 聚合到已存在的条目 (主要是剧集) ---
                    existing_item = final_data_map[target_id]
                    existing_ud = existing_item.get('UserData', {})
                    
                    # 1. 更新播放进度 (取最新的)
                    if 'PlaybackPositionTicks' in new_user_data:
                        existing_ud['PlaybackPositionTicks'] = new_user_data['PlaybackPositionTicks']
                    
                    # 2. 更新已播放状态 (逻辑：只要有一集是 Played，剧集记录就可能被更新，具体看 Emby 返回的 Series 自身状态，这里做累加辅助)
                    if 'Played' in new_user_data:
                        # 如果当前分集是已播放，或者之前的记录已经是已播放，则保持 true
                        existing_ud['Played'] = existing_ud.get('Played', False) or is_played
                    
                    # 3. ★★★ 累加播放次数 ★★★
                    if 'PlayCount' not in existing_ud:
                        existing_ud['PlayCount'] = 0
                    
                    existing_ud['PlayCount'] += current_play_count
            
            # 将 map 转换为 list 准备入库
            final_data_to_upsert = list(final_data_map.values())
            
            user_db.upsert_user_media_data_batch(user_id, final_data_to_upsert)
            
            logger.info(f"  ➜ 成功为用户 '{user_name}' 同步了 {len(final_data_to_upsert)} 条媒体状态。")

        final_message = f"任务完成！已成功为 {total_users} 个用户同步数据。"
        if processor.is_stop_requested(): final_message = "任务已中断。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 检查并禁用过期用户 ★★★
def task_check_expired_users(processor):
    """
    【核心任务】检查并禁用所有已过期的用户。
    """
    task_name = "检查并禁用过期用户"
    logger.info(f"  ➜ 开始执行 [{task_name}] 任务...")
    task_manager.update_status_from_thread(0, "正在检查过期用户...")
    
    expired_users = []
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 查询所有状态为'active'，且到期时间早于当前时间的用户，并获取用户名用于日志
            cursor.execute(
                """
                SELECT uex.emby_user_id, u.name
                FROM emby_users_extended uex
                LEFT JOIN emby_users u ON uex.emby_user_id = u.id
                WHERE uex.status = 'active' AND uex.expiration_date IS NOT NULL AND uex.expiration_date < NOW()
                """
            )
            expired_users = [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 检查过期用户时，查询数据库失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败：查询数据库出错")
        return

    if not expired_users:
        logger.info("  ➜ 本次检查未发现已过期的用户。")
        task_manager.update_status_from_thread(100, "任务完成：未发现过期用户")
        return

    total_to_disable = len(expired_users)
    logger.warning(f"  ➜ 检测到 {total_to_disable} 个已过期的用户，准备开始禁用...")
    task_manager.update_status_from_thread(10, f"  ➜ 发现 {total_to_disable} 个过期用户，正在处理...")
    
    config = processor.config
    emby_url = config.get("emby_server_url")
    api_key = config.get("emby_api_key")

    successful_disables = 0
    for i, user_info in enumerate(expired_users):
        if processor.is_stop_requested():
            logger.warning("  🚫 任务被用户中止。")
            break

        user_id = user_info['emby_user_id']
        user_name = user_info.get('name') or user_id # 如果join失败，用ID作为备用名
        
        progress = 10 + int((i / total_to_disable) * 90)
        task_manager.update_status_from_thread(progress, f"  ➜ ({i+1}/{total_to_disable}) 正在禁用: {user_name}")

        try:
            # 1. 调用 Emby API 禁用用户
            success = emby.set_user_disabled_status(
                user_id, 
                disable=True, 
                base_url=emby_url, 
                api_key=api_key
            )

            if success:
                logger.info(f"  ➜ Emby 用户 '{user_name}' (ID: {user_id}) 禁用成功。正在更新本地数据库状态...")
                # 2. 如果 Emby 禁用成功，则更新我们自己数据库中的状态为 'expired'
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE emby_users_extended SET status = 'expired' WHERE emby_user_id = %s",
                        (user_id,)
                    )
                    conn.commit()
                logger.info(f"  ➜ 本地数据库状态已更新为 'expired'。")
                successful_disables += 1
            else:
                logger.error(f"  ➜ 禁用 Emby 用户 '{user_name}' (ID: {user_id}) 失败，请检查 Emby API 连接。")

        except Exception as e:
            logger.error(f"  ➜ 处理过期用户 '{user_name}' (ID: {user_id}) 时发生未知错误: {e}", exc_info=True)
            continue # 即使单个用户处理失败，也继续处理下一个

    final_message = f"  ➜ 任务完成。共成功禁用 {successful_disables}/{total_to_disable} 个过期用户。"
    if processor.is_stop_requested():
        final_message = f"  🚫 任务已中止。本次运行成功禁用了 {successful_disables} 个用户。"
    
    logger.info(f">>> [{task_name}] {final_message}")
    task_manager.update_status_from_thread(100, final_message)

def task_auto_sync_template_on_policy_change(processor, updated_user_id: str):
    """
    当源用户的权限变更时，自动同步关联的模板及其所有用户。
    """
    user_name_for_log = updated_user_id 
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM emby_users WHERE id = %s", (updated_user_id,))
            user_record = cursor.fetchone()
            if user_record: user_name_for_log = user_record['name']
    except Exception:
        pass 

    task_name = f"自动同步权限 (源用户: '{user_name_for_log}')"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT id FROM user_templates WHERE source_emby_user_id = %s",
                (updated_user_id,)
            )
            templates_to_sync = cursor.fetchall()
            
            if not templates_to_sync:
                logger.debug(f"  ➜ 用户 '{user_name_for_log}' 的权限已更新，但他不是任何模板的源用户，无需同步。")
                return

            total_templates = len(templates_to_sync)
            logger.warning(f"  ➜ 检测到 {total_templates} 个模板使用用户 '{user_name_for_log}' 作为源，将开始自动同步...")

            config = processor.config
            
            for i, template_row in enumerate(templates_to_sync):
                template_id = template_row['id']
                
                cursor.execute("SELECT name FROM user_templates WHERE id = %s", (template_id,))
                template_name = cursor.fetchone()['name']
                logger.info(f"  ➜ ({i+1}/{total_templates}) 正在同步模板 '{template_name}'...")

                user_details = emby.get_user_details(
                    updated_user_id, config.get("emby_server_url"), config.get("emby_api_key")
                )
                if not user_details or 'Policy' not in user_details:
                    logger.error(f"  ➜ 无法获取源用户的最新权限策略，跳过模板 '{template_name}'。")
                    continue
                
                new_policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)
                new_policy_dict = user_details['Policy']

                new_config_json = None
                new_config_dict = None
                cursor.execute("SELECT emby_configuration_json IS NOT NULL as has_config FROM user_templates WHERE id = %s", (template_id,))
                if cursor.fetchone()['has_config'] and 'Configuration' in user_details:
                    new_config_json = json.dumps(user_details['Configuration'], ensure_ascii=False)
                    new_config_dict = user_details['Configuration']

                cursor.execute(
                    "UPDATE user_templates SET emby_policy_json = %s, emby_configuration_json = %s WHERE id = %s",
                    (new_policy_json, new_config_json, template_id)
                )

                cursor.execute(
                    "SELECT u.id, u.name FROM emby_users_extended uex JOIN emby_users u ON uex.emby_user_id = u.id WHERE uex.template_id = %s",
                    (template_id,)
                )
                users_to_update = cursor.fetchall()
                
                if users_to_update:
                    logger.info(f"  ➜ 正在将新权限推送到 {len(users_to_update)} 个关联用户...")
                    for user in users_to_update:
                        user_id_to_push = user['id']
                        user_name_to_push = user['name']

                        if user_id_to_push == updated_user_id:
                            logger.warning(f"  ➜ 跳过用户 '{user_name_to_push}'，因为他就是本次同步的触发源，以避免无限循环。")
                            continue

                        logger.info(f"    ├─ 正在将 '{template_name}' 的新策略应用到用户 '{user_name_to_push}'...")
                        
                        # 记录下我们即将要更新这个用户，时间精确到当前
                        with SYSTEM_UPDATE_LOCK:
                            SYSTEM_UPDATE_MARKERS[user_id_to_push] = time.time()
                        
                        # ★★★ 核心修复开始 ★★★
                        # 1. 复制一份源策略，因为我们要针对每个用户修改 IsDisabled 字段
                        # 如果不复制，修改字典会影响到循环中的后续用户
                        policy_to_apply = new_policy_dict.copy()

                        # 2. 获取目标用户当前的实时状态，确保不覆盖 'IsDisabled' (禁用) 状态
                        try:
                            target_current_info = emby.get_user_details(
                                user_id_to_push, 
                                config.get("emby_server_url"), 
                                config.get("emby_api_key")
                            )
                            if target_current_info and 'Policy' in target_current_info:
                                # 获取子用户当前的禁用状态
                                current_disabled_state = target_current_info['Policy'].get('IsDisabled', False)
                                # 将其覆盖到我们要推送的策略中
                                policy_to_apply['IsDisabled'] = current_disabled_state
                                logger.debug(f"    │  保留用户 '{user_name_to_push}' 的禁用状态: {current_disabled_state}")
                        except Exception as e:
                            logger.warning(f"    │  ➜ 获取用户 '{user_name_to_push}' 实时状态失败，可能导致禁用状态重置: {e}")

                        # 3. 使用修改后的策略(policy_to_apply)进行推送
                        emby.force_set_user_policy(
                            user_id_to_push, 
                            policy_to_apply, 
                            config.get("emby_server_url"), 
                            config.get("emby_api_key")
                        )
                        time.sleep(0.2)
            
            conn.commit()
            logger.trace(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)