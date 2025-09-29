# watchlist_processor.py

import time
import json
import os
import concurrent.futures
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
import threading

# 导入我们需要的辅助模块
from database import connection
from database import watchlist_db
import moviepilot_handler
import constants
import tmdb_handler
import emby_handler
import logging

logger = logging.getLogger(__name__)
# ✨✨✨ Tmdb状态翻译字典 ✨✨✨
TMDB_STATUS_TRANSLATION = {
    "Ended": "已完结",
    "Canceled": "已取消",
    "Returning Series": "连载中",
    "In Production": "制作中",
    "Planned": "计划中"
}
# ★★★ 内部状态翻译字典，用于日志显示 ★★★
INTERNAL_STATUS_TRANSLATION = {
    'Watching': '追剧中',
    'Paused': '已暂停',
    'Completed': '已完结'
}
# ★★★ 定义状态常量，便于维护 ★★★
STATUS_WATCHING = 'Watching'
STATUS_PAUSED = 'Paused'
STATUS_COMPLETED = 'Completed'
def translate_status(status: str) -> str:
    """一个简单的辅助函数，用于翻译状态，如果找不到翻译则返回原文。"""
    return TMDB_STATUS_TRANSLATION.get(status, status)
def translate_internal_status(status: str) -> str:
    """★★★ 新增：一个辅助函数，用于翻译内部状态，用于日志显示 ★★★"""
    return INTERNAL_STATUS_TRANSLATION.get(status, status)
class WatchlistProcessor:
    """
    【V12 - 精准强制完结版】
    实现基于待播日期的三态(Watching, Paused, Completed)自动转换，
    并包含一个独立的、用于低频检查已完结剧集“复活”的方法。
    新增对 `force_ended` 标志的支持。
    """
    def __init__(self, config: Dict[str, Any]):
        if not isinstance(config, dict):
            raise TypeError(f"配置参数(config)必须是一个字典，但收到了 {type(config).__name__} 类型。")
        self.config = config
        self.tmdb_api_key = self.config.get("tmdb_api_key", "")
        self.emby_url = self.config.get("emby_server_url")
        self.emby_api_key = self.config.get("emby_api_key")
        self.emby_user_id = self.config.get("emby_user_id")
        self.local_data_path = self.config.get("local_data_path", "")
        self._stop_event = threading.Event()
        self.progress_callback = None
        logger.trace("WatchlistProcessor 初始化完成。")

    # --- 线程控制 ---
    def signal_stop(self): self._stop_event.set()
    def clear_stop_signal(self): self._stop_event.clear()
    def is_stop_requested(self) -> bool: return self._stop_event.is_set()
    def close(self): logger.trace("WatchlistProcessor closed.")

    # --- 数据库和文件辅助方法 ---
    def _read_local_json(self, file_path: str) -> Optional[Dict[str, Any]]:
        if not os.path.exists(file_path): return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f: return json.load(f)
        except Exception as e:
            logger.error(f"读取本地JSON文件失败: {file_path}, 错误: {e}")
            return None

    def _update_watchlist_entry(self, item_id: str, item_name: str, updates: Dict[str, Any]):
        """【V4 - 最终修复版】统一更新追剧列表中的一个条目。"""
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # ★★★ 核心修正：使用 datetime.utcnow() 生成不带时区的UTC时间 ★★★
                    # 这能最大限度地兼容各种数据库时区设置，避免类型冲突
                    current_time = datetime.utcnow()
                    updates['last_checked_at'] = current_time
                    
                    set_clauses = [f"{key} = %s" for key in updates.keys()]
                    values = list(updates.values())
                    values.append(item_id)
                    
                    sql = f"UPDATE watchlist SET {', '.join(set_clauses)} WHERE item_id = %s"
                    
                    cursor.execute(sql, tuple(values))
                conn.commit()
                logger.info(f"  -> 成功更新数据库中 '{item_name}' 的追剧信息。")
        except Exception as e:
            logger.error(f"  更新 '{item_name}' 的追剧信息时数据库出错: {e}", exc_info=True)
    # --- 自动添加追剧列表的方法 ---
    def add_series_to_watchlist(self, item_details: Dict[str, Any]):
        """检查剧集状态，如果是“在播中”，则添加到追剧列表。"""
        if item_details.get("Type") != "Series": return
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name")
        item_id = item_details.get("Id")
        if not tmdb_id: return

        if not self.tmdb_api_key: return
            
        tmdb_details = tmdb_handler.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not tmdb_details: return

        tmdb_status = tmdb_details.get("status")
        if tmdb_status in ["Returning Series", "In Production", "Planned"]:
            try:
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # ★★★ 核心修复: 改为 PostgreSQL 语法 ★★★
                        cursor.execute("""
                            INSERT INTO watchlist (item_id, tmdb_id, item_name, item_type, status)
                            VALUES (%s, %s, %s, %s, 'Watching')
                            ON CONFLICT (item_id) DO NOTHING
                        """, (item_id, tmdb_id, item_name, "Series"))
                        
                        if cursor.rowcount > 0:
                            logger.info(f"  -> 剧集 '{item_name}' (TMDb状态: {translate_status(tmdb_status)}) 已自动加入追剧列表。")
                    conn.commit()
            except Exception as e:
                logger.error(f"自动添加剧集 '{item_name}' 到追剧列表时发生数据库错误: {e}", exc_info=True)

    # --- 核心任务启动器 ---
    def run_regular_processing_task_concurrent(self, progress_callback: callable, item_id: Optional[str] = None):
        """【高铁版 - 并发追剧更新】处理所有活跃的剧集。"""
        self.progress_callback = progress_callback
        task_name = "并发追剧更新"
        if item_id: task_name = f"单项追剧更新 (ID: {item_id})"
        
        self.progress_callback(0, "准备检查待更新剧集...")
        try:
            today_str = datetime.now(timezone.utc).date().isoformat()
            active_series = self._get_series_to_process(
                f"WHERE status = '{STATUS_WATCHING}' OR (status = '{STATUS_PAUSED}' AND paused_until <= '{today_str}')",
                item_id
            )
            # --- 新增：检测 Emby 中已删除的剧集并从追剧列表移除 ---
            if not item_id: # 只在全量处理时执行此检查
                self.progress_callback(0, "正在检测 Emby 中已删除的剧集...")
                
                # 1. 获取 Emby 媒体库中所有剧集的 ID
                emby_series_ids = set()
                try:
                    all_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id)
                    if all_libraries:
                        library_ids_to_scan = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['tvshows', 'mixed']]
                        
                        # 使用并发获取所有剧集
                        all_emby_series_items = []
                        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                            future_to_library = {
                                executor.submit(emby_handler.get_emby_library_items, 
                                                self.emby_url, self.emby_api_key, "Series", self.emby_user_id, [lib_id]): lib_id
                                for lib_id in library_ids_to_scan
                            }
                            for future in concurrent.futures.as_completed(future_to_library):
                                try:
                                    result = future.result()
                                    if result:
                                        all_emby_series_items.extend(result)
                                except Exception as exc:
                                    logger.error(f"从媒体库 {future_to_library[future]} 获取剧集时发生异常: {exc}")
                        
                        emby_series_ids = {item['Id'] for item in all_emby_series_items if item.get('Id')}
                        logger.info(f"已从 Emby 获取到 {len(emby_series_ids)} 个剧集ID。")
                    else:
                        logger.warning("未能从 Emby 获取到任何媒体库，跳过已删除剧集检测。")
                except Exception as e:
                    logger.error(f"获取 Emby 剧集列表时发生错误: {e}", exc_info=True)
                    # 即使出错也继续，避免阻塞主任务
                
                # 2. 获取当前追剧列表中的所有剧集 ID
                watchlist_series_ids = set()
                try:
                    with connection.get_db_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT item_id FROM watchlist WHERE item_type = 'Series'")
                        watchlist_series_ids = {row['item_id'] for row in cursor.fetchall()}
                    logger.info(f"追剧列表中有 {len(watchlist_series_ids)} 个剧集ID。")
                except Exception as e:
                    logger.error(f"获取追剧列表剧集ID时发生数据库错误: {e}", exc_info=True)
                    # 即使出错也继续
                
                # 3. 比较并找出已删除的剧集
                deleted_series_ids = watchlist_series_ids - emby_series_ids
                if deleted_series_ids:
                    logger.warning(f"检测到 {len(deleted_series_ids)} 部剧集已从 Emby 删除，将从追剧列表移除。")
                    for deleted_id in deleted_series_ids:
                        watchlist_db.remove_item_from_watchlist(item_id=deleted_id)
                else:
                    logger.info("未检测到 Emby 中有剧集被删除。")
            # --- 新增逻辑结束 ---

            today_str = datetime.now(timezone.utc).date().isoformat()
            active_series = self._get_series_to_process(
                f"WHERE status = '{STATUS_WATCHING}' OR (status = '{STATUS_PAUSED}' AND paused_until <= '{today_str}')",
                item_id
            )
            total = len(active_series)
            if total == 0:
                self.progress_callback(100, "没有需要立即处理的剧集。")
                return

            self.progress_callback(5, f"开始并发处理 {total} 部剧集 (5个并发)...")
            
            processed_count = 0
            # 使用线程锁来安全地更新共享变量 processed_count
            lock = threading.Lock()

            def worker_process_series(series: dict):
                """
                线程工作单元：处理单部剧集。
                这个函数将在独立的线程中被执行。
                """
                # 检查任务是否在开始处理前就被取消了
                if self.is_stop_requested():
                    return "任务已停止"
                
                try:
                    # ★ 核心耗时操作在这里
                    self._process_one_series(series)
                    return "处理成功"
                except Exception as e:
                    logger.error(f"处理剧集 {series.get('item_name')} (ID: {series.get('item_id')}) 时发生错误: {e}", exc_info=False)
                    return f"处理失败: {e}"

            # ★★★ 核心改造：使用5个并发的线程池 ★★★
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 创建一个 future 到 series 的映射，方便后续获取信息
                future_to_series = {executor.submit(worker_process_series, series): series for series in active_series}
                
                for future in concurrent.futures.as_completed(future_to_series):
                    if self.is_stop_requested():
                        # 如果在处理过程中请求停止，我们可以尝试取消未开始的任务
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

                    series_info = future_to_series[future]
                    try:
                        # 获取线程执行的结果（成功或失败信息）
                        result = future.result()
                        logger.trace(f"'{series_info['item_name']}' - {result}")
                    except Exception as exc:
                        logger.error(f"任务 '{series_info['item_name']}' 执行时产生未捕获的异常: {exc}")

                    # 使用锁来安全地更新进度计数器
                    with lock:
                        processed_count += 1
                    
                    # 实时计算并回调进度
                    progress = 5 + int((processed_count / total) * 95)
                    self.progress_callback(progress, f"进度: {processed_count}/{total} - {series_info['item_name'][:15]}...")

            if not self.is_stop_requested():
                # 根据是全量刷新还是单项刷新，显示不同的过渡消息
                if not item_id:
                    self.progress_callback(100, "常规追剧检查完成，即将开始洗版检查...")
                else:
                    self.progress_callback(100, f"项目 {item_id} 常规检查完成，即将为其单独检查洗版...")
                
                time.sleep(2) # 给用户一点时间看消息

                # 调用我们新的、可复用的洗版检查函数
                # 如果是单项刷新，把 item_id 也传过去
                self._run_wash_plate_check_logic(progress_callback=self.progress_callback, item_id=item_id)
            else:
                # 如果任务被中止，直接结束
                self.progress_callback(100, "任务已停止。")

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    # ★★★ 专门用于“复活检查”的任务方法 ★★★
    def run_revival_check_task(self, progress_callback: callable):
        """【低频任务】检查所有已完结剧集是否“复活”。"""
        self.progress_callback = progress_callback
        task_name = "已完结剧集复活检查"
        self.progress_callback(0, "准备开始复活检查...")
        try:
            # 【修改】查询条件不变，依然是检查所有已完结的剧集
            completed_series = self._get_series_to_process(f"WHERE status = '{STATUS_COMPLETED}'")
            total = len(completed_series)
            if not completed_series:
                self.progress_callback(100, "没有已完结的剧集需要检查。")
                return

            logger.info(f"开始低频检查 {total} 部已完结剧集是否复活...")
            self.progress_callback(10, f"发现 {total} 部已完结剧集，开始检查...")
            revived_count = 0

            for i, series in enumerate(completed_series):
                if self.is_stop_requested(): break
                progress = 10 + int(((i + 1) / total) * 90)
                self.progress_callback(progress, f"检查中: {series['item_name'][:20]}... ({i+1}/{total})")

                tmdb_details = tmdb_handler.get_tv_details(series['tmdb_id'], self.tmdb_api_key)
                if not tmdb_details: continue

                new_tmdb_status = tmdb_details.get('status')
                # 基础条件：TMDb状态不再是“已完结”或“已取消”
                is_status_revived = new_tmdb_status not in ["Ended", "Canceled"]

                # ▼▼▼ 核心修正：增加新季检查，防止误判 ▼▼▼
                has_new_season = False
                if is_status_revived:
                    try:
                        # 从新获取的TMDb详情中拿到总季数
                        new_total_seasons = tmdb_details.get('number_of_seasons', 0)
                        
                        # 从数据库中存储的“最后播出集信息”里解析出旧的季号
                        last_episode_info = series.get('last_episode_to_air_json')
                        old_season_number = 0
                        if last_episode_info and isinstance(last_episode_info, dict):
                            old_season_number = last_episode_info.get('season_number', 0)
                        
                        # 如果新的总季数 > 旧的最后一集季号，说明出了新的一季
                        if new_total_seasons > old_season_number:
                            has_new_season = True
                            logger.info(f"  -> 检测到《{series['item_name']}》有新内容：TMDb总季数 ({new_total_seasons}) > 上次记录的最终季号 ({old_season_number})。")
                        else:
                            logger.info(f"  -> 《{series['item_name']}》TMDb状态为'{new_tmdb_status}'，但未发现新季（TMDb总季数 {new_total_seasons}，记录的最终季 {old_season_number}），不作复活处理。")

                    except Exception as e:
                        logger.error(f"  -> 解析《{series['item_name']}》的新旧季数时出错: {e}", exc_info=True)
                        has_new_season = False # 出错则保守处理，不认为有新季

                # 最终复活判断：状态符合 且 必须有新季
                if is_status_revived and has_new_season:
                    logger.warning(f"检测到剧集 '{series['item_name']}' 已复活！TMDb状态从 '{series.get('tmdb_status')}' 变为 '{new_tmdb_status}'，并发布了新季。")
                    revived_count += 1
                    
                    # 准备更新的数据
                    updates_to_db = {
                        "status": STATUS_WATCHING,
                        "paused_until": None,
                        "tmdb_status": new_tmdb_status,
                        # 【关键】一旦因新一季而复活，就必须重置 force_ended 标志，让它恢复正常追剧逻辑
                        "force_ended": False 
                    }
                    self._update_watchlist_entry(series['item_id'], series['item_name'], updates_to_db)
                
                time.sleep(2) # 保持API调用间隔
            
            final_message = f"复活检查完成。共发现 {revived_count} 部剧集回归。"
            self.progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    # ★★★ 已完结剧集缺集洗版检查 ★★★
    def _run_wash_plate_check_logic(self, progress_callback: callable, item_id: Optional[str] = None):
        """
        【V11 - 最终悖论修复版】
        采用全新的三阶段查询，在不破坏核心状态逻辑的前提下，精确查找所有需要洗版的剧集。
        """
        task_name = "洗版缺集的季"
        
        if not self.config.get(constants.CONFIG_OPTION_RESUBSCRIBE_COMPLETED_ON_MISSING):
            logger.info(f"'{task_name}' 功能未启用，跳过。")
            if progress_callback: progress_callback(100, "所有流程已完成（洗版功能未启用）。")
            return

        logger.trace(f"  -> 后台任务 '{task_name}' 开始执行")
        if progress_callback: progress_callback(0, "正在查找需要洗版的剧集...")

        try:
            series_to_check = []
            if item_id:
                series_to_check = self._get_series_to_process("", item_id=item_id)
            else:
                # ★★★ 核心逻辑：三阶段查询，捕获所有目标 ★★★

                # 阶段一：捕获“TMDb已完结，但因缺集而卡在追剧中”的剧集 (例如《长相思》)
                stuck_series = self._get_series_to_process(
                    f"""
                    WHERE status IN ('{STATUS_WATCHING}', '{STATUS_PAUSED}')
                      AND tmdb_status IN ('Ended', 'Canceled')
                      AND jsonb_typeof(missing_info_json) IN ('object', 'array')
                    """
                )
                logger.info(f"  -> 阶段1：发现 {len(stuck_series)} 部 TMDb已完结但卡在追剧中的剧集。")

                # 阶段二：捕获“僵尸剧”
                today_minus_365_days = (datetime.now(timezone.utc).date() - timedelta(days=365)).isoformat()
                zombie_series = self._get_series_to_process(
                    f"""
                    WHERE status IN ('{STATUS_WATCHING}', '{STATUS_PAUSED}')
                      AND tmdb_status NOT IN ('Ended', 'Canceled')
                      AND jsonb_typeof(last_episode_to_air_json) = 'object'
                      AND (last_episode_to_air_json->>'air_date')::date < '{today_minus_365_days}'
                    """
                )
                logger.info(f"  -> 阶段2：发现 {len(zombie_series)} 部Tmdb状态滞后的“僵尸剧”。")

                # 阶段三：捕获“已正常完结，但后来文件又被删除”的剧集
                completed_missing_series = self._get_series_to_process(
                    f"WHERE status = '{STATUS_COMPLETED}' AND jsonb_typeof(missing_info_json) IN ('object', 'array')"
                )
                logger.info(f"  -> 阶段3：发现 {len(completed_missing_series)} 部已完结但文件缺失的剧集。")

                # 合并所有结果并去重
                all_series_map = {s['item_id']: s for s in stuck_series}
                all_series_map.update({s['item_id']: s for s in zombie_series})
                all_series_map.update({s['item_id']: s for s in completed_missing_series})
                series_to_check = list(all_series_map.values())
            
            total = len(series_to_check)
            if not series_to_check:
                if progress_callback: progress_callback(100, "所有流程已完成，未发现需洗版的剧集。")
                return

            logger.info(f"  -> 共发现 {total} 部剧集需要洗版，开始处理...")
            total_seasons_subscribed = 0

            # 循环内部的订阅逻辑是正确的，保持不变
            for i, series in enumerate(series_to_check):
                if self.is_stop_requested(): break
                item_name = series.get('item_name', '未知剧集')
                
                # 7天宽限期判断 (只对TMDb已完结的剧生效)
                if series.get('tmdb_status') in ['Ended', 'Canceled']:
                    last_episode_info = series.get('last_episode_to_air_json')
                    if last_episode_info and isinstance(last_episode_info, dict):
                        last_air_date_str = last_episode_info.get('air_date')
                        if last_air_date_str:
                            try:
                                last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                                days_since_airing = (datetime.now(timezone.utc).date() - last_air_date).days
                                if days_since_airing < 7:
                                    logger.info(f"  -> 《{item_name}》完结未满7天，跳过洗版。")
                                    continue
                            except ValueError: pass
                
                missing_info = series.get('missing_info_json')
                seasons_to_resubscribe = set()
                if missing_info:
                    for season in missing_info.get("missing_seasons", []):
                        if season.get('season_number') is not None: seasons_to_resubscribe.add(season['season_number'])
                    for episode in missing_info.get("missing_episodes", []):
                        if episode.get('season_number') is not None: seasons_to_resubscribe.add(episode['season_number'])
                
                if not seasons_to_resubscribe: continue

                logger.warning(f"  -> 检测到剧集《{item_name}》存在缺集: {sorted(list(seasons_to_resubscribe))}，准备逐季触发洗版订阅。")

                for season_num in sorted(list(seasons_to_resubscribe)):
                    success = moviepilot_handler.subscribe_series_to_moviepilot(
                        series_info=series, season_number=season_num,
                        config=self.config, best_version=1
                    )
                    if success: total_seasons_subscribed += 1
                    time.sleep(1)
                time.sleep(1)

            final_message = f"  -> 所有流程已完成！共为 {total_seasons_subscribed} 个缺失的季提交了洗版订阅。"
            if progress_callback: progress_callback(100, final_message)
            logger.trace(f"  -> 后台任务 '{task_name}' 结束，最终状态: 处理完成")

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            if progress_callback: progress_callback(-1, f"错误: {e}")
        finally:
            if progress_callback: self.progress_callback = None

    def _get_series_to_process(self, where_clause: str, item_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        【V11 - 修复版】从数据库获取需要处理的剧集列表。
        - 支持传入自定义的 WHERE 子句来筛选剧集。
        - 如果提供了 item_id，则无视 WHERE 子句，强制只处理该项目。
        """
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # ★★★ 核心修复：将传入的 where_clause 作为查询的基础 ★★★
                query = f"SELECT * FROM watchlist {where_clause}"
                params = []
                
                # 如果指定了item_id，则无视where_clause，强制处理这一部
                if item_id:
                    query = "SELECT * FROM watchlist WHERE item_id = %s"
                    params.append(item_id)
                
                cursor.execute(query, tuple(params))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取追剧列表时发生数据库错误: {e}")
            return []
            
    # ★★★ 核心处理逻辑：单个剧集的所有操作在此完成 ★★★
    def _process_one_series(self, series_data: Dict[str, Any]):
        item_id = str(series_data['item_id']).strip()
        tmdb_id = series_data['tmdb_id']
        item_name = series_data['item_name']
        is_force_ended = bool(series_data.get('force_ended', 0))
        
        logger.info(f"【追剧检查】正在处理: '{item_name}' (TMDb ID: {tmdb_id})")

        # 步骤1: 存活检查
        item_details_for_check = emby_handler.get_emby_item_details(
            item_id=item_id, emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
            user_id=self.emby_user_id, fields="Id,Name"
        )
        if not item_details_for_check:
            logger.warning(f"  -> 剧集 '{item_name}' (ID: {item_id}) 在 Emby 中已不存在。将从追剧列表移除。")
            watchlist_db.remove_item_from_watchlist(item_id=item_id)
            return 

        if not self.tmdb_api_key:
            logger.warning("未配置TMDb API Key，跳过。")
            return

        # 步骤2: 从TMDb获取权威数据
        logger.debug(f"  -> 正在从TMDb API获取 '{item_name}' 的最新详情...")
        latest_series_data = tmdb_handler.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not latest_series_data:
            logger.error(f"  -> 无法获取 '{item_name}' 的TMDb详情，本次处理中止。")
            return
        
        all_tmdb_episodes = []
        for season_summary in latest_series_data.get("seasons", []):
            season_num = season_summary.get("season_number")
            if season_num is None or season_num == 0: continue
            season_details = tmdb_handler.get_season_details_tmdb(tmdb_id, season_num, self.tmdb_api_key)
            if season_details and season_details.get("episodes"):
                all_tmdb_episodes.extend(season_details.get("episodes", []))
            time.sleep(0.1)

        # 步骤3: 获取Emby本地数据
        emby_children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields="Id,Name,ParentIndexNumber,IndexNumber,Type,Overview")
        emby_seasons = {}
        if emby_children:
            for child in emby_children:
                s_num, e_num = child.get('ParentIndexNumber'), child.get('IndexNumber')
                if s_num is not None and e_num is not None:
                    emby_seasons.setdefault(s_num, set()).add(e_num)

        # 步骤4: 计算状态和缺失信息
        new_tmdb_status = latest_series_data.get("status")
        is_ended_on_tmdb = new_tmdb_status in ["Ended", "Canceled"]
        
        real_next_episode_to_air = self._calculate_real_next_episode(all_tmdb_episodes, emby_seasons)
        missing_info = self._calculate_missing_info(latest_series_data.get('seasons', []), all_tmdb_episodes, emby_seasons)
        has_missing_media = bool(missing_info["missing_seasons"] or missing_info["missing_episodes"])

        # ★★★ 新增：元数据完整性检查 ★★★
        has_complete_metadata = self._check_all_episodes_have_overview(all_tmdb_episodes)

        # “本季大结局”判断逻辑
        is_season_finale = False
        last_episode_to_air = latest_series_data.get("last_episode_to_air")
        next_episode_to_air_tmdb = latest_series_data.get("next_episode_to_air")

        if last_episode_to_air and not next_episode_to_air_tmdb:
            last_air_date_str = last_episode_to_air.get("air_date")
            if last_air_date_str:
                try:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    if last_air_date <= datetime.now(timezone.utc).date():
                        is_season_finale = True
                        logger.info("  -> 符合“本季大结局”条件：已播出最后一集，且无明确的待播集。")
                except ValueError:
                    logger.warning(f"  -> 解析TMDb最后播出日期 '{last_air_date_str}' 失败。")

        final_status = STATUS_WATCHING
        paused_until_date = None

        # ▼▼▼ 核心状态判断逻辑 (已整合元数据检查) ▼▼▼
        # 完结的【硬性前提】：本地文件完整 且 元数据完整
        can_be_completed = not has_missing_media and has_complete_metadata

        if can_be_completed and (is_ended_on_tmdb or is_season_finale):
            final_status = STATUS_COMPLETED
            if is_season_finale and not is_ended_on_tmdb:
                logger.info(f"  -> 剧集因“本季大结局”且本地/元数据完整，状态变更为: {translate_internal_status(final_status)}")
            else:
                logger.info(f"  -> 剧集已完结且本地/元数据完整，状态变更为: {translate_internal_status(final_status)}")
        elif real_next_episode_to_air and real_next_episode_to_air.get('air_date'):
            air_date_str = real_next_episode_to_air['air_date']
            try:
                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                days_until_air = (air_date - datetime.now(timezone.utc).date()).days
                if days_until_air > 3:
                    final_status = STATUS_PAUSED
                    paused_until_date = air_date - timedelta(days=1)
                    logger.info(f"  -> 下一集在3天后播出，状态变更为: {translate_internal_status(final_status)}，暂停至 {paused_until_date}。")
                else:
                    final_status = STATUS_WATCHING
                    logger.info(f"  -> 下一集即将在3天内播出或已播出，状态保持为: {translate_internal_status(final_status)}。")
            except ValueError:
                logger.warning(f"  -> 解析TMDb待播日期 '{air_date_str}' 失败，将临时暂停。")
                final_status = STATUS_PAUSED
                paused_until_date = datetime.now(timezone.utc).date() + timedelta(days=1)
        else:
            final_status = STATUS_PAUSED
            paused_until_date = datetime.now(timezone.utc).date() + timedelta(days=7)
            # 对暂停原因进行更详细的日志记录
            if not has_complete_metadata and not has_missing_media:
                 logger.info(f"  -> 剧集文件完整但元数据不全，状态变更为: {translate_internal_status(final_status)}，暂停7天以待元数据更新。")
            else:
                 logger.info(f"  -> 暂无待播信息 (季歇期)，状态变更为: {translate_internal_status(final_status)}，暂停7天。")


        if is_force_ended and final_status != STATUS_COMPLETED:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.warning(f"  -> [强制完结生效] 剧集 '{item_name}' 被标记为强制完结，即使系统判断为其他状态，也将强制变更为 '已完结'。")

        # 步骤5: 更新追剧数据库
        updates_to_db = {
            "status": final_status,
            "paused_until": paused_until_date.isoformat() if paused_until_date else None,
            "tmdb_status": new_tmdb_status,
            "next_episode_to_air_json": json.dumps(real_next_episode_to_air) if real_next_episode_to_air else None,
            "missing_info_json": json.dumps(missing_info),
            "last_episode_to_air_json": json.dumps(last_episode_to_air) if last_episode_to_air else None
        }
        self._update_watchlist_entry(item_id, item_name, updates_to_db)

        # 步骤6：更新媒体数据缓存
        try:
            logger.debug(f"  -> 正在为 '{item_name}' 更新 '媒体数据缓存' 中的子项目详情...")
            
            # 复用已经获取的 emby_children 数据来构建详情列表
            children_details = []
            if emby_children:
                for child in emby_children:
                    child_type = child.get("Type")
                    detail = {
                        "Id": child.get("Id"),
                        "Type": child_type,
                        "Name": child.get("Name")
                    }
                    if child_type == "Season":
                        detail["SeasonNumber"] = child.get("IndexNumber")
                    elif child_type == "Episode":
                        detail["SeasonNumber"] = child.get("ParentIndexNumber")
                        detail["EpisodeNumber"] = child.get("IndexNumber")
                        detail["Overview"] = child.get("Overview")
                    children_details.append(detail)
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    update_query = """
                        UPDATE media_metadata
                        SET emby_children_details_json = %s::jsonb,
                            last_synced_at = %s
                        WHERE tmdb_id = %s AND item_type = 'Series'
                    """
                    details_json_str = json.dumps(children_details, ensure_ascii=False)
                    current_utc_time = datetime.now(timezone.utc)
                    
                    cursor.execute(update_query, (details_json_str, current_utc_time, tmdb_id))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"  -> 成功刷新了 '{item_name}' 在 '媒体数据缓存' 中的子项目详情。")
                    else:
                        # 这种情况是正常的，可能该剧集还未被全量扫描过，所以 media_metadata 里没有记录
                        logger.debug(f"  -> 在 '媒体数据缓存' 中未找到 '{item_name}' 的记录，本次不更新子项目详情。")
                conn.commit()

        except Exception as e_sync:
            logger.error(f"  -> [追剧联动] 在同步 '{item_name}' 的子项目详情到 '媒体数据缓存' 时发生错误: {e_sync}", exc_info=True)

        # 步骤7: 【最终动作】如果需要，命令Emby刷新自己
        tmdb_episodes_map = {
            f"S{ep.get('season_number')}E{ep.get('episode_number')}": ep
            for ep in all_tmdb_episodes
            if ep.get('season_number') is not None and ep.get('episode_number') is not None
        }

        for emby_episode in emby_children:
            if emby_episode.get("Type") == "Episode" and not emby_episode.get("Overview"):
                s_num = emby_episode.get("ParentIndexNumber")
                e_num = emby_episode.get("IndexNumber")
                
                if s_num is None or e_num is None:
                    continue

                ep_key = f"S{s_num}E{e_num}"
                ep_name_for_log = f"S{s_num:02d}E{e_num:02d}"
                
                tmdb_data_for_episode = tmdb_episodes_map.get(ep_key)
                if tmdb_data_for_episode:
                    overview = tmdb_data_for_episode.get("overview")
                    if overview and overview.strip():
                        emby_episode_id = emby_episode.get("Id")
                        logger.info(f"  -> 发现分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 缺少简介，准备从TMDb注入...")
                        data_to_inject = {
                            "Name": tmdb_data_for_episode.get("name"),
                            "Overview": overview
                        }
                        
                        success = emby_handler.update_emby_item_details(
                            item_id=emby_episode_id,
                            new_data=data_to_inject,
                            emby_server_url=self.emby_url,
                            emby_api_key=self.emby_api_key,
                            user_id=self.emby_user_id
                        )
                        if not success:
                            logger.error(f"  -> 更新 Emby 分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 简介失败。")
                    else:
                        logger.info(f"  -> TMDb中分集 '{ep_name_for_log}' 尚无简介，跳过更新。")
                else:
                    logger.warning(f"  -> Emby分集 '{ep_name_for_log}' 缺少简介，但在TMDb中未找到对应信息。")
        else:
            logger.info(f"  -> 剧集状态为 '{translate_internal_status(final_status)}' 或本地文件完整，无需执行分集更新。")

    # --- 统一的、公开的追剧处理入口 ★★★
    def process_watching_list(self, item_id: Optional[str] = None):
        if item_id:
            logger.info(f"--- 开始执行单项追剧更新任务 (ItemID: {item_id}) ---")
        else:
            logger.trace("--- 开始执行全量追剧列表更新任务 ---")
        
        series_to_process = []
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                query = "SELECT * FROM watchlist WHERE status = 'Watching'"
                params = []
                if item_id:
                    query = "SELECT * FROM watchlist WHERE item_id = %s"
                    params.append(item_id)
                cursor.execute(query, params)
                series_to_process = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取追剧列表时发生数据库错误: {e}")
            return

        if not series_to_process:
            logger.info("追剧列表中没有需要检查的剧集。")
            return

        total = len(series_to_process)
        logger.info(f"发现 {total} 部剧集需要检查更新...")

        for i, series in enumerate(series_to_process):
            if self.is_stop_requested():
                logger.info("追剧列表更新任务被中止。")
                break
            
            if self.progress_callback:
                progress = 10 + int(((i + 1) / total) * 90)
                self.progress_callback(progress, f"正在处理: {series['item_name'][:20]}... ({i+1}/{total})")

            self._process_one_series(series)
            time.sleep(1)

        logger.info("--- 追剧列表更新任务结束 ---")

    # --- 通过对比计算真正的下一待看集 ---
    def _calculate_real_next_episode(self, all_tmdb_episodes: List[Dict], emby_seasons: Dict) -> Optional[Dict]:
        """
        【逻辑重生】通过对比本地和TMDb全量数据，计算用户真正缺失的第一集。
        """
        # 1. 获取TMDb上所有非特别季的剧集，并严格按季号、集号排序
        all_episodes_sorted = sorted([
            ep for ep in all_tmdb_episodes 
            if ep.get('season_number') is not None and ep.get('season_number') != 0
        ], key=lambda x: (x.get('season_number', 0), x.get('episode_number', 0)))
        
        # 2. 遍历这个完整列表，找到第一个本地没有的剧集
        for episode in all_episodes_sorted:
            s_num = episode.get('season_number')
            e_num = episode.get('episode_number')
            
            if s_num not in emby_seasons or e_num not in emby_seasons.get(s_num, set()):
                # 找到了！这无论是否播出，都是用户最关心的下一集
                logger.info(f"  -> 找到本地缺失的第一集: S{s_num}E{e_num} ('{episode.get('name')}'), 将其设为待播集。")
                return episode
        
        # 3. 如果循环完成，说明本地拥有TMDb上所有的剧集
        logger.info("  -> 本地媒体库已拥有TMDb上所有剧集，无待播信息。")
        return None
    # --- 计算缺失的季和集 ---
    def _calculate_missing_info(self, tmdb_seasons: List[Dict], all_tmdb_episodes: List[Dict], emby_seasons: Dict) -> Dict:
        """
        【逻辑重生】计算所有缺失的季和集，不再关心播出日期。
        """
        missing_info = {"missing_seasons": [], "missing_episodes": []}
        
        tmdb_episodes_by_season = {}
        for ep in all_tmdb_episodes:
            s_num = ep.get('season_number')
            if s_num is not None and s_num != 0:
                tmdb_episodes_by_season.setdefault(s_num, []).append(ep)

        for season_summary in tmdb_seasons:
            s_num = season_summary.get('season_number')
            if s_num is None or s_num == 0: 
                continue

            # 如果本地没有这个季，则整个季都算缺失
            if s_num not in emby_seasons:
                missing_info["missing_seasons"].append(season_summary)
            else:
                # 如果季存在，则逐集检查缺失
                if s_num in tmdb_episodes_by_season:
                    for episode in tmdb_episodes_by_season[s_num]:
                        e_num = episode.get('episode_number')
                        if e_num is not None and e_num not in emby_seasons.get(s_num, set()):
                            missing_info["missing_episodes"].append(episode)
        return missing_info

    def _check_all_episodes_have_overview(self, all_episodes: List[Dict[str, Any]]) -> bool:
        """检查一个剧集的所有集是否都有简介(overview)。"""
        if not all_episodes:
            return True

        # ★★★ 修改：硬编码忽略所有第0季（特别篇）★★★
        missing_overview_episodes = [
            f"S{ep.get('season_number', 'N/A'):02d}E{ep.get('episode_number', 'N/A'):02d}"
            for ep in all_episodes if not ep.get("overview") and ep.get("season_number") != 0
        ]

        if missing_overview_episodes:
            logger.warning(f"  -> 元数据不完整，以下集缺少简介: {', '.join(missing_overview_episodes)}")
            return False
        
        logger.info("  -> 元数据完整性检查通过，所有集都有简介。")
        return True

    def _update_watchlist_status(self, item_id: str, status: str, item_name: str):
        """【V4 - 最终修复版】更新数据库中指定项目的状态。"""
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # ★★★ 核心修正：统一使用 datetime.utcnow() ★★★
                    current_time = datetime.utcnow()
                    cursor.execute("UPDATE watchlist SET status = %s, last_checked_at = %s WHERE item_id = %s", (status, current_time, item_id))
                conn.commit()
            logger.info(f"  成功更新 '{item_name}' 在数据库中的状态为 '{status}'。")
        except Exception as e:
            logger.error(f"  更新 '{item_name}' 状态为 '{status}' 时数据库出错: {e}")

    def _update_watchlist_timestamp(self, item_id: str, item_name: str):
        """【V4 - 最终修复版】仅更新数据库中指定项目的 last_checked_at 时间戳。"""
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # ★★★ 核心修正：统一使用 datetime.utcnow() ★★★
                    current_time = datetime.utcnow()
                    cursor.execute("UPDATE watchlist SET last_checked_at = %s WHERE item_id = %s", (current_time, item_id))
                conn.commit()
        except Exception as e:
            logger.error(f"更新 '{item_name}' 的 last_checked_at 时间戳时失败: {e}")
