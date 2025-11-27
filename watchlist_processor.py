# watchlist_processor.py

import time
import json
import os
import concurrent.futures
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
import threading

# 导入我们需要的辅助模块
from database import connection, media_db, request_db, watchlist_db
import constants
import handler.tmdb as tmdb
import handler.emby as emby
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
    【V13 - media_metadata 适配版】
    - 所有数据库操作完全迁移至 media_metadata 表。
    - 读写逻辑重构，以 tmdb_id 为核心标识符。
    - 保留了所有复杂的状态判断逻辑，使其在新架构下无缝工作。
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

    # ★★★ 核心修改 1: 重构统一的数据库更新函数 ★★★
    def _update_watchlist_entry(self, tmdb_id: str, item_name: str, updates: Dict[str, Any]):
        """【新架构】统一更新 media_metadata 表中的追剧信息。"""
        # 字段名映射：将旧的逻辑键名映射到新的数据库列名
        column_mapping = {
            'status': 'watching_status',
            'paused_until': 'paused_until',
            'tmdb_status': 'watchlist_tmdb_status',
            'next_episode_to_air_json': 'watchlist_next_episode_json',
            'missing_info_json': 'watchlist_missing_info_json',
            'last_episode_to_air_json': 'last_episode_to_air_json', # 这个字段是主元数据的一部分
            'is_airing': 'watchlist_is_airing',
            'force_ended': 'force_ended'
        }
        
        # 使用映射转换 updates 字典
        db_updates = {column_mapping[k]: v for k, v in updates.items() if k in column_mapping}
        
        if not db_updates:
            logger.warning(f"  ➜ 尝试更新 '{item_name}'，但没有提供有效的更新字段。")
            return

        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 NOW() 让数据库自己处理时间，更可靠
                    db_updates['watchlist_last_checked_at'] = 'NOW()'
                    
                    # 动态生成 SET 子句，特殊处理 NOW()
                    set_clauses = [f"{key} = {value}" if key == 'watchlist_last_checked_at' else f"{key} = %s" for key, value in db_updates.items()]
                    values = [v for k, v in db_updates.items() if k != 'watchlist_last_checked_at']
                    values.append(tmdb_id)
                    
                    sql = f"UPDATE media_metadata SET {', '.join(set_clauses)} WHERE tmdb_id = %s AND item_type = 'Series'"
                    
                    cursor.execute(sql, tuple(values))
                conn.commit()
                logger.info(f"  ➜ 成功更新数据库中 '{item_name}' 的追剧信息。")
        except Exception as e:
            logger.error(f"  更新 '{item_name}' 的追剧信息时数据库出错: {e}", exc_info=True)

    # ★★★ 核心修改 2: 重构自动添加追剧列表的函数 ★★★
    def add_series_to_watchlist(self, item_details: Dict[str, Any]):
        """ 将新剧集添加/更新到 media_metadata 表并标记为追剧。"""
        if item_details.get("Type") != "Series": return
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name")
        item_id = item_details.get("Id") # Emby ID
        if not tmdb_id or not item_name or not item_id or not self.tmdb_api_key: return
            
        tmdb_details = tmdb.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not tmdb_details: return

        tmdb_status = tmdb_details.get("status")
        if not tmdb_status:
            logger.warning(f"无法确定剧集 '{item_name}' 的TMDb状态，跳过自动添加。")
            return

        # 保留原有的“冷宫”判断逻辑
        internal_status = STATUS_COMPLETED
        today = datetime.now(timezone.utc).date()
        
        if tmdb_status in ["Returning Series", "In Production", "Planned"]:
            next_episode = tmdb_details.get("next_episode_to_air")
            if next_episode and next_episode.get('air_date'):
                try:
                    air_date = datetime.strptime(next_episode['air_date'], '%Y-%m-%d').date()
                    if (air_date - today).days <= 90:
                        internal_status = STATUS_WATCHING
                except (ValueError, TypeError):
                    pass
        is_airing = (internal_status == STATUS_WATCHING)
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 UPSERT 逻辑，同时更新 watchlist_is_airing
                    sql = """
                        INSERT INTO media_metadata (tmdb_id, item_type, title, watching_status, watchlist_is_airing, emby_item_ids_json)
                        VALUES (%s, 'Series', %s, %s, %s, %s)
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            watching_status = EXCLUDED.watching_status,
                            watchlist_is_airing = EXCLUDED.watchlist_is_airing,
                            -- 智能合并 Emby ID
                            emby_item_ids_json = (
                                SELECT jsonb_agg(DISTINCT elem)
                                FROM (
                                    SELECT jsonb_array_elements_text(media_metadata.emby_item_ids_json) AS elem
                                    UNION ALL
                                    SELECT jsonb_array_elements_text(EXCLUDED.emby_item_ids_json) AS elem
                                ) AS combined
                            );
                    """
                    cursor.execute(sql, (tmdb_id, item_name, internal_status, is_airing, json.dumps([item_id])))
                    
                    if cursor.rowcount > 0:
                        log_status_translated = translate_internal_status(internal_status)
                        logger.info(f"  ➜ 剧集 '{item_name}' 已自动加入追剧列表，初始状态为: {log_status_translated} (连载中: {is_airing})。")
                conn.commit()
        except Exception as e:
            logger.error(f"自动添加剧集 '{item_name}' 到追剧列表时发生数据库错误: {e}", exc_info=True)

    # --- 核心任务启动器  ---
    def run_regular_processing_task_concurrent(self, progress_callback: callable, tmdb_id: Optional[str] = None, force_full_update: bool = False):
        """【V3 - 终极修复版】核心任务启动器，正确处理 tmdb_id。"""
        self.progress_callback = progress_callback
        task_name = "并发追剧更新"
        if force_full_update: task_name = "并发追剧更新 (深度模式)"
        if tmdb_id: task_name = f"单项追剧更新 (TMDb ID: {tmdb_id})"
        
        self.progress_callback(0, "准备检查待更新剧集...")
        try:
            where_clause = ""
            if not tmdb_id: # 只有在非单项刷新时，才构建 WHERE 子句
                if force_full_update:
                    where_clause = "WHERE force_ended = FALSE"
                    logger.info("  ➜ 已启用【深度模式】，将刷新所有追剧列表中的项目。")
                else:
                    today_str = datetime.now(timezone.utc).date().isoformat()
                    where_clause = f"WHERE watching_status = '{STATUS_WATCHING}' OR (watching_status = '{STATUS_PAUSED}' AND paused_until <= '{today_str}')"

            # ★★★★★★★★★★★★★★★ 终极修复 3/3: 将 tmdb_id 传递给数据获取函数 ★★★★★★★★★★★★★★★
            active_series = self._get_series_to_process(where_clause, tmdb_id=tmdb_id)
            
            if active_series:
                # ... (后续的并发处理逻辑完全不变) ...
                total = len(active_series)
                self.progress_callback(5, f"开始并发处理 {total} 部剧集...")
                
                processed_count = 0
                lock = threading.Lock()

                def worker_process_series(series: dict):
                    if self.is_stop_requested(): return "任务已停止"
                    try:
                        self._process_one_series(series)
                        return "处理成功"
                    except Exception as e:
                        logger.error(f"处理剧集 {series.get('item_name')} 时发生错误: {e}", exc_info=False)
                        return f"处理失败: {e}"

                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_series = {executor.submit(worker_process_series, series): series for series in active_series}
                    
                    for future in concurrent.futures.as_completed(future_to_series):
                        if self.is_stop_requested():
                            executor.shutdown(wait=False, cancel_futures=True)
                            break

                        series_info = future_to_series[future]
                        try:
                            result = future.result()
                            logger.trace(f"'{series_info['item_name']}' - {result}")
                        except Exception as exc:
                            logger.error(f"任务 '{series_info['item_name']}' 执行时产生未捕获的异常: {exc}")

                        with lock:
                            processed_count += 1
                        
                        progress = 5 + int((processed_count / total) * 95)
                        self.progress_callback(progress, f"剧集处理: {processed_count}/{total} - {series_info['item_name'][:15]}...")
                
                if not self.is_stop_requested():
                    self.progress_callback(100, "追剧检查完成。")
            else:
                self.progress_callback(100, "没有需要处理的剧集，任务完成。")
            
        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    # ★★★ 专门用于“已完结剧集”预定新季的任务方法 ★★★
    def run_new_season_check_task(self, progress_callback: callable):
        """ 低频扫描所有已完结剧集，发现即将播出的新季，并为其创建预订阅请求。"""
        self.progress_callback = progress_callback
        task_name = "已完结剧集新季预定"
        self.progress_callback(0, "准备开始预定检查...")
        try:
            completed_series = self._get_series_to_process(f"WHERE watching_status = '{STATUS_COMPLETED}' AND force_ended = FALSE")
            total = len(completed_series)
            if not completed_series:
                self.progress_callback(100, "没有需要检查的已完结剧集。")
                return

            logger.info(f"开始低频检查 {total} 部已完结剧集是否有新季上线...")
            self.progress_callback(10, f"发现 {total} 部已完结剧集，开始检查...")
            revived_count = 0
            today = datetime.now(timezone.utc).date()

            for i, series in enumerate(completed_series):
                if self.is_stop_requested(): break
                progress = 10 + int(((i + 1) / total) * 90)
                series_name = series['item_name']
                self.progress_callback(progress, f"检查中: {series_name[:20]}... ({i+1}/{total})")

                tmdb_details = tmdb.get_tv_details(series['tmdb_id'], self.tmdb_api_key)
                if not tmdb_details: continue

                last_episode_info = series.get('last_episode_to_air_json')
                old_season_number = 0
                if last_episode_info and isinstance(last_episode_info, dict):
                    old_season_number = last_episode_info.get('season_number', 0)

                new_total_seasons = tmdb_details.get('number_of_seasons', 0)

                if new_total_seasons > old_season_number:
                    new_season_to_check_num = old_season_number + 1
                    season_details = tmdb.get_tv_season_details(series['tmdb_id'], new_season_to_check_num, self.tmdb_api_key)
                    
                    if season_details and (air_date_str := season_details.get('air_date')):
                        try:
                            air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                            days_until_air = (air_date - today).days
                            
                            # 如果新季在未来7天内（包括今天）上线，直接将其加入待发布订阅列表
                            if 0 <= days_until_air <= 7:
                                revived_count += 1
                                logger.info(f"  ➜ 发现《{series_name}》的新季 (S{new_season_to_check_num}) 将在 {days_until_air} 天后上线，准备提交预订阅！")
                                
                                # 1. 准备新一季的媒体信息
                                season_tmdb_id = str(season_details.get('id'))
                                media_info = {
                                    'tmdb_id': season_tmdb_id,
                                    'item_type': 'Season',
                                    'title': f"{series_name} - {season_details.get('name', f'第 {new_season_to_check_num} 季')}",
                                    'release_date': season_details.get('air_date'),
                                    'poster_path': season_details.get('poster_path'),
                                    'season_number': new_season_to_check_num,
                                    'parent_series_tmdb_id': series['tmdb_id'],
                                    'overview': season_details.get('overview')
                                }
                                
                                # 2. 调用 request_db 将其状态设置为 PENDING_RELEASE
                                request_db.set_media_status_pending_release(
                                    tmdb_ids=season_tmdb_id,
                                    item_type='Season',
                                    source={"type": "watchlist", "reason": "revived_season", "item_id": series['tmdb_id']},
                                    media_info_list=[media_info]
                                )
                                logger.info(f"  ➜ 已成功为《{series_name}》 S{new_season_to_check_num} 创建“待上映”订阅。")

                                # 3. 立即更新本地数据库状态为“追剧中” 
                                updates = {
                                    "is_airing": True,
                                    "force_ended": False, # 核心：移除强制完结标记
                                    "tmdb_status": "Returning Series"
                                }

                                # 决策逻辑：
                                # 如果已经上映，或者未来3天内上映 -> 设为 Watching (高亮显示)
                                # 如果还有很久才上映 -> 设为 Paused 并设置 paused_until (低调等待)
                                if days_until_air <= 3:
                                    updates["status"] = STATUS_WATCHING
                                    updates["paused_until"] = None
                                    log_status = "追剧中 (Watching)"
                                else:
                                    updates["status"] = STATUS_PAUSED
                                    updates["paused_until"] = air_date.isoformat()
                                    log_status = f"已暂停 (Paused) 至 {air_date_str}"

                                self._update_watchlist_entry(series['tmdb_id'], series_name, updates)
                                
                                # 同时更新季的活跃状态 (注意：如果设为Paused，季也会被标记为Paused，这是符合预期的)
                                watchlist_db.sync_seasons_watching_status(series['tmdb_id'], [new_season_to_check_num], updates["status"])
                                
                                logger.info(f"  ➜ 已成功复活《{series_name}》：状态更新为 '{log_status}'，并已提交 S{new_season_to_check_num} 的订阅请求。")

                        except ValueError:
                            logger.warning(f"  ➜ 解析《{series_name}》新季的播出日期 '{air_date_str}' 失败。")
                
                time.sleep(1) # 保持适当的API请求间隔
            
            final_message = f"复活检查完成。共发现并订阅了 {revived_count} 部剧集的待播新季。"
            self.progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    def _get_series_to_process(self, where_clause: str, tmdb_id: Optional[str] = None, include_all_series: bool = False) -> List[Dict[str, Any]]:
        """【V4 - 终极修复版】正确使用 tmdb_id 进行单项查找。"""
        
        base_query = """
            SELECT 
                tmdb_id,
                title AS item_name,
                watching_status AS status,
                emby_item_ids_json, -- ★★★ 获取完整的JSON数组
                force_ended,
                paused_until,
                last_episode_to_air_json,
                watchlist_tmdb_status AS tmdb_status,
                watchlist_missing_info_json AS missing_info_json,
                subscription_status
            FROM media_metadata
        """
        
        # ★★★ 核心修复：单项刷新时，直接用 tmdb_id 查询数据库 ★★★
        if tmdb_id:
            try:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    query = f"{base_query} WHERE item_type = 'Series' AND tmdb_id = %s"
                    cursor.execute(query, (tmdb_id,))
                    result = [dict(row) for row in cursor.fetchall()]
                    if not result:
                        logger.warning(f"  ➜ 数据库中未找到 TMDb ID 为 {tmdb_id} 的追剧记录。")
                    return result
            except Exception as e:
                logger.error(f"为 tmdb_id {tmdb_id} 获取追剧信息时发生数据库错误: {e}", exc_info=True)
                return []

        # --- 以下为批量刷新的逻辑，保持不变 ---
        selected_libraries = self.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
        
        if not selected_libraries:
            try:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    final_where = "WHERE item_type = 'Series'"
                    if not include_all_series:
                        final_where += " AND watching_status != 'NONE'"
                    
                    if where_clause:
                        final_where += f" AND ({where_clause.replace('WHERE', '').strip()})"
                    
                    query = f"{base_query} {final_where}"
                    cursor.execute(query)
                    return [dict(row) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"获取全部追剧列表时发生数据库错误: {e}")
                return []

        logger.info(f"  ➜ 已启用媒体库过滤器，开始从 {len(selected_libraries)} 个选定媒体库中获取剧集ID...")
        
        valid_series_ids_from_emby = set()
        for lib_id in selected_libraries:
            series_ids_in_lib = emby.get_library_series_ids(
                library_id=lib_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id
            )
            valid_series_ids_from_emby.update(series_ids_in_lib)
        
        if not valid_series_ids_from_emby:
            logger.warning("  ➜ 从所选媒体库中未能获取到任何剧集ID，本次任务将不处理任何项目。")
            return []
            
        logger.info(f"  ➜ 成功从Emby获取到 {len(valid_series_ids_from_emby)} 个有效的剧集ID，开始匹配数据库...")

        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                final_where = "WHERE item_type = 'Series'"
                if not include_all_series:
                    final_where += " AND watching_status != 'NONE'"

                if where_clause:
                    final_where += f" AND ({where_clause.replace('WHERE', '').strip()})"
                
                query = f"{base_query} {final_where}"
                cursor.execute(query)
                all_candidate_series = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"过滤前获取追剧列表时发生数据库错误: {e}")
            return []
            
        final_series_to_process = []
        for series in all_candidate_series:
            emby_ids = series.get('emby_item_ids_json', [])
            if isinstance(emby_ids, list) and any(eid in valid_series_ids_from_emby for eid in emby_ids):
                final_series_to_process.append(series)
        
        logger.info(f"  ➜ 媒体库过滤完成：数据库中发现 {len(all_candidate_series)} 个候选项目，最终匹配到 {len(final_series_to_process)} 个。")
        
        return final_series_to_process
            
    # ★★★ 核心处理逻辑：单个剧集的所有操作在此完成 ★★★
    def _process_one_series(self, series_data: Dict[str, Any]):
        tmdb_id = series_data['tmdb_id']
        # ★★★ 关键修改：emby_item_ids_json 是一个列表，我们取第一个作为代表ID ★★★
        emby_ids = series_data.get('emby_item_ids_json', [])
        item_id = emby_ids[0] if emby_ids else None
        item_name = series_data['item_name']
        is_force_ended = bool(series_data.get('force_ended', False))
        
        logger.info(f"  ➜ 【追剧检查】正在处理: '{item_name}' (TMDb ID: {tmdb_id})")

        # 步骤1: 存活检查 (这一步可以简化或移除，因为已经在任务开始时批量过滤了)
        # 为保持单项刷新的健壮性，我们保留一个简单的ID存在性检查
        if not item_id:
            logger.warning(f"  ➜ 剧集 '{item_name}' 在数据库中没有关联的 Emby ID，跳过。")
            return

        if not self.tmdb_api_key:
            logger.warning("  ➜ 未配置TMDb API Key，跳过。")
            return

        # 步骤2: 从TMDb获取权威数据 (逻辑不变)
        logger.debug(f"  ➜ 正在从TMDb API获取 '{item_name}' 的最新详情...")
        latest_series_data = tmdb.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not latest_series_data:
            logger.error(f"  🚫 无法获取 '{item_name}' 的TMDb详情，本次处理中止。")
            return
        
        all_tmdb_episodes = []
        for season_summary in latest_series_data.get("seasons", []):
            season_num = season_summary.get("season_number")
            if season_num is None or season_num == 0: continue
            season_details = tmdb.get_season_details_tmdb(tmdb_id, season_num, self.tmdb_api_key)
            if season_details and season_details.get("episodes"):
                all_tmdb_episodes.extend(season_details.get("episodes", []))
            time.sleep(0.1)

        # ★★★ 步骤3: 从本地数据库获取媒体库数据 (核心重构) ★★★
        # 不再调用 emby.get_series_children，而是调用 media_db
        emby_seasons = media_db.get_series_local_children_info(tmdb_id)
        # ★★★ 同时，获取本地分集元数据用于后续的简介注入检查 ★★★
        local_episodes_metadata = media_db.get_series_local_episodes_overview(tmdb_id)

        # 步骤4: 计算状态和缺失信息 (逻辑不变)
        new_tmdb_status = latest_series_data.get("status")
        is_ended_on_tmdb = new_tmdb_status in ["Ended", "Canceled"]
        
        real_next_episode_to_air = self._calculate_real_next_episode(all_tmdb_episodes, emby_seasons)
        missing_info = self._calculate_missing_info(latest_series_data.get('seasons', []), all_tmdb_episodes, emby_seasons)
        has_missing_media = bool(missing_info["missing_seasons"] or missing_info["missing_episodes"])

        today_str = datetime.now(timezone.utc).date().isoformat()
        aired_episodes = [ep for ep in all_tmdb_episodes if ep.get('air_date') and ep['air_date'] <= today_str]
        has_complete_metadata = self._check_all_episodes_have_overview(aired_episodes)

        last_episode_to_air = latest_series_data.get("last_episode_to_air")
        final_status = STATUS_WATCHING # 默认是追剧中
        paused_until_date = None
        today = datetime.now(timezone.utc).date()

        # 步骤A: 预处理 - 确定是否存在一个“有效的、未来的”下一集
        effective_next_episode = None
        effective_next_episode_air_date = None  # <-- 新增一个变量来存储date对象
        if real_next_episode_to_air and (air_date_str := real_next_episode_to_air.get('air_date')):
            try:
                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                if air_date >= today:
                    effective_next_episode = real_next_episode_to_air
                    effective_next_episode_air_date = air_date 
            except (ValueError, TypeError):
                logger.warning(f"  ➜ 解析待播日期 '{air_date_str}' 失败，将忽略此下一集信息。")

        # 步骤B: 进入全新的、不会被短路的主决策链
        # 规则1：硬性完结条件 (最高优先级)
        if is_ended_on_tmdb and has_complete_metadata:
            final_status = STATUS_COMPLETED
            logger.info(f"  ➜ [判定] 剧集在TMDb已完结且元数据完整，状态变更为: {translate_internal_status(final_status)}")

        # 规则2：如果存在一个“有效的、未来的”下一集
        # 规则2：如果存在一个“有效的、未来的”下一集
        elif effective_next_episode:
            air_date = effective_next_episode_air_date 
            days_until_air = (air_date - today).days
            episode_number = effective_next_episode.get('episode_number')

            if days_until_air <= 3:
                final_status = STATUS_WATCHING
                logger.info(f"  ➜ [判定] 下一集在未来3天内播出，状态保持为: {translate_internal_status(final_status)}。")
            elif 3 < days_until_air <= 90:
                if episode_number is not None and int(episode_number) == 1:
                    final_status = STATUS_COMPLETED
                    logger.warning(f"  ➜ [判定] 下一集是新季首播，在 {days_until_air} 天后播出。当前季已完结，状态变更为“已完结”。")
                else:
                    final_status = STATUS_PAUSED
                    paused_until_date = air_date - timedelta(days=1)
                    logger.info(f"  ➜ [判定] 下一集 (非首集) 在 {days_until_air} 天后播出，状态变更为: {translate_internal_status(final_status)}，暂停至 {paused_until_date}。")
            else: # days_until_air > 90
                final_status = STATUS_COMPLETED
                logger.warning(f"  ➜ [判定] 下一集在 {days_until_air} 天后播出，超过90天阈值，状态强制变更为“已完结”。")

        # 规则3：“僵尸剧”判断 (现在可以被正确地执行了)
        # 只有在没有“未来下一集”的情况下，才会进入此分支
        elif last_episode_to_air and (last_air_date_str := last_episode_to_air.get('air_date')):
            try:
                last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                days_since_last_air = (today - last_air_date).days
                
                # 使用一个更宽容的阈值，比如30天，来应对TMDb更新不及时的情况
                if days_since_last_air > 30:
                    final_status = STATUS_COMPLETED
                    logger.warning(f"  ➜ [判定-僵尸剧] 剧集无未来待播信息，且最后一集播出已超过60天（TMDb数据为 {last_air_date_str}），状态强制变更为“已完结”。")
                else:
                    final_status = STATUS_PAUSED
                    paused_until_date = today + timedelta(days=7)
                    logger.info(f"  ➜ [判定] 剧集无未来待播信息，但上一集在30天内播出，临时暂停7天以待数据更新。")
            except ValueError:
                final_status = STATUS_PAUSED
                paused_until_date = today + timedelta(days=7)
                logger.warning(f"  ➜ [判定] 剧集上次播出日期格式错误，为安全起见，执行默认的7天暂停。")

        # 规则4：绝对的后备方案
        else:
            final_status = STATUS_PAUSED
            paused_until_date = today + timedelta(days=7)
            logger.info(f"  ➜ [判定-后备] 剧集完全缺失播出日期数据，为安全起见，执行默认的7天暂停以待数据更新。")

        # 规则5：强制完结标志拥有最高优先级
        if is_force_ended and final_status != STATUS_COMPLETED:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.warning(f"  ➜ [强制完结生效] 最终状态被覆盖为 '已完结'。")

        # 只有当内部状态是“追剧中”或“已暂停”时，才认为它在“连载中”
        is_truly_airing = final_status in [STATUS_WATCHING, STATUS_PAUSED]
        logger.info(f"  ➜ 最终判定 '{item_name}' 的真实连载状态为: {is_truly_airing} (内部状态: {translate_internal_status(final_status)})")

        # 步骤5: 更新追剧数据库
        updates_to_db = {
            "status": final_status,
            "paused_until": paused_until_date.isoformat() if paused_until_date else None,
            "tmdb_status": new_tmdb_status,
            "next_episode_to_air_json": json.dumps(real_next_episode_to_air) if real_next_episode_to_air else None,
            "missing_info_json": json.dumps(missing_info),
            "last_episode_to_air_json": json.dumps(last_episode_to_air) if last_episode_to_air else None,
            "is_airing": is_truly_airing
        }
        self._update_watchlist_entry(tmdb_id, item_name, updates_to_db)

        active_seasons = set()
        
        # 规则 A: 如果有明确的下一集待播，该集所属的季肯定是活跃的
        if real_next_episode_to_air and real_next_episode_to_air.get('season_number'):
            active_seasons.add(real_next_episode_to_air['season_number'])
            
        # 规则 B: 如果有缺失的集（补番），这些集所属的季也是活跃的
        if missing_info.get('missing_episodes'):
            for ep in missing_info['missing_episodes']:
                if ep.get('season_number'):
                    active_seasons.add(ep['season_number'])
                    
        # 规则 C: 如果有整季缺失，且该季已播出，也视为活跃
        if missing_info.get('missing_seasons'):
            for s in missing_info['missing_seasons']:
                # 简单的判断：如果季有播出日期且在今天之前，算活跃（需要补）
                if s.get('air_date') and s.get('season_number'):
                    try:
                        s_date = datetime.strptime(s['air_date'], '%Y-%m-%d').date()
                        if s_date <= today:
                            active_seasons.add(s['season_number'])
                    except ValueError:
                        pass

        # 调用 DB 模块进行批量更新
        # 注意：如果 final_status 是 Completed，DB函数会自动处理所有季为Completed
        watchlist_db.sync_seasons_watching_status(tmdb_id, list(active_seasons), final_status)

        # 步骤6：把需要订阅的剧加入待订阅队列
        today = datetime.now(timezone.utc).date()

        # ★★★ 场景一：补旧番 - 只处理已完结剧集中，已播出的缺失季 ★★★
        if final_status == STATUS_COMPLETED and has_missing_media:
            logger.info(f"  ➜ 《{item_name}》为已完结状态，开始检查可补全的缺失季...")
            
            for season in missing_info.get("missing_seasons", []):
                season_num = season.get('season_number')
                air_date_str = season.get('air_date')
                
                if season_num is None or not air_date_str:
                    continue

                try:
                    air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                    # 关键判断：只有当这一季的播出日期早于或等于今天，才订阅
                    if air_date <= today:
                        logger.warning(f"  ➜ 发现已完结的缺失季 S{season_num} (播出日期: {air_date_str})，将状态设为 WANTED。")
                        
                        # 准备媒体信息
                        season_tmdb_id = str(season.get('id'))
                        media_info = {
                            'tmdb_id': season_tmdb_id, # ★★★ BUG修复：使用季的TMDB ID作为键 ★★★
                            'item_type': 'Season',     # 概念修正
                            'title': f"{item_name} {season.get('name', f'第 {season_num} 季')}", # 标题构建更健壮
                            'original_title': latest_series_data.get('original_name'),
                            'release_date': season.get('air_date'),
                            'poster_path': season.get('poster_path'),
                            'overview': season.get('overview'), 
                            'season_number': season_num
                        }
                        
                        # 推送需求
                        request_db.set_media_status_wanted(
                            tmdb_ids=str(season.get('id')), # ★★★ 核心修正：使用季的真实 TMDB ID ★★★
                            item_type='Season',             # ★★★ 核心修正：类型明确为 Season ★★★
                            source={"type": "watchlist", "reason": "missing_completed_season", "item_id": item_id},
                            media_info_list=[media_info]
                        )
                    else:
                        logger.info(f"  ➜ 缺失季 S{season_num} 尚未播出 ({air_date_str})，跳过补全订阅。")
                except ValueError:
                    logger.warning(f"  ➜ 解析缺失季 S{season_num} 的播出日期 '{air_date_str}' 失败，跳过。")

        # ★★★ 场景二：追新剧 - 为在追/暂停的剧集，订阅所有缺失内容 (保持原逻辑) ★★★
        elif final_status in [STATUS_WATCHING, STATUS_PAUSED] and has_missing_media:
            logger.info(f"  ➜ 《{item_name}》为在追状态，将订阅所有缺失内容...")
            
            today = datetime.now(timezone.utc).date()

            # a. 处理缺失的整季
            for season in missing_info.get("missing_seasons", []):
                season_num = season.get('season_number')
                if season_num is None: continue

                # 准备通用的采购单信息
                season_tmdb_id = str(season.get('id'))
                media_info = {
                    'tmdb_id': season_tmdb_id,
                    'item_type': 'Season',
                    'title': f"{item_name} - {season.get('name', f'第 {season_num} 季')}",
                    'original_title': latest_series_data.get('original_name'),
                    'release_date': season.get('air_date'),
                    'poster_path': season.get('poster_path'),
                    'overview': season.get('overview'), 
                    'season_number': season_num,
                    'parent_series_tmdb_id': tmdb_id
                }
                
                # ★★★★★★★★★★★★★★★ 核心修复：智能分拣状态 ★★★★★★★★★★★★★★★
                air_date_str = season.get('air_date')
                is_pending = False
                if air_date_str:
                    try:
                        air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                        if air_date > today:
                            is_pending = True
                    except ValueError:
                        # 日期格式错误，按已上映处理
                        pass
                
                if is_pending:
                    # 如果是未来的季，则标记为 PENDING_RELEASE
                    logger.info(f"  ➜ 发现未上映的缺失季 S{season_num} (播出日期: {air_date_str})，将状态设为 PENDING_RELEASE。")
                    request_db.set_media_status_pending_release(
                        tmdb_ids=season_tmdb_id,
                        item_type='Season',
                        source={"type": "watchlist", "reason": "missing_season", "item_id": item_id},
                        media_info_list=[media_info]
                    )
                else:
                    # 如果是已上映或日期未知的季，则标记为 WANTED
                    logger.info(f"  ➜ 发现已上映的缺失季 S{season_num}，将状态设为 WANTED。")
                    request_db.set_media_status_wanted(
                        tmdb_ids=season_tmdb_id,
                        item_type='Season',
                        source={"type": "watchlist", "reason": "missing_season", "item_id": item_id},
                        media_info_list=[media_info]
                    )

        # 步骤7: 命令Emby刷新自己，并同步更新内存中的`emby_children`
        logger.debug(f"  ➜ 开始检查并注入缺失的分集简介到 Emby...")
        tmdb_episodes_map = {
            f"S{ep.get('season_number')}E{ep.get('episode_number')}": ep
            for ep in all_tmdb_episodes
            if ep.get('season_number') is not None and ep.get('episode_number') is not None
        }

        # 使用从本地数据库获取的 local_episodes_metadata
        for local_episode in local_episodes_metadata:
            if not local_episode.get("overview"): # 只处理本地记录里没有简介的
                s_num = local_episode.get("season_number")
                e_num = local_episode.get("episode_number")
                
                if s_num is None or e_num is None: continue

                ep_key = f"S{s_num}E{e_num}"
                ep_name_for_log = f"S{s_num:02d}E{e_num:02d}"
                
                tmdb_data_for_episode = tmdb_episodes_map.get(ep_key)
                if tmdb_data_for_episode and (overview := tmdb_data_for_episode.get("overview")):
                    emby_episode_id = local_episode.get("emby_item_id")
                    if not emby_episode_id: continue

                    logger.info(f"  ➜ 发现分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 缺少简介，准备从TMDb注入...")
                    data_to_inject = {"Name": tmdb_data_for_episode.get("name"), "Overview": overview}
                    
                    success = emby.update_emby_item_details(
                        item_id=emby_episode_id, new_data=data_to_inject,
                        emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
                        user_id=self.emby_user_id
                    )
                    if success:
                        logger.info(f"  ➜ Emby 分集 '{ep_name_for_log}' 简介更新成功。")
                        # ★★★ 可以在此更新本地数据库的 'overview' 字段，形成闭环 ★★★
                        media_db.update_episode_overview(emby_episode_id, overview)
                    else:
                        logger.error(f"  ➜ 更新 Emby 分集 '{ep_name_for_log}' 简介失败。")
        
        logger.info(f"  ➜ 分集简介检查与注入流程完成。")

        # 步骤8：更新媒体数据缓存
        try:
            logger.debug(f"  ➜ 正在为 '{item_name}' 更新 '媒体数据缓存' 中的子项目详情...")
            
            media_db.sync_series_children_metadata(
                parent_tmdb_id=tmdb_id,
                seasons=latest_series_data.get("seasons", []),
                episodes=all_tmdb_episodes,
                local_in_library_info=emby_seasons
            )
            
        except Exception as e_sync:
            logger.error(f"  ➜ [追剧联动] 在同步 '{item_name}' 的子项目详情到 '媒体数据缓存' 时发生错误: {e_sync}", exc_info=True)

    # --- 统一的、公开的追剧处理入口 ★★★
    def process_watching_list(self, item_id: Optional[str] = None):
        if item_id:
            logger.info(f"--- 开始执行单项追剧更新任务 (ItemID: {item_id}) ---")
        else:
            logger.trace("--- 开始执行全量追剧列表更新任务 ---")
        
        series_to_process = self._get_series_to_process(
            where_clause="WHERE status = 'Watching'", 
            item_id=item_id
        )

        if not series_to_process:
            logger.info("  ➜ 追剧列表中没有需要检查的剧集。")
            return

        total = len(series_to_process)
        logger.info(f"  ➜ 发现 {total} 部剧集需要检查更新...")

        for i, series in enumerate(series_to_process):
            if self.is_stop_requested():
                logger.info("  🚫 追剧列表更新任务被中止。")
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
                logger.info(f"  ➜ 找到本地缺失的第一集: S{s_num}E{e_num} ('{episode.get('name')}'), 将其设为待播集。")
                return episode
        
        # 3. 如果循环完成，说明本地拥有TMDb上所有的剧集
        logger.info("  ➜ 本地媒体库已拥有TMDb上所有剧集，无待播信息。")
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
            logger.warning(f"  ➜ 元数据不完整，以下集缺少简介: {', '.join(missing_overview_episodes)}")
            return False
        
        logger.info("  ➜ 元数据完整性检查通过，所有集都有简介。")
        return True
