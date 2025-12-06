# watchlist_processor.py

import time
import json
import os
import concurrent.futures
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
import threading

# 导入我们需要的辅助模块
from database import connection, media_db, request_db, watchlist_db, user_db
import constants
import handler.tmdb as tmdb
import handler.emby as emby
import handler.telegram as telegram
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
        """ 低频扫描所有已完结剧集，全量刷新元数据，发现即将播出的新季并预订。"""
        self.progress_callback = progress_callback
        task_name = "已完结剧集新季预定"
        self.progress_callback(0, "准备开始预定检查...")
        try:
            completed_series = self._get_series_to_process(f"WHERE watching_status = '{STATUS_COMPLETED}' AND force_ended = FALSE")
            total = len(completed_series)
            if not completed_series:
                self.progress_callback(100, "没有需要检查的已完结剧集。")
                return

            logger.info(f"开始低频检查 {total} 部已完结剧集是否有新季上线 (含全量元数据刷新)...")
            self.progress_callback(10, f"发现 {total} 部已完结剧集，开始检查...")
            revived_count = 0
            today = datetime.now(timezone.utc).date()

            for i, series in enumerate(completed_series):
                if self.is_stop_requested(): break
                progress = 10 + int(((i + 1) / total) * 90)
                series_name = series['item_name']
                tmdb_id = series['tmdb_id']
                emby_ids = series.get('emby_item_ids_json', [])
                item_id = emby_ids[0] if emby_ids else None
                
                self.progress_callback(progress, f"刷新并检查: {series_name[:20]}... ({i+1}/{total})")

                # ★★★ 调用通用辅助函数刷新元数据 ★★★
                # 这会自动更新 DB、JSON 和 Emby
                refresh_result = self._refresh_series_metadata(tmdb_id, series_name, item_id)
                
                if not refresh_result:
                    continue # 刷新失败，跳过本剧集
                
                tmdb_details, _, _ = refresh_result

                # --- 以下是新季判断逻辑 ---
                last_episode_info = series.get('last_episode_to_air_json')
                old_season_number = 0
                if last_episode_info and isinstance(last_episode_info, dict):
                    old_season_number = last_episode_info.get('season_number', 0)

                new_total_seasons = tmdb_details.get('number_of_seasons', 0)

                if new_total_seasons > old_season_number:
                    new_season_to_check_num = old_season_number + 1
                    # 获取新季详情 (虽然 _refresh_series_metadata 已经获取过并存了 JSON，但为了逻辑清晰，这里再调一次 API 或者读缓存皆可)
                    # 考虑到 _refresh_series_metadata 已经缓存了 season-X.json，这里其实可以读本地，但为了代码简单，直接调 tmdb 库
                    season_details = tmdb.get_tv_season_details(tmdb_id, new_season_to_check_num, self.tmdb_api_key)
                    
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
                                    'parent_series_tmdb_id': tmdb_id,
                                    'overview': season_details.get('overview')
                                }
                                
                                # 2. 调用 request_db 将其状态设置为 PENDING_RELEASE
                                request_db.set_media_status_pending_release(
                                    tmdb_ids=season_tmdb_id,
                                    item_type='Season',
                                    source={"type": "watchlist", "reason": "revived_season", "item_id": tmdb_id},
                                    media_info_list=[media_info]
                                )
                                logger.info(f"  ➜ 已成功为《{series_name}》 S{new_season_to_check_num} 创建“待上映”订阅。")

                                # 3. 立即更新本地数据库状态为“追剧中” 
                                updates = {
                                    "is_airing": True,
                                    "force_ended": False, # 核心：移除强制完结标记
                                    "tmdb_status": "Returning Series"
                                }

                                if days_until_air <= 3:
                                    updates["status"] = STATUS_WATCHING
                                    updates["paused_until"] = None
                                    log_status = "追剧中 (Watching)"
                                else:
                                    updates["status"] = STATUS_PAUSED
                                    updates["paused_until"] = air_date.isoformat()
                                    log_status = f"已暂停 (Paused) 至 {air_date_str}"

                                self._update_watchlist_entry(tmdb_id, series_name, updates)
                                watchlist_db.sync_seasons_watching_status(tmdb_id, [new_season_to_check_num], updates["status"])
                                
                                logger.info(f"  ➜ 已成功复活《{series_name}》：状态更新为 '{log_status}'，并已提交 S{new_season_to_check_num} 的订阅请求。")

                        except ValueError:
                            logger.warning(f"  ➜ 解析《{series_name}》新季的播出日期 '{air_date_str}' 失败。")
                
                time.sleep(1) # 保持适当的API请求间隔
            
            final_message = f"复活检查完成。共刷新 {total} 部剧集，复活 {revived_count} 部。"
            self.progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    def _get_series_to_process(self, where_clause: str, tmdb_id: Optional[str] = None, include_all_series: bool = False) -> List[Dict[str, Any]]:
        """
        【V5 - 数据库直通版】
        - 单项刷新：直接查 DB。
        - 批量刷新：直接调用 DB 函数，支持 SQL 级媒体库过滤，移除 Emby API 调用。
        """
        
        # 1. 单项刷新逻辑 (保持不变)
        if tmdb_id:
            try:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # 这里需要手动写全字段，或者复用 watchlist_db 的逻辑，为了简单保持原样
                    base_query = """
                        SELECT 
                            tmdb_id, title AS item_name, watching_status AS status,
                            emby_item_ids_json, force_ended, paused_until,
                            last_episode_to_air_json, watchlist_tmdb_status AS tmdb_status,
                            watchlist_missing_info_json AS missing_info_json, subscription_status
                        FROM media_metadata
                        WHERE item_type = 'Series' AND tmdb_id = %s
                    """
                    cursor.execute(base_query, (tmdb_id,))
                    result = [dict(row) for row in cursor.fetchall()]
                    if not result:
                        logger.warning(f"  ➜ 数据库中未找到 TMDb ID 为 {tmdb_id} 的追剧记录。")
                    return result
            except Exception as e:
                logger.error(f"为 tmdb_id {tmdb_id} 获取追剧信息时发生数据库错误: {e}", exc_info=True)
                return []

        # 2. 批量刷新逻辑 (优化后)
        selected_libraries = self.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
        
        # 构建 SQL 条件片段
        conditions = []
        
        # 处理 include_all_series 逻辑
        if not include_all_series:
            conditions.append("watching_status != 'NONE'")
            
        # 处理传入的 where_clause (例如: "WHERE watching_status = 'Watching'")
        if where_clause:
            # 去掉 "WHERE" 前缀，只保留条件部分
            clean_clause = where_clause.replace('WHERE', '', 1).strip()
            if clean_clause:
                conditions.append(clean_clause)
        
        final_condition_sql = " AND ".join(conditions) if conditions else ""

        if selected_libraries:
            logger.info(f"  ➜ 已启用媒体库过滤器 ({len(selected_libraries)} 个库)，正在数据库中筛选...")
        
        # ★★★ 核心调用：直接使用 DB 函数进行筛选 ★★★
        return watchlist_db.get_series_by_dynamic_condition(
            condition_sql=final_condition_sql,
            library_ids=selected_libraries
        )

    def _save_local_json(self, relative_path: str, new_data: Dict[str, Any]):
        """
        保存数据到本地 JSON 缓存文件 (智能合并模式)。
        - ★★★ 智能保护：'series.json' 不更新 'name'，但 'season-*.json' 会更新 'name'。
        """
        if not self.local_data_path:
            return

        full_path = os.path.join(self.local_data_path, relative_path)
        filename = os.path.basename(full_path)
        
        # ★★★ 关键检查：如果文件不存在，直接放弃，绝不创建“残缺”文件 ★★★
        if not os.path.exists(full_path):
            logger.trace(f"  ➜ 本地缓存文件不存在，跳过更新: {filename}")
            return

        try:
            # 读取现有文件
            with open(full_path, 'r', encoding='utf-8') as f:
                final_data = json.load(f)

            # 定义要更新的字段 (TMDb 字段 -> JSON 字段)
            fields_to_update = {
                "overview": "overview",           # 简介：TMDb 更新最快
                "poster_path": "poster_path",     # 海报路径
                "backdrop_path": "backdrop_path", # 背景图路径
                "still_path": "still_path",       # 剧照路径
                "first_air_date": "release_date", # 首播日期 (Series)
                "air_date": "release_date"        # 播出日期 (Episode/Season)
            }

            # 差异化保护：只有非 series.json 才允许更新标题
            if 'series.json' not in filename:
                fields_to_update["name"] = "name"

            # 执行合并更新
            updated = False
            for tmdb_key, json_key in fields_to_update.items():
                if tmdb_key in new_data and new_data[tmdb_key] is not None:
                    # 只有值真的变了才更新，减少文件IO
                    if final_data.get(json_key) != new_data[tmdb_key]:
                        final_data[json_key] = new_data[tmdb_key]
                        updated = True

            # 只有发生变更时才写入
            if updated:
                with open(full_path, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)
                logger.debug(f"  ➜ 已刷新本地元数据: {filename}")
            
        except Exception as e:
            logger.error(f"更新本地缓存文件失败: {full_path}, 错误: {e}")

    # --- 通用的元数据刷新辅助函数 ---
    def _refresh_series_metadata(self, tmdb_id: str, item_name: str, item_id: Optional[str]) -> Optional[tuple]:
        """
        通用辅助函数：
        1. 获取 TMDb 最新剧集详情
        2. 更新本地 JSON 缓存
        3. 更新数据库基础字段 (Series)
        4. 通知 Emby 刷新元数据
        5. ★★★ 同步所有季和集的元数据到数据库 (Seasons & Episodes) ★★★
        
        返回: (latest_series_data, all_tmdb_episodes, emby_seasons_state) 或 None
        """
        if not self.tmdb_api_key:
            logger.warning("  ➜ 未配置TMDb API Key，跳过元数据刷新。")
            return None

        # 1. 从TMDb获取最新元数据
        latest_series_data = tmdb.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not latest_series_data:
            logger.error(f"  🚫 无法获取 '{item_name}' 的TMDb详情，元数据刷新中止。")
            return None
        
        # 2. 将 TMDb 最新数据合并写入本地 JSON (series.json) 
        self._save_local_json(f"override/tmdb-tv/{tmdb_id}/series.json", latest_series_data)

        # 3. 将 TMDb 最新数据写入数据库 (Series 层级)
        series_updates = {
            "original_title": latest_series_data.get("original_name"),
            "overview": latest_series_data.get("overview"),
            "poster_path": latest_series_data.get("poster_path"),
            "release_date": latest_series_data.get("first_air_date") or None,
            "original_language": latest_series_data.get("original_language"),
            "watchlist_tmdb_status": latest_series_data.get("status"),
            "total_episodes": latest_series_data.get("number_of_episodes", 0)
        }
        media_db.update_media_metadata_fields(tmdb_id, 'Series', series_updates)

        # 4. 获取所有季和集的数据
        all_tmdb_episodes = []
        tmdb_seasons = latest_series_data.get("seasons", [])
        
        for season_summary in tmdb_seasons:
            season_num = season_summary.get("season_number")
            if season_num is None or season_num == 0: continue
            
            # 获取分季详情
            season_details = tmdb.get_season_details_tmdb(tmdb_id, season_num, self.tmdb_api_key)
            
            if season_details:
                # 本地 JSON 缓存
                self._save_local_json(f"override/tmdb-tv/{tmdb_id}/season-{season_num}.json", season_details)

                if season_details.get("episodes"):
                    all_tmdb_episodes.extend(season_details.get("episodes", []))
                    
                    for ep in season_details["episodes"]:
                        ep_num = ep.get("episode_number")
                        if ep_num is not None:
                            self._save_local_json(
                                f"override/tmdb-tv/{tmdb_id}/season-{season_num}-episode-{ep_num}.json", 
                                ep
                            )
            time.sleep(0.1)

        # 5. 通知 Emby 刷新元数据 (让 Emby 也就是本地文件系统先更新)
        if item_id:
            emby.refresh_emby_item_metadata(
                item_emby_id=item_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id_for_ops=self.emby_user_id,
                replace_all_metadata_param=True,
                item_name_for_log=item_name
            )

        # 6. ★★★ 核心修复：同步季和集到数据库 ★★★
        # 先获取本地 Emby 的状态（因为刚才刷新了 Emby，现在获取的是最新的本地状态）
        emby_seasons_state = media_db.get_series_local_children_info(tmdb_id)
        
        try:
            # 将 TMDb 的全量数据 + 本地 Emby 的存在状态，同步写入 media_metadata 表
            media_db.sync_series_children_metadata(
                parent_tmdb_id=tmdb_id,
                seasons=tmdb_seasons,
                episodes=all_tmdb_episodes,
                local_in_library_info=emby_seasons_state
            )
            logger.debug(f"  ➜ 已同步 '{item_name}' 的季/集元数据到数据库。")
        except Exception as e_sync:
            logger.error(f"  ➜ 同步 '{item_name}' 子项目数据库时出错: {e_sync}", exc_info=True)
        
        # 返回 emby_seasons_state 供后续逻辑使用，避免重复查询
        return latest_series_data, all_tmdb_episodes, emby_seasons_state

    # ★★★ 核心处理逻辑：单个剧集的所有操作在此完成 ★★★
    def _process_one_series(self, series_data: Dict[str, Any]):
        tmdb_id = series_data['tmdb_id']
        emby_ids = series_data.get('emby_item_ids_json', [])
        item_id = emby_ids[0] if emby_ids else None
        item_name = series_data['item_name']
        is_force_ended = bool(series_data.get('force_ended', False))
        
        logger.info(f"  ➜ 【追剧检查】正在处理: '{item_name}' (TMDb ID: {tmdb_id})")

        if not item_id:
            logger.warning(f"  ➜ 剧集 '{item_name}' 在数据库中没有关联的 Emby ID，跳过。")
            return

        # 调用通用辅助函数刷新元数据
        refresh_result = self._refresh_series_metadata(tmdb_id, item_name, item_id)
        if not refresh_result:
            return # 刷新失败，中止后续逻辑
        
        latest_series_data, all_tmdb_episodes, emby_seasons = refresh_result

        # 计算状态和缺失信息
        new_tmdb_status = latest_series_data.get("status")
        is_ended_on_tmdb = new_tmdb_status in ["Ended", "Canceled"]
        
        # 依然计算缺失信息，用于后续的“补旧番”订阅，但不影响状态判定
        real_next_episode_to_air = self._calculate_real_next_episode(all_tmdb_episodes, emby_seasons)
        missing_info = self._calculate_missing_info(latest_series_data.get('seasons', []), all_tmdb_episodes, emby_seasons)
        has_missing_media = bool(missing_info["missing_seasons"] or missing_info["missing_episodes"])

         # 1. 第一步：必须先定义 today，否则后面计算日期差会报错
        today = datetime.now(timezone.utc).date()

        # 2. 第二步：获取上一集信息
        last_episode_to_air = latest_series_data.get("last_episode_to_air")
        
        # 3. 第三步：计算距离上一集播出的天数 (依赖 today)
        days_since_last = 9999 # 默认给一个很大的值
        if last_episode_to_air and (last_date_str := last_episode_to_air.get('air_date')):
            try:
                last_air_date_obj = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                days_since_last = (today - last_air_date_obj).days
            except ValueError:
                pass
        final_status = STATUS_WATCHING 
        paused_until_date = None

        # 预处理：确定是否存在一个“有效的、未来的”下一集
        effective_next_episode = None
        effective_next_episode_air_date = None
        if real_next_episode_to_air and (air_date_str := real_next_episode_to_air.get('air_date')):
            try:
                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                if air_date >= today:
                    effective_next_episode = real_next_episode_to_air
                    effective_next_episode_air_date = air_date 
            except (ValueError, TypeError):
                pass

        # 预处理：检查是否为本季大结局
        is_season_finale = False
        last_date_str = None # 用于日志
        if last_episode_to_air:
            last_date_str = last_episode_to_air.get('air_date')
            last_s_num = last_episode_to_air.get('season_number')
            last_e_num = last_episode_to_air.get('episode_number')
            
            if last_s_num and last_e_num:
                season_info = next((s for s in latest_series_data.get('seasons', []) if s.get('season_number') == last_s_num), None)
                if season_info:
                    total_ep_count = season_info.get('episode_count', 0)
                    # 只有当该季总集数大于5时，才敢断定这是大结局。
                    # 如果总集数为1，极大概率是新季刚开播 TMDb 还没更新后续集数，
                    # 此时应跳过大结局判定，让其落入后续的“最近播出”逻辑保持 Watching 状态。
                    if total_ep_count > 1 and last_e_num >= total_ep_count:
                        is_season_finale = True

        # ==============================================================================
        # ★★★ 重构后的状态判定逻辑 ★★★
        # ==============================================================================

        # 规则 1: TMDb 状态已完结 -> 直接完结 (不考虑本地是否集齐)
        if is_ended_on_tmdb:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.info(f"  🏁 [判定-规则1] TMDb状态为 '{new_tmdb_status}'，判定为“已完结”。")

        # 规则 2: 本季大结局已播出 (且无明确下一集) -> 直接完结 (不考虑本地是否集齐)
        elif is_season_finale and not effective_next_episode:
            # 定义：是否为“疑似数据缺失”的短季
            # 如果是连载剧，且当前季总集数 <= 3，极大概率是 TMDb 还没更新后续集数
            is_suspiciously_short = (new_tmdb_status == "Returning Series" and total_ep_count <= 3)
            
            # 场景 A: 连载剧 + 集数很少 + 最近7天播出 -> 认为是数据滞后，保持追剧
            if is_suspiciously_short and days_since_last <= 7:
                final_status = STATUS_WATCHING
                paused_until_date = None
                logger.info(f"  🛡️ [安全锁生效] 虽检测到疑似大结局 (S{last_s_num}E{last_e_num})，但该季仅 {total_ep_count} 集且刚播出 {days_since_last} 天，判定为数据滞后，保持“追剧中”。")
            
            # 场景 B: 其他情况 (明确已完结 / 集数很多 / 播出很久) -> 判定完结
            else:
                final_status = STATUS_COMPLETED
                paused_until_date = None
                logger.info(f"  🏁 [判定-规则2] 本季大结局 (S{last_s_num}E{last_e_num}) 已播出，判定为“已完结”。")

        # 规则 3: 连载中逻辑 (保持原有逻辑)
        else:
            # 情况 A: 下一集有明确播出日期
            if effective_next_episode:
                air_date = effective_next_episode_air_date
                days_until_air = (air_date - today).days
                episode_number = effective_next_episode.get('episode_number')
                season_number = effective_next_episode.get('season_number')

                # 子规则 A: 下一集是新季第一集 且 日期在一个月(30天)以后 -> 判定当前季完结
                if episode_number == 1 and days_until_air > 30:
                    final_status = STATUS_COMPLETED
                    paused_until_date = None
                    logger.info(f"  🔄 [判定-连载中] 下一集 (S{season_number}E{episode_number}) 是新季首播且在 {days_until_air} 天后 (>30天) 播出，判定当前季已完结。")
                
                # 子规则 B: 3天内就要播出 (或已播出但未下载) -> 设为“追剧中”
                elif days_until_air <= 3:
                    final_status = STATUS_WATCHING
                    paused_until_date = None
                    logger.info(f"  👀 [判定-连载中] 下一集 (S{season_number}E{episode_number}) 即将在 {days_until_air} 天内播出 (或已播出)，保持“追剧中”。")

                # 子规则 C: 还有很久才播出 -> 暂停至播出日期
                else:
                    final_status = STATUS_PAUSED
                    paused_until_date = air_date 
                    logger.info(f"  ⏸️ [判定-连载中] 下一集 (S{season_number}E{episode_number}) 将在 {days_until_air} 天后 ({air_date}) 播出，暂停至该日期。")

            # 情况 B: 无下一集信息 (或信息不全)
            else:
                if days_since_last != 9999:
                    
                    # 子规则 A: 距上一集播出超过一个月(30天) -> 判定已完结
                    if days_since_last > 30:
                        final_status = STATUS_COMPLETED
                        paused_until_date = None
                        logger.info(f"  🔄 [判定-连载中] 无待播集信息，且上一集已播出 {days_since_last} 天 (>30天)，判定已完结。")
                    
                    # 子规则 B: 距上一集播出在一个月内 -> 保持追剧
                    else:
                        final_status = STATUS_WATCHING
                        paused_until_date = None
                        logger.info(f"  👀 [判定-连载中] 无待播集信息，但上一集仅播出 {days_since_last} 天 (<=30天)，保持“追剧中”。")

                        # 停更报警逻辑
                        if days_since_last > 8:
                            logger.info(f"  🔔 [通知] 剧集 '{item_name}' 停更已满一周，正在发送管理员通知...")
                            try:
                                admin_ids = user_db.get_admin_telegram_chat_ids()
                                if admin_ids:
                                    safe_name = telegram.escape_markdown(item_name)
                                    raw_date_line = f"{last_date_str} ({days_since_last}天前)"
                                    safe_date_line = telegram.escape_markdown(raw_date_line)
                                    msg_text = (
                                        f"⚠️ *追剧停更预警*\n\n"
                                        f"📺 *剧集*: {safe_name}\n"
                                        f"📅 *上一集*: {safe_date_line}\n"
                                        f"❓ *状态*: TMDb无后续排期\n\n"
                                        f"该剧已停更超过一周且无新数据，请人工检查是否已完结\\."
                                    )
                                    for admin_id in admin_ids:
                                        telegram.send_telegram_message(admin_id, msg_text)
                            except Exception as e:
                                logger.error(f"  ❌ 发送停更通知失败: {e}")
                else:
                    # 极端情况：无任何日期信息
                    final_status = STATUS_WATCHING
                    paused_until_date = None
                    logger.info(f"  👀 [判定-连载中] 缺乏播出日期数据，默认保持“追剧中”状态。")

        # ==============================================================================

        # 手动强制完结
        if is_force_ended and final_status != STATUS_COMPLETED:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.warning(f"  🔄 [强制完结生效] 最终状态被覆盖为 '已完结'。")

        # 只有当内部状态是“追剧中”或“已暂停”时，才认为它在“连载中”
        is_truly_airing = final_status in [STATUS_WATCHING, STATUS_PAUSED]
        logger.info(f"  ➜ 最终判定 '{item_name}' 的真实连载状态为: {is_truly_airing} (内部状态: {translate_internal_status(final_status)})")

        # 更新追剧数据库
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

        # 更新季的活跃状态
        active_seasons = set()
        # 规则 A: 如果有明确的下一集待播，该集所属的季肯定是活跃的
        if real_next_episode_to_air and real_next_episode_to_air.get('season_number'):
            active_seasons.add(real_next_episode_to_air['season_number'])
        # 规则 B: 如果有缺失的集（补番），这些集所属的季也是活跃的
        if missing_info.get('missing_episodes'):
            for ep in missing_info['missing_episodes']:
                if ep.get('season_number'): active_seasons.add(ep['season_number'])
        # 规则 C: 如果有整季缺失，且该季已播出，也视为活跃
        if missing_info.get('missing_seasons'):
            for s in missing_info['missing_seasons']:
                if s.get('air_date') and s.get('season_number'):
                    try:
                        s_date = datetime.strptime(s['air_date'], '%Y-%m-%d').date()
                        if s_date <= today: active_seasons.add(s['season_number'])
                    except ValueError: pass

        # 调用 DB 模块进行批量更新
        watchlist_db.sync_seasons_watching_status(tmdb_id, list(active_seasons), final_status)

        # ★★★ 场景一：补旧番 - 只处理已完结剧集中，已播出的缺失季 ★★★
        # 注意：由于现在 TMDb Ended 状态会直接导致 final_status = COMPLETED，
        # 所以即使本地缺集，也会进入这个分支，从而正确触发“补旧番”逻辑。
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
                            'tmdb_id': season_tmdb_id, 
                            'item_type': 'Season',     
                            'title': f"{item_name} {season.get('name', f'第 {season_num} 季')}", 
                            'original_title': latest_series_data.get('original_name'),
                            'release_date': season.get('air_date'),
                            'poster_path': season.get('poster_path'),
                            'overview': season.get('overview'), 
                            'season_number': season_num
                        }
                        
                        # 推送需求
                        request_db.set_media_status_wanted(
                            tmdb_ids=str(season.get('id')), 
                            item_type='Season',             
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
                
                air_date_str = season.get('air_date')
                is_pending = False
                if air_date_str:
                    try:
                        air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                        if air_date > today:
                            is_pending = True
                    except ValueError:
                        pass
                
                if is_pending:
                    logger.info(f"  ➜ 发现未上映的缺失季 S{season_num} (播出日期: {air_date_str})，将状态设为 PENDING_RELEASE。")
                    request_db.set_media_status_pending_release(
                        tmdb_ids=season_tmdb_id,
                        item_type='Season',
                        source={"type": "watchlist", "reason": "missing_season", "item_id": item_id},
                        media_info_list=[media_info]
                    )
                else:
                    logger.info(f"  ➜ 发现已上映的缺失季 S{season_num}，将状态设为 WANTED。")
                    request_db.set_media_status_wanted(
                        tmdb_ids=season_tmdb_id,
                        item_type='Season',
                        source={"type": "watchlist", "reason": "missing_season", "item_id": item_id},
                        media_info_list=[media_info]
                    )

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
