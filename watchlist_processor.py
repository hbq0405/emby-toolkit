# watchlist_processor.py

import time
import json
import os
import concurrent.futures
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
import threading

# 导入我们需要的辅助模块
from database import connection, media_db, watchlist_db
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
        """【新架构】将新剧集添加/更新到 media_metadata 表并标记为追剧。"""
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

        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 UPSERT 逻辑
                    sql = """
                        INSERT INTO media_metadata (tmdb_id, item_type, title, watching_status, emby_item_ids_json)
                        VALUES (%s, 'Series', %s, %s, %s)
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            watching_status = EXCLUDED.watching_status,
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
                    cursor.execute(sql, (tmdb_id, item_name, internal_status, json.dumps([item_id])))
                    
                    if cursor.rowcount > 0:
                        log_status_translated = translate_internal_status(internal_status)
                        logger.info(f"  ➜ 剧集 '{item_name}' 已自动加入追剧列表，初始状态为: {log_status_translated}。")
                conn.commit()
        except Exception as e:
            logger.error(f"自动添加剧集 '{item_name}' 到追剧列表时发生数据库错误: {e}", exc_info=True)

    # --- 核心任务启动器 (无需修改) ---
    def run_regular_processing_task_concurrent(self, progress_callback: callable, item_id: Optional[str] = None, force_full_update: bool = False):
        """【V2 - 流程修复版】修复因没有活跃剧集而导致洗版检查被跳过的流程缺陷。"""
        self.progress_callback = progress_callback
        task_name = "并发追剧更新"
        # ▼▼▼ 根据模式更新日志里的任务名 ▼▼▼
        if force_full_update:
            task_name = "并发追剧更新 (深度模式)"
        if item_id: 
            task_name = f"单项追剧更新 (ID: {item_id})"
        
        self.progress_callback(0, "准备检查待更新剧集...")
        try:
            # ======================================================================
            # 阶段一：处理剧集
            # ======================================================================
            
            # ▼▼▼ 核心修改：根据 deep_mode 动态决定要查哪些剧 ▼▼▼
            where_clause = ""
            if force_full_update:
                # 深度模式：查询所有剧集 (除了手动强制完结的)
                where_clause = "WHERE force_ended = FALSE"
                logger.info("  ➜ 已启用【深度模式】，将刷新所有追剧列表中的项目。")
            else:
                # 快速模式 (默认)：只查询活跃剧集
                today_str = datetime.now(timezone.utc).date().isoformat()
                where_clause = f"WHERE watching_status = '{STATUS_WATCHING}' OR (watching_status = '{STATUS_PAUSED}' AND paused_until <= '{today_str}')"

            active_series = self._get_series_to_process(where_clause, item_id)
            
            # ▼▼▼ 核心流程修正：即使没有活跃剧集，也不再提前退出 ▼▼▼
            if active_series:
                total = len(active_series)
                self.progress_callback(5, f"开始并发处理 {total} 部活跃剧集...")
                
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
                        
                        # 进度条只占前50%
                        progress = 5 + int((processed_count / total) * 45)
                        self.progress_callback(progress, f"活跃剧集: {processed_count}/{total} - {series_info['item_name'][:15]}...")
                
                if not self.is_stop_requested():
                    self.progress_callback(50, "常规追剧检查完成，即将开始洗版检查...")
            else:
                # 如果没有活跃剧集，直接进入下一阶段
                self.progress_callback(50, "没有需要立即处理的活跃剧集，直接开始洗版检查...")
            
            time.sleep(2) # 给用户一点时间看消息

            # ======================================================================
            # 阶段二：处理洗版检查 (无论阶段一结果如何，都必须执行)
            # ======================================================================
            if self.is_stop_requested():
                self.progress_callback(100, "任务已停止。")
                return

            # 调用我们新的、可复用的洗版检查函数
            # 如果是单项刷新，把 item_id 也传过去
            self._run_wash_plate_check_logic(progress_callback=self.progress_callback, item_id=item_id)
            # ▲▲▲ 核心流程修正结束 ▲▲▲

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    # ★★★ 专门用于“复活检查”的任务方法 ★★★
    def run_revival_check_task(self, progress_callback: callable):
        """【新架构】检查所有已完结剧集是否“复活”。"""
        self.progress_callback = progress_callback
        task_name = "已完结剧集复活检查"
        self.progress_callback(0, "准备开始复活检查...")
        try:
            completed_series = self._get_series_to_process(f"WHERE watching_status = '{STATUS_COMPLETED}'")
            total = len(completed_series)
            if not completed_series:
                self.progress_callback(100, "没有已完结的剧集需要检查。")
                return

            logger.info(f"开始低频检查 {total} 部已完结剧集是否复活...")
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

                # 保留原有的精准复活逻辑
                should_revive = False
                last_episode_info = series.get('last_episode_to_air_json')
                old_season_number = 0
                if last_episode_info and isinstance(last_episode_info, dict):
                    old_season_number = last_episode_info.get('season_number', 0)

                new_total_seasons = tmdb_details.get('number_of_seasons', 0)

                if new_total_seasons > old_season_number:
                    new_season_to_check_num = old_season_number + 1
                    season_details = tmdb.get_season_details(series['tmdb_id'], new_season_to_check_num, self.tmdb_api_key)
                    if season_details and season_details.get('episodes'):
                        first_episode = season_details['episodes'][0]
                        air_date_str = first_episode.get('air_date')
                        if air_date_str:
                            try:
                                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                                if 0 <= (air_date - today).days <= 3:
                                    should_revive = True
                            except ValueError: pass
                
                if should_revive:
                    revived_count += 1
                    updates_to_db = {
                        "status": STATUS_WATCHING,
                        "paused_until": None,
                        "tmdb_status": tmdb_details.get('status'),
                        "force_ended": False 
                    }
                    # ★★★ 调用适配 ★★★
                    self._update_watchlist_entry(series['tmdb_id'], series_name, updates_to_db)
                
                time.sleep(2)
            
            final_message = f"复活检查完成。共发现 {revived_count} 部剧集回归。"
            self.progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    # ★★★ 已完结剧集缺集洗版检查 ★★★
    def _run_wash_plate_check_logic(self, progress_callback: callable, item_id: Optional[str] = None):
        """【新架构】处理“中间缺集”的洗版逻辑。"""
        task_name = "洗版缺集的季"
        
        if not self.config.get(constants.CONFIG_OPTION_RESUBSCRIBE_COMPLETED_ON_MISSING):
            logger.info(f"'{task_name}' 功能未启用，跳过。")
            if progress_callback: progress_callback(100, "所有流程已完成（洗版功能未启用）。")
            return

        logger.trace(f"  ➜ 后台任务 '{task_name}' 开始执行")
        if progress_callback: progress_callback(0, "正在查找需要洗版的剧集...")

        try:
            series_to_check = []
            if item_id:
                series_to_check = self._get_series_to_process("", item_id=item_id)
            else:
                # 查询逻辑需要适配新表字段名
                stuck_series = self._get_series_to_process(
                    f"""
                    WHERE watching_status IN ('{STATUS_WATCHING}', '{STATUS_PAUSED}')
                      AND watchlist_tmdb_status IN ('Ended', 'Canceled')
                      AND jsonb_typeof(watchlist_missing_info_json) IN ('object', 'array')
                    """
                )
                today_minus_365_days = (datetime.now(timezone.utc).date() - timedelta(days=365)).isoformat()
                zombie_series = self._get_series_to_process(
                    f"""
                    WHERE watching_status IN ('{STATUS_WATCHING}', '{STATUS_PAUSED}')
                      AND watchlist_tmdb_status NOT IN ('Ended', 'Canceled')
                      AND jsonb_typeof(last_episode_to_air_json) = 'object'
                      AND (last_episode_to_air_json->>'air_date')::date < '{today_minus_365_days}'
                    """
                )
                completed_missing_series = self._get_series_to_process(
                    f"WHERE watching_status = '{STATUS_COMPLETED}' AND jsonb_typeof(watchlist_missing_info_json) IN ('object', 'array')"
                )
                all_series_map = {s['item_id']: s for s in stuck_series}
                all_series_map.update({s['item_id']: s for s in zombie_series})
                all_series_map.update({s['item_id']: s for s in completed_missing_series})
                series_to_check = list(all_series_map.values())
            
            total = len(series_to_check)
            if not series_to_check:
                if progress_callback: progress_callback(100, "所有流程已完成，未发现需洗版的剧集。")
                return

            logger.info(f"  ➜ 共发现 {total} 部潜在的缺集剧集，开始进行精准订阅分析...")
            total_seasons_subscribed = 0

            quota_exhausted = False
            for i, series in enumerate(series_to_check):
                if self.is_stop_requested(): break
                item_name = series.get('item_name', '未知剧集')
                
                # 7天宽限期判断保持不变
                if series.get('tmdb_status') in ['Ended', 'Canceled']:
                    last_episode_info = series.get('last_episode_to_air_json')
                    if last_episode_info and isinstance(last_episode_info, dict):
                        last_air_date_str = last_episode_info.get('air_date')
                        if last_air_date_str:
                            try:
                                last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                                days_since_airing = (datetime.now(timezone.utc).date() - last_air_date).days
                                if days_since_airing < 7:
                                    logger.info(f"  ➜ 《{item_name}》完结未满7天，跳过洗版分析。")
                                    continue
                            except ValueError: pass
                
                missing_info = series.get('missing_info_json')
                if not missing_info: continue

                # ▼▼▼ 核心逻辑修正：只分析“中间缺集”，彻底忽略“整季缺失” ▼▼▼
                logger.info(f"  ➜ 开始为《{item_name}》进行精准的“中间缺集”分析...")
                seasons_with_real_gaps = set()

                # 1. 实时获取 Emby 本地分集数据，用于对比
                emby_children = emby.get_series_children(series['item_id'], self.emby_url, self.emby_api_key, self.emby_user_id)
                emby_seasons = {}
                if emby_children:
                    for child in emby_children:
                        if child.get('Type') == 'Episode':
                            s_num, e_num = child.get('ParentIndexNumber'), child.get('IndexNumber')
                            if s_num is not None and e_num is not None:
                                emby_seasons.setdefault(s_num, set()).add(e_num)
                
                # 2. 忽略完全缺失的季。洗版功能只负责填补“中间”的窟窿，
                #    对于尚未播出的新季或完全没有的旧季，应由常规订阅逻辑处理。
                if missing_info.get("missing_seasons"):
                    missing_season_nums = [s.get('season_number') for s in missing_info.get("missing_seasons", []) if s.get('season_number') is not None]
                    if missing_season_nums:
                        logger.info(f"  ➜ 分析：检测到整季缺失 S{missing_season_nums}，根据精准订阅策略，洗版功能将【忽略】此情况。")
                
                # 3. 分析缺失的集，判断是否为“中间”缺失
                for episode in missing_info.get("missing_episodes", []):
                    s_num = episode.get('season_number')
                    e_num = episode.get('episode_number')
                    
                    if s_num is None or e_num is None or s_num in seasons_with_real_gaps:
                        continue

                    local_episodes_for_season = emby_seasons.get(s_num, set())
                    # 如果本地根本没有这一季的任何文件，那它本质上就是“整季缺失”，不归洗版管
                    if not local_episodes_for_season:
                        logger.info(f"  ➜ 分析 S{s_num}E{e_num}: 本地不存在该季的任何文件，视为整季缺失，【忽略】。")
                        continue

                    # 关键判断：本地是否存在比当前缺失集集号更大的集
                    has_later_episode_locally = any(local_e > e_num for local_e in local_episodes_for_season)

                    if has_later_episode_locally:
                        max_local_episode = max(local_episodes_for_season)
                        logger.info(f"  ➜ 分析 S{s_num}E{e_num}: 本地存在更高集号 S{s_num}E{max_local_episode}，确认为【中间缺失】，需要标记。")
                        seasons_with_real_gaps.add(s_num)
                    else:
                        max_local_episode = max(local_episodes_for_season) if local_episodes_for_season else '无'
                        logger.info(f"  ➜ 分析 S{s_num}E{e_num}: 本地不存在更高集号 (最高为 E{max_local_episode})，判定为【末尾缺失】，【忽略】。")
                

                if not seasons_with_real_gaps:
                    logger.info(f"  ➜ 《{item_name}》分析完成，未发现需要洗版的中间缺失季。")
                    continue

                # 4. 将分析结果写入数据库进行标记
                final_seasons_to_mark = set()
                resubscribe_info = series.get('resubscribe_info_json') or {}
                cooldown_hours = 24  # 冷却时间（小时）

                for season_num in seasons_with_real_gaps:
                    last_subscribed_str = resubscribe_info.get(str(season_num))
                    if last_subscribed_str:
                        try:
                            last_subscribed_time = datetime.fromisoformat(last_subscribed_str.replace('Z', '+00:00'))
                            if datetime.now(timezone.utc) < last_subscribed_time + timedelta(hours=cooldown_hours):
                                logger.info(f"  ➜ 《{item_name}》第 {season_num} 季虽有缺集，但在 {cooldown_hours} 小时冷却期内，暂不标记。")
                                continue  # 跳过这个季，不把它加入最终的标记列表
                        except (ValueError, TypeError):
                            pass  # 如果时间戳格式错误，则忽略冷却，继续标记

                    # 如果没有冷却记录或冷却已过，则加入最终待标记列表
                    final_seasons_to_mark.add(season_num)


                # 4. 将【过滤后】的分析结果与数据库中的现有标记进行比较和更新
                existing_gaps = set(missing_info.get('seasons_with_gaps', []))
                if final_seasons_to_mark != existing_gaps:
                    missing_info['seasons_with_gaps'] = sorted(list(final_seasons_to_mark))
                    
                    self._update_watchlist_entry(
                        tmdb_id=series['tmdb_id'], # ★★★ 调用适配
                        item_name=item_name,
                        updates={"missing_info_json": json.dumps(missing_info)}
                    )
                
                time.sleep(1)

            final_message = "所有流程已完成！洗版检查结束。"
            if progress_callback: progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            if progress_callback: progress_callback(-1, f"错误: {e}")
        finally:
            if progress_callback: self.progress_callback = None

    def _get_series_to_process(self, where_clause: str, item_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """【新架构】从 media_metadata 获取需要处理的剧集列表，并兼容 Emby 库过滤。"""
        
        base_query = """
            SELECT 
                tmdb_id,
                title AS item_name,
                watching_status AS status,
                emby_item_ids_json->>0 AS item_id, -- 提取第一个 Emby ID 作为主 ID
                emby_item_ids_json,
                force_ended,
                paused_until,
                last_episode_to_air_json,
                watchlist_tmdb_status AS tmdb_status,
                watchlist_missing_info_json AS missing_info_json
            FROM media_metadata
        """
        
        # 规则1：如果指定了单个 item_id (Emby ID)，则用 JSONB 查询
        if item_id:
            try:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # 使用 JSONB 包含操作符 @>
                    query = f"{base_query} WHERE item_type = 'Series' AND emby_item_ids_json @> %s::jsonb"
                    cursor.execute(query, (json.dumps([item_id]),))
                    return [dict(row) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"为 item_id {item_id} 获取追剧信息时发生数据库错误: {e}")
                return []

        # 规则2：获取配置中的媒体库列表
        selected_libraries = self.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
        
        # 规则3：如果未选择任何媒体库，则直接查询数据库
        if not selected_libraries:
            try:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # 附加 WHERE 条件
                    final_where = f"WHERE item_type = 'Series' AND watching_status != 'NONE'"
                    if where_clause:
                        final_where += f" AND ({where_clause.replace('WHERE', '').strip()})"
                    
                    query = f"{base_query} {final_where}"
                    cursor.execute(query)
                    return [dict(row) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"获取全部追剧列表时发生数据库错误: {e}")
                return []

        # --- 核心过滤逻辑 ---
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
                final_where = f"WHERE item_type = 'Series' AND watching_status != 'NONE'"
                if where_clause:
                    final_where += f" AND ({where_clause.replace('WHERE', '').strip()})"
                
                query = f"{base_query} {final_where}"
                cursor.execute(query)
                all_candidate_series = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"过滤前获取追剧列表时发生数据库错误: {e}")
            return []
            
        # 在内存中进行最终匹配
        final_series_to_process = [
            series for series in all_candidate_series 
            if series['item_id'] in valid_series_ids_from_emby
        ]
        
        logger.info(f"  ➜ 媒体库过滤完成：数据库中发现 {len(all_candidate_series)} 个候选项目，最终匹配到 {len(final_series_to_process)} 个。")
        
        return final_series_to_process
            
    # ★★★ 核心处理逻辑：单个剧集的所有操作在此完成 ★★★
    def _process_one_series(self, series_data: Dict[str, Any]):
        # ★★★ 核心修改：现在主键是 tmdb_id，item_id 仅用于 Emby API 调用 ★★★
        tmdb_id = series_data['tmdb_id']
        item_id = series_data.get('item_id') # 可能为 None，但后续有检查
        item_name = series_data['item_name']
        is_force_ended = bool(series_data.get('force_ended', False))
        
        logger.info(f"  ➜ 【追剧检查】正在处理: '{item_name}' (TMDb ID: {tmdb_id})")

        # 步骤1: 存活检查 (如果 item_id 存在)
        if item_id:
            item_details_for_check = emby.get_emby_item_details(
                item_id=item_id, emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id, fields="Id,Name"
            )
            if not item_details_for_check:
                logger.warning(f"  ➜ 剧集 '{item_name}' (Emby ID: {item_id}) 在 Emby 中已不存在。将从追剧列表移除。")
                # 使用 watchlist_db 中的函数，它已经被适配了新表
                watchlist_db.remove_item_from_watchlist(tmdb_id=tmdb_id)
                return 
        else:
            logger.warning(f"  ➜ 剧集 '{item_name}' 在数据库中没有关联的 Emby ID，跳过存活检查。")


        if not self.tmdb_api_key:
            logger.warning("  ➜ 未配置TMDb API Key，跳过。")
            return

        # 步骤2: 从TMDb获取权威数据
        logger.debug(f"  ➜ 正在从TMDb API获取 '{item_name}' 的最新详情...")
        latest_series_data = tmdb.get_tv_details(tmdb_id, self.tmdb_api_key)
        if not latest_series_data:
            logger.error(f"  ➜ 无法获取 '{item_name}' 的TMDb详情，本次处理中止。")
            return
        
        all_tmdb_episodes = []
        for season_summary in latest_series_data.get("seasons", []):
            season_num = season_summary.get("season_number")
            if season_num is None or season_num == 0: continue
            season_details = tmdb.get_season_details_tmdb(tmdb_id, season_num, self.tmdb_api_key)
            if season_details and season_details.get("episodes"):
                all_tmdb_episodes.extend(season_details.get("episodes", []))
            time.sleep(0.1)

        # 步骤3: 获取Emby本地数据
        emby_children = emby.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields="Id,Name,ParentIndexNumber,IndexNumber,Type,Overview")
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

        # ★★★ 元数据完整性检查 ★★★
        today_str = datetime.now(timezone.utc).date().isoformat()
        aired_episodes = [ep for ep in all_tmdb_episodes if ep.get('air_date') and ep['air_date'] <= today_str]
        has_complete_metadata = self._check_all_episodes_have_overview(aired_episodes)

        # “本季大结局”判断逻辑
        last_episode_to_air = latest_series_data.get("last_episode_to_air")
        final_status = STATUS_WATCHING # 默认是追剧中
        paused_until_date = None
        today = datetime.now(timezone.utc).date()

        # 规则1：硬性完结条件 (TMDb官方说它完了)
        if is_ended_on_tmdb and has_complete_metadata:
            final_status = STATUS_COMPLETED
            logger.info(f"  ➜ 剧集在TMDb已完结且元数据完整，状态变更为: {translate_internal_status(final_status)}")

        # 规则2：有明确的下一集播出日期 (最复杂的决策树)
        elif real_next_episode_to_air and real_next_episode_to_air.get('air_date'):
            air_date_str = real_next_episode_to_air['air_date']
            try:
                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                days_until_air = (air_date - today).days
                episode_number = real_next_episode_to_air.get('episode_number')

                if days_until_air <= 3:
                    final_status = STATUS_WATCHING
                    logger.info(f"  ➜ 下一集在3天内播出或已播出，状态保持为: {translate_internal_status(final_status)}。")
                
                elif 3 < days_until_air <= 90:
                    # ★★★ 核心判断：这是新一季的开端，还是季中的普通一集？ ★★★
                    if episode_number is not None and int(episode_number) == 1:
                        # 是新一季的第一集，说明当前季已完结，应进入待回归状态
                        final_status = STATUS_COMPLETED
                        paused_until_date = None
                        logger.warning(f"  ➜ 下一集是新季首播 (S{real_next_episode_to_air.get('season_number')}E01)，在 {days_until_air} 天后播出。当前季已完结，状态变更为“已完结”。")
                    else:
                        # 不是第一集，说明是正常的季内停播（如周播剧的间歇）
                        final_status = STATUS_PAUSED
                        paused_until_date = air_date - timedelta(days=1)
                        logger.info(f"  ➜ 下一集 (非首集) 在 {days_until_air} 天后播出，状态变更为: {translate_internal_status(final_status)}，暂停至 {paused_until_date}。")
                
                else: # days_until_air > 90
                    # 播出时间太遥远，无论如何都应视为已完结/长期停播
                    final_status = STATUS_COMPLETED
                    paused_until_date = None
                    logger.warning(f"  ➜ 下一集在 {days_until_air} 天后播出，超过90天阈值，状态强制变更为“已完结”。")

            except (ValueError, TypeError):
                final_status = STATUS_COMPLETED
                logger.warning(f"  ➜ 解析待播日期 '{air_date_str}' 失败，状态强制变更为“已完结”。")

        # 规则3：没有下一集信息，启用30天规则
        elif last_episode_to_air and last_episode_to_air.get('air_date'):
            try:
                last_air_date = datetime.strptime(last_episode_to_air['air_date'], '%Y-%m-%d').date()
                days_since_last_air = (today - last_air_date).days
                
                if days_since_last_air > 30:
                    final_status = STATUS_COMPLETED
                    paused_until_date = None
                    logger.warning(f"  ➜ 剧集无待播信息，且最后一集播出已超过30天，状态强制变更为“已完结”。")
                else:
                    final_status = STATUS_PAUSED
                    paused_until_date = today + timedelta(days=7)
                    logger.info(f"  ➜ 剧集无待播信息，但上一集在30天内播出，临时暂停7天以待数据更新。")
            except ValueError:
                final_status = STATUS_PAUSED
                paused_until_date = today + timedelta(days=7)
                logger.warning(f"  ➜ 剧集上次播出日期格式错误，为安全起见，执行默认的7天暂停。")

        # 规则4：绝对的后备方案 (如果连上一集信息都没有)
        else:
            final_status = STATUS_PAUSED
            paused_until_date = today + timedelta(days=7)
            logger.info(f"  ➜ 剧集完全缺失播出日期数据，为安全起见，执行默认的7天暂停以待数据更新。")

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
                            'poster_path': season.get('poster_path') or latest_series_data.get('poster_path'),
                            'overview': season.get('overview'), 
                            'season_number': season_num
                        }
                        
                        # 推送需求
                        media_db.update_subscription_status(
                            tmdb_ids=str(season.get('id')), # ★★★ 核心修正：使用季的真实 TMDB ID ★★★
                            item_type='Season',             # ★★★ 核心修正：类型明确为 Season ★★★
                            new_status='WANTED',
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

                media_info = {
                    'tmdb_id': tmdb_id, 'item_type': 'Series',
                    'title': f"{item_name} 第 {season_num} 季",
                    'original_title': latest_series_data.get('original_name'),
                    'release_date': season.get('air_date'),
                    'poster_path': season.get('poster_path') or latest_series_data.get('poster_path'),
                    'overview': season.get('overview'), 'season_number': season_num
                }
                
                media_db.update_subscription_status(
                    tmdb_ids=tmdb_id, item_type='Series', new_status='WANTED',
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
                        logger.info(f"  ➜ 发现分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 缺少简介，准备从TMDb注入...")
                        data_to_inject = {
                            "Name": tmdb_data_for_episode.get("name"),
                            "Overview": overview
                        }
                        
                        success = emby.update_emby_item_details(
                            item_id=emby_episode_id,
                            new_data=data_to_inject,
                            emby_server_url=self.emby_url,
                            emby_api_key=self.emby_api_key,
                            user_id=self.emby_user_id
                        )

                        if success:
                            logger.info(f"  ➜ Emby 分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 简介更新成功。")
                            # ★★★ 核心修改：同步更新内存中的数据，为稍后的步骤6做准备 ★★★
                            emby_episode['Overview'] = overview
                            emby_episode['Name'] = data_to_inject.get("Name")
                        else:
                            logger.error(f"  ➜ 更新 Emby 分集 '{ep_name_for_log}' (ID: {emby_episode_id}) 简介失败。")
                    else:
                        logger.info(f"  ➜ TMDb中分集 '{ep_name_for_log}' 尚无简介，跳过更新。")
                else:
                    logger.warning(f"  ➜ Emby分集 '{ep_name_for_log}' 缺少简介，但在TMDb中未找到对应信息。")
        else:
            # 这个else属于for循环，表示循环正常结束，没有被break
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
                logger.info("  ➜ 追剧列表更新任务被中止。")
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