# watchlist_processor.py

import time
import json
import os
import concurrent.futures
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import threading
from collections import defaultdict
# 导入我们需要的辅助模块
from database import connection, media_db, request_db, watchlist_db, user_db, settings_db
import constants
import utils
from ai_translator import AITranslator
import handler.tmdb as tmdb
import handler.emby as emby
import handler.moviepilot as moviepilot
import tasks.helpers as helpers
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
    'Completed': '已完结',
    'Pending': '待定中'
}
# ★★★ 定义状态常量，便于维护 ★★★
STATUS_WATCHING = 'Watching'
STATUS_PAUSED = 'Paused'
STATUS_COMPLETED = 'Completed'
STATUS_PENDING = 'Pending'
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
        self.ai_enabled = self.config.get("ai_translate_episode_overview", False)
        self.ai_translator = AITranslator(self.config) if self.ai_enabled else None
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
        """【新架构】直接调用 DB 层更新，不再做字段映射。"""
        try:
            watchlist_db.update_watchlist_metadata(tmdb_id, updates)
            logger.info(f"  ➜ 成功更新数据库中 '{item_name}' 的追剧信息。")
        except Exception as e:
            logger.error(f"  更新 '{item_name}' 追剧信息时出错: {e}")

    # ★★★ 核心修改 2: 重构自动添加追剧列表的函数 ★★★
    def add_series_to_watchlist(self, item_details: Dict[str, Any]):
        """ 【V14 - 统一判定版】"""
        if item_details.get("Type") != "Series": return
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name")
        item_id = item_details.get("Id") 
        if not tmdb_id or not item_name or not item_id: return

        try:
            # 1. 调用 DB 层进行 Upsert，并拿到当前状态
            db_row = watchlist_db.upsert_series_initial_record(tmdb_id, item_name, item_id)
            
            if db_row:
                # 2. 构造判定数据 (字段名直接对齐数据库)
                series_data = {
                    'tmdb_id': tmdb_id,
                    'item_name': item_name,
                    'watching_status': db_row['watching_status'], # 👈 修复点：使用字符串 Key
                    'force_ended': db_row['force_ended'],
                    'emby_item_ids_json': db_row['emby_item_ids_json']
                }
                # 3. 立即触发一次判定流
                self._process_one_series(series_data)
                
        except Exception as e:
            logger.error(f"自动添加剧集 '{item_name}' 时出错: {e}")

    # --- 核心任务启动器  ---
    def run_regular_processing_task_concurrent(self, progress_callback: callable, tmdb_id: Optional[str] = None):
        """核心任务启动器，只处理活跃剧集。"""
        self.progress_callback = progress_callback
        task_name = "并发追剧更新"
        if tmdb_id: task_name = f"单项追剧更新 (TMDb ID: {tmdb_id})"
        
        self.progress_callback(0, "准备检查待更新剧集...")
        try:
            where_clause = ""
            if not tmdb_id: 
                today_str = datetime.now().date().isoformat()
                where_clause = f"""
                    WHERE watching_status IN ('{STATUS_WATCHING}', '{STATUS_PENDING}', '{STATUS_PAUSED}')
                """

            active_series = self._get_series_to_process(where_clause, tmdb_id=tmdb_id)
            
            if active_series:
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

    # --- 全量刷新已完结剧集任务 ---
    def refresh_completed_series_task(self, progress_callback: callable):
        """ 
        低频扫描所有已完结剧集。
        优化策略：
        1. 近期完结：全量刷新。
        2. 远古完结：轻量检查 TMDb，只有发现新季时才全量刷新。
        """
        self.progress_callback = progress_callback
        task_name = "全量刷新剧集"
        self.progress_callback(0, "准备开始预定检查...")
        
        try:
            # 获取配置
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            # 默认回溯 365 天
            revival_check_days = int(watchlist_cfg.get('revival_check_days', 365))
            
            completed_series = self._get_series_to_process(f"WHERE watching_status = '{STATUS_COMPLETED}' AND force_ended = FALSE")
            total = len(completed_series)
            if not completed_series:
                self.progress_callback(100, "没有需要检查的已完结剧集。")
                return

            logger.info(f"  ➜ 开始检查 {total} 部已完结剧集 (全量刷新回溯期: {revival_check_days}天)...")
            
            revived_count = 0
            skipped_count = 0
            today = datetime.now(timezone.utc).date()

            for i, series in enumerate(completed_series):
                if self.is_stop_requested(): break
                progress = 10 + int(((i + 1) / total) * 90)
                series_name = series['item_name']
                tmdb_id = series['tmdb_id']
                emby_ids = series.get('emby_item_ids_json', [])
                item_id = emby_ids[0] if emby_ids else None
                
                # --- 1. 判断是否属于“远古剧集” ---
                is_ancient = False
                last_air_date_local = None
                
                # 从本地数据库记录中解析最后播出日期
                last_ep_json = series.get('last_episode_to_air_json')
                if last_ep_json:
                    if isinstance(last_ep_json, str):
                        try: last_ep_json = json.loads(last_ep_json)
                        except: pass
                    
                    if isinstance(last_ep_json, dict) and last_ep_json.get('air_date'):
                        try:
                            last_air_date_local = datetime.strptime(last_ep_json['air_date'], '%Y-%m-%d').date()
                            days_since_ended = (today - last_air_date_local).days
                            if days_since_ended > revival_check_days:
                                is_ancient = True
                        except ValueError: pass

                # --- 2. 分流处理 ---
                
                if is_ancient:
                    # ★★★ 核心修复：轻量级检查逻辑 ★★★
                    # 只有当 TMDb 有新动态时，才放行到下方的全量刷新，否则 continue
                    self.progress_callback(progress, f"轻量检查: {series_name[:15]}... ({i+1}/{total})")
                    
                    try:
                        # 1. 轻量请求：只获取 Series 基础详情 (数据量小，速度快)
                        tmdb_basic = tmdb.get_tv_details(tmdb_id, self.tmdb_api_key)
                        if not tmdb_basic: continue

                        has_new_content = False
                        
                        # 2. 比对 A: 检查 TMDb 的最新播出日期是否晚于本地记录
                        tmdb_last_ep = tmdb_basic.get('last_episode_to_air')
                        if tmdb_last_ep and tmdb_last_ep.get('air_date'):
                            try:
                                tmdb_last_date = datetime.strptime(tmdb_last_ep['air_date'], '%Y-%m-%d').date()
                                # 如果 TMDb 的日期比本地新，说明有新集播出了
                                if last_air_date_local and tmdb_last_date > last_air_date_local:
                                    has_new_content = True
                                    logger.info(f"  ⚡ [新季检测] 《{series_name}》发现新播出记录 ({tmdb_last_date} > {last_air_date_local})，触发全量刷新。")
                            except: pass
                        
                        # 3. 决策：如果没有新内容，直接跳过后续所有逻辑
                        if not has_new_content:
                            skipped_count += 1
                            logger.info(f"  💤 《{series_name}》无新内容，跳过全量刷新。")
                            continue 
                        
                        # 如果代码走到这里，说明 has_new_content = True，将自然向下执行到第 3 步

                    except Exception as e:
                        logger.warning(f"  ➜ 轻量检查《{series_name}》失败: {e}")
                        continue
                else:
                    # 近期完结：直接全量刷新
                    self.progress_callback(progress, f"全量刷新: {series_name[:15]}... ({i+1}/{total})")

                # --- 3. 执行全量刷新 (合并后的逻辑) ---
                # 无论是“近期完结”还是“远古诈尸”，只要代码能跑到这里，
                # 就说明需要更新数据库、同步子集和刷新 Emby。
                
                refresh_result = self._refresh_series_metadata(tmdb_id, series_name, item_id)
                if not refresh_result: 
                    continue
                
                # 解包返回结果，供后续复活判定逻辑使用
                tmdb_details, _, emby_seasons_state = refresh_result

                # --- 4. 复活判定逻辑 ---
                
                # 计算本地已有的最大季号
                local_max_season = 0
                if emby_seasons_state:
                    valid_local_seasons = [s for s in emby_seasons_state.keys() if s > 0]
                    if valid_local_seasons:
                        local_max_season = max(valid_local_seasons)

                # 获取 TMDb 上的总季数
                tmdb_seasons = tmdb_details.get('seasons', [])
                valid_tmdb_seasons = [s for s in tmdb_seasons if s.get('season_number', 0) > 0]
                if not valid_tmdb_seasons: continue
                
                tmdb_max_season = max((s.get('season_number', 0) for s in valid_tmdb_seasons), default=0)

                # 核心判断：如果有比本地更新的季
                if tmdb_max_season > local_max_season:
                    for season_info in valid_tmdb_seasons:
                        new_season_num = season_info.get('season_number')
                        if new_season_num <= local_max_season: continue

                        air_date_str = season_info.get('air_date')
                        # ... (日期推断逻辑保持不变) ...
                        if not air_date_str:
                            # 尝试深层查询
                            season_details_deep = tmdb.get_tv_season_details(tmdb_id, new_season_num, self.tmdb_api_key)
                            if season_details_deep:
                                air_date_str = season_details_deep.get('air_date')
                                if not air_date_str and 'episodes' in season_details_deep:
                                    episodes = season_details_deep['episodes']
                                    valid_dates = [e.get('air_date') for e in episodes if e.get('air_date')]
                                    if valid_dates: air_date_str = min(valid_dates)
                                if not season_info.get('poster_path'): season_info['poster_path'] = season_details_deep.get('poster_path')
                                if not season_info.get('overview'): season_info['overview'] = season_details_deep.get('overview')
                        
                        if not air_date_str: continue

                        try:
                            air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                            days_diff = (air_date - today).days
                            
                            if -30 <= days_diff <= 7:
                                revived_count += 1
                                status_desc = "已开播" if days_diff <= 0 else f"{days_diff}天后开播"
                                logger.info(f"  ➜ 发现《{series_name}》的新季 (S{new_season_num}) {status_desc}，触发复活订阅流程！")
                                
                                # 1. 构造媒体信息
                                season_tmdb_id = str(season_info.get('id'))
                                media_info = {
                                    'tmdb_id': season_tmdb_id,
                                    'item_type': 'Season',
                                    'title': f"{series_name} - {season_info.get('name', f'第 {new_season_num} 季')}",
                                    'release_date': air_date_str,
                                    'poster_path': season_info.get('poster_path'),
                                    'season_number': new_season_num,
                                    'parent_series_tmdb_id': tmdb_id,
                                    'overview': season_info.get('overview')
                                }

                                # 2. 提交订阅请求 (只做这一件事)
                                request_db.set_media_status_pending_release(
                                    tmdb_ids=season_tmdb_id,
                                    item_type='Season',
                                    source={"type": "watchlist", "reason": "revived_season", "item_id": tmdb_id},
                                    media_info_list=[media_info]
                                )

                                
                                # 仅更新 TMDb 状态元数据，保持数据新鲜度 (可选，不影响逻辑)
                                self._update_watchlist_entry(tmdb_id, series_name, {
                                    "watchlist_tmdb_status": "Returning Series"
                                })

                                logger.info(f"  ➜ 已为《{series_name}》S{new_season_num} 提交订阅请求。")
                                break 
                        except ValueError: pass
                
                time.sleep(0.5) # 稍微减少一点 sleep，因为轻量检查很快
            
            final_message = f"复活检查完成。共扫描 {total} 部，跳过远古剧 {skipped_count} 部，复活 {revived_count} 部。"
            self.progress_callback(100, final_message)

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            self.progress_callback(-1, f"错误: {e}")
        finally:
            self.progress_callback = None

    def _get_series_to_process(self, where_clause: str, tmdb_id: Optional[str] = None, include_all_series: bool = False) -> List[Dict[str, Any]]:
        """
        【V6 - 数据库统一版】
        - 无论是单项刷新还是批量刷新，统一调用 watchlist_db 接口。
        """
        
        # 1. 准备参数
        target_library_ids = None
        target_condition = None

        # 2. 如果是单项刷新 (tmdb_id 存在)
        if tmdb_id:
            # 单项刷新时，我们不需要 library_ids 和 where_clause
            # 因为我们就是想强制刷新这一部，不管它在哪个库，也不管它是什么状态
            pass 

        # 3. 如果是批量刷新
        else:
            # 获取配置的媒体库
            target_library_ids = self.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
            if target_library_ids:
                logger.info(f"  ➜ 已启用媒体库过滤器 ({len(target_library_ids)} 个库)，正在数据库中筛选...")

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
            
            target_condition = " AND ".join(conditions) if conditions else ""

        # 4. 统一调用数据库接口
        return watchlist_db.get_series_by_dynamic_condition(
            condition_sql=target_condition,
            library_ids=target_library_ids,
            tmdb_id=tmdb_id
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
                # --- 基础视觉与文本 ---
                "overview": "overview",           # 简介
                "poster_path": "poster_path",     # 海报
                "backdrop_path": "backdrop_path", # 背景
                "still_path": "still_path",       # 剧照
                "tagline": "tagline",             # 标语
                
                # --- 日期 ---
                "first_air_date": "release_date", # 首播日期 (Series)
                "air_date": "release_date",       # 播出日期 (Episode/Season)
                
                # --- ★★★ 新增：核心元数据 ★★★ ---
                "genres": "genres",                         # 类型 (对象数组)
                "keywords": "keywords",                     # 关键词 (对象结构)
                "content_ratings": "content_ratings",       # 分级信息 (对象结构)
                "origin_country": "origin_country",         # 产地 (字符串数组)
                "production_companies": "production_companies", # 制作公司 (对象数组)
                
                # --- ★★★ 新增：评分与状态 ★★★ ---
                "vote_average": "vote_average",   # 评分
                "vote_count": "vote_count",       # 评分人数
                "popularity": "popularity"        # 热度
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
        1. ★★★ 调用 TMDb 聚合器并发获取所有数据 (Series + Seasons + Episodes) ★★★
        2. 更新本地 JSON 缓存
        3. 更新数据库基础字段 (Series)
        4. 通知 Emby 刷新元数据
        5. 同步所有季和集的元数据到数据库 (Seasons & Episodes)
        
        返回: (latest_series_data, all_tmdb_episodes, emby_seasons_state) 或 None
        """
        if not self.tmdb_api_key:
            logger.warning("  ➜ 未配置TMDb API Key，跳过元数据刷新。")
            return None

        # ==============================================================================
        # ★★★ 核心优化：直接调用 tmdb.py 中的并发聚合函数 ★★★
        # 这个函数内部已经实现了：
        # 1. 并发请求 (默认5线程)
        # 2. 按季获取 (一次请求拿一整季的集数据，不再一集一集请求)
        # 3. 自动重试和错误处理
        # ==============================================================================
        aggregated_data = tmdb.aggregate_full_series_data_from_tmdb(tmdb_id, self.tmdb_api_key, max_workers=5)

        if not aggregated_data:
            logger.error(f"  🚫 无法聚合 '{item_name}' 的TMDb详情，元数据刷新中止。")
            return None

        # 翻译简介
        if self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW, False):
            helpers.translate_tmdb_metadata_recursively(
                item_type='Series',
                tmdb_data=aggregated_data,
                ai_translator=self.ai_translator,
                item_name=item_name
            )

        # 解包数据
        latest_series_data = aggregated_data['series_details']
        seasons_list = aggregated_data['seasons_details'] # 这是一个包含完整集信息的季列表

        # 在保存 JSON 和写入数据库之前，强制应用分级映射逻辑
        # 这会原地修改 latest_series_data，注入映射后的 'US' 分级
        try:
            helpers.apply_rating_logic(latest_series_data, latest_series_data, 'Series')
            # 顺便把映射后的分级打印出来看看
            mapped_rating = latest_series_data.get('mpaa') or latest_series_data.get('certification')
            logger.debug(f"  ➜ 已对 '{item_name}' 应用分级映射，结果: {mapped_rating}")
        except Exception as e:
            logger.warning(f"  ⚠️ 应用分级映射逻辑时出错: {e}")
        
        # 2. 将 TMDb 最新数据合并写入本地 JSON (series.json) 
        self._save_local_json(f"override/tmdb-tv/{tmdb_id}/series.json", latest_series_data)

        # 3. 更新数据库 (Series 层级) - 代码保持不变
        content_ratings = latest_series_data.get("content_ratings", {}).get("results", [])
        official_rating_json = {}
        if latest_series_data.get('adult') is True:
            official_rating_json['US'] = 'XXX' 
        else:
            content_ratings = latest_series_data.get("content_ratings", {}).get("results", [])
            for r in content_ratings:
                iso = r.get("iso_3166_1")
                rating = r.get("rating")
                if iso and rating:
                    official_rating_json[iso] = rating

        genres_raw = latest_series_data.get("genres", [])
        genres_list = []
        
        for g in genres_raw:
            # TMDb 返回的是字典 {"id": 18, "name": "Drama"}
            if isinstance(g, dict):
                name = g.get('name')
                if name:
                    # 应用汉化补丁
                    if name in utils.GENRE_TRANSLATION_PATCH:
                        name = utils.GENRE_TRANSLATION_PATCH[name]
                    
                    genres_list.append({
                        "id": g.get('id', 0), 
                        "name": name
                    })
            # 防御性编程：如果 TMDb 返回了字符串 (虽然不太可能)
            elif isinstance(g, str):
                name = g
                if name in utils.GENRE_TRANSLATION_PATCH:
                    name = utils.GENRE_TRANSLATION_PATCH[name]
                genres_list.append({"id": 0, "name": name})

        genres_json = genres_list if genres_list else None
        keywords = latest_series_data.get("keywords", {}).get("results", [])
        keywords_json = [{"id": k["id"], "name": k["name"]} for k in keywords]
        studios = latest_series_data.get("production_companies", [])
        studios_json = [{"id": s["id"], "name": s["name"]} for s in studios]
        countries = latest_series_data.get("origin_country", [])
        countries_json = countries if isinstance(countries, list) else [countries]

        series_updates = {
            "original_title": latest_series_data.get("original_name"),
            "overview": latest_series_data.get("overview"),
            "poster_path": latest_series_data.get("poster_path"),
            "release_date": latest_series_data.get("first_air_date") or None,
            "release_year": int(latest_series_data.get("first_air_date")[:4]) if latest_series_data.get("first_air_date") else None,
            "original_language": latest_series_data.get("original_language"),
            "watchlist_tmdb_status": latest_series_data.get("status"),
            "total_episodes": latest_series_data.get("number_of_episodes", 0),
            "rating": latest_series_data.get("vote_average"),
            "official_rating_json": json.dumps(official_rating_json) if official_rating_json else None,
            "genres_json": json.dumps(genres_json) if genres_json else None,
            "keywords_json": json.dumps(keywords_json) if keywords_json else None,
            "studios_json": json.dumps(studios_json) if studios_json else None,
            "countries_json": json.dumps(countries_json) if countries_json else None
        }
        
        media_db.update_media_metadata_fields(tmdb_id, 'Series', series_updates)
        logger.debug(f"  ➜ 已全量刷新 '{item_name}' 的 Series 元数据。")

        # 4. 处理季和集的数据 (保存 JSON + 收集列表)
        # 这里不需要再发网络请求了，直接从 aggregated_data 里拿
        all_tmdb_episodes = []
        
        for season_details in seasons_list:
            season_num = season_details.get("season_number")
            if season_num is None: continue
            
            # 保存 season-X.json
            self._save_local_json(f"override/tmdb-tv/{tmdb_id}/season-{season_num}.json", season_details)

            # 提取集信息
            episodes = season_details.get("episodes", [])
            if episodes:
                all_tmdb_episodes.extend(episodes)
                
                # 保存 season-X-episode-Y.json
                for ep in episodes:
                    ep_num = ep.get("episode_number")
                    if ep_num is not None:
                        self._save_local_json(
                            f"override/tmdb-tv/{tmdb_id}/season-{season_num}-episode-{ep_num}.json", 
                            ep
                        )

        # 5. 通知 Emby 刷新元数据 
        if item_id:
            emby.refresh_emby_item_metadata(
                item_emby_id=item_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id_for_ops=self.emby_user_id,
                replace_all_metadata_param=True,
                item_name_for_log=item_name
            )

        # 6. 同步季和集到数据库 
        emby_seasons_state = media_db.get_series_local_children_info(tmdb_id)
        
        try:
            # 注意：这里传入的 tmdb_seasons 应该是包含基础信息的列表
            # aggregated_data['series_details']['seasons'] 包含了季的基础信息（集数、海报等）
            # 而 seasons_list 包含了完整的集信息
            # sync_series_children_metadata 需要的是基础季列表和完整集列表
            media_db.sync_series_children_metadata(
                parent_tmdb_id=tmdb_id,
                seasons=latest_series_data.get("seasons", []), 
                episodes=all_tmdb_episodes,
                local_in_library_info=emby_seasons_state
            )
            logger.debug(f"  ➜ 已同步 '{item_name}' 的季/集元数据到数据库。")
        except Exception as e_sync:
            logger.error(f"  ➜ 同步 '{item_name}' 子项目数据库时出错: {e_sync}", exc_info=True)
        
        return latest_series_data, all_tmdb_episodes, emby_seasons_state
    
    # ★★★ 辅助方法：检查是否满足自动待定条件 ★★★
    def _check_auto_pending_condition(self, series_details: Dict[str, Any], auto_pending_cfg: Dict = None) -> bool:
        """
        检查剧集最新季是否满足“自动待定”条件。
        优化点：
        1. 使用 UTC 时间，避免时区误差。
        2. 逻辑与 helpers.py 保持一致 (Days <= Threshold AND Count <= Threshold)。
        3. 直接使用 series_details 中的 episode_count，无需额外 API 请求。
        """
        try:
            # 1. 获取配置
            if auto_pending_cfg is None:
                watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
                auto_pending_cfg = watchlist_cfg.get('auto_pending', {})
            
            if not auto_pending_cfg.get('enabled', False):
                return False

            threshold_days = int(auto_pending_cfg.get('days', 30))
            threshold_episodes = int(auto_pending_cfg.get('episodes', 1))
            
            # 使用 UTC 时间
            today = datetime.now(timezone.utc).date()

            # 2. 获取季列表
            seasons = series_details.get('seasons', [])
            if not seasons: return False
            
            # 3. 找到“最新”的一季 (过滤掉第0季，按季号倒序取第一个)
            valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], 
                                   key=lambda x: x['season_number'], reverse=True)
            
            if not valid_seasons: return False
            
            latest_season = valid_seasons[0]
            
            # 4. 核心判断
            air_date_str = latest_season.get('air_date')
            # 直接读取 TMDb 官方提供的该季总集数 (这是最准确的字段)
            episode_count = latest_season.get('episode_count', 0)

            if air_date_str:
                try:
                    air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                    days_diff = (today - air_date).days
                    
                    # 逻辑：
                    # 1. days_diff >= 0: 必须是已经开播的（未来的剧集由其他逻辑处理）
                    # 2. days_diff <= threshold_days: 开播时间在观察期内 (如30天)
                    # 3. episode_count <= threshold_episodes: 集数很少 (如只有1集)
                    # 只有同时满足这三点，才认为是“刚开播且信息不全”，需要待定
                    if (days_diff >= 0) and (days_diff <= threshold_days) and (episode_count <= threshold_episodes):
                        logger.info(f"  🛡️ [自动待定] 触发: S{latest_season.get('season_number')} 上线{days_diff}天, 集数{episode_count} (阈值: {threshold_episodes})")
                        return True
                except ValueError:
                    pass
            
            return False
        except Exception as e:
            logger.warning(f"检查自动待定条件时出错: {e}")
            return False

    # ★★★ 辅助方法：同步状态给 MoviePilot ★★★
    def _sync_status_to_moviepilot(self, tmdb_id: str, series_name: str, series_details: Dict[str, Any], final_status: str, old_status: str = None):
        """
        根据最终计算出的 watching_status，调用 MP 接口更新订阅状态及总集数。
        逻辑优化：
        1. 只要 MP 有订阅，就同步状态（覆盖所有季）。
        2. 如果 MP 无订阅，仅自动补订【最新季】（防止已完结的老季诈尸）。
        """
        try:
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            enable_auto_pause = watchlist_cfg.get('auto_pause', False)
            auto_pending_cfg = watchlist_cfg.get('auto_pending', {})
            enable_sync_sub = watchlist_cfg.get('sync_mp_subscription', False)
            
            # 获取配置的虚标集数 (默认99)
            fake_total_episodes = int(auto_pending_cfg.get('default_total_episodes', 99))

            # 1. 确定 MP 目标状态
            target_mp_status = 'R' 
            if final_status == STATUS_PENDING:
                target_mp_status = 'P'
            elif final_status == STATUS_PAUSED:
                target_mp_status = 'S' if enable_auto_pause else 'R'
            elif final_status == STATUS_WATCHING:
                target_mp_status = 'R'
            else:
                return 

            # ★★★ 计算最新季号 ★★★
            all_seasons = series_details.get('seasons', [])
            valid_seasons = [s for s in all_seasons if s.get('season_number', 0) > 0]
            latest_season_num = max((s['season_number'] for s in valid_seasons), default=0)

            # 2. 遍历所有季进行同步
            for season in all_seasons:
                s_num = season.get('season_number')
                if not s_num or s_num <= 0:
                    continue

                # --- A. 检查订阅是否存在 ---
                exists = moviepilot.check_subscription_exists(tmdb_id, 'Series', self.config, season=s_num)
                
                # --- B. 自动补订逻辑 (核心修改) ---
                if not exists:
                    if not self.config.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
                        return
                    # 只有【最新季】才允许自动补订
                    # 逻辑：S1-S3 没了就没了，不补；S4(最新) 没了必须补回来，因为要追更。
                    if s_num == latest_season_num:
                        if not enable_sync_sub:
                            logger.debug("  ➜ 自动补订开关关闭，跳过自动补订。")
                            continue
                        logger.info(f"  🔍 [MP同步] 发现《{series_name}》最新季 S{s_num} 在 MoviePilot 中无活跃订阅，正在自动补订...")
                        sub_success = moviepilot.subscribe_series_to_moviepilot(
                            series_info={'title': series_name, 'tmdb_id': tmdb_id},
                            season_number=s_num,
                            config=self.config
                        )
                        if not sub_success:
                            logger.warning(f"  ❌ [MP同步] 补订 S{s_num} 失败，跳过。")
                            continue
                        logger.info(f"  ✅ [MP同步] 《{series_name}》S{s_num} 补订成功。")
                    else:
                        # 旧季不存在，直接跳过，不打扰
                        continue

                # --- C. 计算目标总集数 ---
                real_episode_count = season.get('episode_count', 0)
                current_target_total = None
                
                if target_mp_status == 'P':
                    current_target_total = fake_total_episodes
                elif target_mp_status == 'R':
                    if real_episode_count > 0:
                        current_target_total = real_episode_count

                # --- D. 执行状态同步 ---
                sync_success = moviepilot.update_subscription_status(
                    int(tmdb_id), 
                    s_num, 
                    target_mp_status, 
                    self.config, 
                    total_episodes=current_target_total
                )

                if sync_success:
                    # 仅记录有意义的变更日志
                    should_log = False
                    log_msg = ""

                    if target_mp_status != 'R':
                        should_log = True
                        status_desc = "待定(P)" if target_mp_status == 'P' else "暂停(S)"
                        ep_msg = f", 集数->{current_target_total}" if current_target_total else ""
                        log_msg = f"  ➜ [MP同步] 《{series_name}》S{s_num} -> {status_desc}{ep_msg} (原因: {translate_internal_status(final_status)})"
                    
                    elif target_mp_status == 'R' and (old_status == STATUS_PENDING or (not exists and s_num == latest_season_num)):
                        should_log = True
                        reason = "重新补订" if not exists else "解除待定"
                        ep_msg = f", 集数修正->{current_target_total}" if current_target_total else ""
                        log_msg = f"  ➜ [MP同步] 《{series_name}》S{s_num} -> 恢复订阅(R){ep_msg} (原因: {reason})"

                    if should_log:
                        logger.info(log_msg)

        except Exception as e:
            logger.warning(f"同步状态给 MoviePilot 时出错: {e}")

    def _check_season_consistency(self, tmdb_id: str, season_number: int, expected_episode_count: int) -> bool:
        """
        检查指定季的本地文件是否满足“无需洗版”的条件：
        1. 集数已齐 (本地集数 >= TMDb集数)
        2. 一致性达标 (分辨率、制作组、编码 必须完全统一)
        """
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                # 获取该季所有集的文件资产信息
                sql = """
                    SELECT asset_details_json 
                    FROM media_metadata 
                    WHERE parent_series_tmdb_id = %s 
                      AND season_number = %s 
                      AND item_type = 'Episode'
                      AND in_library = TRUE
                """
                cursor.execute(sql, (tmdb_id, season_number))
                rows = cursor.fetchall()

            # 1. 检查集数是否集齐
            local_count = len(rows)
            if local_count < expected_episode_count:
                logger.info(f"  🔍 [一致性检查] S{season_number} 尚未集齐 (本地:{local_count} / TMDb:{expected_episode_count})，需要洗版补全。")
                return False

            # 2. 检查一致性 (分辨率、制作组、编码)
            resolutions = set()
            groups = set()
            codecs = set()

            for row in rows:
                assets = row.get('asset_details_json')
                if not assets: continue
                
                # 取主文件 (第一个)
                main_asset = assets[0]
                
                resolutions.add(main_asset.get('resolution_display', 'Unknown'))
                codecs.add(main_asset.get('codec_display', 'Unknown'))
                
                # 制作组处理：取第一个识别到的组，如果没有则标记为 Unknown
                raw_groups = main_asset.get('release_group_raw', [])
                group_name = raw_groups[0] if raw_groups else 'Unknown'
                groups.add(group_name)

            # 判定逻辑：所有集合长度必须为 1 (即只有一种规格)
            is_consistent = (len(resolutions) == 1 and len(groups) == 1 and len(codecs) == 1)
            
            if is_consistent:
                # 获取唯一的那个规格，用于日志展示
                res = list(resolutions)[0]
                grp = list(groups)[0]
                logger.info(f"  ✅ [一致性检查] S{season_number} 完美达标 (集齐且统一): [{res} / {grp}]，跳过洗版。")
                return True
            else:
                logger.info(f"  ⚠️ [一致性检查] S{season_number} 版本混杂，需要洗版。分布: 分辨率{resolutions}, 制作组{groups}, 编码{codecs}")
                return False

        except Exception as e:
            logger.error(f"  ❌ 检查 S{season_number} 一致性时出错: {e}")
            return False # 出错默认不跳过，继续洗版以防万一

    def _handle_auto_resub_ended(self, tmdb_id: str, series_name: str, season_number: int, episode_count: int):
        """
        【重构版】针对指定季进行完结洗版。
        参数直接传入季号和集数，不再需要在内部计算。
        """
        try:
            logger.info(f"  🎉 剧集《{series_name}》已自然完结，正在对最终季 (S{season_number}) 执行洗版流程...")
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            # 1.检查配额
            if settings_db.get_subscription_quota() <= 0:
                logger.warning(f"  ⚠️ 每日订阅配额已用尽，跳过《{series_name}》S{season_number} 的完结洗版。")
                return
            # 2. 直接使用传入的集数进行一致性检查
            if self._check_season_consistency(tmdb_id, season_number, episode_count):
                return
            
            # 3. 检查是否需要删除旧文件 (Emby)
            if watchlist_cfg.get('auto_delete_old_files', False):
                logger.info(f"  🗑️ [自动清理] 检测到“删除 Emby 旧文件”已开启，正在查找并删除 S{season_number}...")
                try:
                    target_season_id = watchlist_db.get_season_emby_id(tmdb_id, season_number)
                    if target_season_id:
                        if emby.delete_item(target_season_id, self.emby_url, self.emby_api_key, self.emby_user_id):
                            logger.info(f"  ✅ [自动清理] 已成功从 Emby 删除 S{season_number} (ID: {target_season_id})。")
                            time.sleep(2)
                        else:
                            logger.error(f"  ❌ [自动清理] 删除 S{season_number} 失败，将继续执行洗版订阅。")
                    else:
                        logger.warning(f"  ⚠️ [自动清理] 数据库中未找到 S{season_number} 的 Emby ID，跳过删除。")
                except Exception as e:
                    logger.error(f"  ❌ [自动清理] 执行删除逻辑时出错: {e}")

            # 4. 删除整理记录 (MoviePilot) - 新增开关控制
            related_hashes = []
            if watchlist_cfg.get('auto_delete_mp_history', False):
                logger.info(f"  🗑️ [自动清理] 正在删除 MoviePilot 整理记录...")
                related_hashes = moviepilot.delete_transfer_history(tmdb_id, season_number, series_name, self.config)

            # 5. 清理下载器中的旧任务 - 新增开关控制
            if watchlist_cfg.get('auto_delete_download_tasks', False):
                logger.info(f"  🗑️ [自动清理] 正在删除下载器旧任务...")
                # 如果第4步没开，related_hashes 为空，delete_download_tasks 内部应有处理逻辑(如按名字删)或仅跳过hash删除
                moviepilot.delete_download_tasks(series_name, self.config, hashes=related_hashes)

            # 6. 取消旧订阅
            moviepilot.cancel_subscription(tmdb_id, 'Series', self.config, season=season_number)
            
            # 7. 发起新订阅 (洗版)
            payload = {
                "name": series_name,
                "tmdbid": int(tmdb_id),
                "type": "电视剧",
                "season": season_number,
                "best_version": 1 # ★ 核心：洗版模式
            }
            
            if moviepilot.subscribe_with_custom_payload(payload, self.config):
                settings_db.decrement_subscription_quota()
                logger.info(f"  ➜ [完结洗版] 《{series_name}》S{season_number} 已提交洗版订阅。")
            else:
                logger.error(f"  ❌ [完结洗版] 《{series_name}》S{season_number} 提交失败。")

        except Exception as e:
            logger.error(f"  ⚠️ 执行完结自动洗版逻辑时出错: {e}", exc_info=True)

    # ★★★ 核心处理逻辑：单个剧集的所有操作在此完成 ★★★
    def _process_one_series(self, series_data: Dict[str, Any]):
        tmdb_id = series_data['tmdb_id']
        emby_ids = series_data.get('emby_item_ids_json', [])
        item_id = emby_ids[0] if emby_ids else None
        item_name = series_data['item_name']
        old_status = series_data.get('watching_status') 
        is_force_ended = bool(series_data.get('force_ended', False))
        
        logger.info(f"  ➜ 【追剧检查】正在处理: '{item_name}' (TMDb ID: {tmdb_id})")

        if not item_id:
            logger.warning(f"  ➜ 剧集 '{item_name}' 在数据库中没有关联的 Emby ID，跳过。")
            return
        
        # --- 获取配置 ---
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        auto_pending_cfg = watchlist_cfg.get('auto_pending', {})
        aggressive_threshold = int(auto_pending_cfg.get('episodes', 5)) 

        # 调用通用辅助函数刷新元数据
        refresh_result = self._refresh_series_metadata(tmdb_id, item_name, item_id)
        if not refresh_result:
            return # 刷新失败，中止后续逻辑
        
        latest_series_data, all_tmdb_episodes, emby_seasons = refresh_result

        # ==================== 季总集数锁定过滤器 ====================
        # 如果总集数被锁定，我们需要剔除 TMDb 返回的“多余”集数
        # 这样后续的“下一集计算”和“缺集计算”就不会看到这些不存在的集了
        try:
            # 1. 获取所有季的锁定配置
            seasons_lock_map = watchlist_db.get_series_seasons_lock_info(tmdb_id)
            
            if seasons_lock_map:
                filtered_episodes = []
                discarded_count = 0
                
                for ep in all_tmdb_episodes:
                    s_num = ep.get('season_number')
                    e_num = ep.get('episode_number')
                    
                    # 获取该季的锁定配置
                    lock_info = seasons_lock_map.get(s_num)
                    
                    # 判断逻辑：
                    # 如果该季存在锁定配置，且已开启锁定，且当前集号 > 锁定集数 -> 剔除
                    if (lock_info and 
                        lock_info.get('locked') and 
                        e_num is not None and 
                        e_num > (lock_info.get('count') or 0)):
                        
                        discarded_count += 1
                        # 仅在第一次剔除时打印详细日志，避免刷屏
                        if discarded_count == 1:
                            lock_count = lock_info.get('count') or 0
                            logger.info(f"  🔒 [分季锁定生效] S{s_num} 锁定为 {lock_count} 集，正在剔除 TMDb 多余集数 (如 S{s_num}E{e_num})...")
                        continue
                    
                    # 否则保留该集
                    filtered_episodes.append(ep)
                
                if discarded_count > 0:
                    logger.info(f"  🗑️ 共剔除了 {discarded_count} 个不符合分季锁定规则的集。")
                    all_tmdb_episodes = filtered_episodes
            
            else:
                # 如果没查到任何季信息（罕见），就不做过滤
                pass

        except Exception as e:
            logger.error(f"  ⚠️ 执行分季锁定过滤时出错: {e}", exc_info=True)

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
                    
                    # 如果总集数很少（例如3集），可能是新剧刚开播 TMDb 还没更新后续集数，
                    # 此时应跳过大结局判定，让其落入后续的“最近播出”或“自动待定”逻辑。
                    if total_ep_count > aggressive_threshold and last_e_num >= total_ep_count:
                        is_season_finale = True
                        logger.debug(f"  🔍 [预判] S{last_s_num} 总集数({total_ep_count}) > 保护阈值({aggressive_threshold}) 且已播至最后一集，标记为本季大结局。")

        # ==============================================================================
        # ★★★ 激进完结策略 ★★★
        # ==============================================================================
        is_aggressive_completed = False
        
        # 1. 获取 TMDb 记录的总集数
        calculated_total = len([ep for ep in all_tmdb_episodes if ep.get('season_number', 0) > 0])
        current_total_episodes = calculated_total if calculated_total > 0 else latest_series_data.get('number_of_episodes', 0)

        # 2. 计算本地已入库的正片总集数
        local_total_episodes = 0
        if emby_seasons:
            for s_num, ep_set in emby_seasons.items():
                if s_num > 0: local_total_episodes += len(ep_set)
        
        # 3. 判断逻辑
        # 前置条件: 总集数超过阈值 (防止误伤短剧，短剧交给后续的7天规则处理)
        if current_total_episodes > aggressive_threshold:
            
            # ★★★ 修正点：获取最新播出集的集号 ★★★
            last_ep_number = 0
            last_air_date = None
            if last_episode_to_air:
                last_ep_number = last_episode_to_air.get('episode_number', 0)
                if date_str := last_episode_to_air.get('air_date'):
                    try:
                        last_air_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError: pass

            # 条件 A: 时间维度 (最后一集已播出)
            # 逻辑：最新播出的集号 >= 总集数 AND 播出日期 <= 今天
            if last_ep_number >= current_total_episodes and last_air_date and last_air_date <= today:
                is_aggressive_completed = True
                logger.info(f"  🚀 《{item_name}》大结局(E{last_ep_number})已播出，判定完结。")
            
            # 条件 B: 收藏维度 (本地已集齐)
            # 逻辑：本地集数 >= TMDb总集数
            elif not is_aggressive_completed and local_total_episodes >= current_total_episodes:
                is_aggressive_completed = True
                logger.info(f"  🚀 《{item_name}》本地已集齐 {local_total_episodes}/{current_total_episodes} 集，判定完结。")

        # ==============================================================================
        # ★★★ 重构后的状态判定逻辑 ★★★
        # ==============================================================================

        # 规则 1: 激进策略优先 -> 直接完结
        if is_aggressive_completed:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            
            # 补充日志：解释为什么这么做
            if real_next_episode_to_air:
                logger.info(f"  🏁 [判定-TMDb已完结] 虽本地缺集，但满足完结策略，强制判定“已完结”以触发洗版(抢完结包)。")
            else:
                logger.info(f"  🏁 [判定-本地已集齐] 满足完结策略，判定“已完结”。")

        # 规则 2: TMDb 状态已完结 -> 直接完结 (不考虑本地是否集齐)
        elif is_ended_on_tmdb:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.info(f"  🏁 [判定-规则1] TMDb状态为 '{new_tmdb_status}'，判定为“已完结”。")

        # 规则 3: 本季大结局已播出 (且无明确下一集) -> 直接完结 (不考虑本地是否集齐)
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

        # 规则 4: 连载中逻辑
        else:
            # 情况 A: 下一集有明确播出日期
            if effective_next_episode:
                season_number = effective_next_episode.get('season_number')
                episode_number = effective_next_episode.get('episode_number')
                air_date = effective_next_episode_air_date
                days_until_air = (air_date - today).days

                # ==============================================================================
                # ★★★ 核心逻辑：不见兔子不撒鹰 ★★★
                # 只有当下一集所属的季在本地至少有一集时，才允许进入 Watching/Paused 状态。
                # 否则一律视为 Completed (等待新季入库)。
                # ==============================================================================
                has_local_season = season_number in emby_seasons

                if not has_local_season:
                    final_status = STATUS_COMPLETED
                    paused_until_date = None
                    logger.info(f"  zzz [判定-未入库] 下一集 (S{season_number}E{episode_number}) 虽有排期，但本地无该季任何文件，判定为“已完结”。")
                
                # --- 只有本地有该季文件，才根据时间判断是追剧还是暂停 ---
                else:
                    # 子规则 A: 1天内就要播出 (或已播出但未下载) -> 设为“追剧中”
                    if days_until_air <= 1:
                        final_status = STATUS_WATCHING
                        paused_until_date = None
                        logger.info(f"  👀 [判定-连载中] S{season_number} 本地已入库，且下一集 (E{episode_number}) 即将在 {days_until_air} 天内播出 (或已播出)，保持“追剧中”。")

                    # 子规则 B: 还有很久才播出 -> 暂停至播出日期
                    else:
                        final_status = STATUS_PAUSED
                        paused_until_date = air_date 
                        logger.info(f"  ⏸️ [判定-连载中] S{season_number} 本地已入库，但下一集 (E{episode_number}) 将在 {days_until_air} 天后 ({air_date}) 播出，暂停至该日期。")

            # 情况 B: 无下一集信息 (或信息不全)
            else:
                if days_since_last != 9999:
                    # 1. 获取当前季的 TMDb 总集数
                    current_season_total = 0
                    last_s_num = last_episode_to_air.get('season_number')
                    last_e_num = last_episode_to_air.get('episode_number')
                    
                    if last_s_num:
                        # 从 series_details 的 seasons 列表中找到对应季的 info
                        season_info = next((s for s in latest_series_data.get('seasons', []) if s.get('season_number') == last_s_num), None)
                        if season_info:
                            current_season_total = season_info.get('episode_count', 0)

                    # 2. 核心判断：
                    # 条件：状态是“连载中” AND (当前季总集数 > 0) AND (已播集号 < 总集数)
                    # 只要满足这个条件，说明这季还没播完，绝对不能判完结。
                    if new_tmdb_status == "Returning Series" and last_e_num and current_season_total > 0 and last_e_num < current_season_total:
                        final_status = STATUS_WATCHING
                        paused_until_date = None
                        logger.info(f"  🛡️ [判定-连载中] 虽无未来排期，但本季尚未播完 (进度: S{last_s_num} - {last_e_num}/{current_season_total})，判定为数据滞后，保持“追剧中”。")
                    
                    # 否则 -> 判定完结
                    # 包含情况：
                    # 1. Status 是 Ended/Canceled (直接完结)
                    # 2. Status 是 Returning，但已播集数 >= 总集数 (本季完结 -> 视为完结，等待后续复活逻辑)
                    else:
                        final_status = STATUS_COMPLETED
                        paused_until_date = None
                        logger.info(f"  🏁 [判定-已完结] 无待播集信息，且本季已完结或剧集已完结 (进度: S{last_s_num} - {last_e_num}/{current_season_total})。")
                
                else:
                    # 极端情况：无任何日期信息
                    final_status = STATUS_WATCHING
                    paused_until_date = None
                    logger.info(f"  👀 [判定-连载中] 缺乏播出日期数据，默认保持“追剧中”状态。")

        # 自动待定 (Auto Pending) 覆盖逻辑
        # 读取配置 (提前读取，后面要用)
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        auto_pending_cfg = watchlist_cfg.get('auto_pending', {})
        
        # ★★★ 修复：将 STATUS_COMPLETED 加入检查列表 ★★★
        # 只有这样，当逻辑误判为“已完结”时，下面的代码才有机会把它救回来
        if final_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_COMPLETED]:
            
            # 安全检查：如果 TMDb 明确说是 Ended/Canceled，那就不救了，是真的完结了
            if new_tmdb_status in ["Ended", "Canceled"]:
                 pass 
            
            # 核心检查：如果 TMDb 还在连载(Returning Series)，但满足新剧条件(集数少、时间短)
            elif self._check_auto_pending_condition(latest_series_data, auto_pending_cfg):
                final_status = STATUS_PENDING
                paused_until_date = None 
                # 这里的日志会出现在“判定已完结”之后，表示修正成功
                logger.info(f"  🛡️ [自动待定生效] 《{item_name}》虽被判定完结，但满足新剧保护条件，状态强制修正为 '待定 (Pending)'。")

        # 手动强制完结
        if is_force_ended and final_status != STATUS_COMPLETED:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.warning(f"  🔄 [强制完结生效] 最终状态被覆盖为 '已完结'。")

        # 只有当内部状态是“追剧中”或“已暂停”时，才认为它在“连载中”
        is_truly_airing = final_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_PENDING]
        logger.info(f"  ➜ 最终判定 '{item_name}' 的真实连载状态为: {is_truly_airing} (内部状态: {translate_internal_status(final_status)})")

        # ======================================================================
        # ★★★ 完结自动洗版逻辑 (V4 - 纯状态流转驱动) ★★★
        # ======================================================================
        # 核心逻辑：只有从“活跃追剧状态”转变为“完结状态”时，才视为“新鲜完结”
        logger.debug(f"  🔍 [状态流转] 剧名: {item_name}, 旧状态: {old_status}, 新状态: {final_status}")
        if final_status == STATUS_COMPLETED and old_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_PENDING] and not is_force_ended:
            
            # 检查功能开关
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            if watchlist_cfg.get('auto_resub_ended', False):
                
                # 获取最后一季信息
                seasons = latest_series_data.get('seasons', [])
                valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])
                
                if valid_seasons:
                    target_season = valid_seasons[-1]
                    last_s_num = target_season.get('season_number')
                    last_ep_count = target_season.get('episode_count', 0)
                    
                    # 🚀 直接触发洗版，不再检查 TMDb 的日期（充分信赖本地判定和状态流转）
                    logger.info(f"  🚀 [完结洗版] 《{item_name}》由 {translate_internal_status(old_status)} 转为完结，立即提交 S{last_s_num} 的洗版订阅。")
                    self._handle_auto_resub_ended(tmdb_id, item_name, last_s_num, last_ep_count)

        # 更新追剧数据库
        updates_to_db = {
            "watching_status": final_status, 
            "paused_until": paused_until_date.isoformat() if paused_until_date else None,
            "watchlist_tmdb_status": new_tmdb_status, 
            "watchlist_next_episode_json": json.dumps(real_next_episode_to_air) if real_next_episode_to_air else None,
            "watchlist_missing_info_json": json.dumps(missing_info),
            "last_episode_to_air_json": json.dumps(last_episode_to_air) if last_episode_to_air else None,
            "watchlist_is_airing": is_truly_airing
        }
        # 如果是待定状态，强制修改总集数为“虚标”值
        if final_status == STATUS_PENDING:
            # 获取配置的默认集数，默认为 99
            fake_total = int(auto_pending_cfg.get('default_total_episodes', 99))
            
            current_tmdb_total = latest_series_data.get('number_of_episodes', 0)
            
            if current_tmdb_total < fake_total:
                # 1. 更新 Series 记录 (保持原样)
                updates_to_db['total_episodes'] = fake_total
                
                # 2. ★★★ 新增：同时更新最新一季的 Season 记录 ★★★
                # 只有更新了 Season 记录，前端分季卡片才会显示虚标集数
                seasons = latest_series_data.get('seasons', [])
                # 过滤掉第0季，按季号倒序找到最新季
                valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], 
                                       key=lambda x: x['season_number'], reverse=True)
                
                if valid_seasons:
                    latest_season_num = valid_seasons[0]['season_number']
                    # 调用 DB 更新
                    watchlist_db.update_specific_season_total_episodes(tmdb_id, latest_season_num, fake_total)
                    logger.debug(f"  ➜ 已同步更新 S{latest_season_num} 的总集数为 {fake_total}")
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

        # ======================================================================
        # ★★★ 新增：MP 状态接管与同步 (自动待定 & 自动暂停) ★★★
        # ======================================================================
        self._sync_status_to_moviepilot(
            tmdb_id=tmdb_id, 
            series_name=item_name, 
            series_details=latest_series_data, 
            final_status=final_status,
            old_status=old_status
        )

    # --- 统一的、公开的追剧处理入口 ★★★
    def process_watching_list(self, item_id: Optional[str] = None):
        if item_id:
            logger.trace(f"--- 开始执行单项追剧更新任务 (ItemID: {item_id}) ---")
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
        通过对比本地和TMDb全量数据，计算用户真正缺失的第一集。
        【修复版】忽略本地最大季号之前的“整季缺失”，只关注当前季或未来季。
        """
        # 1. 获取本地已有的最大季号 (用于判断什么是"旧季")
        valid_local_seasons = [s for s in emby_seasons.keys() if s > 0]
        max_local_season = max(valid_local_seasons) if valid_local_seasons else 0

        # 2. 获取TMDb上所有非特别季的剧集，并严格按季号、集号排序
        all_episodes_sorted = sorted([
            ep for ep in all_tmdb_episodes 
            if ep.get('season_number') is not None and ep.get('season_number') != 0
        ], key=lambda x: (x.get('season_number', 0), x.get('episode_number', 0)))
        
        # 3. 遍历这个完整列表
        for episode in all_episodes_sorted:
            s_num = episode.get('season_number')
            e_num = episode.get('episode_number')
            
            # ======================= ★★★ 核心修复逻辑 ★★★ =======================
            # 如果这一集所属的季号 < 本地已有的最大季号
            # 并且本地完全没有这一季 (emby_seasons中没有这个key)
            # 说明这是用户故意跳过的“旧季” (例如只追S2，不想要S1)
            # 此时直接 continue 跳过，不要把它当成“待播集”
            if max_local_season > 0 and s_num < max_local_season and s_num not in emby_seasons:
                continue
            # ===================================================================

            if s_num not in emby_seasons or e_num not in emby_seasons.get(s_num, set()):
                # 找到了！这才是基于用户当前进度的“下一集”
                # 可能是当前季的下一集，也可能是新的一季的第一集
                logger.info(f"  ➜ 找到本季缺失的下一集: S{s_num}E{e_num} ('{episode.get('name')}')。")
                return episode
        
        # 4. 如果循环完成，说明本地拥有TMDb上所有的剧集 (或者只缺了未来的)
        logger.info("  ➜ 本地媒体库已拥有当前进度所有剧集，无待播信息。")
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