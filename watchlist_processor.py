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
    def __init__(self, config: Dict[str, Any], ai_translator=None, douban_api=None):
        if not isinstance(config, dict):
            raise TypeError(f"配置参数(config)必须是一个字典，但收到了 {type(config).__name__} 类型。")
        self.config = config
        self.tmdb_api_key = self.config.get("tmdb_api_key", "")
        self.emby_url = self.config.get("emby_server_url")
        self.emby_api_key = self.config.get("emby_api_key")
        self.emby_user_id = self.config.get("emby_user_id")
        self.p115_enable_organize = self.config.get("p115_enable_organize", False)
        self.ai_translator = ai_translator
        self.douban_api = douban_api
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
                                    logger.info(f"  ➜ [新季检测] 《{series_name}》发现新播出记录 ({tmdb_last_date} > {last_air_date_local})，触发全量刷新。")
                            except: pass
                        
                        # 3. 决策：如果没有新内容，直接跳过后续所有逻辑
                        if not has_new_content:
                            skipped_count += 1
                            logger.info(f"  ➜ 《{series_name}》无新内容，跳过全量刷新。")
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
            logger.error(f"  ➜ 无法聚合 '{item_name}' 的TMDb详情，元数据刷新中止。")
            return None

        # 翻译简介、标题、标语 (大一统引擎)
        if self.ai_translator:
            helpers.translate_tmdb_metadata_recursively(
                item_type='Series',
                tmdb_data=aggregated_data,
                ai_translator=self.ai_translator,
                item_name=item_name,
                tmdb_api_key=self.tmdb_api_key,
                config=self.config
            )

        # ======================================================================
        # ★★★ 老六专属：无简介笑话占位功能 (追剧刷新) ★★★
        # ======================================================================
        if self.config.get("ai_joke_fallback", False) and self.ai_translator:
            jokes_to_generate = {}
            
            # 1. 检查主干
            if not aggregated_data['series_details'].get("overview"):
                jokes_to_generate["main"] = item_name
                
            # 2. 检查分集
            # 尝试从数据库读取旧数据，继承已有笑话，省 Token！
            import extensions
            old_payload, _ = extensions.media_processor_instance._reconstruct_full_data_from_db(tmdb_id, 'Series') if hasattr(extensions, 'media_processor_instance') else (None, None)
            old_episodes = {}
            if old_payload and "episodes_details" in old_payload:
                old_eps = old_payload["episodes_details"]
                old_episodes = old_eps if isinstance(old_eps, dict) else {f"S{e.get('season_number')}E{e.get('episode_number')}": e for e in old_eps}

            for season_details in aggregated_data['seasons_details']:
                for ep in season_details.get("episodes", []):
                    if not ep.get("overview"):
                        ep_key = f"S{ep.get('season_number')}E{ep.get('episode_number')}"
                        old_overview = old_episodes.get(ep_key, {}).get("overview") or ""
                        if "【老六占位简介】" in old_overview:
                            ep["overview"] = old_overview # 继承老笑话
                        else:
                            jokes_to_generate[ep_key] = f"{item_name} {ep_key}"

            # 3. 批量生成并回填
            if jokes_to_generate:
                logger.info(f"  ➜ [老六模式] 追剧刷新发现 {len(jokes_to_generate)} 处缺失简介，正在呼叫 AI 编段子...")
                generated_jokes = self.ai_translator.batch_generate_jokes(jokes_to_generate)
                
                if "main" in generated_jokes:
                    aggregated_data['series_details']["overview"] = generated_jokes["main"]
                
                for season_details in aggregated_data['seasons_details']:
                    for ep in season_details.get("episodes", []):
                        ep_key = f"S{ep.get('season_number')}E{ep.get('episode_number')}"
                        if ep_key in generated_jokes:
                            ep["overview"] = generated_jokes[ep_key]

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
            logger.warning(f"  ➜ 应用分级映射逻辑时出错: {e}")
        
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

        # 2. 处理类型 (Genres)
        genres_raw = latest_series_data.get("genres", [])
        genres_list = [{"id": g.get('id', 0), "name": utils.GENRE_TRANSLATION_PATCH.get(g.get('name'), g.get('name'))} 
                       for g in genres_raw if isinstance(g, dict)]

        # 3. 处理关键词 (Keywords)
        keywords = latest_series_data.get("keywords", {}).get("results", [])
        keywords_json = [{"id": k["id"], "name": k["name"]} for k in keywords]

        # 4. 处理制作公司 (Production Companies) 
        production_companies = latest_series_data.get("production_companies", [])
        production_companies_json = [{"id": p["id"], "name": p["name"], "logo_path": p.get("logo_path")} for p in production_companies]

        # 5. 处理播出网络 (Networks) 
        networks = latest_series_data.get("networks", [])
        networks_json = [{"id": n["id"], "name": n["name"], "logo_path": n.get("logo_path")} for n in networks]

        # 6. 处理产地
        countries = latest_series_data.get("origin_country", [])
        countries_json = countries if isinstance(countries, list) else [countries]

        # ★★★ 综合提取剧集导演 (created_by + crew) 保持与 core_processor 一致 ★★★
        top_directors = helpers.extract_top_directors(latest_series_data, max_count=3)
        directors = [{'id': d['id'], 'name': d['name']} for d in top_directors]

        # 构造更新字典
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
            "genres_json": json.dumps(genres_list) if genres_list else None,
            "keywords_json": json.dumps(keywords_json) if keywords_json else None,
            "production_companies_json": json.dumps(production_companies_json) if production_companies_json else None,
            "networks_json": json.dumps(networks_json) if networks_json else None,
            "countries_json": json.dumps(countries_json) if countries_json else None,
            "directors_json": json.dumps(directors, ensure_ascii=False),
            "imdb_id": latest_series_data.get("external_ids", {}).get("imdb_id")
        }
        
        watchlist_db.update_media_metadata_fields(tmdb_id, 'Series', series_updates)
        logger.debug(f"  ➜ 已全量刷新 '{item_name}' 的 Series 元数据。")

        # 4. 处理季和集的数据 (保存 JSON + 收集列表)
        # 这里不需要再发网络请求了，直接从 aggregated_data 里拿
        all_tmdb_episodes = []
        
        for season_details in seasons_list:
            season_num = season_details.get("season_number")
            if season_num is None: continue
            
            # 👇 补回缺失的代码：提取该季的所有集并加入总列表
            episodes = season_details.get("episodes", [])
            all_tmdb_episodes.extend(episodes)
            
        # ★★★ 4.5 新增：并发下载缺失的图片 & 补全 NFO ★★★
        try:
            import extensions
            if extensions.media_processor_instance:
                current_item_details = None
                if item_id:
                    current_item_details = emby.get_emby_item_details(
                        item_id, self.emby_url, self.emby_api_key, self.emby_user_id
                    )

                # 1. 补全图片
                logger.debug(f"  ➜ 正在检查并下载 '{item_name}' 缺失的图片(含最新分集)...")
                extensions.media_processor_instance.download_images_from_tmdb(
                    tmdb_id=tmdb_id,
                    item_type='Series',
                    aggregated_tmdb_data=aggregated_data,
                    item_details=current_item_details
                )

                # 2. ★★★ 核心修复：NFO 模式下，追剧刷新必须补全 NFO 文件 ★★★
                if current_item_details:
                    logger.debug(f"  ➜ [NFO模式] 正在为 '{item_name}' 补全 NFO 文件...")
                    
                    # A. 构造正确的 Payload 结构 (将嵌套的 series_details 提级到根目录)
                    payload_for_nfo = latest_series_data.copy()
                    payload_for_nfo['seasons_details'] = aggregated_data.get('seasons_details', [])
                    payload_for_nfo['episodes_details'] = aggregated_data.get('episodes_details', {})
                    
                    # ★★★ 将辛苦抓取的导演强行塞入 payload，确保 NFO Builder 能读到 ★★★
                    # 兼容 NFO Builder 的读取习惯，把导演伪装成 crew 塞进 credits 里
                    if 'credits' not in payload_for_nfo:
                        payload_for_nfo['credits'] = {'crew': []}
                    elif 'crew' not in payload_for_nfo['credits']:
                        payload_for_nfo['credits']['crew'] = []
                        
                    existing_crew_ids = {c.get('id') for c in payload_for_nfo['credits']['crew'] if c.get('job') in ['Director', 'Series Director']}
                    for d in directors:
                        if d.get('id') not in existing_crew_ids:
                            payload_for_nfo['credits']['crew'].append({
                                'id': d.get('id'),
                                'name': d.get('name'),
                                'job': 'Director'
                            })
                    
                    # B. 从数据库逆向恢复之前精修过的演员表 (防止 NFO 演员表被清空)
                    _, db_actors = extensions.media_processor_instance._reconstruct_full_data_from_db(tmdb_id, 'Series')
                    
                    # C. 写入 NFO
                    extensions.media_processor_instance.sync_item_metadata(
                        item_details=current_item_details,
                        tmdb_id=tmdb_id,
                        final_cast_override=db_actors, # 传入从数据库恢复的精修演员表
                        metadata_override=payload_for_nfo, # 传入结构正确的 TMDb 数据
                        is_series_refresh=True # ★★★ 修复4：标记为追剧刷新模式，跳过 tvshow.nfo ★★★
                    )

        except Exception as e_img:
            logger.warning(f"  ➜ 追剧刷新时处理物理资产失败: {e_img}")

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
                        logger.info(f"  ➜ [自动待定] 触发: S{latest_season.get('season_number')} 上线{days_diff}天, 集数{episode_count} (阈值: {threshold_episodes})")
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
            auto_pause_days = int(watchlist_cfg.get('auto_pause', 0))
            enable_auto_pause = auto_pause_days > 0
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
                
                # --- B. 自动补订逻辑 ---
                if not exists:
                    # 只有【最新季】才允许自动补订
                    # 逻辑：S1-S3 没了就没了，不补；S4(最新) 没了必须补回来，因为要追更。
                    if s_num == latest_season_num:
                        if not enable_sync_sub:
                            logger.debug("  ➜ 自动补订开关关闭，跳过自动补订。")
                            continue
                        logger.info(f"  ➜ [MP同步] 发现《{series_name}》最新季 S{s_num} 在 MoviePilot 中无活跃订阅，正在自动补订...")
                        sub_success = moviepilot.subscribe_series_to_moviepilot(
                            series_info={'title': series_name, 'tmdb_id': tmdb_id},
                            season_number=s_num,
                            config=self.config
                        )
                        if not sub_success:
                            logger.warning(f"  ➜ [MP同步] 补订 S{s_num} 失败，跳过。")
                            continue
                        logger.info(f"  ➜ [MP同步] 《{series_name}》S{s_num} 补订成功。")
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

            # 检查一致性 (分辨率、制作组、编码)
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
                logger.info(f"  ➜ [一致性检查] S{season_number} 完美达标: [{res} / {grp}]，跳过洗版。")
                return True
            else:
                logger.info(f"  ➜ [一致性检查] S{season_number} 版本混杂，需要洗版。分布: 分辨率{resolutions}, 制作组{groups}, 编码{codecs}")
                return False

        except Exception as e:
            logger.error(f"  ➜ 检查 S{season_number} 一致性时出错: {e}")
            return False # 出错默认不跳过，继续洗版以防万一

    def _handle_auto_resub_ended(self, tmdb_id: str, series_name: str, season_number: int, episode_count: int):
        """
        针对指定季进行完结洗版。
        参数直接传入季号和集数，不再需要在内部计算。
        """
        try:
            logger.info(f"  🎉 剧集《{series_name}》已自然完结，正在对最终季 (S{season_number}) 执行洗版流程...")
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            # 1.检查配额
            if settings_db.get_subscription_quota() <= 0:
                logger.warning(f"  ➜ 每日订阅配额已用尽，跳过《{series_name}》S{season_number} 的完结洗版。")
                return
            # 2. 直接使用传入的集数进行一致性检查
            if self._check_season_consistency(tmdb_id, season_number, episode_count):
                return
            
            # 3. 检查是否需要删除旧文件 (Emby)
            if watchlist_cfg.get('auto_delete_old_files', False):
                logger.info(f"  ➜ [自动清理] 检测到“删除 Emby 旧文件”已开启，正在评估删除范围...")
                try:
                    # 调用 DB 层获取剧集 Emby ID 和所有在库的季
                    series_emby_id, in_library_seasons = watchlist_db.get_series_deletion_info(tmdb_id)
                    
                    # 过滤掉当前准备删除的季，看看还有没有剩下的季 (包括 SP/第0季)
                    other_seasons = [s for s in in_library_seasons if s != season_number]

                    if not other_seasons and series_emby_id:
                        # 只有这一季，直接删除整部剧，防止留下空壳
                        logger.info(f"  ➜ [自动清理] 剧集下无其他在库季，准备直接删除整部剧集 (Emby ID: {series_emby_id})...")
                        if emby.delete_item(series_emby_id, self.emby_url, self.emby_api_key, self.emby_user_id):
                            logger.info(f"  ➜ [自动清理] 已成功从 Emby 删除整部剧集。")
                            time.sleep(2)
                        else:
                            logger.error(f"  ➜ [自动清理] 删除整部剧集失败，将继续执行洗版订阅。")
                    else:
                        # 还有其他季，仅删除当前季
                        target_season_id = watchlist_db.get_season_emby_id(tmdb_id, season_number)
                        if target_season_id:
                            logger.info(f"  ➜ [自动清理] 剧集下还有其他季 {other_seasons}，仅删除 S{season_number} (Emby ID: {target_season_id})...")
                            if emby.delete_item(target_season_id, self.emby_url, self.emby_api_key, self.emby_user_id):
                                logger.info(f"  ➜ [自动清理] 已成功从 Emby 删除 S{season_number}。")
                                time.sleep(2)
                            else:
                                logger.error(f"  ➜ [自动清理] 删除 S{season_number} 失败，将继续执行洗版订阅。")
                        else:
                            logger.warning(f"  ➜ [自动清理] 数据库中未找到 S{season_number} 的 Emby ID，跳过删除。")

                except Exception as e:
                    logger.error(f"  ➜ [自动清理] 执行删除逻辑时出错: {e}")
            
            # 4. 删除整理记录 (MoviePilot) - 
            related_hashes = []
            if watchlist_cfg.get('auto_delete_mp_history', False):
                logger.info(f"  ➜ [自动清理] 正在删除 MoviePilot 整理记录...")
                related_hashes = moviepilot.delete_transfer_history(tmdb_id, season_number, series_name, self.config)

                # 4-1. 清理下载器中的旧任务 -
                if watchlist_cfg.get('auto_delete_download_tasks', False):
                    logger.info(f"  ➜ [自动清理] 正在删除下载器旧任务...")
                    moviepilot.delete_download_tasks(series_name, self.config, hashes=related_hashes)

            # 5. 取消旧订阅
            moviepilot.cancel_subscription(tmdb_id, 'Series', self.config, season=season_number)
            
            # 6. 发起新订阅 (洗版)
            payload = {
                "name": series_name,
                "tmdbid": int(tmdb_id),
                "type": "电视剧",
                "season": season_number,
                "best_version": 1 # ★ 核心：洗版模式
            }
            
            if moviepilot.subscribe_with_custom_payload(payload, self.config):
                settings_db.decrement_subscription_quota()
                logger.info(f"  ➜ [完结洗版] 《{series_name}》 第 {season_number} 季 已提交洗版订阅。")
                
                # ★★★ 新增：点亮洗版特权灯，让 Organizer 强制替换 ★★★
                try:
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("UPDATE media_metadata SET active_washing = TRUE WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
                            conn.commit()
                except Exception as e:
                    logger.error(f"  ➜ 开启洗版状态失败: {e}")
            else:
                logger.error(f"  ➜ [完结洗版] 《{series_name}》 第 {season_number} 季 提交失败。")

        except Exception as e:
            logger.error(f"  ➜ 执行完结自动洗版逻辑时出错: {e}", exc_info=True)

    # --- 尝试从豆瓣获取总集数 ---
    def _try_fetch_douban_episode_count(self, series_name: str, season_number: int, year: str, imdb_id: Optional[str] = None) -> Optional[int]:
        """
        尝试从豆瓣获取剧集的总集数。
        策略：
        1. 优先使用 IMDb ID (如果提供)。
        2. 否则使用名称搜索：
           - 第1季: 剧名 + 年份
           - 第N季: 剧名 + 季号 + 年份 (如 "乡村爱情18 2026")
        """
        if not self.douban_api or not self.config.get(constants.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API, True):
            return None

        try:
            # --- 构造搜索条件 ---
            search_name = series_name
            
            # 如果是第2季及以上，修改搜索名称为 "剧名+季号"
            if season_number > 1:
                search_name = f"{series_name}{season_number}"
            
            logger.debug(f"  ➜ [豆瓣辅助] 准备查询 《{series_name}》第 {season_number} 季 集数。IMDb: {imdb_id}, 搜索名: {search_name}, 年份: {year}")

            # 1. 搜索/匹配豆瓣条目 (match_info 内部优先处理 IMDb ID)
            match_result = self.douban_api.match_info(
                name=search_name, 
                imdbid=imdb_id, 
                mtype='tv', 
                year=year
            )
            
            if not match_result or not match_result.get('id'):
                logger.debug(f"  ➜ [豆瓣辅助] 未匹配到豆瓣条目: {search_name}")
                return None
            
            douban_id = match_result['id']
            
            # 2. 获取详情 (使用 protected 方法 _get_subject_details)
            details = self.douban_api._get_subject_details(douban_id, "tv")
            
            if details and not details.get("error"):
                # 优先读取 episodes_count (int)
                ep_count = details.get('episodes_count')
                
                # 兜底：有时候豆瓣返回的是字符串
                if not ep_count and details.get('episodes_count_str'):
                     try: ep_count = int(details.get('episodes_count_str'))
                     except: pass
                
                if ep_count:
                    logger.debug(f"  ➜ [豆瓣辅助] 获取成功: ID {douban_id} ({details.get('title')}) -> {ep_count} 集")
                    return int(ep_count)
            
            return None

        except Exception as e:
            logger.warning(f"  ➜ 尝试从豆瓣获取集数失败 (《{series_name}》第 {season_number} 季): {e}")
            return None
    
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
        auto_pause_days = int(watchlist_cfg.get('auto_pause', 0))
        enable_auto_pause = auto_pause_days > 0

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
            
            # 2. 获取豆瓣辅助修正开关配置
            enable_douban_correction = watchlist_cfg.get('douban_count_correction', False)
            
            # A. 确定最新季
            tmdb_seasons_list = latest_series_data.get('seasons', [])
            valid_tmdb_seasons = sorted(
                [s for s in tmdb_seasons_list if s.get('season_number', 0) > 0], 
                key=lambda x: x['season_number'], 
                reverse=True
            )
            
            if valid_tmdb_seasons:
                latest_season_info = valid_tmdb_seasons[0]
                latest_s_num = latest_season_info.get('season_number')
                current_tmdb_count = latest_season_info.get('episode_count', 0)
                
                # B. 检查锁定状态
                is_locked = False
                if seasons_lock_map and latest_s_num in seasons_lock_map:
                    is_locked = seasons_lock_map[latest_s_num].get('locked', False)
                
                # C. 如果未锁定，尝试查询豆瓣
                if not is_locked and self.config.get(constants.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API, True) and enable_douban_correction:
                    release_date = latest_season_info.get('air_date') or latest_series_data.get('first_air_date')
                    year = release_date[:4] if release_date else ""
                    
                    # 尝试获取该剧的 IMDb ID（如果是 S1，且 TMDb 有提供剧集级 IMDb ID，则使用它；否则不传）
                    target_imdb_id = None
                    
                    # 策略：
                    # 1. 如果是第 1 季，使用剧集(Series)层面的 IMDb ID。
                    #    (TMDb 的 aggregate_full_series_data_from_tmdb 已经请求了 external_ids，直接取即可，无需额外请求)
                    # 2. 如果是第 2+ 季，强制不使用 IMDb ID。
                    #    (因为主剧的 IMDb ID 在豆瓣通常只对应 S1，传了反而可能导致 S2 匹配成 S1 的数据)
                    
                    if latest_s_num == 1:
                        external_ids = latest_series_data.get('external_ids', {})
                        target_imdb_id = external_ids.get('imdb_id')
                        
                        if target_imdb_id:
                            logger.trace(f"  ➜ [豆瓣辅助] 《{item_name}》 -> IMDb ID: {target_imdb_id}")
                        else:
                            logger.trace(f"  ➜ [豆瓣辅助] 《{item_name}》 未找到 IMDb ID，将回退到名称搜索。")
                    else:
                        logger.debug(f"  🔀 [豆瓣辅助] 《{item_name}》第 {latest_s_num} 季 非首季，将使用名称+季号搜索。")

                    # ==============================================================================
                    
                    douban_count = self._try_fetch_douban_episode_count(
                        series_name=item_name, 
                        season_number=latest_s_num, 
                        year=year,
                        imdb_id=target_imdb_id # ★ 传入处理后的 ID
                    )
                    
                    # 信任豆瓣权威数据，查到即锁定
                    if douban_count and douban_count > 0:
                        # 优化日志显示：如果数字变了叫“修正”，没变叫“加锁保护”
                        if douban_count != current_tmdb_count:
                            logger.info(f"  ✨ [豆瓣修正] 《{item_name}》第{latest_s_num}季 TMDb集数({current_tmdb_count}) -> 豆瓣集数({douban_count})。正在锁定...")
                        else:
                            logger.info(f"  ➜ [豆瓣锁定] 《{item_name}》第{latest_s_num}季 集数与豆瓣一致({douban_count})。正在锁定以防TMDb变动...")
                        
                        # 1. 更新数据库并锁定 (locked=True)
                        watchlist_db.update_specific_season_total_episodes(
                            tmdb_id, latest_s_num, douban_count, locked=True
                        )
                        
                        # 2. ★★★ 关键：立即更新内存中的数据，以便后续逻辑使用新集数 ★★★
                        latest_season_info['episode_count'] = douban_count
                        # 如果是单季剧，同步更新 series 级的 total_episodes
                        if len(valid_tmdb_seasons) == 1:
                            latest_series_data['number_of_episodes'] = douban_count
                            
                        # 3. 刷新一下锁缓存，防止下面逻辑出错
                        if not seasons_lock_map: seasons_lock_map = {}
                        seasons_lock_map[latest_s_num] = {'locked': True, 'count': douban_count}
                    
                    else:
                        logger.debug(f"  ➜ [豆瓣辅助] 《{item_name}》第{latest_s_num}季 未获取到有效集数，跳过修正。")
                else:
                    if is_locked:
                        logger.debug(f"  ➜ 《{item_name}》第{latest_s_num}季 已锁定为 {seasons_lock_map[latest_s_num].get('count')} 集，跳过豆瓣修正。")
                    else:
                        logger.debug(f"  ➜ 《{item_name}》第{latest_s_num}季 未锁定，但豆瓣修正未启用，跳过。")
            
            if seasons_lock_map:
                for season_obj in latest_series_data.get('seasons', []):
                    s_num = season_obj.get('season_number')
                    # 如果该季在锁定表中，且已启用锁定
                    if s_num in seasons_lock_map and seasons_lock_map[s_num].get('locked'):
                        locked_count = seasons_lock_map[s_num].get('count')
                        # 如果 TMDb 原生集数与锁定集数不一致，强制覆盖
                        if locked_count is not None and season_obj.get('episode_count') != locked_count:
                            logger.debug(f"  ➜ [元数据同步] 将 S{s_num} 的总集数由 TMDb({season_obj.get('episode_count')}) 修正为锁定值({locked_count})，以便正确判定完结。")
                            season_obj['episode_count'] = locked_count
                            
                            # 如果是单季剧，通常 series 级的 number_of_episodes 也需要修正
                            if len(valid_tmdb_seasons) == 1:
                                latest_series_data['number_of_episodes'] = locked_count
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
                            logger.info(f"  ➜ [分季锁定生效] S{s_num} 锁定为 {lock_count} 集，正在剔除 TMDb 多余集数 (如 S{s_num}E{e_num})...")
                        continue
                    
                    # 否则保留该集
                    filtered_episodes.append(ep)
                
                if discarded_count > 0:
                    logger.info(f"  ➜ 共剔除了 {discarded_count} 个不符合分季锁定规则的集。")
                    all_tmdb_episodes = filtered_episodes
            
            else:
                # 如果没查到任何季信息（罕见），就不做过滤
                pass

        except Exception as e:
            logger.error(f"  ➜ 执行分季锁定过滤时出错: {e}", exc_info=True)

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
                        logger.debug(f"  ➜ [预判] S{last_s_num} 总集数({total_ep_count}) > 保护阈值({aggressive_threshold}) 且已播至最后一集，标记为本季大结局。")

        # ==============================================================================
        # ★★★ 激进完结策略 ★★★
        # ==============================================================================
        is_aggressive_completed = False
        
        # ★ 新增：读取 TG 追更配置
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        tg_channel_tracking = watchlist_cfg.get('tg_channel_tracking', False)
        
        # 1. 找到最新一季的信息
        tmdb_seasons_list = latest_series_data.get('seasons', [])
        valid_tmdb_seasons = sorted(
            [s for s in tmdb_seasons_list if s.get('season_number', 0) > 0], 
            key=lambda x: x['season_number'], 
            reverse=True
        )

        local_latest_s_episodes = 0
        latest_s_total_episodes = 0
        latest_s_num = 0

        if valid_tmdb_seasons:
            latest_s_info = valid_tmdb_seasons[0]
            latest_s_num = latest_s_info.get('season_number')
            # TMDb 记录的最新季总集数
            latest_s_total_episodes = latest_s_info.get('episode_count', 0)
            
            # 本地已入库的最新季集数
            local_latest_s_episodes = len(emby_seasons.get(latest_s_num, set()))

            # 2. 获取最新播出集的信息 (用于时间判定)
            last_ep_number = 0
            last_air_date = None
            if last_episode_to_air:
                # 只有当最后播出集属于最新一季时，才参与进度判定
                if last_episode_to_air.get('season_number') == latest_s_num:
                    last_ep_number = last_episode_to_air.get('episode_number', 0)
                
                if date_str := last_episode_to_air.get('air_date'):
                    try:
                        last_air_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError: pass

            # 3. 核心判定逻辑 (针对最新季)
            # 只有当最新季集数超过保护阈值时才触发
            if latest_s_total_episodes > aggressive_threshold:
                
                # 条件 A: 时间维度 (最新季的最后一集已播出)
                if last_ep_number >= latest_s_total_episodes and last_air_date and last_air_date <= today:
                    # ★ 核心修改：如果开启了 TG 追更，禁用时间维度的激进完结！必须等本地集齐！
                    if tg_channel_tracking:
                        logger.debug(f"  ➜ [TG追更保护] 大结局(E{last_ep_number})已播出，但因开启了TG追更，禁用时间维度的激进完结，等待本地集齐。")
                    else:
                        is_aggressive_completed = True
                        logger.info(f"  ➜ 《{item_name}》 （第 {latest_s_num} 季） 大结局(E{last_ep_number})已播出，判定完结。")
                
                # 条件 B: 收藏维度 (最新季本地已集齐)
                if not is_aggressive_completed and local_latest_s_episodes >= latest_s_total_episodes:
                    is_aggressive_completed = True
                    logger.info(f"  ➜ 《{item_name}》 （第 {latest_s_num} 季） 本地已集齐 {local_latest_s_episodes}/{latest_s_total_episodes} 集，判定完结。")

        # ==============================================================================
        # ★★★ 重构后的状态判定逻辑 ★★★
        # ==============================================================================

        # 如果开启了 TG 追更，且本地最新季还没集齐，绝对不允许系统将其判定为“已完结”！
        tg_protection_active = tg_channel_tracking and (local_latest_s_episodes < latest_s_total_episodes) and (latest_s_total_episodes > 0)

        # 防无限洗版死循环 
        # 如果旧状态已经是“已完结”，且没有明确的未来新集排期，说明当前的“缺集”是因为洗版删除了旧文件。
        # 此时必须临时解除 TG 保护锁，防止状态倒退回“追剧中”从而引发无限洗版。
        if tg_protection_active and old_status == STATUS_COMPLETED and not effective_next_episode:
            logger.info(f"  ➜ [防洗版死循环] 《{item_name}》当前缺集，但旧状态已是完结，判定为洗版下载中，临时解除 TG 追更保护锁。")
            tg_protection_active = False

        # 规则 1: 激进策略优先 -> 直接完结
        if is_aggressive_completed:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            
            if real_next_episode_to_air:
                logger.info(f"  ➜ [判定-TMDb已完结] 虽本地缺集，但满足完结策略，强制判定“已完结”以触发洗版(抢完结包)。")
            else:
                logger.info(f"  ➜ [判定-本地已集齐] 满足完结策略，判定“已完结”。")

        # 规则 2: TMDb 状态已完结 -> 直接完结 (不考虑本地是否集齐)
        elif is_ended_on_tmdb:
            # ★ 核心修改：TG 保护锁拦截
            if tg_protection_active:
                final_status = STATUS_WATCHING
                paused_until_date = None
                logger.info(f"  ➜ [TG追更保护] TMDb状态虽为 '{new_tmdb_status}'，但本地未集齐 ({local_latest_s_episodes}/{latest_s_total_episodes})，强制保持“追剧中”以接收TG推送。")
            else:
                final_status = STATUS_COMPLETED
                paused_until_date = None
                logger.info(f"  ➜ [判定-规则1] TMDb状态为 '{new_tmdb_status}'，判定为“已完结”。")

        # 规则 3: 本季大结局已播出 (且无明确下一集) -> 直接完结 (不考虑本地是否集齐)
        elif is_season_finale and not effective_next_episode:
            is_suspiciously_short = (new_tmdb_status == "Returning Series" and total_ep_count <= 3)
            
            if is_suspiciously_short and days_since_last <= 7:
                final_status = STATUS_WATCHING
                paused_until_date = None
                logger.info(f"  ➜ [安全锁生效] 虽检测到疑似大结局 (S{last_s_num}E{last_e_num})，但该季仅 {total_ep_count} 集且刚播出 {days_since_last} 天，判定为数据滞后，保持“追剧中”。")
            else:
                # ★ 核心修改：TG 保护锁拦截
                if tg_protection_active:
                    final_status = STATUS_WATCHING
                    paused_until_date = None
                    logger.info(f"  ➜ [TG追更保护] 本季大结局虽已播出，但本地未集齐 ({local_latest_s_episodes}/{latest_s_total_episodes})，强制保持“追剧中”以接收TG推送。")
                else:
                    final_status = STATUS_COMPLETED
                    paused_until_date = None
                    logger.info(f"  ➜ [判定-规则2] 本季大结局 (S{last_s_num}E{last_e_num}) 已播出，判定为“已完结”。")

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
                    # 子规则 A: 播出时间 >= 设定天数 -> 设为“暂停”
                    if enable_auto_pause and days_until_air >= auto_pause_days:
                        final_status = STATUS_PAUSED
                        paused_until_date = air_date
                        logger.info(f"  ➜ [判定-连载中] (第 {episode_number} 集) 将在 {days_until_air} 天后播出 (阈值: {auto_pause_days}天)，设为“已暂停”。")
                    # 子规则 B: 即将播出 -> 设为“追剧中”
                    else:
                        final_status = STATUS_WATCHING
                        paused_until_date = None
                        logger.info(f"  ➜ [判定-连载中] (第 {episode_number} 集) 将在 {days_until_air} 天内 ({air_date}) 播出，设为“追剧中”。")

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

                    # ==============================================================================
                    # ★★★ 核心修复：兜底逻辑也要“不见兔子不撒鹰” ★★★
                    # 如果 TMDb 认为的“当前正在播出的季 (last_s_num)”在本地根本不存在，
                    # 说明用户根本没开始追这一季，直接判定为完结！
                    # ==============================================================================
                    has_local_last_season = last_s_num in emby_seasons if last_s_num else False

                    if not has_local_last_season:
                        final_status = STATUS_COMPLETED
                        paused_until_date = None
                        logger.info(f"  zzz [判定-未入库] 虽本季尚未播完，但本地无该季 (S{last_s_num}) 任何文件，判定为“已完结”。")
                    
                    # 2. 核心判断：
                    # 条件：状态是“连载中” AND (当前季总集数 > 0) AND (已播集号 < 总集数)
                    # 只要满足这个条件，说明这季还没播完，绝对不能判完结。
                    elif new_tmdb_status == "Returning Series" and last_e_num and current_season_total > 0 and last_e_num < current_season_total:
                        final_status = STATUS_WATCHING
                        paused_until_date = None
                        logger.info(f"  ➜ [判定-连载中] 虽无未来排期，但本季尚未播完 (进度: S{last_s_num} - {last_e_num}/{current_season_total})，判定为数据滞后，保持“追剧中”。")
                    
                    # 否则 -> 判定完结
                    else:
                        final_status = STATUS_COMPLETED
                        paused_until_date = None
                        logger.info(f"  ➜ [判定-已完结] 无待播集信息，且本季已完结或剧集已完结 (进度: S{last_s_num} - {last_e_num}/{current_season_total})。")
                
                else:
                    # 极端情况：无任何日期信息
                    final_status = STATUS_WATCHING
                    paused_until_date = None
                    logger.info(f"  ➜ [判定-连载中] 缺乏播出日期数据，默认保持“追剧中”状态。")

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
                logger.info(f"  ➜ [自动待定生效] 《{item_name}》虽被判定完结，但满足新剧保护条件，状态强制修正为 '待定 (Pending)'。")

        # 手动强制完结
        if is_force_ended and final_status != STATUS_COMPLETED:
            final_status = STATUS_COMPLETED
            paused_until_date = None
            logger.warning(f"  ➜ [强制完结生效] 最终状态被覆盖为 '已完结'。")

        # 只有当内部状态是“追剧中”或“已暂停”时，才认为它在“连载中”
        is_truly_airing = final_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_PENDING]
        logger.info(f"  ➜ 最终判定 '{item_name}' 的真实连载状态为: {is_truly_airing} (内部状态: {translate_internal_status(final_status)})")

        # ======================================================================
        # ★★★ 完结自动洗版逻辑 (TG解耦 + 标志位驱动) ★★★
        # ======================================================================
        logger.debug(f"  ➜ [状态流转] 剧名: {item_name}, 旧状态: {translate_internal_status(old_status)}, 新状态: {translate_internal_status(final_status)}")
        
        # 定义一个变量，用于控制是否更新等待标志
        set_waiting_flag = None

        if final_status == STATUS_COMPLETED and old_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_PENDING] and not is_force_ended:
            watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
            if watchlist_cfg.get('auto_resub_ended', False):
                seasons = latest_series_data.get('seasons', [])
                valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])
                
                if valid_seasons:
                    target_season = valid_seasons[-1]
                    last_s_num = target_season.get('season_number')
                    last_ep_count = target_season.get('episode_count', 0)
                    
                    tg_channel_tracking = watchlist_cfg.get('tg_channel_tracking', False)
                    
                    if tg_channel_tracking:
                        if self._check_season_consistency(tmdb_id, last_s_num, last_ep_count):
                            logger.info(f"  ➜ [TG洗版拦截] 《{item_name}》S{last_s_num} 本地文件一致性完美，无需洗版。")
                        else:
                            # ★ 核心：不一致，开启等待标志！
                            set_waiting_flag = True
                            logger.info(f"  ➜ [TG洗版拦截] 《{item_name}》S{last_s_num} 完结但文件不一致。已开启 '等待完结包' 标志，静候 TG 频道发布。")
                    else:
                        # 未开启 TG 追更，走原来的 MP 洗版逻辑
                        logger.info(f"  ➜ [完结洗版] 《{item_name}》由 {translate_internal_status(old_status)} 转为完结，立即提交 MP 洗版。")
                        self._handle_auto_resub_ended(tmdb_id, item_name, last_s_num, last_ep_count)

        # 如果剧集恢复连载（出新季了），必须清除等待标志，防止误判
        if final_status in [STATUS_WATCHING, STATUS_PAUSED, STATUS_PENDING]:
            set_waiting_flag = False

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
        
        # ★ 将标志位合入数据库更新字典
        if set_waiting_flag is not None:
            updates_to_db['waiting_for_completed_pack'] = set_waiting_flag
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

        # ======================================================================
        # ★★★ 提前计算季的活跃状态 (供数据库同步和目录重组使用) ★★★
        # ======================================================================
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
        # 规则 D (兜底规则)
        valid_local_seasons = [s for s in emby_seasons.keys() if s > 0]
        if valid_local_seasons:
            active_seasons.add(max(valid_local_seasons))
        else:
            tmdb_seasons_list = latest_series_data.get('seasons', [])
            valid_tmdb_seasons = [s for s in tmdb_seasons_list if s.get('season_number', 0) > 0]
            if valid_tmdb_seasons:
                active_seasons.add(max(s['season_number'] for s in valid_tmdb_seasons))

        # ======================================================================
        # ★★★ 追剧目录自动重组 (大脑指挥官 - 优雅在播季限定版) ★★★
        # ======================================================================
        if self.p115_enable_organize:
            try:
                # 判断是否发生了关键的状态流转
                status_changed_to_watching = (old_status in [None, 'NONE'] and final_status in ['Watching', 'Paused', 'Pending'])
                status_changed_to_completed = (old_status in ['Watching', 'Paused', 'Pending'] and final_status == 'Completed')

                if status_changed_to_watching or status_changed_to_completed:
                    logger.info(f"  ➜ [智能追剧] 检测到状态流转 ({old_status} -> {final_status})，准备重新评估 115 目录分类...")
                    
                    target_seasons_for_move = set(active_seasons)
                    valid_local_seasons = [s for s in emby_seasons.keys() if s > 0]
                    if valid_local_seasons:
                        target_seasons_for_move.add(max(valid_local_seasons))

                    if not target_seasons_for_move:
                        logger.debug("  ➜ [智能追剧] 未找到有效的目标季，跳过重组。")
                    else:
                        from handler.p115_service import P115Service, SmartOrganizer, ManualCorrectTaskQueue
                        from database.connection import get_db_connection
                        
                        client = P115Service.get_client()
                        if client:
                            # 1. 提前计算出新的目标目录 CID
                            organizer = SmartOrganizer(client, tmdb_id, 'tv', item_name)
                            new_target_cid = organizer.get_target_cid(ignore_memory=True)
                            
                            if new_target_cid:
                                records_to_process = []
                                skipped_count = 0
                                
                                with get_db_connection() as conn:
                                    with conn.cursor() as cursor:
                                        # ★ 核心优化 1：直接查出 season_number 和 target_cid
                                        cursor.execute("SELECT id, season_number, target_cid FROM p115_organize_records WHERE tmdb_id = %s", (str(tmdb_id),))
                                        all_records = cursor.fetchall()
                                
                                for row in all_records:
                                    s_num = row['season_number']
                                    current_cid = row['target_cid']
                                    
                                    # ★ 核心优化 2：如果当前目录已经等于目标目录，静默跳过！(防原地摩擦)
                                    if str(current_cid) == str(new_target_cid):
                                        skipped_count += 1
                                        continue

                                    # ★ 核心优化 3：直接使用数据库里的季号，告别正则！
                                    if s_num and s_num in target_seasons_for_move:
                                        records_to_process.append((row['id'], s_num))
                                    elif not s_num and len(target_seasons_for_move) == 1:
                                        # 兜底：如果实在没有季号(如花絮)，但目标季只有一个，兜底带上
                                        records_to_process.append((row['id'], list(target_seasons_for_move)[0]))

                                if records_to_process:
                                    logger.info(f"  ➜ [智能追剧] 重新匹配出新目录 CID: {new_target_cid}，精准锁定 {len(records_to_process)} 个需要移动的文件加入重组队列 (目标季: {list(target_seasons_for_move)})。")
                                    for rid, s_num in records_to_process:
                                        ManualCorrectTaskQueue.add(rid, tmdb_id, 'tv', new_target_cid, s_num)
                                else:
                                    if skipped_count > 0:
                                        logger.info(f"  ➜ [智能追剧] 目标季的文件已在正确目录 (CID: {new_target_cid})，无需移动，跳过重组。")
                                    else:
                                        logger.debug("  ➜ [智能追剧] 未找到需要移动的文件。")
            except Exception as e:
                logger.error(f"  ➜ 触发 115 自动分类迁移失败: {e}", exc_info=True)

        # 调用 DB 模块进行批量更新 (使用上面提前算好的 active_seasons)
        watchlist_db.sync_seasons_watching_status(tmdb_id, list(active_seasons), final_status)

        # ======================================================================
        # ★★★ MP 状态接管与同步 (自动待定 & 自动暂停) ★★★
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