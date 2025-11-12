# actor_subscription_processor.py

import time
import re
import json
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any, List, Set, Callable, Tuple
import threading
from enum import Enum
import concurrent.futures # 新增：导入 concurrent.futures

import handler.tmdb as tmdb
import handler.emby as emby
from database.connection import get_db_connection
from database import media_db, actor_db
import constants
import utils

logger = logging.getLogger(__name__)

class MediaStatus(Enum):
    IN_LIBRARY = 'IN_LIBRARY'
    PENDING_RELEASE = 'PENDING_RELEASE'
    SUBSCRIBED = 'SUBSCRIBED'
    MISSING = 'MISSING'
    IGNORED = 'IGNORED'

class MediaType(Enum):
    MOVIE = 'Movie'
    SERIES = 'Series'

class ActorSubscriptionProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tmdb_api_key = config.get('tmdb_api_key')
        self.emby_url = config.get('emby_server_url')
        self.emby_api_key = config.get('emby_api_key')
        self.emby_user_id = config.get('emby_user_id')
        self.subscribe_delay_sec = config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5)
        self._stop_event = threading.Event()
        self._quota_warning_logged = False

    def signal_stop(self):
        self._stop_event.set()

    def is_stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def clear_stop_signal(self):
        self._stop_event.clear()

    def close(self):
        logger.trace("ActorSubscriptionProcessor closed.")

    def run_scheduled_task(self, update_status_callback: Optional[Callable] = None):
        def _update_status(progress, message):
            if update_status_callback:
                safe_progress = max(0, min(100, int(progress)))
                update_status_callback(safe_progress, message)

        logger.trace("--- 开始执行定时刷新演员订阅任务 ---")
        _update_status(0, "正在准备订阅列表...")

        self._quota_warning_logged = False
        
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, actor_name FROM actor_subscriptions WHERE status = 'active'")
                subs_to_process = cursor.fetchall()
        except Exception as e:
            logger.error(f"定时任务：获取启用的订阅列表时失败: {e}", exc_info=True)
            _update_status(-1, "错误：获取订阅列表失败。")
            return
            
        if not subs_to_process:
            logger.info("  ➜ 没有找到需要处理的演员订阅，任务结束。")
            _update_status(100, "没有需要处理的演员订阅。")
            return
            
        total_subs = len(subs_to_process)
        logger.info(f"  ➜ 共找到 {total_subs} 个启用的订阅需要处理。")
        
        _update_status(5, "  ➜ 正在从 Emby 获取媒体库信息...")
        logger.info("  ➜ 正在从 Emby 一次性获取全量媒体库及剧集结构数据...")
        emby_media_map: Dict[str, str] = {}
        emby_series_seasons_map: Dict[str, Set[int]] = {}
        emby_series_name_to_tmdb_id_map: Dict[str, str] = {}

        try:
            # ... (这部分获取 Emby 数据的逻辑完全不变) ...
            all_libraries = emby.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id)
            library_ids_to_scan = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['movies', 'tvshows']]
            emby_items = emby.get_emby_library_items(base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id, library_ids=library_ids_to_scan, media_type_filter="Movie,Series")
            
            if self.is_stop_requested():
                logger.info("任务在获取Emby媒体库后被用户中断。")
                return

            series_to_check = []
            for item in emby_items:
                tmdb_id = item.get('ProviderIds', {}).get('Tmdb')
                if tmdb_id:
                    emby_media_map[tmdb_id] = item['Id']
                    if item.get('Type') == 'Series':
                        series_to_check.append({'tmdb_id': tmdb_id, 'emby_id': item['Id']})
                        normalized_name = utils.normalize_name_for_matching(item.get('Name', ''))
                        if normalized_name:
                            emby_series_name_to_tmdb_id_map[normalized_name] = tmdb_id

            logger.debug(f"  ➜ 已从 Emby 获取 {len(emby_media_map)} 个媒体的基础映射。")
            
            if series_to_check:
                logger.info(f"  ➜ 正在为 {len(series_to_check)} 个剧集获取季结构...")
                for series in series_to_check:
                    seasons = emby.get_series_children(
                        series_id=series['emby_id'],
                        base_url=self.emby_url,
                        api_key=self.emby_api_key,
                        user_id=self.emby_user_id,
                        include_item_types="Season",
                        fields="IndexNumber"
                    )
                    if seasons:
                        season_numbers = {s.get('IndexNumber') for s in seasons if s.get('IndexNumber') is not None}
                        if season_numbers:
                            emby_series_seasons_map[series['tmdb_id']] = season_numbers
                logger.debug(f"  ➜ 成功构建了 {len(emby_series_seasons_map)} 个剧集的季信息映射。")
        except Exception as e:
            logger.error(f"  ➜ 从 Emby 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            _update_status(-1, "错误：连接 Emby 或获取数据失败。")
            return

        # ★★★ 核心修正：不再需要 session_subscribed_ids ★★★

        for i, sub in enumerate(subs_to_process):
            if self.is_stop_requested():
                logger.info("定时刷新演员订阅任务被用户中断。")
                break
            
            progress = int(5 + ((i + 1) / total_subs) * 95)
            message = f"  ➜ ({i+1}/{total_subs}) 正在扫描演员: {sub['actor_name']}"
            _update_status(progress, message)
            logger.info(message)
            
            # ★★★ 核心修正：调用正确的函数签名 ★★★
            self.run_full_scan_for_actor(
                sub['id'], 
                emby_media_map, 
                emby_series_seasons_map, 
                emby_series_name_to_tmdb_id_map
            )
            
            if not self.is_stop_requested() and i < total_subs - 1:
                time.sleep(1) 
                
        if not self.is_stop_requested():
            logger.trace("--- 定时刷新演员订阅任务执行完毕 ---")
            _update_status(100, "  ➜ 所有订阅扫描完成。")


    def run_full_scan_for_actor(self, subscription_id: int, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str]):
        """【V2 - 新架构核心处理器】"""
        logger.trace(f"--- 开始为订阅ID {subscription_id} 执行全量作品扫描 (新架构) ---")
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
                sub = cursor.fetchone()
                if not sub: return
                
                actor_name = sub['actor_name']
                tmdb_person_id = sub['tmdb_person_id']
                logger.trace(f"  ➜ 正在处理演员: {actor_name} (TMDb ID: {tmdb_person_id})")

                old_tracked_media = actor_db.get_single_subscription_details(subscription_id)
                
                credits = tmdb.get_person_credits_tmdb(sub['tmdb_person_id'], self.tmdb_api_key)
                if self.is_stop_requested() or not credits: return
                
                movie_works = credits.get('movie_credits', {}).get('cast', [])
                tv_works = credits.get('tv_credits', {}).get('cast', [])
                for work in movie_works: work['media_type'] = 'movie'
                for work in tv_works: work['media_type'] = 'tv'
                all_works_raw = movie_works + tv_works
                # 步骤 1: 按标准化的标题对所有作品进行分组
                work_groups = {}
                for work in all_works_raw:
                    # 确保有ID，否则是无效数据
                    if not work.get('id'):
                        continue
                    
                    title = work.get('title') or work.get('name', '')
                    normalized_title = utils.normalize_name_for_matching(title)
                    if not normalized_title:
                        continue
                        
                    if normalized_title not in work_groups:
                        work_groups[normalized_title] = []
                    work_groups[normalized_title].append(work)

                # 步骤 2: 在每个分组内，选出 popularity 最高的作为唯一代表
                unique_works = []
                for title, group in work_groups.items():
                    if len(group) == 1:
                        # 没有重复，直接采纳
                        unique_works.append(group[0])
                    else:
                        # 有重复，开始“竞选”
                        best_work = max(group, key=lambda x: x.get('popularity', 0))
                        unique_works.append(best_work)
                        
                        # 记录日志，让我们知道发生了什么
                        discarded_ids = [w['id'] for w in group if w['id'] != best_work['id']]
                        logger.debug(f"  ➜ 在同名作品组 '{title}' 中，保留了热度最高的条目 (ID: {best_work['id']})，忽略了其他重复条目 (IDs: {discarded_ids})。")

                # 步骤 3: 使用清洗后的唯一作品列表进行后续所有操作
                all_works_raw = unique_works
                
                unique_works = self._deduplicate_works(credits)
                logger.info(f"  ➜ 从TMDb获取到演员 {actor_name} 的 {len(unique_works)} 部【去重后】的唯一作品记录。")

                # 步骤 2: 获取当前已经被此订阅追踪的所有媒体
                source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
                cursor.execute(
                    "SELECT tmdb_id, item_type, subscription_status FROM media_metadata WHERE subscription_sources_json @> %s::jsonb",
                    (source_filter,)
                )
                old_tracked_media = {row['tmdb_id']: row for row in cursor.fetchall()}

                # 步骤 3: 丰富作品信息（番位），为筛选做准备
                enriched_works = self._enrich_works_with_order(unique_works, tmdb_person_id, self.tmdb_api_key)

                # 步骤 4: 遍历所有作品，决定每一部的最终状态和操作
                today_str = datetime.now().strftime('%Y-%m-%d')
                
                # 定义订阅源，所有操作都会用到
                subscription_source = {
                    "type": "actor_subscription", 
                    "id": subscription_id, 
                    "name": actor_name,
                    "person_id": tmdb_person_id
                }

                for work in enriched_works:
                    tmdb_id = str(work.get('id'))
                    if not tmdb_id: continue

                    media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
                    
                    # 从旧的追踪列表中移除，剩下的就是需要解绑的
                    old_tracked_media.pop(tmdb_id, None)

                    # 核心判断逻辑
                    is_kept, reason = self._filter_work_and_get_reason(work, sub)
                    
                    # 准备媒体元数据，用于更新
                    media_info = self._prepare_media_dict_for_upsert(work)

                    if is_kept:
                        # 筛选通过，状态可能是 IN_LIBRARY, MISSING, SUBSCRIBED, PENDING_RELEASE
                        status, emby_id = self._determine_library_status(work, emby_media_map, emby_series_seasons_map, emby_series_name_to_tmdb_id_map, today_str)
                        
                        # 获取当前媒体项在此订阅源下的追踪状态
                        current_tracked_status = old_tracked_media.get(tmdb_id, {}).get('subscription_status')
                        
                        new_sub_status = 'NONE' # 默认不操作
                        
                        # 根据用户反馈，除了统一订阅模块，其他模块一律没有权限把状态改为 SUBSCRIBED。
                        # 因此，这里只将符合条件的媒体项标记为 WANTED。
                        if status == MediaStatus.MISSING or status == MediaStatus.PENDING_RELEASE:
                            # 媒体不在库中或未发行，且筛选通过，则标记为 WANTED
                            new_sub_status = 'WANTED'
                        # 如果媒体已在库中 (MediaStatus.IN_LIBRARY)，则不应由演员订阅模块将其状态改为 SUBSCRIBED。
                        # 此时，new_sub_status 保持为 'NONE'，即不进行状态更新。
                        
                        if new_sub_status == 'WANTED': # 只有当状态明确为 WANTED 时才进行更新
                            media_db.update_subscription_status(
                                tmdb_ids=tmdb_id,
                                item_type=media_type,
                                new_status=new_sub_status,
                                source=subscription_source,
                                media_info_list=[media_info]
                            )
                    else:
                        # 筛选不通过，标记为 IGNORED
                        media_db.update_subscription_status(
                            tmdb_ids=tmdb_id,
                            item_type=media_type,
                            new_status='IGNORED',
                            source=subscription_source,
                            media_info_list=[media_info],
                            ignore_reason=reason
                        )

                # 步骤 5: 处理那些需要解绑的媒体 (即演员已不再参演的作品)
                if old_tracked_media:
                    logger.info(f"  ➜ 发现 {len(old_tracked_media)} 个过时的追踪记录，将为其解绑...")
                    for tmdb_id, item_info in old_tracked_media.items():
                        media_db.remove_subscription_source(tmdb_id, item_info['item_type'], subscription_source)

                # 步骤 6: 更新订阅本身的最后检查时间
                cursor.execute("UPDATE actor_subscriptions SET last_checked_at = CURRENT_TIMESTAMP WHERE id = %s", (subscription_id,))
                conn.commit()
                logger.info(f"  ✅ {actor_name} 的处理成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {subscription_id} 执行扫描时发生严重错误: {e}", exc_info=True)

    def _determine_library_status(self, work: Dict, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str], today_str: str) -> Tuple[MediaStatus, Optional[str]]:
        """仅判断媒体是否在库、是否缺失、是否未发行，返回状态和Emby ID。"""
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')

        # 1. 最高优先级：如果作品本身的TMDb ID就在Emby库中
        if media_id_str in emby_media_map:
            return MediaStatus.IN_LIBRARY, emby_media_map.get(media_id_str)
        
        # 2. 对电视剧进行特殊处理 (分季)
        if work.get('media_type') == 'tv':
            title = work.get('name', '')
            base_name, season_num = utils.parse_series_title_and_season(title)
            
            if base_name and season_num:
                parent_tmdb_id = self._find_parent_series_tmdb_id_from_emby_cache(base_name, emby_series_name_to_tmdb_id_map)
                if parent_tmdb_id and str(parent_tmdb_id) in emby_series_seasons_map:
                    if season_num in emby_series_seasons_map[str(parent_tmdb_id)]:
                        parent_emby_id = emby_media_map.get(str(parent_tmdb_id))
                        return MediaStatus.IN_LIBRARY, parent_emby_id

        # 3. 如果还未上映
        if release_date_str and release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE, None

        # 4. 其他所有情况都视为缺失
        return MediaStatus.MISSING, None
    
    def _prepare_media_dict_for_upsert(self, work: Dict) -> Dict:
        """准备一个标准的 media_info 字典，用于传递给 update_subscription_status。"""
        media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
        release_date = work.get('release_date') or work.get('first_air_date') or None
        
        # 解析季号
        title = work.get('title') or work.get('name', '')
        base_name, season_num = utils.parse_series_title_and_season(title)

        return {
            "tmdb_id": str(work.get('id')),
            "item_type": media_type,
            "title": title,
            "original_title": work.get('original_title') or work.get('original_name'),
            "release_date": release_date,
            "poster_path": work.get('poster_path'),
            "overview": work.get('overview'),
            "season_number": season_num
        }

    def _find_parent_series_tmdb_id_from_emby_cache(self, base_name: str, name_to_id_map: Dict[str, str]) -> Optional[str]:
        """
        【V2 - 本地优先版】根据基础剧名，在Emby本地剧集缓存中查找父剧集TMDb ID。
        """
        normalized_base_name = utils.normalize_name_for_matching(base_name)
        
        # 直接在本地缓存的映射中查找，精准且高效
        parent_id = name_to_id_map.get(normalized_base_name)
        
        if parent_id:
            logger.debug(f"  ➜ 在Emby本地缓存中为 '{base_name}' 匹配到父剧集 (TMDb ID: {parent_id})")
        else:
            logger.debug(f"  ➜ 在Emby本地缓存中未找到名为 '{base_name}' 的父剧集。")
            
        return parent_id
    
    def _filter_work_and_get_reason(self, work: Dict, sub_config) -> Tuple[bool, Optional[str]]:
        """
        对单个作品进行完整筛选。
        """
        # --- 准备工作 (保持不变) ---
        config_start_year = sub_config['config_start_year']
        release_date_str = work.get('release_date') or work.get('first_air_date', '')
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        media_type = MediaType.MOVIE.value if media_type_raw == 'movie' else MediaType.SERIES.value

        # 筛选1：上映日期年份
        if not release_date_str: return False, "缺少发行日期"
        try:
            if int(release_date_str.split('-')[0]) < config_start_year:
                return False, f"发行年份早于 {config_start_year}"
        except (ValueError, IndexError): pass

        # 筛选2：媒体类型
        raw_types_from_db = sub_config['config_media_types'].split(',')
        config_media_types = {
            'Series' if t.strip().lower() == 'tv' else t.strip().capitalize()
            for t in raw_types_from_db if t.strip()
        }
        if media_type not in config_media_types:
            return False, "排除的媒体类型"

        # 筛选3：题材
        config_genres_include = set(sub_config['config_genres_include_json'] or [])
        config_genres_exclude = set(sub_config['config_genres_exclude_json'] or [])
        genre_ids = set(work.get('genre_ids', []))
        if config_genres_exclude and not genre_ids.isdisjoint(config_genres_exclude):
            return False, "排除的题材"
        if config_genres_include and genre_ids.isdisjoint(config_genres_include):
            return False, "不包含指定的题材"

        # 筛选4：评分 
        config_min_rating = sub_config['config_min_rating']
        if config_min_rating > 0:
            tmdb_rating = work.get('vote_average', 0.0)
            vote_count = work.get('vote_count', 0)
            
            # 从 sub_config 字典（即数据库行）中读取正确的列名
            min_vote_count_threshold = sub_config.get('config_min_vote_count', 10)
            
            is_exempted = (vote_count < min_vote_count_threshold) or (tmdb_rating == 0.0)
            
            if is_exempted:
                logger.debug(f"  ➜ 作品 '{work.get('title') or work.get('name')}' 的评分被豁免 (评分: {tmdb_rating}, 票数: {vote_count} < {min_vote_count_threshold})。")
                pass
            else:
                if tmdb_rating < config_min_rating:
                    return False, f"评分过低 ({tmdb_rating:.1f}, {vote_count}人评价)"

        # 筛选5：中文片名
        chinese_char_regex = re.compile(r'[\u4e00-\u9fff]')
        title = work.get('title') or work.get('name', '')
        if not chinese_char_regex.search(title):
            return False, "缺少中文标题"

        # 筛选6：主演番位
        config_main_role_only = sub_config.get('config_main_role_only', False)
        if config_main_role_only:
            cast_order = work.get('order', 999)
            if cast_order >= 3:
                return False, f"非主演 (番位: {cast_order+1})"

        return True, None

    def _determine_media_status(self, work: Dict, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str], today_str: str, old_status: Optional[str], session_subscribed_ids: Set[str]) -> Tuple[Optional[MediaStatus], Optional[str]]:
        """
        智能识别分季剧集，并检查父剧集下的【特定季】是否入库。
        """
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')
        emby_tmdb_ids = set(emby_media_map.keys())

        # 1. 最高优先级：如果作品本身的TMDb ID就在Emby库中
        if media_id_str in emby_tmdb_ids:
            # ▼▼▼ 返回状态和它自己的 Emby ID ▼▼▼
            return MediaStatus.IN_LIBRARY, emby_media_map.get(media_id_str), None, None, None
        
        # 2. 对电视剧进行特殊处理
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        if media_type_raw == 'tv':
            title = work.get('name', '')
            base_name, season_num = utils.parse_series_title_and_season(title)
            
            if base_name and season_num:
                parent_tmdb_id = self._find_parent_series_tmdb_id_from_emby_cache(base_name, emby_series_name_to_tmdb_id_map)
                
                if parent_tmdb_id:
                    parent_tmdb_id_str = str(parent_tmdb_id)
                    if parent_tmdb_id_str in emby_series_seasons_map:
                        available_seasons = emby_series_seasons_map[parent_tmdb_id_str]
                        if season_num in available_seasons:
                            # ▼▼▼ 返回状态和父剧集的 Emby ID ▼▼▼
                            parent_emby_id = emby_media_map.get(parent_tmdb_id_str)
                            logger.info(f"  ➜ 父剧集 '{base_name}' (Emby ID: {parent_emby_id}) 已在库且包含第 {season_num} 季，标记为【已入库】。")
                            return MediaStatus.IN_LIBRARY, parent_emby_id, parent_tmdb_id_str, season_num, base_name
        # 3. 如果之前已被标记为 SUBSCRIBED
        if old_status == MediaStatus.SUBSCRIBED.value:
            return MediaStatus.SUBSCRIBED, None, None, None, None

        # 4. 如果还未上映
        if release_date_str and release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE, None, None, None, None

        # 5. 其他所有情况
        return MediaStatus.MISSING, None, None, None, None

    def _prepare_media_dict(self, work: Dict, subscription_id: int, status: MediaStatus, emby_item_id: Optional[str] = None, ignore_reason: Optional[str] = None, parent_tmdb_id: Optional[str] = None, season_number: Optional[int] = None, base_name: Optional[str] = None) -> Dict:
        """根据作品信息和状态，准备用于插入数据库的字典。"""
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        media_type = MediaType.SERIES if media_type_raw == 'tv' else MediaType.MOVIE
        
        release_date = work.get('release_date') or work.get('first_air_date')
        if not release_date:
            release_date = None

        return {
            'subscription_id': subscription_id,
            'tmdb_media_id': work.get('id'),
            'media_type': media_type.value,
            'title': base_name if base_name else (work.get('title') or work.get('name')),
            'release_date': release_date, 
            'poster_path': work.get('poster_path'),
            'status': status.value,
            'emby_item_id': emby_item_id,
            'ignore_reason': ignore_reason,
            'parent_series_tmdb_id': parent_tmdb_id,
            'parsed_season_number': season_number
        }

    def _enrich_works_with_order(self, works: List[Dict], tmdb_person_id: int, api_key: str) -> List[Dict]:
        """
        【新增】通过并发请求，为演员的作品列表补充其在作品中的 'order' 字段。
        """
        if not works:
            return []

        logger.info(f"  ➜ 正在为 {len(works)} 部作品并发获取演员番位信息...")
        
        enriched_works = []
        
        # 分离需要额外查询的作品和不需要查询的作品
        works_to_fetch_credits = []
        works_already_have_order = []

        for work in works:
            # 电影作品的 order 字段通常在 get_person_credits_tmdb 返回的原始数据中
            # 电视剧作品的 order 字段则需要额外查询
            if work.get('media_type') == 'movie':
                # 电影作品直接使用原始数据中的 order，如果不存在则为 999
                work['order'] = work.get('order', 999)
                works_already_have_order.append(work)
            elif work.get('media_type') == 'tv':
                works_to_fetch_credits.append(work)
            else:
                # 未知类型或无 media_type 的作品，也给默认 order
                work['order'] = 999
                works_already_have_order.append(work)

        # 处理不需要额外查询的作品
        enriched_works.extend(works_already_have_order)

        if not works_to_fetch_credits:
            logger.info("  ➜ 没有需要额外获取番位信息的电视剧作品。")
            return enriched_works

        logger.info(f"  ➜ 正在为 {len(works_to_fetch_credits)} 部电视剧作品并发获取演员番位信息...")
        
        # 使用线程池并发获取电视剧作品的详细信息
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_work = {
                executor.submit(self._fetch_tv_work_credits, work, api_key): work
                for work in works_to_fetch_credits
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_work)):
                original_work = future_to_work[future]
                if self.is_stop_requested():
                    logger.info("任务在获取作品番位信息时被中断。")
                    break
                try:
                    detailed_credits = future.result()
                    if detailed_credits:
                        cast_list = detailed_credits.get('cast', [])
                        actor_order = 999
                        for cast_member in cast_list:
                            if cast_member.get('id') == tmdb_person_id:
                                actor_order = cast_member.get('order', 999)
                                break
                        original_work['order'] = actor_order
                    else:
                        original_work['order'] = 999
                except Exception as exc:
                    logger.error(f"  ➜ 获取电视剧作品 '{original_work.get('title') or original_work.get('name')}' 的番位信息时发生错误: {exc}", exc_info=True)
                    original_work['order'] = 999
                
                enriched_works.append(original_work)
        
        logger.info(f"  ➜ 已为 {len(works_to_fetch_credits)} 部电视剧作品补充番位信息。")
        return enriched_works

    def _fetch_tv_work_credits(self, work: Dict, api_key: str) -> Optional[Dict[str, Any]]:
        """
        【新增】辅助函数：专门用于获取电视剧作品的详细 credits 信息。
        """
        media_id = work.get('id')
        if not media_id:
            return None
        
        details = tmdb.get_tv_details(media_id, api_key, append_to_response="credits")
        
        if details:
            logger.debug(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情成功。")
            # 调试：打印完整的 details 和 credits 部分
            # logger.debug(f"    完整详情: {details}")
            # logger.debug(f"    Credits 部分: {details.get('credits')}")
            return details.get('credits')
        else:
            logger.warning(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情失败。")
            return None
        
    def _deduplicate_works(self, credits: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        对从TMDb获取的演员作品列表进行去重。
        TMDb有时会为同一部作品返回多个条目（例如不同地区版本），此函数旨在选出唯一的代表。
        策略：按标准化的标题分组，然后选出每组中 'popularity' 最高的一个。
        """
        if not credits:
            return []

        # 1. 合并电影和电视剧作品
        movie_works = credits.get('movie_credits', {}).get('cast', [])
        tv_works = credits.get('tv_credits', {}).get('cast', [])
        
        # 统一添加 'media_type' 标识，以便后续处理
        for work in movie_works: work['media_type'] = 'movie'
        for work in tv_works: work['media_type'] = 'tv'
        
        all_works_raw = movie_works + tv_works

        # 2. 按标准化的标题对所有作品进行分组
        work_groups = {}
        for work in all_works_raw:
            # 确保有ID，否则是无效数据
            if not work.get('id'):
                continue
            
            title = work.get('title') or work.get('name', '')
            # 使用 utils.normalize_name_for_matching 进行标准化，这是关键
            normalized_title = utils.normalize_name_for_matching(title)
            if not normalized_title:
                continue
                
            if normalized_title not in work_groups:
                work_groups[normalized_title] = []
            work_groups[normalized_title].append(work)

        # 3. 在每个分组内，选出 popularity 最高的作为唯一代表
        unique_works = []
        for title, group in work_groups.items():
            if len(group) == 1:
                # 没有重复，直接采纳
                unique_works.append(group[0])
            else:
                # 有重复，选出热度最高的
                best_work = max(group, key=lambda x: x.get('popularity', 0))
                unique_works.append(best_work)
                
                # (可选) 记录日志，方便调试，看看哪些作品被合并了
                discarded_ids = [w['id'] for w in group if w['id'] != best_work['id']]
                logger.debug(f"  ➜ 在同名作品组 '{title}' 中，保留了热度最高的条目 (ID: {best_work['id']})，忽略了其他重复条目 (IDs: {discarded_ids})。")

        return unique_works
