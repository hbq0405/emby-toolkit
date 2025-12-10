# tasks/resubscribe.py
# 媒体洗版专属任务模块

import os
import re 
import time
import logging
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed 
from collections import defaultdict

# 导入需要的底层模块
import task_manager
import handler.emby as emby
import handler.moviepilot as moviepilot
import config_manager 
import constants  
from database import resubscribe_db, settings_db, maintenance_db, request_db, watchlist_db

# 从 helpers 导入的辅助函数和常量
from .helpers import (
    analyze_media_asset, 
    _get_resolution_tier, 
    _get_detected_languages_from_streams, 
    _get_standardized_effect, 
    _extract_quality_tag_from_filename,
    build_exclusion_regex_from_groups,
    AUDIO_SUBTITLE_KEYWORD_MAP,
    AUDIO_DISPLAY_MAP,            
    SUB_DISPLAY_MAP
)

logger = logging.getLogger(__name__)

def _evaluate_rating_rule(rule: dict, rating_value: Any) -> tuple[bool, bool, str]:
    """
    【辅助函数】评估评分规则。
    
    参数:
        rule: 规则字典
        rating_value: 媒体评分 (可能是 None, float, int)
        
    返回:
        (should_skip, is_needed, reason)
        - should_skip: True 表示在洗版模式下评分过低，应直接跳过该项目。
        - is_needed:   True 表示在删除模式下评分过低，应标记为需要处理(删除)。
        - reason:      原因描述。
    """
    if not rule.get("filter_rating_enabled"):
        return False, False, ""

    current_rating = float(rating_value or 0)
    threshold = float(rule.get("filter_rating_min", 0))
    ignore_zero = rule.get("filter_rating_ignore_zero", False)
    rule_type = rule.get('rule_type', 'resubscribe')

    is_low_rating = False
    # 0分保护逻辑
    if current_rating == 0 and ignore_zero:
        pass 
    elif current_rating < threshold:
        is_low_rating = True

    if is_low_rating:
        if rule_type == 'delete':
            # 删除模式：低分 -> 命中规则 -> 需要处理
            return False, True, f"评分过低({current_rating})"
        else:
            # 洗版模式：低分 -> 忽略 -> 跳过
            return True, False, ""

    return False, False, ""

# ======================================================================
# 核心任务：刷新洗版状态
# ======================================================================
def task_update_resubscribe_cache(processor): 
    """
    - 刷新洗版状态 (优化版：状态保护与自动清理)
    """
    task_name = "刷新媒体整理"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # --- 步骤 1: 加载规则 ---
        task_manager.update_status_from_thread(0, "正在加载规则...")
        time.sleep(0.5) 
        
        all_enabled_rules = [rule for rule in resubscribe_db.get_all_resubscribe_rules() if rule.get('enabled')]
        
        library_to_rule_map = {}
        all_target_lib_ids = set()
        for rule in reversed(all_enabled_rules):
            if target_libs := rule.get('target_library_ids'):
                all_target_lib_ids.update(target_libs)
                for lib_id in target_libs:
                    library_to_rule_map[lib_id] = rule
        
        # 如果没有规则，清空所有索引
        if not all_target_lib_ids:
            logger.info("  ➜ 未检测到启用规则，将清理所有洗版索引...")
            all_keys = resubscribe_db.get_all_resubscribe_index_keys()
            if all_keys:
                resubscribe_db.delete_resubscribe_index_by_keys(list(all_keys))
            task_manager.update_status_from_thread(100, "任务完成：规则为空，已清理所有索引。")
            return

        # --- 步骤 2: 从本地数据库获取全量媒体数据 ---
        task_manager.update_status_from_thread(5, "正在加载媒体索引...")
        
        all_movies = resubscribe_db.fetch_all_active_movies_for_analysis()
        all_series = resubscribe_db.fetch_all_active_series_for_analysis()

        if not all_movies and not all_series:
            task_manager.update_status_from_thread(100, "任务完成：本地数据库为空。")
            return

        # 只有 needed, ignored, subscribed, auto_subscribed 的项目会被加入此集合
        # 凡是不在此集合中的（即已达标 ok 的），最后都会被清理掉
        keys_to_keep_in_db = set()

        # --- 步骤 3: 全量处理流程 ---
        total = len(all_movies) + len(all_series)
        logger.info(f"  ➜ 将对 {len(all_movies)} 部电影和 {len(all_series)} 部剧集进行洗版计算...")
        
        index_update_batch = []
        processed_count = 0
        current_statuses = resubscribe_db.get_current_index_statuses()
        update_interval = max(50, min(500, total // 20))

        # ====== 3a. 处理所有电影 ======
        for movie in all_movies:
            if processor.is_stop_requested(): break
            processed_count += 1
            
            if processed_count % update_interval == 0:
                progress = int(10 + (processed_count / total) * 85)
                task_manager.update_status_from_thread(progress, f"正在分析: {movie['title']}")

            # 跳过多版本
            emby_ids = movie.get('emby_item_ids_json')
            if emby_ids and len(emby_ids) > 1:
                continue
            
            assets = movie.get('asset_details_json')
            if not assets: continue
            
            source_lib_id = assets[0].get('source_library_id')
            if not source_lib_id: continue 

            rule = library_to_rule_map.get(source_lib_id)
            if not rule: continue 

            # ==================== 1. 评分预检查 (调用辅助函数) ====================
            should_skip, rating_needed, rating_reason = _evaluate_rating_rule(rule, movie.get('rating'))
            
            if should_skip:
                # 洗版模式下低分，直接忽略
                continue
            
            # 计算物理状态 (True=需要洗版, False=已达标)
            needs, reason = _item_needs_resubscribe(assets[0], rule, movie)
            # 如果评分导致需要删除，强制覆盖状态
            if rating_needed:
                needs = True
                reason = rating_reason
            # 获取数据库中的当前状态
            item_key_tuple = (str(movie['tmdb_id']), "Movie", -1)
            existing_status = current_statuses.get(item_key_tuple)

            # ★★★ 核心逻辑优化 ★★★
            if not needs:
                # 物理文件已达标 (OK)
                # 策略：不加入 keys_to_keep_in_db，也不加入 update_batch。
                # 结果：如果数据库里有它，最后会被 cleanup 逻辑删除；如果没它，就不添加。
                continue

            # 物理文件依然不达标 (Needed)
            final_status = 'needed' # 默认值

            if existing_status in ['subscribed', 'auto_subscribed']:
                # 场景 A: 已经在洗版流程中 (等待下载或入库)
                # 策略：保持原状态，防止重复提交或重置
                final_status = existing_status
            
            elif existing_status == 'ignored':
                # 场景 B: 用户已手动忽略
                # 策略：保持忽略
                final_status = 'ignored'
            
            else:
                # 场景 C: 新发现的不达标项目，或者之前是 needed
                logger.info(f"  ➜ 《{movie['title']}》需要处理。原因: {reason}")
                if rule.get('auto_resubscribe'):
                    final_status = 'auto_subscribed'
                    # 只有当状态发生变化（从 None/needed -> auto_subscribed）时，才触发 webhook
                    if existing_status != 'auto_subscribed':
                        _handle_auto_resubscribe_trigger(
                            item_details={
                                'tmdb_id': movie['tmdb_id'],
                                'item_type': 'Movie',
                                'item_name': movie['title'],
                                'season_number': None,
                                'release_group_raw': assets[0].get('release_group_raw', [])
                            },
                            rule=rule,
                            reason=reason
                        )
                else:
                    final_status = 'needed'

            # 只要还需要洗版（或正在洗版），就保留在数据库中
            keys_to_keep_in_db.add(item_key_tuple[0])
            index_update_batch.append({
                "tmdb_id": movie['tmdb_id'], "item_type": "Movie", "season_number": -1,
                "status": final_status, "reason": reason, "matched_rule_id": rule.get('id')
            })

        # ====== 3b. 处理所有剧集 ======
        if all_series:
            series_tmdb_ids = [str(s['tmdb_id']) for s in all_series]
            all_episodes_simple = resubscribe_db.fetch_episodes_simple_batch(series_tmdb_ids)
            
            episodes_map = defaultdict(list)
            for ep in all_episodes_simple:
                episodes_map[ep['parent_series_tmdb_id']].append(ep)
            
            for series in all_series:
                if processor.is_stop_requested(): break
                processed_count += 1
                
                if processed_count % update_interval == 0:
                    progress = int(10 + (processed_count / total) * 85)
                    task_manager.update_status_from_thread(progress, f"正在分析: {series['title']}")

                # 追更保护
                watching_status = series.get('watching_status', 'NONE')
                if watching_status in ['Watching', 'Paused', 'Pending']:
                    continue

                tmdb_id = str(series['tmdb_id'])
                episodes = episodes_map.get(tmdb_id)
                if not episodes: continue

                source_lib_id = None
                for ep in episodes:
                    assets = ep.get('asset_details_json')
                    if assets and assets[0].get('source_library_id'):
                        source_lib_id = assets[0].get('source_library_id')
                        break
                
                if not source_lib_id: continue
                rule = library_to_rule_map.get(source_lib_id)
                if not rule: continue

                episodes_by_season = defaultdict(list)
                for ep in episodes:
                    episodes_by_season[ep.get('season_number')].append(ep)

                series_meta_wrapper = {
                    'title': series['title'],
                    'tmdb_id': tmdb_id,
                    'item_type': 'Series',
                    'original_language': series.get('original_language'),
                    'rating': series.get('rating')
                }

                for season_num, eps_in_season in episodes_by_season.items():
                    if season_num is None: continue
                    # 排除第0季 (通常不计算缺集)
                    if int(season_num) == 0:
                        continue

                    # ==========================================================
                    # ★★★ 新增：内存中计算是否缺集 (复用 watchlist 的逻辑) ★★★
                    # ==========================================================
                    has_gaps = False
                    # 1. 筛选出有实体文件的集 (排除只有元数据但没文件的)
                    valid_eps = [
                        e for e in eps_in_season 
                        if e.get('episode_number') and e.get('asset_details_json')
                    ]
                    
                    if valid_eps:
                        # 2. 获取当前有的最大集号
                        max_ep = max(e['episode_number'] for e in valid_eps)
                        # 3. 获取当前有的总集数
                        count_ep = len(valid_eps)
                        # 4. 核心逻辑：如果 最大集号 > 总集数，说明中间肯定缺了
                        #    (例如：有1, 3集。max=3, count=2。3 > 2，缺集成立)
                        if max_ep > count_ep:
                            has_gaps = True
                    # ==========================================================
                    # 跳过多版本
                    has_multi_version_episode = False
                    for ep in eps_in_season:
                        ep_ids = ep.get('emby_item_ids_json')
                        if ep_ids and len(ep_ids) > 1:
                            has_multi_version_episode = True
                            break
                    
                    if has_multi_version_episode:
                        continue
                    
                    eps_in_season.sort(key=lambda x: x.get('episode_number', 0))
                    rep_ep = eps_in_season[0]
                    season_tmdb_id = rep_ep.get('season_tmdb_id')
                    assets = rep_ep.get('asset_details_json')
                    if not assets: continue

                    # 构建一个专用的 context 对象传进去
                    current_season_wrapper = series_meta_wrapper.copy()
                    current_season_wrapper.update({
                        'item_type': 'Season',
                        'season_number': int(season_num),
                        'has_gaps': has_gaps  # <--- 传入计算结果
                    })

                    # --- 初始化计算状态 ---
                    status_calculated = 'ok'
                    reason_calculated = ""

                    # ==================== 1. 评分预检查 (调用辅助函数) ====================
                    should_skip, rating_needed, rating_reason = _evaluate_rating_rule(rule, current_season_wrapper.get('rating'))

                    if should_skip:
                        # 洗版模式下低分，直接忽略本季
                        continue

                    if rating_needed:
                        # 删除模式下低分，标记为需要处理
                        status_calculated = 'needed'
                        reason_calculated = rating_reason

                    # ==================== 2. 常规洗版检查 ====================
                    # 只有当前状态还是 ok 时才检查 (即评分没问题)
                    if status_calculated == 'ok':
                        needs_upgrade, upgrade_reason = _item_needs_resubscribe(assets[0], rule, current_season_wrapper)
                        if needs_upgrade:
                            status_calculated = 'needed'
                            reason_calculated = upgrade_reason

                    # ==================== 3. 一致性检查 ====================
                    # 只有当前状态还是 ok 且开启了一致性检查时才执行
                    if status_calculated == 'ok' and rule.get('consistency_check_enabled'):
                        needs_fix, fix_reason = _check_season_consistency(eps_in_season, rule)
                        if needs_fix:
                            status_calculated = 'needed'
                            reason_calculated = fix_reason

                    # --- ★★★ 核心逻辑优化 ★★★ ---
                    item_key_tuple = (tmdb_id, "Season", int(season_num))
                    existing_status = current_statuses.get(item_key_tuple)
                    
                    if status_calculated == 'ok':
                        # 物理文件已达标，跳过（后续会被清理）
                        continue

                    # 物理文件不达标
                    final_status = 'needed'

                    if existing_status in ['subscribed', 'auto_subscribed']:
                        # 保护进行中状态
                        final_status = existing_status
                    
                    elif existing_status == 'ignored':
                        final_status = 'ignored'
                    
                    else:
                        # 新增或 needed
                        logger.info(f"  ➜ 《{series['title']} - 第{season_num}季》需要处理。原因: {reason_calculated}")
                        if rule.get('auto_resubscribe'):
                            final_status = 'auto_subscribed'
                            if existing_status != 'auto_subscribed':
                                _handle_auto_resubscribe_trigger(
                                    item_details={
                                        'tmdb_id': tmdb_id,
                                        'season_tmdb_id': season_tmdb_id,
                                        'item_type': 'Season',
                                        'item_name': f"{series['title']} - 第{season_num}季",
                                        'season_number': season_num,
                                        'release_group_raw': assets[0].get('release_group_raw', [])
                                    },
                                    rule=rule,
                                    reason=reason_calculated
                                )
                        else:
                            final_status = 'needed'

                    # 保留并更新
                    keys_to_keep_in_db.add(f"{tmdb_id}-S{season_num}")
                    index_update_batch.append({
                        "tmdb_id": tmdb_id, "item_type": "Season", "season_number": season_num,
                        "status": final_status, "reason": reason_calculated, "matched_rule_id": rule.get('id')
                    })

        # --- 步骤 4: 执行数据库更新与清理 ---
        
        # 4.1 更新有效记录
        if index_update_batch:
            task_manager.update_status_from_thread(95, f"正在保存 {len(index_update_batch)} 条结果...")
            resubscribe_db.upsert_resubscribe_index_batch(index_update_batch)
        
        # 4.2 清理陈旧记录 (ok 的，或者已删除的)
        all_db_keys = resubscribe_db.get_all_resubscribe_index_keys()
        
        # 差集：数据库里有，但本次不需要保留的 (即：变成了 ok 的，或者源文件没了的)
        keys_to_purge = all_db_keys - keys_to_keep_in_db
        
        if keys_to_purge:
            logger.info(f"  ➜ 清理 {len(keys_to_purge)} 条已达标(OK)或失效的索引...")
            resubscribe_db.delete_resubscribe_index_by_keys(list(keys_to_purge))
        else:
            logger.info("  ➜ 索引清理完成，无过期条目。")

        final_message = "媒体洗版状态刷新完成！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        time.sleep(1)
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ======================================================================
# 核心任务：执行洗版订阅
# ======================================================================

def task_resubscribe_library(processor):
    """一键媒体整理所有状态为 'needed' 的项目。"""
    _execute_resubscribe(processor, "一键媒体整理", "needed")

def task_resubscribe_batch(processor, item_ids: List[str]):
    """精准媒体整理指定的项目。"""
    _execute_resubscribe(processor, "批量媒体整理", item_ids)

# ======================================================================
# 内部辅助函数
# ======================================================================
# 辅助函数：处理自动洗版触发 
def _handle_auto_resubscribe_trigger(item_details: dict, rule: dict, reason: str):
    """
    当发现项目需要洗版且规则开启了自动洗版时，将其推送到统一订阅队列 (WANTED)。
    """
    try:
        tmdb_id = str(item_details['tmdb_id']) # Series ID
        item_type = item_details['item_type']
        season_number = item_details.get('season_number')
        season_tmdb_id = item_details.get('season_tmdb_id') # <--- 获取传入的季 ID

        # ★★★ 核心修复：确定存储 ID ★★★
        storage_tmdb_id = tmdb_id
        
        if item_type == 'Season' and season_number is not None:
            if season_tmdb_id:
                # 优先使用数据库中已存在的原生季 ID
                storage_tmdb_id = str(season_tmdb_id)
            else:
                # 兜底：如果数据库里没有季的条目（极少见），使用复合 ID
                storage_tmdb_id = f"{tmdb_id}_S{season_number}"
                logger.warning(f"  ⚠ 剧集 {tmdb_id} 第 {season_number} 季未关联到原生ID，使用复合ID: {storage_tmdb_id}")

        # 1. 构建 Source 对象
        source = {
            "type": "resubscribe",
            "rule_id": rule.get('id'),
            "rule_name": rule.get('name'),
            "reason": reason,
            "created_at": time.time()
        }

        # 2. 如果开启了自定义洗版，生成 Payload
        if rule.get('custom_resubscribe_enabled'):
            payload = build_resubscribe_payload(item_details, rule)
            if payload:
                source['payload'] = payload
        
        # 3. 清理旧来源
        request_db.remove_sources_by_type(storage_tmdb_id, item_type, 'resubscribe')

        # 4. 推送到 media_metadata 表
        request_db.set_media_status_wanted(
            tmdb_ids=storage_tmdb_id, 
            item_type=item_type,
            source=source,
            media_info_list=[{
                'tmdb_id': storage_tmdb_id, 
                'title': item_details['item_name'],
                'season_number': season_number,
                'parent_series_tmdb_id': tmdb_id if item_type == 'Season' else None,
                'reason': reason
            }]
        )
        logger.info(f"  ➜ [自动洗版] 已更新《{item_details['item_name']}》的洗版请求 (WANTED)。")

    except Exception as e:
        logger.error(f"  ➜ [自动洗版] 触发失败: {e}", exc_info=True)

def _item_needs_resubscribe(asset_details: dict, rule: dict, media_metadata: Optional[dict]) -> tuple[bool, str]:
    """
    【V5 - 终极修正版】
    完全依赖 asset_details 中预先分析好的数据进行判断，不再进行任何二次解析。
    """
    item_name = media_metadata.get('title', '未知项目')
    reasons = []

    # --- 1. 分辨率检查 (直接使用 resolution_display) ---
    try:
        if rule.get("resubscribe_resolution_enabled"):
            # 定义清晰度等级的顺序
            RESOLUTION_ORDER = {
                "4k": 4,
                "1080p": 3,
                "720p": 2,
                "480p": 1,
                "未知": 0,
            }
            
            # 获取当前媒体的清晰度等级
            current_res_str = asset_details.get('resolution_display', '未知')
            current_tier = RESOLUTION_ORDER.get(current_res_str, 1)

            # 获取规则要求的清晰度等级
            required_width = int(rule.get("resubscribe_resolution_threshold", 1920))
            required_tier = 1
            if required_width >= 3800: required_tier = 4
            elif required_width >= 1900: required_tier = 3
            elif required_width >= 1200: required_tier = 2
            elif required_width >= 700: required_tier = 1

            if current_tier < required_tier:
                reasons.append("分辨率不达标")
    except (ValueError, TypeError) as e:
        logger.warning(f"  ➜ [分辨率检查] 处理时发生错误: {e}")

    # --- 2. 质量检查 (直接使用 quality_display) ---
    try:
        # 检查规则是否启用了质量洗版
        if rule.get("resubscribe_quality_enabled"):
            # 获取规则中要求的质量列表，例如 ['BluRay', 'WEB-DL']
            required_qualities = rule.get("resubscribe_quality_include", [])
            
            # 仅当规则中明确配置了要求时，才执行检查
            if required_qualities:
                # 1. 定义权威的“质量金字塔”等级（数字越大，等级越高）
                QUALITY_HIERARCHY = {
                    'remux': 6,
                    'bluray': 5,
                    'web-dl': 4,
                    'webrip': 3,
                    'hdtv': 2,
                    'dvdrip': 1,
                    '未知': 0
                }

                # 2. 计算规则要求的“最高目标等级”
                #    例如，如果规则是 ['BluRay', 'WEB-DL']，那么目标就是达到 BluRay (等级5)
                highest_required_tier = 0
                for req_quality in required_qualities:
                    highest_required_tier = max(highest_required_tier, QUALITY_HIERARCHY.get(req_quality.lower(), 0))

                # 3. 获取当前文件经过分析后得出的“质量标签”
                current_quality_tag = asset_details.get('quality_display', '未知').lower()
                
                # 4. 计算当前文件所处的“实际质量等级”
                current_actual_tier = QUALITY_HIERARCHY.get(current_quality_tag, 0)

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("质量不符")
    except Exception as e:
        logger.warning(f"  ➜ [质量检查] 处理时发生错误: {e}")

    # --- 3. 特效检查 (直接使用 effect_display) ---
    try:
        # 检查规则是否启用了特效洗版
        if rule.get("resubscribe_effect_enabled"):
            # 获取规则中要求的特效列表，例如 ['dovi_p8', 'hdr10+']
            required_effects = rule.get("resubscribe_effect_include", [])
            
            # 仅当规则中明确配置了要求时，才执行检查
            if required_effects:
                # 1. 定义权威的“特效金字塔”等级（数字越大，等级越高）
                #    这个层级严格对应 helpers.py 中 _get_standardized_effect 的输出
                EFFECT_HIERARCHY = {
                    "dovi_p8": 7,
                    "dovi_p7": 6,
                    "dovi_p5": 5,
                    "dovi_other": 4,
                    "hdr10+": 3,
                    "hdr": 2,
                    "sdr": 1
                }

                # 2. 计算规则要求的“最高目标等级”
                #    例如，如果规则是 ['hdr', 'dovi_p5']，那么目标就是达到 d_p5 (等级5)
                highest_required_tier = 0
                for req_effect in required_effects:
                    highest_required_tier = max(highest_required_tier, EFFECT_HIERARCHY.get(req_effect.lower(), 0))

                # 3. 获取当前文件经过 helpers.py 分析后得出的“权威特效标识”
                #    asset_details['effect_display'] 现在存储的是 'dovi_p8' 这样的精确字符串
                current_effect_tag = asset_details.get('effect_display', 'sdr')
                
                # 4. 计算当前文件所处的“实际特效等级”
                current_actual_tier = EFFECT_HIERARCHY.get(current_effect_tag.lower(), 1) # 默认为sdr等级

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("特效不达标")
    except Exception as e:
        logger.warning(f"  ➜ [特效检查] 处理时发生错误: {e}")

    # --- 4. 编码检查 ---
    try:
        # 检查规则是否启用了编码洗版
        if rule.get("resubscribe_codec_enabled"):
            # 获取规则中要求的编码列表，例如 ['hevc']
            required_codecs = rule.get("resubscribe_codec_include", [])
            
            if required_codecs:
                # 1. 定义“编码金字塔”等级（数字越大，等级越高）
                #    为常见别名设置相同等级，增强兼容性
                CODEC_HIERARCHY = {
                    'hevc': 2, 'h265': 2,
                    'h264': 1, 'avc': 1,
                    '未知': 0
                }

                # 2. 计算规则要求的“最高目标等级”
                highest_required_tier = 0
                for req_codec in required_codecs:
                    highest_required_tier = max(highest_required_tier, CODEC_HIERARCHY.get(req_codec.lower(), 0))

                # 3. 获取当前文件经过分析后得出的“编码标签”
                current_codec_tag = asset_details.get('codec_display', '未知').lower()
                
                # 4. 计算当前文件所处的“实际编码等级”
                current_actual_tier = CODEC_HIERARCHY.get(current_codec_tag, 0)

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("编码不符")
    except Exception as e:
        logger.warning(f"  ➜ [编码检查] 处理时发生错误: {e}")

    # --- 4. 文件大小检查 (直接使用 size_bytes) ---
    try:
        if rule.get("resubscribe_filesize_enabled"):
            file_size_bytes = asset_details.get('size_bytes')
            if file_size_bytes:
                operator = rule.get("resubscribe_filesize_operator", 'lt')
                threshold_gb = float(rule.get("resubscribe_filesize_threshold_gb", 10.0))
                file_size_gb = file_size_bytes / (1024**3)
                needs_resubscribe = False
                reason_text = ""
                if operator == 'lt' and file_size_gb < threshold_gb:
                    needs_resubscribe = True
                    reason_text = f"文件 < {threshold_gb} GB"
                elif operator == 'gt' and file_size_gb > threshold_gb:
                    needs_resubscribe = True
                    reason_text = f"文件 > {threshold_gb} GB"
                if needs_resubscribe:
                    reasons.append(reason_text)
    except (ValueError, TypeError, IndexError) as e:
        logger.warning(f"  ➜ [文件大小检查] 处理时发生错误: {e}")

    # --- 5. 音轨检查 (V3 - 集成通用豁免) ---
    try:
        if rule.get("resubscribe_audio_enabled"):
            required_langs = rule.get("resubscribe_audio_missing_languages", [])
            if required_langs:
                existing_audio_codes = set(asset_details.get('audio_languages_raw', []))
                
                for lang_code in required_langs:
                    # ★★★ 核心修改：在循环内部调用新的豁免函数 ★★★
                    if _is_exempted_from_language_check(media_metadata, lang_code):
                        continue
                    
                    if lang_code not in existing_audio_codes:
                        display_name = AUDIO_DISPLAY_MAP.get(lang_code, lang_code)
                        reasons.append(f"缺{display_name}音轨")
    except Exception as e:
        logger.warning(f"  ➜ [音轨检查] 处理时发生未知错误: {e}")

    # --- 6. 字幕检查 (V3 - 集成通用豁免) ---
    try:
        if rule.get("resubscribe_subtitle_enabled"):
            required_langs = rule.get("resubscribe_subtitle_missing_languages", [])
            if required_langs:
                existing_subtitle_codes = set(asset_details.get('subtitle_languages_raw', []))
                
                for lang_code in required_langs:
                    # ★★★ 核心修改：在循环内部调用新的豁免函数 ★★★
                    if _is_exempted_from_language_check(media_metadata, lang_code):
                        continue
                    
                    # ★★★ 新功能逻辑开始 ★★★
                    # 检查规则是否开启了“音轨豁免”功能
                    if rule.get("resubscribe_subtitle_skip_if_audio_exists", False):
                        # 获取已存在的音轨语言代码
                        existing_audio_codes = asset_details.get('audio_languages_raw', [])
                        # 如果要求的字幕语言 (如 'chi') 已经存在于音轨中
                        if lang_code in existing_audio_codes:
                            continue # 则跳过对这条字幕的检查，相当于豁免
                    # ★★★ 新功能逻辑结束 ★★★
                    
                    # 如果未被豁免，且字幕确实不存在
                    if lang_code not in existing_subtitle_codes:
                        display_name = SUB_DISPLAY_MAP.get(lang_code, lang_code)
                        reasons.append(f"缺{display_name}字幕")
    except Exception as e:
        logger.warning(f"  ➜ [字幕检查] 处理时发生未知错误: {e}")

    # --- 6.缺集检查 (仅限剧集) ---
    try:
        if rule.get("filter_missing_episodes_enabled") and media_metadata.get('item_type') == 'Season':
            if media_metadata.get('has_gaps'):
                reasons.append("存在中间缺集")
                    
    except Exception as e:
        logger.warning(f"  ➜ [缺集检查] 处理时发生错误: {e}")
                 
    if reasons:
        final_reason = "; ".join(sorted(list(set(reasons))))
        return True, final_reason
    else:
        logger.debug(f"  ➜ 《{item_name}》质量达标。")
        return False, ""

def _check_season_consistency(episodes: List[dict], rule: dict) -> tuple[bool, str]:
    """
    检查整季的一致性。
    返回: (是否需要洗版, 原因)
    """
    # 如果规则没开启一致性检查，直接通过
    if not rule.get('consistency_check_enabled'):
        return False, ""

    # 收集该季所有集的属性
    stats = {
        "resolution": set(),
        "group": set(),
        "codec": set()
    }
    
    # 忽略只有一集的情况（无法比较一致性）
    if len(episodes) < 2:
        return False, ""

    for ep in episodes:
        assets = ep.get('asset_details_json')
        if not assets: continue
        asset = assets[0] # 取主文件

        # 1. 分辨率
        if rule.get('consistency_must_match_resolution'):
            res = asset.get('resolution_display', 'Unknown')
            stats["resolution"].add(res)

        # 2. 制作组 (取第一个识别到的组)
        if rule.get('consistency_must_match_group'):
            groups = asset.get('release_group_raw', [])
            group = groups[0] if groups else 'Unknown'
            # 忽略 Unknown，避免因为识别失败导致的误报
            if group != 'Unknown':
                stats["group"].add(group)

        # 3. 编码
        if rule.get('consistency_must_match_codec'):
            codec = asset.get('codec_display', 'Unknown')
            stats["codec"].add(codec)

    reasons = []
    
    # 判定逻辑
    if len(stats["resolution"]) > 1:
        reasons.append(f"分辨率混杂({','.join(stats['resolution'])})")
    
    if len(stats["group"]) > 1:
        reasons.append(f"发布组混杂({','.join(stats['group'])})")
        
    if len(stats["codec"]) > 1:
        reasons.append(f"编码混杂({','.join(stats['codec'])})")

    if reasons:
        return True, "; ".join(reasons)
    
    return False, ""

def _is_exempted_from_language_check(media_metadata: Optional[dict], language_code_to_check: str) -> bool:
    """
    【V3 - 通用语言豁免版】
    判断一个媒体是否应该免除对特定语言（音轨/字幕）的检查。
    主要依据媒体的原始语言元数据。
    """
    if not media_metadata:
        return False

    # 1. 定义 TMDB 语言代码到我们内部代码的映射
    LANG_CODE_MAP = {
        'zh': 'chi', 'cn': 'chi', 'cmn': 'chi',
        'yue': 'yue', 'hk': 'yue',
        'en': 'eng',
        'ja': 'jpn',
        'ko': 'kor',
        # ...可以根据需要添加更多映射...
    }

    # 2. 优先使用 original_language 进行判断 (最可靠)
    if original_lang := media_metadata.get('original_language'):
        mapped_lang = LANG_CODE_MAP.get(original_lang.lower())
        if mapped_lang and mapped_lang == language_code_to_check:
            return True

    # 3. 其次，使用原始标题中的 CJK 字符作为中文/日文/韩文的辅助判断
    if language_code_to_check in ['chi', 'jpn', 'kor']:
        if original_title := media_metadata.get('original_title'):
            # 使用正则表达式查找中日韩字符
            if len(re.findall(r'[\u4e00-\u9fff]', original_title)) >= 2:
                return True
    
    # 默认不豁免
    return False

def build_resubscribe_payload(item_details: dict, rule: Optional[dict]) -> Optional[dict]:
    """构建发送给 MoviePilot 的订阅 payload。"""
    from .subscriptions import AUDIO_SUBTITLE_KEYWORD_MAP
    from datetime import date, datetime

    item_name = item_details.get('item_name')
    tmdb_id_str = str(item_details.get('tmdb_id', '')).strip()
    item_type = item_details.get('item_type')

    if not all([item_name, tmdb_id_str, item_type]):
        logger.error(f"构建Payload失败：缺少核心媒体信息。来源: {item_details}")
        return None
    
    try:
        tmdb_id = int(tmdb_id_str)
    except (ValueError, TypeError):
        logger.error(f"构建Payload失败：TMDB ID '{tmdb_id_str}' 无效。")
        return None

    base_series_name = item_name.split(' - 第')[0]
    media_type_for_payload = "电视剧" if item_type in ["Series", "Season"] else "电影"

    payload = {
        "name": base_series_name,
        "tmdbid": tmdb_id,
        "type": media_type_for_payload,
        "best_version": 1
    }

    if item_type == "Season":
        season_num = item_details.get('season_number')
        if season_num is not None:
            payload['season'] = int(season_num)
        else:
            logger.error(f"严重错误：项目 '{item_name}' 类型为 'Season' 但未找到 'season_number'！")

    # --- 排除原发布组 ---
    should_exclude_current_groups = True
    
    # 如果规则存在，且开启了一致性检查，则不排除原发布组
    if item_type == "Season":
        should_exclude_current_groups = False
        logger.info(f"  ➜ 剧集洗版跳过排除原发布组。")

    if should_exclude_current_groups:
        # --- 原有的排除逻辑 (放入 if 块内) ---
        detected_group_names = item_details.get('release_group_raw', [])
        
        if detected_group_names:
            # 调用 helper 反查这些组名对应的所有关键词
            exclusion_regex = build_exclusion_regex_from_groups(detected_group_names)
            
            if exclusion_regex:
                payload['exclude'] = exclusion_regex
                logger.info(f"  ➜ 精准排除模式：已为《{item_name}》生成排除正则: {payload['exclude']}")
            else:
                logger.warning(f"  ⚠ 虽然检测到发布组 {detected_group_names}，但无法生成对应的正则关键词。")
        else:
            logger.info(f"  ✅ 未找到预分析的发布组，不添加排除规则。")

    if not rule:
        return payload

    rule_name = rule.get('name', '未知规则')
    final_include_lookaheads = []

    # --- 分辨率、质量 (逻辑不变) ---
    if rule.get("resubscribe_resolution_enabled"):
        threshold = rule.get("resubscribe_resolution_threshold")
        target_resolution = None
        if threshold == 3840: target_resolution = "4k"
        elif threshold == 1920: target_resolution = "1080p"
        if target_resolution:
            payload['resolution'] = target_resolution
            logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 分辨率: {target_resolution}")
    if rule.get("resubscribe_quality_enabled"):
        quality_list = rule.get("resubscribe_quality_include")
        if isinstance(quality_list, list) and quality_list:
            payload['quality'] = ",".join(quality_list)
            logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 质量: {payload['quality']}")

    # --- 编码订阅逻辑 ---
    try:
        if rule.get("resubscribe_codec_enabled"):
            codec_list = rule.get("resubscribe_codec_include", [])
            if isinstance(codec_list, list) and codec_list:
                # 定义编码到正则表达式关键字的映射，增强匹配成功率
                CODEC_REGEX_MAP = {
                    'hevc': ['hevc', 'h265', 'x265'],
                    'h264': ['h264', 'avc', 'x264']
                }
                
                # 根据用户选择，构建一个大的 OR 正则组
                # 例如，如果用户选了 'hevc'，最终会生成 (hevc|h265|x265)
                regex_parts = []
                for codec in codec_list:
                    if codec.lower() in CODEC_REGEX_MAP:
                        regex_parts.extend(CODEC_REGEX_MAP[codec.lower()])
                
                if regex_parts:
                    # 将所有关键字用 | 连接，并放入一个正向先行断言中
                    # 这意味着“标题中必须包含这些关键字中的任意一个”
                    include_regex = f"(?=.*({'|'.join(regex_parts)}))"
                    final_include_lookaheads.append(include_regex)
                    logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加编码过滤器: {include_regex}")
    except Exception as e:
        logger.warning(f"  ➜ [编码订阅] 构建正则时发生错误: {e}")
    
    # --- 特效订阅逻辑 (实战优化) ---
    if rule.get("resubscribe_effect_enabled"):
        effect_list = rule.get("resubscribe_effect_include", [])
        if isinstance(effect_list, list) and effect_list:
            simple_effects_for_payload = set()
            
            EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
            # ★★★ 核心修改：将 "dv" 加入正则 ★★★
            EFFECT_PARAM_MAP = {
                "dovi_p8": ("(?=.*(dovi|dolby|dv))(?=.*hdr)", "dovi"),
                "dovi_p7": ("(?=.*(dovi|dolby|dv))(?=.*(p7|profile.?7))", "dovi"),
                "dovi_p5": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "dovi_other": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "hdr10+": ("(?=.*(hdr10\+|hdr10plus))", "hdr10+"),
                "hdr": ("(?=.*hdr)", "hdr")
            }
            OLD_EFFECT_MAP = {"杜比视界": "dovi_other", "HDR": "hdr"}

            highest_req_priority = 999
            best_effect_choice = None
            for choice in effect_list:
                normalized_choice = OLD_EFFECT_MAP.get(choice, choice)
                try:
                    priority = EFFECT_HIERARCHY.index(normalized_choice)
                    if priority < highest_req_priority:
                        highest_req_priority = priority
                        best_effect_choice = normalized_choice
                except ValueError: continue
            
            if best_effect_choice:
                regex_pattern, simple_effect = EFFECT_PARAM_MAP.get(best_effect_choice, (None, None))
                if regex_pattern:
                    final_include_lookaheads.append(regex_pattern)
                if simple_effect:
                    simple_effects_for_payload.add(simple_effect)

            if simple_effects_for_payload:
                 payload['effect'] = ",".join(simple_effects_for_payload)

    # --- 音轨、字幕处理 (逻辑不变) ---
    if rule.get("resubscribe_audio_enabled"):
        audio_langs = rule.get("resubscribe_audio_missing_languages", [])
        if isinstance(audio_langs, list) and audio_langs:
            audio_keywords = [k for lang in audio_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(lang, [])]
            if audio_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(audio_keywords)), key=len, reverse=True))}))")

    if rule.get("resubscribe_subtitle_effect_only"):
        final_include_lookaheads.append("(?=.*特效)")
    elif rule.get("resubscribe_subtitle_enabled"):
        subtitle_langs = rule.get("resubscribe_subtitle_missing_languages", [])
        if isinstance(subtitle_langs, list) and subtitle_langs:
            subtitle_keywords = [k for lang in subtitle_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(f"sub_{lang}", [])]
            if subtitle_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(subtitle_keywords)), key=len, reverse=True))}))")

    if final_include_lookaheads:
        payload['include'] = "".join(final_include_lookaheads)
        logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 生成的 AND 正则过滤器(精筛): {payload['include']}")

    return payload

def _execute_resubscribe(processor, task_name: str, target):
    """执行媒体整理的通用函数。"""
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    if isinstance(target, str) and target == "needed":
        items_to_subscribe = resubscribe_db.get_all_needed_resubscribe_items()
    elif isinstance(target, list):
        items_to_subscribe = resubscribe_db.get_resubscribe_items_by_ids(target)
    else:
        task_manager.update_status_from_thread(-1, "任务失败：无效的目标参数")
        return

    total = len(items_to_subscribe)
    if total == 0:
        task_manager.update_status_from_thread(100, "任务完成：没有需要洗版的项目。")
        return

    all_rules = resubscribe_db.get_all_resubscribe_rules()
    config = processor.config
    delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))
    resubscribed_count, deleted_count = 0, 0

    for i, item in enumerate(items_to_subscribe):
        if processor.is_stop_requested(): break
        
        current_quota = settings_db.get_subscription_quota()
        if current_quota <= 0:
            logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
            break

        item_id = item.get('item_id')
        item_name = item.get('item_name')
        tmdb_id = item.get('tmdb_id')
        item_type = item.get('item_type') # Movie, Season
        season_number = item.get('season_number') if item_type == 'Season' else None

        task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) [配额:{current_quota}] 正在订阅: {item_name}")

        rule = next((r for r in all_rules if r['id'] == item.get('matched_rule_id')), None)
        if not rule: continue

        # ==================== 分支逻辑 ====================
        rule_type = rule.get('rule_type', 'resubscribe')

        # --- 分支 1: 仅删除模式 ---
        if rule_type == 'delete':
            delete_mode = rule.get('delete_mode', 'episode') # 'episode' (逐集) or 'series' (整锅端)
            delay_seconds = int(rule.get('delete_delay_seconds', 0))
            
            # 1. 确定要删除的目标 ID 列表
            ids_to_delete_queue = []
            main_target_id = item.get('emby_item_id') # 季ID 或 电影ID
            
            # 如果是电影，无论什么模式，都只有一个 ID
            if item_type == 'Movie':
                if main_target_id:
                    ids_to_delete_queue.append(main_target_id)
            
            # 如果是季 (Season)
            elif item_type == 'Season':
                if delete_mode == 'series':
                    # 模式 A: 整季删除 (直接删季 ID)
                    if main_target_id:
                        ids_to_delete_queue.append(main_target_id)
                else:
                    # 模式 B: 逐集删除 (查询所有集 ID)
                    tmdb_id = item.get('tmdb_id')
                    season_number = item.get('season_number')
                    episode_ids = resubscribe_db.get_episode_ids_for_season(tmdb_id, season_number)
                    
                    if episode_ids:
                        ids_to_delete_queue.extend(episode_ids)
                        logger.info(f"  ➜ [防风控] 已获取《{item_name}》下的 {len(episode_ids)} 个分集，将逐一删除。")
                    else:
                        # 如果没找到分集（可能是空季），则回退到删除季本身
                        if main_target_id:
                            ids_to_delete_queue.append(main_target_id)

            if not ids_to_delete_queue:
                logger.warning(f"  ➜ 无法执行删除 {item_name}: 未找到有效的 Emby ID。")
                continue

            # 2. 执行删除队列
            success_count = 0
            total_files = len(ids_to_delete_queue)
            
            task_manager.update_status_from_thread(int((i / total) * 100), f"正在清理: {item_name} ({total_files}个文件)")

            for idx, target_id in enumerate(ids_to_delete_queue):
                if processor.is_stop_requested(): break
                
                # 执行删除
                if emby.delete_item(target_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
                    success_count += 1
                    logger.info(f"    - 已删除文件 ({idx+1}/{total_files}): ID {target_id}")
                else:
                    logger.error(f"    - 删除失败: ID {target_id}")

                # ★★★ 核心：防风控延迟 ★★★
                if delay_seconds > 0 and idx < total_files - 1:
                    time.sleep(delay_seconds)

            # 3. 善后处理
            if success_count > 0:
                # 如果是逐集删除模式，删完所有集后，尝试把那个空的“季”文件夹也删了（清理垃圾）
                if item_type == 'Season' and delete_mode == 'episode' and main_target_id:
                    try:
                        logger.info(f"    - 分集清理完毕，正在移除空的季容器: {main_target_id}")
                        emby.delete_item(main_target_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                    except:
                        pass # 删不掉也无所谓，Emby 扫库后会自动处理

                # 数据库清理
                try:
                    maintenance_db.cleanup_deleted_media_item(main_target_id, item_name, item_type)
                except Exception as e:
                    logger.error(f"  ➜ 善后清理失败: {e}")
                
                deleted_count += 1
                # 从洗版索引中移除
                resubscribe_db.delete_resubscribe_index_by_keys([item.get('tmdb_id') if item_type == 'Movie' else f"{item.get('tmdb_id')}-S{item.get('season_number')}"])
            
            continue # 删除模式结束，跳过后续逻辑
        
        # --- 分支 2: 洗版模式 ---
        payload = build_resubscribe_payload(item, rule)
        if not payload: continue

        # ======================================================================
        # ★★★ 先尝试取消旧订阅，确保洗版参数生效 ★★★
        # ======================================================================
        try:
            logger.info(f"  ➜ 正在检查并清理《{item_name}》的旧订阅...")
            
            # 调用 moviepilot.cancel_subscription
            # 即使订阅不存在，该函数也会返回 True，所以直接调用即可
            if moviepilot.cancel_subscription(str(tmdb_id), item_type, config, season=season_number):
                logger.info(f"  ➜ 旧订阅清理指令已发送，等待 2 秒以确保 MoviePilot 数据库同步...")
                time.sleep(2) # <--- 增加延时，防止竞态条件
            else:
                logger.warning(f"  ➜ 旧订阅清理失败（可能是网络问题），尝试强行提交新订阅...")
                
        except Exception as e:
            logger.error(f"  ➜ 清理旧订阅时发生错误: {e}，继续尝试提交...")
        # ======================================================================

        # 提交新订阅
        if moviepilot.subscribe_with_custom_payload(payload, config):
            settings_db.decrement_subscription_quota()
            resubscribed_count += 1
            
            # 处理“洗版后删除”逻辑
            if rule and rule.get('delete_after_resubscribe'):
                id_to_delete = item.get('emby_item_id') or item_id
                if emby.delete_item(id_to_delete, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
                    try:
                        logger.info(f"  ➜ 源文件删除成功，开始为 Emby ID {id_to_delete} (Name: {item_name}) 执行数据库善后清理...")
                        maintenance_db.cleanup_deleted_media_item(
                            item_id=id_to_delete,
                            item_name=item_name,
                            item_type=item_type
                        )
                        logger.info(f"  ➜ Emby ID {id_to_delete} 的善后清理已完成。")
                    except Exception as cleanup_e:
                        logger.error(f"  ➜ 执行善后清理 media item {id_to_delete} 时发生错误: {cleanup_e}", exc_info=True)
                    deleted_count += 1
                else:
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
            else:
                resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
            
            if i < total - 1: time.sleep(delay)

    final_message = f"任务完成！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
    task_manager.update_status_from_thread(100, final_message)