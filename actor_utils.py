# actor_utils.py
import threading
import concurrent.futures
import time
import psycopg2
import constants
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
# 导入底层工具箱和日志
import logging
from database import connection
from database.actor_db import ActorDBManager
import utils
import tmdb_handler
import emby_handler
from douban import DoubanApi
from ai_translator import AITranslator
from utils import contains_chinese

logger = logging.getLogger(__name__)

# --- 演员选择 ---
def select_best_role(current_role: str, candidate_role: str) -> str:
    """
    根据优先级选择最佳角色名。
    【最终修正版】确保有价值的中文名不会被英文名覆盖。

    优先级顺序:
    1. 有内容的豆瓣中文角色名
    2. 有内容的本地中文角色名
    3. 有内容的英文角色名 (豆瓣来源优先)
    4. '演员' (或其他占位符)
    5. 空字符串
    """
    # --- 步骤 1: 清理和规范化输入 ---
    original_current = current_role # 保存原始值用于日志
    original_candidate = candidate_role # 保存原始值用于日志
    
    current_role = str(current_role or '').strip()
    candidate_role = str(candidate_role or '').strip()

    # --- 步骤 2: 准备日志和判断标志 ---
    logger.info(f"  ➜ 备选角色名: 当前='{current_role}', 豆瓣='{candidate_role}'")

    current_is_chinese = utils.contains_chinese(current_role)
    candidate_is_chinese = utils.contains_chinese(candidate_role)
    
    # 定义一个更广泛的占位符列表
    placeholders = {"actor", "actress", "演员", "配音"}
    current_is_placeholder = current_role.lower() in placeholders
    candidate_is_placeholder = candidate_role.lower() in placeholders

    # --- 步骤 3: 应用优先级规则并记录决策 ---

    # 优先级 1: 豆瓣角色是有效的中文名
    if candidate_is_chinese and not candidate_is_placeholder:
        logger.trace(f"  ➜ 决策: [优先级1] 豆瓣角色是有效中文名。选择豆瓣角色。")
        logger.info(f"    └─ 选择: '{candidate_role}'")
        return candidate_role

    # 优先级 2: 当前角色是有效的中文名，而豆瓣角色不是。必须保留当前角色！
    if current_is_chinese and not current_is_placeholder and not candidate_is_chinese:
        logger.trace(f"  ➜ 决策: [优先级2] 当前角色是有效中文名，而豆瓣不是。保留当前角色。")
        logger.info(f"      └─ 选择: '{current_role}'")
        return current_role

    # 优先级 3: 两者都不是有效的中文名（或都是）。选择一个非占位符的，豆瓣者优先。
    if candidate_role and not candidate_is_placeholder:
        logger.trace(f"  ➜ 决策: [优先级3] 豆瓣角色是有效的非中文名/占位符。选择豆瓣角色。")
        logger.info(f"      └─ 选择: '{candidate_role}'")
        return candidate_role
    
    if current_role and not current_is_placeholder:
        logger.trace(f"  ➜ 决策: [优先级4] 当前角色是有效的非中文名/占位符，而豆瓣角色是无效的。保留当前角色。")
        logger.info(f"      └─ 选择: '{current_role}'")
        return current_role

    # 优先级 4: 处理占位符。如果两者之一是占位符，则返回一个（豆瓣优先）。
    if candidate_role: # 如果豆瓣有内容（此时只能是占位符）
        logger.trace(f"  ➜ 决策: [优先级5] 豆瓣角色是占位符。选择豆瓣角色。")
        logger.info(f"      └─ 选择: '{candidate_role}'")
        return candidate_role
        
    if current_role: # 如果当前有内容（此时只能是占位符）
        logger.trace(f"  ➜ 决策: [优先级6] 当前角色是占位符，豆瓣为空。保留当前角色。")
        logger.info(f"      └─ 选择: '{current_role}'")
        return current_role

    # 优先级 5: 所有情况都处理完，只剩下两者都为空。
    logger.trace(f"  ➜ 决策: [优先级7] 所有输入均为空或无效。返回空字符串。")
    logger.info(f"      └─ 选择: ''")
    return ""

# --- 质量评估 ---
def evaluate_cast_processing_quality(
    final_cast: List[Dict[str, Any]], 
    original_cast_count: int, 
    expected_final_count: Optional[int] = None,
    is_animation: bool = False  # ✨✨✨ 新增参数，默认为 False ✨✨✨
) -> float:
    """
    【V-Final 极简版 - 动画片优化】
    只关心最终产出的中文化质量和演员数量。
    如果检测到是动画片，则跳过所有关于数量的惩罚。
    """
    if not final_cast:
        # ✨ 如果是动画片且演员列表为空，可以给一个基础通过分，避免进手动列表
        if is_animation:
            logger.info("  ➜ 质量评估：动画片/纪录片演员列表为空，属于正常情况，给予基础通过分 7.0。")
            return 7.0
        else:
            logger.warning("  ➜ 处理后演员列表为空！评为 0.0 分。")
            return 0.0
        
    total_actors = len(final_cast)
    accumulated_score = 0.0
    
    logger.debug(f"--- 质量评估开始 ---")
    logger.debug(f"  - 原始演员数: {original_cast_count}")
    logger.debug(f"  - 处理后演员数: {total_actors}")
    logger.debug(f"------------------")

    for i, actor_data in enumerate(final_cast):
        # 每个演员的基础分是 0.0，通过加分项累加
        score = 0.0
        
        # --- 智能获取数据 ---
        actor_name = actor_data.get("name") or actor_data.get("Name")
        actor_role = actor_data.get("character") or actor_data.get("Role")
        
        # --- 演员名评分 (满分 5.0) ---
        if actor_name and utils.contains_chinese(actor_name):
            score += 5.0
        elif actor_name:
            score += 1.0 # 保留一个较低的基础分给英文名

        # --- 角色名评分 (满分 5.0) ---
        placeholders = {"演员", "配音"}
        is_placeholder = (str(actor_role).endswith("(配音)")) or (str(actor_role) in placeholders)

        if actor_role and utils.contains_chinese(actor_role) and not is_placeholder:
            score += 5.0 # 有意义的中文角色名
        elif actor_role and utils.contains_chinese(actor_role) and is_placeholder:
            score += 2.5 # 中文占位符
        elif actor_role:
            score += 0.5 # 英文角色名

        final_actor_score = min(10.0, score)
        accumulated_score += final_actor_score
        
        logger.debug(f"    ├─ [{i+1}/{total_actors}] 演员: '{actor_name}' (角色: '{actor_role}') | 单项评分: {final_actor_score:.1f}")

    avg_score = accumulated_score / total_actors if total_actors > 0 else 0.0
    
    # --- ✨✨✨ 核心修改：条件化的数量惩罚逻辑 ✨✨✨ ---
    logger.debug(f"------------------------------------")
    logger.debug(f"  ➜ 基础平均分 (惩罚前): {avg_score:.2f}")

    if is_animation:
        logger.debug("  ➜ 惩罚: 检测到为动画片/纪录片，跳过所有数量相关的惩罚。")
    else:
        # 只有在不是动画片时，才执行原来的数量惩罚逻辑
        if total_actors < 10:
            penalty_factor = total_actors / 10.0
            logger.warning(f"  ➜ 惩罚: 最终演员数({total_actors})少于10个，乘以惩罚因子 {penalty_factor:.2f}")
            avg_score *= penalty_factor
            
        elif expected_final_count is not None:
            if total_actors < expected_final_count * 0.8:
                penalty_factor = total_actors / expected_final_count
                logger.warning(f"  ➜ 惩罚: 数量({total_actors})远少于预期({expected_final_count})，乘以惩罚因子 {penalty_factor:.2f}")
                avg_score *= penalty_factor
        elif total_actors < original_cast_count * 0.8:
            penalty_factor = total_actors / original_cast_count
            logger.warning(f"  ➜ 惩罚: 数量从{original_cast_count}大幅减少到{total_actors}，乘以惩罚因子 {penalty_factor:.2f}")
            avg_score *= penalty_factor
        else:
            logger.debug(f"  ➜ 惩罚: 数量正常，不进行惩罚。")
    
    final_score_rounded = round(avg_score, 1)
    logger.info(f"  ➜ 最终评分: {final_score_rounded:.1f} ---")
    return final_score_rounded

# --- 翻译演员的特定字段 ---
def translate_actor_field(text: Optional[str], db_manager: ActorDBManager, db_cursor: psycopg2.extensions.cursor, ai_translator: Optional[AITranslator], translator_engines: List[str], ai_enabled: bool) -> Optional[str]:
    """翻译演员的特定字段，智能选择AI或传统翻译引擎。"""
    # 1. 前置检查：如果文本为空、是纯空格，或已包含中文，则直接返回原文
    if not text or not text.strip() or utils.contains_chinese(text):
        return text
    
    text_stripped = text.strip()

    # 2. 前置检查：跳过短的大写字母缩写
    if len(text_stripped) <= 2 and text_stripped.isupper():
        return text

    # 3. 核心修复：优先从数据库读取缓存，并处理所有情况
    cached_entry = db_manager.get_translation_from_db(db_cursor, text_stripped)
    if cached_entry:
        # 情况 A: 缓存中有成功的翻译结果
        if cached_entry.get("translated_text"):
            cached_translation = cached_entry.get("translated_text")
            logger.info(f"数据库翻译缓存命中 for '{text_stripped}' -> '{cached_translation}'")
            return cached_translation
        # 情况 B: 缓存中明确记录了这是一个失败的翻译
        else:
            logger.debug(f"数据库翻译缓存命中 (失败记录) for '{text_stripped}'，不再尝试在线翻译。")
            return text # 直接返回原文，避免重复请求

    # 4. 如果缓存中完全没有记录，才进行在线翻译
    logger.debug(f"'{text_stripped}' 在翻译缓存中未找到，将进行在线翻译...")
    final_translation = None
    final_engine = "unknown"

    # 根据配置选择翻译方式
    ai_translation_attempted = False

    # 步骤 1: 如果AI翻译启用，优先尝试AI
    if ai_translator and ai_enabled:
        ai_translation_attempted = True
        logger.debug(f"AI翻译已启用，优先尝试使用 '{ai_translator.provider}' 进行翻译...")
        try:
            # ai_translator.translate 应该在失败时返回 None 或抛出异常
            ai_result = ai_translator.translate(text_stripped)
            if ai_result: # 确保AI返回了有效结果
                final_translation = ai_result
                final_engine = ai_translator.provider
        except Exception as e_ai:
            # 如果AI翻译器内部抛出异常，在这里捕获
            logger.error(f"AI翻译器在翻译 '{text_stripped}' 时发生异常: {e_ai}")
            # 不做任何事，让流程继续往下走，尝试传统引擎

    # 5. 处理在线翻译的结果，并更新缓存
    if final_translation and final_translation.strip() and final_translation.strip().lower() != text_stripped.lower():
        # 翻译成功，存入缓存并返回结果
        logger.info(f"在线翻译成功: '{text_stripped}' -> '{final_translation}' (使用引擎: {final_engine})")
        db_manager.save_translation_to_db(db_cursor, text_stripped, final_translation, final_engine)
        return final_translation
    else:
        # 翻译失败或返回原文，将失败状态存入缓存，并返回原文
        logger.warning(f"在线翻译未能翻译 '{text_stripped}' 或返回了原文 (使用引擎: {final_engine})。")
        db_manager.save_translation_to_db(db_cursor, text_stripped, None, f"failed_or_same_via_{final_engine}")
        return text

# ✨✨✨从豆瓣API获取指定媒体的演员原始数据列表✨✨✨
def find_douban_cast(douban_api: DoubanApi, media_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从豆瓣API获取演员原始数据。"""
        # 假设 constants 和 self.douban_api 已经存在
        # if not (getattr(constants, 'DOUBAN_API_AVAILABLE', False) and self.douban_api and \
        #         self.data_source_mode in [constants.DOMESTIC_SOURCE_MODE_LOCAL_THEN_ONLINE, constants.DOMESTIC_SOURCE_MODE_ONLINE_ONLY]):
        #     return []
        if not douban_api:
            logger.warning("未提供 DoubanApi 实例，无法获取豆瓣演员。")
            return []
        douban_data = douban_api.get_acting(
            name=media_info.get("Name"),
            imdbid=media_info.get("ProviderIds", {}).get("Imdb"),
            mtype="movie" if media_info.get("Type") == "Movie" else ("tv" if media_info.get("Type") == "Series" else None),
            year=str(media_info.get("ProductionYear", "")),
            douban_id_override=media_info.get("ProviderIds", {}).get("Douban")
        )
        if douban_data and not douban_data.get("error") and isinstance(douban_data.get("cast"), list):
            return douban_data["cast"]
        return []

# ✨✨✨格式化从豆瓣获取的原始演员数据，进行初步清理和去重，使其符合内部处理格式✨✨✨
def format_douban_cast(douban_api_actors_raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    【修复版】
    格式化豆瓣原始演员数据并进行初步去重。
    - 新增：提取并保留豆瓣提供的现成头像链接。
    """
    formatted_candidates = []
    seen_douban_ids = set()
    seen_names = set()

    if not douban_api_actors_raw:
        return formatted_candidates

    for item in douban_api_actors_raw:
        name_zh = str(item.get("name", "")).strip()
        if not name_zh: 
            continue
            
        douban_id = str(item.get("id", "")).strip() or None

        # 【严格的去重逻辑】
        if douban_id and douban_id in seen_douban_ids:
            continue
        if name_zh in seen_names:
            continue

        if douban_id:
            seen_douban_ids.add(douban_id)
        seen_names.add(name_zh)
        
        # ▼▼▼ 核心新增：从缓存中安全地提取头像链接 ▼▼▼
        avatar_url = (item.get("avatar", {}) or {}).get("large")
        # ▲▲▲ 新增结束 ▲▲▲

        formatted_candidates.append({
            "Name": name_zh,
            # 修正：根据你提供的JSON，字段应为 latin_name
            "OriginalName": str(item.get("latin_name", "")).strip(), 
            "Role": str(item.get("character", "")).strip(),
            "DoubanCelebrityId": douban_id,
            "ProviderIds": {"Douban": douban_id} if douban_id else {},
            # 新增字段，将头像链接传递下去
            "DoubanAvatarUrl": avatar_url 
        })
        
    return formatted_candidates

# ✨✨✨格式化演员表✨✨✨
def format_and_complete_cast_list(
    cast_list: List[Dict[str, Any]], 
    is_animation: bool, 
    config: Dict[str, Any],
    mode: str = 'auto'  # ★★★ 核心参数: 'auto' 或 'manual' ★★★
) -> List[Dict[str, Any]]:
    """
    【V9 - 最终策略版】根据调用模式格式化并排序演员列表。
    - 'auto': 自动处理流程。严格按原始TMDb的 'order' 字段排序。
    - 'manual': 手动编辑流程。以传入列表的顺序为基准，并将通用角色排到末尾。
    """
    processed_cast = []
    add_role_prefix = config.get(constants.CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX, False)
    generic_roles = {"演员", "配音"}

    logger.debug(f"  ➜ 格式化演员列表，调用模式: '{mode}' (前缀开关: {'开' if add_role_prefix else '关'})")
    # --- 阶段1: 统一的角色名格式化 (所有模式通用) ---
    for idx, actor in enumerate(cast_list):
        new_actor = actor.copy()
        
        # (角色名处理逻辑保持不变)
        character_name = new_actor.get("character")
        final_role = character_name.strip() if character_name else ""
        if utils.contains_chinese(final_role):
            final_role = final_role.replace(" ", "").replace("　", "")
        if add_role_prefix:
            if final_role and final_role not in generic_roles:
                prefix = "配 " if is_animation else "饰 "
                final_role = f"{prefix}{final_role}"
            elif not final_role:
                final_role = "配音" if is_animation else "演员"
        else:
            if not final_role:
                final_role = "配音" if is_animation else "演员"
        new_actor["character"] = final_role
        
        # 为 'manual' 模式记录原始顺序
        new_actor['original_index'] = idx
        
        processed_cast.append(new_actor)

    # --- 阶段2: 根据模式执行不同的排序策略 ---
    if mode == 'manual':
        # 【手动模式】：以用户自定义顺序为基础，并增强（通用角色后置）
        logger.debug("  ➜ 应用 'manual' 排序策略：保留用户自定义顺序，并将通用角色后置。")
        processed_cast.sort(key=lambda actor: (
            1 if actor.get("character") in generic_roles else 0,  # 1. 通用角色排在后面
            actor.get("original_index")                          # 2. 在此基础上，保持原始手动顺序
        ))
    else: # mode == 'auto' 或其他任何默认情况
        # 【自动模式】：严格按照TMDb原始的 'order' 字段排序
        logger.debug("  ➜ 应用 'auto' 排序策略：严格按原始TMDb 'order' 字段排序。")
        processed_cast.sort(key=lambda actor: actor.get('order', 999))
        
    # --- 阶段3: 最终重置 order 索引 (所有模式通用) ---
    for new_idx, actor in enumerate(processed_cast):
        actor["order"] = new_idx
        if 'original_index' in actor:
            del actor['original_index'] # 清理临时key
            
    return processed_cast

# --- 用于获取单个演员的TMDb详情 ---
def fetch_tmdb_details_for_actor(actor_info: Dict, tmdb_api_key: str) -> Optional[Dict]:
    """一个独立的、可在线程中运行的函数，用于获取单个演员的TMDb详情。"""
    tmdb_id = actor_info.get("tmdb_person_id")
    if not tmdb_id:
        return None
    try:
        details = tmdb_handler.get_person_details_tmdb(
            person_id=int(tmdb_id), 
            api_key=tmdb_api_key, 
            append_to_response="external_ids,translations"
        )
        if details:
            # 成功获取，返回详情
            return {"tmdb_id": tmdb_id, "status": "found", "details": details}
        else:
            # API调用成功但返回空，也标记为未找到
            return {"tmdb_id": tmdb_id, "status": "not_found"}

    except tmdb_handler.TMDbResourceNotFound:
        # ★★★ 捕获到404异常，返回一个明确的“未找到”状态 ★★★
        return {"tmdb_id": tmdb_id, "status": "not_found"}
    
    except tmdb_handler.TMDbAPIError as e:
        # 其他API错误（如网络问题），记录日志并返回失败状态
        logger.warning(f"获取演员 {tmdb_id} 详情时遇到API错误: {e}")
        return {"tmdb_id": tmdb_id, "status": "failed"}

# --- 演员数据补充 ---
def enrich_all_actor_aliases_task(
    tmdb_api_key: str, 
    run_duration_minutes: int,
    sync_interval_days: int,
    stop_event: Optional[threading.Event] = None,
    update_status_callback: Optional[Callable] = None,
    force_full_update: bool = False  # <-- 新增参数
):
    """
    【V8 - 真·完整修复版】
    - 新增 阶段三：为那些在TMDb没有头像，但有关联豆瓣ID的演员，从豆瓣补充头像链接。
    - 调整了函数结构和进度反馈，以容纳新的处理阶段。
    - 确保所有原始的数据库事务和错误处理逻辑被完整保留。
    """
    task_mode = "(全量)" if force_full_update else "(增量)"
    logger.info(f"--- 开始执行“演员数据补充”计划任务 [{task_mode}] ---")
    
    start_time = time.time()
    end_time = float('inf')
    if run_duration_minutes > 0:
        end_time = start_time + run_duration_minutes * 60
        end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"任务将运行 {run_duration_minutes} 分钟，预计在 {end_time_str} 左右自动停止。")

    SYNC_INTERVAL_DAYS = sync_interval_days
    logger.info(f"  ➜ 同步冷却时间为 {SYNC_INTERVAL_DAYS} 天。")

    conn = None
    douban_api = None # 在 try 外部定义，确保 finally 中可用
    try:
        # 将 DoubanApi 实例化移到顶层，供所有阶段使用
        douban_api = DoubanApi()

        with connection.get_db_connection() as conn:
            # --- 阶段一：从 TMDb 补充元数据 (并发执行) ---
            logger.info("  ➜ 阶段一：从 TMDb 补充演员元数据 (IMDb ID, 头像等) ---")
            cursor = conn.cursor()
            
            # ▼▼▼ (此部分为您原始代码，保持不变) ▼▼▼
            if force_full_update:
                logger.info("  ➜ 深度模式已激活：将扫描所有演员，无视现有数据。")
                sql_find_actors = f"""
                    SELECT p.* FROM person_identity_map p
                    LEFT JOIN actor_metadata m ON p.tmdb_person_id = m.tmdb_id
                    WHERE p.tmdb_person_id IS NOT NULL
                    ORDER BY m.last_updated_at ASC NULLS FIRST
                """
            else:
                logger.info(f"  ➜ 标准模式：将仅扫描需要补充数据且冷却期已过的演员 (冷却期: {sync_interval_days} 天)。")
                sql_find_actors = f"""
                    SELECT p.* FROM person_identity_map p
                    LEFT JOIN actor_metadata m ON p.tmdb_person_id = m.tmdb_id
                    WHERE p.tmdb_person_id IS NOT NULL AND (p.imdb_id IS NULL OR m.tmdb_id IS NULL OR m.profile_path IS NULL OR m.gender IS NULL OR m.original_name IS NULL)
                    AND (m.last_updated_at IS NULL OR m.last_updated_at < NOW() - INTERVAL '{sync_interval_days} days')
                    ORDER BY m.last_updated_at ASC
                """
            
            cursor.execute(sql_find_actors)
            actors_for_tmdb = cursor.fetchall()
            
            if actors_for_tmdb:
                total_tmdb = len(actors_for_tmdb)
                logger.info(f"  ➜ 找到 {total_tmdb} 位演员需要从 TMDb 补充元数据。")
                
                CHUNK_SIZE = 200
                MAX_TMDB_WORKERS = 5

                for i in range(0, total_tmdb, CHUNK_SIZE):
                    if (stop_event and stop_event.is_set()) or (time.time() >= end_time):
                        logger.info("达到运行时长或收到停止信号，在 TMDb 下批次开始前结束。")
                        break

                    # ▼▼▼ 修改：调整进度条分配，阶段一占 5% -> 60% ▼▼▼
                    progress = 5 + int((i / total_tmdb) * 55)
                    chunk_num = i//CHUNK_SIZE + 1
                    total_chunks = (total_tmdb + CHUNK_SIZE - 1) // CHUNK_SIZE
                    if update_status_callback:
                        update_status_callback(progress, f"阶段1/3 (TMDb): 处理批次 {chunk_num}/{total_chunks}")

                    chunk = actors_for_tmdb[i:i + CHUNK_SIZE]
                    logger.info(f"  ➜ 开始处理 TMDb 第 {chunk_num} 批次，共 {len(chunk)} 个演员 ---")

                    imdb_updates_to_commit = []
                    metadata_to_commit = []
                    invalid_tmdb_ids = []
                    
                    tmdb_success_count, imdb_found_count, metadata_added_count, not_found_count = 0, 0, 0, 0

                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_TMDB_WORKERS) as executor:
                        future_to_actor = {executor.submit(fetch_tmdb_details_for_actor, dict(actor), tmdb_api_key): actor for actor in chunk}
                        
                        for future in concurrent.futures.as_completed(future_to_actor):
                            if stop_event and stop_event.is_set():
                                for f in future_to_actor: f.cancel()
                                raise InterruptedError("任务在TMDb处理批次中被中止")

                            result = future.result()
                            if not result: continue

                            status = result.get("status")
                            tmdb_id = result.get("tmdb_id")
                            details = result.get("details", {})

                            if status == "found" and details:
                                tmdb_success_count += 1
                                imdb_id = details.get("external_ids", {}).get("imdb_id")
                                if imdb_id:
                                    imdb_found_count += 1
                                    imdb_updates_to_commit.append((imdb_id, tmdb_id))
                                
                                best_original_name = None
                                if details.get("english_name_from_translations"):
                                    best_original_name = details.get("english_name_from_translations")
                                elif details.get("original_name") and not contains_chinese(details.get("original_name")):
                                    best_original_name = details.get("original_name")
                                
                                metadata_entry = {
                                    "tmdb_id": tmdb_id,
                                    "profile_path": details.get("profile_path"),
                                    "gender": details.get("gender"),
                                    "adult": details.get("adult", False),
                                    "popularity": details.get("popularity"),
                                    "original_name": best_original_name
                                }
                                metadata_to_commit.append(metadata_entry)
                                metadata_added_count += 1
                            
                            elif status == "not_found":
                                not_found_count += 1
                                invalid_tmdb_ids.append(tmdb_id)

                    logger.info(
                        f"  ➜ 批次处理完成。摘要: "
                        f"成功获取({tmdb_success_count}), 新增IMDb({imdb_found_count}), "
                        f"新增元数据({metadata_added_count}), 未找到({not_found_count})."
                    )
                    
                    if imdb_updates_to_commit or metadata_to_commit or invalid_tmdb_ids:
                        try:
                            logger.info(f"  ➜ 批次完成，准备写入数据库...")

                            if metadata_to_commit:
                                cols = metadata_to_commit[0].keys()
                                cols_str = ", ".join(cols)
                                placeholders_str = ", ".join([f"%({k})s" for k in cols])
                                update_cols = [f"{col} = EXCLUDED.{col}" for col in cols if col != 'tmdb_id']
                                update_str = ", ".join(update_cols)
                                
                                sql_upsert_metadata = f"""
                                    INSERT INTO actor_metadata ({cols_str}, last_updated_at)
                                    VALUES ({placeholders_str}, NOW())
                                    ON CONFLICT (tmdb_id) DO UPDATE SET {update_str}, last_updated_at = NOW()
                                """
                                cursor.executemany(sql_upsert_metadata, metadata_to_commit)
                                logger.trace(f"成功批量写入 {len(metadata_to_commit)} 条演员元数据。")

                            for imdb_id, tmdb_id in imdb_updates_to_commit:
                                try:
                                    cursor.execute("SAVEPOINT imdb_update_savepoint")
                                    cursor.execute("UPDATE person_identity_map SET imdb_id = %s WHERE tmdb_person_id = %s", (imdb_id, tmdb_id))
                                    cursor.execute("RELEASE SAVEPOINT imdb_update_savepoint")
                                except psycopg2.IntegrityError as ie:
                                    cursor.execute("ROLLBACK TO SAVEPOINT imdb_update_savepoint")
                                    if "violates unique constraint" in str(ie):
                                        if force_full_update:
                                            logger.warning(f"  ➜ [深度模式] 检测到 IMDb ID '{imdb_id}' 冲突。将强制以TMDb数据为准。")
                                            cursor.execute("SELECT map_id, primary_name FROM person_identity_map WHERE imdb_id = %s", (imdb_id,))
                                            conflicting_actor = cursor.fetchone()
                                            if conflicting_actor:
                                                logger.warning(f"  ➜ 正在解除演员 '{conflicting_actor['primary_name']}' (map_id: {conflicting_actor['map_id']}) 与 IMDb ID '{imdb_id}' 的旧关联。")
                                                cursor.execute("UPDATE person_identity_map SET imdb_id = NULL WHERE map_id = %s", (conflicting_actor['map_id'],))
                                                
                                                logger.info(f"  ➜ 正在为当前演员 (TMDb: {tmdb_id}) 设置新的 IMDb ID '{imdb_id}'。")
                                                cursor.execute("UPDATE person_identity_map SET imdb_id = %s WHERE tmdb_person_id = %s", (imdb_id, tmdb_id))
                                            else:
                                                logger.error(f"  ➜ 发生冲突但未能找到 IMDb ID '{imdb_id}' 的冲突记录，更新失败。")
                                        else:
                                            logger.warning(f"  ➜ [标准模式] 检测到 IMDb ID '{imdb_id}' (来自TMDb: {tmdb_id}) 冲突。将执行合并逻辑。")
                                            sql_find_target = "SELECT * FROM person_identity_map WHERE imdb_id = %s"
                                            cursor.execute(sql_find_target, (imdb_id,))
                                            target_actor = cursor.fetchone()
                                            
                                            sql_find_source = "SELECT * FROM person_identity_map WHERE tmdb_person_id = %s"
                                            cursor.execute(sql_find_source, (tmdb_id,))
                                            source_actor = cursor.fetchone()

                                            if target_actor and source_actor and source_actor['map_id'] != target_actor['map_id']:
                                                target_map_id = target_actor['map_id']
                                                source_map_id = source_actor['map_id']
                                                
                                                logger.info(f"  ➜ 准备合并：源(map_id:{source_map_id}, tmdb:{tmdb_id}) -> 目标(map_id:{target_map_id}, imdb:{imdb_id})")

                                                # 1. 先从源记录中移除导致冲突的 tmdb_person_id，避免后续更新失败
                                                cursor.execute("UPDATE person_identity_map SET tmdb_person_id = NULL WHERE map_id = %s", (source_map_id,))

                                                # 2. 现在可以安全地将 tmdb_person_id 更新到目标记录
                                                if not target_actor.get('tmdb_person_id'):
                                                    cursor.execute("UPDATE person_identity_map SET tmdb_person_id = %s WHERE map_id = %s", (source_actor['tmdb_person_id'], target_map_id))
                                                
                                                # 3. 合并其他可能存在于源记录但目标记录没有的信息
                                                if source_actor.get('douban_celebrity_id') and not target_actor.get('douban_celebrity_id'):
                                                    cursor.execute("UPDATE person_identity_map SET douban_celebrity_id = %s WHERE map_id = %s", (source_actor['douban_celebrity_id'], target_map_id))
                                                if source_actor.get('emby_person_id') and not target_actor.get('emby_person_id'):
                                                    cursor.execute("UPDATE person_identity_map SET emby_person_id = %s WHERE map_id = %s", (source_actor['emby_person_id'], target_map_id))

                                                # 4. 最后，删除现在已经为空壳的源记录
                                                cursor.execute("DELETE FROM person_identity_map WHERE map_id = %s", (source_map_id,))
                                                logger.info(f"  ➜ 成功将记录 (map_id:{source_map_id}) 合并到 (map_id:{target_map_id}) 并删除原记录。")
                                            
                                            elif not target_actor:
                                                logger.error(f"  ➜ 发生冲突但未能找到 IMDb ID '{imdb_id}' 的目标记录，合并失败。")
                                            elif not source_actor:
                                                logger.error(f"  ➜ 发生冲突但未能找到 TMDb ID '{tmdb_id}' 的源记录，合并失败。")
                                    else:
                                        raise ie

                            if invalid_tmdb_ids:
                                cursor.executemany("UPDATE person_identity_map SET tmdb_person_id = NULL WHERE tmdb_person_id = %s", [(tid,) for tid in invalid_tmdb_ids])

                            conn.commit()
                            logger.info("✅ 数据库更改已成功提交。")

                        except Exception as db_e:
                            logger.error(f"数据库操作失败: {db_e}", exc_info=True)
                            conn.rollback()
            else:
                logger.info("  ➜ 没有需要从 TMDb 补充或清理的演员。")
            # ▲▲▲ (此部分为您原始代码，保持不变) ▲▲▲

            # --- 阶段二：从 豆瓣 补充 IMDb ID (串行执行) ---
            if (stop_event and stop_event.is_set()) or (time.time() >= end_time): raise InterruptedError("任务中止")
            
            logger.info("  ➜ 阶段二：从 豆瓣 补充 IMDb ID ---")
            cursor = conn.cursor()
            sql_find_douban_needy = f"""
                SELECT * FROM person_identity_map 
                WHERE douban_celebrity_id IS NOT NULL AND imdb_id IS NULL AND tmdb_person_id IS NULL
                AND (last_synced_at IS NULL OR last_synced_at < NOW() - INTERVAL '{SYNC_INTERVAL_DAYS} days')
                ORDER BY last_synced_at ASC
            """
            cursor.execute(sql_find_douban_needy)
            actors_for_douban = cursor.fetchall()

            if actors_for_douban:
                total_douban = len(actors_for_douban)
                logger.info(f"  ➜ 找到 {total_douban} 位演员需要从豆瓣补充 IMDb ID。")
                
                processed_count = 0
                for i, actor in enumerate(actors_for_douban):
                    if (stop_event and stop_event.is_set()) or (time.time() >= end_time): break
                    
                    processed_count = i + 1
                    actor_map_id = actor['map_id']
                    actor_douban_id = actor['douban_celebrity_id']
                    actor_primary_name = actor['primary_name']
                    
                    # ▼▼▼ 修改：调整进度条分配，阶段二占 60% -> 80% ▼▼▼
                    progress = 60 + int(((i + 1) / total_douban) * 20)
                    if update_status_callback:
                        update_status_callback(progress, f"阶段2/3 (豆瓣->IMDb): {i+1}/{total_douban} - {actor_primary_name}")
                    
                    # ▼▼▼ (此部分为您原始代码，保持不变) ▼▼▼
                    try:
                        sql_update_sync = "UPDATE person_identity_map SET last_synced_at = NOW() WHERE map_id = %s"
                        cursor.execute(sql_update_sync, (actor_map_id,))

                        details = douban_api.celebrity_details(actor_douban_id)
                        
                        if details and not details.get("error"):
                            new_imdb_id = None
                            for item in details.get("extra", {}).get("info", []):
                                if isinstance(item, list) and len(item) == 2 and item[0] == 'IMDb编号':
                                    new_imdb_id = item[1]
                                    break
                            
                            if new_imdb_id:
                                logger.info(f"  ({i+1}/{total_douban}) 为演员 '{actor_primary_name}' (Douban: {actor_douban_id}) 找到 IMDb ID: {new_imdb_id}")
                                
                                try:
                                    cursor.execute("SAVEPOINT douban_update_savepoint")
                                    sql_update_imdb = "UPDATE person_identity_map SET imdb_id = %s WHERE map_id = %s"
                                    cursor.execute(sql_update_imdb, (new_imdb_id, actor_map_id))
                                    cursor.execute("RELEASE SAVEPOINT douban_update_savepoint")
                                
                                except psycopg2.IntegrityError as ie:
                                    cursor.execute("ROLLBACK TO SAVEPOINT douban_update_savepoint")
                                    if "violates unique constraint" in str(ie):
                                        logger.warning(f"  ➜ 检测到 IMDb ID '{new_imdb_id}' 冲突。将尝试合并记录。")
                                        
                                        sql_find_target = "SELECT map_id FROM person_identity_map WHERE imdb_id = %s"
                                        cursor.execute(sql_find_target, (new_imdb_id,))
                                        target_actor = cursor.fetchone()
                                        
                                        if target_actor:
                                            target_map_id = target_actor['map_id']
                                            sql_merge_douban = "UPDATE person_identity_map SET douban_celebrity_id = %s WHERE map_id = %s"
                                            cursor.execute(sql_merge_douban, (actor_douban_id, target_map_id))
                                            sql_delete_source = "DELETE FROM person_identity_map WHERE map_id = %s"
                                            cursor.execute(sql_delete_source, (actor_map_id,))
                                            logger.info(f"  ➜ 成功将 '{actor_primary_name}' (map_id: {actor_map_id}) 的豆瓣ID合并到记录 (map_id: {target_map_id}) 并删除原记录。")
                                        else:
                                            logger.error(f"  ➜ 发生冲突但未能找到 IMDb ID '{new_imdb_id}' 的目标记录，合并失败。")
                                    else:
                                        raise ie

                        if (i + 1) % 50 == 0:
                            logger.info(f"  ➜ 已处理50条，提交数据库事务...")
                            conn.commit()

                    except Exception as e:
                        conn.rollback()
                        logger.error(f"处理演员 '{actor_primary_name}' (Douban: {actor_douban_id}) 时发生错误: {e}")
                
                conn.commit()
                logger.info(f"豆瓣信息补充完成，本轮共处理 {processed_count} 个。")
            else:
                logger.info("  ➜ 没有需要从豆瓣补充 IMDb ID 的演员。")
            # ▲▲▲ (此部分为您原始代码，保持不变) ▲▲▲
            
            # ▼▼▼ 【核心新增】阶段三：从 豆瓣 补充 头像链接 ▼▼▼
            if (stop_event and stop_event.is_set()) or (time.time() >= end_time): raise InterruptedError("任务中止")

            logger.info("  ➜ 阶段三：从 豆瓣 补充 头像链接 ---")
            cursor = conn.cursor()
            sql_find_douban_avatar_needy = f"""
                SELECT p.map_id, p.tmdb_person_id, p.douban_celebrity_id, p.primary_name
                FROM person_identity_map p
                JOIN actor_metadata m ON p.tmdb_person_id = m.tmdb_id
                WHERE p.douban_celebrity_id IS NOT NULL
                  AND m.profile_path IS NULL
                  AND (p.last_synced_at IS NULL OR p.last_synced_at < NOW() - INTERVAL '{SYNC_INTERVAL_DAYS} days')
                ORDER BY p.last_synced_at ASC NULLS FIRST
            """
            cursor.execute(sql_find_douban_avatar_needy)
            actors_for_douban_avatar = cursor.fetchall()

            if actors_for_douban_avatar:
                total_douban_avatar = len(actors_for_douban_avatar)
                logger.info(f"  ➜ 找到 {total_douban_avatar} 位演员需要从豆瓣补充头像。")

                for i, actor in enumerate(actors_for_douban_avatar):
                    if (stop_event and stop_event.is_set()) or (time.time() >= end_time): break

                    # 进度条分配：阶段三占 80% -> 100%
                    progress = 80 + int(((i + 1) / total_douban_avatar) * 20)
                    if update_status_callback:
                        update_status_callback(progress, f"阶段3/3 (豆瓣->头像): {i+1}/{total_douban_avatar} - {actor['primary_name']}")

                    actor_map_id = actor['map_id']
                    actor_douban_id = actor['douban_celebrity_id']
                    actor_tmdb_id = actor['tmdb_person_id']
                    actor_primary_name = actor['primary_name']
                    
                    try:
                        # 无论成功与否，都更新同步时间戳，避免在冷却期内重复查询
                        cursor.execute("UPDATE person_identity_map SET last_synced_at = NOW() WHERE map_id = %s", (actor_map_id,))

                        details = douban_api.celebrity_details(actor_douban_id)
                        
                        if details and not details.get("error"):
                            avatar_url = (details.get("avatars", {}) or {}).get("large")
                            if avatar_url:
                                logger.info(f"  ({i+1}/{total_douban_avatar}) 为演员 '{actor_primary_name}' (TMDb: {actor_tmdb_id}) 找到豆瓣头像。")
                                # 更新 actor_metadata 表中的头像链接
                                sql_update_avatar = "UPDATE actor_metadata SET profile_path = %s, last_updated_at = NOW() WHERE tmdb_id = %s"
                                cursor.execute(sql_update_avatar, (avatar_url, actor_tmdb_id))
                        
                        if (i + 1) % 50 == 0:
                            logger.info(f"  ➜ 已处理50条，提交数据库事务...")
                            conn.commit()

                    except Exception as e:
                        conn.rollback()
                        logger.error(f"为演员 '{actor_primary_name}' (Douban: {actor_douban_id}) 补充头像时发生错误: {e}")
                
                conn.commit()
            else:
                logger.info("  ➜ 没有需要从豆瓣补充头像的演员。")
            # ▲▲▲ 新增结束 ▲▲▲

    except InterruptedError:
        logger.info("演员数据补充任务被中止。")
        if conn: conn.rollback()
    except Exception as e:
        logger.error(f"演员数据补充任务发生严重错误: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        # 将关闭操作移到 finally 块，确保无论如何都能执行
        if douban_api:
            douban_api.close()
        logger.trace("--- “演员数据补充”计划任务已退出 ---")