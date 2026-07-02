# 文件: handler/telegram.py
import json
import threading
import extensions
import requests
import logging
import re
from datetime import datetime
from config_manager import APP_CONFIG, get_proxies_for_requests
from handler.emby import get_emby_item_details
from database import user_db, request_db, media_db
from database.connection import get_db_connection
import constants
from handler.tg_media_candidate import build_channel_task_payload

logger = logging.getLogger(__name__)

_EPISODE_REF_PATTERN = re.compile(r'(?i)S(\d{1,3})\s*E(\d{1,4})(?:\s*-\s*E?(\d{1,4}))?')

def _format_episode_ranges(episode_list: list) -> str:
    """
    辅助函数：将 [(season, episode), ...] 转换为易读的范围字符串。
    输入: [(1, 1), (1, 2), (1, 3), (1, 5)]
    输出: "S01E01-E03, S01E05"
    """
    if not episode_list:
        return ""
    
    # 1. 按季分组
    season_map = {}
    for s, e in episode_list:
        season_map.setdefault(s, []).append(e)
    
    final_parts = []
    
    # 2. 按季排序处理
    for season in sorted(season_map.keys()):
        episodes = sorted(list(set(season_map[season]))) # 去重并排序
        if not episodes: continue
        
        # 3. 查找连续区间
        ranges = []
        start = episodes[0]
        prev = episodes[0]
        
        for ep in episodes[1:]:
            if ep == prev + 1:
                prev = ep
            else:
                # 结算上一段
                if start == prev:
                    ranges.append(f"E{start:02d}")
                else:
                    ranges.append(f"E{start:02d}-E{prev:02d}")
                start = ep
                prev = ep
        
        # 结算最后一段
        if start == prev:
            ranges.append(f"E{start:02d}")
        else:
            ranges.append(f"E{start:02d}-E{prev:02d}")
        
        # 4. 组装当前季的字符串
        for r in ranges:
            final_parts.append(f"S{season:02d}{r}")
            
    return ", ".join(final_parts)


def _extract_episode_refs_from_text(text: str) -> list:
    """从待复核原因等文本里提取季集引用，支持 S1E1 / S01E01-E03。"""
    if not isinstance(text, str) or not text.strip():
        return []

    matches = []
    for season_str, start_str, end_str in _EPISODE_REF_PATTERN.findall(text):
        try:
            season = int(season_str)
            start_ep = int(start_str)
            end_ep = int(end_str) if end_str else start_ep
        except Exception:
            continue

        if season <= 0 or start_ep <= 0 or end_ep <= 0:
            continue
        if end_ep < start_ep:
            start_ep, end_ep = end_ep, start_ep

        # 防御性限制，避免异常文本把通知撑爆。
        if end_ep - start_ep > 500:
            end_ep = start_ep

        for episode in range(start_ep, end_ep + 1):
            matches.append((season, episode))

    # 保持原始顺序去重，方便后续格式化为连续区间。
    seen = set()
    result = []
    for item in matches:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _build_episode_notice_text(episode_list: list, label: str = "🎞️ *集数*") -> str:
    """格式化通知中的季集文本，并在过长时压缩为摘要，避免 Telegram caption 过长。"""
    normalized = []
    seen = set()
    for season, episode in episode_list or []:
        try:
            season = int(season)
            episode = int(episode)
        except Exception:
            continue
        if season <= 0 or episode <= 0:
            continue
        key = (season, episode)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)

    if not normalized:
        return ""

    formatted = _format_episode_ranges(normalized)
    if not formatted:
        return ""

    if len(formatted) > 72:
        season_count = len({season for season, _ in normalized})
        episode_count = len(normalized)
        parts = formatted.split(", ")
        head = ", ".join(parts[:3])
        suffix = f"等{season_count}季{episode_count}集"
        formatted = f"{head} ... {suffix}" if head else suffix

    return f"{label}: `{_markdown_code_text(formatted)}`\n"

def escape_markdown(text: str) -> str:
    """
    Helper function to escape characters for Telegram's MarkdownV2.
    只应该用于转义从外部API获取的、内容不可控的文本部分。
    """
    if not isinstance(text, str):
        return ""
    # 根据 Telegram Bot API 文档，这些字符需要转义: _ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)


def _markdown_code_text(text) -> str:
    """MarkdownV2 code span 内只需要处理反斜杠和反引号。"""
    return str(text or '').replace('\\', '\\\\').replace('`', '\\`')


def _format_size_for_notice(size_bytes) -> str:
    try:
        size = float(size_bytes or 0)
    except Exception:
        return ''
    if size <= 0:
        return ''
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    if units[idx] in {'GB', 'TB'}:
        return f"{size:.1f}{units[idx]}" if size < 100 else f"{size:.0f}{units[idx]}"
    if units[idx] == 'MB':
        return f"{size:.0f}MB"
    return f"{int(size)}{units[idx]}"


def _notice_asset_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _load_episode_refs_by_emby_ids(emby_item_ids: list) -> list:
    """从数据库回退查询分集季号/集号，避免通知强依赖 Emby 实时详情。"""
    refs = []
    seen = set()
    normalized_ids = []
    for value in emby_item_ids or []:
        value = str(value or '').strip()
        if value and value not in seen:
            seen.add(value)
            normalized_ids.append(value)

    if not normalized_ids:
        return refs

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for emby_item_id in normalized_ids:
                    cursor.execute(
                        """
                        SELECT season_number, episode_number
                        FROM media_metadata
                        WHERE item_type = 'Episode'
                          AND emby_item_ids_json @> %s::jsonb
                        ORDER BY in_library DESC, date_added DESC NULLS LAST
                        LIMIT 1
                        """,
                        (json.dumps([emby_item_id], ensure_ascii=False),),
                    )
                    row = cursor.fetchone()
                    if not row:
                        continue
                    season = row.get('season_number')
                    episode = row.get('episode_number')
                    try:
                        season = int(season)
                        episode = int(episode)
                    except Exception:
                        continue
                    if season > 0 and episode > 0:
                        refs.append((season, episode))
    except Exception as e:
        logger.debug(f"  ➜ [通知] 数据库回退查询剧集季集失败: {e}")

    return refs


def _load_series_inventory_episode_refs(parent_series_tmdb_id: str, limit: int = 2000) -> list:
    """读取整部剧当前在库的分集，用于普通入库通知回退展示季集范围。"""
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    if not parent_series_tmdb_id:
        return []

    refs = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT season_number, episode_number
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = %s
                      AND item_type = 'Episode'
                      AND in_library = TRUE
                      AND season_number IS NOT NULL
                      AND episode_number IS NOT NULL
                    ORDER BY season_number ASC, episode_number ASC
                    LIMIT %s
                    """,
                    (parent_series_tmdb_id, limit),
                )
                for row in cursor.fetchall():
                    season = row.get('season_number')
                    episode = row.get('episode_number')
                    try:
                        season = int(season)
                        episode = int(episode)
                    except Exception:
                        continue
                    if season > 0 and episode > 0:
                        refs.append((season, episode))
    except Exception as e:
        logger.debug(f"  ➜ [通知] 读取整剧季集库存失败: series={parent_series_tmdb_id}, err={e}")

    return refs


def _extract_episode_refs_from_values(*values) -> list:
    for value in values:
        refs = _extract_episode_refs_from_text(value)
        if refs:
            return refs
    return []


def _load_notice_asset_details_by_emby_id(emby_item_id: str) -> list:
    """从 media_metadata.asset_details_json 读取通知参数，不再重新查 Emby MediaSources。"""
    emby_item_id = str(emby_item_id or '').strip()
    if not emby_item_id:
        return []

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset_details_json, washing_level
                    FROM media_metadata
                    WHERE asset_details_json IS NOT NULL
                      AND (
                          emby_item_ids_json @> %s::jsonb
                          OR asset_details_json @> %s::jsonb
                      )
                    ORDER BY
                        CASE item_type
                            WHEN 'Episode' THEN 0
                            WHEN 'Movie' THEN 1
                            WHEN 'Season' THEN 2
                            WHEN 'Series' THEN 3
                            ELSE 9
                        END,
                        in_library DESC,
                        date_added DESC NULLS LAST
                    LIMIT 1
                    """,
                    (
                        json.dumps([emby_item_id], ensure_ascii=False),
                        json.dumps([{'emby_item_id': emby_item_id}], ensure_ascii=False),
                    ),
                )
                row = cursor.fetchone()
    except Exception as e:
        logger.warning(f"  ➜ [通知] 查询 asset_details_json 失败: emby_item_id={emby_item_id}, err={e}")
        return []

    if not row:
        return []

    row_data = dict(row)
    assets = _notice_asset_list(row_data.get('asset_details_json'))
    if not assets:
        return []

    # 一条剧集/季记录里可能有多个 asset，优先只取当前 Emby Item 对应的那一个。
    matched = [item for item in assets if str(item.get('emby_item_id') or '').strip() == emby_item_id]
    selected = matched or assets
    for asset in selected:
        asset['_notice_washing_level'] = row_data.get('washing_level')
    return selected


def _notice_asset_value(asset: dict, *keys) -> str:
    for key in keys:
        value = (asset or {}).get(key)
        if value not in (None, ''):
            return str(value).strip()
    return ''


def _notice_asset_size(asset: dict) -> int:
    value = (asset or {}).get('size_bytes')
    if value in (None, ''):
        value = (asset or {}).get('size') or (asset or {}).get('file_size')
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _notice_asset_resolution(asset: dict) -> str:
    resolution = _notice_asset_value(asset, 'resolution_display')
    width = (asset or {}).get('width')
    height = (asset or {}).get('height')
    try:
        width = int(width or 0)
        height = int(height or 0)
    except Exception:
        width, height = 0, 0
    dimension = f"{width}x{height}" if width and height else ''
    if resolution and dimension and dimension not in resolution:
        return f"{resolution} / {dimension}"
    return resolution or dimension


def _notice_join_unique(values, limit: int = 4) -> str:
    out = []
    for value in values or []:
        value = str(value or '').strip()
        if value and value not in out:
            out.append(value)
    if not out:
        return ''
    text = ' / '.join(out[:limit])
    if len(out) > limit:
        text += f" 等{len(out)}种"
    return text


def _notice_asset_washing_level(asset: dict):
    try:
        return int((asset or {}).get('_notice_washing_level'))
    except Exception:
        return None


def _build_notice_washing_text(assets: list) -> str:
    levels = sorted({
        level
        for level in (_notice_asset_washing_level(asset) for asset in assets)
        if level is not None
    })
    if not levels:
        return ''

    def comment_for_level(level: int) -> str:
        if level == 1:
            return "太棒了！是你想要的版本！！"
        if level == 2:
            return "差点意思，坐等洗版"
        if level == 3:
            return "凑合看吧，也不是不行"
        return "太烂了，你确定入库吗？"

    level_text = ' / '.join(f"P{level} {comment_for_level(level)}" for level in levels)
    label = f"🏆 *版本*: `{_markdown_code_text(level_text)}`"
    return label


def _build_notice_asset_params_text(emby_item_ids: list) -> str:
    """生成入库/追更通知参数，数据源固定为 media_metadata.asset_details_json。"""
    assets = []
    seen = set()
    for emby_item_id in emby_item_ids or []:
        for asset in _load_notice_asset_details_by_emby_id(emby_item_id):
            key = str(asset.get('emby_item_id') or '') or str(asset.get('path') or '')
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            assets.append(asset)

    if not assets:
        return ''

    quality = _notice_join_unique(_notice_asset_value(a, 'quality_display') for a in assets)
    resolution = _notice_join_unique(_notice_asset_resolution(a) for a in assets)
    codec = _notice_join_unique(_notice_asset_value(a, 'codec_display', 'video_codec') for a in assets)
    effect = _notice_join_unique(_notice_asset_value(a, 'effect_display') for a in assets)
    audio = _notice_join_unique(_notice_asset_value(a, 'audio_display') for a in assets)
    subtitle = _notice_join_unique(_notice_asset_value(a, 'subtitle_display') for a in assets)

    total_size = sum(_notice_asset_size(a) for a in assets)
    file_count = len(assets)

    lines = []
    washing_text = _build_notice_washing_text(assets)
    if washing_text:
        lines.append(washing_text)

    quality_parts = [part for part in (quality, resolution, codec) if part]
    if quality_parts:
        lines.append(f"🎞️ *画质*: `{_markdown_code_text(' / '.join(quality_parts))}`")
    if effect:
        lines.append(f"🌈 *HDR/杜比*: `{_markdown_code_text(effect)}`")

    size_text = _format_size_for_notice(total_size)
    if size_text:
        if file_count > 1:
            size_text = f"{size_text}（{file_count}个文件）"
        lines.append(f"💾 *体积*: `{_markdown_code_text(size_text)}`")

    if audio:
        lines.append(f"🎧 *音轨*: `{_markdown_code_text(audio)}`")
    if subtitle:
        lines.append(f"💬 *字幕*: `{_markdown_code_text(subtitle)}`")

    return ('\n'.join(lines) + '\n') if lines else ''

# --- 通用的 Telegram 文本消息发送函数 ---
def send_telegram_message(chat_id: str, text: str, disable_notification: bool = False, reply_markup: dict = None):
    """通用的 Telegram 文本消息发送函数，支持内联键盘。"""
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id:
        return False
    
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': final_chat_id,
        'text': text, 
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True,
        'disable_notification': disable_notification,
    }
    
    # 支持传入键盘标记
    if reply_markup:
        payload['reply_markup'] = reply_markup

    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=15, proxies=proxies)
        if response.status_code == 200:
            logger.info("  ➜ Telegram 文本消息发送成功。")
            logger.debug(f"  ➜ Telegram 接收 Chat ID：{final_chat_id}")
            return True
        else:
            logger.error(f"  ➜ 发送 Telegram 文本消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"  ➜ 发送 Telegram 文本消息时发生网络请求错误: {e}")
        return False

# --- 通用的 Telegram 图文消息发送函数 ---
def send_telegram_photo(chat_id: str, photo_url: str, caption: str, disable_notification: bool = False):
    """通用的 Telegram 图文消息发送函数。"""
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id or not photo_url:
        return False
    
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    payload = {
        'chat_id': final_chat_id,
        'photo': photo_url,
        'caption': caption, 
        'parse_mode': 'MarkdownV2',
        'disable_notification': disable_notification,
    }
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=30, proxies=proxies)
        if response.status_code == 200:
            logger.debug(f"  ➜ 成功发送 Telegram 图文消息至 Chat ID: {final_chat_id}")
            return True
        else:
            logger.error(f"  ➜ 发送 Telegram 图文消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"  ➜ 发送 Telegram 图文消息时发生网络请求错误: {e}")
        return False
    
# --- 全能的通知函数 ---
def send_media_notification(item_details: dict, notification_type: str = 'new', new_episode_ids: list = None):
    """
    【全能媒体通知函数】
    根据传入的媒体详情，自动获取图片、组装消息并发送给频道和订阅者。
    """
    notification_name = {'new': '新入库', 'update': '追更入库'}.get(notification_type, notification_type or '媒体')
    logger.info(f"  ➜ 准备发送 Telegram {notification_name}通知：《{item_details.get('Name') or '未知媒体'}》。")
    
    try:
        # --- 1. 准备基础信息 ---
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_id = item_details.get("Id")
        item_name_for_log = item_details.get("Name", f"ID:{item_id}")
        year = item_details.get("ProductionYear", "")
        title = f"{item_name_for_log} ({year})" if year else item_name_for_log
        overview = item_details.get("Overview", "暂无剧情简介。")
        if len(overview) > 200:
            overview = overview[:200] + "..."
            
        item_type = item_details.get("Type")

        escaped_title = escape_markdown(title)
        escaped_overview = escape_markdown(overview)

        # --- 2. 准备剧集信息 + 媒体参数 ---
        # 媒体参数不再临时查 Emby MediaSources，直接读取 process_single_item 已写入的
        # media_metadata.asset_details_json，避免重复请求和字段口径不一致。
        episode_info_text = ""
        raw_episodes = []
        notice_emby_item_ids = []
        if item_type == "Series" and new_episode_ids:
            emby_url = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
            api_key = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
            user_id = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID)

            # 收集原始数据而不是直接格式化字符串，这样我们可以在格式化字符串时使用
            for ep_id in new_episode_ids:
                detail = get_emby_item_details(ep_id, emby_url, api_key, user_id, fields="IndexNumber,ParentIndexNumber")
                if detail:
                    season_num = detail.get("ParentIndexNumber", 0)
                    episode_num = detail.get("IndexNumber", 0)
                    # 收集元组 (季号, 集号)
                    raw_episodes.append((season_num, episode_num))
                notice_emby_item_ids.append(str(ep_id))
        elif item_id:
            notice_emby_item_ids.append(str(item_id))

        media_param_text = _build_notice_asset_params_text(notice_emby_item_ids)

        # --- 3. 调用本地数据库获取图片路径 ---
        photo_url = None
        try:
            db_info = media_db.get_notification_media_info_by_emby_id(item_id)
            if db_info:
                # 优先横幅，其次竖图，如果是分集没图，找它爹(剧集)要横幅
                path = db_info.get('backdrop_path') or db_info.get('poster_path')
                if not path and db_info.get('item_type') == 'Episode':
                    path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                if path:
                    photo_url = f"https://image.tmdb.org/t/p/w780{path}"
        except Exception as e:
            logger.error(f"  ➜ [通知] 从本地数据库获取图片信息时出错: {e}", exc_info=True)

        # =================================================================
        # ★★★ 查询该项目是否被标记为【待复核】 ★★★
        # =================================================================
        needs_review = False
        review_reason = ""
        try:
            # 核心处理器中，分集的报错是挂在父剧集 ID 下的，所以这里要做个转换
            check_id = str(item_id)
            if item_type == 'Episode' and item_details.get('SeriesId'):
                check_id = str(item_details.get('SeriesId'))
                
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT reason FROM failed_log WHERE item_id = %s", (check_id,))
                    row = cursor.fetchone()
                    if row:
                        needs_review = True
                        review_reason = row['reason']
        except Exception as e:
            logger.error(f"  ➜ [通知] 查询待复核状态失败: {e}")
        
        # --- 4. 组装最终的通知文本 (Caption) ---
        notification_title_map = {
            'new': '✨ 入库成功',
            'update': '🔄 已更新'
        }
        notification_title = notification_title_map.get(notification_type, '🔔 状态更新')
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        media_icon = "🎬" if item_type == "Movie" else "📺"
        
        # ★★★ 构建待复核警告文本 ★★★
        review_warning = ""
        if needs_review:
            escaped_reason = escape_markdown(review_reason)
            review_warning = (
                f"\n\n⚠️ *系统提示*: 本次处理被标记为【待复核】\n"
                f"🔍 *原因*: {escaped_reason}\n"
                f"💡 _请前往 WebUI 手动介入处理_"
            )

        if item_type == "Series" and not raw_episodes and new_episode_ids:
            raw_episodes = _load_episode_refs_by_emby_ids(new_episode_ids)
        if item_type == "Series" and not raw_episodes and review_reason:
            raw_episodes = _extract_episode_refs_from_text(review_reason)
        if item_type == "Series" and not raw_episodes and tmdb_id:
            raw_episodes = _load_series_inventory_episode_refs(tmdb_id)
        if raw_episodes:
            episode_info_text = _build_episode_notice_text(raw_episodes, label="🎞️ *集数*")

        # ★★★ 修改：将 review_warning 追加到 caption 尾部 ★★★
        caption = (
            f"{media_icon} *{escaped_title}* {notification_title}\n\n"
            f"{episode_info_text}"
            f"{media_param_text}"
            f"⏰ *时间*: `{current_time}`\n"
            f"📝 *剧情*: {escaped_overview}"
            f"{review_warning}"
        )
        
        # --- 5. 查询订阅者 ---
        subscribers = request_db.get_subscribers_by_tmdb_id(tmdb_id, item_type) if tmdb_id else []
        subscriber_chat_ids = {
            user_db.get_user_telegram_chat_id(sub.get('user_id')) 
            for sub in subscribers 
            if sub.get('type') == 'user_request' and sub.get('user_id')
        }
        subscriber_chat_ids = {chat_id for chat_id in subscriber_chat_ids if chat_id}

        # --- 6 & 7. 发送全局和管理员通知 ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        
        # ★ 严格判定：只有勾选了“入库通知”，才允许向频道和管理员发送非订阅类的公播消息
        if 'library_new' in notify_types:
            # A. 发送给频道
            if global_channel_id:
                logger.info(f"  ➜ 正在向全局频道 {global_channel_id} 发送通知...")
                if photo_url:
                    send_telegram_photo(global_channel_id, photo_url, caption)
                else:
                    send_telegram_message(global_channel_id, caption)

            # B. 发送给管理员
            all_admin_chat_ids = set(user_db.get_admin_telegram_chat_ids())
            if all_admin_chat_ids:
                subscriber_id_set = {str(sid) for sid in subscriber_chat_ids}
                for admin_chat_id in all_admin_chat_ids:
                    # 去重：不发给频道，也不发给已经是订阅者的管理员
                    if str(admin_chat_id) == str(global_channel_id) or str(admin_chat_id) in subscriber_id_set:
                        continue
                    
                    logger.info(f"  ➜ 正在向管理员发送全局入库通知。")
                    if photo_url:
                        send_telegram_photo(admin_chat_id, photo_url, caption)
                    else:
                        send_telegram_message(admin_chat_id, caption)
        else:
            logger.debug(f"  ➜ [通知] '入库通知' 设置为关闭，跳过频道和管理员的全局广播。")

        # --- 8. 发送个人订阅到货通知 ---
        if subscriber_chat_ids:
            personal_caption_map = {
                'new': f"✅ *您的订阅已入库*\n\n{caption}",
                'update': f"🔄 *您的订阅已更新*\n\n{caption}"
            }
            personal_caption = personal_caption_map.get(notification_type, caption)
            
            for chat_id in subscriber_chat_ids:
                if chat_id == global_channel_id: continue
                logger.info(f"  ➜ 正在向订阅者 {chat_id} 发送个人通知...")
                if photo_url:
                    send_telegram_photo(chat_id, photo_url, personal_caption)
                else:
                    send_telegram_message(chat_id, personal_caption)
            
    except Exception as e:
        logger.error(f"  ➜ 发送媒体通知时发生严重错误: {e}", exc_info=True)

def send_transfer_success_notification(task: dict):
    """发送频道监听转存成功的通知"""
    try:
        title = task.get('title', '未知标题')
        year = task.get('year', '')
        item_type = task.get('item_type', 'movie')
        season_number = task.get('season_number')
        episode_number = task.get('episode_number')
        is_pack = task.get('is_pack', False)
        tmdb_id = task.get('tmdb_id')

        display_title = f"{title} ({year})" if year else title
        escaped_title = escape_markdown(display_title)
        
        type_str = "🎬 电影" if item_type == 'movie' else "📺 剧集"
        
        season_info = ""
        if item_type == 'tv':
            candidate = task.get('candidate') if isinstance(task.get('candidate'), dict) else {}
            parsed_refs = _extract_episode_refs_from_values(
                candidate.get('raw_text'),
                candidate.get('title'),
                candidate.get('identify_title'),
                candidate.get('clean_title'),
                title,
            )
            if parsed_refs:
                season_info = _build_episode_notice_text(parsed_refs, label="🎞️ *集数*")
            elif season_number is not None:
                if episode_number is not None:
                    if is_pack:
                        season_info = f"🎞️ *季集*: `S{int(season_number):02d} 共{int(episode_number)}集`\n"
                    else:
                        season_info = f"🎞️ *季集*: `S{int(season_number):02d}E{int(episode_number):02d}`\n"
                else:
                    season_info = f"🎞️ *季集*: `S{int(season_number):02d}`\n"

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 尝试获取 TMDB 图片和评分
        photo_url = None
        rating = ""
        overview_text = "" 
        
        if tmdb_id:
            # ★ 转存时的 ID 绝对是纯种 TMDB ID
            base_tmdb_id = str(tmdb_id).strip()
            
            if base_tmdb_id.isdigit():
                try:
                    from database import media_db
                    # 极速本地盲查，不需要管是电影还是剧集
                    db_info = media_db.get_notification_media_info_by_tmdb_id(base_tmdb_id)
                    
                    if db_info:
                        # 优先横幅，如果没有再找竖图
                        path = db_info.get('backdrop_path') or db_info.get('poster_path')
                        # 如果拿到的是单集或季，且没图，向父剧集借图
                        if not path and db_info.get('item_type') in ['Episode', 'Season']:
                            path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                            
                        if path:
                            photo_url = f"https://image.tmdb.org/t/p/w780{path}"
                            
                        vote_average = db_info.get('rating')
                        if vote_average:
                            rating = f"✨ *评分*: `{vote_average:.1f}/10`\n"
                            
                        raw_overview = db_info.get('overview', '')
                        if raw_overview:
                            if len(raw_overview) > 200:
                                raw_overview = raw_overview[:200] + "..."
                            overview_text = f"📝 *剧情*: {escape_markdown(raw_overview)}\n"
                except Exception as e:
                    logger.error(f"  ➜ 获取转存通知图片(本地查库)失败: {e}")

        # 组装卡片文本
        # ★ 区分是 115 转存还是离线下载
        action_title = "📥 *离线任务已提交*" if task.get('is_offline') else "📥 *转存成功*"
        
        caption = (
            f"{action_title}\n"
            f"*{escaped_title}*\n\n"
            f"{season_info}"
            f"🕒 *时间*: `{current_time}`\n"
            f"🎭 *类别*: {type_str}\n"
            f"{rating}"
            f"{overview_text}" 
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            if photo_url:
                send_telegram_photo(target, photo_url, caption)
            else:
                send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送转存成功通知时出错: {e}", exc_info=True)

def send_playback_notification(data: dict):
    """发送图文并茂的播放状态通知 (附带剧集或电影海报，注入灵魂版)"""
    try:
        event_type = data.get("Event")
        user_name = data.get("User", {}).get("Name", "未知用户")
        device_name = data.get("Session", {}).get("DeviceName", "未知设备")
        client_name = data.get("Session", {}).get("Client", "未知客户端")
        
        item = data.get("Item", {})
        original_item_name = item.get("Name", "未知项目")
        original_item_type = item.get("Type", "Unknown")
        item_id = item.get("Id")
        
        # 优先从 Emby Webhook 数据中提取剧情
        raw_overview = item.get("Overview", "")
        
        display_item_name = original_item_name
        if original_item_type == "Episode" and item.get("SeriesName"):
            display_item_name = f"{item.get('SeriesName')} - {original_item_name}"
            
        # --- 本地数据库提取图片和剧情兜底 (极速，无网络请求依赖) ---
        photo_url = None
        if item_id:
            db_info = media_db.get_notification_media_info_by_emby_id(item_id)
            if db_info:
                # 优先横幅，如果没有再用竖图。如果是分集没图，自动用父剧集的横幅图
                path = db_info.get('backdrop_path') or db_info.get('poster_path')
                if not path and db_info.get('item_type') == 'Episode':
                    path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                if path:
                    photo_url = f"https://image.tmdb.org/t/p/w780{path}"
                
                # ★ 新增：如果 Emby 没传剧情，从本地数据库兜底获取
                if not raw_overview:
                    raw_overview = db_info.get('overview', '')
        
        # 格式化剧情文本 (限制长度防刷屏)
        overview_text = ""
        if raw_overview:
            if len(raw_overview) > 150:
                raw_overview = raw_overview[:150] + "..."
            overview_text = f"\n📝 *剧情*: {escape_markdown(raw_overview)}"
                    
        action_map = {
            "playback.start": "▶️ 开始播放",
            "playback.pause": "⏸ 暂停播放",
            "playback.stop": "⏹ 停止播放"
        }
        action_str = action_map.get(event_type, "🎬 播放状态改变")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ★ 修改：将剧情变量追加到卡片末尾
        caption = (
            f"{action_str}\n\n"
            f"👤 *用户*: `{escape_markdown(user_name)}`\n"
            f"🎬 *媒体*: *{escape_markdown(display_item_name)}*\n"
            f"📱 *设备*: `{escape_markdown(device_name)} ({escape_markdown(client_name)})`\n"
            f"🕒 *时间*: `{escape_markdown(current_time)}`"
            f"{overview_text}" 
        )
        
        # --- 收集发送目标 (频道 + 所有管理员) ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            if aid:
                targets.add(str(aid))

        if not targets:
            logger.debug("  ➜ [播放通知] 未配置接收人 (频道或管理员均为空)，跳过发送。")
            return

        # --- 遍历发送 (移除所有静音参数，让通知发出清脆的叮咚声！) ---
        for target in targets:
            if photo_url:
                send_telegram_photo(target, photo_url, caption)
            else:
                send_telegram_message(target, caption)
                
    except Exception as e:
        logger.error(f"  ➜ 组装/发送播放图文通知时发生异常: {e}")

def send_unrecognized_notification(file_name: str, reason: str = "未匹配到有效的 TMDb 数据"):
    """
    发送文件识别失败/打入未识别目录的 Telegram 通知
    """
    try:
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        # 检查用户是否在设置中勾选了“识别失败”通知
        if 'recognize_fail' not in notify_types:
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        escaped_file_name = escape_markdown(file_name)
        escaped_reason = escape_markdown(reason)

        caption = (
            f"⚠️ *识别失败通知*\n\n"
            f"📁 *文件名*: `{escaped_file_name}`\n"
            f"❓ *原因*: {escaped_reason}\n"
            f"🕒 *时间*: `{current_time}`\n\n"
            f"💡 _文件已被移入「未识别」目录，请前往 WebUI 手动纠错。_"
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送识别失败通知时出错: {e}", exc_info=True)

def send_intercept_notification(file_names, reason: str):
    """
    发送洗版拦截/质检不合格的 Telegram 通知 (支持多文件聚合)
    """
    try:
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        # 检查用户是否在设置中勾选了“拦截通知”
        if 'intercept_notify' not in notify_types:
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        escaped_reason = escape_markdown(reason)

        # 兼容单文件字符串传入
        if isinstance(file_names, str):
            file_names = [file_names]
            
        count = len(file_names)
        if count == 1:
            name_str = f"`{escape_markdown(file_names[0])}`"
        else:
            # 最多显示 5 个文件名，防止消息过长刷屏
            display_names = file_names[:5]
            name_str = "\n".join([f"• `{escape_markdown(n)}`" for n in display_names])
            if count > 5:
                name_str += f"\n_{escape_markdown(f'...等共 {count} 个文件')}_"
            name_str = f"共 {count} 个文件:\n{name_str}"

        caption = (
            f"⛔ *洗版拦截通知*\n\n"
            f"📁 *拦截文件*: {name_str}\n"
            f"🚫 *原因*: {escaped_reason}\n"
            f"🕒 *时间*: `{current_time}`\n\n"
            f"💡 _文件未达到优先级标准，已被标记「质检不合格」。_"
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送洗版拦截通知时出错: {e}", exc_info=True)

# ======================================================================
# ★★★ Telegram 机器人交互监听 (长轮询) ★★★
# ======================================================================
import re
import time
import threading
from handler.p115_service import P115Service

# 全局变量控制轮询线程
_tg_polling_thread = None
_tg_polling_active = False

# Telegram 资源搜索会话：chat_id -> {stage, results/resources, media, created_at}
_tg_resource_search_sessions = {}
_tg_resource_search_lock = threading.Lock()
_TG_RESOURCE_SEARCH_TTL = 15 * 60
# TMDb 候选仍保持 10 个；资源结果单页 10 个，但最多收集 50 个用于翻页。
_TG_TMDB_SEARCH_LIMIT = 10
_TG_RESOURCE_PAGE_SIZE = 10
_TG_RESOURCE_COLLECT_LIMIT = 50
_TG_RESOURCE_SEARCH_LIMIT = _TG_TMDB_SEARCH_LIMIT  # 兼容旧变量名


def _tg_send_plain(chat_id: str, text: str, disable_notification: bool = False, reply_markup: dict = None):
    """发送普通文本；统一转义 MarkdownV2，避免外部片名/资源名导致 TG 发送失败。"""
    return send_telegram_message(
        chat_id,
        escape_markdown(str(text or "")),
        disable_notification=disable_notification,
        reply_markup=reply_markup,
    )


def _tg_get_tmdb_api_key() -> str:
    """兼容不同版本 constants 命名，读取 TMDb API Key。"""
    constant_names = [
        "CONFIG_OPTION_TMDB_API_KEY",
        "CONFIG_OPTION_TMDB_APIKEY",
        "CONFIG_OPTION_TMDB_KEY",
    ]
    for name in constant_names:
        config_key = getattr(constants, name, None)
        if config_key:
            value = APP_CONFIG.get(config_key)
            if value:
                return str(value).strip()

    fallback_keys = [
        "tmdb_api_key",
        "tmdb_apikey",
        "TMDB_API_KEY",
        "tmdb_key",
    ]
    for key in fallback_keys:
        value = APP_CONFIG.get(key)
        if value:
            return str(value).strip()

    return ""


def _tg_normalize_digits(text: str) -> str:
    return str(text or "").translate(str.maketrans("０１２３４５６７８９", "0123456789"))


def _tg_parse_selection_text(text: str):
    """解析“1”“第1个”“2 s3”“2 第3季”这类回复，返回 (序号, 季号)。
    注意：TG 手动影巢搜索不再按季过滤，季号仅兼容旧输入。
    """
    normalized = _tg_normalize_digits(text).strip()
    match = re.match(
        r"^(?:第\s*)?(\d{1,2})(?:\s*(?:个|项|号))?(?:\s*(?:s|S|第)?\s*(\d{1,2})\s*(?:季)?)?$",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return None, None

    number = int(match.group(1))
    season = int(match.group(2)) if match.group(2) else None
    return number, season


def _tg_is_session_expired(session: dict) -> bool:
    if not session:
        return True
    return (time.time() - float(session.get("created_at") or 0)) > _TG_RESOURCE_SEARCH_TTL


def _tg_get_session(chat_id: str):
    with _tg_resource_search_lock:
        session = _tg_resource_search_sessions.get(str(chat_id))
        if _tg_is_session_expired(session):
            _tg_resource_search_sessions.pop(str(chat_id), None)
            return None
        return session


def _tg_set_session(chat_id: str, session: dict):
    session["created_at"] = time.time()
    with _tg_resource_search_lock:
        _tg_resource_search_sessions[str(chat_id)] = session


def _tg_clear_session(chat_id: str):
    with _tg_resource_search_lock:
        _tg_resource_search_sessions.pop(str(chat_id), None)


def _tg_build_number_keyboard(prefix: str, count: int) -> dict:
    keyboard = []
    row = []
    for idx in range(1, min(count, _TG_TMDB_SEARCH_LIMIT) + 1):
        row.append({"text": f"{idx:02d}", "callback_data": f"{prefix}:{idx}"})
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "取消", "callback_data": "tg_search_cancel"}])
    return {"inline_keyboard": keyboard}


def _tg_clamp_page(page: int, total_count: int) -> int:
    try:
        page = int(page)
    except Exception:
        page = 0
    page_count = max(1, (max(0, int(total_count or 0)) + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    return max(0, min(page, page_count - 1))


def _tg_slice_resource_page(resources: list, page: int) -> list:
    page = _tg_clamp_page(page, len(resources or []))
    start = page * _TG_RESOURCE_PAGE_SIZE
    end = start + _TG_RESOURCE_PAGE_SIZE
    return list(resources or [])[start:end]


def _tg_build_resource_page_keyboard(total_count: int, page: int) -> dict:
    total_count = int(total_count or 0)
    page = _tg_clamp_page(page, total_count)
    page_count = max(1, (total_count + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    start = page * _TG_RESOURCE_PAGE_SIZE
    end = min(start + _TG_RESOURCE_PAGE_SIZE, total_count)

    keyboard = []
    row = []
    for idx in range(start + 1, end + 1):
        row.append({"text": f"{idx:02d}", "callback_data": f"tg_hdhive:{idx}"})
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav_row = []
    if page > 0:
        nav_row.append({"text": "⬅️ 上一页", "callback_data": f"tg_res_page:{page - 1}"})
    nav_row.append({"text": f"{page + 1}/{page_count}", "callback_data": "tg_res_page:noop"})
    if page < page_count - 1:
        nav_row.append({"text": "下一页 ➡️", "callback_data": f"tg_res_page:{page + 1}"})
    keyboard.append(nav_row)
    keyboard.append([{"text": "🔔 订阅该项目", "callback_data": "tg_subscribe"}])
    keyboard.append([{"text": "取消", "callback_data": "tg_search_cancel"}])
    return {"inline_keyboard": keyboard}


def _tg_media_type_label(media_type: str) -> str:
    return "电影" if media_type == "movie" else "剧集"


def _tg_tmdb_title(item: dict) -> str:
    return item.get("title") or item.get("name") or item.get("original_title") or item.get("original_name") or "未知标题"


def _tg_tmdb_year(item: dict) -> str:
    date_text = item.get("release_date") or item.get("first_air_date") or ""
    return str(date_text)[:4] if date_text else "未知年份"


def _tg_tmdb_result_line(index: int, item: dict) -> str:
    media_type = item.get("media_type") or "movie"
    title = _tg_tmdb_title(item)
    year = _tg_tmdb_year(item)
    tmdb_id = item.get("id") or "-"
    rating = item.get("vote_average")
    rating_text = f" / 评分 {float(rating):.1f}" if isinstance(rating, (int, float)) and rating else ""
    return f"{index}. [{_tg_media_type_label(media_type)}] {title} ({year}) / TMDb {tmdb_id}{rating_text}"


def _tg_format_tmdb_results(query: str, results: list) -> str:
    lines = [
        f"🔎 TMDb 搜索 | {query}",
        "━━━━━━━━━━━━━━",
        "↩️ 回复序号选择影片/剧集，或点击下方按钮。",
        "📺 剧集资源将全量返回，不按季过滤；需要哪一季请在资源备注里肉眼挑选。",
        "🚫 输入 取消 可结束本次搜索。",
        "",
    ]
    for idx, item in enumerate(results, 1):
        lines.append(_tg_tmdb_result_line(idx, item))
    return "\n".join(lines)


def _tg_truncate(text: str, limit: int = 90) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _tg_resource_title(resource: dict) -> str:
    # remark 往往是质量说明，适合作为独立备注展示，不优先拿来当标题。
    for key in ("title", "name", "resource_name", "share_name", "filename", "file_name", "slug", "remark", "summary"):
        value = resource.get(key)
        if value:
            return _tg_truncate(value, 80)
    return "未知资源"


def _tg_flatten_resource_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " / ".join(_tg_flatten_resource_value(v) for v in value if v)
    if isinstance(value, dict):
        return " / ".join(_tg_flatten_resource_value(v) for v in value.values() if v)
    return re.sub(r"\s+", " ", str(value)).strip()


def _tg_resource_remark(resource: dict, limit: int = 160) -> str:
    for key in ("remark", "description", "summary", "subtitle", "subtitles"):
        value = _tg_flatten_resource_value(resource.get(key))
        if value:
            return _tg_truncate(value, limit=limit)
    return ""


def _tg_resource_size_gb(resource: dict):
    raw = resource.get("share_size") or resource.get("size") or resource.get("file_size") or resource.get("total_size")
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float)):
            return float(raw) / 1024 / 1024 / 1024 if float(raw) > 10000 else float(raw)
        text = str(raw).strip().upper().replace(",", "")
        match = re.search(r"([\d.]+)\s*(TB|GB|MB|KB|B)?", text)
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2) or "GB"
        if unit == "TB":
            return value * 1024
        if unit == "GB":
            return value
        if unit == "MB":
            return value / 1024
        if unit == "KB":
            return value / 1024 / 1024
        if unit == "B":
            return value / 1024 / 1024 / 1024
    except Exception:
        return None
    return None


def _tg_resource_resolution(resource: dict) -> str:
    values = resource.get("video_resolution") or resource.get("resolution") or ""
    if isinstance(values, list):
        values = "/".join(str(v) for v in values if v)
    text = str(values or "未知").strip()
    return text.upper() if text else "未知"


def _tg_resource_pan_text(resource: dict) -> str:
    if resource.get("_tg_source") == "shared_pool" or resource.get("_cloud_source") == "shared_pool":
        return "🟢 共享池"
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        return "📡 频道"
    pan_type = str(resource.get("pan_type") or "115").upper()
    return f"🟡 {pan_type}"


def _tg_resource_points_text(resource: dict) -> str:
    if resource.get("_tg_source") == "shared_pool" or resource.get("_cloud_source") == "shared_pool":
        return "✅ 已持有" if bool(resource.get("already_owned")) else "⚡ 可秒传"
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        return "🆓 可转存"
    points = resource.get("unlock_points")
    already_owned = bool(resource.get("already_owned"))
    if already_owned:
        return "✅ 已拥有"
    if points in (None, 0, "0", "", "free", "FREE"):
        return "🆓 免费"
    return f"💎 {points}积分"

def _tg_resource_size_text(resource: dict) -> str:
    size_gb = _tg_resource_size_gb(resource)
    if size_gb is None:
        return "💾 未知大小"
    if size_gb >= 100:
        return f"💾 {size_gb:.0f}GB"
    return f"💾 {size_gb:.1f}GB"


def _tg_resource_quality_text(resource: dict, limit: int = 96) -> str:
    # 尽量提取一行“版本/质量/来源”摘要，和备注分开展示。
    # 不只按完全相同去重，还要跳过已经包含在 video_display 里的子项，
    # 例如 quality="4K · Dolby Vision P7 / HDR10 · HEVC" 时，不再追加 source="Dolby Vision P7 / HDR10"。
    preferred = []

    def append_unique(value):
        value = _tg_flatten_resource_value(value)
        if not value:
            return
        norm = re.sub(r'[\s/\-_.·|,，]+', '', value).lower()
        if not norm:
            return
        for old in preferred:
            old_norm = re.sub(r'[\s/\-_.·|,，]+', '', str(old or '')).lower()
            if norm == old_norm or norm in old_norm or old_norm in norm:
                return
        preferred.append(value)

    for key in ("quality", "source", "video_codec", "audio", "format", "category", "edition"):
        append_unique(resource.get(key))

    if preferred:
        return _tg_truncate(" / ".join(preferred), limit=limit)

    # 字段不全时，用名称字段兜底，但避免把 slug 当质量说明。
    for key in ("title", "name", "resource_name", "share_name", "filename", "file_name"):
        value = _tg_flatten_resource_value(resource.get(key))
        if value:
            return _tg_truncate(value, limit=limit)
    return ""




def _tg_sp_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _tg_sp_json(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _tg_sp_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'y', 'on', '是', '启用', '开启'}:
        return True
    if text in {'0', 'false', 'no', 'n', 'off', '否', '停用', '关闭'}:
        return False
    return None


def _tg_sp_size_text(value) -> str:
    try:
        size = float(value or 0)
    except Exception:
        size = 0
    if size <= 0:
        return ''
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    if units[idx] in {'GB', 'TB'}:
        return f'{size:.2f} {units[idx]}' if size < 100 else f'{size:.0f} {units[idx]}'
    if units[idx] == 'MB':
        return f'{size:.0f} MB'
    return f'{int(size)} {units[idx]}'


def _tg_sp_first(*values) -> str:
    for value in values:
        if value not in (None, '', [], {}):
            return str(value).strip()
    return ''


def _tg_sp_quality_summary(summary: dict) -> str:
    """共享池展示用媒体参数摘要。

    中心端的 video_display 通常已经包含：分辨率 / HDR(杜比) / 编码 / 位深 / 帧率。
    TG 展示时不能再把 effect 当作 source 拼一次，否则会出现
    "Dolby Vision / HDR10 ... / Dolby Vision / HDR10" 这种重复。
    """
    summary = summary if isinstance(summary, dict) else {}
    video_display = _tg_sp_first(summary.get('video_display'))
    if video_display:
        return video_display

    parts = []

    def add(value):
        value = str(value or '').strip()
        if not value:
            return
        norm = re.sub(r'[\s/\-_.·|,，]+', '', value).lower()
        if not norm:
            return
        for old in parts:
            old_norm = re.sub(r'[\s/\-_.·|,，]+', '', str(old or '')).lower()
            if norm == old_norm or norm in old_norm or old_norm in norm:
                return
        parts.append(value)

    add(_tg_sp_first(summary.get('resolution'), summary.get('resolution_display')))
    add(_tg_sp_first(summary.get('effect'), summary.get('effect_key')))
    add(_tg_sp_first(summary.get('codec'), summary.get('video_codec'), summary.get('codec_display')))
    bit_depth = _tg_sp_first(summary.get('bit_depth'))
    if bit_depth:
        add(f"{bit_depth}bit" if str(bit_depth).isdigit() else bit_depth)
    add(_tg_sp_first(summary.get('fps'), summary.get('frame_rate')))
    return ' · '.join(parts)


def _tg_sp_tag_containers(item: dict) -> list[dict]:
    item = item if isinstance(item, dict) else {}
    out = [item]
    for key in (
        'version_summary', 'summary_json', 'media_signature_json', 'raw_summary_json', 'rapid_meta_json',
        'clean_version_meta_json', 'short_drama_meta_json', 'animation_meta_json', 'completed_certified_meta_json',
    ):
        obj = _tg_sp_json(item.get(key))
        if obj:
            out.append(obj)
    return out


def _tg_sp_flag(item: dict, flag_key: str, meta_key: str = '') -> bool:
    for part in _tg_sp_tag_containers(item):
        state = _tg_sp_bool(part.get(flag_key)) if flag_key in part else None
        if state is True:
            return True
        meta = _tg_sp_json(part.get(meta_key)) if meta_key else {}
        state = _tg_sp_bool(meta.get(flag_key)) if flag_key in meta else None
        if state is True:
            return True
    return False


def _tg_sp_tags(item: dict) -> list[str]:
    tags = []
    def add(label):
        label = str(label or '').strip()
        if label and label not in tags:
            tags.append(label)
    if _tg_sp_flag(item, 'is_clean_version', 'clean_version_meta_json'):
        add('纯净版')
    if _tg_sp_flag(item, 'is_short_drama', 'short_drama_meta_json'):
        add('短剧')
    if _tg_sp_flag(item, 'is_animation', 'animation_meta_json'):
        add('动漫')
    skip = {'已完结', '完结', '已认证完结', '完结认证', '连载中', '可用'}
    for part in _tg_sp_tag_containers(item):
        raw = part.get('tag_labels')
        if isinstance(raw, str):
            raw = [x.strip() for x in re.split(r'[,，/|]', raw) if x.strip()]
        if isinstance(raw, list):
            for label in raw:
                label = str(label or '').strip()
                if label and label not in skip:
                    add(label)
    return tags


def _tg_sp_versions(resource: dict) -> list[dict]:
    parent = dict(resource or {})
    versions = parent.get('versions')
    versions = [dict(x) for x in versions if isinstance(x, dict)] if isinstance(versions, list) else []
    if not versions:
        one = dict(parent)
        one.pop('versions', None)
        return [one]
    rows = []
    total = len(versions)
    parent_source_id = parent.get('source_id') or parent.get('source_ref_id') or parent.get('hub_id')
    inherit_keys = (
        'progress_current', 'progress_total', 'progress_text', 'season_number', 'tmdb_id', 'release_year',
        'has_children', 'children_loaded', 'lazy_children_kind', 'children_count', 'child_count', 'pack_item_count',
        'is_completed_certified', 'is_completed', 'is_ongoing_hub', 'is_clean_version', 'clean_version_meta_json',
        'is_short_drama', 'short_drama_meta_json', 'is_animation', 'animation_meta_json', 'tag_labels',
    )
    for idx, version in enumerate(versions, 1):
        row = dict(parent)
        row.update(version)
        row.pop('versions', None)
        if not version.get('children'):
            row.pop('children', None)
        if not version.get('pack_items'):
            row.pop('pack_items', None)
        row['_shared_pool_parent_source_id'] = parent_source_id
        row['_shared_pool_version_index'] = idx
        row['_shared_pool_version_count'] = total
        for key in inherit_keys:
            if row.get(key) in (None, '', [], {}) and parent.get(key) not in (None, '', [], {}):
                row[key] = parent.get(key)
        rows.append(row)
    return rows


def _tg_sp_year(*values) -> str:
    for value in values:
        m = re.search(r'(19|20)\d{2}', str(value or ''))
        if m:
            return m.group(0)
    return ''


def _tg_sp_title(item: dict) -> str:
    item = item if isinstance(item, dict) else {}
    raw_title = str(item.get('title') or item.get('name') or item.get('file_name') or '共享池资源').strip()
    year = _tg_sp_year(item.get('release_year'), item.get('year'), item.get('release_date'), item.get('first_air_date'))
    season = _tg_sp_int(item.get('season_number'), 0)
    display_kind = str(item.get('display_type') or item.get('item_type') or item.get('source_kind') or '').lower()
    source_kind = str(item.get('source_kind') or '').lower()
    is_pack = season > 0 and (display_kind in {'pack', 'season', 'series'} or source_kind in {'season_hub', 'completed_season'} or item.get('progress_text'))
    base = raw_title
    if is_pack:
        base = re.sub(r'\s*(?:第\s*\d+\s*季|S\d{1,3}|Season\s*\d{1,3})\s*$', '', base, flags=re.I).strip() or raw_title
    if year and not re.search(rf'[（(]\s*{re.escape(year)}\s*[）)]', base):
        base = f'{base}（{year}）'
    if is_pack and not re.search(r'第\s*\d+\s*季', base):
        base = f'{base}第 {season} 季'
    return base


def _tg_sp_season(item: dict) -> int:
    item = item if isinstance(item, dict) else {}
    season = _tg_sp_int(item.get('season_number'), 0)
    if season > 0:
        return season
    text = ' '.join(str(item.get(k) or '') for k in ('title', 'name', 'file_name', 'remark'))
    for pattern in (r'第\s*(\d{1,3})\s*季', r'\bS(\d{1,3})\b', r'Season\s*(\d{1,3})'):
        m = re.search(pattern, text, re.I)
        if m:
            return _tg_sp_int(m.group(1), 0)
    return 0


def _tg_sp_sort_key(index_and_item):
    index, item = index_and_item
    kind_text = str((item or {}).get('item_type') or (item or {}).get('display_type') or (item or {}).get('source_kind') or '').lower()
    media_rank = 0 if kind_text in {'season', 'pack', 'series', 'season_hub', 'completed_season'} else 1
    season = _tg_sp_season(item)
    version = _tg_sp_int((item or {}).get('_shared_pool_version_index'), 0)
    return (media_rank, season if season > 0 else 9999, version if version > 0 else 9999, index)


def _tg_sp_normalize(resource: dict, *, fallback_year: str = '') -> dict:
    item = dict(resource or {})
    if not item.get('release_year') and fallback_year and fallback_year != '未知年份':
        item['release_year'] = fallback_year
    summary = item.get('version_summary') if isinstance(item.get('version_summary'), dict) else {}
    if not summary:
        summary = item.get('summary_json') if isinstance(item.get('summary_json'), dict) else {}
    source_kind = str(item.get('source_kind') or '').strip()
    source_id = str(item.get('source_id') or item.get('source_ref_id') or '').strip()
    sha1 = str(item.get('sha1') or '').strip()
    manifest_hash = str(item.get('manifest_hash') or '').strip()
    unique = f'shared_pool:{source_kind}:{source_id}:{sha1 or manifest_hash}' if source_kind and source_id else f"shared_pool:{item.get('tmdb_id')}:{item.get('season_number') or ''}:{sha1 or manifest_hash or item.get('title') or ''}"
    version_index = _tg_sp_int(item.get('_shared_pool_version_index'), 0)
    version_count = _tg_sp_int(item.get('_shared_pool_version_count'), 0)
    version_label = f'版本 {version_index}/{version_count}' if version_index and version_count > 1 else ''
    tags = _tg_sp_tags(item)
    item.update({
        '_tg_source': 'shared_pool',
        '_cloud_source': 'shared_pool',
        'source_type': 'shared_pool',
        'source_name': '共享池',
        'unique_id': unique,
        'title': _tg_sp_title(item),
        'name': _tg_sp_title(item),
        'pan_type': 'rapid115',
        'already_owned': bool(item.get('is_mine')),
        'unlock_points': 0,
        'share_size': _tg_sp_first(item.get('share_size'), _tg_sp_size_text(item.get('size') or item.get('total_size'))),
        'video_resolution': _tg_sp_first(summary.get('resolution'), summary.get('resolution_display')),
        'quality': _tg_sp_quality_summary(summary),
        # 共享池的 source 不再塞 HDR/杜比信息；quality 里已经包含 effect，
        # 否则 TG 资源行会把 Dolby Vision / HDR10 拼两遍。
        'source': '',
        'source_detail': _tg_sp_first(_tg_sp_quality_summary(summary), summary.get('formatted_by')),
        'remark': ' · '.join(x for x in (item.get('status_message') or '', version_label, f"共享池 · {item.get('progress_text')}" if item.get('progress_text') else '共享池 · 可秒传') if str(x).strip()),
        '_season_match_label': item.get('progress_text') or '',
        '_shared_pool_version_label': version_label,
        '_shared_pool_tag_labels': tags,
        '_shared_pool_tags': tags,
        '_completion_label': '已完结' if item.get('is_completed_certified') or item.get('is_completed') else ('连载中' if item.get('is_ongoing_hub') else ''),
    })
    return item


def _tg_query_shared_pool_resources(tmdb_id, media_type: str, title: str, year: str, target_season=None):
    try:
        from handler.shared_center_client import SharedCenterClient, shared_center_enabled
    except Exception as e:
        return [], 0, [f'共享池模块不可用：{e}']
    if not shared_center_enabled():
        return [], 0, []
    try:
        limit = _TG_RESOURCE_COLLECT_LIMIT
        fetch_limit = max(limit, min(500, limit * 6)) if media_type == 'tv' and target_season in (None, '') else limit
        resp = SharedCenterClient().list_display_sources(
            q='' if tmdb_id else title,
            status='alive,available' if media_type == 'movie' else 'alive,available,updating,inconsistent,incomplete',
            item_type='Movie' if media_type == 'movie' else 'Pack',
            tmdb_id=str(tmdb_id or ''),
            order_by='latest',
            limit=fetch_limit,
            offset=0,
        )
        rows = [x for x in (resp.get('items') or []) if isinstance(x, dict)]
        if media_type == 'tv' and target_season not in (None, ''):
            wanted = _tg_sp_int(target_season, 0)
            if wanted > 0:
                rows = [x for x in rows if _tg_sp_int(x.get('season_number'), -999) == wanted]
        expanded = []
        for row in rows:
            expanded.extend(_tg_sp_versions(row))
        expanded = [x for _, x in sorted(enumerate(expanded), key=_tg_sp_sort_key)]
        total = len(expanded)
        out, seen = [], set()
        for row in expanded[:limit]:
            item = _tg_sp_normalize(row, fallback_year=year)
            key = item.get('unique_id') or f"{item.get('source_kind')}:{item.get('source_id')}:{item.get('sha1')}"
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            out.append(item)
        return out, total, []
    except Exception as e:
        logger.error(f"  ➜ [TG资源搜索] 共享池查询失败: {e}", exc_info=True)
        return [], 0, [f'共享池查询失败：{e}']

def _tg_is_similar_text(a: str, b: str) -> bool:
    a_norm = re.sub(r"\s+", "", str(a or "")).lower()
    b_norm = re.sub(r"\s+", "", str(b or "")).lower()
    if not a_norm or not b_norm:
        return False
    return a_norm == b_norm or a_norm in b_norm or b_norm in a_norm


def _tg_resource_line(index: int, resource: dict) -> str:
    title = _tg_resource_title(resource)
    res_text = _tg_resource_resolution(resource)
    extra = []
    if resource.get("_completion_label"):
        extra.append(str(resource.get("_completion_label")))
    if resource.get("_shared_pool_version_label"):
        extra.append(str(resource.get("_shared_pool_version_label")))
    tags = resource.get("_shared_pool_tag_labels") or resource.get("_shared_pool_tags") or []
    if isinstance(tags, list):
        tag_texts = []
        for tag in tags:
            label = tag.get("label") if isinstance(tag, dict) else tag
            label = str(label or "").strip()
            if label and label not in tag_texts:
                tag_texts.append(label)
        if tag_texts:
            extra.append("标签：" + "/".join(tag_texts[:5]))
    if resource.get("_season_match_label"):
        extra.append(str(resource.get("_season_match_label")))
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        source_channel = resource.get("source_channel") or "未知频道"
        message_date = resource.get("message_date") or ""
        extra.append(f"来自：{source_channel}{' · ' + message_date if message_date else ''}")
    extra_text = f"  {' / '.join(extra)}" if extra else ""

    lines = [
        f"{index:02d}. {_tg_resource_pan_text(resource)}  {_tg_resource_points_text(resource)}  {_tg_resource_size_text(resource)}  🎞 {res_text}{extra_text}",
    ]

    quality = _tg_resource_quality_text(resource)
    if quality:
        lines.append(f"    📦 {quality}")

    remark = _tg_resource_remark(resource)
    if remark and not _tg_is_similar_text(remark, title) and not _tg_is_similar_text(remark, quality):
        lines.append(f"    📝 {remark}")

    # 保留一个可识别标题，避免某些资源只有 remark 时看不出是哪条。
    if title and not _tg_is_similar_text(title, quality) and not _tg_is_similar_text(title, remark):
        lines.append(f"    🎬 {title}")

    return "\n".join(lines)

def _tg_format_hdhive_resources(
    media: dict,
    resources: list,
    raw_count: int,
    filtered_count: int,
    used_filtered: bool,
    channel_count: int = 0,
    shared_pool_count: int = 0,
    notes: list = None,
    page: int = 0,
    total_count: int = None,
) -> str:
    media_type = media.get("media_type") or "movie"
    title = media.get("title") or "未知标题"
    year = media.get("year") or "未知年份"
    tmdb_id = media.get("tmdb_id") or "-"

    resources = resources or []
    total_count = len(resources) if total_count is None else int(total_count or 0)
    page = _tg_clamp_page(page, total_count)
    page_count = max(1, (total_count + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    show_start = page * _TG_RESOURCE_PAGE_SIZE + 1 if total_count else 0
    show_end = min(page * _TG_RESOURCE_PAGE_SIZE + len(resources), total_count)

    hdhive_count = raw_count or 0
    channel_count = channel_count or 0
    shared_pool_count = shared_pool_count or 0
    source_parts = []
    if shared_pool_count:
        source_parts.append(f"🟢 共享池 {shared_pool_count} 条")
    source_parts.append(f"🪺 影巢 {hdhive_count} 条")
    if channel_count:
        source_parts.append(f"📡 频道 {channel_count} 条")
    source_text = " / ".join(source_parts)

    if media_type == "tv":
        count_text = f"🔎 {source_text}；剧集手动搜索不按季过滤。"
    else:
        count_text = f"🔎 {source_text}。"

    if total_count > _TG_RESOURCE_PAGE_SIZE:
        page_text = f"📄 共 {total_count} 条，当前第 {page + 1}/{page_count} 页，显示 {show_start}-{show_end}。"
    else:
        page_text = f"📄 共 {total_count} 条。"

    lines = [
        f"🔎 资源搜索 | {title} ({year})",
        "━━━━━━━━━━━━━━",
        f"🎭 类型：{_tg_media_type_label(media_type)}    🆔 TMDb：{tmdb_id}",
        count_text,
        page_text,
        "↩️ 回复当前页显示的编号直接转存，或点击下方按钮。",
        "➡️ 输入 下一页 / 上一页 也可以翻页。",
        "🚫 输入 取消 结束本次搜索。",
        "",
    ]

    base_index = page * _TG_RESOURCE_PAGE_SIZE
    for offset, item in enumerate(resources, 1):
        lines.append(_tg_resource_line(base_index + offset, item))
        if offset != len(resources):
            lines.append("")

    if notes:
        lines.append("")
        lines.append("ℹ️ " + "；".join(str(n) for n in notes if n))

    return "\n".join(lines)

def _tg_start_tmdb_search(chat_id: str, query: str):
    query = str(query or "").strip()
    if not query:
        _tg_send_plain(chat_id, "请输入要搜索的片名，例如：阿凡达")
        return

    def run():
        try:
            api_key = _tg_get_tmdb_api_key()
            if not api_key:
                _tg_send_plain(chat_id, "❌ 未配置 TMDb API Key，无法搜索。")
                return

            _tg_send_plain(chat_id, f"⏳ 正在搜索 TMDb：{query}", disable_notification=True)

            from handler.tmdb import search_media, search_multi_media

            data = search_multi_media(query=query, api_key=api_key, page=1)
            results = (data or {}).get("results") or []

            # 兼容旧版本：如果 multi 搜不到，再分别查电影/剧集。
            if not results:
                movie_results = search_media(query=query, api_key=api_key, item_type="movie") or []
                tv_results = search_media(query=query, api_key=api_key, item_type="tv") or []
                for item in movie_results:
                    item["media_type"] = "movie"
                for item in tv_results:
                    item["media_type"] = "tv"
                results = movie_results + tv_results

            normalized_results = []
            seen = set()
            for item in results:
                media_type = item.get("media_type")
                tmdb_id = item.get("id")
                if media_type not in {"movie", "tv"} or not tmdb_id:
                    continue
                key = (media_type, str(tmdb_id))
                if key in seen:
                    continue
                seen.add(key)
                normalized_results.append(item)
                if len(normalized_results) >= _TG_TMDB_SEARCH_LIMIT:
                    break

            if not normalized_results:
                _tg_clear_session(chat_id)
                _tg_send_plain(chat_id, f"❌ TMDb 未搜索到：{query}")
                return

            _tg_set_session(chat_id, {
                "stage": "tmdb_results",
                "query": query,
                "results": normalized_results,
            })

            reply_markup = _tg_build_number_keyboard("tg_tmdb", len(normalized_results))
            _tg_send_plain(chat_id, _tg_format_tmdb_results(query, normalized_results), reply_markup=reply_markup)

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] TMDb 搜索失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ TMDb 搜索异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_TMDb", daemon=True).start()


def _tg_query_hdhive_resources(chat_id: str, selection_number: int, target_season=None):
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "tmdb_results":
        _tg_send_plain(chat_id, "❌ 当前没有可选择的 TMDb 搜索结果，请重新输入片名搜索。")
        return

    results = session.get("results") or []
    if selection_number < 1 or selection_number > len(results):
        _tg_send_plain(chat_id, f"❌ 序号无效，请回复 1-{len(results)}。")
        return

    selected = results[selection_number - 1]
    media_type = selected.get("media_type") or "movie"
    tmdb_id = selected.get("id")
    title = _tg_tmdb_title(selected)
    year = _tg_tmdb_year(selected)
    original_title = selected.get("original_title") or selected.get("original_name") or ""

    # TG 手动搜索不再按季过滤。target_season 仅兼容旧输入，不参与剧集筛选。
    media = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "target_season": None,
    }

    def run():
        notes = []
        hdhive_raw_count = 0
        hdhive_filtered_count = 0
        hdhive_used_filtered = False
        hdhive_resources = []
        channel_resources = []
        shared_pool_resources = []
        shared_pool_total = 0

        try:
            season_tip = "（剧集全量，不按季过滤）" if media_type == "tv" else ""
            _tg_send_plain(chat_id, f"⏳ 正在查询资源：{title} ({year}){season_tip}\n来源：共享池 + 影巢 + 已配置监听频道", disable_notification=True)

            # 0. 查询共享池：可直接秒传，展示顺序固定排在影巢前。
            shared_pool_resources, shared_pool_total, shared_notes = _tg_query_shared_pool_resources(
                tmdb_id=tmdb_id, media_type=media_type, title=title, year=year, target_season=target_season
            )
            notes.extend(shared_notes or [])

            # 1. 查询影巢资源：失败不直接中断，继续查频道。
            try:
                from handler.hdhive_client import HDHiveClient
                from tasks.hdhive import filter_hdhive_resources

                client = HDHiveClient()
                if client.ping():
                    query_season = None if media_type == "tv" else target_season
                    raw_resources = client.get_resources(tmdb_id, media_type, target_season=query_season) or []
                    hdhive_raw_count = len(raw_resources)

                    if media_type == "tv":
                        hdhive_resources = raw_resources[:_TG_RESOURCE_COLLECT_LIMIT]
                    else:
                        filtered_resources = filter_hdhive_resources(
                            raw_resources,
                            target_season=None,
                            media_type=media_type,
                            require_complete=False,
                        )
                        hdhive_filtered_count = len(filtered_resources)
                        hdhive_used_filtered = bool(filtered_resources)
                        hdhive_resources = (filtered_resources or raw_resources)[:_TG_RESOURCE_COLLECT_LIMIT]

                    for item in hdhive_resources:
                        item["_tg_source"] = "hdhive"
                else:
                    notes.append("影巢未授权，已跳过影巢查询")
            except Exception as e:
                logger.error(f"  ➜ [TG资源搜索] 影巢资源查询失败: {e}", exc_info=True)
                notes.append(f"影巢查询失败：{e}")

            # 2. 查询已配置监听频道历史。使用 UserBot 账号搜索频道历史消息；不影响原来的频道自动监听。
            try:
                from handler.tg_userbot import TGUserBotManager

                extra_queries = []
                if original_title and original_title != title:
                    extra_queries.append(original_title)
                # 部分频道标题带年份，单独用“片名 年份”有时更准；但仍保留片名搜索。
                if year and year != "未知年份":
                    extra_queries.append(f"{title} {year}")

                ub = TGUserBotManager.get_instance()
                search_result = ub.search_channel_resources(
                    query=title,
                    media_type=media_type,
                    tmdb_id=tmdb_id,
                    year=year,
                    limit=_TG_RESOURCE_COLLECT_LIMIT,
                    extra_queries=extra_queries,
                    timeout=30,
                ) or {}

                if search_result.get("ok"):
                    channel_resources = search_result.get("results") or []
                    for item in channel_resources:
                        item["_tg_source"] = "channel"
                        # 手动点击频道资源应该直接放行，不再要求它本来就在订阅/追剧列表。
                        item["is_keyword_matched"] = True
                        item["is_subscribe"] = False
                        item.setdefault("title", title)
                        item.setdefault("year", year)
                        item.setdefault("tmdb_id", tmdb_id)
                        item.setdefault("item_type", media_type)
                else:
                    err = search_result.get("error")
                    if err:
                        notes.append(f"频道搜索跳过：{err}")
            except Exception as e:
                logger.error(f"  ➜ [TG资源搜索] 频道资源查询失败: {e}", exc_info=True)
                notes.append(f"频道搜索失败：{e}")

            # 3. 合并展示：共享池优先，其次影巢，最后频道。
            all_resources = []
            all_resources.extend(shared_pool_resources)
            all_resources.extend(hdhive_resources)
            all_resources.extend(channel_resources)

            all_resources = all_resources[:_TG_RESOURCE_COLLECT_LIMIT]
            if not all_resources:
                msg = f"❌ 没有找到可处理资源：{title} ({year})"
                if notes:
                    msg += "\n" + "\n".join(f"- {n}" for n in notes)
                # 找不到资源时，保留会话并提供订阅按钮 
                _tg_set_session(chat_id, {
                    "stage": "hdhive_resources",
                    "media": media,
                    "all_resources": [],
                    "resources": [],
                    "page": 0,
                    "raw_count": hdhive_raw_count,
                    "filtered_count": hdhive_filtered_count,
                    "used_filtered": hdhive_used_filtered,
                    "channel_count": len(channel_resources),
                    "shared_pool_count": shared_pool_total,
                    "notes": notes,
                })
                reply_markup = {
                    "inline_keyboard": [
                        [{"text": "🔔 订阅该项目", "callback_data": "tg_subscribe"}],
                        [{"text": "取消", "callback_data": "tg_search_cancel"}]
                    ]
                }
                _tg_send_plain(chat_id, msg, reply_markup=reply_markup)
                return

            _tg_set_session(chat_id, {
                "stage": "hdhive_resources",  # 实际可包含影巢+频道资源。
                "media": media,
                "all_resources": all_resources,
                "resources": _tg_slice_resource_page(all_resources, 0),
                "page": 0,
                "raw_count": hdhive_raw_count,
                "filtered_count": hdhive_filtered_count,
                "used_filtered": hdhive_used_filtered,
                "channel_count": len(channel_resources),
                "shared_pool_count": shared_pool_total,
                "notes": notes,
            })

            _tg_show_resource_page(chat_id, 0)

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 资源查询失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 资源查询异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_All", daemon=True).start()

def _tg_show_resource_page(chat_id: str, page: int):
    """根据当前资源搜索会话发送指定页。每页 10 条，编号使用全局序号。"""
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "hdhive_resources":
        _tg_send_plain(chat_id, "❌ 当前没有可翻页的资源搜索结果，请重新输入片名搜索。")
        return

    all_resources = session.get("all_resources") or session.get("resources") or []
    if not all_resources:
        _tg_send_plain(chat_id, "❌ 当前资源列表为空，请重新输入片名搜索。")
        return

    page = _tg_clamp_page(page, len(all_resources))
    page_resources = _tg_slice_resource_page(all_resources, page)
    session["page"] = page
    session["resources"] = page_resources
    _tg_set_session(chat_id, session)

    _tg_send_plain(
        chat_id,
        _tg_format_hdhive_resources(
            session.get("media") or {},
            page_resources,
            session.get("raw_count") or 0,
            session.get("filtered_count") or 0,
            bool(session.get("used_filtered")),
            channel_count=session.get("channel_count") or 0,
            shared_pool_count=session.get("shared_pool_count") or 0,
            notes=session.get("notes") or [],
            page=page,
            total_count=len(all_resources),
        ),
        reply_markup=_tg_build_resource_page_keyboard(len(all_resources), page),
    )


def _tg_start_hdhive_transfer(chat_id: str, selection_number: int):
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "hdhive_resources":
        _tg_send_plain(chat_id, "❌ 当前没有可选择的资源，请重新输入片名搜索。")
        return

    # 翻页后按钮和正文都使用“全局编号”，所以这里从 all_resources 里取。
    resources = session.get("all_resources") or session.get("resources") or []
    if selection_number < 1 or selection_number > len(resources):
        _tg_send_plain(chat_id, f"❌ 序号无效，请回复 1-{len(resources)}，或点击翻页按钮查看更多。")
        return

    resource = resources[selection_number - 1]
    media = session.get("media") or {}
    source = resource.get("_tg_source") or resource.get("source") or "hdhive"

    title = media.get("title") or resource.get("title") or _tg_resource_title(resource)
    year = media.get("year") or resource.get("year") or ""
    display_title = f"{title} ({year})" if year else title
    media_type = media.get("media_type") or resource.get("item_type") or "movie"
    tmdb_id = media.get("tmdb_id") or resource.get("tmdb_id")

    # 开始转存后清理会话，避免用户重复点按钮造成重复转存。
    _tg_clear_session(chat_id)

    if source == "shared_pool":
        def run_shared_pool_transfer():
            try:
                from handler.shared_center_client import SharedCenterClient, shared_center_enabled
                from handler.shared_subscription_service import consume_center_source_payload

                if not shared_center_enabled():
                    _tg_send_plain(chat_id, "❌ 共享池未启用或未配置中心地址，无法秒传。")
                    return

                shared_source = dict(resource or {})
                source_kind = str(shared_source.get('source_kind') or '').strip()
                source_id = str(shared_source.get('source_id') or shared_source.get('source_ref_id') or '').strip()
                if not source_kind or not source_id:
                    _tg_send_plain(chat_id, "❌ 共享池资源缺少 source_kind/source_id，无法秒传。")
                    return

                _tg_send_plain(chat_id, f"⏳ 已选择共享池资源：{_tg_resource_title(shared_source)}\n正在执行 115 秒传，请稍后查看通知/日志。", disable_notification=True)

                # 连载公共包列表页只返回壳，真正秒传前按需加载该季 children。
                if source_kind == 'season_hub' and not (shared_source.get('children') or shared_source.get('pack_items')):
                    child_resp = SharedCenterClient().list_display_children(
                        source_kind='season_hub',
                        source_id=source_id,
                        hub_id=shared_source.get('hub_id') or source_id,
                        limit=5000,
                    )
                    children = child_resp.get('children') or child_resp.get('items') or []
                    pack_items = child_resp.get('pack_items') or children
                    shared_source['children'] = children
                    shared_source['pack_items'] = pack_items

                result = consume_center_source_payload(shared_source)
                ok = bool(result.get('ok') or result.get('success'))
                msg = result.get('message') or (
                    f"共享池秒传完成：{result.get('success_count', 0)}/{result.get('total', 0)}"
                    if ok else "共享池秒传失败"
                )
                if ok:
                    _tg_send_plain(chat_id, f"✅ 共享池秒传已提交：{display_title}\n{msg}")
                else:
                    _tg_send_plain(chat_id, f"❌ 共享池秒传失败：{display_title}\n{msg}")
            except Exception as e:
                logger.error(f"  ➜ [TG资源搜索] 共享池秒传失败: {e}", exc_info=True)
                _tg_send_plain(chat_id, f"❌ 共享池秒传异常：{e}")

        threading.Thread(target=run_shared_pool_transfer, name="TG_Resource_Search_SharedPool", daemon=True).start()
        return

    if source == "channel":
        try:
            from handler.tg_userbot import tg_task_queue

            task = build_channel_task_payload(
                resource,
                is_brainless=False,
                is_keyword_matched=True,
                is_subscribe=False,
                title_override=title,
                tmdb_id_override=tmdb_id,
                media_type_override=media_type,
                year_override=year,
            )

            if not task.get("target_link") and not task.get("magnet_url"):
                _tg_send_plain(chat_id, "❌ 当前频道资源缺少 115/影巢/磁力链接，无法转存。")
                return

            tg_task_queue.put(task)
            source_channel = resource.get("source_channel") or "频道"
            _tg_send_plain(chat_id, f"✅ 已提交频道资源转存：{display_title}\n来源：{source_channel}\n请稍后查看转存通知/系统日志。")
            return
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 频道资源提交失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 频道资源提交异常：{e}")
            return

    slug = resource.get("slug") or resource.get("resource_slug") or resource.get("id")
    if not slug:
        _tg_send_plain(chat_id, "❌ 当前影巢资源缺少 slug，无法解锁转存。")
        return

    def run():
        try:
            _tg_send_plain(chat_id, f"⏳ 已选择影巢资源：{_tg_resource_title(resource)}\n正在解锁并转存到 115，请稍后查看通知/日志。", disable_notification=True)

            from tasks.hdhive import task_download_from_hdhive

            ok = task_download_from_hdhive(
                api_key=None,
                slug=slug,
                tmdb_id=tmdb_id,
                media_type=media_type,
                title=display_title,
            )

            if ok:
                _tg_send_plain(chat_id, f"✅ 影巢资源已提交转存：{display_title}")
            else:
                _tg_send_plain(chat_id, f"❌ 影巢资源转存失败：{display_title}\n请查看系统日志确认原因。")

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 影巢转存失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 影巢转存异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_Transfer", daemon=True).start()

def _tg_handle_subscribe(chat_id: str):
    """处理 TG 搜索界面的订阅按钮点击"""
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "hdhive_resources":
        _tg_send_plain(chat_id, "❌ 当前没有可订阅的项目，请重新搜索。")
        return

    media = session.get("media") or {}
    tmdb_id = media.get("tmdb_id")
    media_type = media.get("media_type")
    title = media.get("title") or "未知标题"
    year = media.get("year") or ""
    display_title = f"{title} ({year})" if year else title

    if not tmdb_id:
        _tg_send_plain(chat_id, "❌ 缺少 TMDb ID，无法订阅。")
        return

    # 清理会话防止重复点击
    _tg_clear_session(chat_id)
    _tg_send_plain(chat_id, f"⏳ 正在提交订阅：{display_title}...", disable_notification=True)

    def run():
        try:
            from tasks.helpers import process_subscription_items_and_update_db
            from handler.tmdb import get_tv_details
            
            api_key = _tg_get_tmdb_api_key()
            tmdb_items = []
            
            if media_type == "movie":
                tmdb_items.append({
                    'tmdb_id': tmdb_id,
                    'media_type': 'Movie',
                    'season': None
                })
            elif media_type == "tv":
                # 剧集需要按季订阅，拉取详情获取所有季
                details = get_tv_details(tmdb_id, api_key)
                if details and 'seasons' in details:
                    for s in details['seasons']:
                        s_num = s.get('season_number')
                        # 过滤掉第 0 季 (特别篇)，通常只订阅正片
                        if s_num is not None and s_num > 0:
                            tmdb_items.append({
                                'tmdb_id': tmdb_id,
                                'media_type': 'Series',
                                'season': s_num
                            })
                else:
                    # 兜底订阅第 1 季
                    tmdb_items.append({
                        'tmdb_id': tmdb_id,
                        'media_type': 'Series',
                        'season': 1
                    })

            if not tmdb_items:
                _tg_send_plain(chat_id, f"❌ 无法解析订阅信息：{display_title}")
                return

            # --- 新增：查询 TG ID 绑定的 Emby 用户名 ---
            emby_username = 'TG 搜索'  # 默认兜底名称
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT u.name 
                            FROM emby_users u
                            JOIN emby_users_extended e ON u.id = e.emby_user_id
                            WHERE e.telegram_chat_id = %s
                        """, (str(chat_id),))
                        row = cursor.fetchone()
                        if row and row.get('name'):
                            # 如果查到了绑定的用户，使用该用户名 (可按需加上 TG 后缀以作区分)
                            emby_username = f"{row['name']}" 
            except Exception as e:
                logger.error(f"  ➜ [TG交互] 查询 TG ID {chat_id} 绑定的 Emby 用户名失败: {e}")

            # 标记订阅来源，使用查询到的用户名
            subscription_source = {'type': 'telegram_search', 'user_id': chat_id, 'name': emby_username}
            
            # 调用 helpers 的通用订阅函数
            # tmdb_to_emby_item_map 传空字典即可，内部会自动查库校验
            processed_ids = process_subscription_items_and_update_db(
                tmdb_items=tmdb_items,
                tmdb_to_emby_item_map={}, 
                subscription_source=subscription_source,
                tmdb_api_key=api_key
            )
            
            if processed_ids:
                _tg_send_plain(chat_id, f"✅ 订阅已提交：{display_title}\n系统将在后台自动监控并处理。")
            else:
                _tg_send_plain(chat_id, f"⚠️ 订阅请求已处理：{display_title}\n(可能已在库或已处于订阅状态)")

        except Exception as e:
            logger.error(f"  ➜ [TG交互] 提交订阅失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 提交订阅异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Subscribe", daemon=True).start()

def _tg_try_handle_resource_session_input(chat_id: str, text: str) -> bool:
    """处理资源搜索会话中的数字回复/取消。返回 True 表示已消费消息。"""
    stripped = str(text or "").strip()
    if stripped.lower() in {"取消", "cancel", "/cancel", "退出", "停止"}:
        if _tg_get_session(chat_id):
            _tg_clear_session(chat_id)
            _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
            return True
        return False

    session = _tg_get_session(chat_id)
    if session and session.get("stage") == "hdhive_resources":
        lower = stripped.lower()
        if lower in {"下一页", "下页", "next", "n", ">", "➡️"}:
            _tg_show_resource_page(chat_id, int(session.get("page") or 0) + 1)
            return True
        if lower in {"上一页", "上页", "prev", "previous", "p", "<", "⬅️"}:
            _tg_show_resource_page(chat_id, int(session.get("page") or 0) - 1)
            return True

    number, season = _tg_parse_selection_text(stripped)
    if number is None:
        return False

    session = _tg_get_session(chat_id)
    if not session:
        return False

    if session.get("stage") == "tmdb_results":
        _tg_query_hdhive_resources(chat_id, number, target_season=season)
        return True

    if session.get("stage") == "hdhive_resources":
        _tg_start_hdhive_transfer(chat_id, number)
        return True

    return False


def _execute_task_from_tg(chat_id: str, task_key: str):
    """在后台线程中执行选定的任务"""
    from tasks.core import get_task_registry
    registry = get_task_registry(context='all')
    task_info = registry.get(task_key)
    
    if not task_info:
        send_telegram_message(chat_id, escape_markdown("❌ 任务不存在或已失效。"))
        return

    task_function, task_description, processor_type = task_info[:3]
    
    # 获取对应的处理器实例
    target_processor = None
    if processor_type == 'media':
        target_processor = extensions.media_processor_instance
    elif processor_type == 'watchlist':
        target_processor = extensions.watchlist_processor_instance
    elif processor_type == 'actor':
        target_processor = extensions.actor_subscription_processor_instance

    if not target_processor:
        send_telegram_message(chat_id, escape_markdown(f"❌ 无法获取 {processor_type} 处理器实例。"))
        return

    current_version = ""
    target_version = ""
    update_container_name = ""
    update_image_name = ""
    update_strategy = ""
    if task_key == 'system-auto-update':
        try:
            from tasks.system_update import get_system_update_version_info, resolve_update_target, resolve_update_strategy
            version_info = get_system_update_version_info() or {}
            current_version = str(version_info.get('current_version') or '').strip()
            target_version = str(version_info.get('target_version') or '').strip()
            update_target = resolve_update_target(getattr(target_processor, 'config', {}) or {})
            update_container_name = str(update_target.get('container_name') or '').strip()
            update_image_name = str(update_target.get('docker_image_name') or '').strip()
            strategy_info = resolve_update_strategy(getattr(target_processor, 'config', {}) or {})
            update_strategy = str(strategy_info.get('strategy') or '').strip()
        except Exception as e:
            logger.debug(f"  ➜ [TG交互] 获取系统更新版本信息失败: {e}")

    start_lines = [f"🚀 任务已启动：*{task_description}*"]
    if task_key == 'system-auto-update':
        if current_version:
            start_lines.append(f"当前版本: `{current_version}`")
        if target_version:
            start_lines.append(f"目标版本: `{target_version}`")
        if update_container_name:
            start_lines.append(f"目标容器: `{update_container_name}`")
        if update_image_name:
            start_lines.append(f"目标镜像: `{update_image_name}`")
        if update_strategy:
            start_lines.append(f"更新策略: `{update_strategy}`")
    start_lines.append("请在系统日志或任务中心查看进度。")
    send_telegram_message(chat_id, escape_markdown("\n".join(start_lines)))
    logger.info(f"  ➜ [TG交互] 管理员 {chat_id} 触发了任务: {task_description}")

    # 包装执行逻辑，处理特殊参数
    def run_wrapper():
        try:
            task_result = None
            tasks_requiring_force_flag = ['role-translation', 'enrich-aliases', 'populate-metadata']
            if task_key in tasks_requiring_force_flag:
                task_result = task_function(target_processor, force_full_update=False)
            else:
                task_result = task_function(target_processor)

            if task_key == 'system-auto-update':
                result = task_result if isinstance(task_result, dict) else {}
                ok = bool(result.get('ok'))
                updated = bool(result.get('updated'))
                message = str(result.get('message') or '').strip()
                before_version = str(result.get('current_version') or current_version or '').strip()
                after_version = str(result.get('target_version') or target_version or '').strip()

                if not ok:
                    fail_lines = [f"❌ 任务执行失败：*{task_description}*"]
                    if before_version:
                        fail_lines.append(f"当前版本: `{before_version}`")
                    if after_version:
                        fail_lines.append(f"目标版本: `{after_version}`")
                    if message:
                        fail_lines.append(f"错误信息: {message}")
                    send_telegram_message(chat_id, escape_markdown("\n".join(fail_lines)))
                    return

                success_lines = [f"✅ 任务执行完毕：*{task_description}*"]
                if updated:
                    if before_version and after_version:
                        success_lines.append(f"版本变化: `{before_version}` -> `{after_version}`")
                    elif after_version:
                        success_lines.append(f"更新目标版本: `{after_version}`")
                else:
                    if before_version:
                        success_lines.append(f"当前版本: `{before_version}`")
                    if after_version:
                        success_lines.append(f"最新版本: `{after_version}`")
                if message:
                    success_lines.append(message)
                send_telegram_message(chat_id, escape_markdown("\n".join(success_lines)))
                return

            send_telegram_message(chat_id, escape_markdown(f"✅ 任务执行完毕：*{task_description}*"))
        except Exception as e:
            logger.error(f"  ➜ TG触发任务 '{task_description}' 失败: {e}", exc_info=True)
            send_telegram_message(chat_id, escape_markdown(f"❌ 任务执行失败：*{task_description}*\n错误信息: {str(e)}"))

    # 启动独立线程执行任务，避免阻塞 TG 轮询
    threading.Thread(target=run_wrapper, name=f"TG_Task_{task_key}", daemon=True).start()

def _handle_callback_query(callback_query: dict):
    """处理内联键盘的按钮点击事件"""
    query_id = callback_query.get('id')
    from_user = callback_query.get('from', {})
    requester_id = str(from_user.get('id', ''))
    message_chat = (callback_query.get('message') or {}).get('chat') or {}
    chat_id = str(message_chat.get('id') or requester_id)
    data = callback_query.get('data', '')

    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    
    # 1. 权限校验：按钮点击按点击者身份校验；消息发送仍回到原聊天。
    admin_ids = [str(aid) for aid in user_db.get_admin_telegram_chat_ids()]
    if requester_id not in admin_ids:
        logger.warning(f"  ➜ [TG交互] 收到未授权用户 ({requester_id}) 的回调请求，已拒绝。")
        return

    # 2. 响应 Callback Query (消除按钮上的加载圈圈)
    if bot_token and query_id:
        answer_url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
        try:
            requests.post(answer_url, json={'callback_query_id': query_id}, proxies=get_proxies_for_requests(), timeout=5)
        except Exception:
            pass

    # 3. 处理资源搜索/转存选择按钮
    if data == 'tg_search_cancel':
        _tg_clear_session(chat_id)
        _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
        return

    if data.startswith('tg_tmdb:'):
        try:
            _tg_query_hdhive_resources(chat_id, int(data.split(':', 1)[1]))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理 TMDb 选择按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 选择失败，请重新输入片名搜索。")
        return

    if data.startswith('tg_res_page:'):
        page_value = data.split(':', 1)[1]
        if page_value == 'noop':
            return
        try:
            _tg_show_resource_page(chat_id, int(page_value))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理资源翻页按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 翻页失败，请重新输入片名搜索。")
        return

    if data.startswith('tg_hdhive:'):
        try:
            _tg_start_hdhive_transfer(chat_id, int(data.split(':', 1)[1]))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理资源选择按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 选择失败，请重新输入片名搜索。")
        return

    # 处理订阅按钮点击 
    if data == 'tg_subscribe':
        _tg_handle_subscribe(chat_id)
        return

    # 4. 处理任务触发逻辑
    if data.startswith('run_task_'):
        task_key = data.replace('run_task_', '')
        _execute_task_from_tg(chat_id, task_key)
        return

def _handle_incoming_message(message: dict):
    """处理接收到的单条消息 (纯手动遥控器模式)"""
    chat_id = str(message.get('chat', {}).get('id', ''))
    text = message.get('text', '') or message.get('caption', '') # 兼容带图片的 caption
    text = text.strip()
    if not chat_id or not text:
        return

    # 1. 权限校验：只允许管理员发送指令 (或者来自全局频道)
    admin_ids = [str(aid) for aid in user_db.get_admin_telegram_chat_ids()]
    from_user_id = str((message.get('from') or {}).get('id', ''))
    global_channel = str(APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID, ''))
    is_admin = chat_id in admin_ids or from_user_id in admin_ids
    
    if not is_admin and chat_id != global_channel:
        logger.warning(f"  ➜ [TG交互] 收到未授权用户/聊天 ({from_user_id or chat_id}) 的消息，已忽略。")
        return


    # 会话中的“回复序号/取消”优先处理
    if is_admin and _tg_try_handle_resource_session_input(chat_id, text):
        return

    # ★★★ 处理 M 菜单发来的命令 ★★★
    if text.startswith('/'):
        cmd_body = text[1:].strip()
        cmd_token = cmd_body.split()[0].lower() if cmd_body else ''
        cmd = cmd_token.split('@', 1)[0]
        cmd_args = cmd_body[len(cmd_token):].strip() if cmd_token else ''

        if cmd in ['cancel', '取消']:
            _tg_clear_session(chat_id)
            _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
            return

        if cmd in ['search', 'find', 'hdhive']:
            if not is_admin:
                _tg_send_plain(chat_id, "❌ 只有管理员可以使用资源搜索。")
                return
            if not cmd_args:
                _tg_send_plain(chat_id, "请输入要搜索的片名，例如：/search 阿凡达")
                return
            _tg_start_tmdb_search(chat_id, cmd_args)
            return

        from tasks.core import get_task_registry
        registry = get_task_registry(context='all')

        if cmd in ['all_tasks', 'tasks', 'menu']:
            keyboard = []
            row = []
            for key, info in registry.items():
                desc = info[1]
                row.append({"text": desc, "callback_data": f"run_task_{key}"})
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row: keyboard.append(row)
            reply_markup = {"inline_keyboard": keyboard}
            send_telegram_message(chat_id, escape_markdown("📋 *所有可用任务列表*\n请点击下方按钮执行对应任务："), reply_markup=reply_markup)
            return

        for key in registry.keys():
            expected_cmd = key.replace('-', '_').lower()
            if cmd == expected_cmd:
                _execute_task_from_tg(chat_id, key)
                return

    # 2. 识别链接类型
    is_magnet = text.lower().startswith('magnet:?')
    is_ed2k = text.lower().startswith('ed2k://')
    is_115_share = re.search(r'115(?:cdn)?\.com/s/', text, re.IGNORECASE) is not None

    if not (is_magnet or is_ed2k or is_115_share):
        # 管理员私聊/群聊中输入普通片名，进入 TMDb -> 影巢 -> 115 转存流程。
        # 全局频道普通文本不触发搜索，避免频道公告被误当作片名。
        if is_admin:
            _tg_start_tmdb_search(chat_id, text)
        return

    # =================================================================
    # ★ 纯手动处理逻辑 (不再包含任何自动订阅和查库代码)
    # =================================================================
    logger.info(f"  ➜ [TG交互] 收到来自 {chat_id} 的手动资源链接，准备处理...")
    send_telegram_message(chat_id, escape_markdown("⏳ *收到链接，正在提交至 115...*"), disable_notification=True)

    client = P115Service.get_client()
    if not client:
        send_telegram_message(chat_id, "❌ *提交失败*：115 客户端未初始化，请检查配置。")
        return
        
    target_cid = APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')

    try:
        # --- 处理 115 分享链接转存 ---
        if is_115_share:
            share_code_match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', text, re.IGNORECASE)
            share_code = share_code_match.group(1) if share_code_match else None
            
            receive_code = ""
            pwd_match = re.search(r'(?:访问码|提取码|密码|password)[:：=\s]*([a-zA-Z0-9]{4})', text, re.IGNORECASE)
            if pwd_match: receive_code = pwd_match.group(1)

            if not share_code:
                send_telegram_message(chat_id, escape_markdown("❌ *解析失败*：未找到有效的 115 分享码。"))
                return

            res = client.share_import(share_code, receive_code, target_cid)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, escape_markdown("✅ *分享链接转存成功！*\n系统已自动触发整理任务。"))
                try:
                    import task_manager
                    threading.Timer(5.0, task_manager.trigger_115_organize_task).start()
                except Exception as e:
                    logger.error(f"  ➜ 唤醒整理任务失败: {e}")
            else:
                err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                send_telegram_message(chat_id, escape_markdown(f"❌ *转存失败*：{err}"))
                logger.error(f"  ➜ [TG交互] 转存失败: {err}")

        # --- 处理磁力/ED2K 离线下载 ---
        if is_magnet or is_ed2k:
            link_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)
            target_url = link_match.group(1) if link_match else text

            payload = {"url[0]": target_url, "wp_path_id": target_cid}
            res = client.offline_add_urls(payload)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, escape_markdown("✅ *离线任务提交成功！*\n系统将在后台自动监控并整理入库。"))
                try:
                    import task_manager
                    threading.Timer(10.0, task_manager.trigger_115_organize_task).start()
                except: pass
            else:
                err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                send_telegram_message(chat_id, escape_markdown(f"❌ *离线提交失败*：{err}"))

    except Exception as e:
        logger.error(f"  ➜ [TG交互] 处理链接失败: {e}", exc_info=True)
        send_telegram_message(chat_id, f"❌ *系统异常*：处理链接时发生错误。")

def _setup_bot_commands(bot_token: str):
    """
    向 Telegram 注册机器人的命令菜单 (生成输入框左侧的 Menu 按钮)
    将常用任务直接注册为快捷命令。
    """
    from tasks.core import get_task_registry
    registry = get_task_registry(context='all')

    # ==========================================
    # ★★★ 修改：使用常量读取 TG 菜单任务列表 ★★★
    # ==========================================
    # 从 APP_CONFIG 中获取前端保存的配置，如果没有则使用 constants 中的默认值
    allowed_tasks = APP_CONFIG.get(
        constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS, 
        constants.DEFAULT_TELEGRAM_MENU_TASKS
    )
    
    # 如果前端传过来的是空列表（用户清空了菜单），为了防止菜单为空报错，回退到默认值
    if not allowed_tasks:
        allowed_tasks = constants.DEFAULT_TELEGRAM_MENU_TASKS

    commands = []
    for key in allowed_tasks:
        if key in registry:
            desc = registry[key][1]
            # Telegram 命令只允许小写字母、数字和下划线，所以把横杠替换为下划线
            cmd_name = key.replace('-', '_').lower()
            commands.append({"command": cmd_name, "description": f"🚀 {desc}"})

    # 在菜单最下方追加资源搜索和“查看所有任务”的备选命令
    commands.append({"command": "search", "description": "🔎 搜索云资源并转存/秒传"})
    commands.append({"command": "all_tasks", "description": "📋 查看所有可用任务"})

    api_url = f"https://api.telegram.org/bot{bot_token}/setMyCommands"
    payload = {"commands": commands}
    
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=10, proxies=proxies)
        if response.status_code == 200:
            logger.trace("  ➜ 成功注册 Telegram 机器人快捷菜单。")
        else:
            logger.warning(f"  ➜ 注册 Telegram 菜单命令失败: {response.text}")
    except Exception as e:
        logger.error(f"  ➜ 注册 Telegram 菜单命令时发生网络异常: {e}")

def _telegram_polling_worker():
    """后台轮询线程"""
    global _tg_polling_active
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        logger.info("  ➜ 未配置 Telegram Bot Token，交互功能未启动。")
        return

    # ==========================================
    # ★★★ 新增：启动时自动向 TG 注册菜单按钮 ★★★
    _setup_bot_commands(bot_token)
    # ==========================================

    api_url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    offset = None
    
    logger.trace("  ➜ Telegram 机器人交互监听已启动！")
    
    while _tg_polling_active:
        try:
            # ★★★ 修改：允许接收 message 和 callback_query ★★★
            params = {'timeout': 30, 'allowed_updates': ['message', 'callback_query']}
            if offset:
                params['offset'] = offset
                
            proxies = get_proxies_for_requests()
            response = requests.get(api_url, params=params, timeout=40, proxies=proxies)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    for update in data.get('result', []):
                        offset = update['update_id'] + 1
                        
                        # ★★★ 修改：分发不同类型的更新 ★★★
                        if 'message' in update:
                            _handle_incoming_message(update['message'])
                        elif 'callback_query' in update:
                            _handle_callback_query(update['callback_query'])
                            
            elif response.status_code == 401 or response.status_code == 404:
                logger.error("  ➜ Telegram Bot Token 无效，停止轮询。")
                break
                
        except requests.exceptions.Timeout:
            pass 
        except Exception as e:
            logger.debug(f"  ➜ Telegram 轮询网络异常 (将自动重试): {e}")
            time.sleep(5) 
            
        time.sleep(1)

def send_hdhive_checkin_notification(checkin_res: dict, is_gambler: bool, user_info: dict = None):
    """
    发送影巢签到结果的 Telegram 通知卡片 (精简版)
    """
    if user_info is None:
        user_info = {}
        
    res_data = checkin_res.get("data", {})
    message_text = res_data.get("message") or checkin_res.get("message", "签到请求成功")
    # 判断是否真正签到成功 (success 为 true 且 checked_in 不为 false)
    is_success = checkin_res.get("success", False) and res_data.get("checked_in") is not False

    # 提取奖励积分 (正则匹配 "获得 X 积分")
    import re
    reward_match = re.search(r'获得\s*(-?\d+)\s*积分', message_text)
    reward = reward_match.group(1) if reward_match else "0"

    # 提取用户名 (OpenAPI 返回的是 username)
    username = user_info.get("username") or user_info.get("name") or "未知用户"
    mode_text = "赌狗签到" if is_gambler else "普通签到"

    status_icon = "✅" if is_success else "⚠️"
    status_title = "影巢签到成功" if is_success else "影巢签到提示"
    status_text = "签到成功" if is_success else "今日已签到或失败"

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    separator = "\\-" * 24

    # 构造精简版 MarkdownV2 文本 
    text = (
        f"【{status_icon} *{escape_markdown(status_title)}*】\n"
        f"📢 *执行结果*\n"
        f"{separator}\n"
        f"🕒 *时间*: `{escape_markdown(current_time)}`\n"
        f"👤 *用户*: `{escape_markdown(username)}`\n"
        f"📍 *模式*: {escape_markdown(mode_text)}\n"
        f"✨ *状态*: {escape_markdown(status_text)}\n\n"
        f"📊 *签到详情*\n"
        f"💬 *消息*: {escape_markdown(message_text)}\n"
        f"🎁 *奖励*: {escape_markdown(reward)} 积分"
    )

    # 发送给频道和所有管理员
    global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
    admin_ids = set(user_db.get_admin_telegram_chat_ids())

    if global_channel_id:
        send_telegram_message(global_channel_id, text)

    for admin_id in admin_ids:
        if str(admin_id) != str(global_channel_id):
            send_telegram_message(admin_id, text)

def start_telegram_bot():
    """启动 Telegram 机器人监听"""
    global _tg_polling_thread, _tg_polling_active
    
    # Pro 权限拦截
    if not APP_CONFIG.get('is_pro_active', False):
        return

    if _tg_polling_active:
        return
        
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        return
        
    _tg_polling_active = True
    _tg_polling_thread = threading.Thread(target=_telegram_polling_worker, daemon=True, name="TG_Polling_Thread")
    _tg_polling_thread.start()

    try:
        from config_manager import retry_pending_system_update_result
        threading.Thread(
            target=retry_pending_system_update_result,
            kwargs={"max_attempts": 10, "interval_seconds": 3.0},
            daemon=True,
            name="SystemUpdateNotifyRetry",
        ).start()
    except Exception as e:
        logger.debug(f"  ➜ 启动系统更新结果通知重试线程失败: {e}")

def stop_telegram_bot():
    """停止 Telegram 机器人监听"""
    global _tg_polling_active
    _tg_polling_active = False
    logger.info("  ➜ Telegram 机器人交互监听已停止。")


def send_share_request_push_notification(event: dict, result: dict = None, success: bool = True):
    """求分享命中后，客户端长轮询自动转存完成通知。"""
    try:
        result = result or {}
        title = str((event or {}).get('title') or ((event or {}).get('payload') or {}).get('title') or '未知资源')
        group_id = str((event or {}).get('group_id') or '')
        source_id = str((event or {}).get('source_id') or '')
        target_type = str((event or {}).get('target_type') or '')
        season_number = (event or {}).get('season_number')
        episode_number = (event or {}).get('episode_number')
        bounty = (event or {}).get('current_bounty')
        mode = str(result.get('mode') or result.get('action_type') or '')
        message = str(result.get('message') or result.get('error') or '')
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        target_text = ''
        try:
            if target_type in ('series', 'tv'):
                target_text = '全剧'
            elif target_type == 'season' and season_number not in (None, ''):
                target_text = f"S{int(season_number):02d}"
            elif target_type == 'episode' and season_number not in (None, '') and episode_number not in (None, ''):
                target_text = f"S{int(season_number):02d}E{int(episode_number):02d}"
        except Exception:
            target_text = ''

        status_title = '✅ 求分享已命中并自动转存成功' if success else '⚠️ 求分享命中但自动转存失败'
        lines = [
            f"*{escape_markdown(status_title)}*",
            "",
            f"🎬 *资源*: `{_markdown_code_text(title)}`",
        ]
        if target_text:
            lines.append(f"🎯 *目标*: `{_markdown_code_text(target_text)}`")
        if bounty not in (None, ''):
            lines.append(f"🏆 *悬赏*: `{_markdown_code_text(str(bounty))}`")
        if mode:
            lines.append(f"📥 *模式*: `{_markdown_code_text(mode)}`")
        if source_id:
            lines.append(f"🆔 *来源*: `{_markdown_code_text(source_id)}`")
        if group_id:
            lines.append(f"🔖 *求分享*: `{_markdown_code_text(group_id)}`")
        lines.append(f"🕒 *时间*: `{_markdown_code_text(current_time)}`")
        if message:
            lines.append(f"📝 *结果*: {escape_markdown(message[:300])}")

        text = "\n".join(lines)
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())
        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            if aid:
                targets.add(str(aid))
        for target in targets:
            send_telegram_message(target, text)
    except Exception as e:
        logger.error(f"  ➜ 发送求分享自动转存通知失败: {e}", exc_info=True)
