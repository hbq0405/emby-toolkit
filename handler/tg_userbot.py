# handler/tg_userbot.py
import os
import re
import asyncio
import threading
import queue
import logging
import time
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, AuthKeyUnregisteredError

import config_manager
import constants
from database import settings_db
from handler.p115_service import P115Service, P115CacheManager
from utils import DEFAULT_TG_REGEX
from handler.tg_media_candidate import (
    build_channel_task_payload,
    build_tg_media_candidate,
    candidate_to_recognition_hints,
    remember_candidate_hint,
)
from database.connection import get_db_connection
from gevent import spawn

logger = logging.getLogger(__name__)

# 线程安全的队列，用于把 asyncio 线程的数据传递给 gevent 协程
tg_task_queue = queue.Queue()

class TGUserBotManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.client = None
        self.loop = None
        self.thread = None
        self.is_running = False
        self.session_path = os.path.join(config_manager.PERSISTENT_DATA_PATH, 'tg_userbot.session')
        self.session_journal_path = self.session_path + '-journal'
        self.phone_code_hash = None

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = TGUserBotManager()
            return cls._instance

    def _get_config(self):
        cfg = settings_db.get_setting('tg_userbot_config') or {}
        return {
            'enabled': cfg.get('enabled', False),
            'api_id': cfg.get('api_id', ''),
            'api_hash': cfg.get('api_hash', ''),
            'phone': cfg.get('phone', ''),
            'password': cfg.get('password', ''),
            'channels': cfg.get('channels', []),
            'monitor_types': cfg.get('monitor_types', ['movie', 'tv']),
            'transfer_modes': cfg.get('transfer_modes', ['subscribe']),
            'transfer_keywords': cfg.get('transfer_keywords', []),
            'block_keywords': cfg.get('block_keywords', []),
            'custom_regex': cfg.get('custom_regex', {})
        }

    @staticmethod
    def _channel_rule_matches(target_channel, chat_username, chat_id):
        """判断一条频道隔离规则是否适用于当前频道。"""
        target_channel = str(target_channel or '').strip().lower()
        if not target_channel:
            return True

        chat_username = str(chat_username or '').strip().lower().lstrip('@')
        chat_id = str(chat_id or '').strip()
        target_clean = target_channel.lstrip('@')
        target_id_clean = target_clean.replace('-100', '') if target_clean.startswith('-100') else target_clean
        curr_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id

        return (
            chat_username == target_clean
            or chat_id == target_channel
            or curr_id_clean == target_id_clean
        )

    def _match_block_rule(self, text, chat_username='', chat_id='', rules=None):
        """复用频道监听配置里的拦截规则。

        返回命中的规则 pattern；未命中返回 None。
        该函数只做判断，不决定调用场景。实时监听、统一订阅自动搜索可以调用；
        手动 TG 搜索和云下载模态框不调用，因此不受拦截规则影响。
        """
        if rules is None:
            rules = (self._get_config().get('block_keywords') or [])

        if not text or not rules:
            return None

        for rule_obj in rules:
            if isinstance(rule_obj, str):
                pattern = rule_obj.strip()
                target_channel = ''
            else:
                pattern = str((rule_obj or {}).get('pattern', '')).strip()
                target_channel = str((rule_obj or {}).get('channel', '')).strip().lower()

            if not pattern:
                continue

            if not self._channel_rule_matches(target_channel, chat_username, chat_id):
                continue

            try:
                if re.search(pattern, text, re.IGNORECASE):
                    return pattern
            except Exception as e:
                logger.error(f"  ➜ [频道监听] 拦截规则正则解析错误 '{pattern}': {e}")

        return None

    def is_resource_blocked_by_rules(self, resource):
        """供统一订阅自动流程调用：判断频道历史搜索候选是否命中拦截规则。

        注意：手动 TG 搜索、云下载模态框不调用这个方法，所以仍然允许人工肉眼挑选。
        """
        resource = resource or {}
        text = resource.get('text') or resource.get('remark') or resource.get('title') or ''
        chat_username = resource.get('source_username') or ''
        chat_id = resource.get('source_chat_id') or resource.get('chat_id') or ''
        return self._match_block_rule(text, chat_username=chat_username, chat_id=chat_id)

    def start(self):
        """启动后台线程"""
        cfg = self._get_config()
        if not cfg['enabled'] or not cfg['api_id'] or not cfg['api_hash']:
            if self.is_running:
                logger.info("  ➜ [频道监听] 监听已在配置中关闭，正在停止服务...")
                self.stop()
            return

        if self.is_running and self.thread and self.thread.is_alive():
            return

        self.stop() 

        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="TG_UserBot_Thread")
        self.thread.start()

    def stop(self):
        """停止后台线程"""
        self.is_running = False
        if self.client and self.loop and self.loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(self.client.disconnect(), self.loop).result(timeout=3)
            except Exception:
                pass
                
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
            
        self.client = None
        self.loop = None

    def _run_loop(self):
        """在独立线程中运行 asyncio 事件循环"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        cfg = self._get_config()
        try:
            telethon_proxy = None
            app_cfg = config_manager.APP_CONFIG
            if app_cfg.get(constants.CONFIG_OPTION_NETWORK_PROXY_ENABLED) and app_cfg.get(constants.CONFIG_OPTION_NETWORK_HTTP_PROXY):
                proxy_url = app_cfg.get(constants.CONFIG_OPTION_NETWORK_HTTP_PROXY)
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(proxy_url)
                    scheme = parsed.scheme.lower()
                    p_type = 'socks5' if scheme == 'socks5' else ('socks4' if scheme == 'socks4' else 'http')
                    telethon_proxy = {
                        'proxy_type': p_type,
                        'addr': parsed.hostname,
                        'port': parsed.port
                    }
                    if parsed.username and parsed.password:
                        telethon_proxy['username'] = parsed.username
                        telethon_proxy['password'] = parsed.password
                except Exception as e:
                    logger.warning(f"  ➜ [频道监听] 解析代理 URL 失败: {e}")

            self.client = TelegramClient(
                self.session_path, 
                int(cfg['api_id']), 
                cfg['api_hash'], 
                loop=self.loop,
                proxy=telethon_proxy
            )
            
            @self.client.on(events.NewMessage())
            async def handler(event):
                await self._handle_message(event)

            async def _daemon():
                await self.client.connect()
                
                try:
                    is_auth = await self.client.is_user_authorized()
                except AuthKeyUnregisteredError:
                    logger.error("  ➜ [频道监听] 登录凭证已失效 (AuthKeyUnregistered)。已自动清理，请在前端重新登录！")
                    if self.client:
                        await self.client.disconnect()
                    if os.path.exists(self.session_path):
                        os.remove(self.session_path)
                    return 

                if is_auth:
                    logger.info("  ➜ [频道监听] 服务已启动，开始监听频道消息...")
                    await self.client.run_until_disconnected()
                else:
                    logger.info("  ➜ [频道监听] Telegram 客户端已连接，等待前端输入验证码授权...")
                    while self.is_running:
                        if await self.client.is_user_authorized():
                            logger.info("  ➜ [频道监听] 授权成功，开始监听频道消息...")
                            await self.client.run_until_disconnected()
                            break
                        await asyncio.sleep(2)

            self.loop.run_until_complete(_daemon())
            
        except Exception as e:
            logger.error(f"  ➜ [频道监听] 运行异常: {e}", exc_info=True)
        finally:
            self.is_running = False
            self.loop.close()

    async def _handle_message(self, event):
        """处理收到的消息 (在 asyncio 线程中运行)"""
        cfg = self._get_config()
        
        raw_channels = cfg.get('channels') or []
        monitor_channels = [c.replace('@', '').strip().lower() for c in raw_channels if c and c.strip()]
        
        if not monitor_channels:
            return

        chat = await event.get_chat()
        chat_username = getattr(chat, 'username', '') or ''
        chat_id = str(getattr(chat, 'id', ''))

        # 白名单匹配逻辑
        matched = False
        for c in monitor_channels:
            c_clean = c.replace('-100', '') if c.startswith('-100') else c
            chat_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id
            if chat_username.lower() == c_clean or chat_id == c or chat_id_clean == c_clean:
                matched = True
                break

        if not matched:
            return

        text = event.raw_text
        if not text:
            return

        # =================================================================
        # 自定义关键词拦截逻辑 (支持频道隔离)
        # =================================================================
        matched_block_rule = self._match_block_rule(
            text,
            chat_username=chat_username,
            chat_id=chat_id,
            rules=cfg.get('block_keywords', [])
        )
        if matched_block_rule:
            logger.debug(f"  ➜ [频道监听] 消息触发拦截规则 '{matched_block_rule}'，已直接丢弃。")
            return

        # =================================================================
        # ★ 关键词转存匹配逻辑
        # =================================================================
        transfer_modes = cfg.get('transfer_modes', ['subscribe'])
        is_brainless = 'brainless' in transfer_modes
        is_subscribe = 'subscribe' in transfer_modes
        is_keyword_enabled = 'keyword' in transfer_modes
        is_keyword_matched = False
        
        if is_keyword_enabled:
            transfer_keywords = cfg.get('transfer_keywords', [])
            for rule_obj in transfer_keywords:
                if isinstance(rule_obj, str):
                    pattern = rule_obj
                    target_channel = ""
                else:
                    pattern = rule_obj.get('pattern', '').strip()
                    target_channel = rule_obj.get('channel', '').strip().lower()

                if not pattern: continue

                if target_channel:
                    target_clean = target_channel.replace('-100', '') if target_channel.startswith('-100') else target_channel
                    curr_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id
                    if not (chat_username.lower() == target_clean or chat_id == target_channel or curr_id_clean == target_clean):
                        continue

                try:
                    if re.search(pattern, text, re.IGNORECASE):
                        is_keyword_matched = True
                        logger.debug(f"  ➜ [频道监听] 消息触发关键词转存规则 '{pattern}'")
                        break
                except Exception as e:
                    logger.error(f"  ➜ [频道监听] 关键词转存正则解析错误 '{pattern}': {e}")

        # =================================================================
        # ★ 辅助正则执行函数 (支持频道隔离)
        # =================================================================
        def _apply_regex(text, custom_rules, default_rules, curr_username, curr_id, flags=re.IGNORECASE):
            applicable_rules = []
            
            # 1. 筛选适用于当前频道的自定义规则
            for rule_obj in (custom_rules or []):
                # 兼容旧版纯字符串配置
                if isinstance(rule_obj, str):
                    applicable_rules.append(rule_obj)
                    continue
                    
                pattern = rule_obj.get('pattern', '').strip()
                target_channel = rule_obj.get('channel', '').strip().lower()
                
                if not pattern: continue
                
                if not target_channel:
                    # 未指定频道，全局生效
                    applicable_rules.append(pattern)
                else:
                    # 指定了频道，进行匹配校验
                    target_clean = target_channel.replace('-100', '') if target_channel.startswith('-100') else target_channel
                    curr_id_clean = curr_id.replace('-100', '') if curr_id.startswith('-100') else curr_id
                    if curr_username.lower() == target_clean or curr_id == target_channel or curr_id_clean == target_clean:
                        applicable_rules.append(pattern)

            # 2. 合并默认规则
            rules = applicable_rules + default_rules
            
            for rule in rules:
                if not rule or not rule.strip(): continue
                try:
                    match = re.search(rule, text, flags)
                    if match: return match
                except Exception as e:
                    logger.error(f"  ➜ [频道监听] 正则执行错误: {rule} -> {e}")
            return None

        custom_regex = cfg.get('custom_regex', {})
        all_urls = self._extract_message_urls(event.message)
        candidate = build_tg_media_candidate(
            text,
            urls=all_urls,
            chat_username=chat_username,
            chat_id=chat_id,
            chat_title=getattr(chat, 'title', '') or chat_username or chat_id,
            message_id=getattr(event.message, 'id', None),
            message_date=(getattr(event.message, 'date', None).strftime('%Y-%m-%d %H:%M') if getattr(event.message, 'date', None) else ''),
            message_link=(f"https://t.me/{chat_username}/{getattr(event.message, 'id', None)}" if chat_username and getattr(event.message, 'id', None) else ''),
            custom_regex=custom_regex,
        )
        if not candidate:
            return

        target_link = candidate.get('target_link')
        receive_code = candidate.get('receive_code', '')
        tmdb_id = candidate.get('tmdb_id')
        title = candidate.get('title')
        year = candidate.get('year')
        season_number = candidate.get('season_number')
        episode_number = candidate.get('episode_number')
        is_pack = bool(candidate.get('is_pack'))
        is_completed_pack = bool(candidate.get('is_completed_pack'))
        item_type = candidate.get('media_type') or candidate.get('item_type') or 'movie'

        allowed_types = cfg.get('monitor_types', ['movie', 'tv'])
        
        # ★ 如果既不是无脑转存，也没命中关键词，且类型不符，则丢弃
        if item_type not in allowed_types and not is_brainless and not is_keyword_matched:
            return

        magnet_url = candidate.get('magnet_url')

        # =================================================================
        # ★ 核心分流逻辑 (统一合并到复杂校验流水线)
        # =================================================================
        if (target_link or magnet_url) and (tmdb_id or title or is_brainless or is_keyword_matched):
            logger.debug(f"  ➜ [频道监听] 监听到频道资源 -> 标题: {title or '未知'}, TMDB: {tmdb_id or '缺失'} (S{season_number}E{episode_number}), 判定类型: {'剧集' if item_type=='tv' else '电影'}, 准备推入处理队列...")
            
            tg_task_queue.put(
                build_channel_task_payload(
                    candidate,
                    is_brainless=is_brainless,
                    is_keyword_matched=is_keyword_matched,
                    is_subscribe=is_subscribe,
                )
            )


    # ==========================================
    # 频道历史搜索能力：供 Telegram 手动资源搜索复用
    # ==========================================
    @staticmethod
    def _clean_channel_key(value):
        value = str(value or '').strip()
        if not value:
            return ''
        value = value.replace('https://t.me/', '').replace('http://t.me/', '')
        value = value.split('/')[0]
        value = value.lstrip('@').strip().lower()
        return value

    @staticmethod
    def _normalize_text(text):
        return re.sub(r'\s+', ' ', str(text or '')).strip()

    def _apply_search_regex(self, text, custom_rules, default_rules, curr_username='', curr_id='', flags=re.IGNORECASE):
        """频道历史搜索使用的正则执行器，兼容已有“频道隔离”规则。"""
        applicable_rules = []
        curr_username = str(curr_username or '').lower()
        curr_id = str(curr_id or '')
        curr_id_clean = curr_id.replace('-100', '') if curr_id.startswith('-100') else curr_id

        for rule_obj in (custom_rules or []):
            if isinstance(rule_obj, str):
                pattern = rule_obj.strip()
                target_channel = ''
            else:
                pattern = str(rule_obj.get('pattern', '')).strip()
                target_channel = str(rule_obj.get('channel', '')).strip().lower()

            if not pattern:
                continue

            if not target_channel:
                applicable_rules.append(pattern)
                continue

            target_clean = target_channel.lstrip('@')
            target_clean = target_clean.replace('-100', '') if target_clean.startswith('-100') else target_clean
            if curr_username == target_clean or curr_id == target_channel or curr_id_clean == target_clean:
                applicable_rules.append(pattern)

        for rule in applicable_rules + (default_rules or []):
            if not rule or not str(rule).strip():
                continue
            try:
                match = re.search(rule, text, flags)
                if match:
                    return match
            except Exception as e:
                logger.error(f"  ➜ [频道搜索] 正则执行错误: {rule} -> {e}")
        return None

    def _extract_message_urls(self, message):
        """提取正文、隐藏链接和按钮链接。"""
        text = getattr(message, 'raw_text', None) or getattr(message, 'message', '') or ''
        urls = []

        # 正文里的裸链
        for match in re.finditer(r'https?://[^\s\])}>"\']+', text, re.IGNORECASE):
            urls.append(match.group(0).strip())

        # Markdown/HTML 隐藏链接
        entities = getattr(message, 'entities', None) or []
        for entity in entities:
            url = getattr(entity, 'url', None)
            if url:
                urls.append(url)

        # Inline Keyboard 按钮链接
        reply_markup = getattr(message, 'reply_markup', None)
        if reply_markup and hasattr(reply_markup, 'rows'):
            for row in reply_markup.rows:
                for button in getattr(row, 'buttons', []) or []:
                    url = getattr(button, 'url', None)
                    if url:
                        urls.append(url)

        # 去重保序
        seen = set()
        deduped = []
        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append(url)
        return deduped

    @staticmethod
    def _guess_size_text(text):
        match = re.search(r'(\d+(?:\.\d+)?)\s*(TB|GB|G|MB|M)\b', text, re.IGNORECASE)
        if not match:
            return ''
        value, unit = match.group(1), match.group(2).upper()
        if unit == 'G':
            unit = 'GB'
        elif unit == 'M':
            unit = 'MB'
        return f"{value}{unit}"

    @staticmethod
    def _guess_resolution(text):
        upper = str(text or '').upper()
        for token in ('8K', '4K', '2160P', '1080P', '720P'):
            if token in upper:
                return '4K' if token == '2160P' else token
        return ''

    @staticmethod
    def _guess_quality_text(text):
        lines = [re.sub(r'\s+', ' ', line).strip() for line in str(text or '').splitlines() if line.strip()]
        quality_words = ('WEB-DL', 'WEBRIP', 'BLURAY', 'REMUX', 'HDR', 'DV', 'DDP', 'HEVC', 'H265', 'H.265', 'X265', 'X264', '内嵌', '外挂', '中字', '简中', '繁中')
        for line in lines:
            upper = line.upper()
            if any(word.upper() in upper for word in quality_words):
                return line[:120]
        return lines[0][:120] if lines else ''

    @staticmethod
    def _guess_title_from_text(text):
        for line in str(text or '').splitlines():
            line = re.sub(r'[#*_`>\[\]【】]+', ' ', line).strip()
            line = re.sub(r'^[^\w\u4e00-\u9fa5]+', '', line).strip()
            if line:
                return line[:80]
        return ''


    @staticmethod
    def _normalize_title_for_match(value):
        text = str(value or '').lower()
        text = re.sub(r'[\s\-_·.．・:：,，;；!！?？()\[\]【】{}<>《》"“”\'’‘`~～/\\|]+', '', text)
        return text

    @staticmethod
    def _extract_explicit_tmdb_id(text):
        """从频道正文中提取明确标注的 TMDb ID。

        支持常见格式：
        - TMDB ID: 12345 / TMDb：12345 / TMDBID 12345
        - {tmdb-12345}
        - tmdb-12345
        """
        text = str(text or '')
        patterns = [
            r'\bTMDB\s*(?:ID|Id|id)?\s*[:：#-]?\s*(\d{2,10})\b',
            r'\{\s*tmdb-(\d{2,10})\s*\}',
            r'\btmdb-(\d{2,10})\b',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _extract_year_candidates(text):
        """提取频道消息中的可能年份。只取 1900-2099，避免把大小/集数当年份。"""
        years = set()
        for match in re.finditer(r'(?<!\d)((?:19|20)\d{2})(?!\d)', str(text or '')):
            try:
                years.add(int(match.group(1)))
            except Exception:
                pass
        return years

    @classmethod
    def _channel_text_matches_year(cls, text, expected_year):
        expected_year = str(expected_year or '').strip()
        if not expected_year:
            return True
        try:
            expected = int(expected_year[:4])
        except Exception:
            return True
        years = cls._extract_year_candidates(text)
        # 兜底逻辑要求“片名 + 年份”：没有年份也视为不匹配。
        return expected in years

    @staticmethod
    def _channel_title_candidate_lines(text):
        """提取更像标题的行，避免演员/简介/标签里的关键词造成串台。"""
        candidates = []
        text = str(text or '')
        lines = [re.sub(r'\s+', ' ', line).strip() for line in text.splitlines() if line.strip()]

        title_patterns = [
            r'(?:^|[\[【📺🎬🎥🎞️ ]+)(?:电影|影片|剧集|电视剧|番剧|动漫|片名|标题|名称)\s*[:：]\s*(.+)$',
            r'^[\[【]([^\]】]{2,80})[\]】]',
        ]
        for line in lines[:12]:
            clean = line.strip()
            for pattern in title_patterns:
                match = re.search(pattern, clean, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    value = re.split(r'\s+(?:TMDB|评分|类型|分类|大小|质量|主演|标签)\s*[:：]', value, 1, flags=re.IGNORECASE)[0].strip()
                    if value:
                        candidates.append(value)

        # 兜底：前几行里不像元数据字段的行。
        meta_prefix = re.compile(
            r'^(?:⭐|🌟|🏷|📁|💾|🎬|👥|🎭|🌏|🗣|📺|🔥|📎|🔗|简介|分享|标签|分类|类型|评分|主演|大小|质量|版本|语言|地区|字幕|链接|公映|投稿|搜索|机场)\s*[:：]',
            re.IGNORECASE
        )
        for line in lines[:6]:
            clean = re.sub(r'[#*_`>]+', ' ', line).strip()
            if not clean or meta_prefix.search(clean):
                continue
            if re.search(r'https?://|TMDB\s*ID|#\w+', clean, re.IGNORECASE):
                continue
            if len(clean) <= 100:
                candidates.append(clean)

        # 去重保序。
        seen = set()
        result = []
        for item in candidates:
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
        return result

    @classmethod
    def _channel_text_matches_query_title(cls, text, query):
        """频道历史搜索的标题校验。

        这里只校验“标题候选行”，不再拿全文匹配，避免演员、简介、标签中出现搜索词导致串台。
        TMDb ID 的优先判定在 _extract_channel_resource_candidate() 里处理；本函数只服务于
        “没有 TMDb ID 时的片名兜底”。
        """
        query = str(query or '').strip()
        if not query:
            return True

        normalized_query = cls._normalize_title_for_match(query)
        if not normalized_query:
            return True

        candidates = cls._channel_title_candidate_lines(text)
        if not candidates:
            return False

        # 中文/无空格标题：要求完整片名出现在标题候选里，或标题候选完整出现在片名中。
        if len(normalized_query) >= 3:
            for cand in candidates:
                normalized_cand = cls._normalize_title_for_match(cand)
                if normalized_query in normalized_cand or normalized_cand in normalized_query:
                    return True

        # 英文/混合标题：主要词在标题候选里大部分命中。
        words = [
            w.lower()
            for w in re.findall(r'[A-Za-z0-9]+|[\u4e00-\u9fa5]{2,}', query)
            if len(w.strip()) >= 2
        ]
        if not words:
            return False

        candidate_blob = ' '.join(candidates).lower()
        normalized_blob = cls._normalize_title_for_match(candidate_blob)
        hit = 0
        for word in words:
            if cls._normalize_title_for_match(word) in normalized_blob or word in candidate_blob:
                hit += 1

        required = len(words) if len(words) <= 2 else max(2, int(len(words) * 0.7))
        return hit >= required

    def _extract_channel_resource_candidate(self, message, chat, query='', expected_tmdb_id=None, expected_year=None, expected_media_type=None, strict_title_match=False):
        """把频道消息解析成可展示、可转存的资源候选。"""
        text = getattr(message, 'raw_text', None) or getattr(message, 'message', '') or ''
        if not text:
            return None

        cfg = self._get_config()
        custom_regex = cfg.get('custom_regex', {}) or {}
        urls = self._extract_message_urls(message)

        chat_username = getattr(chat, 'username', '') or ''
        chat_id = str(getattr(chat, 'id', ''))
        chat_title = getattr(chat, 'title', '') or chat_username or chat_id
        msg_id = getattr(message, 'id', None)
        date_obj = getattr(message, 'date', None)
        date_text = date_obj.strftime('%Y-%m-%d %H:%M') if date_obj else ''
        message_link = ''
        if chat_username and msg_id:
            message_link = f"https://t.me/{chat_username}/{msg_id}"
        return build_tg_media_candidate(
            text,
            urls=urls,
            chat_username=chat_username,
            chat_id=chat_id,
            chat_title=chat_title,
            message_id=msg_id,
            message_date=date_text,
            message_link=message_link,
            custom_regex=custom_regex,
            query=query,
            expected_tmdb_id=expected_tmdb_id,
            expected_year=expected_year,
            expected_media_type=expected_media_type,
            strict_title_match=strict_title_match,
        )

    async def _search_channel_resources_async(self, query, media_type=None, tmdb_id=None, year=None, limit=10, extra_queries=None, include_tmdb_query=False, strict_title_match=False):
        """在已配置监听频道的历史消息里搜索资源。"""
        cfg = self._get_config()
        raw_channels = cfg.get('channels') or []
        monitor_channels = [c for c in raw_channels if c and str(c).strip()]
        if not monitor_channels:
            return {'ok': False, 'error': '未配置频道监听列表', 'results': []}

        if not self.client:
            return {'ok': False, 'error': 'UserBot 客户端未启动', 'results': []}

        if not self.client.is_connected():
            await self.client.connect()

        if not await self.client.is_user_authorized():
            return {'ok': False, 'error': 'UserBot 尚未完成 Telegram 授权', 'results': []}

        search_queries = []
        for q in [query] + (extra_queries or []):
            q = str(q or '').strip()
            if q and q not in search_queries:
                search_queries.append(q)
        # 搜索阶段始终按片名/片名+年份/片名+季号搜索，避免漏掉未标 TMDb ID 的频道资源。
        # TMDb ID 只用于返回结果的优先判定，不再作为搜索关键词。
        if not search_queries:
            return {'ok': False, 'error': '搜索关键词为空', 'results': []}

        results = []
        seen = set()
        errors = []

        for channel in monitor_channels:
            channel_key = self._clean_channel_key(channel)
            if not channel_key:
                continue

            entity = None
            resolve_candidates = []
            if re.fullmatch(r'-?\d+', channel_key):
                resolve_candidates.extend([int(channel_key), int(channel_key.replace('-100', '') if channel_key.startswith('-100') else channel_key)])
            else:
                resolve_candidates.extend([channel_key, '@' + channel_key])

            for candidate in resolve_candidates:
                try:
                    entity = await self.client.get_entity(candidate)
                    break
                except Exception:
                    continue

            if not entity:
                errors.append(f"{channel}: 无法解析频道")
                continue

            for q in search_queries:
                try:
                    async for message in self.client.iter_messages(entity, search=q, limit=max(limit * 5, 30)):
                        candidate = self._extract_channel_resource_candidate(
                            message,
                            entity,
                            query=query,
                            expected_tmdb_id=tmdb_id,
                            expected_year=year,
                            expected_media_type=media_type,
                            strict_title_match=strict_title_match,
                        )
                        if not candidate:
                            continue

                        # 不再用 115 链接/磁力链接做全局去重。
                        # 同一资源经常会被热更频道、完结频道、转发频道重复发布，链接可能完全相同。
                        # 如果按 target_link 去重，排在后面的频道（例如完结频道）会被隐藏，导致“搜不彻底”。
                        # 这里只去重同一频道同一条消息，避免同一消息被多个 search query 重复命中。
                        msg_id = candidate.get('message_id')
                        source_key = candidate.get('source_username') or candidate.get('source_chat_id') or candidate.get('source_channel') or ''
                        if msg_id:
                            dedup_key = f"{source_key}:{msg_id}"
                        else:
                            dedup_key = f"{source_key}:{candidate.get('target_link') or candidate.get('magnet_url') or candidate.get('title')}"
                        if dedup_key in seen:
                            continue
                        seen.add(dedup_key)
                        results.append(candidate)
                        if len(results) >= limit:
                            return {'ok': True, 'results': results, 'errors': errors}
                except Exception as e:
                    errors.append(f"{channel}: {e}")
                    logger.warning(f"  ➜ [频道搜索] 搜索频道 {channel} 失败: {e}")

        return {'ok': True, 'results': results[:limit], 'errors': errors}

    def search_channel_resources(self, query, media_type=None, tmdb_id=None, year=None, limit=10, extra_queries=None, timeout=30, include_tmdb_query=False, strict_title_match=True):
        """线程安全包装：供 handler.telegram 的同步线程调用。"""
        if not self.is_running or not self.loop or not self.client:
            return {'ok': False, 'error': '频道监听未启动。请先启用并完成 UserBot 授权。', 'results': []}

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._search_channel_resources_async(
                    query=query,
                    media_type=media_type,
                    tmdb_id=tmdb_id,
                    year=year,
                    limit=limit,
                    extra_queries=extra_queries,
                    include_tmdb_query=include_tmdb_query,
                    strict_title_match=strict_title_match,
                ),
                self.loop,
            )
            return future.result(timeout=timeout)
        except Exception as e:
            logger.error(f"  ➜ [频道搜索] 执行频道历史搜索失败: {e}", exc_info=True)
            return {'ok': False, 'error': str(e), 'results': []}

    # ==========================================
    # 以下是供前端 API 调用的登录交互方法 (保持不变)
    # ==========================================
    def get_status(self):
        if not self.client or not self.loop:
            return {"status": "stopped", "msg": "服务未启动"}
        
        future = asyncio.run_coroutine_threadsafe(self.client.is_user_authorized(), self.loop)
        try:
            is_auth = future.result(timeout=5)
            return {"status": "authorized" if is_auth else "unauthorized"}
        except:
            return {"status": "error", "msg": "获取状态超时"}

    def send_login_code(self):
        cfg = self._get_config()
        phone = cfg['phone'].strip() if cfg['phone'] else ''
        if not phone: 
            raise Exception("未配置手机号")
        if not phone.startswith('+'):
            raise Exception("手机号格式错误，必须以 '+' 号开头，例如: +8613800138000")
        
        if not self.is_running or not self.loop or not self.client:
            logger.info("  ➜ [频道监听] 正在唤醒后台服务以发送验证码...")
            self.start()
            import time
            time.sleep(2.5) 
            
        if not self.loop or not self.client:
            raise Exception("UserBot 服务启动失败，请检查 API ID 和 Hash 是否正确")

        async def _send():
            try:
                logger.info(f"  ➜ [频道监听] 正在向 TG 服务器请求发送验证码至 {phone}...")
                if not self.client.is_connected(): 
                    await self.client.connect()
                res = await self.client.send_code_request(phone)
                self.phone_code_hash = res.phone_code_hash
                logger.info("  ➜ [频道监听] 验证码发送请求已成功响应！")
                return True
            except Exception as e:
                logger.error(f"  ➜ [频道监听] 发送验证码被 TG 拒绝: {e}")
                raise e
            
        future = asyncio.run_coroutine_threadsafe(_send(), self.loop)
        
        try:
            return future.result(timeout=20)
        except TimeoutError:
            logger.warning("  ➜ [频道监听] 请求验证码超时！但后台仍在尝试发送。")
            return True

    def submit_login_code(self, code):
        cfg = self._get_config()
        async def _submit():
            try:
                logger.info("  ➜ [频道监听] 正在向 TG 服务器提交验证码...")
                await self.client.sign_in(cfg['phone'], code, phone_code_hash=self.phone_code_hash)
                logger.info("  ➜ [频道监听] 验证码校验通过！")
                return {"success": True}
            except SessionPasswordNeededError:
                if not cfg['password']:
                    return {"success": False, "need_2fa": True, "msg": "需要两步验证密码，请在配置中填写后重试"}
                logger.info("  ➜ [频道监听] 正在提交两步验证密码...")
                await self.client.sign_in(password=cfg['password'])
                logger.info("  ➜ [频道监听] 两步验证密码校验通过！")
                return {"success": True}
            except Exception as e:
                logger.error(f"  ➜ [频道监听] 提交验证码失败: {e}")
                raise e
                
        future = asyncio.run_coroutine_threadsafe(_submit(), self.loop)
        return future.result(timeout=60)

    def logout(self):
        async def _logout():
            await self.client.log_out()
        if self.client and self.loop:
            asyncio.run_coroutine_threadsafe(_logout(), self.loop).result(timeout=5)
        if os.path.exists(self.session_path):
            os.remove(self.session_path)

    def clear_session_files(self):
        """停止 UserBot 并删除本地 Telegram 登录会话文件。"""
        self.stop()
        deleted = []
        missing = []
        for path in (self.session_path, self.session_journal_path):
            name = os.path.basename(path)
            if os.path.exists(path):
                os.remove(path)
                deleted.append(name)
            else:
                missing.append(name)
        return {"deleted": deleted, "missing": missing}


# =================================================================
# ★★★ ETK 侧的消费者协程 (处理队列中的任务) (保持不变)
# =================================================================
_TG_OFFLINE_LINK_RE = re.compile(
    r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)',
    re.IGNORECASE | re.DOTALL,
)


def _extract_tg_offline_links(*values):
    text_parts = []
    for value in values:
        if isinstance(value, list):
            text_parts.extend(str(item or '') for item in value)
        else:
            text_parts.append(str(value or ''))

    links = []
    seen = set()
    for link in _TG_OFFLINE_LINK_RE.findall("\n".join(text_parts)):
        link = str(link or '').strip()
        key = link.lower()
        if link and key not in seen:
            seen.add(key)
            links.append(link)
    return links


def _submit_tg_offline_links(client, links, target_cid, task, log_prefix):
    links = [str(link or '').strip() for link in (links or []) if str(link or '').strip()]
    if not links:
        return False

    payload = {"wp_path_id": target_cid}
    for index, link in enumerate(links):
        payload[f"url[{index}]"] = link

    res = client.offline_add_urls(payload)
    if res and res.get('state'):
        logger.info(f"  ➜ {log_prefix} 离线下载任务提交成功！共 {len(links)} 个链接，正在触发整理...")

        notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        if 'transfer_success' in notify_types:
            try:
                from handler.telegram import send_transfer_success_notification
                task['is_offline'] = True
                task['magnet_url'] = links[0]
                send_transfer_success_notification(task)
            except Exception as e:
                logger.error(f"  ➜ {log_prefix} 发送离线通知失败: {e}")

        try:
            import task_manager
            import threading
            threading.Timer(10.0, task_manager.trigger_115_organize_task).start()
        except Exception:
            pass
        return True

    err = (res or {}).get('error_msg') or (res or {}).get('message') or str(res) or '未知错误'
    logger.error(f"  ➜ {log_prefix} 离线提交失败: {err}")
    return False


def _process_tg_queue():
    import requests 

    while True:
        try:
            task = tg_task_queue.get() 
            task_type = task.get('type')
            
            client = P115Service.get_client()
            target_cid = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')
            
            if not client:
                logger.error("  ➜ [频道监听] 115 客户端未初始化，无法执行任务。")
                continue

            if task_type == "channel_resource_complex":
                candidate = task.get('candidate') or {}
                tmdb_id = task.get('tmdb_id')
                title = task.get('title')
                year = task.get('year')
                target_link = task.get('target_link')
                receive_code = task.get('receive_code', '')
                season_number = task.get('season_number')
                episode_number = task.get('episode_number')
                is_pack = task.get('is_pack', False) 
                is_brainless = task.get('is_brainless', False) 
                is_keyword_matched = task.get('is_keyword_matched', False)
                is_subscribe = task.get('is_subscribe', True)

                item_type = task.get('item_type', 'movie')
                candidate_hints = candidate_to_recognition_hints(candidate) if candidate else {}
                if candidate_hints:
                    remember_candidate_hint(candidate_hints)
                    tmdb_id = tmdb_id or candidate_hints.get('tmdb_id')
                    title = candidate_hints.get('identify_title') or candidate_hints.get('clean_title') or title
                    year = year or candidate_hints.get('year')
                    item_type = candidate_hints.get('media_type') or item_type
                    if season_number is None:
                        season_number = candidate_hints.get('season_number')
                    if episode_number is None:
                        episode_number = candidate_hints.get('episode_number')
                    task['tmdb_id'] = tmdb_id
                    task['title'] = title
                    task['year'] = year
                    task['item_type'] = item_type
                    task['season_number'] = season_number
                    task['episode_number'] = episode_number
                    task['is_special'] = bool(task.get('is_special') or candidate_hints.get('is_special'))

                if not tmdb_id and title:
                    logger.debug(f"  ➜ [频道监听] 缺失 TMDB ID，正在通过 TMDb 接口反查: {title} ({year}), 严格限定类型: {'剧集' if item_type=='tv' else '电影'}...")
                    from handler import tmdb
                    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                    results = tmdb.search_media(title, api_key, item_type=item_type, year=year)
                    if results:
                        tmdb_id = str(results[0]['id'])
                        task['tmdb_id'] = tmdb_id 
                        logger.debug(f"  ➜ [频道监听] 反查成功！精准匹配到 TMDB ID: {tmdb_id}")
                    else:
                        if is_brainless or is_keyword_matched:
                            logger.warning(f"  ➜ [频道监听] 反查失败，但当前为【无脑/关键词转存】模式，强制放行！")
                        else:
                            logger.warning(f"  ➜ [频道监听] 反查失败，TMDb 未找到该{'剧集' if item_type=='tv' else '电影'}，任务终止。")
                            continue

                if not tmdb_id and not (is_brainless or is_keyword_matched): continue 

                should_process = is_brainless or is_keyword_matched
                # 如果没命中无脑/关键词，且开启了订阅转存，才去查库
                if not should_process and is_subscribe:
                    try:
                        with get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute("SELECT subscription_status FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Movie'", (tmdb_id,))
                                row = cursor.fetchone()
                                if row and row['subscription_status'] in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED']:
                                    should_process = True
                                
                                if not should_process:
                                    cursor.execute("""
                                        SELECT subscription_status 
                                        FROM media_metadata 
                                        WHERE (tmdb_id = %s OR parent_series_tmdb_id = %s) 
                                        AND item_type = 'Season'
                                    """, (tmdb_id, tmdb_id))
                                    rows = cursor.fetchall()
                                    for r in rows:
                                        if r['subscription_status'] in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED']:
                                            should_process = True
                                            break
                                
                                if not should_process:
                                    cursor.execute("SELECT watching_status, waiting_for_completed_pack FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
                                    row = cursor.fetchone()
                                    if row:
                                        status = row['watching_status']
                                        is_waiting = row.get('waiting_for_completed_pack', False)

                                        if status in ['Watching', 'Paused', 'Pending']:
                                            should_process = True
                                        
                                        elif status == 'Completed' and task.get('is_completed_pack'):
                                            if is_waiting:
                                                should_process = True
                                                logger.info(f"  ➜ [TG洗版特权] 识别到完结包，且剧集正在等待洗版，特权放行！(TMDB: {tmdb_id})")
                                                
                                                cursor.execute("""
                                                    UPDATE media_metadata 
                                                    SET waiting_for_completed_pack = FALSE,
                                                        active_washing = TRUE
                                                    WHERE tmdb_id = %s AND item_type = 'Series'
                                                """, (tmdb_id,))
                                                conn.commit()
                                            else:
                                                logger.info(f"  ➜ [TG洗版拦截] 识别到完结包，但该剧集不需要洗版 (标志位为False)，已忽略。")
                    except Exception as e:
                        logger.error(f"  ➜ [频道监听] 查库失败: {e}")
                        continue

                if not should_process:
                    logger.debug(f"  ➜ [频道监听] 资源 (TMDB: {tmdb_id}) 不在已订阅/追剧列表中，已忽略。")
                    continue

                if not (is_brainless or is_keyword_matched) and season_number is not None and episode_number is not None: 
                    from database import media_db
                    local_seasons = media_db.get_series_local_children_info(tmdb_id)
                    if task.get('is_completed_pack'):
                        logger.info(f"  ➜ [频道监听] 这是一个完结洗版包，无视本地已有集数，强制放行转存！")
                    elif is_pack:
                        local_ep_count = len(local_seasons.get(season_number, []))
                        if local_ep_count >= episode_number:
                            logger.debug(f"  ➜ [频道监听] 合集包 (TMDB: {tmdb_id} S{season_number:02d} 宣称 {episode_number} 集)，本地已有 {local_ep_count} 集，判定为已满足，跳过转存！")
                            continue
                        else:
                            logger.info(f"  ➜ [频道监听] 发现合集包 (S{season_number:02d} 宣称 {episode_number} 集)，本地仅有 {local_ep_count} 集，放行转存以补齐缺集！")
                    else:
                        if season_number in local_seasons and episode_number in local_seasons[season_number]:
                            logger.debug(f"  ➜ [频道监听] 单集资源 (TMDB: {tmdb_id} S{season_number:02d}E{episode_number:02d}) 本地已存在，跳过转存！")
                            continue

                share_code = None
                
                target_link = task.get('target_link')
                magnet_url = task.get('magnet_url')

                # --- 分支 A: 处理 115 分享链接 ---
                if target_link:
                    share_code = None
                    if 'hdhive.com' in target_link:
                        logger.debug(f"  ➜ [频道监听] 检测到 HDHive 资源链接，准备通过官方 API 获取真实地址 (将扣除积分)...")
                        try:
                            slug_match = re.search(r'hdhive\.com/resource/(?:115|magnet|ed2k|bt)?/?([a-fA-F0-9-]{32,36})', target_link)
                            if not slug_match:
                                logger.error(f"  ➜ [频道监听] 无法从影巢链接中提取 Slug 标识: {target_link}")
                                continue
                                
                            slug = slug_match.group(1)

                            # 新版 HDHive OpenAPI 已废弃个人 API Key 模式。
                            # 频道监听这里不能再读取 hdhive_config.api_key，必须复用 ETK 统一的 OAuth Relay 授权。
                            from handler.hdhive_client import HDHiveClient
                            hd_client = HDHiveClient()

                            if not hd_client.ping():
                                logger.error("  ➜ [频道监听] 解析影巢链接失败：尚未完成影巢授权，请先在影巢配置页完成授权。")
                                continue

                            resource_data = hd_client.unlock_resource(slug)
                            
                            if resource_data and resource_data.get('url'):
                                real_url = resource_data.get('url')
                                full_url = resource_data.get('full_url', '')

                                offline_links = _extract_tg_offline_links(real_url, full_url)
                                if offline_links:
                                    logger.debug(f"  ➜ [频道监听] 影巢 API 返回 {len(offline_links)} 个磁力/ED2K 链接，准备提交离线下载...")
                                    _submit_tg_offline_links(client, offline_links, target_cid, task, "[频道监听]")
                                    continue
                                
                                match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', real_url)
                                if match:
                                    share_code = match.group(1)
                                    
                                    if resource_data.get('access_code'):
                                        receive_code = resource_data.get('access_code')
                                    if not receive_code:
                                        pwd_match = re.search(r'(?:pwd|password|code)=([a-zA-Z0-9]+)', full_url + "&" + real_url, re.IGNORECASE)
                                        if pwd_match:
                                            receive_code = pwd_match.group(1)

                                    if candidate_hints:
                                        candidate_hints = dict(candidate_hints)
                                        candidate_hints['target_link'] = real_url
                                        candidate_hints['receive_code'] = receive_code or candidate_hints.get('receive_code')
                                        remember_candidate_hint(candidate_hints)

                                    if candidate:
                                        candidate['target_link'] = real_url
                                        candidate['receive_code'] = receive_code or candidate.get('receive_code')
                                        task['candidate'] = candidate
                                    task['target_link'] = real_url
                                    task['receive_code'] = receive_code or task.get('receive_code', '')
                                            
                                    logger.debug(f"  ➜ [频道监听] 影巢 API 解析成功！真实 Share Code: {share_code}, 密码: {receive_code or '无'}")
                                else:
                                    logger.error(f"  ➜ [频道监听] 影巢 API 返回的 URL 中未找到 115 提取码: {real_url}")
                                    continue
                            else:
                                logger.error("  ➜ [频道监听] 影巢 API 未返回真实的 115 链接，可能是积分不足或资源已失效。")
                                continue
                                
                        except Exception as e:
                            logger.error(f"  ➜ [频道监听] 请求影巢 API 异常: {e}")
                            continue
                    else:
                        match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', target_link)
                        if match: share_code = match.group(1)

                    if not share_code:
                        logger.error("  ➜ [频道监听] 无法获取有效的 115 Share Code，任务终止。")
                        continue

                    logger.debug(f"  ➜ [频道监听] 命中订阅资源 (TMDB: {tmdb_id})！准备转存...")
                    
                    res = client.share_import(share_code, receive_code, target_cid)
                    if res and res.get('state'):
                        logger.info(f"  ➜ [频道监听] 资源转存成功！正在触发整理...")
                        receive_title = (res.get('data') or {}).get('receive_title') if isinstance(res, dict) else None
                        transfer_media_type = 'movie' if str(task.get('item_type') or '').lower() == 'movie' else 'tv'
                        if receive_title and task.get('tmdb_id') and task.get('title'):
                            authority_role = candidate_hints.get('authority_role') or 'advisory'
                            transfer_source = candidate_hints.get('source') or 'tg-channel-import'
                            transfer_source_kind = candidate_hints.get('source_kind') or transfer_source
                            P115CacheManager.save_transfer_context(
                                receive_title,
                                task.get('tmdb_id'),
                                transfer_media_type,
                                task.get('title'),
                                season_number=task.get('season_number'),
                                episode_number=task.get('episode_number'),
                                source=transfer_source,
                                source_kind=transfer_source_kind,
                                source_kinds=candidate_hints.get('source_kinds') or [transfer_source_kind],
                                confidence=candidate_hints.get('confidence') or 'high',
                                authority_role=authority_role,
                                identify_title=candidate_hints.get('identify_title') or task.get('title'),
                                clean_title=candidate_hints.get('clean_title') or task.get('title'),
                                matched_rules=candidate_hints.get('matched_rules') or [],
                                evidence=candidate_hints.get('evidence') or [],
                                conflict_reason=candidate_hints.get('conflict_reason') or '',
                                alias_titles=candidate_hints.get('alias_titles') or [],
                                parse_version=candidate_hints.get('parse_version') or '',
                                is_special=task.get('is_special'),
                            )
                        
                        notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
                        if 'transfer_success' in notify_types:
                            try:
                                from handler.telegram import send_transfer_success_notification
                                send_transfer_success_notification(task)
                            except Exception as e:
                                logger.error(f"  ➜ [频道监听] 发送转存通知失败: {e}")

                        try:
                            import task_manager
                            import threading
                            threading.Timer(3.0, task_manager.trigger_115_organize_task).start()
                        except: pass
                    else:
                        err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                        logger.error(f"  ➜ [频道监听] 转存失败: {err}")

                # --- 分支 B: 处理磁力/ED2K 离线下载 ---
                elif magnet_url:
                    logger.debug(f"  ➜ [频道监听] 命中订阅资源 (TMDB: {tmdb_id})！准备提交离线下载...")
                    offline_links = _extract_tg_offline_links(magnet_url)
                    if not offline_links:
                        offline_links = [magnet_url]
                    _submit_tg_offline_links(client, offline_links, target_cid, task, "[频道监听]")

            elif task_type == "offline_download":
                target_url = task['url']
                payload = {"url[0]": target_url, "wp_path_id": target_cid}
                
                res = client.offline_add_urls(payload)
                if res and res.get('state'):
                    logger.info(f"  ➜ [TG订阅] 离线下载任务提交成功！")
                    try:
                        import task_manager
                        import threading
                        threading.Timer(10.0, task_manager.trigger_115_organize_task).start()
                    except: pass
                else:
                    err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                    logger.error(f"  ➜ [频道监听] 离线提交失败: {err}")

        except Exception as e:
            logger.error(f"  ➜ [频道监听] 队列处理异常: {e}")

spawn(_process_tg_queue)
