# handler/tg_userbot.py
import os
import re
import asyncio
import threading
import queue
import logging
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, AuthKeyUnregisteredError

import config_manager
import constants
from database import settings_db
from handler.p115_service import P115Service
from utils import DEFAULT_TG_REGEX
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
            'block_keywords': cfg.get('block_keywords', [])
        }

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
        block_keywords = cfg.get('block_keywords', [])
        if block_keywords:
            for rule_obj in block_keywords:
                # 兼容旧版纯字符串
                if isinstance(rule_obj, str):
                    pattern = rule_obj
                    target_channel = ""
                else:
                    pattern = rule_obj.get('pattern', '').strip()
                    target_channel = rule_obj.get('channel', '').strip().lower()

                if not pattern: continue

                # 校验频道归属
                if target_channel:
                    target_clean = target_channel.replace('-100', '') if target_channel.startswith('-100') else target_channel
                    curr_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id
                    if not (chat_username.lower() == target_clean or chat_id == target_channel or curr_id_clean == target_clean):
                        continue # 频道不匹配，跳过此条拦截规则

                try:
                    if re.search(pattern, text, re.IGNORECASE):
                        logger.debug(f"  ➜ [频道监听] 消息触发拦截规则 '{pattern}'，已直接丢弃。")
                        return
                except Exception as e:
                    logger.error(f"  ➜ [频道监听] 拦截规则正则解析错误 '{pattern}': {e}")

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
        all_urls = []

        # 1. 提取 Markdown/HTML 隐藏的超链接
        if event.message.entities:
            for entity in event.message.entities:
                if hasattr(entity, 'url') and entity.url:
                    all_urls.append(entity.url)

        # 2. 提取底部内联键盘 (Inline Keyboard) 的按钮链接
        if event.message.reply_markup and hasattr(event.message.reply_markup, 'rows'):
            for row in event.message.reply_markup.rows:
                for button in row.buttons:
                    if hasattr(button, 'url') and button.url:
                        all_urls.append(button.url)

        # 3. 寻找目标链接 (115 或 中间页) 和 提取码
        target_link = None
        receive_code = ""

        link_match = re.search(r'(https?://(?:115cdn|115)\.com/s/[a-zA-Z0-9]+(?:[?&]password=[a-zA-Z0-9]+)?)', text, re.IGNORECASE)
        if link_match:
            all_urls.insert(0, link_match.group(1))

        for url in all_urls:
            if '115.com/s/' in url or '115cdn.com/s/' in url or 'hdhive.com/resource/115/' in url:
                target_link = url
                pwd_in_url = _apply_regex(url, custom_regex.get('password', []), DEFAULT_TG_REGEX['password_url'], chat_username, chat_id)
                if pwd_in_url:
                    receive_code = pwd_in_url.group(1)
                break

        # 4. 如果 URL 里没有密码，再从正文里找
        if not receive_code:
            pwd_match = _apply_regex(text, custom_regex.get('password', []), DEFAULT_TG_REGEX['password_text'], chat_username, chat_id)
            if pwd_match:
                receive_code = pwd_match.group(1)

        # 5. 提取 TMDB ID
        tmdb_id = None
        tmdb_match = _apply_regex(text, custom_regex.get('tmdb', []), DEFAULT_TG_REGEX['tmdb'], chat_username, chat_id)
        if tmdb_match:
            tmdb_id = tmdb_match.group(1)

        # 6. 提取标题和年份
        title = None
        year = None
        title_match = _apply_regex(text, custom_regex.get('title_year', []), DEFAULT_TG_REGEX['title_year'], chat_username, chat_id, flags=0)
        if title_match:
            title = title_match.group(1).strip()
            title = re.sub(r'^\[.*?\]\s*', '', title).strip()
            title = re.sub(r'^[^\w\u4e00-\u9fa5]+', '', title).strip()
            year = title_match.group(2)

        # 7. 提取季号和集号
        season_number = None
        episode_number = None
        is_pack = False 
        is_completed_pack = False 

        if re.search(r'(完结|全\d+集|\d+集全)', text, re.IGNORECASE):
            is_completed_pack = True
            is_pack = True
        
        # ★ 季集自定义正则 (支持频道隔离)
        custom_se = custom_regex.get('season_episode', [])
        se_matched = False
        for rule_obj in custom_se:
            if isinstance(rule_obj, str):
                pattern = rule_obj
                target_channel = ""
            else:
                pattern = rule_obj.get('pattern', '').strip()
                target_channel = rule_obj.get('channel', '').strip().lower()
                
            if not pattern: continue
            
            # 校验频道
            if target_channel:
                target_clean = target_channel.replace('-100', '') if target_channel.startswith('-100') else target_channel
                curr_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id
                if not (chat_username.lower() == target_clean or chat_id == target_channel or curr_id_clean == target_clean):
                    continue
                    
            try:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    if len(groups) >= 2:
                        season_number = int(groups[0])
                        episode_number = int(groups[1])
                    elif len(groups) == 1:
                        episode_number = int(groups[0])
                    se_matched = True
                    break
            except Exception as e:
                logger.error(f"  ➜ [频道监听] 季集自定义正则错误: {pattern} -> {e}")

        if not se_matched:
            range_match = re.search(r'S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})\s*(?:-|~|至)\s*(?:E|EP)?\s*(\d{1,4})', text, re.IGNORECASE)
            if range_match:
                season_number = int(range_match.group(1))
                episode_number = int(range_match.group(3)) 
                is_pack = True
            else:
                se_match = re.search(r'S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})', text, re.IGNORECASE)
                if se_match:
                    season_number = int(se_match.group(1))
                    episode_number = int(se_match.group(2))
                else:
                    s_match = re.search(r'(?:S|Season|第)\s*(\d{1,2})\s*(?:季)?', text, re.IGNORECASE)
                    e_match = re.search(r'(?:E|EP|Episode|第)\s*(\d{1,4})\s*(?:集|话)', text, re.IGNORECASE)
                    if s_match: season_number = int(s_match.group(1))
                    if e_match: episode_number = int(e_match.group(1))
                    
                    if episode_number is None:
                        bulk_match = re.search(r'(?:更新至|全|至)(?:第)?\s*(\d{1,4})\s*(?:集|话)|(?:^|\s)\d{1,3}-(\d{1,4})(?:集|话)?', text)
                        if bulk_match:
                            ep_str = bulk_match.group(1) or bulk_match.group(2)
                            if ep_str:
                                episode_number = int(ep_str)
                                is_pack = True 

        if episode_number is not None and season_number is None:
            season_number = 1

        # 8. 精准判定媒体类型
        item_type = 'movie' 
        
        if re.search(r'(?:📺|🖥️)?\s*(?:电视剧|剧集|动漫|番剧)[:：]', text):
            item_type = 'tv'
        elif re.search(r'(?:🎬|🎥|🎞️)?\s*电影[:：]', text):
            item_type = 'movie'
        elif season_number is not None or episode_number is not None:
            item_type = 'tv'
        else:
            tags = " ".join(re.findall(r'#\w+', text))
            if re.search(r'#(?:电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集|动画)', tags, re.IGNORECASE):
                item_type = 'tv'
            elif re.search(r'#(?:电影|Movie)', tags, re.IGNORECASE):
                item_type = 'movie'
            else:
                header_text = "\n".join(text.split('\n')[:8])
                if re.search(r'(电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集)', header_text, re.IGNORECASE):
                    item_type = 'tv'
                elif re.search(r'(电影|Movie)', header_text, re.IGNORECASE):
                    item_type = 'movie'

        allowed_types = cfg.get('monitor_types', ['movie', 'tv'])
        
        # ★ 如果既不是无脑转存，也没命中关键词，且类型不符，则丢弃
        if item_type not in allowed_types and not is_brainless and not is_keyword_matched:
            return

        is_magnet = text.lower().startswith('magnet:?')
        is_ed2k = text.lower().startswith('ed2k://')
        magnet_ed2k_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)

        # 提取磁力/ED2K
        is_magnet = text.lower().startswith('magnet:?')
        is_ed2k = text.lower().startswith('ed2k://')
        magnet_ed2k_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)
        magnet_url = magnet_ed2k_match.group(1) if magnet_ed2k_match else None
        if not magnet_url and (is_magnet or is_ed2k):
            magnet_url = text.strip()

        # =================================================================
        # ★ 核心分流逻辑 (统一合并到复杂校验流水线)
        # =================================================================
        if (target_link or magnet_url) and (tmdb_id or title or is_brainless or is_keyword_matched):
            logger.debug(f"  ➜ [频道监听] 监听到频道资源 -> 标题: {title or '未知'}, TMDB: {tmdb_id or '缺失'} (S{season_number}E{episode_number}), 判定类型: {'剧集' if item_type=='tv' else '电影'}, 准备推入处理队列...")
            
            tg_task_queue.put({
                "type": "channel_resource_complex",
                "tmdb_id": tmdb_id,
                "title": title,
                "year": year,
                "item_type": item_type,
                "target_link": target_link, # 115 分享链接
                "magnet_url": magnet_url,   # 磁力/ED2K 链接
                "receive_code": receive_code,
                "season_number": season_number,
                "episode_number": episode_number,
                "is_pack": is_pack,
                "is_completed_pack": is_completed_pack,
                "is_brainless": is_brainless,
                "is_keyword_matched": is_keyword_matched,
                "is_subscribe": is_subscribe
            })

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


# =================================================================
# ★★★ ETK 侧的消费者协程 (处理队列中的任务) (保持不变)
# =================================================================
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
                                if row and row['subscription_status'] in ['SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED']:
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
                                        if r['subscription_status'] in ['SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED']:
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
                            slug_match = re.search(r'hdhive\.com/resource/115/([a-fA-F0-9]{32})', target_link)
                            if not slug_match:
                                logger.error(f"  ➜ [频道监听] 无法从影巢链接中提取 Slug 标识: {target_link}")
                                continue
                                
                            slug = slug_match.group(1)
                            from database import settings_db
                            hdhive_api_key = settings_db.get_setting('hdhive_api_key')
                            
                            if not hdhive_api_key:
                                logger.error("  ➜ [频道监听] 解析失败：未配置影巢 API Key！")
                                continue
                                
                            from handler.hdhive_client import HDHiveClient
                            hd_client = HDHiveClient(hdhive_api_key)
                            resource_data = hd_client.unlock_resource(slug)
                            
                            if resource_data and resource_data.get('url'):
                                real_url = resource_data.get('url')
                                full_url = resource_data.get('full_url', '')
                                
                                match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', real_url)
                                if match:
                                    share_code = match.group(1)
                                    
                                    if resource_data.get('access_code'):
                                        receive_code = resource_data.get('access_code')
                                    if not receive_code:
                                        pwd_match = re.search(r'(?:pwd|password|code)=([a-zA-Z0-9]+)', full_url + "&" + real_url, re.IGNORECASE)
                                        if pwd_match:
                                            receive_code = pwd_match.group(1)
                                            
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
                    payload = {"url[0]": magnet_url, "wp_path_id": target_cid}
                    
                    res = client.offline_add_urls(payload)
                    if res and res.get('state'):
                        logger.info(f"  ➜ [频道监听] 离线下载任务提交成功！正在触发整理...")
                        
                        notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
                        if 'transfer_success' in notify_types:
                            try:
                                from handler.telegram import send_transfer_success_notification
                                task['is_offline'] = True # 标记为离线任务，供通知文案区分
                                send_transfer_success_notification(task)
                            except Exception as e:
                                logger.error(f"  ➜ [频道监听] 发送离线通知失败: {e}")

                        try:
                            import task_manager
                            import threading
                            threading.Timer(10.0, task_manager.trigger_115_organize_task).start()
                        except: pass
                    else:
                        err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                        logger.error(f"  ➜ [频道监听] 离线提交失败: {err}")

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