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
from handler.p115_service import P115Service
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
        cfg = config_manager.APP_CONFIG
        return {
            'enabled': cfg.get(constants.CONFIG_OPTION_TG_USER_ENABLED, False),
            'api_id': cfg.get(constants.CONFIG_OPTION_TG_USER_API_ID, ''),
            'api_hash': cfg.get(constants.CONFIG_OPTION_TG_USER_API_HASH, ''),
            'phone': cfg.get(constants.CONFIG_OPTION_TG_USER_PHONE, ''),
            'password': cfg.get(constants.CONFIG_OPTION_TG_USER_2FA, ''),
            'channels': cfg.get(constants.CONFIG_OPTION_TG_MONITOR_CHANNELS) or [],
            'monitor_types': cfg.get(constants.CONFIG_OPTION_TG_MONITOR_TYPE) or ['movie', 'tv']
        }

    def start(self):
        """启动后台线程"""
        cfg = self._get_config()
        if not cfg['enabled'] or not cfg['api_id'] or not cfg['api_hash']:
            if self.is_running:
                logger.info("  ➜ [频道监听] 监听已在配置中关闭，正在停止服务...")
                self.stop()
            return

        # ★ 绝对安全锁：只要线程活着，绝不重复启动，防止 asyncio 炸膛！
        if self.is_running and self.thread and self.thread.is_alive():
            return

        self.stop() # 确保旧的残骸清理干净

        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="TG_UserBot_Thread")
        self.thread.start()

    def stop(self):
        """停止后台线程"""
        self.is_running = False
        if self.client and self.loop and self.loop.is_running():
            try:
                # 优雅地断开连接，设置超时防止卡死
                asyncio.run_coroutine_threadsafe(self.client.disconnect(), self.loop).result(timeout=3)
            except Exception:
                pass
                
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
            
        # ★★★ 核心修复：必须彻底清空旧实例和事件循环！
        # 否则下次 start 时复用旧实例会导致 Event Loop 冲突崩溃
        self.client = None
        self.loop = None

    def _run_loop(self):
        """在独立线程中运行 asyncio 事件循环"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        cfg = self._get_config()
        try:
            # --- 代理设置 ---
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
                    return # 退出当前 daemon，等待前端重新触发 start()

                if is_auth:
                    logger.info("  ➜ [频道监听] 服务已启动，开始监听频道消息...")
                    await self.client.run_until_disconnected()
                else:
                    logger.info("  ➜ [频道监听] Telegram 客户端已连接，等待前端输入验证码授权...")
                    # 保持协程存活，等待前端调用登录接口
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
            # 如果你想知道为什么某个频道没被监听到，可以把下面这行注释打开看日志
            # logger.debug(f"  [UserBot 忽略] 收到消息 -> Username: {chat_username}, ID: {chat_id}")
            return

        text = event.raw_text
        if not text:
            return

        # =================================================================
        # ★ 史诗级增强 2.0：全方位雷达 (提取实体链接 + 按钮链接 + URL密码)
        # =================================================================
        
        all_urls = []

        # 1. 提取 Markdown/HTML 隐藏的超链接
        if event.message.entities:
            for entity in event.message.entities:
                if hasattr(entity, 'url') and entity.url:
                    all_urls.append(entity.url)

        # 2. 提取底部内联键盘 (Inline Keyboard) 的按钮链接 (解决星河频道)
        if event.message.reply_markup and hasattr(event.message.reply_markup, 'rows'):
            for row in event.message.reply_markup.rows:
                for button in row.buttons:
                    if hasattr(button, 'url') and button.url:
                        all_urls.append(button.url)

        # 3. 寻找目标链接 (115 或 中间页) 和 提取码
        target_link = None
        receive_code = ""

        # 先看文本里有没有明文的 115 链接，有的话插到最前面优先处理
        link_match = re.search(r'(https?://(?:115cdn|115)\.com/s/[a-zA-Z0-9]+(?:[?&]password=[a-zA-Z0-9]+)?)', text, re.IGNORECASE)
        if link_match:
            all_urls.insert(0, link_match.group(1))

        # 遍历所有收集到的链接
        for url in all_urls:
            if '115.com/s/' in url or '115cdn.com/s/' in url or 'hdhive.com/resource/115/' in url:
                target_link = url
                # ★ 核心修复：尝试直接从 URL 中提取密码 (解决 ?password=8888 的情况)
                pwd_in_url = re.search(r'(?:password|pwd)=([a-zA-Z0-9]{4})', url, re.IGNORECASE)
                if pwd_in_url:
                    receive_code = pwd_in_url.group(1)
                break

        # 4. 如果 URL 里没有密码，再从正文里找
        if not receive_code:
            pwd_match = re.search(r'(?:password=|访问码|提取码|密码)[:：=\s]*([a-zA-Z0-9]{4})', text, re.IGNORECASE)
            if pwd_match:
                receive_code = pwd_match.group(1)

        # 5. 提取 TMDB ID (如果有)
        tmdb_id = None
        tmdb_match = re.search(r'TMDB ID[:：\s]*(\d+)', text, re.IGNORECASE)
        if tmdb_match:
            tmdb_id = tmdb_match.group(1)

        # 6. 提取标题和年份 (用于没有 TMDB ID 时的反查)
        title = None
        year = None
        title_match = re.search(r'(?:电视剧|电影|名称)[:：\s]*([^\n]+?)\s*\((\d{4})\)', text)
        if not title_match:
            title_match = re.search(r'^([^\n]+?)\s*\((\d{4})\)', text)
            
        if title_match:
            title = title_match.group(1).strip()
            title = re.sub(r'^[^\w\u4e00-\u9fa5]+', '', title).strip()
            year = title_match.group(2)

        # 7. 提取季号和集号 (泥石流级增强版 + 合集包识别)
        season_number = None
        episode_number = None
        is_pack = False # ★ 新增：标记是否为合集包
        
        # 规则 A: 范围匹配合集包 (例如 S01E01-E19, S1 EP01~10)
        range_match = re.search(r'S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})\s*(?:-|~|至)\s*(?:E|EP)?\s*(\d{1,4})', text, re.IGNORECASE)
        if range_match:
            season_number = int(range_match.group(1))
            episode_number = int(range_match.group(3)) # 记录最大集号
            is_pack = True
        else:
            # 规则 B: 标准单集 S01E01 或 S1 EP10
            se_match = re.search(r'S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})', text, re.IGNORECASE)
            if se_match:
                season_number = int(se_match.group(1))
                episode_number = int(se_match.group(2))
            else:
                # 规则 C: 第x季 第x集
                s_match = re.search(r'(?:S|Season|第)\s*(\d{1,2})\s*(?:季)?', text, re.IGNORECASE)
                e_match = re.search(r'(?:E|EP|Episode|第)\s*(\d{1,4})\s*(?:集|话)', text, re.IGNORECASE)
                if s_match: season_number = int(s_match.group(1))
                if e_match: episode_number = int(e_match.group(1))
                
                # 规则 D: 应对群魔乱舞的合集包 (更新至X集, 全X集, 1-X集)
                if episode_number is None:
                    bulk_match = re.search(r'(?:更新至|全|至)(?:第)?\s*(\d{1,4})\s*(?:集|话)|(?:^|\s)\d{1,3}-(\d{1,4})(?:集|话)?', text)
                    if bulk_match:
                        ep_str = bulk_match.group(1) or bulk_match.group(2)
                        if ep_str:
                            episode_number = int(ep_str)
                            is_pack = True # 标记为合集

        # ★ 兜底神技：国产剧/韩剧通常不写季号，如果提取到了集号但没季号，默认第一季！
        if episode_number is not None and season_number is None:
            season_number = 1

        # 8. 精准判定媒体类型 (防张冠李戴 - 优先级隔离版)
        item_type = 'movie' # 默认兜底为电影
        
        # 优先级 1: 明确的标题前缀 (最高优先级，直接秒杀)
        if re.search(r'(?:📺|🖥️)?\s*(?:电视剧|剧集|动漫|番剧)[:：]', text):
            item_type = 'tv'
        elif re.search(r'(?:🎬|🎥|🎞️)?\s*电影[:：]', text):
            item_type = 'movie'
            
        # 优先级 2: 季号和集号特征 (只要带 S01E01 或 更新至X集，绝对是剧集)
        elif season_number is not None or episode_number is not None:
            item_type = 'tv'
            
        # 优先级 3: 标签特征 (只在 #标签 里找，防止被简介误伤)
        else:
            tags = " ".join(re.findall(r'#\w+', text))
            if re.search(r'#(?:电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集|动画)', tags, re.IGNORECASE):
                item_type = 'tv'
            elif re.search(r'#(?:电影|Movie)', tags, re.IGNORECASE):
                item_type = 'movie'
                
            # 优先级 4: 头部文本特征 (只看前8行，坚决不看简介)
            else:
                header_text = "\n".join(text.split('\n')[:8])
                if re.search(r'(电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集)', header_text, re.IGNORECASE):
                    item_type = 'tv'
                elif re.search(r'(电影|Movie)', header_text, re.IGNORECASE):
                    item_type = 'movie'

        # =================================================================
        # 根据用户配置的订阅类型进行拦截
        # =================================================================
        allowed_types = cfg.get('monitor_types', ['movie', 'tv'])
        if item_type not in allowed_types:
            logger.debug(f"  ➜ [频道监听] 资源类型为 {'剧集' if item_type=='tv' else '电影'}，未在允许的订阅类型中，已忽略。")
            return

        # 9. 解析磁力/ED2K 链接
        is_magnet = text.lower().startswith('magnet:?')
        is_ed2k = text.lower().startswith('ed2k://')
        magnet_ed2k_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)

        # =================================================================
        # ★ 核心分流逻辑
        # =================================================================
        
        # 情况 A：找到了目标链接 (115 或 中间页)，且 (有 TMDB ID 或 有标题)
        if target_link and (tmdb_id or title):
            logger.debug(f"  ➜ [频道监听] 监听到频道资源 -> 标题: {title or '未知'}, TMDB: {tmdb_id or '缺失'} (S{season_number}E{episode_number}), 判定类型: {'剧集' if item_type=='tv' else '电影'}, 准备推入处理队列...")
            
            tg_task_queue.put({
                "type": "115_share_complex",
                "tmdb_id": tmdb_id,
                "title": title,
                "year": year,
                "item_type": item_type,
                "target_link": target_link,
                "receive_code": receive_code,
                "season_number": season_number,
                "episode_number": episode_number,
                "is_pack": is_pack  # <--- ★ 新增这行，传给消费者
            })
            
        # 情况 B：手动发的磁力链或 ED2K
        elif is_magnet or is_ed2k or magnet_ed2k_match:
            target_url = magnet_ed2k_match.group(1) if magnet_ed2k_match else text
            logger.info(f"  ➜ [频道监听] 收到手动离线下载请求 -> {target_url[:30]}...")
            
            tg_task_queue.put({
                "type": "offline_download",
                "url": target_url
            })

    # ==========================================
    # 以下是供前端 API 调用的登录交互方法
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
                # 只要后台拿到 hash，就存起来供提交时使用
                self.phone_code_hash = res.phone_code_hash
                logger.info("  ➜ [频道监听] 验证码发送请求已成功响应！")
                return True
            except Exception as e:
                logger.error(f"  ➜ [频道监听] 发送验证码被 TG 拒绝: {e}")
                raise e
            
        future = asyncio.run_coroutine_threadsafe(_send(), self.loop)
        
        try:
            # 我们只等 20 秒
            return future.result(timeout=20)
        except TimeoutError:
            logger.warning("  ➜ [频道监听] 请求验证码超时！但后台仍在尝试发送。")
            # ★★★ 老六的终极骗术：捕获超时错误，不抛出异常，强行让前端弹出输入框！ ★★★
            # 只要这里不抛出异常，routes/system.py 就会给前端返回 success: True
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
                
        # 同样放宽到 60 秒
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
# ★★★ ETK 侧的消费者协程 (处理队列中的任务) ★★★
# =================================================================
def _process_tg_queue():
    """死循环读取队列，执行查库和 115 转存/离线下载 (在 gevent 协程中运行)"""
    import requests # 确保引入 requests
    
    while True:
        try:
            task = tg_task_queue.get() # 阻塞等待
            task_type = task.get('type')
            
            client = P115Service.get_client()
            target_cid = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')
            
            if not client:
                logger.error("  ➜ [频道监听] 115 客户端未初始化，无法执行任务。")
                continue

            # =================================================================
            # ★ 任务类型 A：处理 115 频道分享链接 (支持隐藏链接、中间页、无 TMDB ID)
            # =================================================================
            if task_type == "115_share_complex":
                tmdb_id = task.get('tmdb_id')
                title = task.get('title')
                year = task.get('year')
                target_link = task.get('target_link')
                receive_code = task.get('receive_code', '')
                season_number = task.get('season_number')
                episode_number = task.get('episode_number')
                is_pack = task.get('is_pack', False) # 获取合集标记

                # -----------------------------------------------------------
                # 1. 最强大脑：缺失 TMDB ID 时自动反查 (必须最先执行，拿到ID)
                # -----------------------------------------------------------
                item_type = task.get('item_type', 'movie')
                if not tmdb_id and title:
                    logger.debug(f"  ➜ [频道监听] 缺失 TMDB ID，正在通过 TMDb 接口反查: {title} ({year}), 严格限定类型: {'剧集' if item_type=='tv' else '电影'}...")
                    from handler import tmdb
                    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                    results = tmdb.search_media(title, api_key, item_type=item_type, year=year)
                    if results:
                        tmdb_id = str(results[0]['id'])
                        task['tmdb_id'] = tmdb_id # 更新任务字典，供后续步骤使用
                        logger.debug(f"  ➜ [频道监听] 反查成功！精准匹配到 TMDB ID: {tmdb_id}")
                    else:
                        logger.warning(f"  ➜ [频道监听] 反查失败，TMDb 未找到该{'剧集' if item_type=='tv' else '电影'}，任务终止。")
                        continue

                if not tmdb_id: continue

                # -----------------------------------------------------------
                # 2. 查库校验 (判断是否在追剧列表中，不在直接扔掉，绝不浪费积分)
                # -----------------------------------------------------------
                should_process = False
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
                                cursor.execute("SELECT watching_status FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
                                row = cursor.fetchone()
                                if row and row['watching_status'] in ['Watching', 'Paused', 'Pending']:
                                    should_process = True
                except Exception as e:
                    logger.error(f"  ➜ [频道监听] 查库失败: {e}")
                    continue

                if not should_process:
                    logger.debug(f"  ➜ [频道监听] 资源 (TMDB: {tmdb_id}) 不在已订阅/追剧列表中，已忽略。")
                    continue

                # -----------------------------------------------------------
                # 3. 精准去重逻辑 (本地已有的集数直接跳过，绝不浪费积分)
                # -----------------------------------------------------------
                if season_number is not None and episode_number is not None:
                    from database import media_db
                    local_seasons = media_db.get_series_local_children_info(tmdb_id)
                    
                    if is_pack:
                        # 如果是合集包，检查本地该季的总集数是否满足
                        local_ep_count = len(local_seasons.get(season_number, []))
                        if local_ep_count >= episode_number:
                            logger.debug(f"  ➜ [频道监听] 合集包 (TMDB: {tmdb_id} S{season_number:02d} 宣称 {episode_number} 集)，本地已有 {local_ep_count} 集，判定为已满足，跳过转存！")
                            continue
                        else:
                            logger.info(f"  ➜ [频道监听] 发现合集包 (S{season_number:02d} 宣称 {episode_number} 集)，本地仅有 {local_ep_count} 集，放行转存以补齐缺集！")
                    else:
                        # 如果是单集，严格检查该集是否存在
                        if season_number in local_seasons and episode_number in local_seasons[season_number]:
                            logger.debug(f"  ➜ [频道监听] 单集资源 (TMDB: {tmdb_id} S{season_number:02d}E{episode_number:02d}) 本地已存在，跳过转存！")
                            continue

                # -----------------------------------------------------------
                # 4. 追踪弹：解析真实的 115 Share Code (★★★ 此时才允许扣积分解锁 ★★★)
                # -----------------------------------------------------------
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
                                
                                # 提取码兜底逻辑
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
                    # 普通 115 链接，直接提取
                    match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', target_link)
                    if match: share_code = match.group(1)

                if not share_code:
                    logger.error("  ➜ [频道监听] 无法获取有效的 115 Share Code，任务终止。")
                    continue

                # -----------------------------------------------------------
                # 5. 执行转存
                # -----------------------------------------------------------
                logger.debug(f"  ➜ [频道监听] 命中订阅资源 (TMDB: {tmdb_id})！准备转存...")
                
                res = client.share_import(share_code, receive_code, target_cid)
                if res and res.get('state'):
                    logger.info(f"  ➜ [频道监听] 资源转存成功！正在触发整理...")
                    
                    # ★★★ 发送转存成功通知 ★★★
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

            # =================================================================
            # ★ 任务类型 B：处理手动发送的磁力/ED2K 离线下载 (保持不变)
            # =================================================================
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

# 启动消费者协程
spawn(_process_tg_queue)