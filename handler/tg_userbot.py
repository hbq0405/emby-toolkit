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
            'channels': cfg.get(constants.CONFIG_OPTION_TG_MONITOR_CHANNELS) or []
        }

    def start(self):
        """启动后台线程"""
        cfg = self._get_config()
        if not cfg['enabled'] or not cfg['api_id'] or not cfg['api_hash']:
            return

        if self.is_running:
            self.stop()

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
                    logger.info(f"  🌐 [UserBot] 已加载网络代理: {p_type}://{parsed.hostname}:{parsed.port}")
                except Exception as e:
                    logger.warning(f"  ⚠️ [UserBot] 解析代理 URL 失败: {e}")

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
                    logger.error("  ❌ [UserBot] 登录凭证已失效 (AuthKeyUnregistered)。已自动清理，请在前端重新登录！")
                    if self.client:
                        await self.client.disconnect()
                    if os.path.exists(self.session_path):
                        os.remove(self.session_path)
                    return # 退出当前 daemon，等待前端重新触发 start()

                if is_auth:
                    logger.info("  🚀 [UserBot] Telegram 客户端已授权，开始监听频道消息...")
                    await self.client.run_until_disconnected()
                else:
                    logger.info("  ⏳ [UserBot] Telegram 客户端已连接，等待前端输入验证码授权...")
                    # 保持协程存活，等待前端调用登录接口
                    while self.is_running:
                        if await self.client.is_user_authorized():
                            logger.info("  🚀 [UserBot] 授权成功，开始监听频道消息...")
                            await self.client.run_until_disconnected()
                            break
                        await asyncio.sleep(2)

            self.loop.run_until_complete(_daemon())
            
        except Exception as e:
            logger.error(f"  ❌ [UserBot] 运行异常: {e}", exc_info=True)
        finally:
            self.is_running = False
            self.loop.close()

    async def _handle_message(self, event):
        """处理收到的消息 (在 asyncio 线程中运行)"""
        cfg = self._get_config()
        
        # ★ 修复：增加安全检查，防止 cfg['channels'] 为 None
        raw_channels = cfg.get('channels') or []
        monitor_channels = [c.replace('@', '').strip().lower() for c in raw_channels if c and c.strip()]
        
        if not monitor_channels:
            return

        chat = await event.get_chat()
        chat_username = getattr(chat, 'username', '') or ''
        chat_id = str(getattr(chat, 'id', ''))

        # 检查是否在白名单中
        if chat_username.lower() not in monitor_channels and chat_id not in monitor_channels:
            return

        text = event.raw_text
        if not text:
            return

        # 解析 TMDB ID 和 115 链接
        tmdb_match = re.search(r'TMDB ID[:：\s]*(\d+)', text, re.IGNORECASE)
        link_match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', text, re.IGNORECASE)
        pwd_match = re.search(r'(?:password=|访问码|提取码|密码)[:：=\s]*([a-zA-Z0-9]{4})', text, re.IGNORECASE)

        if tmdb_match and link_match:
            tmdb_id = tmdb_match.group(1)
            share_code = link_match.group(1)
            receive_code = pwd_match.group(1) if pwd_match else ""
            
            logger.info(f"  📥 [UserBot] 监听到频道资源 -> TMDB: {tmdb_id}, 准备推入处理队列...")
            # 推入队列，交由 gevent 协程处理
            tg_task_queue.put({
                "tmdb_id": tmdb_id,
                "share_code": share_code,
                "receive_code": receive_code
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
            logger.info("  ➜ [UserBot] 正在唤醒后台服务以发送验证码...")
            self.start()
            import time
            time.sleep(2.5) 
            
        if not self.loop or not self.client:
            raise Exception("UserBot 服务启动失败，请检查 API ID 和 Hash 是否正确")

        async def _send():
            try:
                logger.info(f"  ➜ [UserBot] 正在向 TG 服务器请求发送验证码至 {phone}...")
                if not self.client.is_connected(): 
                    await self.client.connect()
                res = await self.client.send_code_request(phone)
                # 只要后台拿到 hash，就存起来供提交时使用
                self.phone_code_hash = res.phone_code_hash
                logger.info("  ✅ [UserBot] 验证码发送请求已成功响应！")
                return True
            except Exception as e:
                logger.error(f"  ❌ [UserBot] 发送验证码被 TG 拒绝: {e}")
                raise e
            
        future = asyncio.run_coroutine_threadsafe(_send(), self.loop)
        
        try:
            # 我们只等 20 秒
            return future.result(timeout=20)
        except TimeoutError:
            logger.warning("  ⚠️ [UserBot] 请求验证码超时！但后台仍在尝试发送。")
            # ★★★ 老六的终极骗术：捕获超时错误，不抛出异常，强行让前端弹出输入框！ ★★★
            # 只要这里不抛出异常，routes/system.py 就会给前端返回 success: True
            return True

    def submit_login_code(self, code):
        cfg = self._get_config()
        async def _submit():
            try:
                logger.info("  ➜ [UserBot] 正在向 TG 服务器提交验证码...")
                await self.client.sign_in(cfg['phone'], code, phone_code_hash=self.phone_code_hash)
                logger.info("  ✅ [UserBot] 验证码校验通过！")
                return {"success": True}
            except SessionPasswordNeededError:
                if not cfg['password']:
                    return {"success": False, "need_2fa": True, "msg": "需要两步验证密码，请在配置中填写后重试"}
                logger.info("  ➜ [UserBot] 正在提交两步验证密码...")
                await self.client.sign_in(password=cfg['password'])
                logger.info("  ✅ [UserBot] 两步验证密码校验通过！")
                return {"success": True}
            except Exception as e:
                logger.error(f"  ❌ [UserBot] 提交验证码失败: {e}")
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
    """死循环读取队列，执行查库和 115 转存 (在 gevent 协程中运行)"""
    while True:
        try:
            task = tg_task_queue.get() # 阻塞等待
            tmdb_id = task['tmdb_id']
            share_code = task['share_code']
            receive_code = task['receive_code']

            should_process = False
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT subscription_status FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Movie'", (tmdb_id,))
                        row = cursor.fetchone()
                        if row and row['subscription_status'] in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE']:
                            should_process = True
                        
                        if not should_process:
                            cursor.execute("SELECT watching_status FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (tmdb_id,))
                            row = cursor.fetchone()
                            if row and row['watching_status'] in ['Watching', 'Paused', 'Pending']:
                                should_process = True
            except Exception as e:
                logger.error(f"  ❌ [UserBot] 查库失败: {e}")
                continue

            if not should_process:
                logger.debug(f"  ⏭️ [UserBot] 资源 (TMDB: {tmdb_id}) 不在订阅列表中，已忽略。")
                continue

            logger.info(f"  🎯 [UserBot] 命中订阅资源 (TMDB: {tmdb_id})！准备转存...")
            client = P115Service.get_client()
            target_cid = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')
            
            if client:
                res = client.share_import(share_code, receive_code, target_cid)
                if res and res.get('state'):
                    logger.info(f"  ✅ [UserBot] 资源转存成功！正在触发整理...")
                    try:
                        import task_manager
                        import threading
                        threading.Timer(3.0, task_manager.trigger_115_organize_task).start()
                    except: pass
                else:
                    err = res.get('error_msg', '未知错误') if res else '无响应'
                    logger.error(f"  ❌ [UserBot] 转存失败: {err}")

        except Exception as e:
            logger.error(f"  ❌ [UserBot] 队列处理异常: {e}")

# 启动消费者协程
spawn(_process_tg_queue)