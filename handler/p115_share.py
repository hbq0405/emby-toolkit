# handler/p115_share.py
import logging
import os
import re
import json
import gzip
import time
from pathlib import Path
from tempfile import gettempdir
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice

from handler.p115_service import P115Service, get_config
import constants

try:
    from p115center import P115Center
except ImportError:
    P115Center = None

logger = logging.getLogger(__name__)

def batched(iterable, n):
    """Python 3.12 以下版本的 batched 替代方案"""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

class ShareOOPServerHelper:
    """处理 P115Center 中心化共享数据的助手"""
    
    @staticmethod
    def get_center_client():
        if not P115Center:
            return None
        try:
            return P115Center() 
        except Exception as e:
            logger.debug(f"P115Center 初始化失败: {e}")
            return None

    @staticmethod
    def download_share_files_data(share_code: str, receive_code: str, temp_file: str) -> bool:
        client = ShareOOPServerHelper.get_center_client()
        if not client: return False
        
        batch_id = f"{share_code}{receive_code}"
        logger.info(f"  🌐 [分享挂载] 尝试从中心服务器下载缓存数据，batch_id: {batch_id}")
        try:
            client.download_share_file_iter(batch_id, temp_file)
            size_mb = os.path.getsize(temp_file) / 1024 / 1024
            logger.info(f"  ✅ [分享挂载] 中心缓存下载成功，文件大小: {size_mb:.2f} MB")
            return True
        except Exception as e:
            if '404' in str(e):
                logger.info(f"  ℹ️ [分享挂载] 中心服务器暂无该分享的缓存数据。")
            else:
                logger.debug(f"  ⚠️ [分享挂载] 下载中心缓存失败: {e}")
            return False

    @staticmethod
    def upload_share_files_data(share_code: str, receive_code: str, temp_file: str):
        client = ShareOOPServerHelper.get_center_client()
        if not client: return
        
        if not os.path.exists(temp_file) or os.path.getsize(temp_file) == 0:
            return

        batch_id = f"{share_code}{receive_code}"
        logger.info(f"  ⬆️ [分享挂载] 开始上传解析数据到中心服务器反哺社区，batch_id: {batch_id}")
        try:
            client.upload_share_file_iter(batch_id, temp_file)
            logger.info(f"  ✅ [分享挂载] 数据反哺上传成功！")
        except Exception as e:
            logger.warning(f"  ⚠️ [分享挂载] 数据上传中心服务器失败: {e}")
        finally:
            try:
                os.remove(temp_file)
            except: pass

class ShareStrmManager:
    def __init__(self):
        self.config = get_config()
        self.local_dir = self.config.get('p115_share_local_dir', '')
        self.etk_url = self.config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "http://127.0.0.1:5257").rstrip('/')
        
        exts = self.config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        if not exts:
            exts = ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg', 'mp3', 'flac', 'wav']
        self.allowed_exts = set(e.lower() for e in exts)
        
        self.client = P115Service.get_cookie_client()
        self.strm_count = 0
        self.total_count = 0

    def extract_share_codes(self, link: str):
        link = link.strip()
        share_code, receive_code = None, None
        
        match = re.search(r'/s/([a-zA-Z0-9]+)', link)
        if match: share_code = match.group(1)
        
        pwd_match = re.search(r'password=([a-zA-Z0-9]+)', link)
        if pwd_match: receive_code = pwd_match.group(1)
        
        if not receive_code:
            parts = link.split()
            for part in parts:
                if len(part) == 4 and part.isalnum():
                    receive_code = part
                    break
                    
        return share_code, receive_code

    def fetch_share_list_from_115(self, share_code, receive_code, temp_file):
        if not self.client:
            raise Exception("未配置 115 Cookie，无法拉取分享列表")
            
        logger.info(f"  🚀 [分享挂载] 开始从 115 官方接口递归拉取分享数据...")
        
        # 1. 请求 snap 接口获取第一层数据
        snap_url = f"https://webapi.115.com/share/snap?share_code={share_code}&receive_code={receive_code}"
        snap_res = self.client.request(snap_url)
        
        if not snap_res.get('state'):
            raise Exception(f"分享链接无效或提取码错误: {snap_res.get('error', '未知错误')}")
            
        share_info = snap_res.get('data', {})
        share_title = share_info.get('share_title', share_code)
        top_list = share_info.get('list', [])
        
        collected_count = 0
        
        # 2. 定义处理列表的生成器
        def _process_list(item_list, current_path):
            for item in item_list:
                item_name = item.get('n') or item.get('file_name')
                is_dir = str(item.get('fc', item.get('type'))) == '0'
                
                if is_dir:
                    new_path = f"{current_path}/{item_name}" if current_path else item_name
                    # 文件夹的 ID 可能是 cid 或 fid
                    folder_id = item.get('cid') or item.get('fid')
                    yield from _fetch_dir(folder_id, new_path)
                else:
                    yield {
                        "id": item.get('fid') or item.get('file_id'),
                        "name": item_name,
                        "path": f"{current_path}/{item_name}" if current_path else item_name,
                        "size": item.get('s') or item.get('size', 0),
                        "pc": item.get('pc') or item.get('pick_code'),
                        "sha1": item.get('sha') or item.get('sha1')
                    }

        # 3. 定义递归请求 down 接口的生成器
        def _fetch_dir(cid, current_path):
            offset = 0
            limit = 1000
            while True:
                url = f"https://webapi.115.com/share/down?share_code={share_code}&receive_code={receive_code}&cid={cid}&limit={limit}&offset={offset}"
                res = self.client.request(url)
                
                if not res.get('state'): break
                
                data_list = res.get('data', {}).get('list', [])
                if not data_list: break
                
                yield from _process_list(data_list, current_path)
                
                if len(data_list) < limit: break
                offset += limit
                time.sleep(0.5) # 防风控休眠

        # 4. 开始边解析边写入 GZIP
        with gzip.open(temp_file, "wb") as f:
            # 从 snap 返回的第一层列表开始处理
            for record in _process_list(top_list, share_title):
                f.write(json.dumps(record).encode('utf-8') + b"\n")
                collected_count += 1
                if collected_count % 1000 == 0:
                    logger.info(f"  ⏳ [分享挂载] 已拉取 {collected_count} 条数据...")
                    
        return collected_count, share_title

    def process_single_item(self, item, share_code, receive_code, share_title):
        file_name = item.get('name', '')
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        
        if ext not in self.allowed_exts: return
            
        file_id = item.get('id')
        if not file_id: return
        
        rel_path = item.get('path', file_name)
        if not rel_path.startswith(share_title):
            rel_path = f"{share_title}/{rel_path}"
            
        strm_rel_path = os.path.splitext(rel_path)[0] + ".strm"
        strm_full_path = os.path.join(self.local_dir, strm_rel_path)
        
        os.makedirs(os.path.dirname(strm_full_path), exist_ok=True)
        
        # ★ 核心修改：生成动态转存路由，包含 share_code, receive_code 和 file_id
        strm_content = f"{self.etk_url}/api/p115/play_share/{share_code}/{receive_code}/{file_id}/{file_name}"
        
        need_write = True
        if os.path.exists(strm_full_path):
            try:
                with open(strm_full_path, 'r', encoding='utf-8') as f:
                    if f.read().strip() == strm_content:
                        need_write = False
            except: pass
            
        if need_write:
            with open(strm_full_path, 'w', encoding='utf-8') as f:
                f.write(strm_content)
            self.strm_count += 1
            
        try:
            from monitor_service import enqueue_file_actively
            enqueue_file_actively(strm_full_path)
        except: pass

    def execute(self):
        links = self.config.get('p115_share_links', [])
        if not links or not self.local_dir:
            logger.warning("  ⚠️ [分享挂载] 未配置分享链接或本地目录，任务取消。")
            return

        logger.info(f"=== 开始执行 115 分享挂载任务 (共 {len(links)} 个链接) ===")
        
        for link in links:
            if not link.strip(): continue
            
            share_code, receive_code = self.extract_share_codes(link)
            if not share_code or not receive_code:
                logger.warning(f"  ⚠️ [分享挂载] 无法解析链接: {link}")
                continue
                
            logger.info(f"  🔗 [分享挂载] 正在处理分享: share_code={share_code}")
            
            temp_file = os.path.join(gettempdir(), f"share_data_{share_code}{receive_code}.json.gz")
            download_success = ShareOOPServerHelper.download_share_files_data(share_code, receive_code, temp_file)
            
            share_title = share_code
            
            if not download_success:
                try:
                    count, share_title = self.fetch_share_list_from_115(share_code, receive_code, temp_file)
                    if count > 0:
                        ShareOOPServerHelper.upload_share_files_data(share_code, receive_code, temp_file)
                    else:
                        logger.warning(f"  ⚠️ [分享挂载] 该分享为空或拉取失败。")
                        continue
                except Exception as e:
                    logger.error(f"  ❌ [分享挂载] 拉取分享失败: {e}")
                    continue
            
            logger.info(f"  📝 [分享挂载] 开始生成 STRM 文件...")
            
            def read_gzip_iter():
                with gzip.open(temp_file, "rb") as f:
                    for line in f:
                        if line.strip():
                            yield json.loads(line)
                            
            try:
                with ThreadPoolExecutor(max_workers=32) as executor:
                    for batch in batched(read_gzip_iter(), 1000):
                        self.total_count += len(batch)
                        futures = [executor.submit(self.process_single_item, item, share_title) for item in batch]
                        for future in as_completed(futures):
                            future.result()
            except Exception as e:
                logger.error(f"  ❌ [分享挂载] 生成 STRM 发生异常: {e}")
            finally:
                if os.path.exists(temp_file):
                    try: os.remove(temp_file)
                    except: pass
                    
        logger.info(f"=== 分享挂载任务结束！共遍历 {self.total_count} 个文件，新增/更新 {self.strm_count} 个 STRM ===")

def task_sync_share_links():
    try:
        manager = ShareStrmManager()
        manager.execute()
    except Exception as e:
        logger.error(f"执行分享挂载任务失败: {e}", exc_info=True)