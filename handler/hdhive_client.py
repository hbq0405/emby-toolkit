# handler/hdhive_client.py
import requests
import logging
import re
import config_manager # ★ 引入配置管理器

logger = logging.getLogger(__name__)

class HDHiveClient:
    BASE_URL = "https://hdhive.com/api/open"

    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }
        # ★ 核心修复：获取全局代理配置
        self.proxies = config_manager.get_proxies_for_requests()

    def ping(self):
        """测试 API Key 是否有效"""
        try:
            # ★ 挂上代理
            res = requests.get(f"{self.BASE_URL}/ping", headers=self.headers, proxies=self.proxies, timeout=10)
            res.raise_for_status() # ★ 遇到 502/504 等网络错误直接抛出，防止 json() 解析报错
            return res.json().get("success") is True
        except Exception as e:
            logger.error(f"HDHive Ping 失败 (请检查网络或代理): {e}")
            return False

    def get_user_info(self):
        """获取当前用户信息 (兼容普通用户和 Premium 用户)"""
        try:
            url = f"{self.BASE_URL}/me"
            res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=10)
            
            if res.status_code == 403:
                return {
                    "nickname": "普通用户", 
                    "user_meta": {"points": "未知 (需Premium)"}
                }
                
            res.raise_for_status()
            data = res.json()
            if data.get("success"):
                return data.get("data")
            return None
        except Exception as e:
            logger.error(f"HDHive 获取用户信息失败: {e}")
            return None

    def get_quota(self):
        """获取每日 API 配额 (所有用户可用)"""
        try:
            url = f"{self.BASE_URL}/quota"
            res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=10)
            res.raise_for_status()
            data = res.json()
            if data.get("success"):
                return data.get("data")
            return None
        except Exception as e:
            logger.error(f"HDHive 获取配额失败: {e}")
            return None

    def get_resources(self, tmdb_id, media_type, target_season=None):
        """
        根据 TMDB ID 获取资源列表，并支持本地过滤特定季
        """
        try:
            url = f"{self.BASE_URL}/resources/{media_type}/{tmdb_id}"
            res = requests.get(url, headers=self.headers, proxies=self.proxies, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            if not data.get("success"):
                return []
                
            resources = [r for r in data.get("data", []) if r.get("pan_type") == "115" or r.get("pan_type") is None]
            
            if media_type == 'movie' or target_season is None:
                return resources
                
            filtered_resources = []
            target_s_str = f"S{int(target_season):02d}" 
            target_s_num = str(int(target_season))      
            
            zh_num_map = {1:"一", 2:"二", 3:"三", 4:"四", 5:"五", 6:"六", 7:"七", 8:"八", 9:"九", 10:"十"}
            zh_season = f"第{zh_num_map.get(int(target_season), target_s_num)}季"
            
            for r in resources:
                title = r.get("title", "").upper()
                remark = r.get("remark", "").upper()
                combined_text = f"{title} {remark}"
                
                is_match = False
                
                if target_s_str in combined_text or f"S{target_s_num}" in combined_text or zh_season in combined_text:
                    is_match = True
                else:
                    range_match = re.search(r'S(\d{1,2})\s*-\s*S?(\d{1,2})', combined_text)
                    if range_match:
                        start_s = int(range_match.group(1))
                        end_s = int(range_match.group(2))
                        if start_s <= int(target_season) <= end_s:
                            is_match = True
                            
                if is_match:
                    filtered_resources.append(r)
                    
            return filtered_resources
            
        except Exception as e:
            logger.error(f"HDHive 获取资源失败: {e}")
            return []

    def unlock_resource(self, slug):
        """
        解锁资源，获取真实网盘链接和提取码
        """
        try:
            url = f"{self.BASE_URL}/resources/unlock"
            payload = {"slug": slug}
            res = requests.post(url, headers=self.headers, json=payload, proxies=self.proxies, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            if data.get("success"):
                return data.get("data")
            else:
                logger.error(f"HDHive 解锁失败: {data.get('message')}")
                return None
        except Exception as e:
            logger.error(f"HDHive 解锁请求异常: {e}")
            return None

    def checkin(self, is_gambler=False):
        """
        每日签到 (支持赌狗模式)
        """
        try:
            url = f"{self.BASE_URL}/checkin"
            payload = {"is_gambler": is_gambler}
            res = requests.post(url, headers=self.headers, json=payload, proxies=self.proxies, timeout=10)
            
            # 签到接口即使失败（比如已签到），也会返回 200 和 JSON，所以直接解析
            data = res.json()
            return data
        except Exception as e:
            logger.error(f"HDHive 签到失败: {e}")
            return {"success": False, "message": str(e)}