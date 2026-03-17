# handler/hdhive_client.py
import requests
import re
import logging

logger = logging.getLogger(__name__)

class HDHiveClient:
    BASE_URL = "https://hdhive.com/api/open"

    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }

    def ping(self):
        """测试 API Key 是否有效"""
        try:
            res = requests.get(f"{self.BASE_URL}/ping", headers=self.headers, timeout=10).json()
            return res.get("success") is True
        except Exception as e:
            logger.error(f"HDHive Ping 失败: {e}")
            return False

    def get_resources(self, tmdb_id, media_type, target_season=None):
        """
        根据 TMDB ID 获取资源列表，并支持本地过滤特定季
        :param target_season: int类型，比如 1 代表第一季。如果不传，则返回所有。
        """
        try:
            url = f"{self.BASE_URL}/resources/{media_type}/{tmdb_id}"
            res = requests.get(url, headers=self.headers, timeout=15).json()
            
            if not res.get("success"):
                return []
                
            # 1. 过滤出 115 网盘的资源
            resources = [r for r in res.get("data", []) if r.get("pan_type") == "115" or r.get("pan_type") is None]
            
            # 2. 如果是电影，或者没有指定季，直接返回
            if media_type == 'movie' or target_season is None:
                return resources
                
            # 3. 如果指定了季，进行本地正则过滤
            filtered_resources = []
            target_s_str = f"S{int(target_season):02d}" # 格式化为 S01, S02
            target_s_num = str(int(target_season))      # 格式化为 1, 2
            
            # 中文数字映射表 (支持到第十季，更长的通常用阿拉伯数字了)
            zh_num_map = {1:"一", 2:"二", 3:"三", 4:"四", 5:"五", 6:"六", 7:"七", 8:"八", 9:"九", 10:"十"}
            zh_season = f"第{zh_num_map.get(int(target_season), target_s_num)}季"
            
            for r in resources:
                title = r.get("title", "").upper()
                remark = r.get("remark", "").upper()
                combined_text = f"{title} {remark}"
                
                # 匹配逻辑：
                # 1. 包含 S01 或 S1
                # 2. 包含 第一季
                # 3. 包含 S01-S05 这种全集大包（需要解析范围）
                
                is_match = False
                
                # 精确匹配 S01 或 第一季
                if target_s_str in combined_text or f"S{target_s_num}" in combined_text or zh_season in combined_text:
                    is_match = True
                else:
                    # 匹配范围大包，例如 S01-S08 或 S1-S3
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
            res = requests.post(url, headers=self.headers, json=payload, timeout=15).json()
            
            if res.get("success"):
                return res.get("data") # 包含 url, access_code 等
            else:
                logger.error(f"HDHive 解锁失败: {res.get('message')}")
                return None
        except Exception as e:
            logger.error(f"HDHive 解锁请求异常: {e}")
            return None
        
    def get_user_info(self):
        """获取当前用户信息 (兼容普通用户和 Premium 用户)"""
        try:
            url = f"{self.BASE_URL}/me"
            res = requests.get(url, headers=self.headers, timeout=10)
            
            # ★ 核心修复：如果返回 403，说明是普通用户，没有权限调用 /me 接口
            if res.status_code == 403:
                return {
                    "nickname": "普通用户", 
                    "user_meta": {"points": "未知 (需Premium)"}
                }
                
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
            res = requests.get(url, headers=self.headers, timeout=10).json()
            if res.get("success"):
                return res.get("data")
            return None
        except Exception as e:
            logger.error(f"HDHive 获取配额失败: {e}")
            return None
        
    def checkin(self, is_gambler=False):
        """
        每日签到 (支持赌狗模式)
        """
        try:
            url = f"{self.BASE_URL}/checkin"
            payload = {"is_gambler": is_gambler}
            # 签到接口是 POST 请求
            res = requests.post(url, headers=self.headers, json=payload, timeout=10).json()
            return res
        except Exception as e:
            logger.error(f"HDHive 签到失败: {e}")
            return {"success": False, "message": str(e)}