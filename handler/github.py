# handler/github.py

import requests
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

def get_github_releases(owner: str, repo: str, token: Optional[str] = None, proxies: Optional[Dict[str, str]] = None) -> Optional[List[Dict[str, Any]]]:
    """
    【V2 - 支持认证版】从 GitHub API 获取指定仓库的所有 Release 信息。
    如果提供了 token，则会使用认证来提高速率限制。
    """
    if not owner or not repo:
        logger.error("获取 GitHub releases 失败：缺少 owner 或 repo。")
        return None

    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # ★★★ 核心修改：如果提供了 token，就添加到请求头中 ★★★
    if token:
        logger.debug("检测到 GitHub Token，将使用认证模式请求。")
        headers["Authorization"] = f"Bearer {token}"
    else:
        logger.debug("未提供 GitHub Token，将使用匿名模式请求。")
    
    logger.trace(f"正在从 GitHub API 获取 releases: {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=20, proxies=proxies)
        
        # 打印速率限制信息，方便调试
        remaining = response.headers.get('X-RateLimit-Remaining')
        limit = response.headers.get('X-RateLimit-Limit')
        if remaining and limit:
            logger.debug(f"GitHub API 速率限制: {remaining}/{limit} 次剩余。")

        response.raise_for_status()
        releases_data = response.json()

        # 解析并提取我们需要的信息
        parsed_releases = []
        for release in releases_data:
            parsed_releases.append({
                "version": release.get("tag_name"),
                "published_at": release.get("published_at"),
                "changelog": release.get("body"), # 更新日志通常在 body 字段
                "url": release.get("html_url")
            })
        
        logger.trace(f"成功从 GitHub 获取到 {len(parsed_releases)} 个 release。")
        return parsed_releases

    except requests.exceptions.RequestException as e:
        logger.error(f"请求 GitHub API 时发生网络错误: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"处理 GitHub API 响应时发生未知错误: {e}", exc_info=True)
        return None