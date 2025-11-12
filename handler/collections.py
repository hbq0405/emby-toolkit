# handler/collections.py

import logging
from typing import Dict, List, Any, Set
from datetime import datetime
import concurrent.futures

# 导入数据访问层和外部 API 处理器
from database import collection_db, media_db
import handler.emby as emby
import handler.tmdb as tmdb
import config_manager

logger = logging.getLogger(__name__)

def sync_and_subscribe_native_collections():
    """
    【V4.1 - 修复未定义函数错误】
    - 补上了缺失的 fetch_tmdb_details 辅助函数定义。
    - 在处理合集内电影时，会过滤掉没有海报或没有上映日期的项目。
    """
    logger.info("--- (SYNC) 开始执行原生合集扫描与自动订阅任务 ---")
    
    config = config_manager.APP_CONFIG
    tmdb_api_key = config.get("tmdb_api_key")
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    emby_collections = emby.get_all_native_collections_from_emby(
        base_url=config.get('emby_server_url'),
        api_key=config.get('emby_api_key'),
        user_id=config.get('emby_user_id')
    )
    if not emby_collections:
        logger.info("  ➜ (SYNC) 未找到原生合集，任务结束。")
        return

    all_movie_parts = {}
    collection_tmdb_details_map = {}

    # ★★★ 核心修正：在这里补上缺失的函数定义 ★★★
    def fetch_tmdb_details(collection):
        """这是一个定义在 sync_and_subscribe_native_collections 内部的辅助函数"""
        tmdb_coll_id = collection.get('tmdb_collection_id')
        if not tmdb_coll_id: return None, None
        details = tmdb.get_collection_details(tmdb_coll_id, tmdb_api_key)
        return collection.get('emby_collection_id'), details

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # 现在调用 fetch_tmdb_details 就不会报错了
        future_to_coll = {executor.submit(fetch_tmdb_details, c): c for c in emby_collections}
        for future in concurrent.futures.as_completed(future_to_coll):
            emby_id, details = future.result()
            if emby_id and details and 'parts' in details:
                collection_tmdb_details_map[emby_id] = details
                for part in details['parts']:
                    if not part.get('poster_path') or not part.get('release_date'):
                        logger.debug(f"  ➜ (SYNC) 过滤掉无效电影条目: '{part.get('title')}' (ID: {part.get('id')})，因为它缺少海报或上映日期。")
                        continue
                    all_movie_parts[str(part['id'])] = part

    # ... 后续的代码逻辑完全不变 ...
    all_movie_tmdb_ids = list(all_movie_parts.keys())
    in_library_tmdb_ids = set(media_db.get_media_in_library_status_by_tmdb_ids(all_movie_tmdb_ids).keys())
    logger.info(f"  ➜ (SYNC) 扫描涉及 {len(all_movie_tmdb_ids)} 部有效电影，其中 {len(in_library_tmdb_ids)} 部真正在库。")

    released_requests = []
    unreleased_requests = []
    
    for collection in emby_collections:
        emby_collection_id = collection.get('emby_collection_id')
        tmdb_details = collection_tmdb_details_map.get(emby_collection_id)
        
        if not tmdb_details: continue

        authoritative_tmdb_ids = {str(part['id']) for part in tmdb_details['parts'] if part.get('id')}
        truly_missing_tmdb_ids = list(authoritative_tmdb_ids - in_library_tmdb_ids)
        in_library_count = len(authoritative_tmdb_ids) - len(truly_missing_tmdb_ids)
        
        collection_db.upsert_native_collection({
            'emby_collection_id': emby_collection_id,
            'name': collection.get('name'),
            'tmdb_collection_id': collection.get('tmdb_collection_id'),
            'status': 'ok',
            'has_missing': bool(truly_missing_tmdb_ids),
            'missing_tmdb_ids': truly_missing_tmdb_ids,
            'poster_path': tmdb_details.get('poster_path'),
            'in_library_count': in_library_count
        })

        for tmdb_id in truly_missing_tmdb_ids:
            part_details = all_movie_parts.get(tmdb_id)
            if not part_details: continue

            release_date = part_details.get('release_date')
            media_info = {
                'tmdb_id': tmdb_id, 'title': part_details.get('title'),
                'original_title': part_details.get('original_title'), 'release_date': release_date,
                'poster_path': part_details.get('poster_path'), 'overview': part_details.get('overview'),
                'source': {'type': 'native_collection', 'id': emby_collection_id, 'name': collection.get('name')}
            }

            if release_date and release_date > today_str:
                unreleased_requests.append(media_info)
            else:
                released_requests.append(media_info)

    if released_requests:
        logger.info(f"  ➜ (SYNC) 发现 {len(released_requests)} 个已上映的缺失电影，状态将设为 'WANTED'...")
        media_db.update_subscription_status(
            tmdb_ids=[req['tmdb_id'] for req in released_requests],
            item_type='Movie', new_status='WANTED',
            source=released_requests[0]['source'], media_info_list=released_requests
        )
        
    if unreleased_requests:
        logger.info(f"  ➜ (SYNC) 发现 {len(unreleased_requests)} 个未上映的电影，状态将设为 'PENDING_RELEASE'...")
        media_db.update_subscription_status(
            tmdb_ids=[req['tmdb_id'] for req in unreleased_requests],
            item_type='Movie', new_status='PENDING_RELEASE',
            source=unreleased_requests[0]['source'], media_info_list=unreleased_requests
        )

    if not released_requests and not unreleased_requests:
        logger.info("  ➜ (SYNC) 扫描完成，所有合集均无缺失。")


def assemble_all_collection_details() -> List[Dict[str, Any]]:
    """
    【V14 - 增加垃圾数据过滤器】
    - 在组装前端数据时，同样会过滤掉没有海报或上映日期的项目。
    """
    logger.info("--- (ASSEMBLE) 开始为前端 API 组装原生合集详情 ---")
    
    all_collections_from_db = collection_db.get_all_native_collections()
    if not all_collections_from_db:
        return []

    all_movie_tmdb_ids: Set[str] = set()
    tmdb_api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_coll = {
            executor.submit(tmdb.get_collection_details, coll.get('tmdb_collection_id'), tmdb_api_key): coll
            for coll in all_collections_from_db if coll.get('tmdb_collection_id')
        }
        for future in concurrent.futures.as_completed(future_to_coll):
            collection = future_to_coll[future]
            tmdb_details = future.result()
            if tmdb_details and 'parts' in tmdb_details:
                collection['tmdb_parts'] = tmdb_details['parts']
                for part in tmdb_details['parts']:
                    all_movie_tmdb_ids.add(str(part['id']))

    db_media_map = media_db.get_media_details_by_tmdb_ids(list(all_movie_tmdb_ids))

    for collection in all_collections_from_db:
        final_movie_list = []
        for movie_part in collection.get('tmdb_parts', []):
            # ★★★ 核心过滤点 2/3: 在组装前端数据时再次过滤 ★★★
            if not movie_part.get('poster_path') or not movie_part.get('release_date'):
                continue

            tmdb_id = str(movie_part['id'])
            db_info = db_media_map.get(tmdb_id)
            
            status = 'missing'
            if db_info:
                db_status = db_info.get('subscription_status')
                if db_info.get('in_library'):
                    status = 'in_library'
                elif db_status == 'SUBSCRIBED':
                    status = 'subscribed'
                elif db_status == 'PENDING_RELEASE':
                    status = 'unreleased'
            else:
                release_date = movie_part.get('release_date')
                if release_date and release_date > datetime.now().strftime('%Y-%m-%d'):
                    status = 'unreleased'

            final_movie_list.append({
                'tmdb_id': tmdb_id, 'title': movie_part.get('title'),
                'poster_path': movie_part.get('poster_path'), 'release_date': movie_part.get('release_date'),
                'status': status
            })
        
        collection['movies'] = sorted(final_movie_list, key=lambda x: x.get('release_date') or '9999')
        if 'tmdb_parts' in collection: del collection['tmdb_parts']

    logger.info("--- (ASSEMBLE) 前端 API 数据组装完成 ---")
    return all_collections_from_db