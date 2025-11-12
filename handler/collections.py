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
    【V2 - 适配统一订阅接口版】
    职责：同步数据，计算真实缺失，收集元数据，并发起统一订阅。
    """
    logger.info("--- (SYNC) 开始执行原生合集扫描与自动订阅任务 ---")
    
    config = config_manager.APP_CONFIG
    tmdb_api_key = config.get("tmdb_api_key")
    
    emby_collections = emby.get_all_native_collections_from_emby(
        base_url=config.get('emby_server_url'),
        api_key=config.get('emby_api_key'),
        user_id=config.get('emby_user_id')
    )
    if not emby_collections:
        logger.info("  ➜ (SYNC) 未找到原生合集，任务结束。")
        return

    all_movie_parts = {} # 使用字典来存储 part 详情，避免重复
    collection_tmdb_details_map = {}

    def fetch_tmdb_details(collection):
        tmdb_coll_id = collection.get('tmdb_collection_id')
        if not tmdb_coll_id: return None, None
        details = tmdb.get_collection_details(tmdb_coll_id, tmdb_api_key)
        return collection.get('emby_collection_id'), details

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_coll = {executor.submit(fetch_tmdb_details, c): c for c in emby_collections}
        for future in concurrent.futures.as_completed(future_to_coll):
            emby_id, details = future.result()
            if emby_id and details and 'parts' in details:
                collection_tmdb_details_map[emby_id] = details
                for part in details['parts']:
                    all_movie_parts[str(part['id'])] = part

    all_movie_tmdb_ids = list(all_movie_parts.keys())
    in_library_tmdb_ids = set(media_db.get_media_in_library_status_by_tmdb_ids(all_movie_tmdb_ids).keys())
    logger.info(f"  ➜ (SYNC) 扫描涉及 {len(all_movie_tmdb_ids)} 部电影，其中 {len(in_library_tmdb_ids)} 部真正在库。")

    # 1. 准备一个列表，用于收集所有需要订阅的、包含完整元数据的电影信息
    media_requests_with_info = []
    
    # 2. 遍历合集，计算缺失项，并【收集元数据】
    for collection in emby_collections:
        emby_collection_id = collection.get('emby_collection_id')
        tmdb_details = collection_tmdb_details_map.get(emby_collection_id)
        
        if not tmdb_details: continue

        authoritative_tmdb_ids = {str(part['id']) for part in tmdb_details['parts']}
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

        # 为这个合集里所有缺失的电影，构建包含元数据的订阅请求
        for tmdb_id in truly_missing_tmdb_ids:
            # 从我们之前收集的 all_movie_parts 中获取电影详情
            part_details = all_movie_parts.get(tmdb_id)
            if not part_details: continue

            media_requests_with_info.append({
                'tmdb_id': tmdb_id,
                'title': part_details.get('title'),
                'original_title': part_details.get('original_title'),
                'release_date': part_details.get('release_date'),
                'poster_path': part_details.get('poster_path'),
                'overview': part_details.get('overview'),
                'source': {'type': 'native_collection', 'id': emby_collection_id, 'name': collection.get('name')}
            })

    # 3. 使用“五合一”终极函数发起批量订阅
    if media_requests_with_info:
        logger.info(f"  ➜ (SYNC) 发现 {len(media_requests_with_info)} 个缺失的电影订阅请求，正在全部加入订阅队列...")
        
        # 提取所有需要订阅的 TMDb ID
        tmdb_ids_to_subscribe = [req['tmdb_id'] for req in media_requests_with_info]
        # 使用第一个请求的 source 作为代表
        representative_source = media_requests_with_info[0]['source']

        media_db.update_subscription_status(
            tmdb_ids=tmdb_ids_to_subscribe,
            item_type='Movie',
            new_status='WANTED',
            source=representative_source,
            media_info_list=media_requests_with_info # 将包含完整元数据的列表传递过去
        )
    else:
        logger.info("  ➜ (SYNC) 扫描完成，所有合集均无缺失。")


def assemble_all_collection_details() -> List[Dict[str, Any]]:
    """
    【V11 - 最终正确版】前端 API 函数。
    职责：组装数据用于前端展示。
    判断标准：将 in_library, WANTED, SUBSCRIBED 都视为“非缺失”，用于UI展示。
    """
    logger.info("--- (ASSEMBLE) 开始为前端 API 组装原生合集详情 ---")
    
    all_collections_from_db = collection_db.get_all_native_collections()
    if not all_collections_from_db:
        return []

    all_movie_tmdb_ids: Set[str] = set()
    tmdb_api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
    
    # (为了性能，可以并发获取 TMDB 详情)
    tmdb_api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
    for collection in all_collections_from_db:
        tmdb_collection_id = collection.get('tmdb_collection_id')
        if tmdb_collection_id:
            tmdb_details = tmdb.get_collection_details(tmdb_collection_id, tmdb_api_key)
            if tmdb_details and 'parts' in tmdb_details:
                collection['tmdb_parts'] = tmdb_details['parts']
                for part in tmdb_details['parts']:
                    all_movie_tmdb_ids.add(str(part['id']))

    # ★★★ 核心：获取包含订阅状态的完整媒体详情 ★★★
    db_media_map = media_db.get_media_details_by_tmdb_ids(list(all_movie_tmdb_ids))

    for collection in all_collections_from_db:
        final_movie_list = []
        for movie_part in collection.get('tmdb_parts', []):
            tmdb_id = str(movie_part['id'])
            db_info = db_media_map.get(tmdb_id)
            
            # ★★★ 核心：为前端 UI 计算状态 ★★★
            status = 'missing'
            if db_info and db_info.get('in_library'):
                status = 'in_library'
            elif db_info and db_info.get('subscription_status') in ['SUBSCRIBED']:
                status = 'subscribed'
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