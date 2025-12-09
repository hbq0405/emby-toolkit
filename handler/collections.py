# handler/collections.py

import logging
import json
from typing import Dict, List, Any, Set
from datetime import datetime
import concurrent.futures

# 导入数据访问层和外部 API 处理器
from database import collection_db, media_db, request_db
import handler.emby as emby
import handler.tmdb as tmdb
import config_manager

logger = logging.getLogger(__name__)

def sync_and_subscribe_native_collections(progress_callback=None):
    """
    扫描 Emby 合集。
    """
    if progress_callback:
        progress_callback(0, "正在连接 Emby 获取合集列表...")

    logger.info("--- 开始执行原生合集扫描任务 ---")
    
    config = config_manager.APP_CONFIG
    tmdb_api_key = config.get("tmdb_api_key")
    
    # 1. 获取 Emby 合集
    emby_collections = emby.get_all_native_collections_from_emby(
        base_url=config.get('emby_server_url'),
        api_key=config.get('emby_api_key'),
        user_id=config.get('emby_user_id')
    )
    
    libraries_to_process = config.get("libraries_to_process", [])
    if libraries_to_process:
        emby_collections = [c for c in emby_collections if c.get('ParentId') in libraries_to_process]
    
    total_collections = len(emby_collections)
    if total_collections == 0:
        if progress_callback: progress_callback(100, "未找到需要处理的合集。")
        return

    if progress_callback:
        progress_callback(5, f"共找到 {total_collections} 个合集，开始并发获取 TMDb 详情...")

    # 2. 并发获取 TMDb 详情
    collection_tmdb_details_map = {}
    
    def fetch_tmdb_details(collection):
        tmdb_coll_id = collection.get('tmdb_collection_id')
        if not tmdb_coll_id: return None, None, collection.get('name')
        # 返回 emby_id, details, name 以便回调使用
        return collection.get('emby_collection_id'), tmdb.get_collection_details(tmdb_coll_id, tmdb_api_key), collection.get('name')

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_coll = {executor.submit(fetch_tmdb_details, c): c for c in emby_collections}
        
        finished_count = 0
        
        for future in concurrent.futures.as_completed(future_to_coll):
            finished_count += 1
            coll_name = "未知"
            try:
                emby_id, details, name = future.result()
                coll_name = name or "未知"
                if emby_id and details and 'parts' in details:
                    collection_tmdb_details_map[emby_id] = details
            except Exception as e:
                logger.warning(f"获取合集详情失败: {e}")
            
            if progress_callback:
                percent = 5 + int((finished_count / total_collections) * 85)
                percent = min(percent, 90)
                progress_callback(percent, f"正在获取 TMDb ({finished_count}/{total_collections}): {coll_name}")

    if progress_callback:
        progress_callback(90, "TMDb 数据获取完毕，开始写入数据库...")

    for i, collection in enumerate(emby_collections):
        if progress_callback:
            percent = 90 + int(((i + 1) / total_collections) * 10)
            percent = min(percent, 99)
            progress_callback(percent, f"正在入库 ({i+1}/{total_collections}): {collection.get('name')}")

        emby_collection_id = collection.get('emby_collection_id')
        tmdb_details = collection_tmdb_details_map.get(emby_collection_id)
        
        if not tmdb_details: continue

        # A. 提取数据
        all_parts = []
        all_tmdb_ids = []
        
        for part in tmdb_details.get('parts', []):
            if not part.get('poster_path') or not part.get('release_date'): continue
            
            t_id = str(part['id'])
            all_parts.append({
                'tmdb_id': t_id,
                'title': part['title'],
                'original_title': part.get('original_title'),
                'release_date': part['release_date'],
                'poster_path': part['poster_path'],
                'overview': part.get('overview')
            })
            all_tmdb_ids.append(t_id)

        if not all_tmdb_ids: continue

        # B. 确保 media_metadata 存在基础数据
        media_db.batch_ensure_basic_movies(all_parts)

        # C. 写入合集关系表
        collection_db.upsert_native_collection({
            'emby_collection_id': emby_collection_id,
            'name': collection.get('name'),
            'tmdb_collection_id': collection.get('tmdb_collection_id'),
            'poster_path': tmdb_details.get('poster_path'),
            'all_tmdb_ids': all_tmdb_ids
        })

    logger.info("--- 原生合集扫描完成 ---")
    if progress_callback:
        progress_callback(100, "原生合集扫描完成！")
    
    # 扫描完开始检查缺失标记待订阅
    subscribe_all_missing_in_native_collections()

def subscribe_all_missing_in_native_collections():
    """
    把所有原生合集中缺失的电影加入待订阅列表。
    """
    logger.info("--- 开始执行原生合集缺失电影批量待订阅 ---")
    
    # 1. 一次性拿到所有缺失的电影
    missing_movies = collection_db.get_all_missing_movies_in_collections()
    
    if not missing_movies:
        logger.info("  ➜ 没有发现需要订阅的缺失电影。")
        return {'subscribed_count': 0, 'skipped_count': 0, 'quota_exceeded': False}

    today_str = datetime.now().strftime('%Y-%m-%d')
    
    requests_released = []
    requests_unreleased = []
    
    # 2. 分类 (已上映 vs 未上映)
    for movie in missing_movies:
        # 处理日期类型 (数据库可能返回 date 对象)
        r_date = movie.get('release_date')
        r_date_str = str(r_date) if r_date else None
        
        # 构造标准 media_info
        media_info = {
            'tmdb_id': movie['tmdb_id'],
            'title': movie['title'],
            'original_title': movie.get('original_title'),
            'release_date': r_date_str,
            'poster_path': movie.get('poster_path'),
            'overview': movie.get('overview'),
            'source': {'type': 'native_collection_batch', 'name': '原生合集补全'}
        }

        if r_date_str and r_date_str > today_str:
            requests_unreleased.append(media_info)
        else:
            requests_released.append(media_info)

    total_count = 0
    
    # 3. 批量写入 request_db
    if requests_released:
        count = len(requests_released)
        total_count += count
        logger.info(f"  ➜ 批量待订阅: {count} 部已上映电影设为 WANTED...")
        request_db.set_media_status_wanted(
            tmdb_ids=[m['tmdb_id'] for m in requests_released],
            item_type='Movie',
            source=requests_released[0]['source'],
            media_info_list=requests_released
        )

    if requests_unreleased:
        count = len(requests_unreleased)
        total_count += count
        logger.info(f"  ➜ 批量待订阅: {count} 部未上映电影设为 PENDING_RELEASE...")
        request_db.set_media_status_pending_release(
            tmdb_ids=[m['tmdb_id'] for m in requests_unreleased],
            item_type='Movie',
            source=requests_unreleased[0]['source'],
            media_info_list=requests_unreleased
        )

    logger.info(f"--- 批量待订阅完成，共处理 {total_count} 部电影 ---")
    
    return {
        'subscribed_count': total_count, 
        'skipped_count': 0, 
        'quota_exceeded': False
    }

def assemble_all_collection_details() -> List[Dict[str, Any]]:
    """
    【V5 - 动态统计版】
    读取时，根据 ID 列表实时去 media_metadata 统计 缺失/入库/订阅/未上映 数量。
    """
    logger.info("--- 开始组装原生合集详情 (动态统计) ---")
    
    all_collections = collection_db.get_all_native_collections()
    if not all_collections: return []

    # 1. 收集所有 ID
    global_tmdb_ids = set()
    for coll in all_collections:
        ids = coll.get('all_tmdb_ids_json')
        if ids:
            if isinstance(ids, str):
                try: ids = json.loads(ids)
                except: ids = []
            coll['parsed_ids'] = ids
            global_tmdb_ids.update(ids)
        else:
            coll['parsed_ids'] = []

    if not global_tmdb_ids: return all_collections

    # 2. 批量获取元数据
    media_details_map = media_db.get_media_details_by_tmdb_ids(list(global_tmdb_ids))
    today_str = datetime.now().strftime('%Y-%m-%d')

    # 3. 动态计算统计数据
    for coll in all_collections:
        # 初始化计数器
        stats = {
            'missing': 0,
            'in_library': 0,
            'subscribed': 0,
            'unreleased': 0
        }
        
        final_movies = []
        
        for tmdb_id in coll['parsed_ids']:
            tmdb_id_str = str(tmdb_id)
            item = media_details_map.get(tmdb_id_str)
            
            if not item: continue # 理论上不应发生

            # 处理日期
            raw_date = item.get('release_date')
            release_date_str = str(raw_date) if raw_date else None

            # 提取 Emby ID
            emby_id = None
            if item.get('in_library'):
                ids_json = item.get('emby_item_ids_json')
                # 兼容处理：可能是 list 对象，也可能是 json 字符串
                if ids_json:
                    if isinstance(ids_json, list) and len(ids_json) > 0:
                        emby_id = ids_json[0]
                    elif isinstance(ids_json, str):
                        try:
                            parsed = json.loads(ids_json)
                            if isinstance(parsed, list) and len(parsed) > 0:
                                emby_id = parsed[0]
                        except: pass

            # 判断状态
            status = 'missing'
            if item.get('in_library'):
                status = 'in_library'
                stats['in_library'] += 1
            elif item.get('subscription_status') == 'SUBSCRIBED':
                status = 'subscribed'
                stats['subscribed'] += 1
            elif item.get('subscription_status') == 'PAUSED':
                status = 'paused' # 暂停也算订阅的一种，或者单独统计
                stats['subscribed'] += 1
            else:
                if release_date_str and release_date_str > today_str:
                    status = 'unreleased'
                    stats['unreleased'] += 1
                else:
                    # 既不在库，也没订阅，且已上映 -> 缺失
                    stats['missing'] += 1

            final_movies.append({
                'tmdb_id': tmdb_id_str,
                'emby_id': emby_id,
                'title': item.get('title'),
                'poster_path': item.get('poster_path'),
                'release_date': release_date_str,
                'status': status
            })

        # 将统计结果注入到集合对象中，供前端使用
        coll['statistics'] = stats
        coll['movies'] = sorted(final_movies, key=lambda x: x.get('release_date') or '9999')
        
        # 清理
        coll.pop('all_tmdb_ids_json', None)
        coll.pop('parsed_ids', None)

    return all_collections
