# database/queries_db.py

import logging
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_sorted_and_paginated_ids(all_emby_ids, sort_by, sort_order, limit, offset):
    """
    在本地 media_metadata 表中执行排序和分页，只返回一页的 emby_id。
    V5.6 版：增加了对 in_library = TRUE 的强制筛选，确保不返回“幽灵ID”。
    """
    if not all_emby_ids:
        return []

    sort_column_map = {
        'PremiereDate': 'release_date',
        'DateCreated': 'date_added',
        'CommunityRating': 'rating',
        'ProductionYear': 'release_year',
        'SortName': 'title'
    }

    if sort_by not in sort_column_map:
        logger.trace(f"  ➜ 不支持的本地排序字段: '{sort_by}'，将返回原始顺序分页。")
        # 即使是原始分页，也需要过滤掉不在库中的项目
        # (这一步其实上层已经保证了，但作为兜底更安全)
        return all_emby_ids[offset : offset + limit]

    sort_column = sort_column_map[sort_by]
    order_direction = 'DESC' if sort_order.lower() == 'descending' else 'ASC'
    nulls_order = 'NULLS LAST' if order_direction == 'DESC' else 'NULLS FIRST'
    
    # ★★★ 核心修正：在 WHERE 子句中，强制要求 in_library = TRUE ★★★
    query = f"""
        SELECT emby_item_id
        FROM media_metadata
        WHERE emby_item_id IN %s AND in_library = TRUE
        ORDER BY {sort_column} {order_direction} {nulls_order}, title ASC
        LIMIT %s OFFSET %s;
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (tuple(all_emby_ids), limit, offset))
                results = cursor.fetchall()
                return [row['emby_item_id'] for row in results]
    except Exception as e:
        logger.error(f"在本地数据库排序分页时出错: {e}", exc_info=True)
        return []