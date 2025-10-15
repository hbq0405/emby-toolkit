# database/queries_db.py

import logging
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_sorted_and_paginated_ids(all_emby_ids, sort_by, sort_order, limit, offset):
    """
    在本地 media_metadata 表中执行排序和分页，只返回一页的 emby_id。
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
        logger.warning(f"不支持的本地排序字段: '{sort_by}'，将返回原始顺序分页。")
        return all_emby_ids[offset : offset + limit]

    sort_column = sort_column_map[sort_by]
    order_direction = 'DESC' if sort_order.lower() == 'descending' else 'ASC'
    nulls_order = 'NULLS LAST' if order_direction == 'DESC' else 'NULLS FIRST'
    
    query = f"""
        SELECT emby_item_id
        FROM media_metadata
        WHERE emby_item_id IN %s
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