# database/queries_db.py

import logging
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_sorted_ids(all_emby_ids, sort_by, sort_order):
    """
    【V5.7 新增】
    在本地 media_metadata 表中执行完整排序，返回所有排序后的 emby_id。
    增加了对 in_library = TRUE 的强制筛选。
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
        logger.warning(f"不支持的本地排序字段: '{sort_by}'，将返回原始ID顺序。")
        return all_emby_ids

    sort_column = sort_column_map[sort_by]
    order_direction = 'DESC' if sort_order.lower() == 'descending' else 'ASC'
    nulls_order = 'NULLS LAST' if order_direction == 'DESC' else 'NULLS FIRST'
    
    query = f"""
        SELECT emby_item_id
        FROM media_metadata
        WHERE emby_item_id IN %s AND in_library = TRUE
        ORDER BY {sort_column} {order_direction} {nulls_order}, title ASC;
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (tuple(all_emby_ids),))
                results = cursor.fetchall()
                return [row['emby_item_id'] for row in results]
    except Exception as e:
        logger.error(f"在本地数据库进行完整排序时出错: {e}", exc_info=True)
        return []