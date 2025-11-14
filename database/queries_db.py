# database/queries_db.py

import logging
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_sorted_and_paginated_ids(all_emby_ids, sort_by, sort_order, limit, offset):
    """
    在本地 media_metadata 表中执行排序和分页，只返回一页的 emby_id。
    V5.7 版：适配 emby_item_ids_json 字段，能够查询 JSON 数组内的 ID。
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
        return all_emby_ids[offset : offset + limit]

    sort_column = sort_column_map[sort_by]
    order_direction = 'DESC' if sort_order.lower() == 'descending' else 'ASC'
    nulls_order = 'NULLS LAST' if order_direction == 'DESC' else 'NULLS FIRST'
    
    # ★★★ 核心修正：重写 SQL 查询以处理 JSONB 数组 ★★★
    query = f"""
        SELECT emby_id
        FROM 
            media_metadata, 
            jsonb_array_elements_text(emby_item_ids_json) as emby_id
        WHERE 
            emby_id IN %s AND in_library = TRUE
        ORDER BY 
            {sort_column} {order_direction} {nulls_order}, title ASC
        LIMIT %s OFFSET %s;
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (tuple(all_emby_ids), limit, offset))
                results = cursor.fetchall()
                # ★★★ 核心修正：返回的列名现在是 emby_id ★★★
                return [row['emby_id'] for row in results]
    except Exception as e:
        logger.error(f"在本地数据库排序分页时出错: {e}", exc_info=True)
        # 增加一个回退机制，避免在数据库查询失败时前端完全卡死
        logger.warning("数据库排序失败，将回退到内存分页。")
        return all_emby_ids[offset : offset + limit]