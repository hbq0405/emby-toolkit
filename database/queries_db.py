# database/queries_db.py
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_user_allowed_library_ids(user_id: str, emby_url: str, emby_api_key: str) -> List[str]:
    """
    辅助函数：调用 Emby API 获取指定用户有权访问的顶层 View ID 列表。
    主要用于反向代理层做缓存或兜底，核心查询逻辑已下沉到 SQL。
    """
    import requests
    try:
        # 使用 /Users/{Id}/Views 接口获取用户可见的顶层库
        url = f"{emby_url.rstrip('/')}/emby/Users/{user_id}/Views"
        resp = requests.get(url, params={'api_key': emby_api_key}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        return [item['Id'] for item in data.get('Items', [])]
    except Exception as e:
        logger.error(f"获取用户 {user_id} 媒体库权限失败: {e}")
        return []

def query_virtual_library_items(
    rules: List[Dict[str, Any]], 
    logic: str, 
    user_id: str,
    limit: int = 50, 
    offset: int = 0,
    sort_by: str = 'DateCreated',
    sort_order: str = 'Descending',
    item_types: List[str] = None,
    target_library_ids: List[str] = None,
    tmdb_ids: List[str] = None  
) -> Tuple[List[Dict[str, Any]], int]:
    """
    【核心函数】根据筛选规则 + 用户实时权限，查询媒体项。
    
    权限逻辑 (SQL层实现):
    1. 关联 emby_users 表获取 policy_json。
    2. 检查 EnableAllFolders 是否为 True。
    3. 检查 asset_details_json 中的 ancestor_ids 是否与 EnabledFolders 有交集。
    4. 检查 asset_details_json 中的 source_library_id 是否在 EnabledFolders 中 (兼容)。
    5. 检查 tags_json 是否包含 BlockedTags (黑名单)。
    
    返回: (items_list, total_count)
    """
    
    # 1. 基础 SQL 结构
    # 我们只查询 emby_item_ids_json[0] 作为 Emby ID 返回，代理层会去换取详情
    base_select = """
        SELECT 
            m.emby_item_ids_json->>0 as emby_id
        FROM media_metadata m
        JOIN emby_users u ON u.id = %s
    """
    
    base_count = """
        SELECT COUNT(*) 
        FROM media_metadata m
        JOIN emby_users u ON u.id = %s
    """
    
    params = [user_id]
    where_clauses = []

    # 2. 必须在库中
    where_clauses.append("m.in_library = TRUE")

    # 3. 类型过滤
    if item_types:
        where_clauses.append("m.item_type = ANY(%s)")
        params.append(item_types)

    # 4. 榜单类过滤
    if tmdb_ids:
        where_clauses.append("m.tmdb_id = ANY(%s)")
        params.append(tmdb_ids)

    # 5. 媒体库过滤
    if target_library_ids:
        # 建议使用更严谨的 EXISTS 语法，防止 asset_details_json 为空时报错
        lib_filter_sql = """
        EXISTS (
            SELECT 1 FROM jsonb_array_elements(m.asset_details_json) AS a 
            WHERE a->>'source_library_id' = ANY(%s)
        )
        """
        where_clauses.append(lib_filter_sql)
        params.append(target_library_ids)

    # ======================================================================
    # ★★★ 4. 权限控制 (核心逻辑) ★★★
    # ======================================================================
    
    # A. 文件夹/库权限
    # 逻辑：(允许所有) OR (祖先ID匹配) OR (来源库ID匹配)
    folder_perm_sql = """
    EXISTS (
        SELECT 1 
        FROM jsonb_array_elements(m.asset_details_json) AS asset
        WHERE 
            -- 1. 白名单检查
            (
                (u.policy_json->'EnableAllFolders' = 'true'::jsonb) -- 安全的布尔判断
                OR
                COALESCE(asset->'ancestor_ids', '[]'::jsonb) ?| ARRAY(
                    SELECT jsonb_array_elements_text(
                        CASE WHEN jsonb_typeof(u.policy_json->'EnabledFolders') = 'array' 
                             THEN u.policy_json->'EnabledFolders' 
                             ELSE '[]'::jsonb END
                    )
                )
                OR
                (asset->>'source_library_id') = ANY(
                    ARRAY(SELECT jsonb_array_elements_text(
                        CASE WHEN jsonb_typeof(u.policy_json->'EnabledFolders') = 'array' 
                             THEN u.policy_json->'EnabledFolders' 
                             ELSE '[]'::jsonb END
                    ))
                )
            )
            -- 2. 黑名单检查
            AND NOT (
                COALESCE(asset->'ancestor_ids', '[]'::jsonb) ?| ARRAY(
                    SELECT jsonb_array_elements_text(
                        CASE WHEN jsonb_typeof(u.policy_json->'ExcludedSubFolders') = 'array' 
                             THEN u.policy_json->'ExcludedSubFolders' 
                             ELSE '[]'::jsonb END
                    )
                )
            )
    )
    """
    where_clauses.append(folder_perm_sql)

    # B. 标签屏蔽 (黑名单)
    tag_block_sql = """
    NOT (
        COALESCE(m.tags_json, '[]'::jsonb) ?| ARRAY(
            SELECT jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(u.policy_json->'BlockedTags') = 'array' 
                     THEN u.policy_json->'BlockedTags' 
                     ELSE '[]'::jsonb END
            )
        )
    )
    """
    where_clauses.append(tag_block_sql)

    # C. 分级控制 (Parental Control)
    parental_control_sql = """
    (
        (u.policy_json->'MaxParentalRating' IS NULL)
        OR
        (
            m.unified_rating IS NOT NULL 
            AND m.unified_rating ~ '^[0-9]+$' -- 确保是数字
            AND (m.unified_rating)::int <= (u.policy_json->>'MaxParentalRating')::int
        )
    )
    AND NOT (
        (u.policy_json->'BlockUnratedItems' = 'true'::jsonb) -- 安全的布尔判断
        AND (
            m.unified_rating IS NULL 
            OR m.unified_rating = '' 
            OR (CASE WHEN m.unified_rating ~ '^[0-9]+$' THEN (m.unified_rating)::int ELSE 0 END) = 0
        )
    )
    """
    where_clauses.append(parental_control_sql)

    # ======================================================================
    # 5. 动态构建筛选规则 SQL
    # ======================================================================
    rule_clauses = []
    for rule in rules:
        field = rule.get('field')
        op = rule.get('operator')
        value = rule.get('value')
        
        if value is None or value == '':
            continue

        clause = None
        
        # --- 1. 风格流派 (JSONB 字符串数组) ---
        if field == 'genres':
            if op == 'contains' or op == 'eq':
                clause = "m.genres_json ? %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.genres_json ?| %s"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_none_of':
                clause = "NOT (m.genres_json ?| %s)"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 2. 标签 (JSONB 字符串数组) ---
        elif field == 'tags':
            if op == 'contains' or op == 'eq':
                clause = "m.tags_json ? %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.tags_json ?| %s"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_none_of':
                clause = "NOT (m.tags_json ?| %s)"
                params.append(list(value) if isinstance(value, list) else [value])
        
        # --- 3. 制作公司/制片厂 (JSONB 字符串数组) ---
        elif field == 'studios':
            if op == 'contains' or op == 'eq':
                clause = "m.studios_json ? %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.studios_json ?| %s"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 4. 国家地区 (JSONB 字符串数组) ---
        elif field == 'countries':
            if op == 'contains' or op == 'eq':
                clause = "m.countries_json ? %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.countries_json ?| %s"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 5. 导演 (适配前端对象数组格式) ---
        elif field == 'directors':
            ids = []
            names = []
            
            # 1. 解析前端传来的复杂 value (可能是对象数组，也可能是字符串)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get('id'):
                        ids.append(item['id'])
                    elif isinstance(item, str):
                        names.append(item)
            elif isinstance(value, dict) and value.get('id'):
                ids.append(value['id'])
            else:
                names.append(str(value))

            # 2. 构建 SQL
            if ids:
                # 匹配 ID 数组中的任意一个 (使用 JSONB 包含语法)
                # 构造类似: (m.directors_json @> '[{"id": 2710}]' OR m.directors_json @> '[{"id": 123}]')
                id_clauses = []
                for i in ids:
                    id_clauses.append("m.directors_json @> %s")
                    params.append(json.dumps([{"id": int(i)}]))
                clause = f"({' OR '.join(id_clauses)})"
            elif names:
                # 模糊匹配名字
                name_clauses = []
                for n in names:
                    name_clauses.append("EXISTS (SELECT 1 FROM jsonb_array_elements(m.directors_json) d WHERE d->>'name' ILIKE %s)")
                    params.append(f"%{n}%")
                clause = f"({' OR '.join(name_clauses)})"

        # --- 6. 演员 (适配前端对象数组格式) ---
        elif field == 'actors':
            ids = []
            names = []
            
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get('id'):
                        ids.append(item['id'])
                    elif isinstance(item, str):
                        names.append(item)
            elif isinstance(value, dict) and value.get('id'):
                ids.append(value['id'])
            else:
                names.append(str(value))

            if ids:
                # 演员表里存的是 tmdb_id
                id_clauses = []
                for i in ids:
                    id_clauses.append("m.actors_json @> %s")
                    params.append(json.dumps([{"tmdb_id": int(i)}]))
                clause = f"({' OR '.join(id_clauses)})"
            elif names:
                name_clauses = []
                for n in names:
                    name_clauses.append("""
                    EXISTS (
                        SELECT 1 FROM jsonb_array_elements(m.actors_json) act
                        JOIN person_identity_map p ON (act->>'tmdb_id')::int = p.tmdb_person_id
                        WHERE p.name ILIKE %s OR p.original_text ILIKE %s
                    )
                    """)
                    params.extend([f"%{n}%", f"%{n}%"])
                clause = f"({' OR '.join(name_clauses)})"

        # --- 7. 时长 (分钟，整数比较) ---
        elif field == 'runtime':
            try:
                val_int = int(value)
                if op == 'gte':
                    clause = "m.runtime_minutes >= %s"
                elif op == 'lte':
                    clause = "m.runtime_minutes <= %s"
                elif op == 'eq':
                    clause = "m.runtime_minutes = %s"
                if clause: params.append(val_int)
            except (ValueError, TypeError): pass

        # --- 8. 发行年份 (整数比较) ---
        elif field == 'release_year':
            try:
                val_int = int(value)
                if op == 'eq':
                    clause = "m.release_year = %s"
                elif op == 'gte':
                    clause = "m.release_year >= %s"
                elif op == 'lte':
                    clause = "m.release_year <= %s"
                if clause: params.append(val_int)
            except (ValueError, TypeError): pass
        
        # --- 9. 评分 (浮点数比较) ---
        elif field == 'rating':
            try:
                val_float = float(value)
                if op == 'gte':
                    clause = "m.rating >= %s"
                elif op == 'lte':
                    clause = "m.rating <= %s"
                if clause: params.append(val_float)
            except (ValueError, TypeError): pass
            
        # --- 10. 入库时间 (日期运算) ---
        elif field == 'date_added': 
            if op == 'in_last_days':
                try:
                    days = int(value)
                    clause = f"m.date_added >= NOW() - INTERVAL '{days} days'"
                except (ValueError, TypeError): pass
        
        # --- 11. 标题 (文本模糊匹配) ---
        elif field == 'title':
            if op == 'contains':
                clause = "m.title ILIKE %s"
                params.append(f"%{value}%")
            elif op == 'starts_with':
                clause = "m.title ILIKE %s"
                params.append(f"{value}%")
            elif op == 'eq':
                clause = "m.title = %s"
                params.append(value)

        # --- 12. 原始语言 (字符串匹配) ---
        elif field == 'original_language':
            if op == 'eq':
                clause = "m.original_language = %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.original_language = ANY(%s)"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 13. 剧情关键词 (JSONB 字符串数组) ---
        elif field == 'keywords':
            if op == 'contains':
                clause = "m.keywords_json ? %s"
                params.append(value)

        # --- 14. 是否跟播中 (对应数据库 watchlist_is_airing) ---
        elif field == 'is_in_progress':
            if op == 'is':
                # 直接匹配布尔值
                clause = "m.watchlist_is_airing = %s"
                params.append(value)

        if clause:
            rule_clauses.append(clause)

    # 6. 组合规则逻辑 (AND / OR)
    if rule_clauses:
        join_op = " AND " if logic.upper() == 'AND' else " OR "
        combined_rules = f"({join_op.join(rule_clauses)})"
        where_clauses.append(combined_rules)

    # 7. 最终 WHERE 组装
    full_where = " AND ".join(where_clauses)
    
    # 8. 排序映射
    sort_map = {
        'DateCreated': 'm.date_added',
        'DateLastContentAdded': 'm.last_synced_at',
        'DatePlayed': 'm.date_added',
        'SortName': 'm.title',
        'ProductionYear': 'm.release_year',
        'CommunityRating': 'm.rating',
        'PremiereDate': 'm.release_date',
        'Random': 'RANDOM()'
    }
    db_sort_col = sort_map.get(sort_by, 'm.date_added')
    
    # Random 排序不需要 ASC/DESC
    if db_sort_col == 'RANDOM()':
        db_sort_dir = ""
    else:
        db_sort_dir = "DESC" if sort_order == 'Descending' else "ASC"

    # 9. 执行查询
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # A. 获取总数 (用于分页)
                # 注意：count_sql 的参数和 query_sql 的前缀参数是一样的
                final_count_sql = f"{base_count} WHERE {full_where}"
                cursor.execute(final_count_sql, tuple(params))
                row = cursor.fetchone()
                total_count = row['count'] if row else 0

                if total_count == 0:
                    return [], 0

                # B. 获取分页数据
                final_query_sql = f"""
                    {base_select}
                    WHERE {full_where}
                    ORDER BY {db_sort_col} {db_sort_dir}
                    LIMIT %s OFFSET %s
                """
                # 添加分页参数
                query_params = params + [limit, offset]
                
                cursor.execute(final_query_sql, tuple(query_params))
                rows = cursor.fetchall()
                
                # 提取 Emby ID 列表并构造返回对象
                # 返回格式: [{'Id': 'xxx'}, {'Id': 'yyy'}]
                items = [{'Id': row['emby_id']} for row in rows if row['emby_id']]
                
                return items, total_count

    except Exception as e:
        logger.error(f"实时筛选查询失败: {e}", exc_info=True)
        return [], 0

def get_sorted_and_paginated_ids(
    item_ids: List[str], 
    sort_by: str, 
    sort_order: str, 
    limit: int, 
    offset: int
) -> List[str]:
    """
    辅助函数：对给定的 Emby ID 列表进行排序和分页。
    主要用于“个人推荐”或“榜单”类合集，这些合集的 ID 列表是预先计算好的，
    但需要根据前端请求进行排序和分页。
    """
    if not item_ids:
        return []

    # 排序映射
    sort_map = {
        'DateCreated': 'date_added',
        'SortName': 'title',
        'ProductionYear': 'release_year',
        'CommunityRating': 'rating',
        'PremiereDate': 'release_date',
        'Random': 'RANDOM()'
    }
    db_sort_col = sort_map.get(sort_by, 'date_added')
    
    if db_sort_col == 'RANDOM()':
        db_sort_dir = ""
    else:
        db_sort_dir = "DESC" if sort_order == 'Descending' else "ASC"

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 使用 jsonb_array_elements 展开 emby_item_ids_json 来匹配
                # 或者更简单：如果 emby_item_ids_json 包含 item_ids 中的任意一个
                
                # 既然我们已经有了明确的 Emby ID 列表，我们可以反查 media_metadata
                # 注意：media_metadata 存的是 JSON 数组，我们需要匹配数组里包含该 ID 的记录
                
                sql = f"""
                    SELECT emby_item_ids_json->>0 as emby_id
                    FROM media_metadata
                    WHERE emby_item_ids_json ?| %s -- 检查 JSON 数组是否包含列表中的任意 ID
                    ORDER BY {db_sort_col} {db_sort_dir}
                    LIMIT %s OFFSET %s
                """
                
                cursor.execute(sql, (item_ids, limit, offset))
                rows = cursor.fetchall()
                
                return [row['emby_id'] for row in rows]

    except Exception as e:
        logger.error(f"对 ID 列表进行排序分页失败: {e}", exc_info=True)
        # 出错时回退到简单的切片（无排序）
        return item_ids[offset : offset + limit]