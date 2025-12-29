# database/queries_db.py
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def _expand_keyword_labels(value) -> List[str]:
    """将中文标签展开为英文关键词列表"""
    from database import settings_db
    mapping_data = settings_db.get_setting('keyword_mapping') or []
    
    # 兼容处理：如果是新版 List 格式，转为 Dict 以便查找
    mapping = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping = mapping_data
    
    target_en_keywords = []
    labels = value if isinstance(value, list) else [value]
    
    for label in labels:
        if label in mapping:
            # 拿到 en 列表: ["monster"]
            target_en_keywords.extend(mapping[label].get('en', []))
        else:
            # 没映射的才保留原词
            target_en_keywords.append(label)
    return list(set(filter(None, target_en_keywords)))

def _expand_studio_labels(value) -> List[str]:
    """
    【新增】将中文工作室简称展开为英文原名列表
    例如: 输入 "漫威" -> 返回 ["Marvel Studios"]
    """
    from database import settings_db
    mapping_data = settings_db.get_setting('studio_mapping') or []
    
    # 兼容处理：List 转 Dict
    mapping = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping = mapping_data

    target_en_names = []
    labels = value if isinstance(value, list) else [value]
    
    for label in labels:
        if label in mapping:
            # 拿到 en 列表
            target_en_names.extend(mapping[label].get('en', []))
        else:
            # 没映射的保留原词 (支持用户直接搜英文的情况)
            target_en_names.append(label)
            
    return list(set(filter(None, target_en_names)))

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
    user_id: Optional[str],
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
    """
    
    # 1. 基础 SQL 结构
    if user_id:
        base_select = """
            SELECT 
                m.emby_item_ids_json->>0 as emby_id,
                m.tmdb_id
            FROM media_metadata m
            JOIN emby_users u ON u.id = %s
        """
        base_count = """
            SELECT COUNT(*) 
            FROM media_metadata m
            JOIN emby_users u ON u.id = %s
        """
        params = [user_id]
    else:
        base_select = """
            SELECT 
                m.emby_item_ids_json->>0 as emby_id,
                m.tmdb_id
            FROM media_metadata m
        """
        base_count = """
            SELECT COUNT(*) 
            FROM media_metadata m
        """
        params = []

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
        lib_filter_sql = """
        EXISTS (
            SELECT 1 FROM jsonb_array_elements(COALESCE(m.asset_details_json, '[]'::jsonb)) AS a 
            WHERE a->>'source_library_id' = ANY(%s)
        )
        """
        where_clauses.append(lib_filter_sql)
        params.append(target_library_ids)

    # ======================================================================
    # ★★★ 4. 权限控制 (核心逻辑) ★★★
    # ======================================================================
    
    if user_id:
        # A. 文件夹/库权限
        folder_perm_sql = """
        EXISTS (
            SELECT 1 
            FROM jsonb_array_elements(COALESCE(m.asset_details_json, '[]'::jsonb)) AS asset
            WHERE 
                (
                    (u.policy_json->'EnableAllFolders' = 'true'::jsonb)
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

        # B. 标签屏蔽
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

        # C. 分级控制
        parental_control_sql = """
    (
        (u.policy_json->'MaxParentalRating' IS NULL)
        OR
        (
            m.official_rating IS NOT NULL 
            AND (
                CASE 
                    WHEN m.official_rating = 'PG-13' THEN 7
                    WHEN m.official_rating = 'TV-PG' THEN 7 
                    WHEN m.official_rating = 'TV-Y7' THEN 7
                    WHEN m.official_rating = 'TV-14' THEN 8
                    WHEN m.official_rating IN ('G', 'TV-G', 'TV-Y') THEN 0
                    WHEN m.official_rating = 'PG' THEN 6 
                    WHEN m.official_rating = 'R' THEN 17
                    WHEN m.official_rating = 'TV-MA' THEN 17
                    WHEN m.official_rating = 'NC-17' THEN 18
                    WHEN m.official_rating IN ('X', 'XXX', 'AO') THEN 18
                    WHEN m.official_rating IN ('NR', 'UR', 'Unrated', 'Not Rated') THEN 0
                    ELSE COALESCE(
                        NULLIF(REGEXP_REPLACE(m.official_rating, '[^0-9]', '', 'g'), ''), 
                        '0'
                    )::int
                END
            ) <= (u.policy_json->>'MaxParentalRating')::int
        )
    )
    AND NOT (
        (
            jsonb_typeof(u.policy_json->'BlockUnratedItems') = 'array'
            AND
            u.policy_json->'BlockUnratedItems' @> to_jsonb(m.item_type)
        )
        AND
        (
            m.official_rating IS NULL 
            OR m.official_rating = '' 
            OR m.official_rating IN ('NR', 'UR', 'Unrated', 'Not Rated')
            OR (
                m.official_rating NOT IN (
                    'G','PG','PG-13','R','NC-17','X','XXX','AO',
                    'TV-Y','TV-Y7','TV-G','TV-PG','TV-14','TV-MA'
                )
                AND REGEXP_REPLACE(m.official_rating, '[^0-9]', '', 'g') = ''
            )
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
        
        if value is None or value == '' or (isinstance(value, list) and len(value) == 0):
            continue

        clause = None
        
        # --- 1. 基础 JSONB 数组类型 (Genres, Tags, Countries) ---
        # ★★★ 修改：移除 studios，因为它需要特殊处理 ★★★
        jsonb_array_fields = ['genres', 'tags', 'countries']
        if field in jsonb_array_fields:
            column = f"COALESCE(m.{field}_json, '[]'::jsonb)"
            if op in ['contains', 'eq']:
                clause = f"{column} ? %s"
                params.append(str(value))
            elif op == 'is_one_of':
                clause = f"{column} ?| %s"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_none_of':
                clause = f"NOT ({column} ?| %s)"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_primary':
                clause = f"{column}->>0 = %s"
                params.append(str(value))

        # --- 2. 关键词 (Keywords) ---
        elif field == 'keywords':
            expanded_keywords = _expand_keyword_labels(value)
            if not expanded_keywords: continue
            
            # 1. 搜索词转小写
            search_terms = [str(k).lower() for k in expanded_keywords]
            
            column = "COALESCE(m.keywords_json, '[]'::jsonb)"
            
            # 2. 【优化】直接提取 name 字段并转小写匹配
            # 既然数据已清洗，不再需要 jsonb_typeof 判断
            if op in ['contains', 'is_one_of', 'eq']:
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) k 
                    WHERE LOWER(k->>'name') = ANY(%s)
                )
                """
                params.append(search_terms)
            elif op == 'is_none_of':
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) k 
                    WHERE LOWER(k->>'name') = ANY(%s)
                )
                """
                params.append(search_terms)

        # --- 3. 工作室 (Studios) ---
        elif field == 'studios':
            expanded_studios = _expand_studio_labels(value)
            if not expanded_studios: continue
            
            # 1. 搜索词转小写
            search_terms = [str(s).lower() for s in expanded_studios]
            
            column = "COALESCE(m.studios_json, '[]'::jsonb)"
            
            # 2. 【优化】直接提取 name 字段并转小写匹配
            if op in ['contains', 'is_one_of', 'eq']:
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) s 
                    WHERE LOWER(s->>'name') = ANY(%s)
                )
                """
                params.append(search_terms)
            elif op == 'is_none_of':
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) s 
                    WHERE LOWER(s->>'name') = ANY(%s)
                )
                """
                params.append(search_terms)
            elif op == 'is_primary':
                # 主工作室是数组第一个
                clause = f"LOWER({column}->0->>'name') = ANY(%s)"
                params.append(search_terms)

        # --- 4. 复杂对象数组 (Actors, Directors) ---
        elif field in ['actors', 'directors']:
            ids = []
            if isinstance(value, list):
                ids = [item['id'] if isinstance(item, dict) else item for item in value]
            elif isinstance(value, dict):
                ids = [value.get('id')]
            else:
                ids = [value]
            ids = [int(i) for i in ids if str(i).isdigit()]
            if not ids: continue

            id_key = 'tmdb_id' if field == 'actors' else 'id'
            safe_column = f"COALESCE(m.{field}_json, '[]'::jsonb)"

            if op == 'is_primary':
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({safe_column}) WITH ORDINALITY AS t(elem, ord) 
                    WHERE t.ord <= 3 AND (t.elem->>'{id_key}')::int = ANY(%s)
                )
                """
                params.append(ids)
            elif op in ['contains', 'is_one_of', 'eq']:
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements({safe_column}) elem WHERE (elem->>'{id_key}')::int = ANY(%s))"
                params.append(ids)
            elif op == 'is_none_of':
                clause = f"NOT EXISTS (SELECT 1 FROM jsonb_array_elements({safe_column}) elem WHERE (elem->>'{id_key}')::int = ANY(%s))"
                params.append(ids)

        # --- 5. 家长分级 (Unified Rating) ---
        elif field == 'unified_rating':
            if op == 'eq':
                clause = "m.unified_rating = %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.unified_rating = ANY(%s)"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_none_of':
                clause = "(m.unified_rating IS NULL OR NOT (m.unified_rating = ANY(%s)))"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 6. 数值比较 (Runtime, Year, Rating) ---
        elif field == 'runtime':
            try:
                val = float(value)
                runtime_logic = """
                (CASE
                    WHEN m.item_type = 'Series' THEN (
                        SELECT COALESCE(AVG(ep.runtime_minutes), 0)
                        FROM media_metadata ep
                        WHERE ep.parent_series_tmdb_id = m.tmdb_id 
                          AND ep.item_type = 'Episode'
                          AND ep.runtime_minutes > 0
                    )
                    ELSE COALESCE(m.runtime_minutes, 0)
                END)
                """
                if op == 'gte': clause = f"{runtime_logic} >= %s"
                elif op == 'lte': clause = f"{runtime_logic} <= %s"
                elif op == 'eq': clause = f"{runtime_logic} = %s"
                if clause: params.append(val)
            except (ValueError, TypeError): continue

        elif field in ['release_year', 'rating']:
            col_map = {'release_year': 'm.release_year', 'rating': 'm.rating'}
            column = col_map[field]
            try:
                val = float(value)
                safe_col = f"COALESCE({column}, 0)"
                if op == 'gte': clause = f"{safe_col} >= %s"
                elif op == 'lte': clause = f"{safe_col} <= %s"
                elif op == 'eq': clause = f"{safe_col} = %s"
                if clause: params.append(val)
            except (ValueError, TypeError): continue

        # --- 7. 日期偏移 ---
        elif field in ['date_added', 'release_date']:
            column = f"m.{field}"
            try:
                days = int(value)
                if op == 'in_last_days':
                    clause = f"{column} >= NOW() - INTERVAL '%s days'"
                elif op == 'not_in_last_days':
                    clause = f"{column} < NOW() - INTERVAL '%s days'"
                if clause: params.append(days)
            except (ValueError, TypeError): continue

        # --- 8. 文本模糊匹配 ---
        elif field == 'title':
            if op == 'contains':
                clause = "m.title ILIKE %s"
                params.append(f"%{value}%")
            elif op == 'starts_with':
                clause = "m.title ILIKE %s"
                params.append(f"{value}%")
            elif op == 'ends_with':
                clause = "m.title ILIKE %s"
                params.append(f"%{value}")
            elif op == 'eq':
                clause = "m.title = %s"
                params.append(value)
            elif op == 'does_not_contain':
                clause = "m.title NOT ILIKE %s"
                params.append(f"%{value}%")

        # --- 9. 原始语言 ---
        elif field == 'original_language':
            if op == 'eq':
                clause = "m.original_language = %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.original_language = ANY(%s)"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 10. 追剧状态 ---
        elif field == 'is_in_progress':
            if op == 'is':
                clause = "m.watchlist_is_airing = %s"
                params.append(bool(value))

        # --- 11. 视频流属性 ---
        asset_map = {
            'resolution': 'resolution_display',
            'quality': 'quality_display',
            'effect': 'effect_display',
            'codec': 'codec_display'
        }
        if field in asset_map:
            json_key = asset_map[field]
            safe_assets = "COALESCE(m.asset_details_json, '[]'::jsonb)"
            
            if op == 'eq':
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements({safe_assets}) a WHERE a->>'{json_key}' = %s)"
                params.append(value)
            elif op == 'is_one_of':
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements({safe_assets}) a WHERE a->>'{json_key}' = ANY(%s))"
                params.append(list(value))
            elif op == 'is_none_of':
                clause = f"NOT EXISTS (SELECT 1 FROM jsonb_array_elements({safe_assets}) a WHERE a->>'{json_key}' = ANY(%s))"
                params.append(list(value))

        # --- 12. 音轨筛选 ---
        elif field == 'audio_lang':
            safe_assets = "COALESCE(m.asset_details_json, '[]'::jsonb)"
            if op in ['contains', 'eq']:
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements({safe_assets}) a WHERE a->>'audio_display' ILIKE %s)"
                params.append(f"%{value}%")
            elif op == 'is_one_of':
                sub_clauses = []
                for val in (value if isinstance(value, list) else [value]):
                    sub_clauses.append(f"a->>'audio_display' ILIKE %s")
                    params.append(f"%{val}%")
                if sub_clauses:
                    clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements({safe_assets}) a WHERE ({' OR '.join(sub_clauses)}))"

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
        'DatePlayed': 'm.date_added',
        'SortName': 'm.title',
        'ProductionYear': 'm.release_year',
        'CommunityRating': 'm.rating',
        'PremiereDate': 'm.release_date',
        'Random': 'RANDOM()'
    }
    db_sort_col = sort_map.get(sort_by, 'm.date_added')
    
    if db_sort_col == 'RANDOM()':
        db_sort_dir = ""
    else:
        db_sort_dir = "DESC" if sort_order == 'Descending' else "ASC"

    # 9. 执行查询
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                final_count_sql = f"{base_count} WHERE {full_where}"
                cursor.execute(final_count_sql, tuple(params))
                row = cursor.fetchone()
                total_count = row['count'] if row else 0

                if total_count == 0:
                    return [], 0

                final_query_sql = f"""
                    {base_select}
                    WHERE {full_where}
                    ORDER BY {db_sort_col} {db_sort_dir}
                    LIMIT %s OFFSET %s
                """
                query_params = params + [limit, offset]
                
                cursor.execute(final_query_sql, tuple(query_params))
                rows = cursor.fetchall()
                
                items = [
                    {
                        'Id': row['emby_id'], 
                        'tmdb_id': row['tmdb_id']
                    } 
                    for row in rows if row['emby_id']
                ]
                
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
    """
    if not item_ids:
        return []

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
                sql = f"""
                    SELECT emby_item_ids_json->>0 as emby_id
                    FROM media_metadata
                    WHERE emby_item_ids_json ?| %s 
                    ORDER BY {db_sort_col} {db_sort_dir}
                    LIMIT %s OFFSET %s
                """
                
                cursor.execute(sql, (item_ids, limit, offset))
                rows = cursor.fetchall()
                
                return [row['emby_id'] for row in rows]

    except Exception as e:
        logger.error(f"对 ID 列表进行排序分页失败: {e}", exc_info=True)
        return item_ids[offset : offset + limit]
    
def get_missing_items_metadata(tmdb_ids: List[str]) -> Dict[str, Dict]:
    if not tmdb_ids: return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT tmdb_id, subscription_status, title, release_year, 
                           release_date, item_type, poster_path, emby_item_ids_json 
                    FROM media_metadata 
                    WHERE tmdb_id = ANY(%s) AND item_type IN ('Movie', 'Series')
                """, (tmdb_ids,))
                rows = cursor.fetchall()
                return {str(r['tmdb_id']): dict(r) for r in rows}
    except Exception as e:
        logger.error(f"获取缺失项元数据失败: {e}")
        return {}