# database/queries_db.py
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def _expand_keyword_labels(value) -> List[str]:
    """将中文标签展开为英文关键词列表"""
    from database import settings_db
    mapping = settings_db.get_setting('keyword_mapping') or {}
    
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
        
        # 基础校验：跳过空值
        if value is None or value == '' or (isinstance(value, list) and len(value) == 0):
            continue

        clause = None
        
        # --- 1. 基础 JSONB 数组类型 ---
        jsonb_array_fields = ['genres', 'tags', 'studios', 'countries'] # 👈 删掉 keywords
        if field in jsonb_array_fields:
            column = f"m.{field}_json"
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

        # --- 2. 关键词 (Keywords) 专项处理 ★★★ ---
        elif field == 'keywords':
            # 调用上面的展开函数，把 "怪兽" 变成 ["monster"]
            expanded_keywords = _expand_keyword_labels(value)
            
            if not expanded_keywords:
                continue

            if op in ['contains', 'is_one_of', 'eq']:
                # SQL 变成: keywords_json ?| ARRAY['monster', 'disaster']
                clause = "m.keywords_json ?| %s"
                params.append(expanded_keywords)
            elif op == 'is_none_of':
                clause = "NOT (m.keywords_json ?| %s)"
                params.append(expanded_keywords)

        # --- 3. 复杂对象数组 (actors, directors) ---
        # 数据库存储格式: [{"id": 123, "name": "..."}] 或 [{"tmdb_id": 123, ...}]
        elif field in ['actors', 'directors']:
            # 提取 ID 列表 (适配前端传来的对象数组)
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
            
            # ✨ 核心修改：处理“主要是”逻辑 (取前三名)
            if op == 'is_primary':
                # 逻辑：展开数组并带上序号(ord)，只取序号 <= 3 的元素进行匹配
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements(m.{field}_json) WITH ORDINALITY AS t(elem, ord) 
                    WHERE t.ord <= 3 AND (t.elem->>'{id_key}')::int = ANY(%s)
                )
                """
                params.append(ids)
                
            elif op in ['contains', 'is_one_of', 'eq']:
                # 全表扫描（只要在演职员表里就行）
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements(m.{field}_json) elem WHERE (elem->>'{id_key}')::int = ANY(%s))"
                params.append(ids)
                
            elif op == 'is_none_of':
                clause = f"NOT EXISTS (SELECT 1 FROM jsonb_array_elements(m.{field}_json) elem WHERE (elem->>'{id_key}')::int = ANY(%s))"
                params.append(ids)

        # --- 4. 家长分级 (unified_rating - 字符串匹配) ---
        # 根据你的图片，这里存的是“青少年”、“成人”等中文
        elif field == 'unified_rating':
            if op == 'eq':
                clause = "m.unified_rating = %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.unified_rating = ANY(%s)"
                params.append(list(value) if isinstance(value, list) else [value])
            elif op == 'is_none_of':
                clause = "m.unified_rating IS NOT NULL AND NOT (m.unified_rating = ANY(%s))"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 5. 数值比较 (runtime, release_year, rating) ---
        elif field in ['runtime', 'release_year', 'rating']:
            col_map = {'runtime': 'm.runtime_minutes', 'release_year': 'm.release_year', 'rating': 'm.rating'}
            column = col_map[field]
            try:
                val = float(value)
                if op == 'gte': clause = f"{column} >= %s"
                elif op == 'lte': clause = f"{column} <= %s"
                elif op == 'eq': clause = f"{column} = %s"
                if clause: params.append(val)
            except (ValueError, TypeError): continue

        # --- 6. 日期偏移 (date_added, release_date) ---
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

        # --- 7. 文本模糊匹配 (title) ---
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

        # --- 8. 原始语言 (original_language) ---
        elif field == 'original_language':
            if op == 'eq':
                clause = "m.original_language = %s"
                params.append(value)
            elif op == 'is_one_of':
                clause = "m.original_language = ANY(%s)"
                params.append(list(value) if isinstance(value, list) else [value])

        # --- 9. 追剧状态 (is_in_progress) ---
        elif field == 'is_in_progress':
            if op == 'is':
                clause = "m.watchlist_is_airing = %s"
                params.append(bool(value))

        # --- 10. 视频流属性筛选 (分辨率、质量、特效、编码) ---
        asset_map = {
            'resolution': 'resolution_display',
            'quality': 'quality_display',
            'effect': 'effect_display',
            'codec': 'codec_display'
        }

        if field in asset_map:
            json_key = asset_map[field]
            if op == 'eq':
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements(m.asset_details_json) a WHERE a->>'{json_key}' = %s)"
                params.append(value)
            elif op == 'is_one_of':
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements(m.asset_details_json) a WHERE a->>'{json_key}' = ANY(%s))"
                params.append(list(value))
            elif op == 'is_none_of':
                clause = f"NOT EXISTS (SELECT 1 FROM jsonb_array_elements(m.asset_details_json) a WHERE a->>'{json_key}' = ANY(%s))"
                params.append(list(value))

        # --- 11. 音轨筛选 (全部改为匹配 audio_display 字符串) ---
        elif field == 'audio_lang':
            # 因为 audio_display 是 "国语, 英语" 这种格式，所以用 ILIKE 匹配
            if op in ['contains', 'eq']:
                clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements(m.asset_details_json) a WHERE a->>'audio_display' ILIKE %s)"
                params.append(f"%{value}%")
            elif op == 'is_one_of':
                # 如果是多选，构造多个 ILIKE 的 OR 关系
                sub_clauses = []
                for val in (value if isinstance(value, list) else [value]):
                    sub_clauses.append(f"a->>'audio_display' ILIKE %s")
                    params.append(f"%{val}%")
                
                if sub_clauses:
                    clause = f"EXISTS (SELECT 1 FROM jsonb_array_elements(m.asset_details_json) a WHERE ({' OR '.join(sub_clauses)}))"

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
                items = [
                    {
                        'Id': row['emby_id'], 
                        'tmdb_id': row['tmdb_id']  # 加上这一行
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