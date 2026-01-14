# database/queries_db.py
import logging
from typing import List, Dict, Any, Optional, Tuple
from .connection import get_db_connection
from database import settings_db
import utils

logger = logging.getLogger(__name__)

def _expand_keyword_labels(value) -> Dict[str, List]:
    """
    将中文标签展开为 { 'ids': [...], 'names': [...] }
    """
    mapping_data = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
    
    mapping = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping = mapping_data
    
    target_ids = []
    target_names = []
    
    labels = value if isinstance(value, list) else [value]
    
    for label in labels:
        # 1. 尝试从映射表中找
        if label in mapping:
            item = mapping[label]
            # 收集 ID (转为字符串以便 SQL 处理)
            if item.get('ids'):
                target_ids.extend([str(i) for i in item['ids']])
            # 收集英文名
            if item.get('en'):
                target_names.extend(item['en'])
        else:
            # 2. 没映射，保留原词作为 Name
            target_names.append(label)
            
    return {
        'ids': list(set(target_ids)),
        'names': list(set(filter(None, target_names)))
    }

def _expand_studio_labels(value) -> Dict[str, List]:
    """
    将中文工作室展开为 { 'ids': [...], 'names': [...] }
    兼容 network_ids 和 company_ids
    """
    mapping_data = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
    
    mapping = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping = mapping_data

    target_ids = []
    target_names = []
    
    labels = value if isinstance(value, list) else [value]
    
    for label in labels:
        if label in mapping:
            item = mapping[label]
            
            # ★★★ 核心修改：同时收集三种可能的 ID 字段 ★★★
            # 这样本地筛选时，无论 Emby 存的是哪种 ID，都能命中
            if item.get('ids'):
                target_ids.extend([str(i) for i in item['ids']])
            if item.get('network_ids'):
                target_ids.extend([str(i) for i in item['network_ids']])
            if item.get('company_ids'):
                target_ids.extend([str(i) for i in item['company_ids']])
                
            if item.get('en'):
                target_names.extend(item['en'])
        else:
            target_names.append(label)
            
    return {
        'ids': list(set(target_ids)),
        'names': list(set(filter(None, target_names)))
    }

def _expand_rating_labels(labels: List[str]) -> List[str]:
    """
    将中文分级标签（如 '青少年'）反向展开为所有对应的原始分级代码（如 'PG-13', 'TV-14', '12'）。
    """
    # 1. 获取映射表
    mapping_data = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
    
    target_codes = set()
    
    # 2. 遍历所有国家的规则，寻找匹配的 label
    # mapping_data 结构: { "US": [{"code": "R", "label": "限制级"}, ...], ... }
    for country, rules in mapping_data.items():
        for rule in rules:
            if rule.get('label') in labels:
                if rule.get('code'):
                    target_codes.add(rule['code'])
    
    return list(target_codes)

def _build_rating_value_sql(rating_expr: str) -> str:
    """
    根据配置动态生成分级值转换 SQL
    """
    mapping_data = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
    if not mapping_data:
        from utils import DEFAULT_RATING_MAPPING
        mapping_data = DEFAULT_RATING_MAPPING

    whens = []
    for country, rules in mapping_data.items():
        for rule in rules:
            code = rule.get('code')
            val = rule.get('emby_value')
            if code and val is not None:
                safe_code = code.replace("'", "''")
                whens.append(f"WHEN {rating_expr} = '{safe_code}' THEN {val}")

    else_logic = f"COALESCE(NULLIF(REGEXP_REPLACE({rating_expr}, '[^0-9]', '', 'g'), ''), '0')::int"
    
    if not whens:
        return else_logic

    return f"CASE {chr(10).join(whens)} ELSE {else_logic} END"

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
    tmdb_ids: List[str] = None,
    max_rating_override: Optional[int] = None  
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
    # ★★★ 4. 权限控制 (修复版) ★★★
    # ======================================================================
    
    # 1. 动态构建分级取值表达式
    # 逻辑：
    # 1. 优先取 m.custom_rating (如果非空)
    # 2. 其次根据配置的 rating_priority 顺序取 m.official_rating_json
    # 3. 兜底取 JSON 中任意值
    
    # A. 获取用户配置的优先级 (如果没有配置，使用默认列表)
    priority_list = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY
    
    # B. 构建 SQL 字段列表
    rating_sql_parts = ["NULLIF(m.custom_rating, '')"] # 1. 人工自定义最优先
    
    # ★★★ 核心修复：确立 US 分级的宗主地位 ★★★
    # 因为我们在入库时已经把所有逻辑（包括成人拦截、多国映射）都浓缩进了 US 字段。
    # 所以 SQL 查询时，必须优先读取 US，否则会读到未处理的原产国分级（如 DE:18），导致权限逃逸。
    rating_sql_parts.append("m.official_rating_json->>'US'") 

    for p in priority_list:
        # 避免重复添加 US
        if p == 'US': continue
        
        if p == 'ORIGIN':
            # 特殊处理原产国
            rating_sql_parts.append("m.official_rating_json->>(m.countries_json->>0)")
        else:
            # 标准国家代码
            rating_sql_parts.append(f"m.official_rating_json->>'{p}'")
            
    # C. 添加最终兜底 (取 JSON 里随便一个值，防止前面都匹配不到导致 NULL)
    rating_sql_parts.append("(SELECT value FROM jsonb_each_text(m.official_rating_json) LIMIT 1)")
    
    # D. 组合成 COALESCE 语句
    rating_expr = f"""
    COALESCE(
        {', '.join(rating_sql_parts)}
    )
    """

    # --- A. 处理分级数值限制 (Rating Value Limit) ---
    # 逻辑：未分级(Unrated/None) 默认为 0 (安全)。
    # 只要 (计算出的分级值 <= 限制值) 即放行。
    
    limit_value_sql = None
    
    if max_rating_override is not None:
        # 情况1: 强制覆盖 (封面生成器)
        limit_value_sql = str(max_rating_override)
    elif user_id:
        # 情况2: 使用用户策略
        limit_value_sql = "(u.policy_json->>'MaxParentalRating')::int"
    
    if limit_value_sql:
        # 计算分级数值 (利用 _build_rating_value_sql 的逻辑：映射不到或非数字则为0)
        rating_value_calc_sql = _build_rating_value_sql(rating_expr)
        
        rating_limit_sql = f"""
        (
            -- 1. 如果限制值本身为空(无限制)，则通过
            ({limit_value_sql} IS NULL)
            OR
            -- 2. 数值比较：分级值 <= 限制值
            -- 未分级/无分级 内容会被计算为 0。
            -- 0 <= 8 (PG-13) -> TRUE (通过)
            -- 9 (R) <= 8 (PG-13) -> FALSE (拦截)
            (({rating_value_calc_sql}) <= {limit_value_sql})
        )
        """
        where_clauses.append(rating_limit_sql)

    # --- B. 处理用户专属逻辑 (依赖 emby_users 表) ---
    if user_id:
        # 1. 文件夹/库权限 (保持原样)
        folder_perm_sql = """
        EXISTS (
            SELECT 1 
            FROM jsonb_array_elements(COALESCE(m.asset_details_json, '[]'::jsonb)) AS asset
            WHERE 
                (
                    (u.policy_json->'EnableAllFolders' = 'true'::jsonb)
                    OR
                    COALESCE(asset->'ancestor_ids', '[]'::jsonb) ?| ARRAY(
                        SELECT jsonb_array_elements_text(COALESCE(u.policy_json->'EnabledFolders', '[]'::jsonb))
                    )
                    OR
                    (asset->>'source_library_id') = ANY(
                        ARRAY(SELECT jsonb_array_elements_text(COALESCE(u.policy_json->'EnabledFolders', '[]'::jsonb)))
                    )
                )
                AND NOT (
                    COALESCE(asset->'ancestor_ids', '[]'::jsonb) ?| ARRAY(
                        SELECT jsonb_array_elements_text(COALESCE(u.policy_json->'ExcludedSubFolders', '[]'::jsonb))
                    )
                )
        )
        """
        where_clauses.append(folder_perm_sql)

        # 2. 标签屏蔽 (保持原样)
        tag_block_sql = """
        NOT (
            COALESCE(m.tags_json, '[]'::jsonb) ?| ARRAY(
                SELECT jsonb_array_elements_text(COALESCE(u.policy_json->'BlockedTags', '[]'::jsonb))
            )
        )
        """
        where_clauses.append(tag_block_sql)

        # 3. 屏蔽未分级内容 (BlockUnratedItems)
        # ★ 注意：这个逻辑必须放在 if user_id 里，因为它依赖 u.policy_json
        block_unrated_sql = f"""
        NOT (
            (
                jsonb_typeof(u.policy_json->'BlockUnratedItems') = 'array'
                AND
                u.policy_json->'BlockUnratedItems' @> to_jsonb(m.item_type)
            )
            AND
            (
                {rating_expr} IS NULL 
                OR {rating_expr} = '' 
                OR {rating_expr} IN ('NR', 'UR', 'Unrated', 'Not Rated')
                OR (
                    {rating_expr} NOT IN (
                        'G','PG','PG-13','R','NC-17','X','XXX','AO',
                        'TV-Y','TV-Y7','TV-G','TV-PG','TV-14','TV-MA'
                    )
                    AND REGEXP_REPLACE({rating_expr}, '[^0-9]', '', 'g') = ''
                )
            )
        )
        """
        where_clauses.append(block_unrated_sql)

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
        jsonb_array_fields = ['tags', 'countries']
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

        # --- 2.类型 (Genres) - 对象列表处理 ---
        elif field == 'genres':
            # 目标值可能是单个字符串，也可能是列表
            target_names = list(value) if isinstance(value, list) else [value]
            # 转为小写以便模糊匹配 (虽然数据库存的是原样，但为了稳健)
            # target_names = [str(n).strip() for n in target_names] # 暂时不转小写，因为前端传来的通常是标准值
            
            if not target_names: continue
            
            column = "COALESCE(m.genres_json, '[]'::jsonb)"
            
            # 逻辑：匹配 name 字段
            # g->>'name' 取出来是文本
            
            if op in ['contains', 'is_one_of', 'eq']:
                # 只要有一个匹配即可
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) g 
                    WHERE g->>'name' = ANY(%s)
                )
                """
                params.append(target_names)
                
            elif op == 'is_none_of':
                # 一个都不能匹配
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) g 
                    WHERE g->>'name' = ANY(%s)
                )
                """
                params.append(target_names)
                
            elif op == 'is_primary':
                # 主类型是数组第0个
                clause = f"({column}->0->>'name') = ANY(%s)"
                params.append(target_names)

        # --- 2. 关键词 (Keywords) ---
        elif field == 'keywords':
            expanded = _expand_keyword_labels(value)
            target_ids = expanded['ids']
            # 名字转小写以便模糊匹配
            target_names = [str(n).lower() for n in expanded['names']]
            
            if not target_ids and not target_names: continue
            
            column = "COALESCE(m.keywords_json, '[]'::jsonb)"
            
            # 逻辑：(ID 匹配) OR (Name 匹配)
            # s->>'id' 取出来是文本，所以我们把 target_ids 也转成了文本
            match_logic = """
            (
                (k->>'id') = ANY(%s) 
                OR 
                LOWER(k->>'name') = ANY(%s)
            )
            """
            
            if op in ['contains', 'is_one_of', 'eq']:
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) k 
                    WHERE {match_logic}
                )
                """
                params.extend([target_ids, target_names])
                
            elif op == 'is_none_of':
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) k 
                    WHERE {match_logic}
                )
                """
                params.extend([target_ids, target_names])

        # --- 3. 工作室 (Studios) ---
        elif field == 'studios':
            expanded = _expand_studio_labels(value)
            target_ids = expanded['ids']
            target_names = [str(n).lower() for n in expanded['names']]
            
            if not target_ids and not target_names: continue
            
            column = "COALESCE(m.studios_json, '[]'::jsonb)"
            
            match_logic = """
            (
                (s->>'id') = ANY(%s) 
                OR 
                LOWER(s->>'name') = ANY(%s)
            )
            """
            
            if op in ['contains', 'is_one_of', 'eq']:
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) s 
                    WHERE {match_logic}
                )
                """
                params.extend([target_ids, target_names])
                
            elif op == 'is_none_of':
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({column}) s 
                    WHERE {match_logic}
                )
                """
                params.extend([target_ids, target_names])
                
            elif op == 'is_primary':
                # 主工作室是数组第0个
                clause = f"""
                (
                    ({column}->0->>'id') = ANY(%s)
                    OR
                    LOWER({column}->0->>'name') = ANY(%s)
                )
                """
                params.extend([target_ids, target_names])

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
            # 1. 反向展开：中文标签 -> 原始代码列表
            target_codes = _expand_rating_labels(list(value) if isinstance(value, list) else [value])
            
            if not target_codes:
                continue 

            # A. 获取用户配置的优先级
            priority_list = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY

            json_keys = []
            json_keys.append("NULLIF(m.custom_rating, '')")

            for p in priority_list:
                if p == 'ORIGIN':
                    json_keys.append("m.official_rating_json->>(m.countries_json->>0)")
                else:
                    json_keys.append(f"m.official_rating_json->>'{p}'")
            
            if not json_keys:
                json_keys = ["m.official_rating_json->>'US'"]
                
            target_rating_expr = f"COALESCE({', '.join(json_keys)})"

            # C. 构建查询语句
            if op in ['eq', 'is_one_of']:
                # 正向筛选 (是...): 必须有分级且匹配
                clause = f"{target_rating_expr} = ANY(%s)"
                params.append(target_codes)
                
            elif op == 'is_none_of':
                # ★★★ 修复：反向筛选 (不是...) ★★★
                # 旧逻辑: (IS NOT NULL AND NOT MATCH) -> 导致无分级内容被误杀
                # 新逻辑: (IS NULL OR NOT MATCH) -> 保留无分级内容
                clause = f"({target_rating_expr} IS NULL OR NOT ({target_rating_expr} = ANY(%s)))"
                params.append(target_codes)

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

        # --- 13. 媒体库筛选 (Library) ---
        elif field == 'library': 
            # 逻辑：检查 asset_details_json 中的 source_library_id
            safe_assets = "COALESCE(m.asset_details_json, '[]'::jsonb)"
            
            # 确保 value 是列表
            val_list = list(value) if isinstance(value, list) else [value]
            
            if op in ['is_one_of', 'eq', 'contains']: # 包含于
                clause = f"""
                EXISTS (
                    SELECT 1 FROM jsonb_array_elements({safe_assets}) a 
                    WHERE a->>'source_library_id' = ANY(%s)
                )
                """
                params.append(val_list)
                
            elif op == 'is_none_of': # 不包含于
                clause = f"""
                NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements({safe_assets}) a 
                    WHERE a->>'source_library_id' = ANY(%s)
                )
                """
                params.append(val_list)

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
                # ★★★ 修改开始：支持 Season，并关联父剧集获取标题和海报 ★★★
                cursor.execute("""
                    SELECT 
                        m.tmdb_id, 
                        m.subscription_status, 
                        -- 如果是季，拼接父标题 (例如: "某某剧 第 1 季")
                        CASE 
                            WHEN m.item_type = 'Season' THEN COALESCE(p.title, '') || ' ' || m.title
                            ELSE m.title 
                        END AS title,
                        m.release_year, 
                        m.release_date, 
                        m.item_type, 
                        -- 如果季海报为空，回退使用父剧集海报
                        COALESCE(m.poster_path, p.poster_path) AS poster_path, 
                        m.emby_item_ids_json 
                    FROM media_metadata m
                    LEFT JOIN media_metadata p ON m.parent_series_tmdb_id = p.tmdb_id AND p.item_type = 'Series'
                    WHERE m.tmdb_id = ANY(%s) AND m.item_type IN ('Movie', 'Series', 'Season')
                """, (tmdb_ids,))
                # ★★★ 修改结束 ★★★
                
                rows = cursor.fetchall()
                return {str(r['tmdb_id']): dict(r) for r in rows}
    except Exception as e:
        logger.error(f"获取缺失项元数据失败: {e}")
        return {}
    
def get_best_metadata_by_tmdb_id(tmdb_id: str) -> Dict[str, Any]:
    """
    【反代专用】根据 TMDb ID 获取最佳元数据。
    逻辑：
    1. 同时查找该 ID 本身 (Movie/Series) 以及 以该 ID 为父级的子项 (Season)。
    2. 按状态优先级排序 (SUBSCRIBED > PENDING > WANTED > ...)。
    3. 返回优先级最高的那条记录的状态和海报。
    这样即使数据库只存了 Season 的订阅，查 Series ID 也能拿到正确的 PENDING_RELEASE 状态。
    """
    if not tmdb_id: return {}
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT 
                        subscription_status, 
                        poster_path,
                        item_type
                    FROM media_metadata
                    WHERE tmdb_id = %s 
                       OR parent_series_tmdb_id = %s
                    ORDER BY 
                        -- 1. 状态优先级排序 (数值越小越优先)
                        CASE subscription_status
                            WHEN 'SUBSCRIBED' THEN 1
                            WHEN 'PENDING_RELEASE' THEN 2
                            WHEN 'WANTED' THEN 3
                            WHEN 'PAUSED' THEN 4
                            ELSE 99
                        END ASC,
                        -- 2. 如果状态相同，优先取 Series 本身的记录，其次是 Season
                        CASE item_type
                            WHEN 'Series' THEN 1
                            WHEN 'Season' THEN 2
                            ELSE 3
                        END ASC
                    LIMIT 1
                """
                cursor.execute(sql, (tmdb_id, tmdb_id))
                row = cursor.fetchone()
                if row:
                    return dict(row)
    except Exception as e:
        logger.error(f"获取最佳元数据失败 (ID: {tmdb_id}): {e}")
        
    return {}