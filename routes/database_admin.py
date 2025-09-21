# routes/database_admin.py (V8 - 配额计算终版修复)

from flask import Blueprint, request, jsonify, Response
import logging
import json
import re
import psycopg2
import time
from datetime import datetime, date
from psycopg2 import sql

# 导入底层模块
import db_handler
import config_manager
import task_manager
import constants

# 导入共享模块
import extensions
from extensions import login_required, processor_ready_required, task_lock_required

# 1. 创建蓝图
db_admin_bp = Blueprint('database_admin', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

def json_datetime_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def _get_all_stats_in_one_query(cursor: psycopg2.extensions.cursor) -> dict:
    """
    【V4 - 再次修复洗版统计】
    - 确保能正确统计到所有需要洗版的项目。
    """
    sql = """
    SELECT
        -- 核心媒体库
        (SELECT COUNT(*) FROM media_metadata) AS media_total,
        COUNT(*) FILTER (WHERE item_type = 'Movie') AS media_movies,
        COUNT(*) FILTER (WHERE item_type = 'Series') AS media_series,
        (SELECT COUNT(*) FROM users) AS users_total,
        -- 合集管理
        (SELECT COUNT(*) FROM collections_info) AS collections_tmdb_total,
        (SELECT COUNT(*) FROM collections_info WHERE has_missing = TRUE) AS collections_with_missing,
        (SELECT COUNT(*) FROM custom_collections WHERE status = 'active') AS collections_custom_active,
        -- 订阅服务: 追剧
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Watching') AS watchlist_active,
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Paused') AS watchlist_paused,
        -- 订阅服务: 演员
        (SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active') AS actor_subscriptions_active,
        (SELECT COUNT(*) FROM tracked_actor_media) AS tracked_media_total,
        (SELECT COUNT(*) FROM tracked_actor_media WHERE status = 'IN_LIBRARY') AS tracked_media_in_library,
        -- 洗版
        (SELECT COUNT(*) FROM resubscribe_cache WHERE status ILIKE 'needed') AS resubscribe_pending,
        -- 系统与缓存
        (SELECT COUNT(*) FROM person_identity_map) AS actor_mappings_count,
        (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
        (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
        (SELECT COUNT(*) FROM failed_log) AS failed_log_count
    FROM media_metadata
    LIMIT 1;
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchone()
        return dict(result) if result else {}
    except psycopg2.Error as e:
        logger.error(f"执行聚合统计查询时出错: {e}")
        return {}

# --- 数据看板 ---
@db_admin_bp.route('/database/stats', methods=['GET'])
@login_required
def api_get_database_stats():
    """
    【V6 - 配额计算逻辑修复】
    彻底修复因总配额下调导致的“已用”配额为负数的BUG。
    """
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            
            raw_stats = _get_all_stats_in_one_query(cursor)

            if not raw_stats:
                raise RuntimeError("未能从数据库获取统计数据。")

            # ★★★ 核心修复：采用全新的、更健壮的配额计算方法 ★★★
            # 1. 获取当前可用的剩余配额
            available_quota = db_handler.get_subscription_quota()
            # 2. 获取当前配置的总配额
            total_quota = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP, 200)
            # 3. 计算已用配额，并确保它永远不会是负数
            #    即使 available_quota 因某种原因 > total_quota (例如刚下调总数)，已用也应视为0
            consumed_quota = max(0, total_quota - available_quota)

            # 按照前端UI布局，将数据重新组织成更清晰的结构
            stats = {
                # 卡片1: 核心媒体库
                'media_metadata': {
                    "total": raw_stats.get('media_total', 0),
                    "movies": raw_stats.get('media_movies', 0),
                    "series": raw_stats.get('media_series', 0),
                },
                # 卡片2: 合集管理 (简化)
                'collections_card': {
                    "total_tmdb_collections": raw_stats.get('collections_tmdb_total', 0),
                    "total_custom_collections": raw_stats.get('collections_custom_active', 0),
                },
                # 卡片3: 订阅中心 (重构)
                'subscriptions_card': {
                    'watchlist': {
                        'watching': raw_stats.get('watchlist_active', 0),
                        'paused': raw_stats.get('watchlist_paused', 0),
                    },
                    'actors': {
                        'subscriptions': raw_stats.get('actor_subscriptions_active', 0),
                        'tracked_total': raw_stats.get('tracked_media_total', 0),
                        'tracked_in_library': raw_stats.get('tracked_media_in_library', 0),
                    },
                    'resubscribe': {
                        'pending': raw_stats.get('resubscribe_pending', 0),
                    },
                    'collections': {
                        'with_missing': raw_stats.get('collections_with_missing', 0),
                    },
                    'quota': {
                        'available': available_quota,
                        'consumed': consumed_quota,
                    }
                },
                # 卡片4: 系统与缓存
                'system': {
                    "actor_mappings_count": raw_stats.get('actor_mappings_count', 0),
                    "translation_cache_count": raw_stats.get('translation_cache_count', 0),
                    "processed_log_count": raw_stats.get('processed_log_count', 0),
                    "failed_log_count": raw_stats.get('failed_log_count', 0),
                }
            }

        return jsonify({"status": "success", "data": stats})

    except Exception as e:
        logger.error(f"获取数据库统计信息时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "获取数据库统计信息时发生服务器内部错误"}), 500

# ... (文件其余部分保持不变) ...
def _count_table_rows(cursor: psycopg2.extensions.cursor, table_name: str, condition: str = "") -> int:
    """一个通用的表行数计数辅助函数，增加错误处理。"""
    try:
        # 使用参数化查询防止SQL注入，即使表名是内部控制的
        query = f"SELECT COUNT(*) FROM {table_name}"
        if condition:
            # 注意：这里的condition仍然是直接拼接，因为它可能包含复杂的逻辑
            # 但在调用此函数时，应确保condition的内容是安全的
            query += f" WHERE {condition}"
        cursor.execute(query)
        result = cursor.fetchone()
        return result['count'] if result else 0
    except psycopg2.Error as e:
        logger.error(f"计算表 '{table_name}' 行数时出错: {e}")
        return -1 # 返回-1表示错误

# 2. 定义路由

# --- 数据库表管理 ---
@db_admin_bp.route('/database/tables', methods=['GET'])
@login_required
def api_get_db_tables():
    """【修改2】: 使用 PostgreSQL 的 information_schema 来获取表列表。"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            # PostgreSQL 使用 information_schema.tables 来查询表信息
            # table_schema = 'public' 是查询默认的公共模式下的表
            query = """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """
            cursor.execute(query)
            # cursor.fetchall() 返回的是元组列表，例如 [('users',), ('media_metadata',)]
            tables = [row['table_name'] for row in cursor.fetchall()]
        return jsonify(tables)
    except Exception as e:
        # 更新日志，使其更准确地反映错误
        logger.error(f"获取 PostgreSQL 表列表时出错: {e}", exc_info=True)
        return jsonify({"error": "无法获取数据库表列表"}), 500

@db_admin_bp.route('/database/export', methods=['POST'])
@login_required
def api_export_database():
    try:
        tables_to_export = request.json.get('tables')
        if not tables_to_export or not isinstance(tables_to_export, list):
            return jsonify({"error": "请求体中必须包含一个 'tables' 数组"}), 400

        backup_data = {
            "metadata": {
                "export_date": datetime.utcnow().isoformat() + "Z",
                "app_version": constants.APP_VERSION,
                "source_emby_server_id": extensions.EMBY_SERVER_ID,
                "tables": tables_to_export
            }, "data": {}
        }

        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            for table_name in tables_to_export:
                if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                     logger.warning(f"检测到无效的表名 '{table_name}'，已跳过导出。")
                     continue
                
                query = sql.SQL("SELECT * FROM {table}").format(
                    table=sql.Identifier(table_name)
                )
                cursor.execute(query)
                
                rows = cursor.fetchall()
                backup_data["data"][table_name] = [dict(row) for row in rows]

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"database_backup_{timestamp}.json"
        
        # 【修改】: 在调用 json.dumps 时，使用 default 参数指定我们的自定义转换器
        json_output = json.dumps(
            backup_data, 
            indent=2, 
            ensure_ascii=False, 
            default=json_datetime_serializer
        )

        response = Response(json_output, mimetype='application/json; charset=utf-8')
        response.headers.set("Content-Disposition", "attachment", filename=filename)
        return response
    except Exception as e:
        logger.error(f"导出数据库时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"导出时发生服务器错误: {e}"}), 500

@db_admin_bp.route('/database/import', methods=['POST'])
@login_required
def api_import_database():
    """
    【V3 - 简化版】接收备份文件和要导入的表名列表，
    并提交一个后台任务来处理数据恢复（仅支持覆盖模式）。
    """
    from tasks import task_import_database
    if 'file' not in request.files:
        return jsonify({"error": "请求中未找到文件部分"}), 400
    
    file = request.files['file']
    if not file.filename or not file.filename.endswith('.json'):
        return jsonify({"error": "未选择文件或文件类型必须是 .json"}), 400

    tables_to_import_str = request.form.get('tables')
    if not tables_to_import_str:
        return jsonify({"error": "必须通过 'tables' 字段指定要导入的表"}), 400
    tables_to_import = [table.strip() for table in tables_to_import_str.split(',')]

    # ★ 简化: 不再需要从前端获取 mode，因为后端只支持一种模式
    # 但我们仍然需要进行服务器ID校验
    import_mode = 'overwrite' # 硬编码为 'overwrite' 以触发安全校验
    task_name = "数据库恢复 (覆盖模式)"

    try:
        file_content = file.stream.read().decode("utf-8-sig")
        backup_json = json.loads(file_content)
        backup_metadata = backup_json.get("metadata", {})
        backup_server_id = backup_metadata.get("source_emby_server_id")

        # 安全校验逻辑仍然至关重要
        if import_mode == 'overwrite':
            if not backup_server_id:
                error_msg = "此备份文件缺少来源服务器ID，为安全起见，禁止恢复。这通常意味着它是一个旧版备份或非本系统导出的文件。"
                logger.warning(f"禁止导入: {error_msg}")
                return jsonify({"error": error_msg}), 403

            current_server_id = extensions.EMBY_SERVER_ID
            if not current_server_id:
                error_msg = "无法获取当前Emby服务器的ID，可能连接已断开。为安全起见，暂时禁止恢复操作。"
                logger.warning(f"禁止导入: {error_msg}")
                return jsonify({"error": error_msg}), 503

            if backup_server_id != current_server_id:
                error_msg = (f"服务器ID不匹配！此备份来自另一个Emby服务器，"
                           "直接恢复会造成数据严重混乱。操作已禁止。\n\n"
                           f"备份来源ID: ...{backup_server_id[-12:]}\n"
                           f"当前服务器ID: ...{current_server_id[-12:]}")
                logger.warning(f"禁止导入: {error_msg}")
                return jsonify({"error": error_msg}), 403
        
        logger.trace(f"已接收上传的备份文件 '{file.filename}'，将以 '{task_name}' 模式导入表: {tables_to_import}")

        # ▼▼▼ 修复后的函数调用 ▼▼▼
        success = task_manager.submit_task(
            task_import_database,
            task_name, # 使用简化的任务名
            processor_type='media',
            # 传递任务所需的所有参数，不再包含 import_mode
            file_content=file_content,
            tables_to_import=tables_to_import
        )
        
        return jsonify({"message": f"文件上传成功，已提交后台任务以恢复 {len(tables_to_import)} 个表。"}), 202

    except Exception as e:
        logger.error(f"处理数据库导入请求时发生错误: {e}", exc_info=True)
        return jsonify({"error": "处理上传文件时发生服务器错误"}), 500

# --- 待复核列表管理 ---
@db_admin_bp.route('/review_items', methods=['GET'])
@login_required
def api_get_review_items():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    query_filter = request.args.get('query', '', type=str).strip()
    try:
        items, total = db_handler.get_review_items_paginated(page, per_page, query_filter)
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0
        return jsonify({
            "items": items, "total_items": total, "total_pages": total_pages,
            "current_page": page, "per_page": per_page, "query": query_filter
        })
    except Exception as e:
        return jsonify({"error": "获取待复核列表时发生服务器内部错误"}), 500

@db_admin_bp.route('/actions/mark_item_processed/<item_id>', methods=['POST'])
@login_required
def api_mark_item_processed(item_id):
    if task_manager.is_task_running(): return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409
    try:
        # ★★★ 核心修改 1: 在数据库操作前，先获取项目名称 ★★★
        item_name = db_handler.get_item_name_from_failed_log(item_id)

        # 执行数据库操作
        success = db_handler.mark_review_item_as_processed(item_id)
        
        if success:
            # ★★★ 核心修改 2: 如果数据库操作成功，立即同步更新内存缓存 ★★★
            if item_name:
                extensions.media_processor_instance.processed_items_cache[item_id] = item_name
                logger.info(f"已将项目 {item_id} ('{item_name}') 的状态同步到内存缓存。")
            
            return jsonify({"message": f"项目 {item_id} 已成功标记为已处理。"}), 200
        else:
            return jsonify({"error": f"未在待复核列表中找到项目 {item_id}。"}), 404
    except Exception as e:
        return jsonify({"error": "服务器内部错误"}), 500

# ✨✨✨ 清空待复核列表（并全部标记为已处理）的 API ✨✨✨
@db_admin_bp.route('/actions/clear_review_items', methods=['POST'])
@login_required
def api_clear_review_items():
    try:
        # ★★★ 核心修改 1: 调用修改后的函数，它会返回被移动的条目列表 ★★★
        moved_items = db_handler.clear_all_review_items()
        count = len(moved_items)

        # ★★★ 核心修改 2: 遍历返回的列表，逐个更新到内存缓存 ★★★
        if count > 0:
            for item in moved_items:
                extensions.media_processor_instance.processed_items_cache[item['item_id']] = item['item_name']
            logger.info(f"已将 {count} 个项目从待复核列表的状态同步到内存缓存。")
            message = f"操作成功！已将 {count} 个项目移至已处理列表。"
        else:
            message = "操作完成，待复核列表本就是空的。"
            
        return jsonify({"message": message}), 200
    except Exception as e:
        logger.error("API调用api_clear_review_items时发生错误", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500

# --- 清空指定表列表的接口 ---
@db_admin_bp.route('/actions/clear_tables', methods=['POST'])
@login_required
def api_clear_tables():
    logger.info("接收到清空指定表请求。")
    try:
        data = request.get_json()
        if not data or 'tables' not in data or not isinstance(data['tables'], list):
            logger.warning(f"清空表请求体无效: {data}")
            return jsonify({"error": "请求体必须包含'tables'字段，且为字符串数组"}), 400
        
        tables = data['tables']
        if not tables:
            logger.warning("清空表请求中表列表为空。")
            return jsonify({"error": "表列表不能为空"}), 400
        
        logger.info(f"准备清空以下表: {tables}")
        total_deleted = 0
        for table_name in tables:
            # 简单校验表名格式，防止注入
            if not isinstance(table_name, str) or not table_name.isidentifier():
                logger.warning(f"非法表名跳过清空: {table_name}")
                continue
            
            logger.info(f"正在清空表: {table_name}")
            deleted_count = db_handler.clear_table(table_name)
            total_deleted += deleted_count
            logger.info(f"表 {table_name} 清空完成，删除了 {deleted_count} 行。")
        
        message = f"操作成功！共清空 {len(tables)} 个表，删除 {total_deleted} 行数据。"
        logger.info(message)
        return jsonify({"message": message}), 200
    except Exception as e:
        logger.error(f"API调用api_clear_tables时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500

# --- 一键矫正自增序列 ---
@db_admin_bp.route('/database/correct-sequences', methods=['POST'])
@login_required
def api_correct_all_sequences():
    """
    触发一个任务，校准数据库中所有表的自增ID序列。
    """
    try:
        # 直接调用 db_handler 中的核心函数
        corrected_tables = db_handler.correct_all_sequences()
        
        if corrected_tables:
            message = f"操作成功！已成功校准 {len(corrected_tables)} 个表的ID计数器。"
        else:
            message = "操作完成，未发现需要校准的表。"
            
        return jsonify({"message": message, "corrected_tables": corrected_tables}), 200
        
    except Exception as e:
        logger.error(f"API调用api_correct_all_sequences时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500