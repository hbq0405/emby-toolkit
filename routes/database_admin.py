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

import config_manager
import task_manager
import constants
from database import (
    connection,
    log_db,
    maintenance_db,
    settings_db
)
# 导入共享模块
import extensions
from extensions import admin_required, processor_ready_required, task_lock_required

# 1. 创建蓝图
db_admin_bp = Blueprint('database_admin', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

def json_datetime_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def _get_all_stats_in_one_query(cursor: psycopg2.extensions.cursor) -> dict:
    """
    【V11 - 演员映射细分】
    - 将“演员映射”总数拆分为“已关联”和“未关联”两个指标。
    """
    sql = """
    SELECT
        -- 核心媒体库
        (SELECT COUNT(*) FROM media_metadata) AS media_cached_total,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = TRUE) AS media_in_library_total,
        COUNT(*) FILTER (WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        COUNT(*) FILTER (WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = FALSE) AS media_missing_total,
        
        -- 用户与邀请
        (SELECT COUNT(*) FROM emby_users) AS emby_users_total,
        (SELECT COUNT(*) FROM emby_users_extended WHERE status = 'active') AS emby_users_active,
        (SELECT COUNT(*) FROM emby_users_extended WHERE status = 'disabled') AS emby_users_disabled,
        
        -- 自动化维护
        (SELECT COUNT(*) FROM media_cleanup_tasks WHERE status = 'pending') AS cleanup_tasks_pending,
        (SELECT COUNT(*) FROM resubscribe_rules WHERE enabled = TRUE) AS resubscribe_rules_enabled,

        -- 合集管理
        (SELECT COUNT(*) FROM collections_info) AS collections_tmdb_total,
        (SELECT COUNT(*) FROM collections_info WHERE has_missing = TRUE) AS collections_with_missing,
        (SELECT COUNT(*) FROM custom_collections WHERE status = 'active') AS collections_custom_active,
        
        -- 订阅服务
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Watching') AS watchlist_active,
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Paused') AS watchlist_paused,
        (SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active') AS actor_subscriptions_active,
        (SELECT COUNT(*) FROM tracked_actor_media) AS tracked_media_total,
        (SELECT COUNT(*) FROM tracked_actor_media WHERE status = 'IN_LIBRARY') AS tracked_media_in_library,
        (SELECT COUNT(*) FROM resubscribe_cache WHERE status ILIKE 'needed') AS resubscribe_pending,
        
        -- ★★★ 核心修改：细分演员映射统计 ★★★
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NOT NULL) AS actor_mappings_linked,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NULL) AS actor_mappings_unlinked,
        
        -- 系统与缓存 (其余部分)
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
@admin_required
def api_get_database_stats():
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            raw_stats = _get_all_stats_in_one_query(cursor)
            if not raw_stats:
                raise RuntimeError("未能从数据库获取统计数据。")

            # --- 配额计算 (保持不变) ---
            available_quota = settings_db.get_subscription_quota()
            total_quota = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP, 200)
            consumed_quota = max(0, total_quota - available_quota)

            # ★★★ 核心修改：只组装前端需要的数据 ★★★
            stats = {
                # --- 卡片1: 核心数据缓存 ---
                'media_library': {
                    "cached_total": raw_stats.get('media_cached_total', 0),
                    "movies_in_library": raw_stats.get('media_movies_in_library', 0),
                    "series_in_library": raw_stats.get('media_series_in_library', 0),
                    "missing_total": raw_stats.get('media_missing_total', 0),
                },
                'system': {
                    # 新增 actor_mappings_total 用于前端直接显示，无需计算
                    "actor_mappings_total": raw_stats.get('actor_mappings_linked', 0) + raw_stats.get('actor_mappings_unlinked', 0),
                    "actor_mappings_linked": raw_stats.get('actor_mappings_linked', 0),
                    "actor_mappings_unlinked": raw_stats.get('actor_mappings_unlinked', 0),
                    "translation_cache_count": raw_stats.get('translation_cache_count', 0),
                    "processed_log_count": raw_stats.get('processed_log_count', 0),
                    "failed_log_count": raw_stats.get('failed_log_count', 0),
                },

                # --- 卡片2: 智能订阅 ---
                'subscriptions_card': {
                    'watchlist': {'watching': raw_stats.get('watchlist_active', 0), 'paused': raw_stats.get('watchlist_paused', 0)},
                    'actors': {'subscriptions': raw_stats.get('actor_subscriptions_active', 0), 'tracked_total': raw_stats.get('tracked_media_total', 0), 'tracked_in_library': raw_stats.get('tracked_media_in_library', 0)},
                    'resubscribe': {'pending': raw_stats.get('resubscribe_pending', 0)},
                    'collections': {'with_missing': raw_stats.get('collections_with_missing', 0)},
                    'quota': {'available': available_quota, 'consumed': consumed_quota}
                },
                
                # 'collections_card', 'user_management_card', 'maintenance_card' 已被移除
            }

        return jsonify({"status": "success", "data": stats})

    except Exception as e:
        logger.error(f"获取数据库统计信息时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "获取数据库统计信息时发生服务器内部错误"}), 500

# 2. 定义路由

# --- 数据库表管理 ---
@db_admin_bp.route('/database/tables', methods=['GET'])
@admin_required
def api_get_db_tables():
    """【修改2】: 使用 PostgreSQL 的 information_schema 来获取表列表。"""
    try:
        with connection.get_db_connection() as conn:
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
@admin_required
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

        with connection.get_db_connection() as conn:
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
@admin_required
def api_import_database():
    """
    【V4 - 共享导入终版】接收备份文件和要导入的表名列表，
    并根据服务器ID是否匹配，自动决定采用“覆盖”或“共享”模式提交后台任务。
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

    try:
        file_content = file.stream.read().decode("utf-8-sig")
        backup_json = json.loads(file_content)
        backup_metadata = backup_json.get("metadata", {})
        backup_server_id = backup_metadata.get("source_emby_server_id")

        # ★★★ 核心修改：在这里决定导入策略 ★★★
        import_strategy = 'overwrite' # 默认为覆盖模式
        
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
            # ID不匹配，自动切换到共享导入模式
            import_strategy = 'share'
            task_name = "数据库恢复 (共享模式)"
            logger.info(f"服务器ID不匹配，将以共享模式导入可共享数据。备份源: ...{backup_server_id[-12:]}, 当前: ...{current_server_id[-12:]}")
        else:
            # ID匹配，使用标准的覆盖模式
            task_name = "数据库恢复 (覆盖模式)"
            logger.info("服务器ID匹配，将以覆盖模式导入。")
        
        logger.trace(f"已接收上传的备份文件 '{file.filename}'，将以 '{task_name}' 模式导入表: {tables_to_import}")

        success = task_manager.submit_task(
            task_import_database,
            task_name,
            processor_type='media',
            # 传递任务所需的所有参数，新增 import_strategy
            file_content=file_content,
            tables_to_import=tables_to_import,
            import_strategy=import_strategy
        )
        
        return jsonify({"message": f"文件上传成功，已提交后台任务以 '{task_name}' 模式恢复 {len(tables_to_import)} 个表。"}), 202

    except Exception as e:
        logger.error(f"处理数据库导入请求时发生错误: {e}", exc_info=True)
        return jsonify({"error": "处理上传文件时发生服务器错误"}), 500

# --- 待复核列表管理 ---
@db_admin_bp.route('/review_items', methods=['GET'])
@admin_required
def api_get_review_items():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    query_filter = request.args.get('query', '', type=str).strip()
    try:
        items, total = log_db.get_review_items_paginated(page, per_page, query_filter)
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0
        return jsonify({
            "items": items, "total_items": total, "total_pages": total_pages,
            "current_page": page, "per_page": per_page, "query": query_filter
        })
    except Exception as e:
        return jsonify({"error": "获取待复核列表时发生服务器内部错误"}), 500

@db_admin_bp.route('/actions/mark_item_processed/<item_id>', methods=['POST'])
@admin_required
def api_mark_item_processed(item_id):
    if task_manager.is_task_running(): return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409
    try:
        success = log_db.mark_review_item_as_processed(item_id)
        
        if success:
            return jsonify({"message": f"项目 {item_id} 已成功从待复核列表移除。"}), 200
        else:
            return jsonify({"error": f"未在待复核列表中找到项目 {item_id}。"}), 404
    except Exception as e:
        return jsonify({"error": "服务器内部错误"}), 500

# ✨✨✨ 清空待复核列表 ✨✨✨
@db_admin_bp.route('/actions/clear_review_items', methods=['POST'])
@admin_required
def api_clear_review_items():
    try:
        count = log_db.clear_all_review_items()

        if count > 0:
            message = f"操作成功！已从待复核列表移除 {count} 个项目。"
        else:
            message = "操作完成，待复核列表本就是空的。"
            
        return jsonify({"message": message}), 200
    except Exception as e:
        logger.error("API调用api_clear_review_items时发生错误", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500

# --- 清空指定表列表的接口 ---
@db_admin_bp.route('/actions/clear_tables', methods=['POST'])
@admin_required
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
            deleted_count = maintenance_db.clear_table(table_name)
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
@admin_required
def api_correct_all_sequences():
    """
    触发一个任务，校准数据库中所有表的自增ID序列。
    """
    try:
        # 直接调用 db_handler 中的核心函数
        corrected_tables = maintenance_db.correct_all_sequences()
        
        if corrected_tables:
            message = f"操作成功！已成功校准 {len(corrected_tables)} 个表的ID计数器。"
        else:
            message = "操作完成，未发现需要校准的表。"
            
        return jsonify({"message": message, "corrected_tables": corrected_tables}), 200
        
    except Exception as e:
        logger.error(f"API调用api_correct_all_sequences时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500
    
# --- 重置Emby数据 ---
@db_admin_bp.route('/actions/prepare-for-library-rebuild', methods=['POST'])
@admin_required
def api_prepare_for_library_rebuild():
    """
    【高危操作】为 Emby 媒体库重建做准备。
    此操作会清空所有与 Emby 直接相关的数据表，并断开元数据表中与 Emby ID 的关联。
    执行此操作后，你需要重新运行“同步 Emby 用户”、“扫描媒体库元数据”等任务来重建关联。
    前端应已提供高危操作警告。
    """
    logger.warning("接收到“为 Emby 重建做准备”的请求，这是一个高危操作，将重置所有 Emby 关联数据。")

    try:
        # 定义需要完全清空的表 (这些表的数据完全依赖于 Emby)
        tables_to_truncate = [
            'emby_users',               # Emby 用户列表
            'emby_users_extended',      # Emby 用户扩展信息
            'user_media_data',          # 用户播放状态、收藏等
            'user_collection_cache',    # 虚拟库权限缓存
            'collections_info',         # Emby 原生合集信息
            'watchlist',                # 追剧列表 (主键是 Emby Item ID)
            'resubscribe_cache',        # 媒体洗版缓存 (主键是 Emby Item ID)
            'media_cleanup_tasks'       # 多版本清理任务
        ]

        # 定义需要置空 Emby 关联 ID 的表和字段
        columns_to_reset = {
            'media_metadata': 'emby_item_id',
            'person_identity_map': 'emby_person_id',
            'custom_collections': 'emby_collection_id',
            'tracked_actor_media': 'emby_item_id'
        }

        results = {
            "truncated_tables": {},
            "updated_columns": {}
        }

        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 完全清空表
                logger.info("第一步：开始清空 Emby 专属数据表...")
                for table_name in tables_to_truncate:
                    logger.warning(f"  ➜ 正在清空表: {table_name}")
                    # 使用 TRUNCATE ... RESTART IDENTITY CASCADE 可以高效清空并重置序列，同时处理外键
                    query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(
                        table=sql.Identifier(table_name)
                    )
                    cursor.execute(query)
                    results["truncated_tables"][table_name] = "清空成功"
                
                # 2. 置空关联字段
                logger.info("第二步：开始断开元数据与 Emby ID 的关联...")
                for table_name, column_name in columns_to_reset.items():
                    logger.warning(f"  ➜ 正在重置表 '{table_name}' 中的 '{column_name}' 字段...")
                    query = sql.SQL("UPDATE {table} SET {column} = NULL WHERE {column} IS NOT NULL;").format(
                        table=sql.Identifier(table_name),
                        column=sql.Identifier(column_name)
                    )
                    cursor.execute(query)
                    affected_rows = cursor.rowcount
                    results["updated_columns"][f"{table_name}.{column_name}"] = f"重置了 {affected_rows} 行"
                    logger.info(f"    ➜ 操作完成，影响了 {affected_rows} 行。")

        message = "为 Emby 媒体库重建的准备工作已成功完成！"
        logger.info(message)
        return jsonify({"message": message, "details": results}), 200
        
    except Exception as e:
        logger.error(f"API 调用 api_prepare_for_library_rebuild 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误，操作可能未完全执行。"}), 500