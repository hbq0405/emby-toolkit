# routes/database_admin.py (V8 - 配额计算终版修复)

from flask import Blueprint, request, jsonify, Response
import logging
import json
import gzip
import io
import time
from datetime import datetime, date

# 导入底层模块 (不再导入 connection！)
import config_manager
import task_manager
import constants
from database import log_db, maintenance_db, settings_db

# 导入共享模块
import extensions
from extensions import any_login_required, admin_required

# 1. 创建蓝图
db_admin_bp = Blueprint('database_admin', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

def json_datetime_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# --- 数据看板 (拆分版 API) ---
@db_admin_bp.route('/database/stats/core', methods=['GET'])
@any_login_required
def api_get_stats_core():
    try:
        data = maintenance_db.get_stats_core()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"获取核心统计失败: {e}")
        return jsonify({"status": "error", "data": {}}), 500

@db_admin_bp.route('/database/stats/library', methods=['GET'])
@any_login_required
def api_get_stats_library():
    try:
        data = maintenance_db.get_stats_library()
        # 格式化一下分辨率数据以匹配前端预期
        formatted_data = {
            "movies_in_library": data.get('media_movies_in_library', 0),
            "series_in_library": data.get('media_series_in_library', 0),
            "episodes_in_library": data.get('media_episodes_in_library', 0),
            "missing_total": data.get('media_missing_total', 0),
            "resolution_stats": data.get('resolution_stats', [])
        }
        return jsonify({"status": "success", "data": formatted_data})
    except Exception as e:
        logger.error(f"获取媒体库统计失败: {e}")
        return jsonify({"status": "error", "data": {}}), 500

@db_admin_bp.route('/database/stats/system', methods=['GET'])
@any_login_required
def api_get_stats_system():
    try:
        raw = maintenance_db.get_stats_system()
        formatted_data = {
            "actor_mappings_total": raw.get('actor_mappings_linked', 0) + raw.get('actor_mappings_unlinked', 0),
            "actor_mappings_linked": raw.get('actor_mappings_linked', 0),
            "actor_mappings_unlinked": raw.get('actor_mappings_unlinked', 0),
            "translation_cache_count": raw.get('translation_cache_count', 0),
            "processed_log_count": raw.get('processed_log_count', 0),
            "failed_log_count": raw.get('failed_log_count', 0),
        }
        return jsonify({"status": "success", "data": formatted_data})
    except Exception as e:
        logger.error(f"获取系统统计失败: {e}")
        return jsonify({"status": "error", "data": {}}), 500

@db_admin_bp.route('/database/stats/subscription', methods=['GET'])
@any_login_required
def api_get_stats_subscription():
    try:
        raw = maintenance_db.get_stats_subscription()
        
        # 计算配额
        available_quota = settings_db.get_subscription_quota()
        total_quota = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP, 200)
        consumed_quota = max(0, total_quota - available_quota)

        formatted_data = {
            'watchlist': {
                'watching': raw.get('watchlist_active', 0),
                'paused': raw.get('watchlist_paused', 0),
                'completed': raw.get('watchlist_completed', 0) # ★★★ 新增这一行 ★★★
            },
            'actors': {
                'subscriptions': raw.get('actor_subscriptions_active', 0), 
                'tracked_in_library': raw.get('actor_works_in_library', 0)
            },
            'resubscribe': {'pending': raw.get('resubscribe_pending', 0)},
            'native_collections': {
                'total': raw.get('native_collections_total', 0), 
                'count': raw.get('native_collections_with_missing', 0),
                'missing_items': raw.get('native_collections_missing_items', 0) or 0
            },
            'custom_collections': {
                'total': raw.get('custom_collections_total', 0), 
                'count': raw.get('custom_collections_with_missing', 0),
                'missing_items': raw.get('custom_collections_missing_items', 0) or 0
            },
            'quota': {'available': available_quota, 'consumed': consumed_quota}
        }
        return jsonify({"status": "success", "data": formatted_data})
    except Exception as e:
        logger.error(f"获取订阅统计失败: {e}")
        return jsonify({"status": "error", "data": {}}), 500

@db_admin_bp.route('/database/stats/rankings', methods=['GET'])
@any_login_required
def api_get_stats_rankings():
    try:
        data = {
            'release_group_ranking': maintenance_db.get_release_group_ranking(5),
            'historical_release_group_ranking': maintenance_db.get_historical_release_group_ranking(5)
        }
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        logger.error(f"获取排行统计失败: {e}")
        return jsonify({"status": "error", "data": {}}), 500

# --- 数据库表管理 ---
@db_admin_bp.route('/database/tables', methods=['GET'])
@admin_required
def api_get_db_tables():
    try:
        # ### 核心修改：调用新的数据库函数 ###
        tables = maintenance_db.get_all_table_names()
        return jsonify(tables)
    except Exception as e:
        logger.error(f"获取 PostgreSQL 表列表时出错: {e}", exc_info=True)
        return jsonify({"error": "无法获取数据库表列表"}), 500

@db_admin_bp.route('/database/export', methods=['POST'])
@admin_required
def api_export_database():
    try:
        tables_to_export = request.json.get('tables')
        if not tables_to_export or not isinstance(tables_to_export, list):
            return jsonify({"error": "请求体中必须包含一个 'tables' 数组"}), 400

        tables_data = maintenance_db.export_tables_data(tables_to_export)

        backup_data = {
            "metadata": {
                "export_date": datetime.utcnow().isoformat() + "Z",
                "app_version": constants.APP_VERSION,
                "source_emby_server_id": extensions.EMBY_SERVER_ID,
                "tables": tables_to_export
            }, 
            "data": tables_data
        }

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"database_backup_{timestamp}.json.gz"
        
        json_output_bytes = json.dumps(
            backup_data, 
            ensure_ascii=False, 
            default=json_datetime_serializer
        ).encode('utf-8')

        compressed_data = gzip.compress(json_output_bytes)

        response = Response(compressed_data, mimetype='application/gzip')
        response.headers.set("Content-Disposition", "attachment", filename=filename)
        
        return response
    except Exception as e:
        logger.error(f"导出数据库时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"导出时发生服务器错误: {e}"}), 500
    
@db_admin_bp.route('/database/preview-backup', methods=['POST'])
@admin_required
def api_preview_backup_file():
    """
    【V2 - 增强版】
    接收上传的备份文件，解析其内容，并返回：
    1. 其中包含的表名列表。
    2. 根据服务器ID匹配结果，决定导入模式 ('overwrite' 或 'share')。
    """
    if 'file' not in request.files:
        return jsonify({"error": "请求中未找到文件部分"}), 400
    
    file = request.files['file']
    if not file.filename:
        return jsonify({"error": "未选择文件"}), 400

    try:
        file_bytes = file.stream.read()
        
        if file.filename.endswith('.gz'):
            content_bytes = gzip.decompress(file_bytes)
        else:
            content_bytes = file_bytes
            
        file_content = content_bytes.decode("utf-8-sig")
        backup_json = json.loads(file_content)
        
        tables = list(backup_json.get("data", {}).keys())
        
        # ★★★ 核心修改：在这里进行服务器ID检查 ★★★
        backup_metadata = backup_json.get("metadata", {})
        backup_server_id = backup_metadata.get("source_emby_server_id")
        current_server_id = extensions.EMBY_SERVER_ID
        
        import_mode = 'overwrite' # 默认为覆盖模式
        if not backup_server_id or not current_server_id:
            # 如果任一ID缺失，为安全起见，强制使用共享模式
            import_mode = 'share'
            logger.warning("备份文件或当前服务器缺少ID，将强制使用共享模式。")
        elif backup_server_id != current_server_id:
            # ID不匹配，明确设置为共享模式
            import_mode = 'share'
            logger.info(f"服务器ID不匹配，预览接口确定使用共享模式。")

        # ★★★ 在返回的数据中加入 import_mode 字段 ★★★
        return jsonify({"status": "success", "tables": tables, "import_mode": import_mode})

    except gzip.BadGzipFile:
        logger.error(f"上传的备份文件 '{file.filename}' 不是一个有效的 Gzip 文件。")
        return jsonify({"error": "文件不是有效的 Gzip 格式。"}), 400
    except json.JSONDecodeError:
        logger.error(f"解析备份文件 '{file.filename}' 的 JSON 内容时失败。")
        return jsonify({"error": "无法解析文件的 JSON 内容，文件可能已损坏。"}), 400
    except Exception as e:
        logger.error(f"预览备份文件时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "处理文件时发生服务器内部错误。"}), 500

@db_admin_bp.route('/database/import', methods=['POST'])
@admin_required
def api_import_database():
    """
    【V6 - 健壮的 Gzip 导入】接收备份文件和要导入的表名列表...
    """
    from tasks import task_import_database
    if 'file' not in request.files:
        return jsonify({"error": "请求中未找到文件部分"}), 400
    
    file = request.files['file']
    if not file.filename or not (file.filename.endswith('.json') or file.filename.endswith('.json.gz')):
        return jsonify({"error": "未选择文件或文件类型必须是 .json 或 .json.gz"}), 400

    tables_to_import_str = request.form.get('tables')
    if not tables_to_import_str:
        return jsonify({"error": "必须通过 'tables' 字段指定要导入的表"}), 400
    tables_to_import = [table.strip() for table in tables_to_import_str.split(',')]

    try:
        file_bytes = file.stream.read()
        
        # ★★★ 核心修复：增加健壮的解压逻辑 ★★★
        file_content = ""
        if file.filename.endswith('.gz'):
            try:
                # 尝试作为 Gzip 文件解压
                file_content = gzip.decompress(file_bytes).decode("utf-8-sig")
            except gzip.BadGzipFile:
                # 如果解压失败，则认为它是一个被错误命名的普通文本文件
                logger.warning(f"文件 '{file.filename}' 扩展名为 .gz 但内容不是 Gzip 格式。将尝试作为纯文本处理。")
                file_content = file_bytes.decode("utf-8-sig")
        else:
            # 如果是 .json 文件，直接解码
            file_content = file_bytes.decode("utf-8-sig")
            
        backup_json = json.loads(file_content)
        # ... (后续所有逻辑完全不变) ...
        backup_metadata = backup_json.get("metadata", {})
        backup_server_id = backup_metadata.get("source_emby_server_id")

        import_strategy = 'overwrite'
        
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
            import_strategy = 'share'
            task_name = "数据库恢复 (共享模式)"
            logger.info(f"服务器ID不匹配，将以共享模式导入可共享数据。备份源: ...{backup_server_id[-12:]}, 当前: ...{current_server_id[-12:]}")
        else:
            task_name = "数据库恢复 (覆盖模式)"
            logger.info("服务器ID匹配，将以覆盖模式导入。")
        
        logger.trace(f"已接收上传的备份文件 '{file.filename}'，将以 '{task_name}' 模式导入表: {tables_to_import}")

        success = task_manager.submit_task(
            task_import_database,
            task_name,
            processor_type='media',
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
    logger.info("  ➜ 接收到清空指定表请求。")
    try:
        data = request.get_json()
        if not data or 'tables' not in data or not isinstance(data['tables'], list):
            logger.warning(f"  ➜ 清空表请求体无效: {data}")
            return jsonify({"error": "请求体必须包含'tables'字段，且为字符串数组"}), 400
        
        tables = data['tables']
        if not tables:
            logger.warning("  ➜ 清空表请求中表列表为空。")
            return jsonify({"error": "表列表不能为空"}), 400
        
        logger.info(f"  ➜ 准备清空以下表: {tables}")
        total_deleted = 0
        for table_name in tables:
            # 简单校验表名格式，防止注入
            if not isinstance(table_name, str) or not table_name.isidentifier():
                logger.warning(f"  ➜ 非法表名跳过清空: {table_name}")
                continue
            
            logger.info(f"  ➜ 正在清空表: {table_name}")
            deleted_count = maintenance_db.clear_table(table_name)
            total_deleted += deleted_count
            logger.info(f"  ➜ 表 {table_name} 清空完成，删除了 {deleted_count} 行。")
        
        message = f"  ➜ 操作成功！共清空 {len(tables)} 个表，删除 {total_deleted} 行数据。"
        logger.info(message)
        return jsonify({"message": message}), 200
    except Exception as e:
        logger.error(f"  ➜ API调用api_clear_tables时发生错误: {e}", exc_info=True)
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
    logger.warning("接收到“为 Emby 重建做准备”的请求，这是一个高危操作，将重置所有 Emby 关联数据。")
    try:
        # ### 核心修改：调用新的数据库函数 ###
        results = maintenance_db.prepare_for_library_rebuild()
        
        message = "为 Emby 媒体库重建的准备工作已成功完成！"
        logger.info(message)
        return jsonify({"message": message, "details": results}), 200
        
    except Exception as e:
        logger.error(f"API 调用 api_prepare_for_library_rebuild 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误，操作可能未完全执行。"}), 500
    
# --- 清理离线媒体 (数据库瘦身) ---
@db_admin_bp.route('/actions/cleanup-offline-media', methods=['POST'])
@admin_required
def api_cleanup_offline_media():
    """
    触发清理离线媒体的任务。
    """
    logger.info("接收到清理离线媒体请求。")
    try:
        # 调用维护模块的函数
        stats = maintenance_db.cleanup_offline_media()
        
        deleted_count = stats.get('media_metadata_deleted', 0)
        
        if deleted_count > 0:
            message = f"瘦身成功！已清除 {deleted_count} 条无用的离线媒体记录。"
        else:
            message = "数据库很干净，没有发现需要清理的离线记录。"
            
        return jsonify({
            "message": message,
            "data": stats
        }), 200
        
    except Exception as e:
        logger.error(f"API调用 api_cleanup_offline_media 时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500
    
# --- 清空向量数据 ---
@db_admin_bp.route('/actions/clear-vectors', methods=['POST'])
@admin_required
def api_clear_vectors():
    """
    触发清空向量数据的任务。
    """
    logger.info("  ➜ 接收到清空向量数据请求。")
    try:
        count = maintenance_db.clear_all_vectors()
        
        if count > 0:
            message = f"操作成功！已清除 {count} 条旧的向量数据。请重新运行扫描以生成新向量。"
        else:
            message = "数据库中没有发现向量数据，无需清理。"
            
        return jsonify({"message": message}), 200
        
    except Exception as e:
        logger.error(f"API调用 api_clear_vectors 时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理时发生内部错误"}), 500