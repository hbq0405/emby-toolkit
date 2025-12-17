# routes/logs.py

from flask import Blueprint, request, jsonify, abort, Response
import logging
import os
from werkzeug.utils import secure_filename
import re

import config_manager
from extensions import admin_required

logs_bp = Blueprint('logs', __name__, url_prefix='/api/logs')
logger = logging.getLogger(__name__)

@logs_bp.route('/list', methods=['GET'])
@admin_required
def list_log_files():
    """列出日志目录下的所有日志文件 (app.log*)"""
    try:
        # config_manager.PERSISTENT_DATA_PATH 变量在当前作用域中可以直接使用
        all_files = os.listdir(config_manager.LOG_DIRECTORY)
        log_files = [f for f in all_files if f.startswith('app.log')]
        
        # 对日志文件进行智能排序，确保 app.log 在最前，然后是 .1.gz, .2.gz ...
        def sort_key(filename):
            if filename == 'app.log':
                return -1
            parts = filename.split('.')
            # 适用于 'app.log.1.gz' 这样的格式
            if len(parts) > 2 and parts[-1] == 'gz' and parts[-2].isdigit():
                return int(parts[-2])
            return float('inf') # 其他不规范的格式排在最后

        log_files.sort(key=sort_key)
        return jsonify(log_files)
    except Exception as e:
        logging.error(f"API: 无法列出日志文件: {e}", exc_info=True)
        return jsonify({"error": "无法读取日志文件列表"}), 500

@logs_bp.route('/view', methods=['GET'])
@admin_required
def view_log_file():
    """查看指定日志文件的内容，自动处理 .gz 文件"""
    # 安全性第一：防止目录遍历攻击
    filename = secure_filename(request.args.get('filename', ''))
    if not filename or not filename.startswith('app.log'):
        abort(403, "禁止访问非日志文件或无效的文件名。")

    full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)

    # 再次确认最终路径仍然在合法的日志目录下
    if not os.path.abspath(full_path).startswith(os.path.abspath(config_manager.LOG_DIRECTORY)):
        abort(403, "检测到非法路径访问。")
        
    if not os.path.exists(full_path):
        abort(404, "文件未找到。")

    try:
        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()  # 将所有行读入一个列表
        
        lines.reverse()  # 反转列表顺序
        content = "".join(lines)  # 将列表重新组合成一个字符串
        
        return Response(content, mimetype='text/plain')
        
    except Exception as e:
        logging.error(f"API: 读取日志文件 '{filename}' 时出错: {e}", exc_info=True)
        abort(500, f"读取文件 '{filename}' 时发生内部错误。")

@logs_bp.route('/search', methods=['GET'])
@admin_required
def search_all_logs():
    """
    在所有日志文件 (app.log*) 中搜索关键词。
    """
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({"error": "搜索关键词不能为空"}), 400
    TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")

    search_results = []
    
    try:
        # 1. 获取并排序所有日志文件，确保从新到旧搜索
        all_files = os.listdir(config_manager.LOG_DIRECTORY)
        log_files = [f for f in all_files if f.startswith('app.log')]
        
        # --- 代码修改点 ---
        # 简化了排序键，不再处理 .gz 后缀
        def sort_key(filename):
            if filename == 'app.log':
                return -1  # app.log 永远排在最前面
            parts = filename.split('.')
            # 适用于 app.log.1, app.log.2 等格式
            if len(parts) == 3 and parts[0] == 'app' and parts[1] == 'log' and parts[2].isdigit():
                return int(parts[2])
            return float('inf') # 其他不符合格式的文件排在最后
        
        log_files.sort(key=sort_key)

        # 2. 遍历每个文件进行搜索
        for filename in log_files:
            full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)
            try:
                # --- 代码修改点 ---
                # 移除了 opener 的判断，直接使用 open 函数
                with open(full_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    # 逐行读取，避免内存爆炸
                    for line_num, line in enumerate(f, 1):
                        # 不区分大小写搜索
                        if query.lower() in line.lower():
                            match = TIMESTAMP_REGEX.search(line)
                            line_date = match.group(1) if match else "" # 如果匹配失败则为空字符串
                            
                            # 2. 将提取到的日期添加到返回结果中
                            search_results.append({
                                "file": filename,
                                "line_num": line_num,
                                "content": line.strip(),
                                "date": line_date  # <--- 新增的日期字段
                            })
            except Exception as e:
                # 如果单个文件读取失败，记录错误并继续
                logging.warning(f"API: 搜索时无法读取文件 '{filename}': {e}")

        search_results.sort(key=lambda x: x['date'], reverse=True)
        return jsonify(search_results)

    except Exception as e:
        logging.error(f"API: 全局日志搜索时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "搜索过程中发生服务器内部错误"}), 500

@logs_bp.route('/search_context', methods=['GET'])
@admin_required
def search_logs_with_context():
    """
    【V9 - 最终无干扰版】在所有日志文件中定位与关键词匹配的、完整的、未被中断的处理块。
    此版本能正确忽略在目标块内部出现的、不相关的其他处理块起始标记。
    """
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({"error": "搜索关键词不能为空"}), 400

    START_MARKER = re.compile(r"(开始处理|手动处理)\s'(.+?)'\s\(TMDb ID: \d+\)")
    END_MARKER = re.compile(r"最终状态: 处理完成\s'(.+?)'")
    TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")

    found_blocks = []
    
    try:
        all_files = os.listdir(config_manager.LOG_DIRECTORY)
        log_files = sorted([f for f in all_files if f.startswith('app.log')], reverse=True)

        for filename in log_files:
            full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)
            
            current_block = []
            active_item_name = None # 状态变量：记录当前正在追踪的片名

            try:
                with open(full_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue

                        start_match = START_MARKER.search(line)
                        end_match = END_MARKER.search(line)

                        # ★★★★★★★★★★★★★★★ 核心逻辑修正 ★★★★★★★★★★★★★★★
                        if not active_item_name and start_match:
                            # 状态：当前未追踪任何块，并且遇到了一个起始标记
                            start_item_name = start_match.group(2)
                            if query.lower() in start_item_name.lower():
                                # 关键词匹配！开始追踪
                                active_item_name = start_item_name
                                current_block = [line]
                        
                        elif active_item_name and end_match:
                            # 状态：正在追踪一个块，并且遇到了一个结束标记
                            end_item_name = end_match.group(1)
                            
                            if end_item_name == active_item_name:
                                # 完美闭环！保存
                                current_block.append(line)
                                
                                block_date = "Unknown Date"
                                if current_block:
                                    match = TIMESTAMP_REGEX.search(current_block[0])
                                    if match:
                                        block_date = match.group(1).split(' ')[0]

                                found_blocks.append({
                                    "file": filename,
                                    "date": block_date,
                                    "lines": current_block
                                })
                                
                                # 任务完成，重置状态
                                active_item_name = None
                                current_block = []
                            else:
                                # 结束的片名不匹配，说明日志错乱，抛弃当前块
                                active_item_name = None
                                current_block = []

                        elif active_item_name:
                            # 状态：正在追踪一个块，当前行是普通日志，追加
                            current_block.append(line)
                        
                        # 如果 active_item_name 为空，且当前行不是匹配的 start_match，则什么都不做，直接忽略

            except Exception as e:
                logging.warning(f"API: 上下文搜索时无法读取文件 '{filename}': {e}")
        
        found_blocks.sort(key=lambda x: x['date'], reverse=True)
        
        return jsonify(found_blocks)

    except Exception as e:
        logging.error(f"API: 上下文日志搜索时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "搜索过程中发生服务器内部错误"}), 500
