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
    【V10 - 智能去噪版】
    精准截取从 '收到入库事件/手动处理' 到 '后台任务结束' 的完整日志块。
    自动剔除中间穿插的其他媒体（如 '第 xx 集'）的入库、预检、队列等干扰日志。
    """
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({"error": "搜索关键词不能为空"}), 400

    # 1. 定义正则表达式
    # ---------------------------------------------------------
    # 捕获组1: 媒体名称
    
    # [起点] 匹配: "Webhook: 收到入库事件 'Name'" 或 "手动处理 'Name'"
    START_MARKER = re.compile(r"(?:Webhook: 收到入库事件|手动处理)\s'(.+?)'")
    
    # [终点] 匹配: "后台任务 'Webhook完整处理: Name' 结束"
    # 注意：日志中结束语包含 "结束，最终状态..."，所以匹配 "结束" 即可
    END_MARKER = re.compile(r"后台任务 'Webhook完整处理:\s(.+?)'\s结束")

    # [干扰项检测] 
    # 如果当前行包含以下模式，且名字不是当前追踪的名字，则视为干扰
    # 涵盖: 入库事件, 队列项目, 预检检测, 开始处理, 处理完成
    INTERFERENCE_MARKER = re.compile(r"(?:Webhook: 收到入库事件|项目|预检.+?检测到|开始检查|开始处理|处理完成)\s'(.+?)'")

    # [日期提取]
    TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")
    # ---------------------------------------------------------

    found_blocks = []
    
    try:
        # 按时间倒序获取日志文件（优先看最新的日志）
        all_files = os.listdir(config_manager.LOG_DIRECTORY)
        log_files = sorted([f for f in all_files if f.startswith('app.log')], reverse=True)

        for filename in log_files:
            full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)
            
            current_block = []
            active_item_name = None # 当前正在追踪的片名 (例如: "方世玉与洪熙官")

            try:
                with open(full_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line_strip = line.strip()
                        if not line_strip: continue

                        # 尝试匹配起点和终点
                        start_match = START_MARKER.search(line_strip)
                        end_match = END_MARKER.search(line_strip)

                        # --- 场景 A: 尚未开始追踪，寻找起点 ---
                        if not active_item_name:
                            if start_match:
                                item_name = start_match.group(1)
                                # 只有当名字包含搜索关键词时才开始追踪
                                if query.lower() in item_name.lower():
                                    active_item_name = item_name
                                    current_block = [line] # 初始化块
                            continue

                        # --- 场景 B: 正在追踪中 (active_item_name 有值) ---
                        
                        # 1. 检查是否是当前追踪对象的【终点】
                        if end_match:
                            end_name = end_match.group(1)
                            if end_name == active_item_name:
                                # 完美闭环：找到结束语，保存并重置
                                current_block.append(line)
                                
                                # 提取日期
                                block_date = "Unknown Date"
                                if current_block:
                                    date_match = TIMESTAMP_REGEX.search(current_block[0])
                                    if date_match:
                                        block_date = date_match.group(1)

                                found_blocks.append({
                                    "file": filename,
                                    "date": block_date,
                                    "lines": current_block
                                })
                                
                                # 重置状态，继续寻找下一个匹配
                                active_item_name = None
                                current_block = []
                                continue

                        # 2. 智能去噪逻辑 (核心优化)
                        # 如果行中明确包含了【其他】媒体对象的关键操作日志，则跳过该行
                        interference_match = INTERFERENCE_MARKER.search(line_strip)
                        if interference_match:
                            other_name = interference_match.group(1)
                            # 如果这行日志提到的名字 不是 当前追踪的名字，那就是干扰项 (例如: '第 15 集')
                            if other_name != active_item_name:
                                continue 

                        # 3. 补充逻辑：防止死锁
                        # 如果遇到了同一个名字的【新的起点】，说明上一次处理可能异常中断了没有打印结束语
                        # 这种情况下，我们把之前的块作废（或者保存为残缺块），重新开始追踪新的
                        if start_match:
                            new_name = start_match.group(1)
                            if new_name == active_item_name:
                                # 发现重复起点，重置当前块，从这行重新开始
                                current_block = [line]
                                continue

                        # 4. 常规日志：添加到当前块
                        # 这里包含：通用日志、当前片名的详细日志、以及未被识别为干扰的日志
                        current_block.append(line)

            except Exception as e:
                logging.warning(f"API: 上下文搜索时无法读取文件 '{filename}': {e}")
        
        # 按日期倒序排列结果
        found_blocks.sort(key=lambda x: x['date'], reverse=True)
        
        return jsonify(found_blocks)

    except Exception as e:
        logging.error(f"API: 上下文日志搜索时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "搜索过程中发生服务器内部错误"}), 500
