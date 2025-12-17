# routes/logs.py

from flask import Blueprint, request, jsonify, abort, Response
import logging
import os
from werkzeug.utils import secure_filename
import re
import html
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

def render_log_html(blocks, query):
    """
    辅助函数：将日志块渲染为漂亮的深色主题 HTML
    """
    css_styles = """
    <style>
        :root {
            --bg-color: #1e1e1e;
            --text-color: #d4d4d4;
            --block-bg: #252526;
            --border-color: #333;
            --accent-color: #007acc;
            --highlight-bg: #414339;
            --highlight-text: #f8f8f2;
        }
        body {
            background-color: var(--bg-color);
            color: var(--text-color);
            font-family: 'JetBrains Mono', 'Fira Code', Consolas, 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.5;
            margin: 0;
            padding: 20px;
            font-style: normal !important; /* 强制去除斜体 */
        }
        h2 { color: #fff; border-bottom: 1px solid var(--border-color); padding-bottom: 10px; }
        .summary { margin-bottom: 20px; color: #888; }
        .log-block {
            background-color: var(--block-bg);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        .block-header {
            background-color: #333;
            padding: 8px 15px;
            font-size: 12px;
            color: #aaa;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
        }
        .log-content {
            padding: 10px 15px;
            white-space: pre-wrap; /* 保留换行 */
            overflow-x: auto;
        }
        .line { display: block; }
        .line:hover { background-color: #2a2d2e; }
        
        /* 语法高亮 */
        .ts { color: #569cd6; margin-right: 10px; opacity: 0.7; } /* 时间戳 */
        .level-INFO { color: #4ec9b0; font-weight: bold; }
        .level-DEBUG { color: #808080; }
        .level-WARN { color: #ce9178; }
        .level-ERROR { color: #f44747; font-weight: bold; }
        .arrow { color: #c586c0; font-weight: bold; } /* ➜ 符号 */
        .keyword { background-color: var(--highlight-bg); color: var(--highlight-text); border-radius: 2px; padding: 0 2px; }
    </style>
    """

    html_content = [f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>日志追踪: {html.escape(query)}</title>
        {css_styles}
    </head>
    <body>
        <h2>🔍 追踪日志: <span style="color: #4ec9b0;">{html.escape(query)}</span></h2>
        <div class="summary">共找到 {len(blocks)} 个完整处理流程</div>
    """]

    for block in blocks:
        file_name = block['file']
        date_str = block['date']
        lines = block['lines']
        
        html_content.append(f"""
        <div class="log-block">
            <div class="block-header">
                <span>📄 {html.escape(file_name)}</span>
                <span>📅 {html.escape(date_str)}</span>
            </div>
            <div class="log-content">
        """)

        for line in lines:
            # 1. HTML 转义，防止脚本注入
            safe_line = html.escape(line)
            
            # 2. 高亮处理
            # 高亮时间戳 (假设开头是时间)
            safe_line = re.sub(r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})', r'<span class="ts">\1</span>', safe_line)
            
            # 高亮日志级别
            safe_line = safe_line.replace('INFO', '<span class="level-INFO">INFO</span>')
            safe_line = safe_line.replace('DEBUG', '<span class="level-DEBUG">DEBUG</span>')
            safe_line = safe_line.replace('WARNING', '<span class="level-WARN">WARNING</span>')
            safe_line = safe_line.replace('ERROR', '<span class="level-ERROR">ERROR</span>')
            
            # 高亮特殊符号
            safe_line = safe_line.replace('➜', '<span class="arrow">➜</span>')
            
            # 高亮搜索关键词 (忽略大小写)
            pattern = re.compile(re.escape(query), re.IGNORECASE)
            safe_line = pattern.sub(lambda m: f'<span class="keyword">{m.group(0)}</span>', safe_line)

            html_content.append(f'<span class="line">{safe_line}</span>')

        html_content.append("""
            </div>
        </div>
        """)

    html_content.append("</body></html>")
    return "".join(html_content)


@logs_bp.route('/search_context', methods=['GET'])
@admin_required
def search_logs_with_context():
    """
    【V11 - 最终美化版】
    1. 精准截取 '收到入库' -> '任务结束' 的闭环日志。
    2. 自动剔除中间乱入的其他媒体日志。
    3. 支持 format=html 参数，直接返回 VS Code 风格的深色日志页面。
    """
    query = request.args.get('q', '').strip()
    output_format = request.args.get('format', 'json').lower() # 新增 format 参数

    if not query:
        return jsonify({"error": "搜索关键词不能为空"}), 400

    # --- 正则定义 (保持 V10 的精准逻辑) ---
    START_MARKER = re.compile(r"(?:Webhook: 收到入库事件|手动处理)\s'(.+?)'")
    END_MARKER = re.compile(r"后台任务 'Webhook完整处理:\s(.+?)'\s结束")
    INTERFERENCE_MARKER = re.compile(r"(?:Webhook: 收到入库事件|项目|预检.+?检测到|开始检查|开始处理|处理完成)\s'(.+?)'")
    TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})")

    found_blocks = []
    
    try:
        all_files = os.listdir(config_manager.LOG_DIRECTORY)
        log_files = sorted([f for f in all_files if f.startswith('app.log')], reverse=True)

        for filename in log_files:
            full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)
            
            current_block = []
            active_item_name = None 

            try:
                with open(full_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line_strip = line.strip()
                        if not line_strip: continue

                        start_match = START_MARKER.search(line_strip)
                        end_match = END_MARKER.search(line_strip)

                        if not active_item_name:
                            if start_match:
                                item_name = start_match.group(1)
                                if query.lower() in item_name.lower():
                                    active_item_name = item_name
                                    current_block = [line]
                            continue

                        # --- 正在追踪 ---
                        if end_match:
                            end_name = end_match.group(1)
                            if end_name == active_item_name:
                                current_block.append(line)
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
                                active_item_name = None
                                current_block = []
                                continue

                        # 去噪逻辑
                        interference_match = INTERFERENCE_MARKER.search(line_strip)
                        if interference_match:
                            other_name = interference_match.group(1)
                            if other_name != active_item_name:
                                continue 

                        # 防止死锁：遇到同名新起点
                        if start_match:
                            new_name = start_match.group(1)
                            if new_name == active_item_name:
                                current_block = [line]
                                continue

                        current_block.append(line)

            except Exception as e:
                logging.warning(f"API: 读取文件 '{filename}' 出错: {e}")
        
        found_blocks.sort(key=lambda x: x['date'], reverse=True)
        
        # --- 关键修改：根据 format 参数返回不同格式 ---
        if output_format == 'html':
            html_response = render_log_html(found_blocks, query)
            return Response(html_response, mimetype='text/html')
        else:
            return jsonify(found_blocks)

    except Exception as e:
        logging.error(f"API: 上下文日志搜索错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
