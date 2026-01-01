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
    """
    查看指定日志文件的内容
    支持 format=html 参数，返回美化后的 HTML
    """
    filename = secure_filename(request.args.get('filename', ''))
    output_format = request.args.get('format', 'json').lower() # 新增参数

    if not filename or not filename.startswith('app.log'):
        abort(403, "禁止访问非日志文件或无效的文件名。")

    full_path = os.path.join(config_manager.LOG_DIRECTORY, filename)

    if not os.path.abspath(full_path).startswith(os.path.abspath(config_manager.LOG_DIRECTORY)):
        abort(403, "检测到非法路径访问。")
        
    if not os.path.exists(full_path):
        abort(404, "文件未找到。")

    try:
        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        # ★★★ 核心：倒序排列，最新的在最上面 ★★★
        lines.reverse() 
        
        if output_format == 'html':
            # 构造一个伪造的 block 结构，以便复用 render_log_html
            # 这样普通查看和搜索查看的风格就完全一致了
            fake_blocks = [{
                'file': filename,
                'lines': lines # 已经是倒序的了
            }]
            # 调用渲染函数 (query为空，不进行高亮)
            html_response = render_log_html(fake_blocks, query='')
            return Response(html_response, mimetype='text/html')
        
        else:
            # 保持原有的纯文本/JSON兼容性 (虽然前端可能不再用它了)
            content = "".join(lines)
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
    辅助函数：将日志块渲染为极简、高亮、去噪的 HTML
    """
    css_styles = """
    <style>
        :root {
            --bg-color: #1e1e1e;
            --time-color: #6a9955; /* 柔和的绿色时间 */
            --arrow-color: #569cd6; /* 蓝色箭头 */
            --text-color: #d4d4d4;
            --debug-color: #808080; /* Debug 变暗 */
            --info-color: #b5cea8;  /* Info 亮色 */
            --warn-color: #ce9178;
            --error-color: #f44747;
            --highlight-bg: #264f78;
        }
        body {
            background-color: var(--bg-color);
            color: var(--text-color);
            font-family: 'JetBrains Mono', 'Fira Code', Consolas, monospace;
            font-size: 13px;
            line-height: 1.6;
            margin: 0;
            padding: 15px;
            height: 100vh;
            box-sizing: border-box;
        }
        /* 隐藏滚动条但允许滚动 */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #1e1e1e; }
        ::-webkit-scrollbar-thumb { background: #424242; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #4f4f4f; }

        .log-block {
            margin-bottom: 25px; /* 块之间留出呼吸空间 */
            border-left: 2px solid #333;
            padding-left: 10px;
        }
        .block-header {
            font-size: 12px;
            color: #555;
            margin-bottom: 5px;
            font-style: italic;
            border-bottom: 1px dashed #333;
            padding-bottom: 2px;
            display: inline-block;
        }
        
        .line { 
            display: flex; 
            align-items: flex-start; /* 顶部对齐 */
        }
        .line:hover { background-color: #2a2d2e; }
        
        /* 时间列：固定宽度，不换行 */
        .ts { 
            color: var(--time-color); 
            min-width: 70px; 
            margin-right: 10px; 
            opacity: 0.8;
            font-size: 12px;
            user-select: none; /* 防止复制时把时间也复制进去，看个人喜好 */
        }

        /* 消息体 */
        .msg { 
            white-space: pre-wrap; 
            word-break: break-all; 
            flex: 1;
        }

        /* 级别颜色定义 */
        .lvl-DEBUG { color: var(--debug-color); }
        .lvl-INFO { color: var(--info-color); }
        .lvl-WARNING { color: var(--warn-color); }
        .lvl-ERROR { color: var(--error-color); font-weight: bold; }
        
        /* 搜索高亮 */
        .keyword { background-color: var(--highlight-bg); color: #fff; border-radius: 2px; }
        
        /* 箭头符号优化 */
        .arrow { color: var(--arrow-color); margin-right: 5px; font-weight: bold;}
    </style>
    """

    html_content = [f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        {css_styles}
    </head>
    <body>
    """]

    # 正则：提取 时间(Group 2), 级别(Group 4), 消息(Group 5)
    # 忽略：日期(Group 1), Logger名(Group 3)
    # 匹配格式示例: 2025-12-17 18:30:58 ,926 - database.actor_db - INFO - -> 消息...
    LOG_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\d+\s+-\s+(.*?)\s+-\s+(INFO|DEBUG|WARNING|ERROR)\s+-\s+(.*)$")

    for block in blocks:
        file_name = block['file']
        # ★★★ 新增：获取该块对应的日期 (search_logs_with_context 已经传了这个字段)
        # 如果是普通查看模式，date 可能不存在，所以用 .get()
        full_date = block.get('date', '') 
        
        # 构造页眉内容
        header_html = f"📄 {html.escape(file_name)}"
        if full_date:
            # 提取日期部分 (YYYY-MM-DD)，因为行内已经有具体时间了
            date_only = full_date.split(' ')[0] if ' ' in full_date else full_date
            header_html += f" <span style='margin-left: 12px; color: #888; font-weight: normal;'>📅 {html.escape(date_only)}</span>"

        html_content.append(f"""
        <div class="log-block">
            <div class="block-header">{header_html}</div>
        """)

        for line in block['lines']:
            line = line.strip()
            if not line: continue

            match = LOG_PATTERN.match(line)
            if match:
                # 提取我们需要的部分
                time_str = match.group(2) # 18:30:58
                level = match.group(4)    # INFO
                message = match.group(5)  # -> 消息内容...

                # 处理消息内容中的 HTML 转义
                safe_msg = html.escape(message)
                
                # 再次美化消息内部：
                # 1. 高亮箭头
                safe_msg = safe_msg.replace('➜', '<span class="arrow">➜</span>')
                safe_msg = safe_msg.replace('-&gt;', '<span class="arrow">➜</span>') # 处理转义后的 ->
                
                # 2. 高亮搜索关键词 (忽略大小写)
                if query:
                    pattern = re.compile(re.escape(query), re.IGNORECASE)
                    safe_msg = pattern.sub(lambda m: f'<span class="keyword">{m.group(0)}</span>', safe_msg)

                # 生成行 HTML
                html_content.append(f"""
                <div class="line lvl-{level}">
                    <span class="ts">{time_str}</span>
                    <span class="msg">{safe_msg}</span>
                </div>
                """)
            else:
                # 匹配失败（可能是堆栈报错信息），直接显示原样，标红
                safe_line = html.escape(line)
                html_content.append(f"""
                <div class="line lvl-ERROR">
                    <span class="ts">-----</span>
                    <span class="msg">{safe_line}</span>
                </div>
                """)

        html_content.append("</div>")

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
    START_MARKER = re.compile(r"后台任务\s'(?:Webhook: 收到入库事件|手动处理):\s(.+?)'\s开始执行")
    END_MARKER = re.compile(r"后台任务\s'(?:元数据同步:|手动处理):\s(.+?)'\s结束")
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
