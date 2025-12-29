# handler/poster_generator.py
import os
import requests
import io
import glob
from PIL import Image, ImageDraw, ImageFont
import config_manager
from database.connection import get_db_connection
from database import media_db

STATUS_CONF = {
    'WANTED': {'color': '#2196F3', 'text': '待订阅'},
    'SUBSCRIBED': {'color': '#FF9800', 'text': '已订阅'},
    'PENDING_RELEASE': {'color': '#9C27B0', 'text': '上线日期'},
    'PAUSED': {'color': '#9E9E9E', 'text': '暂无资源'},
    'IGNORED': {'color': '#F44336', 'text': '已忽略'}
}

INTERNAL_DATA_DIR = "/config"

def cleanup_placeholder(tmdb_id):
    """
    智能清理：只有当该 ID 在数据库中没有任何活跃订阅任务时，才物理删除文件。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT subscription_status FROM media_metadata WHERE tmdb_id = %s", 
                (str(tmdb_id),)
            )
            rows = cursor.fetchall()
            # 如果还有任何类型处于订阅相关状态，则保留海报
            active_statuses = {'WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED'}
            for row in rows:
                if row.get('subscription_status') in active_statuses:
                    return 
    except: pass

    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    for f in glob.glob(os.path.join(cache_dir, f"{tmdb_id}_*.jpg")):
        try: os.remove(f)
        except: pass

def get_missing_poster(tmdb_id, status, poster_path, release_date=None):
    """
    生成单张占位海报 (2025 酷炫流媒体版 - 全状态通用)
    设计：底部黑色渐变遮罩 + 极简文字 + 状态色条 + 英文副标题
    """
    if status == 'NONE':
        cleanup_placeholder(tmdb_id)
        return None
        
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{tmdb_id}_{status}.jpg")
    
    # 清理旧图
    for f in glob.glob(os.path.join(cache_dir, f"{tmdb_id}_*.jpg")):
        if f != cache_path:
            try: os.remove(f)
            except: pass

    if os.path.exists(cache_path):
        return cache_path

    # 1. 加载底图
    img = None
    if poster_path:
        try:
            resp = requests.get(f"https://image.tmdb.org/t/p/w500{poster_path}", timeout=5)
            img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
        except: pass
    
    if img is None:
        img = Image.new('RGBA', (500, 750), color='#1A1A1A')
    else:
        img = img.resize((500, 750), Image.Resampling.LANCZOS)

    # 2. 准备配置
    conf = STATUS_CONF.get(status, STATUS_CONF['WANTED'])
    accent_color = conf['color']
    
    # 3. 绘制底部黑色渐变 (Cinematic Gradient)
    gradient = Image.new('RGBA', img.size, (0,0,0,0))
    draw_grad = ImageDraw.Draw(gradient)
    
    grad_height = int(img.height * 0.55) 
    start_y = img.height - grad_height
    
    for y in range(grad_height):
        # 保持之前的渐变参数，效果很好
        alpha = int((y / grad_height) ** 1.2 * 250) 
        draw_grad.line([(0, start_y + y), (img.width, start_y + y)], fill=(0, 0, 0, alpha))
        
    img = Image.alpha_composite(img, gradient)
    draw = ImageDraw.Draw(img)

    # 4. 绘制文字和装饰
    font_path = os.path.join(INTERNAL_DATA_DIR, 'cover_generator', 'fonts', 'zh_font.ttf')
    
    try:
        font_date = ImageFont.truetype(font_path, 62) 
        font_label = ImageFont.truetype(font_path, 26) 
    except:
        font_date = ImageFont.load_default()
        font_label = ImageFont.load_default()

    # --- ★★★ 核心修改：全状态文案适配 ★★★ ---
    # 定义副标题映射 (英文装饰，提升质感)
    sub_text_map = {
        'WANTED': 'QUEUED',           # 待订阅 -> 队列中
        'SUBSCRIBED': 'ACTIVE',       # 已订阅 -> 活跃中
        'PENDING_RELEASE': 'COMING SOON', # 未发行 -> 即将到来
        'PAUSED': 'NO SOURCES',       # 暂无资源 -> 无资源
        'IGNORED': 'IGNORED'          # 已忽略 -> 已忽略
    }

    if status == 'PENDING_RELEASE' and release_date:
        # 特殊逻辑：有日期显示日期
        main_text = str(release_date)
        sub_text = "COMING SOON | 即将上线"
    else:
        # 通用逻辑：显示中文状态名
        main_text = conf['text'] # 如 "待订阅", "已订阅"
        # 获取对应的英文副标题，如果没有则显示状态码
        sub_text = sub_text_map.get(status, status)

    # --- 布局计算 (保持优化后的参数) ---
    base_y = img.height - 200
    
    # 1. 装饰线条 (颜色跟随状态变化)
    line_w = 60
    line_h = 6
    line_x = (img.width - line_w) // 2
    line_y = base_y 
    
    draw.rounded_rectangle(
        [line_x, line_y, line_x + line_w, line_y + line_h], 
        radius=3, 
        fill=accent_color
    )

    # 2. 主标题 (日期 或 中文状态)
    left, top, right, bottom = draw.textbbox((0, 0), main_text, font=font_date)
    text_w = right - left
    text_h = bottom - top
    
    text_y = line_y + 35
    
    # 绘制阴影和文字
    draw.text(((img.width - text_w)/2 + 2, text_y + 3), main_text, font=font_date, fill=(0,0,0,160))
    draw.text(((img.width - text_w)/2, text_y), main_text, font=font_date, fill="#FFFFFF")

    # 3. 副标题 (英文装饰)
    left, top, right, bottom = draw.textbbox((0, 0), sub_text, font=font_label)
    sub_w = right - left
    
    sub_y = text_y + text_h + 18
    
    draw.text(((img.width - sub_w)/2, sub_y), sub_text, font=font_label, fill="#BBBBBB")

    # 5. 保存
    img.convert('RGB').save(cache_path, "JPEG", quality=95)
    return cache_path

def sync_all_subscription_posters():
    """
    ✨ 增强版：全量同步并清理占位海报
    """
    import logging
    logger = logging.getLogger(__name__)
    
    subscriptions = media_db.get_all_subscriptions()
    active_tmdb_ids = set()
    
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    os.makedirs(cache_dir, exist_ok=True)

    logger.info(f"  ➜ [占位海报同步] 正在校验 {len(subscriptions) if subscriptions else 0} 个订阅项...")

    if subscriptions:
        for item in subscriptions:
            if item.get('item_type') == 'Season' and item.get('series_tmdb_id'):
                target_id = str(item.get('series_tmdb_id'))
            else:
                target_id = str(item.get('tmdb_id'))
            status = item.get('subscription_status')
            
            if status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED', 'IGNORED']:
                active_tmdb_ids.add(target_id)
                
                # ★★★ 修改 3: 传入 release_date ★★★
                get_missing_poster(
                    tmdb_id=target_id,
                    status=status,
                    poster_path=item.get('poster_path'),
                    release_date=item.get('release_date') # 从数据库字典中获取
                )

    # 3. ✨ 垃圾回收阶段：清理孤儿文件
    # 扫描缓存目录下所有的 jpg 文件
    all_cached_files = glob.glob(os.path.join(cache_dir, "*.jpg"))
    cleanup_count = 0
    
    for file_path in all_cached_files:
        filename = os.path.basename(file_path)
        # 文件名格式通常是: {tmdb_id}_{status}.jpg
        # 我们提取开头的 tmdb_id
        try:
            file_tmdb_id = filename.split('_')[0]
            
            # 如果这个文件的 ID 不在活跃订阅集合里，说明是过期的，直接删除
            if file_tmdb_id not in active_tmdb_ids:
                os.remove(file_path)
                cleanup_count += 1
        except Exception as e:
            logger.warning(f"  ➜ [占位海报同步] 解析缓存文件 {filename} 失败: {e}")

    logger.info(f"  ➜ [占位海报同步] 同步完成。当前活跃海报: {len(active_tmdb_ids)} 张，清理过期海报: {cleanup_count} 张。")