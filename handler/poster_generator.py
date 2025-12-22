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
    'PENDING_RELEASE': {'color': '#9C27B0', 'text': '未发行'},
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

def get_missing_poster(tmdb_id, status, poster_path):
    """生成单张占位海报的核心逻辑"""
    if status == 'NONE':
        cleanup_placeholder(tmdb_id)
        return None
        
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{tmdb_id}_{status}.jpg")
    
    # 清理该 ID 的旧状态海报
    for f in glob.glob(os.path.join(cache_dir, f"{tmdb_id}_*.jpg")):
        if f != cache_path:
            try: os.remove(f)
            except: pass

    if os.path.exists(cache_path):
        return cache_path

    # --- 下面是生成逻辑 (保持不变) ---
    img = None
    if poster_path:
        try:
            resp = requests.get(f"https://image.tmdb.org/t/p/w500{poster_path}", timeout=5)
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        except: pass
    if img is None:
        img = Image.new('RGB', (500, 750), color='#2C2C2C')

    # 绘制印章 (使用之前的印章代码...)
    conf = STATUS_CONF.get(status, STATUS_CONF['WANTED'])
    stamp_w, stamp_h = 340, 150
    stamp_img = Image.new('RGBA', (stamp_w, stamp_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(stamp_img)
    draw.rounded_rectangle([5, 5, stamp_w-5, stamp_h-5], radius=15, fill="#FFFFFF")
    draw.rounded_rectangle([5, 5, stamp_w-5, stamp_h-5], radius=15, outline=conf['color'], width=10)

    font_path = os.path.join(INTERNAL_DATA_DIR, 'cover_generator', 'fonts', 'zh_font.ttf')
    try: font = ImageFont.truetype(font_path, 65)
    except: font = ImageFont.load_default()

    left, top, right, bottom = draw.textbbox((0, 0), conf['text'], font=font)
    draw.text(((stamp_w-(right-left))/2, (stamp_h-(bottom-top))/2 - 8), conf['text'], font=font, fill=conf['color'])

    rotated = stamp_img.rotate(15, expand=True, resample=Image.BICUBIC)
    overlay = Image.new('RGBA', img.size, (0, 0, 0, 0))
    overlay.paste(rotated, ((img.width-rotated.width)//2, (img.height-rotated.height)//2), rotated)
    
    img.paste(overlay, (0, 0), overlay)
    img.convert('RGB').save(cache_path, "JPEG", quality=92)
    return cache_path

def sync_all_subscription_posters():
    """
    ✨ 增强版：全量同步并清理占位海报
    1. 确保所有活跃订阅项都有海报。
    2. 物理删除所有不再属于活跃订阅项的陈旧海报文件。
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # 1. 获取所有活跃订阅项
    subscriptions = media_db.get_all_subscriptions()
    active_tmdb_ids = set()
    
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    os.makedirs(cache_dir, exist_ok=True)

    logger.info(f"  ➜ [占位海报同步] 正在校验 {len(subscriptions) if subscriptions else 0} 个订阅项...")

    # 2. 遍历数据库，生成/校验海报，并记录活跃 ID
    if subscriptions:
        for item in subscriptions:
            tmdb_id = str(item.get('tmdb_id'))
            status = item.get('subscription_status')
            
            # 只有这些状态需要占位海报
            if status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED', 'IGNORED']:
                active_tmdb_ids.add(tmdb_id)
                get_missing_poster(
                    tmdb_id=tmdb_id,
                    status=status,
                    poster_path=item.get('poster_path')
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