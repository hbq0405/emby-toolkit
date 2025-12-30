# handler/poster_generator.py
import os
import requests
import io
import glob
from PIL import Image, ImageDraw, ImageFont, ImageEnhance
import config_manager
from database.connection import get_db_connection
from database import media_db

STATUS_CONF = {
    'WANTED': {'color': '#2196F3', 'text': '待订阅'},
    'SUBSCRIBED': {'color': '#FF9800', 'text': '已订阅'},
    'PENDING_RELEASE': {'color': '#9C27B0', 'text': '未上映'},
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
    生成单张占位海报 (2025 优雅UI卡片版)
    设计：全图压暗微去色 + 状态色内边框 + 底部悬浮黑玻胶囊
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

    # --- ★★★ 核心修改 1: 视觉降噪处理 (虚实区分) ★★★ ---
    # A. 降低饱和度 (0.6): 让颜色不那么艳丽，区别于正片
    enhancer = ImageEnhance.Color(img)
    img = enhancer.enhance(0.6)
    
    # B. 降低亮度 (0.4): 压暗背景，让白色文字和亮色边框更醒目
    enhancer = ImageEnhance.Brightness(img)
    img = enhancer.enhance(0.4)

    draw = ImageDraw.Draw(img)

    # --- ★★★ 核心修改 2: 状态内边框 (UI感) ★★★ ---
    # 在四周画一个半透明的有色边框，像取景框一样
    border_width = 15
    draw.rectangle(
        [0, 0, img.width - 1, img.height - 1], 
        outline=accent_color, 
        width=border_width
    )

    # 3. 准备文字
    font_path = os.path.join(INTERNAL_DATA_DIR, 'cover_generator', 'fonts', 'zh_font.ttf')
    try:
        font_main = ImageFont.truetype(font_path, 52) 
        font_sub = ImageFont.truetype(font_path, 22) 
    except:
        font_main = ImageFont.load_default()
        font_sub = ImageFont.load_default()

    sub_text_map = {
        'WANTED': 'QUEUED',
        'SUBSCRIBED': 'ACTIVE',
        'PENDING_RELEASE': 'COMING SOON',
        'PAUSED': 'NO SOURCES',
        'IGNORED': 'IGNORED'
    }

    if status == 'PENDING_RELEASE' and release_date:
        main_text = str(release_date)
        sub_text = "COMING SOON"
    else:
        main_text = conf['text']
        sub_text = sub_text_map.get(status, status)

    # --- ★★★ 核心修改 3: 底部悬浮胶囊 (Floating Capsule) ★★★ ---
    
    # 计算文字大小
    left, top, right, bottom = draw.textbbox((0, 0), main_text, font=font_main)
    main_w = right - left
    main_h = bottom - top
    
    left, top, right, bottom = draw.textbbox((0, 0), sub_text, font=font_sub)
    sub_w = right - left
    sub_h = bottom - top

    # 胶囊尺寸
    capsule_w = max(main_w, sub_w) + 120 # 左右留白
    if capsule_w < 300: capsule_w = 300  # 最小宽度
    capsule_h = main_h + sub_h + 50      # 上下留白

    capsule_x = (img.width - capsule_w) // 2
    capsule_y = img.height - capsule_h - 100 # 距离底部 100px 悬浮

    # 绘制胶囊背景 (黑色半透明)
    draw.rounded_rectangle(
        [capsule_x, capsule_y, capsule_x + capsule_w, capsule_y + capsule_h],
        radius=20,
        fill=(0, 0, 0, 200), # 黑色背景，透明度 200/255
        outline=None
    )

    # 绘制主标题 (白色)
    main_x = (img.width - main_w) // 2
    main_y = capsule_y + 25
    draw.text((main_x, main_y), main_text, font=font_main, fill="#FFFFFF")

    # 绘制副标题 (使用状态色，呼应边框)
    sub_x = (img.width - sub_w) // 2
    sub_y = main_y + main_h + 8
    draw.text((sub_x, sub_y), sub_text, font=font_sub, fill=accent_color)

    # 5. 保存
    img.convert('RGB').save(cache_path, "JPEG", quality=95)
    return cache_path

def sync_all_subscription_posters():
    """
    全量同步并清理占位海报
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
                
                get_missing_poster(
                    tmdb_id=target_id,
                    status=status,
                    poster_path=item.get('poster_path'),
                    release_date=item.get('release_date')
                )

    # 垃圾回收阶段
    all_cached_files = glob.glob(os.path.join(cache_dir, "*.jpg"))
    cleanup_count = 0
    
    for file_path in all_cached_files:
        filename = os.path.basename(file_path)
        try:
            file_tmdb_id = filename.split('_')[0]
            if file_tmdb_id not in active_tmdb_ids:
                os.remove(file_path)
                cleanup_count += 1
        except Exception as e:
            logger.warning(f"  ➜ [占位海报同步] 解析缓存文件 {filename} 失败: {e}")

    logger.info(f"  ➜ [占位海报同步] 同步完成。当前活跃海报: {len(active_tmdb_ids)} 张，清理过期海报: {cleanup_count} 张。")