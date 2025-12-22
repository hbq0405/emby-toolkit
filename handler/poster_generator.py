# handler/poster_generator.py
import os
import requests
import io
import glob  # <--- 新增：用于匹配旧文件
from PIL import Image, ImageDraw, ImageFont
import config_manager

STATUS_CONF = {
    'WANTED': {'color': '#2196F3', 'text': '待订阅'},
    'SUBSCRIBED': {'color': '#FF9800', 'text': '已订阅'},
    'PENDING_RELEASE': {'color': '#9C27B0', 'text': '未发行'},
    'PAUSED': {'color': '#9E9E9E', 'text': '暂无资源'},
    'IGNORED': {'color': '#F44336', 'text': '已忽略'}
}

INTERNAL_DATA_DIR = "/config"

def cleanup_placeholder(tmdb_id):
    """专门负责删除某个 TMDb ID 关联的所有占位海报"""
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    if not os.path.exists(cache_dir):
        return
    
    # 匹配该 ID 的所有状态文件，如 19995_WANTED.jpg, 19995_NONE.jpg 等
    existing_files = glob.glob(os.path.join(cache_dir, f"{tmdb_id}_*.jpg"))
    for f in existing_files:
        try:
            os.remove(f)
            # logger.debug(f"已彻底清理入库项的占位缓存: {f}")
        except:
            pass

def get_missing_poster(tmdb_id, status, poster_path):
    if status == 'NONE':
        cleanup_placeholder(tmdb_id)
        return None
    cache_dir = os.path.join(INTERNAL_DATA_DIR, "cache", "missing_posters")
    os.makedirs(cache_dir, exist_ok=True)
    
    # 当前期望的文件名
    cache_path = os.path.join(cache_dir, f"{tmdb_id}_{status}.jpg")
    
    # --- ✨ 核心改进：自动清理旧状态海报 ✨ ---
    # 查找所有以该 tmdb_id 开头的 jpg 文件
    existing_files = glob.glob(os.path.join(cache_dir, f"{tmdb_id}_*.jpg"))
    
    for f in existing_files:
        # 如果发现的文件名不是我们当前要用的状态，就删掉它
        if f != cache_path:
            try:
                os.remove(f)
                # print(f"已清理过期海报缓存: {os.path.basename(f)}")
            except Exception as e:
                pass # 忽略删除失败的情况

    # 如果当前状态的海报已经存在，直接返回
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