# services/cover_generator/styles/style_multi_1.py

import logging
import os
import random
import math
import base64
import io
import colorsys
from pathlib import Path
from collections import Counter
import numpy as np
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps

from .badge_drawer import draw_badge

logger = logging.getLogger(__name__)

# ========== 配置 ==========
POSTER_GEN_CONFIG = {
    "ROWS": 3, 
    "COLS": 3, 
    "MARGIN": 22, 
    "CORNER_RADIUS": 46.1, 
    "ROTATION_ANGLE": -15.8,
    "START_X": 1050, 
    "START_Y": -362, 
    "COLUMN_SPACING": 100, 
    "SAVE_COLUMNS": True,
    "CELL_WIDTH": 410, 
    "CELL_HEIGHT": 610, 
    "CANVAS_WIDTH": 1920, 
    "CANVAS_HEIGHT": 1080,
}

# ========== 辅助函数 (从单图风格文件中复制过来) ==========
def is_not_black_white_gray_near(color, threshold=20):
    r, g, b = color
    if (r < threshold and g < threshold and b < threshold) or \
       (r > 255 - threshold and g > 255 - threshold and b > 255 - threshold):
        return False
    gray_diff_threshold = 10
    if abs(r - g) < gray_diff_threshold and abs(g - b) < gray_diff_threshold and abs(r - b) < gray_diff_threshold:
        return False
    return True

def rgb_to_hsv(color):
    r, g, b = [x / 255.0 for x in color]
    return colorsys.rgb_to_hsv(r, g, b)

def hsv_to_rgb(h, s, v):
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return (int(r * 255), int(g * 255), int(b * 255))

def adjust_to_macaron(h, s, v, target_saturation_range=(0.2, 0.7), target_value_range=(0.55, 0.85)):
    adjusted_s = min(max(s, target_saturation_range[0]), target_saturation_range[1])
    adjusted_v = min(max(v, target_value_range[0]), target_value_range[1])
    return adjusted_s, adjusted_v

def find_dominant_vibrant_colors(image, num_colors=5):
    img = image.copy()
    img.thumbnail((100, 100))
    img = img.convert('RGB')
    pixels = list(img.getdata())
    filtered_pixels = [p for p in pixels if is_not_black_white_gray_near(p)]
    if not filtered_pixels: return []
    color_counter = Counter(filtered_pixels)
    dominant_colors = color_counter.most_common(num_colors * 3)
    macaron_colors = []
    seen_hues = set()
    for color, count in dominant_colors:
        h, s, v = rgb_to_hsv(color)
        adjusted_s, adjusted_v = adjust_to_macaron(h, s, v)
        adjusted_rgb = hsv_to_rgb(h, adjusted_s, adjusted_v)
        hue_degree = int(h * 360)
        is_similar_hue = any(abs(hue_degree - seen) < 15 for seen in seen_hues)
        if not is_similar_hue and adjusted_rgb not in macaron_colors:
            macaron_colors.append(adjusted_rgb)
            seen_hues.add(hue_degree)
            if len(macaron_colors) >= num_colors: break
    return macaron_colors

def darken_color(color, factor=0.7):
    r, g, b = color
    return (int(r * factor), int(g * factor), int(b * factor))

def add_film_grain(image, intensity=0.05):
    img_array = np.array(image)
    noise = np.random.normal(0, intensity * 255, img_array.shape)
    img_array = np.clip(img_array + noise, 0, 255).astype(np.uint8)
    return Image.fromarray(img_array)

def add_shadow(img, offset=(5, 5), shadow_color=(0, 0, 0, 100), blur_radius=3):
    shadow_width = img.width + offset[0] + blur_radius * 2
    shadow_height = img.height + offset[1] + blur_radius * 2
    shadow = Image.new("RGBA", (shadow_width, shadow_height), (0, 0, 0, 0))
    shadow_layer = Image.new("RGBA", img.size, shadow_color)
    shadow.paste(shadow_layer, (blur_radius + offset[0], blur_radius + offset[1]))
    shadow = shadow.filter(ImageFilter.GaussianBlur(blur_radius))
    result = Image.new("RGBA", shadow.size, (0, 0, 0, 0))
    result.paste(img, (blur_radius, blur_radius), img if img.mode == "RGBA" else None)
    return Image.alpha_composite(shadow, result)

def draw_text_on_image(image, text, position, font_path, default_font_path, font_size, fill_color=(255, 255, 255, 255), shadow=False, shadow_color=None, shadow_offset=10, shadow_alpha=75):
    img_copy = image.copy()
    text_layer = Image.new('RGBA', img_copy.size, (255, 255, 255, 0))
    shadow_layer = Image.new('RGBA', img_copy.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(text_layer)
    shadow_draw = ImageDraw.Draw(shadow_layer)
    font = ImageFont.truetype(font_path, font_size)
    if shadow:
        fill_color = (fill_color[0], fill_color[1], fill_color[2], 229)
        if shadow_color is None:
            r, g, b = [max(0, int(c * 0.7)) for c in fill_color[:3]]
            shadow_color_with_alpha = (r, g, b, shadow_alpha)
        else:
            shadow_color_with_alpha = shadow_color[:3] + (shadow_alpha,)
        for offset in range(3, shadow_offset + 1, 2):
            shadow_draw.text((position[0] + offset, position[1] + offset), text, font=font, fill=shadow_color_with_alpha)
    draw.text(position, text, font=font, fill=fill_color)
    blurred_shadow = shadow_layer.filter(ImageFilter.GaussianBlur(radius=shadow_offset))
    combined = Image.alpha_composite(img_copy, blurred_shadow)
    return Image.alpha_composite(combined, text_layer)

def draw_multiline_text_on_image(image, text, position, font_path, default_font_path, font_size, line_spacing=10, fill_color=(255, 255, 255, 255), shadow=False, shadow_color=None, shadow_offset=4, shadow_alpha=100):
    img_copy = image.copy()
    text_layer = Image.new('RGBA', img_copy.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(text_layer)
    font = ImageFont.truetype(font_path, font_size)
    lines = text.split(" ")
    if shadow:
        fill_color = (fill_color[0], fill_color[1], fill_color[2], 229)
        if shadow_color is None:
            r, g, b = [max(0, int(c * 0.7)) for c in fill_color[:3]]
            shadow_color_with_alpha = (r, g, b, shadow_alpha)
        else:
            shadow_color_with_alpha = shadow_color[:3] + (shadow_alpha,)
    if len(lines) <= 1:
        if shadow:
            for offset in range(3, shadow_offset + 1, 2):
                draw.text((position[0] + offset, position[1] + offset), text, font=font, fill=shadow_color_with_alpha)
        draw.text(position, text, font=font, fill=fill_color)
        return Image.alpha_composite(img_copy, text_layer), 1
    x, y = position
    for i, line in enumerate(lines):
        current_y = y + i * (font_size + line_spacing)
        if shadow:
            for offset in range(3, shadow_offset + 1, 2):
                draw.text((x + offset, current_y + offset), line, font=font, fill=shadow_color_with_alpha)
        draw.text((x, current_y), line, font=font, fill=fill_color)
    return Image.alpha_composite(img_copy, text_layer), len(lines)

def get_random_color(image_path):
    try:
        img = Image.open(image_path)
        width, height = img.size
        random_x = random.randint(int(width * 0.5), int(width * 0.8))
        random_y = random.randint(int(height * 0.5), int(height * 0.8))
        pixel = img.getpixel((random_x, random_y))
        return pixel[:3] + (255,) if isinstance(pixel, tuple) else (pixel, pixel, pixel, 255)
    except Exception:
        return (random.randint(50, 200), random.randint(50, 200), random.randint(50, 200), 255)

def draw_color_block(image, position, size, color):
    draw = ImageDraw.Draw(image)
    draw.rectangle([position, (position[0] + size[0], position[1] + size[1])], fill=color)
    return image

def create_gradient_background(width, height, color=None):
    def _normalize_rgb(input_rgb):
        if isinstance(input_rgb, tuple):
            if len(input_rgb) == 2 and isinstance(input_rgb[0], tuple): return _normalize_rgb(input_rgb[0])
            if len(input_rgb) >= 3: return input_rgb[:3]
        raise ValueError(f"无法识别的颜色格式: {input_rgb!r}")
    def _is_mid_bright_hsl(input_rgb, min_l=0.3, max_l=0.7):
        r, g, b = [c/255.0 for c in _normalize_rgb(input_rgb)]
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        return min_l <= l <= max_l
    selected_color = None
    if isinstance(color, list) and color:
        for c in color:
            if _is_mid_bright_hsl(c):
                selected_color = _normalize_rgb(c)
                break
    if selected_color is None:
        h = random.uniform(0, 1)
        l = random.uniform(0.5, 0.8)
        s = random.uniform(0.5, 1.0)
        r, g, b = [int(c*255) for c in colorsys.hls_to_rgb(h, l, s)]
        selected_color = (r, g, b)
    r, g, b = [int(c * 0.65) for c in selected_color]
    color1 = (max(0, r), max(0, g), max(0, b), 255)
    r2, g2, b2 = [min(255, int(c * 1.9)) for c in color1[:3]]
    color2 = (r2, g2, b2, 255)
    left_image = Image.new("RGBA", (width, height), color1)
    right_image = Image.new("RGBA", (width, height), color2)
    mask = Image.new("L", (width, height), 0)
    mask_data = [int(255.0 * (x / width) ** 0.7) for y in range(height) for x in range(width)]
    mask.putdata(mask_data)
    return Image.composite(right_image, left_image, mask)

def get_poster_primary_color(image_path):
    try:
        img = Image.open(image_path).resize((100, 150), Image.LANCZOS).convert('RGBA')
        pixels = list(img.getdata())
        filtered_pixels = [(r, g, b, 255) for r, g, b, a in pixels if a > 200 and not (r < 30 and g < 30 and b < 30) and not (r > 220 and g > 220 and b > 220)]
        if not filtered_pixels: filtered_pixels = [(p[0], p[1], p[2], 255) for p in pixels if p[3] > 100]
        if not filtered_pixels: return [(150, 100, 50, 255)]
        return Counter(filtered_pixels).most_common(10)
    except Exception:
        return [(150, 100, 50, 255)]

def create_blur_background(image_path, template_width, template_height, background_color, blur_size, color_ratio, lighten_gradient_strength=0.6):
    # 【修复】从 RGBA 改为 RGB，避免通道不匹配
    original_img = Image.open(image_path).convert('RGB')
    bg_img = ImageOps.fit(original_img.copy(), (template_width, template_height), method=Image.LANCZOS).filter(ImageFilter.GaussianBlur(radius=int(blur_size)))
    
    actual_color = darken_color(background_color, 0.85)
    # 确保 bg_color 是 3 通道
    bg_color = actual_color[:3]
    
    # bg_img_array 现在是 (H, W, 3)
    bg_img_array = np.array(bg_img, dtype=float)
    # bg_color_array 是 (1, 1, 3)，形状匹配
    bg_color_array = np.array([[bg_color]], dtype=float)
    
    blended_bg_array = np.clip(bg_img_array * (1 - float(color_ratio)) + bg_color_array * float(color_ratio), 0, 255).astype(np.uint8)
    
    # 【修复】从 RGB 数组创建图像，然后转换为 RGBA 以进行后续合成
    blended_bg_img = Image.fromarray(blended_bg_array, 'RGB').convert('RGBA')

    if lighten_gradient_strength > 0:
        gradient_mask = Image.new("L", (template_width, template_height), 0)
        draw_mask = ImageDraw.Draw(gradient_mask)
        max_alpha = int(255 * np.clip(lighten_gradient_strength, 0.0, 1.0))
        for x in range(template_width):
            draw_mask.line([(x, 0), (x, template_height)], fill=int((x / template_width) * max_alpha))
        lighten_layer = Image.new("RGBA", (template_width, template_height), (255, 255, 255, 0))
        lighten_layer.putalpha(gradient_mask)
        blended_bg_img = Image.alpha_composite(blended_bg_img, lighten_layer)
        
    return add_film_grain(blended_bg_img, intensity=0.03)

def image_to_base64(image):
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')

# ========== 主函数 ==========
def create_style_multi_1(library_dir, title, font_path, font_size=(1,1), is_blur=False, blur_size=50, color_ratio=0.8, item_count=None, config=None):
    try:
        zh_font_size_ratio, en_font_size_ratio = font_size
        if int(blur_size) < 0: blur_size = 50
        if not (0 <= float(color_ratio) <= 1): color_ratio = 0.8
        if not float(zh_font_size_ratio) > 0: zh_font_size_ratio = 1
        if not float(en_font_size_ratio) > 0: en_font_size_ratio = 1
        
        zh_font_path, en_font_path = font_path
        title_zh, title_en = title
        
        poster_folder = Path(library_dir)
        first_image_path = poster_folder / "1.jpg"
        
        rows, cols, margin, corner_radius, rotation_angle, start_x, start_y, column_spacing = [POSTER_GEN_CONFIG[k] for k in ["ROWS", "COLS", "MARGIN", "CORNER_RADIUS", "ROTATION_ANGLE", "START_X", "START_Y", "COLUMN_SPACING"]]
        template_width, template_height = POSTER_GEN_CONFIG["CANVAS_WIDTH"], POSTER_GEN_CONFIG["CANVAS_HEIGHT"]

        color_img = Image.open(first_image_path).convert("RGB")
        vibrant_colors = find_dominant_vibrant_colors(color_img)
        soft_colors = [(237, 159, 77), (255, 183, 197), (186, 225, 255), (255, 223, 186), (202, 231, 200), (245, 203, 255)]
        
        if vibrant_colors:
            blur_color = vibrant_colors[0]
        else:
            blur_color = random.choice(soft_colors)
        
        base_color_for_badge = blur_color
        gradient_color = get_poster_primary_color(first_image_path)

        if is_blur:
          colored_bg_img = create_blur_background(first_image_path, template_width, template_height, blur_color, blur_size, color_ratio)
        else:
          colored_bg_img = create_gradient_background(template_width, template_height, gradient_color)

        supported_formats = (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp")
        custom_order = "315426987"
        order_map = {num: index for index, num in enumerate(custom_order)}
        poster_files = sorted([os.path.join(poster_folder, f) for f in os.listdir(poster_folder) if os.path.isfile(os.path.join(poster_folder, f)) and f.lower().endswith(supported_formats) and os.path.splitext(f)[0] in order_map], key=lambda x: order_map[os.path.splitext(os.path.basename(x))[0]])

        if not poster_files: return False
        poster_files = poster_files[:rows * cols]
        cell_width, cell_height = POSTER_GEN_CONFIG["CELL_WIDTH"], POSTER_GEN_CONFIG["CELL_HEIGHT"]
        grouped_posters = [poster_files[i : i + rows] for i in range(0, len(poster_files), rows)]

        result = colored_bg_img.copy()
        for col_index, column_posters in enumerate(grouped_posters):
            if col_index >= cols: break
            column_x = start_x + col_index * column_spacing
            column_height = rows * cell_height + (rows - 1) * margin
            shadow_extra = 40
            column_image = Image.new("RGBA", (cell_width + shadow_extra, column_height + shadow_extra), (0, 0, 0, 0))
            for row_index, poster_path in enumerate(column_posters):
                try:
                    poster = ImageOps.fit(Image.open(poster_path), (cell_width, cell_height), method=Image.LANCZOS)
                    if corner_radius > 0:
                        mask = Image.new("L", (cell_width, cell_height), 0)
                        ImageDraw.Draw(mask).rounded_rectangle([(0, 0), (cell_width, cell_height)], radius=corner_radius, fill=255)
                        poster_with_corners = Image.new("RGBA", poster.size, (0, 0, 0, 0))
                        poster_with_corners.paste(poster, (0, 0), mask)
                        poster = poster_with_corners
                    poster_with_shadow = add_shadow(poster, offset=(20, 20), shadow_color=(0, 0, 0, 216), blur_radius=20)
                    y_position = row_index * (cell_height + margin)
                    column_image.paste(poster_with_shadow, (0, y_position), poster_with_shadow)
                except Exception: continue
            
            rotation_canvas_size = int(math.sqrt((cell_width + shadow_extra) ** 2 + (column_height + shadow_extra) ** 2) * 1.5)
            rotation_canvas = Image.new("RGBA", (rotation_canvas_size, rotation_canvas_size), (0, 0, 0, 0))
            paste_x = (rotation_canvas_size - column_image.width) // 2
            paste_y = (rotation_canvas_size - column_image.height) // 2
            rotation_canvas.paste(column_image, (paste_x, paste_y), column_image)
            rotated_column = rotation_canvas.rotate(rotation_angle, Image.BICUBIC, expand=True)
            
            column_center_y = start_y + column_height // 2
            column_center_x = column_x
            if col_index == 1: column_center_x += cell_width - 50
            elif col_index == 2:
                column_center_y += -155
                column_center_x += (cell_width) * 2 - 40
            
            final_x = column_center_x - rotated_column.width // 2
            final_y = column_center_y - rotated_column.height // 2
            result.paste(rotated_column, (final_x, final_y), rotated_column)

        random_color = get_random_color(poster_files[0]) if poster_files else (random.randint(50, 200), random.randint(50, 200), random.randint(50, 200), 255)
        
        # ==================== 可微调旋鈕 (文字阴影) ====================
        # `darken_color` 的第二个参数 (0.0 - 1.0) 控制标题文字阴影的深浅。值越小，阴影越深。
        # 推荐范围: 0.7 - 0.9
        text_shadow_color = darken_color(blur_color, 0.8)
        # ===========================================================
        result = draw_text_on_image(result, title_zh, (73.32, 427.34), zh_font_path, "ch.ttf", int(163 * float(zh_font_size_ratio)), shadow=is_blur, shadow_color=text_shadow_color)

        if title_en:
            base_font_size = 50 * float(en_font_size_ratio)
            line_spacing = base_font_size * 0.1
            words = title_en.split()
            word_count = len(words)
            max_chars_per_line = max(len(word) for word in words) if words else 0
            font_size = base_font_size * (10 / max(max_chars_per_line, word_count * 3)) ** 0.8 if max_chars_per_line > 10 or word_count > 3 else base_font_size
            font_size = max(font_size, 30)
            result, line_count = draw_multiline_text_on_image(result, title_en, (124.68, 624.55), en_font_path, "en.otf", int(font_size), line_spacing, shadow=is_blur, shadow_color=text_shadow_color)
            color_block_height = base_font_size + line_spacing + (line_count - 1) * (int(font_size) + line_spacing)
            result = draw_color_block(result, (84.38, 620.06), (21.51, color_block_height), random_color)

        if config and config.get("show_item_count", False) and item_count is not None:
            result = result.convert('RGBA')
            result = draw_badge(
                image=result, item_count=item_count, font_path=zh_font_path,
                style=config.get('badge_style', 'badge'),
                size_ratio=config.get('badge_size_ratio', 0.12),
                base_color=base_color_for_badge
            )

        return image_to_base64(result)

    except Exception as e:
        logger.error(f"创建多图封面时出错: {e}", exc_info=True)
        return False