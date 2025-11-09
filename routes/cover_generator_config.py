# routes/cover_generator_config.py

import logging
from flask import Blueprint, request, jsonify

from extensions import media_processor_instance
import config_manager # 导入以获取数据路径
from extensions import admin_required
from tasks import get_task_registry
import handler.emby as emby
from services.cover_generator.styles.badge_drawer import draw_badge
from services.cover_generator import CoverGeneratorService 
from database import settings_db

logger = logging.getLogger(__name__)

# 创建一个新的蓝图
cover_generator_config_bp = Blueprint('cover_generator_config', __name__, url_prefix='/api/config/cover_generator')

def get_default_config():
    """返回一份与新UI匹配的、精简后的默认配置"""
    return {
        # 基础设置
        "enabled": False,
        "transfer_monitor": True,
        "exclude_libraries": [], # 现在是勾选框
        "sort_by": "Latest", # 默认改为最新添加

        # 媒体数量开关
        "show_item_count": False, # 默认为关闭
        "badge_style": "badge",
        "badge_size_ratio": 0.12,
        
        # 封面风格
        "cover_style": "single_1",
        "tab": "style-tab",

        # 封面标题
        "title_config": "# 配置封面标题（按媒体库名称对应）\n# 格式如下：\n#\n# 媒体库名称:\n#   - 中文标题\n#   - 英文标题\n#\n",

        # 单图风格设置
        "zh_font_path_local": "", "en_font_path_local": "",
        "zh_font_url": "", "en_font_url": "",
        "zh_font_size": 1.0, "en_font_size": 1.0,
        "blur_size": 50, "color_ratio": 0.8,
        "single_use_primary": False,

        # 多图风格1设置
        "zh_font_path_multi_1_local": "", "en_font_path_multi_1_local": "",
        "zh_font_url_multi_1": "", "en_font_url_multi_1": "",
        "zh_font_size_multi_1": 1.0, "en_font_size_multi_1": 1.0,
        "blur_size_multi_1": 50, "color_ratio_multi_1": 0.8,
        "multi_1_blur": False, "multi_1_use_main_font": False,
        "multi_1_use_primary": True,
    }

# --- 获取封面生成器的配置 ---
@cover_generator_config_bp.route('', methods=['GET'])
@admin_required
def get_cover_generator_config():
    """获取封面生成器的配置"""
    try:
        # ★★★ 核心修改 3：从数据库读取配置 ★★★
        config = settings_db.get_setting('cover_generator_config')
        
        if config:
            # 如果数据库中有配置，为了确保未来新增的配置项也能显示，与默认值合并
            default_config = get_default_config()
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
            return jsonify(config)
        else:
            # 如果数据库中没有，说明是第一次，直接返回默认配置
            return jsonify(get_default_config())
            
    except Exception as e:
        logger.error(f"读取封面生成器配置失败: {e}", exc_info=True)
        return jsonify({"error": "读取配置失败"}), 500

# --- 保存封面生成器的配置 ---
@cover_generator_config_bp.route('', methods=['POST'])
@admin_required
def save_cover_generator_config():
    """保存封面生成器的配置"""
    try:
        new_config = request.json
        # ★★★ 核心修改 4：将配置保存到数据库 ★★★
        settings_db.save_setting('cover_generator_config', new_config)
        
        logger.info("封面生成器配置已保存到数据库。")
        return jsonify({"message": "配置已成功保存！"})
    except Exception as e:
        logger.error(f"保存封面生成器配置失败: {e}", exc_info=True)
        return jsonify({"error": "保存配置失败"}), 500
    
# --- 获取媒体库列表 ---
@cover_generator_config_bp.route('/libraries', methods=['GET'])
@admin_required
def get_all_libraries():
    """
    【V3 - 参数修正版】获取所有媒体库和合集，用于UI勾选。
    """
    try:
        # ★★★ 核心修复：使用正确的参数名来调用函数 ★★★
        full_libraries_list = emby.get_emby_libraries(
            emby_server_url=config_manager.APP_CONFIG.get('emby_server_url'), 
            emby_api_key=config_manager.APP_CONFIG.get('emby_api_key'),       
            user_id=config_manager.APP_CONFIG.get('emby_user_id')
        )
        
        if full_libraries_list:
            formatted_libs = [
                {
                    'label': lib.get('Name'), 
                    'value': lib.get('Id')
                } 
                for lib in full_libraries_list
                if lib.get('Name') and lib.get('Id')
            ]
            return jsonify(formatted_libs)
            
        return jsonify([])

    except Exception as e:
        logger.error(f"为封面生成器获取媒体库列表失败: {e}", exc_info=True)
        return jsonify({"error": "获取媒体库列表失败"}), 500

# --- 创建实时预览 ---
@cover_generator_config_bp.route('/preview', methods=['POST'])
@admin_required
def api_generate_cover_preview():
    """根据传入的设置，实时生成带徽章的预览图"""
    try:
        data = request.json
        base_image_b64 = data.get('base_image')
        item_count = data.get('item_count', 99) # 使用一个示例数字
        badge_style = data.get('badge_style', 'badge')
        badge_size_ratio = data.get('badge_size_ratio', 0.12)
        
        if not base_image_b64:
            return jsonify({"error": "缺少基础图片数据"}), 400

        # --- 准备绘图所需资源 ---
        # 1. 解码基础图片
        from PIL import Image
        import base64
        from io import BytesIO

        # 移除 base64 头部 (e.g., "data:image/png;base64,")
        if ',' in base_image_b64:
            header, encoded = base_image_b64.split(',', 1)
            image_data = base64.b64decode(encoded)
        else:
            image_data = base64.b64decode(base_image_b64)
        
        image = Image.open(BytesIO(image_data)).convert("RGBA")

        # 2. 获取字体路径 (我们需要实例化一个临时的 CoverGeneratorService 来获取)
        # 这里的 config 可以是默认的，因为它只影响字体路径
        temp_config = get_default_config() 
        cover_service = CoverGeneratorService(config=temp_config)
        cover_service._CoverGeneratorService__get_fonts() # 调用私有方法来加载字体路径
        font_path = str(cover_service.en_font_path)

        # 3. 调用核心绘图函数
        image_with_badge = draw_badge(
            image=image,
            item_count=item_count,
            font_path=font_path,
            style=badge_style,
            size_ratio=badge_size_ratio
        )

        # 4. 将结果编码回 base64
        buffered = BytesIO()
        image_with_badge.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')

        return jsonify({"image": "data:image/png;base64," + img_str})

    except Exception as e:
        logger.error(f"生成封面预览时出错: {e}", exc_info=True)
        return jsonify({"error": "生成预览失败"}), 500
