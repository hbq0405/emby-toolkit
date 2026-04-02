# routes/tg_userbot.py
import logging
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
from handler.tg_userbot import TGUserBotManager

tg_userbot_bp = Blueprint('tg_userbot', __name__, url_prefix='/api/tg_userbot')
logger = logging.getLogger(__name__)

@tg_userbot_bp.route('/config', methods=['GET'])
@admin_required
def get_config():
    """获取独立的频道监听配置"""
    cfg = settings_db.get_setting('tg_userbot_config') or {}
    # 提供默认值
    default_cfg = {
        'enabled': False, 'api_id': '', 'api_hash': '', 
        'phone': '', 'password': '', 'channels': [], 'monitor_types': ['movie', 'tv']
    }
    default_cfg.update(cfg)
    return jsonify({"success": True, "data": default_cfg})

@tg_userbot_bp.route('/config', methods=['POST'])
@admin_required
def save_config():
    """保存独立的频道监听配置，并根据状态控制后台进程"""
    new_cfg = request.json
    settings_db.save_setting('tg_userbot_config', new_cfg)
    
    manager = TGUserBotManager.get_instance()
    if new_cfg.get('enabled'):
        manager.start() # 如果启用了，立刻启动/重启
    else:
        manager.stop()  # 如果关闭了，立刻停止
        
    return jsonify({"success": True, "message": "频道订阅监听配置已保存生效"})

@tg_userbot_bp.route('/status', methods=['GET'])
@admin_required
def tg_userbot_status():
    manager = TGUserBotManager.get_instance()
    return jsonify({"success": True, "data": manager.get_status()})

@tg_userbot_bp.route('/send_code', methods=['POST'])
@admin_required
def tg_userbot_send_code():
    try:
        TGUserBotManager.get_instance().send_login_code()
        return jsonify({"success": True, "message": "验证码已发送到您的 Telegram 客户端"})
    except Exception as e:
        logger.error(f"发送验证码失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"发送失败: {str(e)}"})

@tg_userbot_bp.route('/login', methods=['POST'])
@admin_required
def tg_userbot_login():
    code = request.json.get('code')
    if not code: return jsonify({"success": False, "message": "请输入验证码"})
    try:
        res = TGUserBotManager.get_instance().submit_login_code(code)
        if res.get('success'):
            return jsonify({"success": True, "message": "登录成功！监听服务已启动。"})
        else:
            return jsonify({"success": False, "message": res.get('msg', '登录失败')})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@tg_userbot_bp.route('/logout', methods=['POST'])
@admin_required
def tg_userbot_logout():
    TGUserBotManager.get_instance().logout()
    return jsonify({"success": True, "message": "已注销并清除凭证"})