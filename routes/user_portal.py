# routes/user_portal.py
import logging
import requests
import threading
from flask import Blueprint, jsonify, session, request
from datetime import datetime, timedelta

from extensions import emby_login_required 
from database import user_db, settings_db, media_db, request_db
import config_manager     
import constants
import handler.tmdb as tmdb
import handler.emby as emby
from handler.telegram import send_telegram_message
from routes.discover import check_and_replenish_pool
import task_manager
from tasks.subscriptions import task_manual_subscribe_batch

# 1. 创建一个新的蓝图
user_portal_bp = Blueprint('user_portal_bp', __name__, url_prefix='/api/portal')
logger = logging.getLogger(__name__)

@user_portal_bp.route('/subscribe', methods=['POST'])
@emby_login_required
def request_subscription():
    """
    【V9 - 终极统一版】
    - 普通用户的请求状态为 REQUESTED，VIP/管理员的请求状态为 WANTED。
    """
    data = request.json
    emby_user_id = session['emby_user_id']
    emby_username = session.get('emby_username', emby_user_id)
    
    is_emby_admin = session.get('emby_is_admin', False)
    is_vip = user_db.get_user_subscription_permission(emby_user_id)
    
    tmdb_id = str(data.get('tmdb_id'))
    item_type = data.get('item_type')
    item_name = data.get('item_name') # 仅作为备用

    message = ""
    new_status_for_frontend = None

    # ★★★ 核心修改：无论是谁，我们都需要先获取媒体的详细信息 ★★★
    config = config_manager.APP_CONFIG
    tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    details = None
    try:
        if item_type == 'Movie':
            details = tmdb.get_movie_details(int(tmdb_id), tmdb_api_key)
        elif item_type == 'Series':
            details = tmdb.get_tv_details(int(tmdb_id), tmdb_api_key)
        if not details:
            raise ValueError("无法从TMDb获取媒体详情")
    except Exception as e:
        logger.error(f"用户 {emby_username} 请求订阅时，获取TMDb详情失败 (ID: {tmdb_id}): {e}")
        return jsonify({"status": "error", "message": "无法获取媒体详情，请稍后再试。"}), 500

    media_info = {
        'tmdb_id': tmdb_id, 'item_type': item_type,
        'title': details.get('title') or details.get('name') or item_name,
        'original_title': details.get('original_title') or details.get('original_name'),
        'release_date': details.get('release_date') or details.get('first_air_date'),
        'poster_path': details.get('poster_path'), 'overview': None
    }

    if is_vip or is_emby_admin:
        log_user_type = "管理员" if is_emby_admin else "VIP 用户"
        
        # 发行日期检查
        is_released = True
        release_date_str = media_info.get('release_date')
        if release_date_str:
            try:
                from datetime import datetime, date
                release_date_obj = datetime.strptime(release_date_str, '%Y-%m-%d').date()
                if release_date_obj > date.today():
                    is_released = False
            except (ValueError, TypeError):
                logger.warning(f"无法解析媒体 {tmdb_id} 的发行日期 '{release_date_str}'，将按已发行处理。")

        if not is_released:
            logger.info(f"  ➜ 【{log_user_type}-待发行通道】'{emby_username}' 请求的项目尚未发行，状态将设置为 PENDING_RELEASE...")
            request_db.set_media_status_pending_release(
                tmdb_ids=[tmdb_id], item_type=item_type,
                source={"type": "user_request", "user_id": emby_user_id, "user_type": log_user_type},
                media_info_list=[media_info]
            )
            message = "该项目尚未发行，已为您加入待发行监控队列。"
            new_status_for_frontend = 'pending' # 前端统一显示为处理中
        else:
            logger.info(f"  ➜ 【{log_user_type}-待订阅通道】'{emby_username}' 的订阅请求将直接加入待订阅队列...")
            request_db.set_media_status_wanted(
                tmdb_ids=[tmdb_id], item_type=item_type,
                source={"type": "user_request", "user_id": emby_user_id, "user_type": log_user_type},
                media_info_list=[media_info]
            )
            message = "订阅请求已提交，系统将自动处理！"
            new_status_for_frontend = 'approved'

    else:
        # --- ★★★ 普通用户通道终极改造 ★★★ ---
        existing_status = request_db.get_global_request_status_by_tmdb_id(tmdb_id)
        if existing_status:
            message = "该项目正在等待审核。" if existing_status == 'pending' else "该项目已在订阅队列中。"
            return jsonify({"status": existing_status, "message": message}), 200
        
        request_db.set_media_status_requested(
            tmdb_ids=[tmdb_id], item_type=item_type,
            source={"type": "user_request", "user_id": emby_user_id},
            media_info_list=[media_info]
        )
        message = "“想看”请求已提交，请等待管理员审核。"
        new_status_for_frontend = 'pending'

        try:
            admin_chat_ids = user_db.get_admin_telegram_chat_ids()
            if admin_chat_ids:
                notification_text = (
                    f"🔔 *新的订阅审核请求*\n\n"
                    f"用户 *{emby_username}* 提交了想看请求：\n"
                    f"*{item_name}*\n\n"
                    f"请前往管理后台审核。"
                )
                for admin_id in admin_chat_ids:
                    send_telegram_message(admin_id, notification_text)
        except Exception as e:
            logger.error(f"  ➜ 发送管理员审核通知时出错: {e}", exc_info=True)

    # 1. 【核心】后端直接触发“订阅直通车”
    # 只有状态为 approved (即管理员/VIP且已上映) 时才立即触发
    if new_status_for_frontend == 'approved':
        logger.info(f"  ➜ [直通车] 为管理员/VIP '{emby_username}' 立即触发订阅任务: {item_name}")
        
        req_item = {
            'tmdb_id': tmdb_id,
            'item_type': item_type,
            'title': item_name,
            'user_id': emby_user_id,
            'season_number': data.get('season_number')
        }
        
        # 提交任务
        task_manager.submit_task(
            task_function=task_manual_subscribe_batch,
            task_name=f"立即订阅: {item_name}",
            processor_type='media',
            subscribe_requests=[req_item]
        )

    # 2. 推荐池处理
    if new_status_for_frontend in ['approved', 'pending'] and item_type == 'Movie':
        # 先移除
        settings_db.remove_item_from_recommendation_pool(tmdb_id)
        # 再异步补货 (发后即忘)
        threading.Thread(target=check_and_replenish_pool).start()

    try:
        user_chat_id = user_db.get_user_telegram_chat_id(emby_user_id)
        if user_chat_id and not (is_vip or is_emby_admin):
            message_text = f"🔔 *您的订阅请求已提交*\n\n您想看的 *{item_name}* 已进入待审队列，管理员处理后会通知您。"
            send_telegram_message(user_chat_id, message_text)
    except Exception as e:
        logger.error(f"发送订阅请求提交通知时出错: {e}")
        
    return jsonify({"status": new_status_for_frontend, "message": message})
    
# ★★★ 获取当前用户账户信息的接口 ★★★
@user_portal_bp.route('/account-info', methods=['GET'])
@emby_login_required # 必须登录才能访问
def get_account_info():
    """获取当前登录用户的详细账户信息，并附带全局配置信息。"""
    emby_user_id = session['emby_user_id']
    try:
        # 1. 照常获取用户的个人账户详情
        account_info = user_db.get_user_account_details(emby_user_id)
        
        # 2. ★★★ 核心修改：即使个人详情为空，也创建一个空字典 ★★★
        #    这样可以确保即使用户是新来的，也能看到全局频道信息。
        if not account_info:
            account_info = {}

        # 3. ★★★ 从全局配置中读取频道ID，并添加到返回的字典中 ★★★
        channel_id = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        account_info['telegram_channel_id'] = channel_id
            
        return jsonify(account_info)
    except Exception as e:
        logger.error(f"为用户 {emby_user_id} 获取账户信息时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取账户信息失败"}), 500
    
@user_portal_bp.route('/subscription-history', methods=['GET'])
@emby_login_required
def get_subscription_history():
    emby_user_id = session['emby_user_id']
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 10, type=int)
    # 新增获取 status 参数
    status_filter = request.args.get('status', 'all') 
    
    try:
        # 传递 status_filter 给数据库函数
        history, total_records = media_db.get_user_request_history(emby_user_id, page, page_size, status_filter)
        return jsonify({
            "items": history,
            "total_records": total_records,
            "page": page,
            "page_size": page_size
        })
    except Exception as e:
        logger.error(f"为用户 {emby_user_id} 获取订阅历史时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取订阅历史失败"}), 500
    
@user_portal_bp.route('/telegram-chat-id', methods=['POST'])
@emby_login_required
def save_telegram_chat_id():
    """保存当前用户的 Telegram Chat ID。"""
    data = request.json
    chat_id = data.get('chat_id', '').strip() # 获取并去除前后空格
    emby_user_id = session['emby_user_id']

    success = user_db.update_user_telegram_chat_id(emby_user_id, chat_id)
    if success:
        return jsonify({"status": "ok", "message": "Telegram Chat ID 保存成功！"})
    else:
        return jsonify({"status": "error", "message": "保存失败，请联系管理员"}), 500
    
@user_portal_bp.route('/telegram-bot-info', methods=['GET'])
@emby_login_required
def get_telegram_bot_info():
    """安全地获取 Telegram 机器人的用户名，并返回详细的错误信息。"""
    bot_token = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        return jsonify({"bot_username": None, "error": "Bot Token未配置"})

    try:
        api_url = f"https://api.telegram.org/bot{bot_token}/getMe"
        from config_manager import get_proxies_for_requests
        proxies = get_proxies_for_requests()
        
        # ★★★ 核心修改 1: 增加超时时间到20秒，给网络多一点机会 ★★★
        response = requests.get(api_url, timeout=20, proxies=proxies)
        
        if response.status_code == 200:
            bot_info = response.json()
            if bot_info.get("ok"):
                return jsonify({"bot_username": bot_info.get("result", {}).get("username")})
            else:
                # Token正确但API返回错误 (例如被吊销)
                error_desc = bot_info.get('description', '未知API错误')
                return jsonify({"bot_username": None, "error": f"Telegram API 错误: {error_desc}"})
        else:
            # HTTP请求失败
            return jsonify({"bot_username": None, "error": f"HTTP错误, 状态码: {response.status_code}"})

    except requests.RequestException as e:
        # ★★★ 核心修改 2: 捕获异常后，将错误信息返回给前端 ★★★
        logger.error(f"调用 Telegram getMe API 失败: {e}")
        # 将具体的网络错误（如超时）作为 error 字段返回
        return jsonify({"bot_username": None, "error": f"网络请求失败: {str(e)}"})

@user_portal_bp.route('/subscription-stats', methods=['GET'])
@emby_login_required
def get_subscription_stats():
    """获取当前用户的订阅统计数据"""
    emby_user_id = session['emby_user_id']
    stats = media_db.get_user_request_stats(emby_user_id)
    return jsonify(stats)

@user_portal_bp.route('/upload-avatar', methods=['POST'])
@emby_login_required
def upload_avatar():
    """上传用户头像"""
    if 'avatar' not in request.files:
        return jsonify({"status": "error", "message": "未找到文件"}), 400
    
    file = request.files['avatar']
    if file.filename == '':
        return jsonify({"status": "error", "message": "未选择文件"}), 400

    # 读取文件内容
    file_content = file.read()
    # 限制文件大小 (例如 5MB)
    if len(file_content) > 5 * 1024 * 1024:
        return jsonify({"status": "error", "message": "图片大小不能超过 5MB"}), 400

    emby_user_id = session['emby_user_id']
    
    # 1. 上传到 Emby
    # 注意：这里假设 config_manager 已正确配置
    success = emby.upload_user_image(
        config_manager.APP_CONFIG['emby_server_url'],
        config_manager.APP_CONFIG['emby_api_key'],
        emby_user_id,
        file_content,
        file.mimetype or 'image/jpeg'
    )

    if not success:
        return jsonify({"status": "error", "message": "上传到 Emby 服务器失败"}), 500

    # 2. 立即从 Emby 获取最新的 ImageTag (因为上传后 Tag 会变)
    user_info = emby.get_user_info_from_server(
        config_manager.APP_CONFIG['emby_server_url'],
        config_manager.APP_CONFIG['emby_api_key'],
        emby_user_id
    )
    
    new_tag = None
    if user_info:
        new_tag = user_info.get('PrimaryImageTag')
        # 3. 更新本地数据库
        user_db.update_user_image_tag(emby_user_id, new_tag)

    return jsonify({
        "status": "ok", 
        "message": "头像上传成功", 
        "new_tag": new_tag
    })

@user_portal_bp.route('/playback-report', methods=['GET'])
@emby_login_required
def get_playback_report():
    """
    获取播放统计报告 (个人流水 + 全局热榜)
    """
    emby_user_id = session['emby_user_id']
    days = request.args.get('days', 30, type=int)
    config = config_manager.APP_CONFIG
    
    # 1. 获取个人数据 (来自 Playback Reporting 插件)
    personal_res = emby.get_playback_reporting_data(
        config['emby_server_url'], config['emby_api_key'], emby_user_id, days
    )
    
    if "error" in personal_res:
        if personal_res["error"] == "plugin_not_installed":
            return jsonify({"status": "error", "message": "服务端未安装 Playback Reporting 插件"}), 404
        return jsonify({"status": "error", "message": "获取数据失败"}), 500
        
    raw_activity = personal_res.get("data", [])
    
    # --- 步骤 A: 统计总时长 (遍历所有数据) ---
    personal_stats = {
        "total_count": len(raw_activity),
        "total_minutes": 0,
        "history_list": [] 
    }
    
    for item in raw_activity:
        # PlayDuration 通常是秒
        duration_sec = item.get("PlayDuration") or item.get("Duration") or 0
        personal_stats["total_minutes"] += int(duration_sec / 60)

    # --- 步骤 B: 准备历史列表 (只处理前 20 条) ---
    # 我们只对这 20 条进行“回查”，避免性能问题
    top_20_items = raw_activity[:20]
    
    # 1. 收集需要回查详情的 Episode ID
    episode_ids_to_fetch = []
    for item in top_20_items:
        # 注意：插件返回的 item_id 可能是 int，需要转 str
        item_id = str(item.get("ItemId") or item.get("item_id"))
        item_type = item.get("ItemType") or item.get("item_type") or "Video"
        
        if item_type == 'Episode' and item_id:
            episode_ids_to_fetch.append(item_id)
            
    # 2. 批量回查 Emby 获取季号和集号 (只发起 1 次网络请求)
    episode_details_map = {}
    if episode_ids_to_fetch:
        try:
            # 调用 emby.py 中已有的批量获取函数
            # 请求字段: SeriesName(剧集名), ParentIndexNumber(季号), IndexNumber(集号)
            details_list = emby.get_emby_items_by_id(
                base_url=config['emby_server_url'],
                api_key=config['emby_api_key'],
                user_id=emby_user_id, # 这里传 user_id 也没关系，主要是为了查元数据
                item_ids=episode_ids_to_fetch,
                fields="SeriesName,ParentIndexNumber,IndexNumber,Name"
            )
            # 构建字典映射: ID -> Detail
            for d in details_list:
                episode_details_map[d['Id']] = d
        except Exception as e:
            logger.error(f"批量回查集数详情失败: {e}")

    # --- 步骤 C: 构建最终返回列表 ---
    for item in top_20_items:
        item_id = str(item.get("ItemId") or item.get("item_id"))
        item_type = item.get("ItemType") or item.get("item_type") or "Video"
        
        # 原始标题 (作为兜底)
        display_title = item.get("Name") or item.get("item_name") or "未知影片"
        
        # ★★★ 核心逻辑：如果是剧集，且回查到了详情，则重写标题 ★★★
        if item_type == 'Episode' and item_id in episode_details_map:
            detail = episode_details_map[item_id]
            series_name = detail.get('SeriesName')
            season_num = detail.get('ParentIndexNumber')
            episode_num = detail.get('IndexNumber')
            
            if series_name and season_num is not None and episode_num is not None:
                display_title = f"{series_name} - 第 {season_num} 季 - 第 {episode_num} 集"
            elif series_name:
                # 如果缺季号或集号，至少显示剧集名
                display_title = f"{series_name} - {detail.get('Name')}"

        # 日期处理
        date_str = item.get("DateCreated") or item.get("Date") or item.get("date")
        # 如果有 time 字段，拼接到 date 后面 (适配你提供的 JSON 格式)
        if item.get("time") and date_str and " " not in str(date_str):
             date_str = f"{date_str} {item.get('time')}"

        duration_sec = item.get("PlayDuration") or item.get("Duration") or item.get("duration") or 0
        
        personal_stats["history_list"].append({
            "title": display_title,
            "date": date_str,
            "duration": int(float(duration_sec) / 60),
            "item_type": item_type,
            "item_id": item_id
        })

    # 2. 获取全局热榜
    global_res = emby.get_global_popular_items(
        config['emby_server_url'], config['emby_api_key'], days
    )
    
    global_top_list = []
    if "data" in global_res:
        for item in global_res["data"]:
            global_top_list.append({
                "title": item.get("title"),
                "play_count": item.get("play_count", 0),
                # 关键：将秒转换为分钟，前端再除以 60 就会得到正确的小时数
                "total_duration": int(item.get("total_duration", 0) / 60), 
                "item_type": item.get("item_type"),
                "item_id": item.get("item_id")
            })

    return jsonify({
        "personal": personal_stats,
        "global_top": global_top_list
    })