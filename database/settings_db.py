# database/settings_db.py
import psycopg2
import logging
import json
import pytz
from typing import Optional, Any, Dict
from datetime import datetime

from .connection import get_db_connection
import config_manager
import constants

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 配置数据访问
# ======================================================================

def get_setting(setting_key: str) -> Optional[Any]:
    """从 app_settings 表中获取一个设置项的值。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (setting_key,))
            row = cursor.fetchone()
            return row['value_json'] if row else None
    except Exception as e:
        logger.error(f"DB: 获取设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise

def _save_setting_with_cursor(cursor, setting_key: str, value: Dict[str, Any]):
    """【内部函数】使用一个已有的数据库游标来保存设置。"""
    
    sql = """
        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (setting_key) DO UPDATE SET
            value_json = EXCLUDED.value_json,
            last_updated_at = NOW();
    """
    value_as_json = json.dumps(value, ensure_ascii=False)
    cursor.execute(sql, (setting_key, value_as_json))

def save_setting(setting_key: str, value: Dict[str, Any]):
    """【V2 - 重构版】向 app_settings 表中保存或更新一个设置项。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            _save_setting_with_cursor(cursor, setting_key, value)
            conn.commit()
            logger.info(f"  ➜ 成功保存设置 '{setting_key}'。")
    except Exception as e:
        logger.error(f"  ➜ 保存设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise

# --- 全局订阅配额管理器 ---
def get_subscription_quota() -> int:
    """【V3 - 终极健壮版】获取当前可用的订阅配额。"""
    
    try:
        current_max_quota = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP, 200)
        today_str = datetime.now(pytz.timezone(constants.TIMEZONE)).strftime('%Y-%m-%d')

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            state = get_setting('subscription_quota_state') or {}
            last_reset_date = state.get('last_reset_date')
            
            if last_reset_date != today_str:
                logger.info(f"检测到新的一天 ({today_str})，正在重置订阅配额为 {current_max_quota}。")
                new_state = {
                    'current_quota': current_max_quota,
                    'last_reset_date': today_str,
                    'max_quota_on_reset': current_max_quota
                }
                save_setting('subscription_quota_state', new_state)
                return current_max_quota
            else:
                max_quota_on_reset = state.get('max_quota_on_reset', -1)
                current_quota_in_db = state.get('current_quota', 0)
                effective_remaining_quota = 0
                
                if max_quota_on_reset != -1 and current_max_quota != max_quota_on_reset:
                    consumed = max_quota_on_reset - current_quota_in_db
                    effective_remaining_quota = max(0, current_max_quota - consumed)
                else:
                    effective_remaining_quota = current_quota_in_db
                    
                final_remaining_quota = min(effective_remaining_quota, current_max_quota)
                
                if final_remaining_quota != current_quota_in_db or max_quota_on_reset == -1:
                    logger.info(f"动态调整或修正了当日订阅配额。旧剩余: {current_quota_in_db}, 新剩余: {final_remaining_quota}, 当前上限: {current_max_quota}")
                    new_state = {
                        'current_quota': final_remaining_quota,
                        'last_reset_date': today_str,
                        'max_quota_on_reset': current_max_quota
                    }
                    save_setting('subscription_quota_state', new_state)
                
                return final_remaining_quota

    except Exception as e:
        logger.error(f"获取订阅配额时发生严重错误，将返回0以确保安全: {e}", exc_info=True)
        return 0

def decrement_subscription_quota() -> bool:
    """将当前订阅配额减一。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN;")
            try:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state' FOR UPDATE")
                row = cursor.fetchone()
                
                if not row or not row.get('value_json'):
                    conn.rollback()
                    logger.warning("尝试减少配额，但未找到配额状态记录。")
                    return False

                state = row['value_json']
                current_quota = state.get('current_quota', 0)

                if current_quota > 0:
                    state['current_quota'] = current_quota - 1
                    _save_setting_with_cursor(cursor, 'subscription_quota_state', state)
                    logger.debug(f"  ➜ 配额已消耗，剩余: {state['current_quota']}")
                
                conn.commit()
                return True
            except Exception as e_trans:
                conn.rollback()
                logger.error(f"减少配额的数据库事务失败: {e_trans}", exc_info=True)
                return False
    except Exception as e:
        logger.error(f"减少订阅配额时发生严重错误: {e}", exc_info=True)
        return False
    
def remove_item_from_recommendation_pool(tmdb_id: str):
    """
    从 'recommendation_pool' 列表中移除一个指定的媒体项。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = 'recommendation_pool' FOR UPDATE")
                result = cursor.fetchone()
                
                if not result or not result['value_json']:
                    logger.info("推荐池为空或不存在，无需移除。")
                    return

                current_pool = result['value_json']
                new_pool = [item for item in current_pool if str(item.get('id')) != str(tmdb_id)]

                if len(new_pool) == len(current_pool):
                    logger.warning(f"尝试从推荐池移除 TMDB ID {tmdb_id}，但未在池中找到。")
                    return

                new_pool_json = json.dumps(new_pool, ensure_ascii=False)
                cursor.execute("""
                    UPDATE app_settings SET value_json = %s, last_updated_at = NOW()
                    WHERE setting_key = 'recommendation_pool'
                """, (new_pool_json,))
                
                
                logger.info(f"  ✅ 已成功从推荐池中移除 TMDB ID: {tmdb_id}。")

    except Exception as e:
        logger.error(f"从推荐池移除 TMDB ID {tmdb_id} 时失败: {e}", exc_info=True)