# scheduler_manager.py

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
import pytz
from datetime import datetime
from croniter import croniter

# 导入我们的任务链执行器和任务注册表
from tasks import task_run_chain
import config_manager # 导入配置管理器以读取配置
import constants      # 导入常量以获取时区
import extensions     # 导入 extensions 以获取共享的处理器实例
import task_manager   # 导入 task_manager 以提交任务

logger = logging.getLogger(__name__)

# 为我们的任务链定义一个独一无二、固定不变的ID
CHAIN_JOB_ID = 'automated_task_chain_job'
# ★★★ 新增：为复活检查任务定义一个固定的ID ★★★
REVIVAL_CHECK_JOB_ID = 'scheduled_revival_check'


# --- 友好的CRON日志翻译函数】 ---
def _get_next_run_time_str(cron_expression: str) -> str:
    """
    【V3 - 口齿伶俐版】将 CRON 表达式转换为人类可读的、干净的执行计划字符串。
    """
    try:
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("CRON 表达式必须有5个部分")

        minute, hour, day_of_month, month, day_of_week = parts

        # --- 周期描述 ---
        if minute.startswith('*/') and all(p == '*' for p in [hour, day_of_month, month, day_of_week]):
            return f"每隔 {minute[2:]} 分钟"
        
        if hour.startswith('*/') and all(p == '*' for p in [day_of_month, month, day_of_week]):
            if minute == '0':
                return f"每隔 {hour[2:]} 小时的整点"
            else:
                return f"每隔 {hour[2:]} 小时的第 {minute} 分钟"

        # --- 时间点描述 ---
        time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
        
        if day_of_week != '*':
            day_map = {
                '0': '周日', '1': '周一', '2': '周二', '3': '周三', 
                '4': '周四', '5': '周五', '6': '周六', '7': '周日',
                'sun': '周日', 'mon': '周一', 'tue': '周二', 'wed': '周三',
                'thu': '周四', 'fri': '周五', 'sat': '周六'
            }
            days = [day_map.get(d.lower(), d) for d in day_of_week.split(',')]
            return f"每周的 {','.join(days)} {time_str}"
        
        if day_of_month != '*':
            if day_of_month.startswith('*/'):
                 return f"每隔 {day_of_month[2:]} 天的 {time_str}"
            else:
                 return f"每月的 {day_of_month} 号 {time_str}"

        return f"每天 {time_str}"

    except Exception as e:
        logger.warning(f"无法智能解析CRON表达式 '{cron_expression}': {e}，回退到简单模式。")
        try:
            tz = pytz.timezone(constants.TIMEZONE)
            now = datetime.now(tz)
            iterator = croniter(cron_expression, now)
            next_run = iterator.get_next(datetime)
            return f"下一次将在 {next_run.strftime('%Y-%m-%d %H:%M')}"
        except:
            return f"按计划 '{cron_expression}'"

class SchedulerManager:
    def __init__(self):
        # 从 web_app.py 迁移过来的调度器实例
        self.scheduler = BackgroundScheduler(
            timezone=str(pytz.timezone(constants.TIMEZONE)),
            job_defaults={'misfire_grace_time': 60*5}
        )
        # 获取共享的处理器实例
        self.processor = extensions.media_processor_instance

    def start(self):
        """启动调度器并加载所有任务。"""
        if self.scheduler.running:
            logger.info("定时任务调度器已在运行。")
            return
        try:
            self.scheduler.start()
            logger.info("定时任务调度器已启动。")
            # 在启动时，加载所有需要的任务
            self.update_task_chain_job()
            # ★★★ 新增：调用设置独立任务的方法 ★★★
            self._setup_standalone_jobs()
        except Exception as e:
            logger.error(f"启动定时任务调度器失败: {e}", exc_info=True)

    def shutdown(self):
        """安全地关闭调度器。"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("定时任务调度器已关闭。")

    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    # ★★★ 新增：一个专门用于设置独立、硬编码任务的方法 ★★★
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    def _setup_standalone_jobs(self):
        """设置不通过UI配置的、固定的后台任务。"""
        logger.info("正在设置固定的后台计划任务...")

        # --- 任务1: 每周剧集复活检查 ---
        try:
            # 定义一个包装函数，以便安全地调用需要参数的任务
            def revival_check_wrapper():
                logger.info("定时任务触发：已完结剧集复活检查。")
                watchlist_proc = extensions.watchlist_processor_instance
                if watchlist_proc:
                    # 对于后台任务，我们不需要真实的进度回调，所以传递一个空的lambda
                    dummy_callback = lambda progress, message: None
                    watchlist_proc.run_revival_check_task(progress_callback=dummy_callback)
                else:
                    logger.error("无法执行复活检查：WatchlistProcessor 实例未初始化。")

            self.scheduler.add_job(
                func=revival_check_wrapper,
                trigger='cron',
                day_of_week='sun',  # 'sun' 代表周日
                hour=5,             # 5点
                minute=0,           # 0分
                id=REVIVAL_CHECK_JOB_ID,
                name='每周剧集复活检查',
                replace_existing=True,
                misfire_grace_time=3600, # 如果错过了，1小时内仍会尝试执行
                coalesce=True            # 如果错过了多次，只执行一次
            )
            logger.info("✅ 已成功调度【每周剧集复活检查】任务，将在每周日凌晨5点执行。")

        except Exception as e:
            logger.error(f"设置【每周剧集复活检查】任务时失败: {e}", exc_info=True)

    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
    # ★★★ 新增方法结束 ★★★
    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

    def update_task_chain_job(self):
        """
        【核心函数】根据当前配置文件，更新任务链的定时作业。
        这个函数应该在程序启动和每次配置保存后被调用。
        """
        if not self.scheduler.running:
            logger.warning("调度器未运行，无法更新任务。")
            return

        logger.info("正在根据最新配置更新自动化任务链...")

        try:
            # 1. 无论如何，先尝试移除旧的作业，防止重复或配置残留
            self.scheduler.remove_job(CHAIN_JOB_ID)
            logger.debug(f"已成功移除旧的任务链作业 (ID: {CHAIN_JOB_ID})。")
        except JobLookupError:
            logger.debug(f"没有找到旧的任务链作业 (ID: {CHAIN_JOB_ID})，无需移除。")
        except Exception as e:
            logger.error(f"尝试移除旧任务作业时发生意外错误: {e}", exc_info=True)

        # 2. 读取最新的配置
        config = config_manager.APP_CONFIG
        is_enabled = config.get('task_chain_enabled', False)
        cron_str = config.get('task_chain_cron')
        task_sequence = config.get('task_chain_sequence', [])

        # 3. 如果启用且配置有效，则添加新的作业
        if is_enabled and cron_str and task_sequence:
            try:
                # ★★★ 核心：我们不再直接调用 task_run_chain，而是通过 task_manager 提交 ★★★
                # 这样做可以享受到任务锁、状态更新等所有 task_manager 的好处。
                def scheduled_chain_task_wrapper():
                    logger.info(f"定时任务触发：自动化任务链。")
                    # 注意：这里我们传递 task_sequence 作为参数
                    task_manager.submit_task(
                        task_run_chain,
                        "自动化任务链",
                        task_sequence=task_sequence
                    )

                self.scheduler.add_job(
                    func=scheduled_chain_task_wrapper, # 调用包装函数
                    trigger=CronTrigger.from_crontab(cron_str, timezone=str(pytz.timezone(constants.TIMEZONE))),
                    id=CHAIN_JOB_ID,
                    name="自动化任务链",
                    replace_existing=True
                )
                # 调用辅助函数来生成友好的日志
                friendly_cron_str = _get_next_run_time_str(cron_str)
                logger.info(f"✅ 已成功设置自动化任务链，执行计划: {friendly_cron_str}，包含 {len(task_sequence)} 个任务。")
            except ValueError as e:
                logger.error(f"设置任务链失败：CRON表达式 '{cron_str}' 无效。错误: {e}")
            except Exception as e:
                logger.error(f"添加新的任务链作业时发生未知错误: {e}", exc_info=True)
        else:
            logger.info("自动化任务链未启用或配置不完整，本次不设置定时任务。")

# 创建一个全局单例，方便在其他地方调用
scheduler_manager = SchedulerManager()