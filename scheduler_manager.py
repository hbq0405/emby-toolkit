# scheduler_manager.py

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
import pytz
from datetime import datetime
from croniter import croniter
import re

# 导入我们的任务链执行器和任务注册表
import tasks
import config_manager # 导入配置管理器以读取配置
import constants      # 导入常量以获取时区
import extensions     # 导入 extensions 以获取共享的处理器实例
import task_manager   # 导入 task_manager 以提交任务

logger = logging.getLogger(__name__)

# --- 【V10 - 任务ID拆分】 ---
# 为每个独立的定时任务定义清晰的ID
HIGH_FREQ_CHAIN_JOB_ID = 'high_freq_task_chain_job'
LOW_FREQ_CHAIN_JOB_ID = 'low_freq_task_chain_job'
REVIVAL_CHECK_JOB_ID = 'weekly_revival_check_job'


# --- 友好的CRON日志翻译函数 (保持不变) ---
def _get_next_run_time_str(cron_expression: str) -> str:
    """
    【V11 - 幼儿能懂版】将 CRON 表达式转换为人类可读的、极其友好的执行计划字符串。
    能够处理范围、列表、步长等复杂组合，并生成流畅的自然语言描述。
    """
    try:
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("CRON 表达式必须有5个部分")

        minute, hour, day_of_month, month, day_of_week = parts

        # --- 辅助函数：解析单个时间字段 ---
        def parse_part(part: str, unit: str, labels: dict = None) -> str:
            if part == '*':
                return ""  # '*' 表示“每个”，在组合时处理

            if part.isalnum() and labels:
                return f"在{labels.get(part.lower(), part)}{unit}"

            if ',' in part:
                items = [labels.get(p.lower(), p) for p in part.split(',')]
                return f"在 {','.join(items)} {unit}"

            # 核心改进：处理范围和步长
            match = re.match(r'(\d+)-(\d+)/(\d+)', part)
            if match:
                start, end, step = match.groups()
                return f"从{start}{unit}到{end}{unit}，每隔{step}{unit}"

            match = re.match(r'\*/(\d+)', part)
            if match:
                step = match.group(1)
                return f"每隔{step}{unit}"
            
            match = re.match(r'(\d+)-(\d+)', part)
            if match:
                start, end = match.groups()
                return f"从{start}{unit}到{end}{unit}"

            return f"在第{part}{unit}"

        # --- 辅助函数：解析星期字段 ---
        def parse_day_of_week(part: str) -> str:
            day_map = {
                '0': '周日', '1': '周一', '2': '周二', '3': '周三',
                '4': '周四', '5': '周五', '6': '周六', '7': '周日',
                'sun': '周日', 'mon': '周一', 'tue': '周二', 'wed': '周三',
                'thu': '周四', 'fri': '周五', 'sat': '周六'
            }
            if part == '*': return "每天"
            
            # 处理范围，例如 1-5 (周一到周五)
            match = re.match(r'(\w+)-(\w+)', part)
            if match:
                start = day_map.get(match.group(1).lower(), match.group(1))
                end = day_map.get(match.group(2).lower(), match.group(2))
                return f"每周从{start}到{end}"

            days = [day_map.get(d.lower(), d) for d in part.split(',')]
            return f"在每周的 {','.join(days)}"

        # --- 开始组合描述 ---
        time_desc = ""
        # 1. 解析小时
        hour_desc = parse_part(hour, "点")
        if not hour_desc: # hour == '*'
            hour_desc = "每小时"
        
        # 2. 解析分钟
        minute_desc = parse_part(minute, "分钟")
        if not minute_desc: # minute == '*'
            minute_desc = "每分钟"
        elif minute == '0':
            minute_desc = "的整点"
        else:
            minute_desc = f"的第{minute}分钟"

        # 组合时间和分钟
        if "每隔" in hour_desc and minute == '0':
             time_desc = hour_desc.replace("点", "小时") + "的整点"
        elif "每隔" in minute_desc:
             time_desc = f"{hour_desc}，{minute_desc}"
        else:
             time_desc = f"{hour_desc}{minute_desc}"


        # 3. 解析日期和星期
        day_desc = ""
        if day_of_month != '*' and day_of_week != '*':
            # 当同时指定了日期和星期时，cron的行为是“或”，这很难用一句话描述
            # 我们选择更常用的星期，并提示用户
            day_desc = f"{parse_day_of_week(day_of_week)} (注意：日期和星期同时设置，规则较复杂)"
        elif day_of_week != '*':
            day_desc = parse_day_of_week(day_of_week)
        elif day_of_month != '*':
            if day_of_month == '1':
                day_desc = "每月的第一天"
            else:
                day_desc = parse_part(day_of_month, "号").replace("在", "每月的")
        else:
            day_desc = "每天"

        # 最终拼接成一句话
        # 修正一些口语化表达
        final_str = f"{day_desc}的 {time_desc}".replace("每天的 ", "每天 ").replace("在 ", "")
        final_str = re.sub(r'点(\d+)', r'点\1分', final_str) # 避免 "5点30分钟" -> "5点30分"
        final_str = final_str.replace("分钟钟", "分钟")
        
        return final_str.strip()

    except Exception as e:
        logger.warning(f"无法智能解析CRON表达式 '{cron_expression}': {e}，回退到备用模式。")
        try:
            # 备用方案：使用 croniter 计算下一次执行时间
            tz = pytz.timezone(constants.TIMEZONE)
            now = datetime.now(tz)
            iterator = croniter(cron_expression, now)
            next_run = iterator.get_next(datetime)
            return f"下一次将在 {next_run.strftime('%Y-%m-%d %H:%M:%S')}"
        except:
            return f"按计划 '{cron_expression}'"


class SchedulerManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler(
            timezone=str(pytz.timezone(constants.TIMEZONE)),
            job_defaults={'misfire_grace_time': 60*5}
        )
        self.processor = extensions.media_processor_instance

    def start(self):
        """启动调度器并加载所有初始任务。"""
        if self.scheduler.running:
            logger.info("定时任务调度器已在运行。")
            return
        try:
            self.scheduler.start()
            logger.info("定时任务调度器已启动。")
            # 在启动时，根据当前配置更新所有任务
            self.update_all_scheduled_jobs()
        except Exception as e:
            logger.error(f"启动定时任务调度器失败: {e}", exc_info=True)

    def shutdown(self):
        """安全地关闭调度器。"""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("定时任务调度器已关闭。")

    def update_all_scheduled_jobs(self):
        """
        【V10 - 主更新函数】
        根据最新配置，更新所有类型的定时任务。
        这个函数应该在程序启动和每次配置保存后被调用。
        """
        if not self.scheduler.running:
            logger.warning("调度器未运行，无法更新任务。将尝试启动...")
            self.start()
            if not self.scheduler.running: return

        logger.info("正在根据最新配置更新所有定时任务...")
        self.update_high_freq_task_chain_job()
        self.update_low_freq_task_chain_job()
        self.update_revival_check_job()

    def _update_single_task_chain_job(self, job_id: str, job_name: str, task_key: str, enabled_key: str, cron_key: str, sequence_key: str, runtime_key: str):
        """
        【V10 - 内部通用任务链调度器】
        一个通用的函数，用于更新单个任务链（高频或低频）。
        """
        try:
            self.scheduler.remove_job(job_id)
            logger.debug(f"已成功移除旧的 '{job_name}' 作业 (ID: {job_id})。")
        except JobLookupError:
            logger.debug(f"没有找到旧的 '{job_name}' 作业 (ID: {job_id})，无需移除。")
        except Exception as e:
            logger.error(f"尝试移除旧的 '{job_name}' 作业时发生意外错误: {e}", exc_info=True)

        config = config_manager.APP_CONFIG
        is_enabled = config.get(enabled_key, False)
        cron_str = config.get(cron_key)
        task_sequence = config.get(sequence_key, [])

        if is_enabled and cron_str and task_sequence:
            registry = tasks.get_task_registry()
            task_info = registry.get(task_key)
            if not task_info:
                logger.error(f"设置 '{job_name}' 失败：在任务注册表中未找到任务键 '{task_key}'。")
                return
            
            task_function, _, processor_type = task_info

            def scheduled_chain_task_wrapper():
                logger.info(f"定时任务触发：{job_name}。")
                # 新的任务链函数会自己从配置中读取序列，无需再传递参数
                task_manager.submit_task(
                    task_function=task_function,
                    task_name=job_name,
                    processor_type=processor_type
                )

            try:
                self.scheduler.add_job(
                    func=scheduled_chain_task_wrapper,
                    trigger=CronTrigger.from_crontab(cron_str, timezone=str(pytz.timezone(constants.TIMEZONE))),
                    id=job_id,
                    name=job_name,
                    replace_existing=True
                )
                
                friendly_cron_str = _get_next_run_time_str(cron_str)
                chain_max_runtime_minutes = config.get(runtime_key, 0)
                log_message = (
                    f"已成功设置'{job_name}'，执行计划: {friendly_cron_str}，"
                    f"包含 {len(task_sequence)} 个任务。"
                )
                if chain_max_runtime_minutes > 0:
                    log_message += f" 最大运行时长: {chain_max_runtime_minutes} 分钟。"
                else:
                    log_message += " (无时长限制)。"
                
                logger.info(log_message)

            except ValueError as e:
                logger.error(f"设置 '{job_name}' 失败：CRON表达式 '{cron_str}' 无效。错误: {e}")
            except Exception as e:
                logger.error(f"添加新的 '{job_name}' 作业时发生未知错误: {e}", exc_info=True)
        else:
            logger.info(f"'{job_name}' 未启用或配置不完整，本次不设置定时任务。")

    def update_high_freq_task_chain_job(self):
        """更新高频核心任务链的定时作业。"""
        self._update_single_task_chain_job(
            job_id=HIGH_FREQ_CHAIN_JOB_ID,
            job_name="高频核心任务链",
            task_key='task-chain-high-freq',
            enabled_key='task_chain_enabled',
            cron_key='task_chain_cron',
            sequence_key='task_chain_sequence',
            runtime_key=constants.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES
        )

    def update_low_freq_task_chain_job(self):
        """更新低频维护任务链的定时作业。"""
        self._update_single_task_chain_job(
            job_id=LOW_FREQ_CHAIN_JOB_ID,
            job_name="低频维护任务链",
            task_key='task-chain-low-freq',
            enabled_key='task_chain_low_freq_enabled',
            cron_key='task_chain_low_freq_cron',
            sequence_key='task_chain_low_freq_sequence',
            runtime_key=constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_MAX_RUNTIME_MINUTES
        )

    def update_revival_check_job(self):
        """根据硬编码的规则，设置每周的剧集复活检查任务。"""
        if not self.scheduler.running:
            return

        logger.debug("正在更新固定的'剧集复活检查'定时任务...")

        try:
            self.scheduler.remove_job(REVIVAL_CHECK_JOB_ID)
        except JobLookupError:
            pass 

        cron_str = '0 5 * * sun' 
        registry = tasks.get_task_registry()
        task_info = registry.get('revival-check')
        
        if not task_info:
            logger.error("设置'剧集复活检查'任务失败：在任务注册表中未找到 'revival-check'。")
            return
            
        task_function, task_description, processor_type = task_info

        def scheduled_revival_check_wrapper():
            logger.info(f"定时任务触发：{task_description}。")
            task_manager.submit_task(
                task_function=task_function,
                task_name=task_description,
                processor_type=processor_type
            )

        try:
            self.scheduler.add_job(
                func=scheduled_revival_check_wrapper,
                trigger=CronTrigger.from_crontab(cron_str, timezone=str(pytz.timezone(constants.TIMEZONE))),
                id=REVIVAL_CHECK_JOB_ID,
                name=task_description,
                replace_existing=True
            )
            logger.info(f"已成功设置'{task_description}'任务，执行计划: 每周日 05:00。")
        except ValueError as e:
            logger.error(f"设置'{task_description}'任务失败：CRON表达式 '{cron_str}' 无效。错误: {e}")

# 创建一个全局单例，方便在其他地方调用
scheduler_manager = SchedulerManager()