# tasks/vector_tasks.py
import logging
import time
import json
from database import connection
from ai_translator import AITranslator
import config_manager
import task_manager

logger = logging.getLogger(__name__)

def task_generate_embeddings(processor):
    """
    后台任务：为库中缺少向量的媒体生成 Embedding (自动循环直到完成)。
    条件：in_library = TRUE 且 item_type 为 Movie/Series
    """
    task_name = "生成媒体向量 (Embedding)"
    logger.info(f"--- 开始执行 '{task_name}' ---")
    
    try:
        # 1. 初始化 AI (使用全局配置)
        translator = AITranslator(config_manager.APP_CONFIG)
        
        BATCH_SIZE = 50  # 每批处理 50 个
        total_processed_count = 0 # 本次任务累计处理数
        
        # 2. 预先统计需要处理的总数，用于计算进度
        total_to_process = 0
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM media_metadata 
                    WHERE overview IS NOT NULL 
                      AND overview != '' 
                      AND overview_embedding IS NULL
                      AND item_type IN ('Movie', 'Series')
                      AND in_library = TRUE
                """)
                total_to_process = cursor.fetchone()['count']

        if total_to_process == 0:
            msg = "所有在库媒体均已拥有向量，无需处理。"
            task_manager.update_status_from_thread(100, msg)
            logger.info(f"--- {msg} ---")
            return

        logger.info(f"共发现 {total_to_process} 个媒体需要生成向量。")
        task_manager.update_status_from_thread(0, f"准备开始，共 {total_to_process} 个任务...")

        # 3. 循环处理
        while True:
            # 检查是否停止任务
            if processor.is_stop_requested(): 
                logger.info("任务已手动停止。")
                break

            # 获取需要处理的项目 (分批拉取)
            items_to_process = []
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT tmdb_id, item_type, overview 
                    FROM media_metadata 
                    WHERE overview IS NOT NULL 
                      AND overview != '' 
                      AND overview_embedding IS NULL
                      AND item_type IN ('Movie', 'Series')
                      AND in_library = TRUE
                    LIMIT {BATCH_SIZE}
                """)
                items_to_process = cursor.fetchall()
            
            # 如果取不到数据了，说明全部跑完了
            if not items_to_process:
                break

            logger.info(f"  ➜ 本批次获取 {len(items_to_process)} 个项目，开始生成向量...")
            
            # 处理当前批次
            for i, item in enumerate(items_to_process):
                if processor.is_stop_requested(): break
                
                tmdb_id = item['tmdb_id']
                overview = item['overview']
                
                # 调用 AI 生成向量
                embedding = translator.generate_embedding(overview)
                
                if embedding:
                    # 存入数据库
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                UPDATE media_metadata 
                                SET overview_embedding = %s::jsonb 
                                WHERE tmdb_id = %s
                            """, (json.dumps(embedding), tmdb_id))
                        conn.commit()
                    total_processed_count += 1
                else:
                    # 如果生成失败，记录日志，但为了防止死循环，建议标记或暂时跳过
                    # 这里简单处理：仅记录日志，下次循环可能还会取到它（如果一直失败可能会卡住，建议后续增加 failed_count 字段）
                    logger.warning(f"  -> 项目 {tmdb_id} 向量生成失败。")
                
                # 计算并更新进度条
                # 进度 = (已处理 / 总需处理) * 100
                # 注意：total_processed_count 是本次运行处理的，total_to_process 是本次运行开始前统计的待处理总数
                if total_to_process > 0:
                    progress_percent = int((total_processed_count / total_to_process) * 100)
                    # 限制最大 99，直到完全结束
                    progress_percent = min(progress_percent, 99)
                    
                    task_manager.update_status_from_thread(
                        progress_percent, 
                        f"正在生成向量... ({total_processed_count}/{total_to_process})"
                    )

                # 稍微睡一下，避免 QPS 爆炸
                time.sleep(0.1)

        # 4. 任务结束
        final_msg = f"向量生成任务结束。本次共新增 {total_processed_count} 个向量。"
        task_manager.update_status_from_thread(100, final_msg)
        logger.info(f"--- {final_msg} ---")

    except Exception as e:
        logger.error(f"任务 '{task_name}' 失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")