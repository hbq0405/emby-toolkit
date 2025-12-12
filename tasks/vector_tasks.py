# tasks/vector_tasks.py
import logging
import time
import json
from database import connection, media_db
from ai_translator import AITranslator
import config_manager
import task_manager

logger = logging.getLogger(__name__)

def task_generate_embeddings(processor):
    """
    后台任务：为库中缺少向量的媒体生成 Embedding。
    """
    task_name = "生成媒体向量 (Embedding)"
    logger.info(f"--- 开始执行 '{task_name}' ---")
    
    try:
        # 1. 初始化 AI
        translator = AITranslator(config_manager.APP_CONFIG)
        
        # 2. 获取需要处理的项目 (简介不为空，且向量为空)
        items_to_process = []
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tmdb_id, item_type, overview 
                FROM media_metadata 
                WHERE overview IS NOT NULL 
                  AND overview != '' 
                  AND overview_embedding IS NULL
                LIMIT 500 -- 每次处理 500 个，避免任务跑太久
            """)
            items_to_process = cursor.fetchall()
            
        total = len(items_to_process)
        if total == 0:
            task_manager.update_status_from_thread(100, "没有需要生成向量的项目。")
            return

        logger.info(f"  ➜ 发现 {total} 个项目需要生成向量...")
        
        processed_count = 0
        
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
                processed_count += 1
            
            # 进度更新
            progress = int((i / total) * 100)
            task_manager.update_status_from_thread(progress, f"正在生成向量: {i+1}/{total}")
            
            # 避免 API 速率限制，稍微睡一下
            time.sleep(0.2)

        task_manager.update_status_from_thread(100, f"向量生成完成，本次处理 {processed_count} 个。")

    except Exception as e:
        logger.error(f"任务 '{task_name}' 失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")