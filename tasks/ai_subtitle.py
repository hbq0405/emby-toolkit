import json
import logging
import os
from typing import Any, Dict, List

import task_manager
from database.connection import get_db_connection
from handler.ai_subtitle_service import process_ai_subtitle_translation_for_emby_items

logger = logging.getLogger(__name__)


def task_retry_ai_subtitle_temporary_items(processor, limit: int = 0):
    """补救洗版等级为 X.1、但本地缺少 AI 中文字幕外挂的媒体项。"""
    task_manager.update_status_from_thread(0, "正在扫描 AI 字幕临时项...")
    rows = _load_temporary_subtitle_rows(limit)
    if not rows:
        logger.info("  ➜ [AI字幕补救] 未找到需要检查的临时字幕项。")
        task_manager.update_status_from_thread(100, "AI 字幕补救完成：没有需要检查的条目。")
        return

    candidates = []
    skipped_with_subtitle = 0
    skipped_no_path = 0

    for row in rows:
        assets = _safe_json(row.get("asset_details_json"), [])
        emby_ids = _safe_json(row.get("emby_item_ids_json"), [])
        if not isinstance(assets, list) or not isinstance(emby_ids, list):
            skipped_no_path += 1
            continue

        for emby_id in [str(x).strip() for x in emby_ids if str(x or "").strip()]:
            asset = _match_asset_for_emby_id(assets, emby_id)
            media_path = str((asset or {}).get("path") or "").strip()
            if not media_path or media_path.startswith(("http://", "https://")):
                skipped_no_path += 1
                continue

            subtitle_path = _zh_sidecar_path(media_path)
            if os.path.exists(subtitle_path):
                skipped_with_subtitle += 1
                continue

            candidates.append({
                "emby_id": emby_id,
                "title": row.get("title") or row.get("tmdb_id") or emby_id,
            })

    seen = set()
    unique_candidates = []
    for item in candidates:
        emby_id = item["emby_id"]
        if emby_id in seen:
            continue
        seen.add(emby_id)
        unique_candidates.append(item)

    total = len(unique_candidates)
    if total == 0:
        message = (
            f"AI 字幕补救完成：检查 {len(rows)} 条，"
            f"已有外挂字幕 {skipped_with_subtitle} 条，缺少可写入路径 {skipped_no_path} 条。"
        )
        logger.info("  ➜ [AI字幕补救] %s", message)
        task_manager.update_status_from_thread(100, message)
        return

    logger.info(
        "  ➜ [AI字幕补救] 找到 %s 个缺少 AI 中文外挂字幕的临时项，开始逐个重试。",
        total,
    )
    failed = 0
    retried = 0
    for index, item in enumerate(unique_candidates, start=1):
        if processor and getattr(processor, "is_stop_requested", lambda: False)():
            logger.info("  ➜ [AI字幕补救] 收到停止信号，已中止。")
            break

        progress = int(((index - 1) / total) * 95)
        title = item["title"]
        task_manager.update_status_from_thread(progress, f"({index}/{total}) 正在补救 AI 字幕：{title}")
        try:
            process_ai_subtitle_translation_for_emby_items([item["emby_id"]], item_name_for_log=title)
            retried += 1
        except Exception as e:
            failed += 1
            logger.warning("  ➜ [AI字幕补救] 重试失败，已跳过：%s，错误=%s", title, e, exc_info=True)

    final_message = (
        f"AI 字幕补救完成：检查 {len(rows)} 条，重试 {retried} 条，失败 {failed} 条，"
        f"已有外挂字幕 {skipped_with_subtitle} 条，缺少可写入路径 {skipped_no_path} 条。"
    )
    logger.info("  ➜ [AI字幕补救] %s", final_message)
    task_manager.update_status_from_thread(100, final_message)


def _load_temporary_subtitle_rows(limit: int) -> List[Dict[str, Any]]:
    params = []
    limit_sql = ""
    try:
        limit_value = int(limit or 0)
    except Exception:
        limit_value = 0
    if limit_value > 0:
        limit_sql = " LIMIT %s"
        params.append(limit_value)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT tmdb_id, item_type, title, emby_item_ids_json, asset_details_json
                FROM media_metadata
                WHERE in_library IS TRUE
                  AND item_type IN ('Movie', 'Episode')
                  AND washing_level IS NOT NULL
                  AND ABS(washing_level - FLOOR(washing_level) - 0.1) < 0.0001
                  AND emby_item_ids_json IS NOT NULL
                  AND jsonb_typeof(emby_item_ids_json) = 'array'
                  AND jsonb_array_length(emby_item_ids_json) > 0
                ORDER BY date_added DESC NULLS LAST
                {limit_sql}
                """,
                params,
            )
            return [dict(row) for row in cursor.fetchall()]


def _safe_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _match_asset_for_emby_id(assets: List[Any], emby_id: str) -> Dict[str, Any]:
    dict_assets = [asset for asset in assets if isinstance(asset, dict)]
    for asset in dict_assets:
        if str(asset.get("emby_item_id") or "").strip() == str(emby_id):
            return asset
    return dict_assets[0] if len(dict_assets) == 1 else {}


def _zh_sidecar_path(media_path: str) -> str:
    base, _ext = os.path.splitext(media_path)
    return f"{base}.zh-Hans.srt"
