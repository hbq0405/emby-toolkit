import json
import logging
import os
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
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
                "subtitle_path": subtitle_path,
                "size_bytes": _asset_size_bytes(asset),
                "pick_code": _pick_code_for_asset(row, asset, emby_id),
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

    speed_bps = _measure_115_download_speed(unique_candidates)
    timeout_seconds = _estimate_extract_timeout(unique_candidates, speed_bps)
    eta_seconds = _estimate_total_seconds(unique_candidates, speed_bps)
    speed_text = _format_speed(speed_bps)
    eta_text = _format_duration(eta_seconds)
    finish_at = datetime.now() + timedelta(seconds=eta_seconds)
    logger.info(
        "  ➜ [AI字幕补救] 找到 %s 个缺少 AI 中文外挂字幕的临时项，测速=%s，抽取超时=%ss，预计耗时约 %s，预计完成时间 %s。",
        total,
        speed_text,
        timeout_seconds,
        eta_text,
        finish_at.strftime("%H:%M:%S"),
    )
    task_manager.update_status_from_thread(
        1,
        f"AI 字幕补救准备执行：{total} 条，测速 {speed_text}，预计耗时约 {eta_text}，预计 {finish_at:%H:%M:%S} 完成",
    )
    failed = 0
    retried = 0
    generated = 0
    for index, item in enumerate(unique_candidates, start=1):
        if processor and getattr(processor, "is_stop_requested", lambda: False)():
            logger.info("  ➜ [AI字幕补救] 收到停止信号，已中止。")
            break

        progress = int(((index - 1) / total) * 95)
        title = item["title"]
        item_eta = _format_duration(_estimate_item_seconds(item, speed_bps))
        task_manager.update_status_from_thread(progress, f"({index}/{total}) 正在翻译 AI 字幕：{title}，预计本条约 {item_eta}")
        try:
            process_ai_subtitle_translation_for_emby_items(
                [item["emby_id"]],
                item_name_for_log=title,
                extract_timeout_seconds=timeout_seconds,
            )
            retried += 1
            if os.path.exists(item["subtitle_path"]):
                generated += 1
            else:
                failed += 1
                logger.warning("  ➜ [AI字幕补救] 本轮未生成中文字幕：%s", title)
        except Exception as e:
            failed += 1
            logger.warning("  ➜ [AI字幕补救] 重试失败，已跳过：%s，错误=%s", title, e, exc_info=True)

    final_message = (
        f"AI 字幕补救完成：检查 {len(rows)} 条，重试 {retried} 条，生成 {generated} 条，失败 {failed} 条，"
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
                SELECT tmdb_id, item_type, title, emby_item_ids_json, asset_details_json, file_pickcode_json
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


def _asset_size_bytes(asset: Dict[str, Any]) -> int:
    for key in ("size_bytes", "file_size", "size", "Size"):
        try:
            value = int(asset.get(key) or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    return 0


def _pick_code_for_asset(row: Dict[str, Any], asset: Dict[str, Any], emby_id: str) -> str:
    direct = str(asset.get("pick_code") or asset.get("pickcode") or "").strip()
    if direct:
        return direct

    emby_ids = _safe_json(row.get("emby_item_ids_json"), [])
    pickcodes = _safe_json(row.get("file_pickcode_json"), [])
    if isinstance(emby_ids, list) and isinstance(pickcodes, list) and str(emby_id) in [str(x) for x in emby_ids]:
        idx = [str(x) for x in emby_ids].index(str(emby_id))
        if idx < len(pickcodes) and pickcodes[idx]:
            return str(pickcodes[idx]).strip()
    return ""


def _measure_115_download_speed(candidates: List[Dict[str, Any]]) -> float:
    pick_code = next((str(item.get("pick_code") or "").strip() for item in candidates if item.get("pick_code")), "")
    if not pick_code:
        logger.info("  ➜ [AI字幕补救] 未找到可测速的 115 pick_code，使用保守速度估算。")
        return 8 * 1024 * 1024

    try:
        from handler.p115_service import P115Service, get_115_tokens, get_115_ua

        client = P115Service.get_client()
        if not client:
            raise RuntimeError("115 客户端未就绪")
        try:
            _, _, _, app_type = get_115_tokens()
        except Exception:
            app_type = "web"
        user_agent = get_115_ua(app_type or "web") or "Mozilla/5.0"
        url = client.download_url(pick_code, user_agent=user_agent)
        if not url:
            raise RuntimeError("获取 115 直链失败")

        sample_size = 32 * 1024 * 1024
        with tempfile.NamedTemporaryFile(prefix="etk-ai-speed-", delete=False) as tmp:
            tmp_path = tmp.name
        started = time.monotonic()
        cmd = [
            "curl",
            "-L",
            "-r",
            f"0-{sample_size - 1}",
            "--max-time",
            "60",
            "-A",
            user_agent,
            "-o",
            tmp_path,
            str(url),
        ]
        subprocess.run(cmd, capture_output=True, text=True, timeout=70, check=True)
        elapsed = max(time.monotonic() - started, 0.1)
        downloaded = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
        speed = downloaded / elapsed if downloaded > 0 else 0
        logger.info("  ➜ [AI字幕补救] 115 直链测速完成：%s，样本 %.1f MB。", _format_speed(speed), downloaded / 1024 / 1024)
        return speed or 8 * 1024 * 1024
    except Exception as e:
        logger.warning("  ➜ [AI字幕补救] 115 直链测速失败，使用保守速度估算：%s", e)
        return 8 * 1024 * 1024
    finally:
        try:
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _estimate_item_seconds(item: Dict[str, Any], speed_bps: float) -> int:
    size = int(item.get("size_bytes") or 0)
    if size <= 0 or speed_bps <= 0:
        return 300
    return max(90, int((size / speed_bps) * 1.35 + 60))


def _estimate_extract_timeout(candidates: List[Dict[str, Any]], speed_bps: float) -> int:
    seconds = max((_estimate_item_seconds(item, speed_bps) for item in candidates), default=300)
    return max(150, min(seconds, 3600))


def _estimate_total_seconds(candidates: List[Dict[str, Any]], speed_bps: float) -> int:
    # 每条只要成功抽到第一条源字幕就会进入 AI 翻译，给翻译阶段额外留 3 分钟。
    return sum(_estimate_item_seconds(item, speed_bps) + 180 for item in candidates)


def _format_speed(speed_bps: float) -> str:
    try:
        speed = float(speed_bps or 0)
    except Exception:
        speed = 0
    if speed <= 0:
        return "未知"
    return f"{speed / 1024 / 1024:.1f} MB/s"


def _format_duration(seconds: int) -> str:
    try:
        seconds = int(seconds or 0)
    except Exception:
        seconds = 0
    if seconds < 60:
        return f"{seconds} 秒"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} 分 {sec} 秒" if sec else f"{minutes} 分钟"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} 小时 {minutes} 分钟"


def _match_asset_for_emby_id(assets: List[Any], emby_id: str) -> Dict[str, Any]:
    dict_assets = [asset for asset in assets if isinstance(asset, dict)]
    for asset in dict_assets:
        if str(asset.get("emby_item_id") or "").strip() == str(emby_id):
            return asset
    return dict_assets[0] if len(dict_assets) == 1 else {}


def _zh_sidecar_path(media_path: str) -> str:
    base, _ext = os.path.splitext(media_path)
    return f"{base}.zh-Hans.srt"
