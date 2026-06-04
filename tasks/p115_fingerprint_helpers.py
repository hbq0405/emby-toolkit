# tasks/p115_fingerprint_helpers.py
# 115 PC/SHA1 指纹补齐共享辅助函数

import json
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional

from database import media_db

logger = logging.getLogger(__name__)

VIDEO_EXTS = {
    '.mkv', '.mp4', '.ts', '.avi', '.rmvb', '.wmv', '.mov',
    '.m2ts', '.flv', '.mpg', '.iso', '.strm'
}


def p115_fp_is_missing(value) -> bool:
    """判断 media_metadata 中的 PC/SHA1 槽位是否为空。"""
    if value is None:
        return True
    text = str(value).strip()
    return text == '' or text.lower() in {'none', 'null', '[]', '{}'}


def p115_fp_safe_json_list(value) -> list:
    """兼容 jsonb / 字符串 / None，统一返回 list。"""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def p115_fp_clean_path(value) -> Optional[str]:
    """清洗路径字符串，去掉 query/hash，统一路径分隔符。"""
    if not value:
        return None
    try:
        from urllib.parse import unquote
        text = unquote(str(value).strip())
    except Exception:
        text = str(value).strip()
    if not text:
        return None
    if text.startswith('file://'):
        text = text[7:]
    text = text.split('#', 1)[0].split('?', 1)[0]
    return text.replace('\\', '/')


def p115_fp_read_strm_target(path: str) -> Optional[str]:
    """读取 STRM 内容，失败时返回 None。"""
    if not path or not str(path).lower().endswith('.strm'):
        return None
    try:
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        return content.replace('\\', '/') if content else None
    except Exception:
        return None


def p115_fp_build_local_path_candidates(path: str, strm_target: Optional[str], local_root: str = '') -> List[str]:
    """
    为挂载模式/STRM 模式构造 p115_filesystem_cache.local_path 候选值。
    - 标准 STRM：asset path 是 .strm，本地缓存是同目录真实视频扩展名。
    - 挂载模式：STRM 内容可能没有 PC，只能靠路径映射到 local_path。
    """
    candidates = []

    def _add(raw_path):
        cleaned = p115_fp_clean_path(raw_path)
        if not cleaned:
            return
        if re.match(r'^https?://', cleaned, re.IGNORECASE):
            return

        variants = [cleaned]
        if local_root:
            try:
                root_norm = os.path.normpath(local_root).replace('\\', '/').rstrip('/')
                path_norm = os.path.normpath(cleaned).replace('\\', '/')
                if path_norm == root_norm:
                    return
                if path_norm.startswith(root_norm + '/'):
                    variants.append(path_norm[len(root_norm):].lstrip('/'))
            except Exception:
                pass

        for item in variants:
            norm = re.sub(r'/+', '/', str(item).replace('\\', '/')).strip('/')
            if norm and norm not in candidates:
                candidates.append(norm)

    _add(path)
    _add(strm_target)

    # STRM 文件本身通常是 xxx.strm，而 p115_filesystem_cache.local_path 记录的是 xxx.mkv/mp4 等真实视频名。
    clean_path = p115_fp_clean_path(path)
    clean_target = p115_fp_clean_path(strm_target)
    if clean_path and clean_path.lower().endswith('.strm') and clean_target:
        real_ext = os.path.splitext(clean_target)[1]
        if real_ext and real_ext.lower() in VIDEO_EXTS - {'.strm'}:
            _add(os.path.splitext(clean_path)[0] + real_ext)

    return candidates


def p115_fp_compute_fid_from_pickcode(pick_code: Optional[str]):
    """本地按 pick_code 计算 115 FID，库不存在时回退 DB 缓存。"""
    if not pick_code:
        return None
    try:
        from p115pickcode import to_id
        return str(to_id(pick_code))
    except Exception:
        pass
    try:
        from p115client.tool.iterdir import to_id
        return str(to_id(pick_code))
    except Exception:
        pass
    try:
        from handler.p115_service import P115CacheManager
        fid = P115CacheManager.get_fid_by_pickcode(pick_code)
        return str(fid) if fid else None
    except Exception:
        return None


def p115_fp_extract_info_data(info_res) -> Dict[str, Any]:
    """统一提取 fs_get_info 返回中的关键字段。"""
    if not isinstance(info_res, dict) or not info_res.get('state'):
        return {}
    data = info_res.get('data')
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        return {}

    # ★ 兼容 OpenAPI 的 paths 字段提取父目录 ID
    parent_id = data.get('parent_id') or data.get('pid') or data.get('cid')
    if not parent_id and 'paths' in data and isinstance(data['paths'], list) and len(data['paths']) > 0:
        # paths 数组的最后一个元素就是直接父目录
        last_path_node = data['paths'][-1]
        parent_id = last_path_node.get('file_id') or last_path_node.get('cid')

    return {
        'id': data.get('fid') or data.get('file_id') or data.get('id'),
        'parent_id': parent_id,
        'name': data.get('fn') or data.get('n') or data.get('file_name') or data.get('name'),
        'sha1': data.get('sha1') or data.get('sha') or data.get('file_sha1'),
        'pick_code': data.get('pc') or data.get('pick_code') or data.get('pickcode'),
        'size': _parse_size_to_bytes(data.get('size_byte') or data.get('fs') or data.get('size') or data.get('file_size') or 0),
    }


def _merge_cache_row(cache_row, values: Dict[str, Any]) -> bool:
    if not cache_row:
        return False
    changed = False
    for src_key, dst_key in [
        ('id', 'fid'),
        ('parent_id', 'parent_id'),
        ('name', 'name'),
        ('sha1', 'sha1'),
        ('pick_code', 'pc'),
        ('local_path', 'local_path'),
        ('size', 'size'),
    ]:
        val = cache_row.get(src_key) if isinstance(cache_row, dict) else None
        if val not in (None, '') and not values.get(dst_key):
            values[dst_key] = str(val) if dst_key != 'size' else val
            changed = True
    return changed


def _row_to_dict(row) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _guess_item_type(row: Dict[str, Any]) -> str:
    item_type = row.get('item_type') or row.get('type')
    if item_type:
        return str(item_type)
    if row.get('episode_number') not in (None, ''):
        return 'Episode'
    return 'Movie'


def _parse_size_to_bytes(size_val) -> int:
    """将各种格式的大小转换为纯字节数 (BIGINT)"""
    try:
        if size_val is None: return 0
        if isinstance(size_val, (int, float)): return int(size_val)
        if isinstance(size_val, str):
            s = size_val.strip()
            if not s: return 0
            if s.isdigit(): return int(s)
            s_upper = s.upper().replace(',', '')
            mult = 1
            if 'TB' in s_upper: mult = 1024**4
            elif 'GB' in s_upper: mult = 1024**3
            elif 'MB' in s_upper: mult = 1024**2
            elif 'KB' in s_upper: mult = 1024
            match = re.search(r'([\d\.]+)', s_upper)
            if match:
                return int(float(match.group(1)) * mult)
    except Exception:
        pass
    return 0

def _get_asset_size(asset: Dict[str, Any]) -> int:
    val = (
        asset.get('size_bytes')
        or asset.get('size')
        or asset.get('Size')
        or asset.get('file_size')
        or 0
    )
    return _parse_size_to_bytes(val)


def repair_p115_fingerprints_for_rows(
    processor,
    rows: List[Dict[str, Any]],
    *,
    local_root: str = '',
    update_db: bool = True,
    allow_api_fetch: bool = True,
    log_prefix: str = '补齐115指纹',
    should_stop: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[int, str], None]] = None,
    progress_interval: int = 50,
    video_exts: Optional[set] = None,
) -> Dict[str, Any]:
    """
    全方位体检并补齐 115 PC/SHA1 及本地缓存。
    """
    stats = {
        'scanned_assets': 0,
        'missing_assets': 0,
        'fixed_assets': 0,
        'api_fixed_assets': 0,
        'cache_fixed_assets': 0,
        'failed_assets': 0,
        'updated_rows': 0,
        'cache_updates': 0,
        'total_rows': len(rows or []),
        'interrupted': False,
    }

    if not rows:
        return stats

    try:
        from handler.p115_service import P115Service, P115CacheManager
        client = P115Service.get_client() if allow_api_fetch else None
    except Exception as e:
        logger.warning(f"  ➜ [{log_prefix}] 初始化 115 服务失败，将只使用已有路径/字段: {e}")
        P115CacheManager = None
        client = None

    video_exts = video_exts or VIDEO_EXTS
    rows = [_row_to_dict(row) for row in rows]
    total_rows = len(rows)

    for row_idx, row in enumerate(rows):
        if callable(should_stop) and should_stop():
            stats['interrupted'] = True
            logger.warning(f"  ➜ [{log_prefix}] 收到停止信号，提前结束。")
            break

        if callable(progress_callback) and progress_interval > 0 and row_idx % progress_interval == 0:
            try:
                progress = int((row_idx / max(total_rows, 1)) * 100)
                progress_callback(
                    progress,
                    f"正在检查 ({row_idx + 1}/{total_rows})，已修复 {stats['fixed_assets']} 个资产..."
                )
            except Exception:
                pass

        tmdb_id = str(row.get('tmdb_id') or '').strip()
        item_type = _guess_item_type(row)
        title = row.get('title') or tmdb_id or '未知媒体'
        assets = p115_fp_safe_json_list(row.get('asset_details_json'))
        sha1s = p115_fp_safe_json_list(row.get('file_sha1_json'))
        pcs = p115_fp_safe_json_list(row.get('file_pickcode_json'))

        if not assets:
            continue

        while len(sha1s) < len(assets):
            sha1s.append(None)
        while len(pcs) < len(assets):
            pcs.append(None)

        row_changed = False

        for asset_idx, asset in enumerate(assets):
            if not isinstance(asset, dict):
                continue

            path = asset.get('path') or asset.get('Path')
            if not path:
                continue

            clean_path = p115_fp_clean_path(path) or str(path)
            ext = os.path.splitext(clean_path)[1].lower()
            if ext and ext not in video_exts:
                continue

            # ★ 优化 1：释放 GIL 锁，防止全库体检时 CPU 满载导致前端无响应
            import time
            time.sleep(0.002)

            stats['scanned_assets'] += 1
            current_sha1 = sha1s[asset_idx] if asset_idx < len(sha1s) else None
            current_pc = pcs[asset_idx] if asset_idx < len(pcs) else None

            need_sha1 = p115_fp_is_missing(current_sha1)
            need_pc = p115_fp_is_missing(current_pc)
            
            if need_sha1 or need_pc:
                stats['missing_assets'] += 1

            strm_target = p115_fp_read_strm_target(clean_path)
            local_candidates = p115_fp_build_local_path_candidates(clean_path, strm_target, local_root)
            
            # ★ 优化 2：防止 .strm 污染 115 真实文件名
            base_name = os.path.basename(clean_path) if clean_path else None
            if base_name and base_name.lower().endswith('.strm'):
                base_name = None # 置空，强制依赖本地缓存或 115 API 获取真实文件名

            # ★ 核心修复：防止绝对路径污染 local_path
            # 仅当明确配置了 local_root 且成功剥离出相对路径时，才作为候选写入
            best_local_path = None
            if local_root and local_candidates:
                shortest = min(local_candidates, key=len)
                # 如果最短的候选路径比原始路径短，说明成功剥离了 local_root
                if len(shortest) < len(clean_path):
                    best_local_path = shortest

            values = {
                'fid': None,
                'parent_id': None,
                'name': base_name,
                'sha1': None if need_sha1 else str(current_sha1).strip().upper(),
                'pc': None if need_pc else str(current_pc).strip(),
                'local_path': best_local_path, # 传 None 时，SQL 的 COALESCE 会完美保留数据库中原有的正确路径
                'size': _get_asset_size(asset),
            }

            # 1) 走已有万能提取器 (仅在缺失 PC/SHA1 时调用)
            if need_pc or need_sha1:
                extractor = getattr(processor, '_extract_115_fingerprints', None) if processor else None
                if callable(extractor):
                    for probe_path in [clean_path, strm_target]:
                        if not probe_path:
                            continue
                        try:
                            extracted_pc, extracted_sha1 = extractor(probe_path)
                            if extracted_pc and need_pc and not values.get('pc'):
                                values['pc'] = str(extracted_pc).strip()
                            if extracted_sha1 and need_sha1 and not values.get('sha1'):
                                values['sha1'] = str(extracted_sha1).strip().upper()
                        except Exception as e:
                            logger.debug(f"  ➜ [{log_prefix}] 指纹提取器跳过异常路径: {probe_path} -> {e}")

            # 2) 从 p115_filesystem_cache 反查
            cache_hit = False
            cache_is_complete = False # ★ 新增：标记缓存是否完整
            if P115CacheManager:
                try:
                    cache_row = None
                    if values.get('pc'):
                        cache_row = P115CacheManager.get_file_cache_by_pickcode(values['pc'])
                    if not cache_row and values.get('sha1'):
                        cache_row = P115CacheManager.get_file_cache_by_sha1(values['sha1'])
                    if not cache_row and values.get('fid'):
                        cache_row = P115CacheManager.get_file_cache_by_id(values['fid'])

                    if not cache_row:
                        for local_candidate in local_candidates:
                            cache_row = P115CacheManager.get_file_cache_by_local_path(local_candidate)
                            if cache_row:
                                break

                    if cache_row:
                        cache_hit = True
                        _merge_cache_row(cache_row, values)
                        
                        # ★ 核心修改 2：检查缓存六芒星是否齐全
                        if (values.get('fid') and values.get('parent_id') and 
                            values.get('name') and values.get('sha1') and 
                            values.get('pc') and values.get('size')):
                            cache_is_complete = True
                except Exception as e:
                    logger.debug(f"  ➜ [{log_prefix}] 查询 p115_filesystem_cache 失败: {e}")

            # 3) 仍然缺字段时，现场按 PC 计算 FID，再查 115 详情。
            # ★ 核心修改 3：只要缓存不完整，也触发 API 查询
            if (need_sha1 and not values.get('sha1')) or (need_pc and not values.get('pc')) or not cache_is_complete:
                if not values.get('fid') and values.get('pc'):
                    values['fid'] = p115_fp_compute_fid_from_pickcode(values['pc'])

                if values.get('fid') and client:
                    try:
                        info_res = client.fs_get_info(values['fid'])
                        info_data = p115_fp_extract_info_data(info_res)
                        if info_data:
                            if info_data.get('sha1') and not values.get('sha1'):
                                values['sha1'] = str(info_data['sha1']).strip().upper()
                            if info_data.get('pick_code') and not values.get('pc'):
                                values['pc'] = str(info_data['pick_code']).strip()
                            
                            if info_data.get('name'):
                                values['name'] = info_data['name']
                            else:
                                values['name'] = values.get('name')
                                
                            values['parent_id'] = values.get('parent_id') or info_data.get('parent_id')
                            values['size'] = values.get('size') or info_data.get('size') or 0
                            stats['api_fixed_assets'] += 1
                    except Exception as e:
                        logger.warning(f"  ➜ [{log_prefix}] 现场查询 115 详情失败 fid={values.get('fid')}: {e}")

            # 4) 成功拿到字段后，回写 p115_filesystem_cache。
            if P115CacheManager and values.get('fid') and values.get('parent_id') and values.get('name'):
                # ★ 核心修改 4：只要之前缓存不完整，就执行回写
                if not cache_is_complete:
                    try:
                        P115CacheManager.save_file_cache(
                            fid=values['fid'],
                            parent_id=values['parent_id'],
                            name=values['name'],
                            sha1=values.get('sha1'),
                            pick_code=values.get('pc'),
                            local_path=values.get('local_path'),
                            size=values.get('size') or 0,
                        )
                        stats['cache_updates'] += 1
                    except Exception as e:
                        logger.debug(f"  ➜ [{log_prefix}] 回写 p115_filesystem_cache 失败 fid={values.get('fid')}: {e}")

            asset_fixed = False
            if need_sha1 and values.get('sha1'):
                sha1s[asset_idx] = str(values['sha1']).strip().upper()
                row_changed = True
                asset_fixed = True
            if need_pc and values.get('pc'):
                pcs[asset_idx] = str(values['pc']).strip()
                row_changed = True
                asset_fixed = True

            if asset_fixed:
                stats['fixed_assets'] += 1
                if cache_hit:
                    stats['cache_fixed_assets'] += 1
                logger.info(
                    f"  ➜ [{log_prefix}] 已补齐 {title} #{asset_idx + 1}: "
                    f"PC={'✓' if not p115_fp_is_missing(pcs[asset_idx]) else '×'}, "
                    f"SHA1={'✓' if not p115_fp_is_missing(sha1s[asset_idx]) else '×'}"
                )
            elif not need_sha1 and not need_pc:
                # ★ 核心修改 5：如果媒体库本身不缺，但修复了本地缓存，打印一条体检日志
                if not cache_is_complete and values.get('fid') and values.get('parent_id'):
                    logger.info(f"  ➜ [{log_prefix}] 已体检并修复本地缓存 {title} #{asset_idx + 1}")
            else:
                stats['failed_assets'] += 1
                logger.warning(
                    f"  ➜ [{log_prefix}] 暂无法补齐 {title} #{asset_idx + 1}: "
                    f"path={clean_path}"
                )

        if row_changed and update_db and tmdb_id and item_type:
            try:
                media_db.update_media_sha1_and_pc_json(tmdb_id, item_type, sha1s, pcs)
                stats['updated_rows'] += 1
            except Exception as e:
                logger.error(f"  ➜ [{log_prefix}] 写回 media_metadata 失败 {tmdb_id}/{item_type}: {e}", exc_info=True)

    return stats
