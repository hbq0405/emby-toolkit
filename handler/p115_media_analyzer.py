# handler/p115_media_analyzer.py
import json
import logging
import os
import re

from database import settings_db
from tasks import helpers
import utils

logger = logging.getLogger(__name__)


def _get_p115_cache_manager():
    """
    延迟导入，避免 p115_service -> p115_media_analyzer -> p115_service 的循环导入。
    """
    from handler.p115_service import P115CacheManager
    return P115CacheManager


class P115MediaAnalyzerMixin:
    """
    115 视频流 / ffprobe / Emby MediaInfo 解析逻辑。

    这个 mixin 依赖宿主类提供：
    - self.client：具备 download_url/openapi_downurl 方法
    - self.language_map：语言映射表
    """

    def _extract_video_info(self, filename):
        """
        从文件名提取视频信息，返回字典供乐高模块调用
        """
        info_dict = {
            'source': '', 'effect': '', 'resolution': '', 
            'codec': '', 'audio': '', 'group': '', 'stream': '', 'fps': '' # ★ 新增 fps 字段
        }
        name_upper = filename.upper()

        # 1. 来源 (Source)
        if re.search(r'REMUX', name_upper): info_dict['source'] = 'Remux'
        elif re.search(r'BLU-?RAY|BD', name_upper): info_dict['source'] = 'BluRay'
        elif re.search(r'WEB-?DL', name_upper): info_dict['source'] = 'WEB-DL'
        elif re.search(r'WEB-?RIP', name_upper): info_dict['source'] = 'WEBRip'
        elif re.search(r'HDTV', name_upper): info_dict['source'] = 'HDTV'
        elif re.search(r'DVD', name_upper): info_dict['source'] = 'DVD'
        if 'UHD' in name_upper:
            info_dict['source'] = 'UHD BluRay' if info_dict['source'] == 'BluRay' else 'UHD'

        # 2. 特效 (Effect)
        is_dv = re.search(r'(?:^|[\.\s\-\_])(DV|DOVI|DOLBY\s?VISION)(?:$|[\.\s\-\_])', name_upper)
        # 优化正则顺序，优先匹配 HDR10+ 和 HDR10
        is_hdr = re.search(r'(?:^|[\.\s\-\_])(HDR10\+|HDR10|HDR)(?:$|[\.\s\-\_])', name_upper)
        
        hdr_str = is_hdr.group(1) if is_hdr else ""
        if is_dv and is_hdr: info_dict['effect'] = f"{hdr_str} DV"
        elif is_dv: info_dict['effect'] = "DV"
        elif is_hdr: info_dict['effect'] = hdr_str

        # 3. 分辨率 (Resolution)
        res_match = re.search(r'(2160|1080|720|480)[pP]', filename)
        if res_match: info_dict['resolution'] = res_match.group(0).lower()
        elif '4K' in name_upper: info_dict['resolution'] = '2160p'

        # 4. 编码 (Codec) - ★ 统一使用商业名
        codec = ""
        if re.search(r'[HX][\.\s]?265|HEVC', name_upper): codec = 'HEVC'
        elif re.search(r'[HX][\.\s]?264|AVC', name_upper): codec = 'AVC'
        elif re.search(r'AV1', name_upper): codec = 'AV1'
        
        bit_match = re.search(r'(\d{1,2})BIT', name_upper)
        bit_depth = f"{bit_match.group(1)}bit" if bit_match else ""
        
        if codec and bit_depth: info_dict['codec'] = f"{codec} {bit_depth}"
        elif codec: info_dict['codec'] = codec
        elif bit_depth: info_dict['codec'] = bit_depth

        # 5. 音频 (Audio) 与 音轨数 (Audio Count) 分离
        audio_info = []
        audio_count_str = ""
        
        # 提取音轨数
        num_audio_match = re.search(r'\b(\d+)\s?Audios?\b', name_upper, re.IGNORECASE)
        if num_audio_match: 
            audio_count_str = f"{num_audio_match.group(1)}Audios"
        elif re.search(r'\b(Multi|双语|多音轨|Dual-Audio)\b', name_upper, re.IGNORECASE): 
            audio_count_str = 'Multi'
            
        if audio_count_str:
            info_dict['audio_count'] = audio_count_str

        # 提取音频格式
        if re.search(r'ATMOS', name_upper): audio_info.append('Atmos')
        elif re.search(r'TRUEHD', name_upper): audio_info.append('TrueHD')
        elif re.search(r'DTS-?HD(\s?MA)?', name_upper): audio_info.append('DTS-HD')
        elif re.search(r'DTS', name_upper): audio_info.append('DTS')
        elif re.search(r'DDP|EAC3|DOLBY\s?DIGITAL\+', name_upper): audio_info.append('DDP')
        elif re.search(r'AC3|DD', name_upper): audio_info.append('AC3')
        elif re.search(r'AAC', name_upper): audio_info.append('AAC')
        elif re.search(r'FLAC', name_upper): audio_info.append('FLAC')
        
        # 声道
        chan_match = re.search(r'(?<!\d)(7\.1|5\.1|2\.0)(?!\d)', filename)
        if chan_match: audio_info.append(chan_match.group(1))
        
        if audio_info: 
            info_dict['audio'] = " ".join(audio_info)

        # 帧率 (FPS) 提取
        fps_match = re.search(r'(?<!\d)(\d{2,3}FPS)\b', name_upper)
        if fps_match:
            info_dict['fps'] = fps_match.group(1).lower() # 统一转为小写 60fps

        # 流媒体平台识别 (扩充国内平台与HQ标识)
        stream_match = re.search(r'\b(NF|AMZN|DSNP|HMAX|HULU|NETFLIX|DISNEY\+|APPLETV\+|B-GLOBAL|ITUNES|IQ|YK|TC|VIU|HQ)\b', name_upper)
        if stream_match:
            info_dict['stream'] = stream_match.group(1)

        # 6. 发布组 (Group)
        try:
            from tasks import helpers
            for group_name, patterns in helpers.RELEASE_GROUPS.items():
                for pattern in patterns:
                    match = re.search(pattern, filename, re.IGNORECASE)
                    if match:
                        info_dict['group'] = match.group(0) 
                        break
                if info_dict['group']: break
            if not info_dict['group']:
                match_suffix = re.search(r'-([a-zA-Z0-9]+)$', os.path.splitext(filename)[0])
                if match_suffix and len(match_suffix.group(1)) > 2 and match_suffix.group(1).upper() not in ['1080P', '2160P', '4K', 'HDR', 'H265', 'H264']:
                    info_dict['group'] = match_suffix.group(1)
        except: pass

        return info_dict

    def _probe_mediainfo_with_ffprobe(self, file_node, sha1=None, silent_log=False):
        """
        最终兜底：通过 115 直链调用容器内 ffprobe。
        返回 Emby MediaSourceInfo 标准兼容结构，可直接写入 p115_mediainfo_cache。
        """
        if not file_node:
            return None

        if isinstance(file_node, dict):
            pick_code = (
                file_node.get('pc')
                or file_node.get('pick_code')
                or file_node.get('pickcode')
            )
            original_name = (
                file_node.get('fn')
                or file_node.get('n')
                or file_node.get('file_name')
                or file_node.get('original_name')
                or sha1
                or "unknown"
            )
        else:
            pick_code = (
                getattr(file_node, 'pc', None)
                or getattr(file_node, 'pick_code', None)
                or getattr(file_node, 'pickcode', None)
            )
            original_name = (
                getattr(file_node, 'fn', None)
                or getattr(file_node, 'n', None)
                or getattr(file_node, 'file_name', None)
                or getattr(file_node, 'original_name', None)
                or sha1
                or "unknown"
            )

        if not pick_code:
            if not silent_log:
                logger.debug(f"  ➜ [ffprobe] 缺少 pick_code，跳过: {original_name}")
            return None

        try:
            import shutil
            import subprocess

            if not shutil.which("ffprobe"):
                if not silent_log:
                    logger.warning("  ➜ [ffprobe] 容器内未找到 ffprobe，请在镜像中安装 ffmpeg。")
                return None

            direct_url = None

            # 1. Cookie 直链优先
            try:
                direct_url = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
            except Exception as e:
                if not silent_log:
                    logger.debug(f"  ➜ [ffprobe] Cookie 直链获取失败: {e}")

            # 2. OpenAPI 直链兜底
            if not direct_url:
                try:
                    direct_url = self.client.openapi_downurl(pick_code, user_agent="Mozilla/5.0")
                except Exception as e:
                    if not silent_log:
                        logger.debug(f"  ➜ [ffprobe] OpenAPI 直链获取失败: {e}")

            if not direct_url:
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 无法获取直链，跳过: {original_name}")
                return None

            if not silent_log:
                logger.info(f"  ➜ [ffprobe] 尝试用 ffprobe 解析媒体信息")

            cmd = [
                "ffprobe",
                "-hide_banner",
                "-v", "error",
                "-user_agent", "Mozilla/5.0",
                "-rw_timeout", "15000000",
                "-analyzeduration", "20000000",
                "-probesize", "20000000",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                "-show_chapters",
                str(direct_url)
            ]

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=45
            )

            if proc.returncode != 0:
                err = (proc.stderr or "").strip()
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 解析失败: {original_name} -> {err[:300]}")
                return None

            probe_data = json.loads(proc.stdout or "{}")
            emby_json = self._build_emby_mediainfo_from_ffprobe(
                probe_data,
                file_node,
                sha1=sha1
            )

            if not emby_json:
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 未解析出有效 MediaStreams: {original_name}")
                return None

            if not silent_log:
                logger.info(f"  ➜ [ffprobe] 成功生成媒体信息 -> {original_name}")

            return emby_json, probe_data

        except subprocess.TimeoutExpired:
            if not silent_log:
                logger.warning(f"  ➜ [ffprobe] 解析超时，跳过: {original_name}")
            return None
        except Exception as e:
            if not silent_log:
                logger.warning(f"  ➜ [ffprobe] 解析异常: {original_name} -> {e}", exc_info=True)
            return None
    
    def _ffprobe_rate_to_float(self, value):
        """解析 ffprobe 帧率：24000/1001 -> 23.976"""
        if not value or value == "0/0":
            return None
        try:
            value = str(value)
            if "/" in value:
                a, b = value.split("/", 1)
                a = float(a)
                b = float(b)
                if b == 0:
                    return None
                return a / b
            return float(value)
        except Exception:
            return None

    def _safe_int(self, value, default=0):
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except Exception:
            return default

    def _extract_stream_features(self, stream_type, *texts):
        """
        从 Title / DisplayTitle 等文本里提取非语言类标签：
        例如 DYSY -> 东影上译，TX -> 特效，Latin America -> 拉美。
        """
        source_text = " ".join(str(t or "") for t in texts if t is not None)
        if not source_text:
            return []

        feature_map = (
            getattr(self, "stream_feature_map", None)
            or utils.DEFAULT_STREAM_FEATURE_MAPPING
        )

        features = []

        for item in feature_map:
            allowed_types = item.get("types") or ["Audio", "Subtitle"]
            if stream_type not in allowed_types:
                continue

            label = item.get("label")
            patterns = item.get("patterns") or item.get("aliases") or []

            if not label:
                continue

            for pattern in patterns:
                try:
                    if re.search(pattern, source_text, flags=re.IGNORECASE):
                        features.append(label)
                        break
                except re.error:
                    # 如果用户后期在配置里写了坏 regex，不要让整个分析炸掉
                    logger.warning(f"  ➜ 无效的流标签匹配规则: {pattern}")

        return list(dict.fromkeys(features))


    def _format_stream_feature_title(self, base_title, features):
        """
        把基础语言名和特色标签合成最终标题。
        base_title: 已经包含简繁、双语等信息 (e.g., "中文简体", "中英双语简体")
        features: 提取到的特色标签 (e.g., ["特效", "上译", "香港"])
        """
        base_title = str(base_title or "").strip()
        all_features = list(dict.fromkeys([f for f in features if f]))

        if not base_title:
            base_title = "未知"

        # 定义哪些特色标签应该作为后缀（直接连接在主标题后面）
        # 其他特色标签将放入括号中
        features_as_suffixes = ["特效", "听障"] 

        suffix_parts = []
        parenthetical_parts = []

        for f in all_features:
            if f in features_as_suffixes:
                suffix_parts.append(f)
            else:
                parenthetical_parts.append(f)

        final_title_parts = [base_title]

        # 添加后缀特色标签，避免重复
        for sf in suffix_parts:
            if sf not in base_title: 
                final_title_parts.append(sf)

        # 添加括号中的特色标签
        if parenthetical_parts:
            final_title_parts.append(f"（{'·'.join(parenthetical_parts)}）")

        return "".join(final_title_parts).strip()

    def _get_friendly_display_info(
        self,
        raw_lang,
        raw_title,
        stream_type,
        raw_display_title="",
        is_hearing_impaired=False
    ):
        """
        返回：(底层 ISO 代码，Emby DisplayLanguage，Emby Title)

        设计原则：
        1. norm_lang (ISO 代码): 优先从 raw_lang 获取，确保底层语言代码的准确性。
        2. emby_display_language (Emby 主语言标签): 显示更通用的语言名称 (如 "中文", "英文", "国语", "粤语")。
        3. emby_stream_title (Emby 副标题): 包含详细的语言变体、双语状态和特色词 (如 "中英双语简体特效（上译）")。
        """

        # 防御性检查，防止 language_map 未初始化
        if not hasattr(self, "language_map") or not self.language_map:
            self.language_map = settings_db.get_setting("language_mapping") or utils.DEFAULT_LANGUAGE_MAPPING

        raw_lang = str(raw_lang or "").strip()
        raw_title = str(raw_title or "").strip()
        raw_display_title = str(raw_display_title or "").strip()
        stream_type = str(stream_type or "").strip()

        def _normalize_marker_text(text):
            text = str(text or "").lower()
            text = re.sub(r"[\.\s\-\_+/|\\\[\]\(\)【】（）]+", " ", text) # Keep spaces for word boundary detection
            text = re.sub(r"\s+", " ", text).strip()
            return text

        def _has_lang_marker(text, markers):
            """
            安全判断语言标记：
            - 中文词：允许 substring
            - 英文/短码：必须是独立 token，避免 Deutsch 命中 sc
            """
            t = _normalize_marker_text(text)

            for marker in markers:
                m = _normalize_marker_text(marker)
                if not m:
                    continue

                if utils.contains_chinese(m):
                    if m in t:
                        return True
                    continue

                # 英文/短码必须独立成词
                if re.search(rf"(?<![a-z0-9]){re.escape(m)}(?![a-z0-9])", t):
                    return True

            return False
        
        def _lookup_base_label(norm_lang_code):
            if not norm_lang_code:
                return ""

            norm_lang_lower = str(norm_lang_code).lower()

            for item in self.language_map:
                value = str(item.get("value", "")).lower()
                aliases = [str(a).lower() for a in item.get("aliases", [])]

                if norm_lang_lower == value or norm_lang_lower in aliases:
                    return item.get("label") or ""

            return str(norm_lang_code).upper()

        def _display_label_from_base_label(base_label, stream_type):
            if not base_label or base_label == "未知":
                return base_label

            if stream_type == "Subtitle":
                # 字幕的主语言标签，中文统一显示为“中文”
                if base_label in ["国语", "普通话", "中文", "粤语", "广东话"]:
                    return "中文" 
                if base_label.endswith("语") and base_label != "无语言":
                    return base_label[:-1] + "文" # e.g., "法语" -> "法文"

            return base_label # 音轨直接用原始标签，如“国语”

        # 1. 确定底层 ISO 语言代码 (norm_lang)
        norm_lang = helpers.normalize_lang_code(raw_lang)
        
        # 结合所有相关文本用于特征/变体检测
        clean_text_for_detection = _normalize_marker_text(f"{raw_title} {raw_display_title} {raw_lang}")

        # Emby 的 DisplayLanguage 字段 (主语言标签)
        emby_display_language = "" 
        
        # Emby 的 Title 字段 (副标题，包含详细信息)
        emby_stream_title_base = "" # 副标题的基础部分，不含特色标签

        # =========================================================
        # 2. 字幕流解析逻辑
        # =========================================================
        if stream_type == "Subtitle":
            # 如果原始语言是中文，底层代码统一为 'chi'
            if norm_lang in ['chi', 'yue']: 
                norm_lang = 'chi' 
                emby_display_language = "中文" 
            elif norm_lang == 'eng':
                emby_display_language = "英文" 
            elif norm_lang == 'jpn':
                emby_display_language = "日文" 
            elif norm_lang == 'kor':
                emby_display_language = "韩文" 
            else:
                emby_display_language = _display_label_from_base_label(_lookup_base_label(norm_lang), stream_type) or "未知"

            # 检测简繁、双语等具体特征
            has_chs = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_chi", []))
            has_cht = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_yue", []))
            has_eng = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_eng", []))
            has_jpn = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_jpn", []))
            has_kor = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_kor", []))

            is_dual_eng = _has_lang_marker(clean_text_for_detection, ["双语", "中上英下", "英上中下", "繁上英下", "英上繁下", "简体英文", "繁体英文"])
            is_dual_jpn = _has_lang_marker(clean_text_for_detection, ["中上日下", "日上中下", "繁上日下", "日上繁下", "简体日文", "繁体日文"])
            is_dual_kor = _has_lang_marker(clean_text_for_detection, ["中上韩下", "韩上中下", "繁上韩下", "韩上繁下", "简体韩文", "繁体韩文"])

            # 确定 Emby 副标题的基础部分 (emby_stream_title_base)
            if (has_chs and has_eng and not has_cht) or (is_dual_eng and not has_cht):
                emby_stream_title_base = "中英双语简体" 
            elif (has_cht and has_eng) or (is_dual_eng and has_cht):
                emby_stream_title_base = "中英双语繁体" 
            elif (has_chs and has_jpn and not has_cht) or (is_dual_jpn and not has_cht):
                emby_stream_title_base = "中日双语简体" 
            elif (has_cht and has_jpn) or (is_dual_jpn and has_cht):
                emby_stream_title_base = "中日双语繁体" 
            elif (has_chs and has_kor and not has_cht) or (is_dual_kor and not has_cht):
                emby_stream_title_base = "中韩双语简体" 
            elif (has_cht and has_kor) or (is_dual_kor and has_cht):
                emby_stream_title_base = "中韩双语繁体" 
            elif has_cht:
                emby_stream_title_base = "中文繁体" 
            elif has_chs:
                emby_stream_title_base = "中文简体" 
            elif has_eng:
                emby_stream_title_base = "英文"
            elif has_jpn:
                emby_stream_title_base = "日文"
            elif has_kor:
                emby_stream_title_base = "韩文"
            else:
                # 如果没有检测到特定变体，尝试从原始标题中提取中文部分作为副标题基础
                cleaned_raw_title = utils.clean_non_chinese_chars(raw_title)
                if cleaned_raw_title:
                    emby_stream_title_base = cleaned_raw_title
                else:
                    # 如果原始标题也没有中文，则使用主语言标签作为副标题基础
                    emby_stream_title_base = emby_display_language

        # =========================================================
        # 3. 音轨流解析逻辑
        # =========================================================
        else: # Audio stream type
            # 检测国语/粤语等特征
            is_yue = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("yue", []))
            is_chi = _has_lang_marker(clean_text_for_detection, helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("chi", []))

            if is_yue and not is_chi:
                norm_lang = "yue"
                emby_display_language = "粤语"
            elif is_chi and not is_yue:
                norm_lang = "chi"
                emby_display_language = "国语"
            else:
                # 如果 norm_lang 未定义，尝试从关键词中推断
                if not norm_lang or norm_lang == "und":
                    for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                        if key.startswith("sub_"): continue
                        if _has_lang_marker(clean_text_for_detection, keywords):
                            norm_lang = key.replace("sub_", "")
                            break
                emby_display_language = _lookup_base_label(norm_lang) or "未知"
            
            # 音轨的副标题，通常是清理后的原始标题或主语言标签
            emby_stream_title_base = utils.clean_stream_garbage_words(raw_title) or emby_display_language

        # =========================================================
        # 4. 最终调整和特色标签提取
        # =========================================================
        # 确保 emby_display_language 不为空
        if not emby_display_language:
            emby_display_language = _lookup_base_label(norm_lang) or "未知"
            if stream_type == "Subtitle":
                # 字幕的主语言标签可能需要从通用标签转换（如“国语”->“中文”）
                emby_display_language = _display_label_from_base_label(emby_display_language, stream_type)

        # 确保 emby_stream_title_base 不为空
        if not emby_stream_title_base:
            emby_stream_title_base = emby_display_language # 副标题基础部分回退到主语言标签

        # 提取非语言特色标签
        stream_features = self._extract_stream_features(
            stream_type,
            raw_title,
            raw_display_title,
            raw_lang
        )

        # 如果是听障字幕，强制添加“听障”标签
        if stream_type == "Subtitle" and is_hearing_impaired is True:
            if "听障" not in stream_features:
                stream_features.append("听障")

        stream_features = list(dict.fromkeys([f for f in stream_features if f]))

        # 使用 _format_stream_feature_title 构建最终的 Emby Title 字段
        final_emby_stream_title = self._format_stream_feature_title(emby_stream_title_base, stream_features)

        # 冗余副标题清理：如果副标题与主语言标签相同或过于通用，则清空副标题
        if (
            not final_emby_stream_title
            or final_emby_stream_title == "未知"
            or final_emby_stream_title.lower().replace(" ", "") == emby_display_language.lower().replace(" ", "")
        ):
            final_emby_stream_title = "" # 清空副标题，让 Emby 只显示主语言标签

        # 确保 norm_lang 不为空
        if not norm_lang:
            norm_lang = raw_lang

        # 返回：ISO 代码，Emby DisplayLanguage，Emby Title
        return norm_lang, emby_display_language, final_emby_stream_title

    def _build_emby_mediainfo_from_ffprobe(self, probe_data, file_node, sha1=None):
        """
        将 ffprobe 原始 JSON 转成 Emby MediaSourceInfo 兼容格式。
        注意：这是给 p115_mediainfo_cache 用的，不是给 ffprobe 自己用的。
        """
        if not probe_data:
            return None

        original_name = (
            file_node.get("fn")
            or file_node.get("n")
            or file_node.get("file_name")
            or file_node.get("original_name")
            or sha1
            or "unknown"
        )

        ext = ""
        if "." in original_name:
            ext = original_name.rsplit(".", 1)[-1].lower()

        fmt = probe_data.get("format") or {}
        streams = probe_data.get("streams") or []
        chapters_raw = probe_data.get("chapters") or []

        media_streams = []

        size = self._safe_int(
            file_node.get("fs")
            or file_node.get("size")
            or fmt.get("size")
            or 0
        )

        duration = 0.0
        try:
            duration = float(fmt.get("duration") or 0)
        except Exception:
            duration = 0.0

        run_time_ticks = int(duration * 10000000) if duration > 0 else 0

        bitrate = self._safe_int(fmt.get("bit_rate") or 0)
        if not bitrate and size and duration > 0:
            bitrate = int(size * 8 / duration)

        container = ext
        if not container:
            format_name = (fmt.get("format_name") or "").lower()
            if "matroska" in format_name:
                container = "mkv"
            elif "mov" in format_name or "mp4" in format_name:
                container = "mp4"
            else:
                container = format_name.split(",")[0] if format_name else ""

        # 章节
        chapters = []
        for idx, ch in enumerate(chapters_raw):
            tags = ch.get("tags") or {}
            try:
                start_time = float(ch.get("start_time") or 0)
            except Exception:
                start_time = 0

            chapters.append({
                "Name": tags.get("title") or f"章节 {idx + 1}",
                "MarkerType": "Chapter",
                "ChapterIndex": idx,
                "StartPositionTicks": int(start_time * 10000000)
            })

        for s in streams:
            codec_type = (s.get("codec_type") or "").lower()
            codec = (s.get("codec_name") or "").lower()
            tags = s.get("tags") or {}
            disposition = s.get("disposition") or {}

            index = self._safe_int(s.get("index"), len(media_streams))
            is_default = bool(disposition.get("default"))
            is_forced = bool(disposition.get("forced"))

            if codec_type == "video":
                title = tags.get("title") or ""
                width = self._safe_int(s.get("width"))
                height = self._safe_int(s.get("height"))

                bit_depth = 0
                for k in ("bits_per_raw_sample", "bits_per_sample"):
                    bit_depth = self._safe_int(s.get(k))
                    if bit_depth:
                        break

                pix_fmt = (s.get("pix_fmt") or "").lower()
                if not bit_depth:
                    if "12" in pix_fmt:
                        bit_depth = 12
                    elif "10" in pix_fmt:
                        bit_depth = 10
                    elif pix_fmt:
                        bit_depth = 8

                fps = (
                    self._ffprobe_rate_to_float(s.get("avg_frame_rate"))
                    or self._ffprobe_rate_to_float(s.get("r_frame_rate"))
                )

                color_space = s.get("colorspace") or s.get("color_space") or ""
                color_transfer = s.get("color_transfer") or ""
                color_primaries = s.get("color_primaries") or ""
                profile = s.get("profile") or ""

                side_data_list = s.get("side_data_list") or []
                side_text = json.dumps(side_data_list, ensure_ascii=False)

                video_range = ""
                extended_video_type = ""
                extended_video_sub_type = "None"
                extended_video_desc = "None"

                # Dolby Vision
                dv_profile = None
                dv_compat = None

                for sd in side_data_list:
                    if not isinstance(sd, dict):
                        continue

                    side_type = str(sd.get("side_data_type") or "")
                    if "DOVI" in side_type.upper() or "DOLBY VISION" in side_type.upper():
                        dv_profile = sd.get("dv_profile") or sd.get("profile")
                        dv_compat = (
                            sd.get("dv_bl_signal_compatibility_id")
                            or sd.get("bl_signal_compatibility_id")
                            or sd.get("compatibility_id")
                        )
                        break

                if not dv_profile and ("DOVI" in side_text.upper() or "DOLBY VISION" in side_text.upper()):
                    m = re.search(r"dv_profile['\"]?\s*[:=]\s*['\"]?(\d+)", side_text, re.IGNORECASE)
                    if m:
                        dv_profile = m.group(1)

                if dv_profile:
                    dv_profile_str = str(dv_profile)
                    extended_video_type = "DolbyVision"

                    if dv_profile_str == "8":
                        if str(dv_compat or "") == "1":
                            extended_video_sub_type = "DoviProfile81"
                            extended_video_desc = "Profile 8.1 (HDR10 compatible)"
                        else:
                            extended_video_sub_type = "DoviProfile8"
                            extended_video_desc = "Profile 8"
                    elif dv_profile_str == "7":
                        extended_video_sub_type = "DoviProfile7"
                        extended_video_desc = "Profile 7 (HDR10 compatible)" if color_transfer == "smpte2084" else "Profile 7"
                    elif dv_profile_str == "5":
                        extended_video_sub_type = "DoviProfile5"
                        extended_video_desc = "Profile 5"
                    else:
                        extended_video_sub_type = f"DoviProfile{dv_profile_str}"
                        extended_video_desc = f"Profile {dv_profile_str}"

                    # 让现有解析器能得到 HDR10 DoVi P7 / HDR10 DoVi P8
                    if color_transfer == "smpte2084":
                        video_range = "DolbyVision HDR10"
                    else:
                        video_range = "DolbyVision"

                # HDR10+
                elif "HDR10+" in side_text or "SMPTE2094-40" in side_text:
                    video_range = "HDR10+"

                # HDR10 / HDR
                elif color_transfer == "smpte2084":
                    video_range = "HDR10"
                elif color_primaries == "bt2020":
                    video_range = "HDR"

                video_codec_display = {
                    "hevc": "HEVC",
                    "h264": "H264",
                    "av1": "AV1",
                    "mpeg2video": "MPEG2VIDEO",
                    "vc1": "VC1",
                }.get(codec, codec.upper())

                if width >= 3800:
                    res_display = "4K"
                elif width >= 1900:
                    res_display = "1080p"
                elif width >= 1200:
                    res_display = "720p"
                else:
                    res_display = f"{height}p" if height else ""

                if extended_video_type == "DolbyVision":
                    effect_display = "Dolby Vision"
                elif video_range == "HDR10+":
                    effect_display = "HDR10+"
                elif video_range == "HDR10":
                    effect_display = "HDR10"
                elif video_range == "HDR":
                    effect_display = "HDR"
                else:
                    effect_display = ""

                display_title = " ".join([x for x in [res_display, effect_display, video_codec_display] if x])

                media_streams.append({
                    "Type": "Video",
                    "Codec": codec,
                    "Index": index,
                    "Level": self._safe_int(s.get("level")),
                    "Title": title,
                    "Width": width,
                    "Height": height,
                    "BitRate": self._safe_int(s.get("bit_rate") or bitrate),
                    "Profile": profile,
                    "BitDepth": bit_depth,
                    "IsForced": is_forced,
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "RefFrames": self._safe_int(s.get("refs"), 1),
                    "ColorSpace": color_space,
                    "IsExternal": False,
                    "VideoRange": video_range,
                    "AspectRatio": f"{width}:{height}" if width and height else "",
                    "PixelFormat": pix_fmt,
                    "DisplayTitle": display_title,
                    "IsAnamorphic": False,
                    "IsInterlaced": False,
                    "ColorTransfer": color_transfer,
                    "RealFrameRate": fps,
                    "AttachmentSize": 0,
                    "ColorPrimaries": color_primaries,
                    "AverageFrameRate": fps,
                    "ExtendedVideoType": extended_video_type or "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": "None",
                    "IsTextSubtitleStream": False,
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": "None"
                })

            elif codec_type == "audio":
                raw_lang = tags.get("language")
                raw_title = tags.get("title")
                
                # ★ 调用新的智能解析方法，返回 Emby DisplayLanguage 和 Emby Title
                lang, emby_display_language, emby_stream_title = self._get_friendly_display_info(raw_lang, raw_title, "Audio")

                channels = self._safe_int(s.get("channels"))
                channel_layout = self._channel_layout_label(channels, s.get("channel_layout"))
                sample_rate = self._safe_int(s.get("sample_rate"))

                profile = s.get("profile") or ""
                # 使用 emby_stream_title 进行编码配置文件的判断
                codec_display = self._audio_codec_profile_label(codec, profile, emby_stream_title) 

                display_title_parts = []
                if emby_display_language and emby_display_language != "未知": 
                    display_title_parts.append(emby_display_language)
                if codec_display:
                    display_title_parts.append(codec_display)
                if channel_layout:
                    display_title_parts.append(channel_layout)
                # (默认) 将由 _set_smart_default_streams 方法添加

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Audio",
                    "Codec": codec,
                    "Index": index,
                    "Title": emby_stream_title, # ★ Emby 的 Title 字段 (副标题)
                    "BitRate": self._safe_int(s.get("bit_rate")),
                    "BitDepth": self._safe_int(s.get("bits_per_raw_sample") or s.get("bits_per_sample")),
                    "Channels": channels,
                    "IsForced": is_forced,
                    "Language": lang, # ★ 底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "SampleRate": sample_rate,
                    "DisplayTitle": display_title, # ★ Emby 的 DisplayLanguage 字段 (主标题，包含编码/声道)
                    "IsInterlaced": False,
                    "ChannelLayout": channel_layout,
                    "AttachmentSize": 0,
                    "DisplayLanguage": emby_display_language, # ★ Emby 的 DisplayLanguage 字段 (纯语言标签)
                    "ExtendedVideoType": "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": "None",
                    "IsTextSubtitleStream": False,
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": "None",
                    "Profile": profile or codec_display
                })

            elif codec_type == "subtitle":
                raw_lang = tags.get("language")
                raw_title = tags.get("title")
                raw_display_title = tags.get("DisplayTitle")
                is_hearing_impaired = tags.get("IsHearingImpaired", False)
                
                # ★ 调用新的智能解析方法，返回 Emby DisplayLanguage 和 Emby Title
                lang, emby_display_language, emby_stream_title = self._get_friendly_display_info(raw_lang, raw_title, "Subtitle", raw_display_title, is_hearing_impaired)

                sub_codec = self._subtitle_codec_label(codec)
                is_text_sub = codec in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"}

                display_title_parts = []
                if emby_display_language and emby_display_language != "未知": 
                    display_title_parts.append(emby_display_language)
                display_title_parts.append(f"({sub_codec})") # 主标题中始终显示编码格式

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Subtitle",
                    "Codec": sub_codec,
                    "Index": index,
                    "Title": emby_stream_title, # ★ Emby 的 Title 字段 (副标题)
                    "IsForced": is_forced,
                    "Language": lang, # ★ 底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "DisplayTitle": display_title, # ★ Emby 的 DisplayLanguage 字段 (主标题，包含编码)
                    "IsInterlaced": False,
                    "AttachmentSize": 0,
                    "DisplayLanguage": emby_display_language, # ★ Emby 的 DisplayLanguage 字段 (纯语言标签)
                    "ExtendedVideoType": "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": "None",
                    "IsTextSubtitleStream": is_text_sub,
                    "SubtitleLocationType": "InternalStream",
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": "None"
                })

        # =================================================================
        # ★★★ 终极智能默认轨道选择算法 (独立配置 + 拖拽优先级 + 智能跟随) ★★★
        # =================================================================
        def _set_smart_default_streams(streams):
            # 1. 从独立数据库读取用户配置
            stream_config = settings_db.get_setting('p115_default_stream_config') or {
                "audio_lang": "",
                "subtitle_lang": "",
                "audio_priority_order": ["param", "feature"],
                "audio_features": ["公映", "上译", "京译", "长译", "八一", "台配", "粤语", "评论", "导评"],
                "audio_param_priority": ["atmos", "dts_x", "truehd", "dts_hd_ma", "dts_hd_hra", "ddp", "dts", "flac", "ac3", "aac", "7_1", "5_1", "2_0"],
                "sub_priority": ["effect", "chs", "cht", "chs_eng", "cht_eng", "chs_jpn", "cht_jpn", "chs_kor", "cht_kor"]
            }
            audio_pref_code = stream_config.get("audio_lang", "")
            subtitle_pref = stream_config.get("subtitle_lang", "")
            raw_audio_priority_order = stream_config.get("audio_priority_order", ["param", "feature"])
            audio_priority_order = []
            if isinstance(raw_audio_priority_order, list):
                for priority_id in raw_audio_priority_order:
                    priority_id = str(priority_id or "").strip().lower()
                    if priority_id in ["param", "feature"] and priority_id not in audio_priority_order:
                        audio_priority_order.append(priority_id)
            for priority_id in ["param", "feature"]:
                if priority_id not in audio_priority_order:
                    audio_priority_order.append(priority_id)

            audio_features_config = stream_config.get("audio_features", [])
            audio_param_priority = stream_config.get("audio_param_priority", [
                "atmos", "dts_x", "truehd", "dts_hd_ma", "dts_hd_hra",
                "ddp", "dts", "flac", "ac3", "aac", "7_1", "5_1", "2_0"
            ])
            sub_priority = stream_config.get("sub_priority", [])

            audio_streams = [s for s in streams if s.get("Type") == "Audio"]
            sub_streams = [s for s in streams if s.get("Type") == "Subtitle"]

            default_audio = None
            
            # -----------------------------------------
            # 1. 决出默认音轨 (真太子)
            # -----------------------------------------
            if audio_streams:
                candidates = audio_streams

                if audio_pref_code:
                    lang_matched = [s for s in candidates if s.get("Language") == audio_pref_code]
                    if lang_matched:
                        candidates = lang_matched

                def _audio_match_text(audio):
                    return " ".join([
                        str(audio.get("Title", "")),
                        str(audio.get("DisplayTitle", "")),
                        str(audio.get("Codec", "")),
                        str(audio.get("Profile", "")),
                        str(audio.get("ChannelLayout", "")),
                        str(audio.get("DisplayLanguage", "")),
                    ]).lower()

                def _audio_matches_param(audio, param_id):
                    text = _audio_match_text(audio)
                    codec = str(audio.get("Codec", "")).lower()
                    profile = str(audio.get("Profile", "")).lower()
                    layout = str(audio.get("ChannelLayout", "")).lower()
                    channels = self._safe_int(audio.get("Channels"), 0)
                    param_id = str(param_id or "").lower()

                    is_dts_x = bool(re.search(r'dts[\s\-:]?x', text))
                    is_dts_hd_ma = (
                        "dts-hd ma" in text or "dts hd ma" in text
                        or (codec == "dts" and ("ma" in profile or "master" in profile or "xll" in profile))
                    )
                    is_dts_hd_hra = (
                        "dts-hd hra" in text or "dts hd hra" in text
                        or (codec == "dts" and ("hra" in profile or "high resolution" in profile))
                    )

                    if param_id == "atmos":
                        return "atmos" in text or "全景声" in text
                    if param_id == "dts_x":
                        return is_dts_x
                    if param_id == "truehd":
                        return codec == "truehd" or "truehd" in text or "true hd" in text
                    if param_id == "dts_hd_ma":
                        return is_dts_hd_ma
                    if param_id == "dts_hd_hra":
                        return is_dts_hd_hra
                    if param_id == "ddp":
                        return codec == "eac3" or "ddp" in text or "e-ac-3" in text or "eac3" in text or "dolby digital+" in text
                    if param_id == "dts":
                        return (codec == "dts" or "dts" in text) and not (is_dts_x or is_dts_hd_ma or is_dts_hd_hra)
                    if param_id == "flac":
                        return codec == "flac" or "flac" in text
                    if param_id == "ac3":
                        return codec == "ac3" or " ac3" in f" {text}" or "dolby digital" in text
                    if param_id == "aac":
                        return codec == "aac" or "aac" in text
                    if param_id == "7_1":
                        return channels == 8 or "7.1" in text or "7 1" in layout
                    if param_id == "5_1":
                        return channels == 6 or "5.1" in text or "5 1" in layout
                    if param_id == "2_0":
                        return channels == 2 or "2.0" in text or "2 0" in layout or "stereo" in text
                    if param_id == "stereo":
                        return channels == 2 or "stereo" in text

                    # 兜底：允许以后新增自定义物理参数 id / 文本，能被 Title 或 DisplayTitle 命中。
                    return bool(param_id and param_id in text)

                # 根据用户拖拽的“大类优先级”决定：物理参数优先，还是特色词优先。
                # 每个大类内部仍按各自列表的拖拽顺序排序。
                def get_audio_score(audio):
                    title_lower = _audio_match_text(audio)

                    total_params = len(audio_param_priority)
                    matched_param_rank = 0
                    matched_param_count = 0

                    # 只取“用户排序中最靠前的命中项”作为主排序，避免 AC3+5.1 这种多标签叠加反杀 AAC / 2.0。
                    for idx, param_id in enumerate(audio_param_priority):
                        if _audio_matches_param(audio, param_id):
                            matched_param_rank = total_params - idx
                            break

                    # 同时统计命中数量，只作为极弱的同级 tie-breaker。
                    for param_id in audio_param_priority:
                        if _audio_matches_param(audio, param_id):
                            matched_param_count += 1

                    total_features = len(audio_features_config)
                    matched_feature_rank = 0
                    for idx, kw in enumerate(audio_features_config):
                        kw_text = str(kw or "").strip().lower()
                        if not kw_text:
                            continue
                        if kw_text in title_lower:
                            matched_feature_rank = max(matched_feature_rank, total_features - idx)

                    category_scores = {
                        "param": matched_param_rank,
                        "feature": matched_feature_rank,
                    }
                    ordered_category_scores = tuple(category_scores.get(priority_id, 0) for priority_id in audio_priority_order)

                    bitrate = self._safe_int(audio.get("BitRate"), 0)
                    index_penalty = -self._safe_int(audio.get("Index"), 9999)

                    # tuple 按顺序比较：用户选择的大类优先级 > 参数命中数 > 原始默认 > 码率 > 原始顺序。
                    return (
                        *ordered_category_scores,
                        matched_param_count,
                        1 if audio.get("IsDefault") else 0,
                        bitrate,
                        index_penalty,
                    )

                # 按排序键降序排列，取最高者作为默认音轨
                sorted_audios = sorted(candidates, key=get_audio_score, reverse=True)
                default_audio = sorted_audios[0]

                for s in audio_streams:
                    is_target = (s == default_audio)
                    s["IsDefault"] = is_target
                    
                    # ★★★ 核心修复：剥夺落选者的强制特权，防止造反 ★★★
                    if not is_target:
                        s["IsForced"] = False

                    # 仅修改 DisplayTitle，Title 保持不变
                    dt = re.sub(r'\(默认\s*', '(', s.get("DisplayTitle", ""))
                    dt = dt.replace('(默认)', '').replace('默认', '').replace('()', '').strip()
                    if is_target:
                        dt += " (默认)"
                    s["DisplayTitle"] = dt.replace("  ", " ")

            # -----------------------------------------
            # 2. 决出默认字幕 (智能跟随最高优 + 用户自定义优先级打分)
            # -----------------------------------------
            if sub_streams:
                default_sub = None
                audio_title = (default_audio.get("Title", "") + " " + default_audio.get("DisplayTitle", "")) if default_audio else ""
                audio_title_lower = audio_title.lower()
                
                # 提取真太子音轨命中的特征词
                active_audio_features = []
                for kw in audio_features_config:
                    if kw.lower() in audio_title_lower:
                        active_audio_features.append(kw.lower())

                # ★ 新增：动态提取音轨中的中文特征词（解决“中译公映”无法匹配的问题）
                if default_audio:
                    # 剔除常见无意义词汇，提取独特的中文描述
                    clean_audio_title = re.sub(r"(默认|特效|双语|简英|繁英|简体|繁体|中英|声道|音轨)", "", audio_title)
                    chinese_chunks = re.findall(r'[\u4e00-\u9fa5]{2,}', clean_audio_title)
                    for chunk in chinese_chunks:
                        if chunk.lower() not in active_audio_features:
                            active_audio_features.append(chunk.lower())

                # ★ 核心打分函数
                # 关键原则：subtitle_lang 是“语言大方向”，effect 只是“字幕特征”。
                # 也就是说：用户选 chs 时，简体特效 > 繁体特效；不能让 effect 绕过简繁偏好。
                def _norm_sub_text(text):
                    text = str(text or "").lower()
                    text = text.replace("（", "(").replace("）", ")")
                    text = re.sub(r"[\._\-+/|\\\[\]【】]+", " ", text)
                    text = re.sub(r"\s+", " ", text).strip()
                    return text

                def _has_token(text, *tokens):
                    text = _norm_sub_text(text)
                    for token in tokens:
                        token = _norm_sub_text(token)
                        if not token:
                            continue
                        # 中文词直接包含判断；英文短码必须边界匹配，避免误伤其它单词。
                        if re.search(r"[\u4e00-\u9fa5]", token):
                            if token in text:
                                return True
                        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text):
                            return True
                    return False

                def _detect_sub_flags(sub):
                    sub_title = _norm_sub_text(" ".join([
                        sub.get("Title", ""),
                        sub.get("DisplayTitle", ""),
                        sub.get("DisplayLanguage", ""),
                        sub.get("Language", ""),
                    ]))

                    is_effect = _has_token(sub_title, "特效", "effect", "effects", "tx")

                    has_cht = _has_token(
                        sub_title,
                        "繁体", "繁體", "繁中", "cht", "tc", "big5", "zh tw", "zh hk", "zh hant"
                    )
                    has_chs = _has_token(
                        sub_title,
                        "简体", "簡體", "简中", "chs", "sc", "gb", "zh cn", "zh hans"
                    )

                    # 纯 chi/zho/zh 没有简繁标记时，按用户偏好兜底；没有偏好则默认简体。
                    if not has_chs and not has_cht and _has_token(sub_title, "chi", "zho", "zh", "中文"):
                        if subtitle_pref == "cht":
                            has_cht = True
                        else:
                            has_chs = True

                    has_eng = _has_token(sub_title, "eng", "en", "英文", "英语", "中英", "双语")
                    has_jpn = _has_token(sub_title, "jpn", "jp", "ja", "日文", "日语", "中日")
                    has_kor = _has_token(sub_title, "kor", "kr", "ko", "韩文", "韩语", "中韩")

                    # “中英双语（繁体）”同时含“中英”和“繁体”，必须判成 cht_eng，不能被“中英”偷渡成 chs_eng。
                    is_cht_eng = has_cht and has_eng
                    is_chs_eng = has_chs and has_eng and not has_cht
                    is_cht_jpn = has_cht and has_jpn
                    is_chs_jpn = has_chs and has_jpn and not has_cht
                    is_cht_kor = has_cht and has_kor
                    is_chs_kor = has_chs and has_kor and not has_cht

                    is_cht = has_cht and not (is_cht_eng or is_cht_jpn or is_cht_kor)
                    is_chs = has_chs and not (is_chs_eng or is_chs_jpn or is_chs_kor)

                    script = "cht" if has_cht else ("chs" if has_chs else "")

                    return {
                        "text": sub_title,
                        "script": script,
                        "is_effect": is_effect,
                        "is_chs": is_chs,
                        "is_cht": is_cht,
                        "is_chs_eng": is_chs_eng,
                        "is_cht_eng": is_cht_eng,
                        "is_chs_jpn": is_chs_jpn,
                        "is_cht_jpn": is_cht_jpn,
                        "is_chs_kor": is_chs_kor,
                        "is_cht_kor": is_cht_kor,
                    }

                def get_sub_score(sub):
                    score = 0
                    flags = _detect_sub_flags(sub)
                    sub_title = flags["text"]

                    # 优先级 0: 用户选择的默认字幕语言是硬偏好。
                    # effect / 双语 / 原始默认只能在同一语言池里竞争，不能让繁体越级打败简体。
                    if subtitle_pref == "chs":
                        if flags["script"] == "chs":
                            score += 10 ** 12  # 简体：绝对第一
                        elif flags["script"] == "cht":
                            score += 10 ** 11  # 繁体：绝对第二（没有简体时，碾压外语）
                    elif subtitle_pref == "cht":
                        if flags["script"] == "cht":
                            score += 10 ** 12  # 繁体：绝对第一
                        elif flags["script"] == "chs":
                            score += 10 ** 11  # 简体：绝对第二
                    else:
                        # 如果用户没选偏好，但它是中文，也给个基础高分，防止被外语抢走
                        if flags["script"] in ["chs", "cht"]:
                            score += 10 ** 11

                    # 优先级 1: 智能跟随音轨特征。
                    # 仍然低于 subtitle_lang，避免“繁体特效/公映字幕”越过用户指定的简体。
                    if active_audio_features and any(f in sub_title for f in active_audio_features):
                        score += 10 ** 10

                    # 优先级 2: 用户拖拽顺序。
                    # 顺序越靠前，权重越高；effect 是特征加分，不是语言替代品。
                    priority_score = 0
                    priority_weight_base = max(len(sub_priority), 1)
                    for idx, p_type in enumerate(sub_priority):
                        weight = (priority_weight_base - idx) * 100000

                        if p_type == "effect" and flags["is_effect"]:
                            priority_score += weight
                        elif p_type == "chs_eng" and flags["is_chs_eng"]:
                            priority_score += weight
                        elif p_type == "cht_eng" and flags["is_cht_eng"]:
                            priority_score += weight
                        elif p_type == "chs_jpn" and flags["is_chs_jpn"]:
                            priority_score += weight
                        elif p_type == "cht_jpn" and flags["is_cht_jpn"]:
                            priority_score += weight
                        elif p_type == "chs_kor" and flags["is_chs_kor"]:
                            priority_score += weight
                        elif p_type == "cht_kor" and flags["is_cht_kor"]:
                            priority_score += weight
                        elif p_type == "chs" and flags["is_chs"]:
                            priority_score += weight
                        elif p_type == "cht" and flags["is_cht"]:
                            priority_score += weight

                    score += priority_score

                    # 原本默认只做极低优先级兜底，不能推翻用户规则。
                    if sub.get("IsDefault"):
                        score += 1

                    return score

                # 按分数降序排列，取最高分作为默认字幕
                sorted_subs = sorted(sub_streams, key=get_sub_score, reverse=True)
                default_sub = sorted_subs[0]

                for s in sub_streams:
                    is_target = (s == default_sub)
                    s["IsDefault"] = is_target
                    
                    # ★★★ 核心修复：剥夺落选者的强制特权，防止造反 ★★★
                    if not is_target:
                        s["IsForced"] = False
                        
                    # 仅修改 DisplayTitle，Title 保持不变
                    dt = re.sub(r'\(默认\s*', '(', s.get("DisplayTitle", ""))
                    dt = dt.replace('(默认)', '').replace('默认', '').replace('()', '').strip()
                    if is_target: dt += " (默认)"
                    s["DisplayTitle"] = dt.replace("  ", " ")

        # 执行智能篡改 (不再需要传 pref_language_code)
        _set_smart_default_streams(media_streams)

        if not media_streams:
            return None

        media_source_info = {
            "Size": size,
            "Type": "Default",
            "Bitrate": bitrate,
            "Formats": [],
            "Chapters": [],
            "IsRemote": True,
            "Protocol": "File",
            "Container": container,
            "MediaStreams": media_streams,
            "RunTimeTicks": run_time_ticks,
            "RequiresClosing": False,
            "RequiresLooping": False,
            "RequiresOpening": False,
            "SupportsProbing": True,
            "IsInfiniteStream": False,
            "HasMixedProtocols": False,
            "SupportsDirectPlay": True,
            "RequiredHttpHeaders": {},
            "SupportsTranscoding": True,
            "SupportsDirectStream": True,
            "ReadAtNativeFramerate": False,
            "AddApiKeyToDirectStreamUrl": False
        }

        return [{
            "Chapters": chapters,
            "MediaSourceInfo": media_source_info
        }]

    def _fetch_and_parse_mediainfo(self, sha1, guessed_info=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None, file_node=None, silent_log=False):
        """
        通过 SHA1 获取真实的媒体信息，并转换为乐高重命名参数。

        唯一数据源策略：
        1. 优先直读本地 p115_mediainfo_cache 数据库；
        2. 本地没有时，才用 ffprobe 解析 115 直链；
        3. 解析成功后写回 p115_mediainfo_cache。

        pre_fetched_mediainfo / local_pre_fetched_mediainfo 参数仅为兼容旧调用保留，
        不再参与事实判断，避免内存缓存与数据库状态不一致。
        """
        if not sha1:
            return {}, False

        sha1 = str(sha1).strip().upper()
        raw_json = None
        is_center = False
        data_source = ""

        # 1. 本地 DB 是唯一真理：每次按 SHA1 直读 p115_mediainfo_cache。
        try:
            cached_text = _get_p115_cache_manager().get_mediainfo_cache_text(sha1)
            if cached_text:
                raw_json = json.loads(cached_text) if isinstance(cached_text, str) else cached_text
                data_source = "本地缓存(DB)"
                if not silent_log:
                    logger.debug(f"  ➜ [媒体信息] 命中本地 DB 缓存: {sha1[:8]}")
        except Exception as e:
            logger.warning(f"  ➜ 读取 p115_mediainfo_cache 失败: {sha1[:8]} -> {e}")

        # 2. 本地 DB 没有，最后才 ffprobe。彻底移除中心服务器路径，保留 ETK 格式化结果。
        if not raw_json and file_node:
            raw_json, raw_ffprobe = self._probe_mediainfo_with_ffprobe(
                file_node, sha1=sha1, silent_log=silent_log
            ) or (None, None)

            if raw_json:
                data_source = "ffprobe解析"
                _get_p115_cache_manager().save_mediainfo_cache(sha1, raw_json, raw_ffprobe)

        if not raw_json:
            return {}, False

        # 3. 开始解析 Emby 的真实数据
        info = {}
        try:
            if isinstance(raw_json, list) and len(raw_json) > 0:
                source_info = raw_json[0].get("MediaSourceInfo", raw_json[0])
            else:
                source_info = raw_json

            streams = source_info.get("MediaStreams", [])
            
            # ★ 核心修复：物理数据是唯一真理。只要存在流信息，强制接管这 6 个核心参数
            if streams:
                # 默认全部置空。如果下方没有解析出对应属性，就会用空值彻底洗掉文件名瞎猜的错误标签
                info = {
                    'resolution': '', 'codec': '', 'effect': '', 
                    'fps': '', 'audio': '', 'audio_count': ''
                }

                video_stream = next((s for s in streams if s.get("Type") == "Video"), None)
                audio_streams = [s for s in streams if s.get("Type") == "Audio"]

                if video_stream:
                    w = video_stream.get("Width", 0)
                    if w >= 3800: info['resolution'] = '2160p'
                    elif w >= 1900: info['resolution'] = '1080p'
                    elif w >= 1200: info['resolution'] = '720p'
                    elif w > 0: info['resolution'] = '480p' # 补充低分辨率兜底，防止伪 4K 逃脱

                    codec_raw = video_stream.get("Codec", "").lower()
                    codec_map = {'hevc': 'HEVC', 'h265': 'HEVC', 'h264': 'AVC', 'avc': 'AVC', 'av1': 'AV1'}
                    c_str = codec_map.get(codec_raw, codec_raw.upper())
                    
                    bit_depth = video_stream.get("BitDepth")
                    if bit_depth and bit_depth > 8:
                        info['codec'] = f"{c_str} {bit_depth}bit"
                    else:
                        info['codec'] = c_str

                    v_range = video_stream.get("VideoRange", "")
                    ext_type = video_stream.get("ExtendedVideoType", "")
                    ext_sub_type = video_stream.get("ExtendedVideoSubType", "")
                    ext_desc = video_stream.get("ExtendedVideoSubTypeDescription", "")

                    is_dv = "DolbyVision" in v_range or "DolbyVision" in ext_type
                    
                    hdr_str = ""
                    if "HDR10+" in v_range or "HDR10+" in ext_desc: hdr_str = "HDR10+"
                    elif "HDR10" in v_range or "HDR10" in ext_desc: hdr_str = "HDR10"
                    elif "HDR" in v_range or video_stream.get("ColorTransfer") == "smpte2084": hdr_str = "HDR"

                    dv_str = "DV"
                    if is_dv:
                        if "Profile8" in ext_sub_type or "Profile 8" in ext_desc: dv_str = "DoVi P8"
                        elif "Profile7" in ext_sub_type or "Profile 7" in ext_desc: dv_str = "DoVi P7"
                        elif "Profile5" in ext_sub_type or "Profile 5" in ext_desc: dv_str = "DoVi P5"
                        else: dv_str = "DoVi"

                    if is_dv and hdr_str: info['effect'] = f"{hdr_str} {dv_str}"
                    elif is_dv: info['effect'] = dv_str
                    elif hdr_str: info['effect'] = hdr_str

                    fps = video_stream.get("RealFrameRate") or video_stream.get("AverageFrameRate")
                    if fps: info['fps'] = f"{round(fps)}fps"

                # ★ 提取真实的音轨和字幕语言数组，供洗版裁判使用
                info['audio_langs'] = []
                info['sub_langs'] = []
                
                def _extract_lang_from_stream(stream_dict):
                    # 1. 优先取 Language 字段
                    lang = stream_dict.get("Language")
                    if lang: return lang
                    
                    # 2. 其次从 Title 或 DisplayTitle 中模糊匹配
                    text_to_search = f"{stream_dict.get('Title', '')} {stream_dict.get('DisplayTitle', '')}".lower()
                    if 'chi' in text_to_search or 'zh' in text_to_search or '中' in text_to_search or '国语' in text_to_search or '粤语' in text_to_search:
                        return 'chi'
                    if 'eng' in text_to_search or 'en' in text_to_search or '英' in text_to_search:
                        return 'eng'
                    if 'jpn' in text_to_search or 'ja' in text_to_search or '日' in text_to_search:
                        return 'jpn'
                    if 'kor' in text_to_search or 'ko' in text_to_search or '韩' in text_to_search:
                        return 'kor'
                    return None

                for s in streams:
                    if s.get("Type") == "Audio":
                        l = _extract_lang_from_stream(s)
                        if l: info['audio_langs'].append(l)
                    elif s.get("Type") == "Subtitle":
                        l = _extract_lang_from_stream(s)
                        if l: info['sub_langs'].append(l)

                if audio_streams:
                    audio_tags = []
                    
                    # ★ 如果这里只有 1 条音轨，info['audio_count'] 会保持上面的空字符串，从而洗掉文件名的 "2Audios"
                    num_audios = len(audio_streams)
                    if num_audios > 1: 
                        info['audio_count'] = f"{num_audios}Audios"

                    primary_audio = next((s for s in audio_streams if s.get("IsDefault")), audio_streams[0])
                    acodec = primary_audio.get("Codec", "").lower()
                    profile = primary_audio.get("Profile", "").lower()

                    if acodec == 'truehd' and 'atmos' in profile: audio_tags.append("TrueHD Atmos")
                    elif acodec == 'truehd': audio_tags.append("TrueHD")
                    elif acodec == 'dts' and 'ma' in profile: audio_tags.append("DTS-HD MA")
                    elif acodec == 'dts': audio_tags.append("DTS")
                    elif acodec == 'eac3': audio_tags.append("DDP")
                    elif acodec == 'ac3': audio_tags.append("AC3")
                    elif acodec == 'aac': audio_tags.append("AAC")
                    elif acodec == 'flac': audio_tags.append("FLAC")

                    channels = primary_audio.get("Channels")
                    if channels == 8: audio_tags.append("7.1")
                    elif channels == 6: audio_tags.append("5.1")
                    elif channels == 2: audio_tags.append("2.0")

                    if audio_tags:
                        info['audio'] = " ".join(audio_tags)

        except Exception as e:
            logger.warning(f"  ➜ 解析真实媒体信息失败: {e}")

        return info, is_center