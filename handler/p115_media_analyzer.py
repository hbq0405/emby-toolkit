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

            return emby_json

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
            or settings_db.get_setting("stream_feature_mapping")
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
        """
        base_title = str(base_title or "").strip()
        features = list(dict.fromkeys([f for f in features if f]))

        if not base_title:
            base_title = "未知"

        # “特效”更适合贴在字幕类型后面，而不是放括号里
        if "特效" in features and base_title in ["简体", "繁体", "简英双语", "繁英双语", "中英双语（简体）", "中英双语（繁体）", "英文", "英语"]:
            if base_title == "中英双语（简体）":
                base_title = "中英双语特效（简体）"
            elif base_title == "中英双语（繁体）":
                base_title = "中英双语特效（繁体）"
            else:
                base_title = f"{base_title}特效"
            features = [f for f in features if f != "特效"]

        if features:
            return f"{base_title}（{'·'.join(features)}）"

        return base_title

    def _get_friendly_display_info(
        self,
        raw_lang,
        raw_title,
        stream_type,
        raw_display_title="",
        is_hearing_impaired=False
    ):
        """
        返回：(底层 ISO 代码，UI 主标题/DisplayLanguage，UI 副标题/Title)

        设计原则：
        1. Language 负责底层语言码。
        2. Title 优先用于识别简繁、双语、特效、压制组、地区、SDH 等信息。
        3. DEFAULT_LANGUAGE_MAPPING 只管语言。
        4. DEFAULT_STREAM_FEATURE_MAPPING 只管 DYSY / TX / SDH / 拉美 / 巴西 / 导评 这类非语言特征。
        """

        # 防御性检查，防止 language_map 未初始化
        if not hasattr(self, "language_map") or not self.language_map:
            self.language_map = settings_db.get_setting("language_mapping") or utils.DEFAULT_LANGUAGE_MAPPING

        raw_lang = str(raw_lang or "").strip()
        raw_title = str(raw_title or "").strip()
        raw_display_title = str(raw_display_title or "").strip()
        stream_type = str(stream_type or "").strip()

        title_lower = raw_title.lower()
        lang_lower = raw_lang.lower()

        def _normalize_marker_text(text):
            text = str(text or "").lower()
            text = re.sub(r"[\.\-_+/|\\\[\]\(\)【】（）]+", " ", text)
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
        def _lookup_base_label(norm_lang):
            if not norm_lang:
                return ""

            norm_lang_lower = str(norm_lang).lower()

            for item in self.language_map:
                value = str(item.get("value", "")).lower()
                aliases = [str(a).lower() for a in item.get("aliases", [])]

                if norm_lang_lower == value or norm_lang_lower in aliases:
                    return item.get("label") or ""

            return str(norm_lang).upper()

        def _display_label_from_base_label(base_label, stream_type):
            if not base_label or base_label == "未知":
                return base_label

            if stream_type == "Subtitle":
                if base_label in ["国语", "普通话", "中文"]:
                    return "简体"
                if base_label in ["粤语", "广东话"]:
                    return "繁体"
                if base_label.endswith("语") and base_label != "无语言":
                    return base_label[:-1] + "文"

            return base_label

        # =========================================================
        # 1. 判断 Title 是否包含明确语言信息
        #    注意：这里必须用 _has_lang_marker，不能裸 in，否则 Deutsch 会命中 sc。
        # =========================================================
        title_clean = _normalize_marker_text(raw_title)

        title_has_lang = _has_lang_marker(title_clean, [
            "chs", "sc", "gb", "zh cn", "zh hans", "简中", "简体", "简英",
            "cht", "tc", "big5", "zh tw", "zh hk", "zh hant", "繁中", "繁体", "繁英",
            "eng", "english", "en", "英文", "英语", "英字",
            "台配", "台灣", "台湾"
        ])

        if not title_has_lang:
            for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                if _has_lang_marker(title_clean, keywords):
                    title_has_lang = True
                    break

        if title_has_lang:
            # Title 已经有语言信息，避免 raw_lang 干扰
            lang_lower_for_detect = ""
            norm_lang = helpers.normalize_lang_code(raw_title)
        else:
            lang_lower_for_detect = lang_lower
            norm_lang = helpers.normalize_lang_code(raw_lang)

        combined_text = f"{title_lower} {lang_lower_for_detect}".strip()
        clean_text = _normalize_marker_text(combined_text)

        display_lang = ""

        # =========================================================
        # 2. 字幕流：字幕底层中文统一 chi，靠 Title 区分简繁/双语
        # =========================================================
        if stream_type == "Subtitle":
            has_chs = _has_lang_marker(clean_text, [
                "chs", "sc", "gb", "zh cn", "zh hans", "简中", "简体", "简英", "中英", "中文", "中上英下", "英上中下"
            ])

            has_cht = _has_lang_marker(clean_text, [
                "cht", "tc", "big5", "zh tw", "zh hk", "zh hant", "繁中", "繁体", "繁英",
                "繁上英下", "英上繁下"
            ])

            has_eng = _has_lang_marker(clean_text, [
                "eng", "english", "en", "英文", "英语", "英字", "简英", "繁英", "中英", "双语",
                "中上英下", "英上中下", "繁上英下", "英上繁下"
            ])

            is_dual = _has_lang_marker(clean_text, ["双语", "中上英下", "英上中下", "繁上英下", "英上繁下"])

            if (has_chs and has_eng and not has_cht) or (is_dual and not has_cht):
                norm_lang = "chi"
                display_lang = "中英双语（简体）"
            elif (has_cht and has_eng) or (is_dual and has_cht):
                norm_lang = "chi"
                display_lang = "中英双语（繁体）"
            elif has_cht:
                norm_lang = "chi"
                display_lang = "繁体"
            elif has_chs:
                norm_lang = "chi"
                display_lang = "简体"
            elif has_eng:
                norm_lang = "eng"
                display_lang = "英文"
            else:
                is_yue = _has_lang_marker(
                    combined_text,
                    helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_yue", [])
                )
                is_chi = _has_lang_marker(
                    combined_text,
                    helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_chi", [])
                )

                if _has_lang_marker(combined_text, ["台配", "台灣", "台湾"]):
                    norm_lang = "chi"
                    display_lang = "繁体"
                elif is_yue:
                    # 字幕里的粤语标签通常意味着繁体中文字幕
                    norm_lang = "chi"
                    display_lang = "繁体"
                elif is_chi:
                    norm_lang = "chi"
                    display_lang = "简体"
                else:
                    # 外语字幕，例如 Deutsch / Japanese / Korean
                    for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                        if _has_lang_marker(combined_text, keywords):
                            norm_lang = key.replace("sub_", "")
                            break

        # =========================================================
        # 3. 音轨流：国语/粤语要保留发音差异
        # =========================================================
        else:
            is_yue = _has_lang_marker(
                combined_text,
                helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("yue", [])
            )
            is_chi = _has_lang_marker(
                combined_text,
                helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("chi", [])
            )

            if is_yue and not is_chi:
                norm_lang = "yue"
            elif is_chi and not is_yue:
                norm_lang = "chi"
            else:
                for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                    if key.startswith("sub_"):
                        continue

                    if _has_lang_marker(combined_text, keywords):
                        norm_lang = key.replace("sub_", "")
                        break

        # =========================================================
        # 4. 生成 display_lang
        # =========================================================
        if not display_lang:
            base_label = _lookup_base_label(norm_lang)
            display_lang = _display_label_from_base_label(base_label, stream_type)

        if not display_lang:
            display_lang = "未知"

        # =========================================================
        # 5. 提取非语言特征：DYSY / TX / SDH / 拉美 / 巴西 / 导评
        # =========================================================
        stream_features = self._extract_stream_features(
            stream_type,
            raw_title,
            raw_display_title,
            raw_lang
        )

        # IsHearingImpaired 也强制视为 SDH
        if stream_type == "Subtitle" and is_hearing_impaired is True:
            if "SDH" not in stream_features:
                stream_features.append("听障")

        stream_features = list(dict.fromkeys([f for f in stream_features if f]))

        friendly_title = raw_title

        # =========================================================
        # 6. 字幕 Title 处理
        # =========================================================
        if stream_type == "Subtitle":
            friendly_title = raw_title

            # 1. 预处理：去掉“画面内简中（iTunes）”这种多余前缀
            friendly_title = re.sub(r"画面内.*?（.*?）", "", friendly_title)
            
            # 2. 暴力净化：听从建议，直接使用 clean_non_chinese_chars 碾碎所有 SUP/ASS/Chs 等英文污染
            friendly_title = utils.clean_non_chinese_chars(friendly_title)

            # 3. 替换常见词
            replace_map = {
                "简中": "简体",
                "简体中文": "简体",
                "中文(简体)": "简体",
                "中文（简体）": "简体",
                "繁中": "繁体",
                "繁体中文": "繁体",
                "繁體中文": "繁体",
                "中文(繁体)": "繁体",
                "中文（繁體）": "繁体",
            }
            for old, new in replace_map.items():
                friendly_title = friendly_title.replace(old, new)
                
            # 4. 修复双语标签
            friendly_title = friendly_title.replace("简英双语", "中英双语（简体）").replace("简英", "中英双语（简体）")
            friendly_title = friendly_title.replace("繁英双语", "中英双语（繁体）").replace("繁英", "中英双语（繁体）")
            friendly_title = friendly_title.replace("中英双语（简体）双语", "中英双语（简体）")
            friendly_title = friendly_title.replace("中英双语（繁体）双语", "中英双语（繁体）")
            friendly_title = friendly_title.replace("中上英下", "中英双语（简体）").replace("英上中下", "中英双语（简体）")
            
            # 5. 兜底与组合
            if not friendly_title:
                # 如果清理后变为空（例如原标题全是英文），靠特征词和 display_lang 兜底
                if display_lang and display_lang != "未知" and stream_features:
                    friendly_title = self._format_stream_feature_title(display_lang, stream_features)
                else:
                    friendly_title = display_lang if display_lang and display_lang != "未知" else raw_title
            else:
                # 如果清理后还有内容（比如“国配简体特效对应中译公映”），完美保留！
                if display_lang in ["简体", "繁体", "中英双语（简体）", "中英双语（繁体）"]:
                    check_kw = "简" if "简" in display_lang else ("繁" if "繁" in display_lang else "")
                    if check_kw and check_kw not in friendly_title:
                        friendly_title = f"{friendly_title} ({display_lang})"

        # =========================================================
        # 7. 音轨 Title 处理
        # =========================================================
        elif stream_type == "Audio":
            friendly_title = raw_title

            # 1. 暴力净化：碾碎所有 DTS-HD, Dolby, kbps 等非中文字符
            friendly_title = utils.clean_non_chinese_chars(friendly_title)

            # 2. 替换常见词
            audio_replace_map = {
                "国语配音": "国语",
                "粤语配音": "粤语",
                "视障口述": "视障口述",
            }
            for old, new in audio_replace_map.items():
                friendly_title = friendly_title.replace(old, new)

            # 3. 兜底与组合
            if not friendly_title:
                if display_lang and display_lang != "未知" and stream_features:
                    friendly_title = self._format_stream_feature_title(display_lang, stream_features)
                else:
                    friendly_title = display_lang if display_lang and display_lang != "未知" else raw_title
            else:
                if display_lang and display_lang != "未知":
                    # 移除开头多余的 display_lang (例如 "国语中译公映国语" -> "中译公映国语")
                    friendly_title = re.sub(rf"^{display_lang}", "", friendly_title)
                    
                    if not friendly_title:
                        friendly_title = display_lang
                    else:
                        # 如果有特色词，比如 "台配"，组合成 "国语（台配）"
                        if display_lang not in friendly_title:
                            friendly_title = f"{display_lang}（{friendly_title}）"

        # =========================================================
        # 8. 其他流兜底
        # =========================================================
        else:
            if display_lang and display_lang != "未知":
                if not friendly_title or not utils.contains_chinese(friendly_title):
                    friendly_title = display_lang

        # =========================================================
        # 9. 冗余标题兜底
        # =========================================================
        redundant_exact_matches = {
            "yue", "cn", "cht", "tc", "chi", "zho", "zh", "chs", "sc",
            "粵語", "國語", "粤语", "国语", "简中", "繁中", "简体", "繁体",
            "中文", "英语", "英文", "english", "korean", "韩语", "韩文",
            "中文(简体)", "中文（简体）", "简体中文",
            "中文(繁体)", "中文（繁體）", "繁体中文", "繁體中文",
            "simplified", "traditional", "simplified(简体)", "traditional(繁体)",
            "简英双语", "繁英双语", "中英双语（简体）", "中英双语（繁体）"
        }

        if (
            not friendly_title
            or friendly_title.lower().replace(" ", "") in redundant_exact_matches
            or friendly_title.lower() == raw_lang.lower()
        ):
            friendly_title = display_lang if display_lang and display_lang != "未知" else raw_title

        if not norm_lang:
            norm_lang = raw_lang

        return norm_lang, display_lang, friendly_title

    def _channel_layout_label(self, channels, channel_layout=None):
        channel_layout = (channel_layout or "").lower()

        if channels == 8:
            return "7.1"
        if channels == 7:
            return "6.1"
        if channels == 6:
            return "5.1"
        if channels == 2:
            return "stereo"
        if channels == 1:
            return "mono"

        if channel_layout:
            return channel_layout.replace("(side)", "")

        return str(channels) if channels else ""

    def _audio_codec_profile_label(self, codec, profile="", title=""):
        codec = (codec or "").lower()
        profile_mix = f"{profile or ''} {title or ''}".lower()

        if codec == "truehd":
            return "TRUEHD Atmos" if "atmos" in profile_mix else "TRUEHD"

        if codec == "eac3":
            return "DDP Atmos" if "atmos" in profile_mix else "DDP"

        if codec == "dts":
            if "ma" in profile_mix or "master" in profile_mix or "xll" in profile_mix:
                return "DTS-HD MA"
            if "hra" in profile_mix or "high resolution" in profile_mix:
                return "DTS-HD HRA"
            return "DTS"

        if codec == "ac3":
            return "AC3"
        if codec == "aac":
            return "AAC"
        if codec == "flac":
            return "FLAC"
        if codec == "opus":
            return "OPUS"
        if codec == "mp3":
            return "MP3"

        return codec.upper() if codec else ""

    def _subtitle_codec_label(self, codec):
        codec = (codec or "").lower()

        mapping = {
            "hdmv_pgs_subtitle": "PGSSUB",
            "pgssub": "PGSSUB",
            "subrip": "SUBRIP",
            "srt": "SUBRIP",
            "ass": "ASS",
            "ssa": "SSA",
            "webvtt": "VTT",
            "mov_text": "MOV_TEXT",
            "dvd_subtitle": "DVDSUB",
        }

        return mapping.get(codec, codec.upper() if codec else "")

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
                    "ExtendedVideoSubType": extended_video_sub_type,
                    "IsTextSubtitleStream": False,
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": extended_video_desc
                })

            elif codec_type == "audio":
                raw_lang = tags.get("language")
                raw_title = tags.get("title")
                
                # ★ 调用新的智能解析方法
                lang, display_lang, title = self._get_friendly_display_info(raw_lang, raw_title, "Audio")

                channels = self._safe_int(s.get("channels"))
                channel_layout = self._channel_layout_label(channels, s.get("channel_layout"))
                sample_rate = self._safe_int(s.get("sample_rate"))

                profile = s.get("profile") or ""
                codec_display = self._audio_codec_profile_label(codec, profile, title)

                display_title_parts = []
                if display_lang and display_lang != "未知":
                    display_title_parts.append(display_lang)
                if codec_display:
                    display_title_parts.append(codec_display)
                if channel_layout:
                    display_title_parts.append(channel_layout)
                if is_default:
                    display_title_parts.append("(默认)")

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Audio",
                    "Codec": codec,
                    "Index": index,
                    "Title": title, # ★ 净化后的副标题
                    "BitRate": self._safe_int(s.get("bit_rate")),
                    "BitDepth": self._safe_int(s.get("bits_per_raw_sample") or s.get("bits_per_sample")),
                    "Channels": channels,
                    "IsForced": is_forced,
                    "Language": lang, # ★ 伪装后的底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "SampleRate": sample_rate,
                    "DisplayTitle": display_title, # ★ 完美的 UI 标题 (如: 国语 AAC stereo (默认))
                    "IsInterlaced": False,
                    "ChannelLayout": channel_layout,
                    "AttachmentSize": 0,
                    "DisplayLanguage": display_lang, # ★ 完美的 UI 语言 (如: 国语)
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
                
                # ★ 调用新的智能解析方法
                lang, display_lang, title = self._get_friendly_display_info(raw_lang, raw_title, "Subtitle", raw_display_title, is_hearing_impaired)

                sub_codec = self._subtitle_codec_label(codec)
                is_text_sub = codec in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"}

                display_title_parts = []
                if display_lang and display_lang != "未知":
                    display_title_parts.append(display_lang)
                if is_default:
                    display_title_parts.append(f"(默认 {sub_codec})")
                else:
                    display_title_parts.append(f"({sub_codec})")

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Subtitle",
                    "Codec": sub_codec,
                    "Index": index,
                    "Title": title, # ★ 净化后的副标题
                    "IsForced": is_forced,
                    "Language": lang, # ★ 伪装后的底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "DisplayTitle": display_title, # ★ 完美的 UI 标题 (如: 简中 (默认 SRT))
                    "IsInterlaced": False,
                    "AttachmentSize": 0,
                    "DisplayLanguage": display_lang, # ★ 完美的 UI 语言 (如: 简中)
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
                "audio_features": ["国配", "上译", "京译", "长译", "八一", "台配", "粤语", "评论", "导评"],
                "sub_priority": ["effect", "chs_eng", "cht_eng", "chs", "cht"]
            }
            audio_pref_code = stream_config.get("audio_lang", "")
            subtitle_pref = stream_config.get("subtitle_lang", "")
            audio_features_config = stream_config.get("audio_features", [])
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

                # ★ 新增：根据用户拖拽的 audio_features 优先级打分
                def get_audio_score(audio):
                    score = 0
                    title_lower = (audio.get("Title", "") + " " + audio.get("DisplayTitle", "")).lower()
                    
                    # 遍历特征词，越靠前的特征词加分越高
                    for idx, kw in enumerate(reversed(audio_features_config)):
                        weight = (idx + 1) * 1000
                        if kw.lower() in title_lower:
                            score = max(score, weight)
                    
                    # 原本默认的给 1 分兜底
                    if audio.get("IsDefault"):
                        score += 1
                        
                    return score

                # 按分数降序排列，取最高分作为默认音轨
                sorted_audios = sorted(candidates, key=get_audio_score, reverse=True)
                default_audio = sorted_audios[0]

                for s in audio_streams:
                    is_target = (s == default_audio)
                    s["IsDefault"] = is_target
                    
                    # ★★★ 核心修复：剥夺落选者的强制特权，防止造反 ★★★
                    if not is_target:
                        s["IsForced"] = False

                    import re
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
                    clean_audio_title = re.sub(r"(国语|粤语|英语|日语|韩语|默认|特效|双语|简英|繁英|简体|繁体|中英|声道|音轨)", "", audio_title)
                    chinese_chunks = re.findall(r'[\u4e00-\u9fa5]{2,}', clean_audio_title)
                    for chunk in chinese_chunks:
                        if chunk.lower() not in active_audio_features:
                            active_audio_features.append(chunk.lower())

                # ★ 核心打分函数
                def get_sub_score(sub):
                    score = 0
                    sub_title = (sub.get("Title", "") + " " + sub.get("DisplayTitle", "")).lower()
                    codec = sub.get("Codec", "").upper()

                    # 统一特征判断
                    is_effect = (
                        "特效" in sub_title
                        or "effect" in sub_title
                        or "effects" in sub_title
                    )

                    is_chs_eng = (
                        "简英" in sub_title
                        or "中英" in sub_title
                        or "chs/eng" in sub_title
                        or "chs&eng" in sub_title
                        or "chs.eng" in sub_title
                    )

                    is_cht_eng = (
                        "繁英" in sub_title
                        or "cht/eng" in sub_title
                        or "cht&eng" in sub_title
                        or "cht.eng" in sub_title
                    )

                    is_chs = (
                        "简体" in sub_title
                        or "简中" in sub_title
                        or "chs" in sub_title
                    ) and not is_chs_eng

                    is_cht = (
                        "繁体" in sub_title
                        or "繁中" in sub_title
                        or "cht" in sub_title
                    ) and not is_cht_eng

                    # 优先级 1: 智能跟随音轨特征
                    if active_audio_features and any(f in sub_title for f in active_audio_features):
                        score += 10000000  # 加大到一千万，确保绝对压制

                    # 优先级 2: 用户拖拽顺序 (★ 升级为指数级叠加打分)
                    priority_score = 0
                    for idx, p_type in enumerate(reversed(sub_priority)):
                        # 使用 10 的指数级权重 (100, 1000, 10000, 100000...)
                        # 确保排在前面的属性具有绝对统治力，同时允许属性叠加！
                        weight = 10 ** (idx + 2) 

                        if p_type == "effect" and is_effect:
                            priority_score += weight
                        elif p_type == "chs_eng" and is_chs_eng:
                            priority_score += weight
                        elif p_type == "cht_eng" and is_cht_eng:
                            priority_score += weight
                        elif p_type == "chs" and is_chs:
                            priority_score += weight
                        elif p_type == "cht" and is_cht:
                            priority_score += weight

                    score += priority_score

                    # 字幕简繁偏好只能做小加分，不能推翻拖拽排序
                    if subtitle_pref:
                        if subtitle_pref == "chs" and (is_chs or is_chs_eng):
                            score += 50
                        elif subtitle_pref == "cht" and (is_cht or is_cht_eng):
                            score += 50

                    # 原本默认只做极小兜底，不能压过用户排序
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
                        
                    import re
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
        通过 SHA1 获取真实的媒体信息，并转换为乐高重命名参数
        """
        if not sha1: return {}, False
        
        raw_json = None
        is_center = False
        data_source = "本地缓存"

        # 1. ★ 核心优化：直接从内存字典读取本地缓存，彻底消除数据库 I/O 瓶颈！
        if local_pre_fetched_mediainfo and sha1 in local_pre_fetched_mediainfo:
            raw_json = local_pre_fetched_mediainfo[sha1]

        # 2. 本地没有，优先查批量预获取的字典 (瞬间读取，无网络延迟)
        if not raw_json and pre_fetched_mediainfo and sha1 in pre_fetched_mediainfo:
            raw_json = pre_fetched_mediainfo[sha1]
            is_center = True
            data_source = "中心服务器(批量)"

        # 3. 尝试查 P115Center 中心服务器 (单次查询)
        if not raw_json and pre_fetched_mediainfo is None:
            try:
                import extensions
                processor = extensions.media_processor_instance
                if processor and getattr(processor, 'p115_center', None):
                    resp = processor.p115_center.download_emby_mediainfo_data([sha1])
                    if resp and sha1 in resp:
                        raw_json = resp[sha1]
                        is_center = True
                        data_source = "中心服务器(单次)"
                        _get_p115_cache_manager().save_mediainfo_cache(sha1, raw_json)
            except Exception:
                pass

        # 4. 本地和中心服务器都没有，最终用 ffprobe 解析 115 直链，并写入本地缓存
        if not raw_json and file_node:
            raw_json = self._probe_mediainfo_with_ffprobe(
                file_node,
                sha1=sha1,
                silent_log=silent_log
            )

            if raw_json:
                is_center = False
                data_source = "ffprobe解析"

                # 写入 p115_mediainfo_cache，后续同 SHA1 直接走本地缓存
                _get_p115_cache_manager().save_mediainfo_cache(sha1, raw_json)

                # 同步塞回本轮预取字典，避免同一批里重复 probe
                if local_pre_fetched_mediainfo is not None:
                    local_pre_fetched_mediainfo[str(sha1).upper()] = raw_json
                    local_pre_fetched_mediainfo[str(sha1)] = raw_json

        if not raw_json:
            return {}, False

        # 5. 开始解析 Emby 的真实数据
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
