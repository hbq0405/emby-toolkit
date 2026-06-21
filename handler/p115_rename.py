import logging
import os
import re

import utils
from jinja2.sandbox import SandboxedEnvironment


logger = logging.getLogger(__name__)

_JINJA_ENV = SandboxedEnvironment(autoescape=False)


class P115RenameRenderer:
    _P115_INVALID_NAME_CHARS_RE = re.compile(r'[\\/:*?"<>|]')

    def __init__(self, details=None, tmdb_id="", original_title=""):
        self.details = details or {}
        self.tmdb_id = tmdb_id
        self.original_title = original_title

    @staticmethod
    def get_format(config, kind, fallback):
        cfg = config if isinstance(config, dict) else {}
        template = cfg.get(f"{kind}_template")
        if isinstance(template, str) and template.strip():
            return template
        return cfg.get(f"{kind}_format", fallback)

    @staticmethod
    def normalize_mp_jinja_template(template):
        if not isinstance(template, str):
            return ""
        return re.sub(
            r"{{\s*([A-Za-z_]\w*)\s*\|\s*string\s*}\s*\.zfill\((\d+)\)\s*}}",
            r"{{ (\1|string).zfill(\2) }}",
            template,
        )

    @staticmethod
    def template_uses_file_ext(format_value):
        if not isinstance(format_value, str):
            return False
        return bool(re.search(r"\b(fileExt|file_ext)\b", format_value))

    @classmethod
    def sanitize_name_component(cls, text):
        cleaned = utils.clean_invisible_chars(text)
        cleaned = cls._P115_INVALID_NAME_CHARS_RE.sub('', cleaned).strip()
        return cleaned

    @staticmethod
    def sanitize_rendered_template(text):
        cleaned = utils.clean_invisible_chars(text).replace("\\", "/")
        cleaned = re.sub(r'[\r\n\t]+', ' ', cleaned)
        return re.sub(r'[:*?"<>|]', '', cleaned).strip()

    def build_template_context(self, is_tv=False, season_num=None, episode_num=None, original_title=None, video_info=None, safe_title=None, file_ext=""):
        video_info = video_info or {}
        date_text = str(self.details.get('date') or '')
        year = date_text[:4] if date_text else ''
        title_zh = self.sanitize_name_component(safe_title if safe_title else (self.details.get('title') or self.original_title))
        title_en = self.sanitize_name_component(self.details.get('title_en') or original_title or self.details.get('original_title') or self.original_title)
        title_orig = self.sanitize_name_component(original_title or self.details.get('original_title') or self.original_title)
        season_val = season_num if season_num is not None else (1 if is_tv else None)
        episode_val = episode_num if episode_num is not None else (1 if is_tv else None)
        season_no = f"{season_val:02d}" if season_val is not None else ""
        episode_no = f"{episode_val:02d}" if episode_val is not None else ""
        season_episode = f"S{season_no}E{episode_no}" if is_tv and season_no and episode_no else ""
        season_name_zh = f"第 {season_val} 季" if is_tv and season_val is not None else ""
        episode_name_zh = f"第 {episode_val} 集" if is_tv and episode_val is not None else ""
        season_episode_zh = f"第 {season_val} 季 {episode_val} 集" if is_tv and season_val is not None and episode_val is not None else ""
        source = video_info.get('source') or ''
        effect = video_info.get('effect') or ''
        codec = video_info.get('codec') or ''
        audio = video_info.get('audio') or ''
        group = video_info.get('group') or ''
        ext_with_dot = f".{str(file_ext).lstrip('.')}" if file_ext else ""

        return {
            # ETK names
            'title': title_zh,
            'title_zh': title_zh,
            'title_en': title_en,
            'title_orig': title_orig,
            'original_title': title_orig,
            'year': year,
            'tmdb': self.tmdb_id,
            'tmdb_id': self.tmdb_id,
            'tmdbid': self.tmdb_id,
            'season': season_val,
            'episode': episode_val,
            'season_no': season_no,
            'episode_no': episode_no,
            's_e': season_episode,
            'season_episode': season_episode,
            'season_name': f"Season {season_no}" if season_no else "",
            'season_name_zh': season_name_zh,
            'episode_name_zh': episode_name_zh,
            's_e_zh': season_episode_zh,
            'season_episode_zh': season_episode_zh,
            'resolution': video_info.get('resolution') or '',
            'source': source,
            'stream': video_info.get('stream') or '',
            'effect': effect,
            'codec': codec,
            'audio_count': video_info.get('audio_count') or '',
            'audio': audio,
            'fps': video_info.get('fps') or '',
            'group': group,
            'original_name': safe_title or title_zh,
            'file_ext': ext_with_dot,
            'fileExt': ext_with_dot,

            # MoviePilot-compatible aliases
            'videoFormat': source,
            'videoCodec': codec,
            'audioCodec': audio,
            'releaseGroup': group,
            'customization': effect,
            'edition': video_info.get('edition') or '',
        }

    def render_template(self, template, is_tv=False, season_num=None, episode_num=None, original_title=None, video_info=None, safe_title=None, file_ext=""):
        template = self.normalize_mp_jinja_template(template)
        if not template.strip():
            return ""
        ctx = self.build_template_context(
            is_tv=is_tv,
            season_num=season_num,
            episode_num=episode_num,
            original_title=original_title,
            video_info=video_info,
            safe_title=safe_title,
            file_ext=file_ext,
        )
        try:
            rendered = _JINJA_ENV.from_string(template).render(**ctx)
        except Exception as e:
            logger.warning(f"  ➜ [重命名模板] Jinja2 渲染失败，已回退为空: {e}")
            return ""
        return self.sanitize_rendered_template(rendered) if rendered else ""

    def build_name(self, format_value, is_tv=False, season_num=None, episode_num=None, original_title=None, video_info=None, safe_title=None, file_ext=""):
        if not format_value:
            return ""

        if isinstance(format_value, str):
            return self.render_template(
                format_value,
                is_tv=is_tv,
                season_num=season_num,
                episode_num=episode_num,
                original_title=original_title,
                video_info=video_info,
                safe_title=safe_title,
                file_ext=file_ext,
            )

        evaluated = []
        for raw_id in format_value:
            block = raw_id.rsplit('_', 1)[0] if re.search(r'_\d+$', raw_id) else raw_id
            val = None
            is_sep = False

            if block == 'title_zh':
                raw_title = safe_title if safe_title else (self.details.get('title') or self.original_title)
                val = self.sanitize_name_component(raw_title)
            elif block == 'title_en':
                raw_title = self.details.get('title_en') or original_title or self.details.get('original_title') or self.original_title
                val = self.sanitize_name_component(raw_title)
            elif block == 'title_orig':
                raw_title = original_title or self.details.get('original_title') or self.original_title
                val = self.sanitize_name_component(raw_title)
            elif block == 'year':
                val = f"({self.details.get('date', '')[:4]})" if self.details.get('date') else None
            elif block == 'year_pure':
                val = self.details.get('date', '')[:4] if self.details.get('date') else None
            elif block == 'tmdb_bracket':
                val = f"{{tmdb={self.tmdb_id}}}"
            elif block == 'tmdb_square':
                val = f"[tmdbid={self.tmdb_id}]"
            elif block == 'tmdb_dash':
                val = f"tmdb-{self.tmdb_id}"
            elif block == 's_e' and is_tv:
                s_val = season_num if season_num is not None else 1
                e_val = episode_num if episode_num is not None else 1
                val = f"S{s_val:02d}E{e_val:02d}"
            elif block in ('episode_name_zh', 'episode_no_zh') and is_tv:
                e_val = episode_num if episode_num is not None else 1
                val = f"第 {e_val} 集"
            elif block in ('s_e_zh', 'season_episode_zh') and is_tv:
                s_val = season_num if season_num is not None else 1
                e_val = episode_num if episode_num is not None else 1
                val = f"第 {s_val} 季 {e_val} 集"
            elif block == 'season_name_en' and is_tv:
                val = f"Season {season_num:02d}" if season_num is not None else None
            elif block == 'season_name_en_no0' and is_tv:
                val = f"Season {season_num}" if season_num is not None else None
            elif block == 'season_name_zh' and is_tv:
                val = f"第 {season_num} 季" if season_num is not None else None
            elif block == 'season_name_s' and is_tv:
                val = f"S{season_num:02d}" if season_num is not None else None
            elif block == 'season_name_s_no0' and is_tv:
                val = f"S{season_num}" if season_num is not None else None
            elif video_info and block in video_info:
                val = video_info.get(block)
            elif block.startswith('sep_'):
                is_sep = True
                if block == 'sep_slash':
                    val = '/'
                elif block.startswith('sep_dash_space'):
                    val = ' - '
                elif block.startswith('sep_middot_space'):
                    val = ' · '
                elif block.startswith('sep_middot'):
                    val = '·'
                elif block.startswith('sep_dot'):
                    val = '.'
                elif block.startswith('sep_dash'):
                    val = '-'
                elif block.startswith('sep_underline'):
                    val = '_'
                elif block.startswith('sep_space'):
                    val = ' '

            if val:
                evaluated.append({'val': str(val).strip() if not is_sep else val, 'is_sep': is_sep})

        final_parts = []
        for i, item in enumerate(evaluated):
            if item['is_sep']:
                has_content_before = any(not x['is_sep'] for x in evaluated[:i])
                has_content_after = any(not x['is_sep'] for x in evaluated[i+1:])
                is_last_sep_in_group = i + 1 >= len(evaluated) or not evaluated[i+1]['is_sep']
                if has_content_before and has_content_after and is_last_sep_in_group:
                    final_parts.append(item['val'])
            else:
                final_parts.append(item['val'])

        return "".join(final_parts)
