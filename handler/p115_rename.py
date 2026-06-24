import logging
import os
import re

import utils
from jinja2.sandbox import SandboxedEnvironment


logger = logging.getLogger(__name__)

_JINJA_ENV = SandboxedEnvironment(autoescape=False)


class P115RenameRenderer:
    _P115_INVALID_NAME_CHARS_RE = re.compile(r'[\\/:*?"<>|]')

    def __init__(self, details=None, tmdb_id="", original_title="", config=None):
        self.details = details or {}
        self.tmdb_id = tmdb_id
        self.original_title = original_title
        self.config = config if isinstance(config, dict) else {}

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

    @staticmethod
    def normalize_video_codec_style(value):
        style = str(value or 'hevc').strip().lower().replace('.', '')
        return 'h265' if style in {'h265', 'h264', 'x265', 'x264', '265', '264'} else 'hevc'

    @classmethod
    def format_video_codec_label(cls, codec, style='hevc'):
        text = str(codec or '')
        if not text:
            return ''
        use_h26x = cls.normalize_video_codec_style(style) == 'h265'
        text = re.sub(r'(?i)\b(?:HEVC|H[\.\s]?265|X265)\b', 'H265' if use_h26x else 'HEVC', text)
        return re.sub(r'(?i)\b(?:AVC|H[\.\s]?264|X264)\b', 'H264' if use_h26x else 'AVC', text)

    @staticmethod
    def format_audio_label(audio, hide_channels=False):
        text = str(audio or '')
        if hide_channels:
            text = re.sub(r'(?<!\d)(?:7[\.\s_]?1|5[\.\s_]?1|2[\.\s_]?0|1[\.\s_]?0)(?!\d)', '', text)
            text = re.sub(r'[\s._-]+$', '', text)
        return re.sub(r'\s+', ' ', text).strip()

    @classmethod
    def sanitize_name_component(cls, text):
        cleaned = utils.clean_invisible_chars(text)
        cleaned = cls._P115_INVALID_NAME_CHARS_RE.sub('', cleaned).strip()
        return cleaned

    @staticmethod
    def sanitize_rendered_template(text):
        cleaned = utils.clean_invisible_chars(text).replace("\\", "/")
        cleaned = re.sub(r'[\r\n\t]+', ' ', cleaned)
        cleaned = re.sub(r'[:*?"<>|]', '', cleaned).strip()
        cleaned = P115RenameRenderer.cleanup_empty_separators(cleaned)
        return cleaned

    @staticmethod
    def cleanup_empty_separators(text):
        cleaned = re.sub(r'\s+', ' ', str(text or '')).strip()
        for _ in range(4):
            next_text = re.sub(r'\s*([·•])\s*(?:\1\s*)+', r' \1 ', cleaned)
            next_text = re.sub(r'\s+-\s*(?:-\s*)+', ' - ', next_text)
            next_text = re.sub(r'\s+\.\s*(?:\.\s*)+', ' . ', next_text)
            next_text = re.sub(r'\s+([·•.-])\s+([·•.-])\s+', r' \2 ', next_text)
            next_text = re.sub(r'\s+[·•-]\s*(\.[A-Za-z0-9]{1,8})$', r'\1', next_text)
            next_text = re.sub(r'\s+\.\s+(\.[A-Za-z0-9]{1,8})$', r'\1', next_text)
            next_text = next_text.strip(' ·•-')
            if next_text == cleaned:
                break
            cleaned = next_text
        return cleaned

    def build_template_context(self, is_tv=False, season_num=None, episode_num=None, original_title=None, original_name=None, video_info=None, safe_title=None, file_ext=""):
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
        season_name_en = f"Season {season_no}" if is_tv and season_no else ""
        season_name_en_no0 = f"Season {season_val}" if is_tv and season_val is not None else ""
        season_name_s = f"S{season_no}" if is_tv and season_no else ""
        season_name_s_no0 = f"S{season_val}" if is_tv and season_val is not None else ""
        season_name_zh = f"第 {season_val} 季" if is_tv and season_val is not None else ""
        episode_name_zh = f"第 {episode_val} 集" if is_tv and episode_val is not None else ""
        season_episode_zh = f"第 {season_val} 季 {episode_val} 集" if is_tv and season_val is not None and episode_val is not None else ""
        source = video_info.get('source') or ''
        effect = video_info.get('effect') or ''
        codec_raw = video_info.get('codec') or video_info.get('videoCodec') or ''
        codec = self.format_video_codec_label(codec_raw, self.config.get('video_codec_style'))
        audio = self.format_audio_label(video_info.get('audio') or video_info.get('audioCodec') or '', self.config.get('hide_audio_channels'))
        group = video_info.get('group') or ''
        ext_with_dot = f".{str(file_ext).lstrip('.')}" if file_ext else ""
        title_year = f"{title_zh} ({year})" if title_zh and year else title_zh
        media_type = "电视剧" if is_tv else "电影"
        poster = self.details.get('poster') or self.details.get('poster_path') or ''
        backdrop = self.details.get('backdrop') or self.details.get('backdrop_path') or ''
        overview = self.details.get('overview') or ''
        actors_raw = self.details.get('actors') or self.details.get('cast') or []
        if isinstance(actors_raw, str):
            actors = actors_raw
        elif isinstance(actors_raw, list):
            actor_names = []
            for actor in actors_raw[:5]:
                if isinstance(actor, dict):
                    actor_name = actor.get('name') or actor.get('original_name')
                else:
                    actor_name = actor
                if actor_name:
                    actor_names.append(str(actor_name))
            actors = "、".join(actor_names)
        else:
            actors = ""
        edition = video_info.get('edition') or " ".join([v for v in [source, effect] if v])
        resource_term = video_info.get('resource_term') or " ".join([v for v in [source, effect, video_info.get('resolution') or ''] if v])
        original_name_text = utils.clean_invisible_chars(original_name or safe_title or title_zh)
        name_from_file = os.path.splitext(os.path.basename(original_name_text))[0] if original_name_text else title_zh
        part_match = re.search(r'(?i)(?:^|[ ._\-\[\(])(?:part|pt|cd)[ ._\-]*(\d{1,2})(?=$|[ ._\-\]\)])', original_name_text)
        part = str(video_info.get('part') or (part_match.group(1) if part_match else '') or '')
        episode_title = video_info.get('episode_title') or self.details.get('episode_title') or ''
        episode_date = video_info.get('episode_date') or self.details.get('episode_date') or ''
        season_year = str(video_info.get('season_year') or self.details.get('season_year') or year or '')

        return {
            # ETK names
            'title': title_zh,
            'title_zh': title_zh,
            'title_en': title_en,
            'en_title': title_en,
            'en_name': title_en,
            'title_orig': title_orig,
            'original_title': title_orig,
            'name': name_from_file,
            'clean_title': name_from_file,
            'identify_title': name_from_file,
            'year': year,
            'year_pure': year,
            'title_year': title_year,
            'type': media_type,
            'category': video_info.get('category') or self.details.get('category') or '',
            'vote_average': self.details.get('vote_average') or '',
            'poster': poster,
            'backdrop': backdrop,
            'actors': actors,
            'overview': overview,
            'tmdb': self.tmdb_id,
            'tmdb_id': self.tmdb_id,
            'tmdbid': self.tmdb_id,
            'imdbid': self.details.get('imdb_id') or self.details.get('imdbid') or '',
            'doubanid': self.details.get('douban_id') or self.details.get('doubanid') or '',
            'season': season_val,
            'episode': episode_val,
            'season_no': season_no,
            'episode_no': episode_no,
            's_e': season_episode,
            'season_episode': season_episode,
            'season_fmt': season_name_s,
            'season_year': season_year,
            'episode_title': episode_title,
            'episode_date': episode_date,
            'season_name': season_name_en,
            'season_name_en': season_name_en,
            'season_name_en_no0': season_name_en_no0,
            'season_name_s': season_name_s,
            'season_name_s_no0': season_name_s_no0,
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
            'original_name': original_name_text,
            'part': part,
            'file_ext': ext_with_dot,
            'fileExt': ext_with_dot,

            # MoviePilot-compatible aliases
            'resourceType': source,
            'videoFormat': video_info.get('resolution') or '',
            'videoCodec': codec,
            'audioCodec': audio,
            'releaseGroup': group,
            'webSource': video_info.get('stream') or '',
            'videoBit': video_info.get('videoBit') or video_info.get('video_bit') or '',
            'resource_term': resource_term,
            'customization': video_info.get('customization') or effect,
            'edition': edition,
        }

    def render_template(self, template, is_tv=False, season_num=None, episode_num=None, original_title=None, original_name=None, video_info=None, safe_title=None, file_ext=""):
        template = self.normalize_mp_jinja_template(template)
        if not template.strip():
            return ""
        ctx = self.build_template_context(
            is_tv=is_tv,
            season_num=season_num,
            episode_num=episode_num,
            original_title=original_title,
            original_name=original_name,
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

    def build_name(self, format_value, is_tv=False, season_num=None, episode_num=None, original_title=None, original_name=None, video_info=None, safe_title=None, file_ext=""):
        if not format_value:
            return ""

        video_info = video_info or {}
        codec_raw = video_info.get('codec') or video_info.get('videoCodec') or ''
        audio_raw = video_info.get('audio') or video_info.get('audioCodec') or ''
        if codec_raw or audio_raw:
            video_info = {
                **video_info,
                'codec': self.format_video_codec_label(codec_raw, self.config.get('video_codec_style')),
                'videoCodec': self.format_video_codec_label(codec_raw, self.config.get('video_codec_style')),
                'audio': self.format_audio_label(audio_raw, self.config.get('hide_audio_channels')),
                'audioCodec': self.format_audio_label(audio_raw, self.config.get('hide_audio_channels')),
            }

        if isinstance(format_value, str):
            return self.render_template(
                format_value,
                is_tv=is_tv,
                season_num=season_num,
                episode_num=episode_num,
                original_title=original_title,
                original_name=original_name,
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
