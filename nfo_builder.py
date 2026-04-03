# nfo_builder.py
import xml.etree.ElementTree as ET
from xml.dom import minidom
import logging
from utils import GENRE_TRANSLATION_PATCH  # ★★★ 引入类型汉化补丁 ★★★

logger = logging.getLogger(__name__)

def _add_element(parent, tag, text):
    if text is not None and str(text).strip() != "":
        elem = ET.SubElement(parent, tag)
        elem.text = str(text)

def build_movie_nfo(data: dict, cast: list) -> str:
    root = ET.Element('movie')
    _add_element(root, 'title', data.get('title'))
    _add_element(root, 'originaltitle', data.get('original_title'))
    _add_element(root, 'sorttitle', data.get('title'))
    _add_element(root, 'plot', data.get('overview'))
    _add_element(root, 'tagline', data.get('tagline'))
    _add_element(root, 'year', data.get('release_year') or (data.get('release_date')[:4] if data.get('release_date') else ''))
    _add_element(root, 'premiered', data.get('release_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    
    # 外部ID
    if data.get('id'):
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(data.get('id'))
    if data.get('imdb_id'):
        ET.SubElement(root, 'uniqueid', type='imdb').text = str(data.get('imdb_id'))

    # ★★★ 修复：类型 (Genre) 中文化映射 ★★★
    for genre in data.get('genres', []):
        genre_name = genre.get('name') if isinstance(genre, dict) else genre
        if genre_name in GENRE_TRANSLATION_PATCH:
            genre_name = GENRE_TRANSLATION_PATCH[genre_name]
        _add_element(root, 'genre', genre_name)
    
    # 关键词 (Tags) - 优先使用净化后的中文标签
    tags_to_write = data.get('_mapped_chinese_tags')
    if tags_to_write is not None:
        for tag in tags_to_write:
            _add_element(root, 'tag', tag)
    else:
        for tag in data.get('keywords', []):
            _add_element(root, 'tag', tag.get('name') if isinstance(tag, dict) else tag)
        
    for studio in data.get('production_companies', []):
        _add_element(root, 'studio', studio.get('name') if isinstance(studio, dict) else studio)

    # 演员表
    for actor in cast:
        actor_elem = ET.SubElement(root, 'actor')
        _add_element(actor_elem, 'name', actor.get('name'))
        _add_element(actor_elem, 'role', actor.get('character'))
        _add_element(actor_elem, 'order', actor.get('order'))
        if actor.get('profile_path'):
            img_url = actor['profile_path'] if actor['profile_path'].startswith('http') else f"https://image.tmdb.org/t/p/w500{actor['profile_path']}"
            _add_element(actor_elem, 'thumb', img_url)
            
        # ★★★ 修复：兼容 TMDb 原生 id 和 数据库恢复的 tmdb_id ★★★
        actor_id = actor.get('id') or actor.get('tmdb_id')
        if actor_id:
            _add_element(actor_elem, 'tmdbid', actor_id)

    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")

def build_tvshow_nfo(data: dict, cast: list) -> str:
    root = ET.Element('tvshow')
    _add_element(root, 'title', data.get('name') or data.get('title'))
    _add_element(root, 'originaltitle', data.get('original_name') or data.get('original_title'))
    _add_element(root, 'sorttitle', data.get('name') or data.get('title'))
    _add_element(root, 'plot', data.get('overview'))
    _add_element(root, 'year', data.get('first_air_date')[:4] if data.get('first_air_date') else '')
    _add_element(root, 'premiered', data.get('first_air_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    _add_element(root, 'status', data.get('status')) # 追剧状态
    
    if data.get('id'):
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(data.get('id'))

    # ★★★ 修复：类型 (Genre) 中文化映射 ★★★
    for genre in data.get('genres', []):
        genre_name = genre.get('name') if isinstance(genre, dict) else genre
        if genre_name in GENRE_TRANSLATION_PATCH:
            genre_name = GENRE_TRANSLATION_PATCH[genre_name]
        _add_element(root, 'genre', genre_name)
        
    # 关键词 (Tags) - 优先使用净化后的中文标签
    tags_to_write = data.get('_mapped_chinese_tags')
    if tags_to_write is not None:
        for tag in tags_to_write:
            _add_element(root, 'tag', tag)
    else:
        for tag in data.get('keywords', []):
            _add_element(root, 'tag', tag.get('name') if isinstance(tag, dict) else tag)
            
    for studio in data.get('networks', []) + data.get('production_companies', []):
        _add_element(root, 'studio', studio.get('name') if isinstance(studio, dict) else studio)

    for actor in cast:
        actor_elem = ET.SubElement(root, 'actor')
        _add_element(actor_elem, 'name', actor.get('name'))
        _add_element(actor_elem, 'role', actor.get('character'))
        _add_element(actor_elem, 'order', actor.get('order'))
        if actor.get('profile_path'):
            img_url = actor['profile_path'] if actor['profile_path'].startswith('http') else f"https://image.tmdb.org/t/p/w500{actor['profile_path']}"
            _add_element(actor_elem, 'thumb', img_url)
            
        # ★★★ 修复：兼容 TMDb 原生 id 和 数据库恢复的 tmdb_id ★★★
        actor_id = actor.get('id') or actor.get('tmdb_id')
        if actor_id:
            _add_element(actor_elem, 'tmdbid', actor_id)

    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")

def build_episode_nfo(data: dict, cast: list) -> str:
    root = ET.Element('episodedetails')
    _add_element(root, 'title', data.get('name') or data.get('title'))
    _add_element(root, 'plot', data.get('overview'))
    _add_element(root, 'season', data.get('season_number'))
    _add_element(root, 'episode', data.get('episode_number'))
    _add_element(root, 'premiered', data.get('air_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    
    if data.get('id'):
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(data.get('id'))

    for actor in cast:
        actor_elem = ET.SubElement(root, 'actor')
        _add_element(actor_elem, 'name', actor.get('name'))
        _add_element(actor_elem, 'role', actor.get('character'))
        _add_element(actor_elem, 'order', actor.get('order'))
        if actor.get('profile_path'):
            img_url = actor['profile_path'] if actor['profile_path'].startswith('http') else f"https://image.tmdb.org/t/p/w500{actor['profile_path']}"
            _add_element(actor_elem, 'thumb', img_url)
            
        # ★★★ 修复：兼容 TMDb 原生 id 和 数据库恢复的 tmdb_id ★★★
        actor_id = actor.get('id') or actor.get('tmdb_id')
        if actor_id:
            _add_element(actor_elem, 'tmdbid', actor_id)

    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")