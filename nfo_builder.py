# nfo_builder.py
import xml.etree.ElementTree as ET
from xml.dom import minidom
import logging
import json
from tasks.helpers import extract_top_directors
from utils import GENRE_TRANSLATION_PATCH, get_pinyin_initials

logger = logging.getLogger(__name__)

def _add_element(parent, tag, text):
    if text is not None and str(text).strip() != "":
        elem = ET.SubElement(parent, tag)
        elem.text = str(text)

def _format_dateadded(date_str):
    """将 Emby 的 ISO 时间转换为 NFO 标准时间 (YYYY-MM-DD HH:MM:SS)"""
    if not date_str: return ""
    return str(date_str).replace('T', ' ')[:19]

def _add_common_elements(root, data):
    """添加所有 NFO 共用的基础标签"""
    _add_element(root, 'outline', data.get('overview'))
    _add_element(root, 'lockdata', 'false')
    date_added = data.get('date_added')
    if date_added:
        _add_element(root, 'dateadded', _format_dateadded(date_added))

def _add_actors(root, cast):
    """统一的演员标签生成器，补全 type 和 imdbid"""
    for actor in cast:
        actor_elem = ET.SubElement(root, 'actor')
        _add_element(actor_elem, 'name', actor.get('name'))
        _add_element(actor_elem, 'role', actor.get('character'))
        _add_element(actor_elem, 'type', actor.get('type', 'Actor'))
        _add_element(actor_elem, 'order', actor.get('order'))
        
        if actor.get('profile_path'):
            img_url = actor['profile_path'] if actor['profile_path'].startswith('http') else f"https://image.tmdb.org/t/p/w500{actor['profile_path']}"
            _add_element(actor_elem, 'thumb', img_url)
            
        actor_id = actor.get('id') or actor.get('tmdb_id')
        if actor_id:
            _add_element(actor_elem, 'tmdbid', actor_id)
            
        imdb_id = actor.get('imdb_id')
        if imdb_id:
            _add_element(actor_elem, 'imdbid', imdb_id) # ★ 补全演员 IMDb

def _add_genres_and_tags(root, data):
    """统一处理类型和标签"""
    for genre in data.get('genres', []):
        genre_name = genre.get('name') if isinstance(genre, dict) else genre
        if genre_name in GENRE_TRANSLATION_PATCH:
            genre_name = GENRE_TRANSLATION_PATCH[genre_name]
        _add_element(root, 'genre', genre_name)
    
    tags_to_write = data.get('_mapped_chinese_tags')
    if tags_to_write is not None:
        for tag in tags_to_write:
            _add_element(root, 'tag', tag)
    else:
        raw_keywords = data.get('keywords', [])
        if isinstance(raw_keywords, dict):
            raw_keywords = raw_keywords.get('keywords') or raw_keywords.get('results') or []
        for tag in raw_keywords:
            _add_element(root, 'tag', tag.get('name') if isinstance(tag, dict) else tag)

def build_movie_nfo(data: dict, cast: list) -> str:
    root = ET.Element('movie')
    _add_element(root, 'plot', data.get('overview'))
    _add_common_elements(root, data)
    
    title = data.get('title')
    _add_element(root, 'title', title)
    _add_element(root, 'originaltitle', data.get('original_title'))
    _add_element(root, 'sorttitle', get_pinyin_initials(title))
    
    _add_element(root, 'tagline', data.get('tagline'))
    _add_element(root, 'year', data.get('release_year') or (data.get('release_date')[:4] if data.get('release_date') else ''))
    _add_element(root, 'premiered', data.get('release_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    _add_element(root, 'mpaa', data.get('mpaa') or data.get('certification'))
    
    # 外部ID
    tmdb_id = data.get('id')
    imdb_id = data.get('imdb_id')
    
    if tmdb_id:
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(tmdb_id)
        _add_element(root, 'tmdbid', tmdb_id)
        
    if imdb_id:
        ET.SubElement(root, 'uniqueid', type='imdb').text = str(imdb_id)
        _add_element(root, 'imdbid', imdb_id) 
        _add_element(root, 'id', imdb_id)     
    elif tmdb_id:
        _add_element(root, 'id', tmdb_id)

    # 合集
    collection = data.get('belongs_to_collection')
    if collection and isinstance(collection, dict) and collection.get('name'):
        set_elem = ET.SubElement(root, 'set')
        _add_element(set_elem, 'name', collection.get('name'))
        if collection.get('id'):
            _add_element(set_elem, 'tmdbcolid', str(collection.get('id')))
        if collection.get('overview'):
            _add_element(set_elem, 'overview', collection.get('overview'))

    _add_genres_and_tags(root, data)
    
    # =================================================================
    # ★★★ 终极修复：导演去重、职务汉化与强制后排 ★★★
    # =================================================================
    # 1. 过滤掉 cast 里可能因为读取旧 NFO 而残留的旧导演数据，防止无限影分身
    extended_cast = [a for a in cast if a.get('type', 'Actor') != 'Director']
    
    top_directors = extract_top_directors(data, max_count=3)
    base_director_order = 1000 # 给导演分配 1000 起步的超大 order
    
    for i, d in enumerate(top_directors):
        # A. 写入标准的 director 标签 (兼容 Kodi 等其他播放器)
        dir_elem = ET.SubElement(root, 'director')
        if d.get('id'): dir_elem.set('tmdbid', str(d.get('id')))
        dir_elem.text = d.get('name')
        
        # B. 伪装成 Actor 塞进列表 (专供 Emby 读取头像)
        d_copy = d.copy()
        d_copy['type'] = 'Director'
        
        # 智能区分并汉化职务
        job = d.get('job', 'Director')
        if job == 'Series Director':
            d_copy['character'] = '总导演'
        else:
            d_copy['character'] = '导演'
            
        d_copy['order'] = base_director_order + i
        extended_cast.append(d_copy)
        
    # 2. 在最终写入前，对整个列表按 order 重新严格排序
    extended_cast.sort(key=lambda x: int(x.get('order', 999)) if x.get('order') not in [None, ''] else 999)
        
    _add_actors(root, extended_cast) 
    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")

def build_tvshow_nfo(data: dict, cast: list) -> str:
    root = ET.Element('tvshow')
    _add_element(root, 'plot', data.get('overview'))
    _add_common_elements(root, data) 
    
    title = data.get('name') or data.get('title')
    _add_element(root, 'title', title)
    _add_element(root, 'originaltitle', data.get('original_name') or data.get('original_title'))
    _add_element(root, 'sorttitle', data.get('sorttitle') or get_pinyin_initials(title))
    _add_element(root, 'tagline', data.get('tagline'))
    _add_element(root, 'year', data.get('first_air_date')[:4] if data.get('first_air_date') else '')
    _add_element(root, 'premiered', data.get('first_air_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    _add_element(root, 'status', data.get('status')) 
    _add_element(root, 'mpaa', data.get('mpaa') or data.get('certification'))
    
    tmdb_id = data.get('id')
    imdb_id = data.get('imdb_id')
    
    if tmdb_id:
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(tmdb_id)
        _add_element(root, 'tmdbid', tmdb_id)
        
    if imdb_id:
        ET.SubElement(root, 'uniqueid', type='imdb').text = str(imdb_id)
        _add_element(root, 'imdb_id', imdb_id)
        
    guide_dict = {}
    if tmdb_id: guide_dict["tmdb"] = str(tmdb_id)
    if imdb_id: guide_dict["imdb"] = str(imdb_id)
    if guide_dict:
        _add_element(root, 'episodeguide', json.dumps(guide_dict))
        
    if imdb_id: _add_element(root, 'id', imdb_id)
    elif tmdb_id: _add_element(root, 'id', tmdb_id)

    _add_element(root, 'season', '-1')
    _add_element(root, 'episode', '-1')
    _add_element(root, 'displayorder', 'aired')

    _add_genres_and_tags(root, data)
            
    for studio in data.get('networks', []) + data.get('production_companies', []):
        _add_element(root, 'studio', studio.get('name') if isinstance(studio, dict) else studio)

    # =================================================================
    # ★★★ 终极修复：导演去重、职务汉化与强制后排 ★★★
    # =================================================================
    # 1. 过滤掉 cast 里可能因为读取旧 NFO 而残留的旧导演数据，防止无限影分身
    extended_cast = [a for a in cast if a.get('type', 'Actor') != 'Director']
    
    top_directors = extract_top_directors(data, max_count=3)
    base_director_order = 1000 # 给导演分配 1000 起步的超大 order
    
    for i, d in enumerate(top_directors):
        # A. 写入标准的 director 标签 (兼容 Kodi 等其他播放器)
        dir_elem = ET.SubElement(root, 'director')
        if d.get('id'): dir_elem.set('tmdbid', str(d.get('id')))
        dir_elem.text = d.get('name')
        
        # B. 伪装成 Actor 塞进列表 (专供 Emby 读取头像)
        d_copy = d.copy()
        d_copy['type'] = 'Director'
        
        # 智能区分并汉化职务
        job = d.get('job', 'Director')
        if job == 'Series Director':
            d_copy['character'] = '总导演'
        else:
            d_copy['character'] = '导演'
            
        d_copy['order'] = base_director_order + i
        extended_cast.append(d_copy)
        
    # 2. 在最终写入前，对整个列表按 order 重新严格排序
    extended_cast.sort(key=lambda x: int(x.get('order', 999)) if x.get('order') not in [None, ''] else 999)
        
    _add_actors(root, extended_cast) 
    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")

def build_season_nfo(data: dict) -> str:
    root = ET.Element('season')
    _add_element(root, 'plot', data.get('overview'))
    _add_common_elements(root, data) 
    
    title = data.get('name')
    _add_element(root, 'title', title)
    _add_element(root, 'sorttitle', get_pinyin_initials(title))
    
    _add_element(root, 'seasonnumber', data.get('season_number'))
    _add_element(root, 'premiered', data.get('air_date'))
    
    if data.get('id'):
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(data.get('id'))
        _add_element(root, 'tmdbid', data.get('id'))

    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")

def build_episode_nfo(data: dict, cast: list) -> str:
    root = ET.Element('episodedetails')
    _add_element(root, 'plot', data.get('overview'))
    _add_common_elements(root, data) 
    
    title = data.get('name') or data.get('title')
    _add_element(root, 'title', title)
    # ★★★ 集也加上拼音排序 ★★★
    _add_element(root, 'sorttitle', get_pinyin_initials(title))
    
    _add_element(root, 'season', data.get('season_number'))
    _add_element(root, 'episode', data.get('episode_number'))
    _add_element(root, 'premiered', data.get('air_date'))
    _add_element(root, 'rating', data.get('vote_average'))
    
    if data.get('id'):
        ET.SubElement(root, 'uniqueid', type='tmdb', default='true').text = str(data.get('id'))
        _add_element(root, 'tmdbid', data.get('id'))

    # =================================================================
    # ★★★ 终极修复：导演去重、职务汉化与强制后排 ★★★
    # =================================================================
    # 1. 过滤掉 cast 里可能因为读取旧 NFO 而残留的旧导演数据，防止无限影分身
    extended_cast = [a for a in cast if a.get('type', 'Actor') != 'Director']
    
    top_directors = extract_top_directors(data, max_count=3)
    base_director_order = 1000 # 给导演分配 1000 起步的超大 order
    
    for i, d in enumerate(top_directors):
        # A. 写入标准的 director 标签 (兼容 Kodi 等其他播放器)
        dir_elem = ET.SubElement(root, 'director')
        if d.get('id'): dir_elem.set('tmdbid', str(d.get('id')))
        dir_elem.text = d.get('name')
        
        # B. 伪装成 Actor 塞进列表 (专供 Emby 读取头像)
        d_copy = d.copy()
        d_copy['type'] = 'Director'
        
        # 智能区分并汉化职务
        job = d.get('job', 'Director')
        if job == 'Series Director':
            d_copy['character'] = '总导演'
        else:
            d_copy['character'] = '导演'
            
        d_copy['order'] = base_director_order + i
        extended_cast.append(d_copy)
        
    # 2. 在最终写入前，对整个列表按 order 重新严格排序
    extended_cast.sort(key=lambda x: int(x.get('order', 999)) if x.get('order') not in [None, ''] else 999)
        
    _add_actors(root, extended_cast) 
    return minidom.parseString(ET.tostring(root, encoding='utf-8')).toprettyxml(indent="  ")