# services/cover_generator/__init__.py

import logging
import shutil
import yaml
import json
import random
import requests
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from gevent import spawn_later
from database import custom_collection_db, queries_db
import config_manager
import handler.emby as emby 
from extensions import UPDATING_IMAGES
from .styles.style_single_1 import create_style_single_1
from .styles.style_single_2 import create_style_single_2
from .styles.style_multi_1 import create_style_multi_1

logger = logging.getLogger(__name__)

class CoverGeneratorService:
    SORT_BY_DISPLAY_NAME = { "Random": "随机", "Latest": "最新添加" }

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._sort_by = self.config.get("sort_by", "Random")
        self._covers_output = self.config.get("covers_output")
        self._covers_input = self.config.get("covers_input")
        self._title_config_str = self.config.get("title_config", "")
        self._cover_style = self.config.get("cover_style", "single_1")
        self._multi_1_blur = self.config.get("multi_1_blur", False)
        self._multi_1_use_primary = self.config.get("multi_1_use_primary", True)
        self._single_use_primary = self.config.get("single_use_primary", False)
        self.data_path = Path(config_manager.PERSISTENT_DATA_PATH) / "cover_generator"
        self.covers_path = self.data_path / "covers"
        self.font_path = self.data_path / "fonts"
        self.covers_path.mkdir(parents=True, exist_ok=True)
        self.font_path.mkdir(parents=True, exist_ok=True)
        self.zh_font_path = None
        self.en_font_path = None
        self.zh_font_path_multi_1 = None
        self.en_font_path_multi_1 = None
        self._fonts_checked_and_ready = False

    def generate_for_library(self, emby_server_id: str, library: Dict[str, Any], item_count: Optional[int] = None, content_types: Optional[List[str]] = None, custom_collection_data: Optional[Dict] = None):
        sort_by_name = self.SORT_BY_DISPLAY_NAME.get(self._sort_by, self._sort_by)
        logger.info(f"  ➜ 开始以排序方式: {sort_by_name} 为媒体库 '{library['Name']}' 生成封面...")
        self.__get_fonts()
        image_data = self.__generate_image_data(emby_server_id, library, item_count, content_types, custom_collection_data)
        if not image_data:
            logger.error(f"  ➜ 为媒体库 '{library['Name']}' 生成封面图片失败。")
            return False
        success = self.__set_library_image(emby_server_id, library, image_data)
        if success:
            logger.info(f"  ➜ 成功更新媒体库 '{library['Name']}' 的封面！")
        else:
            logger.error(f"  ➜ 上传封面到媒体库 '{library['Name']}' 失败。")
        return success

    def __generate_image_data(self, server_id: str, library: Dict[str, Any], item_count: Optional[int] = None, content_types: Optional[List[str]] = None, custom_collection_data: Optional[Dict] = None) -> bytes:
        library_name = library['Name']
        title = self.__get_library_title_from_yaml(library_name)
        custom_image_paths = self.__check_custom_image(library_name)
        if custom_image_paths:
            logger.info(f"  ➜ 发现媒体库 '{library_name}' 的自定义图片，将使用路径模式生成。")
            return self.__generate_image_from_path(library_name, title, custom_image_paths, item_count)
        
        # ★★★ 真实海报兜底 (针对“即将上线”等本地无资源的榜单) ★★★
        if custom_collection_data and custom_collection_data.get('type') in ['list', 'ai_recommendation_global']:
            tmdb_image_data = self.__generate_from_local_tmdb_metadata(library_name, title, custom_collection_data, item_count)
            if tmdb_image_data:
                return tmdb_image_data

        logger.trace(f"  ➜ 未发现自定义图片，将从服务器 '{server_id}' 获取媒体项作为封面来源。")
        return self.__generate_from_server(server_id, library, title, item_count, content_types, custom_collection_data)

    def __generate_from_local_tmdb_metadata(self, library_name: str, title: Tuple[str, str], custom_collection_data: Dict, item_count: Optional[int]) -> Optional[bytes]:
        """
        当本地没有 Emby 媒体项时，利用数据库里存储的 poster_path 下载海报。
        """
        try:
            media_info_list = custom_collection_data.get('generated_media_info_json') or []
            if isinstance(media_info_list, str):
                media_info_list = json.loads(media_info_list)

            # 检查是否有足够的 Emby ID
            valid_emby_ids = [i for i in media_info_list if i.get('emby_id')]
            
            # 如果本地已经有不少于 3 个的匹配项，优先用 Emby 的
            if len(valid_emby_ids) >= 3:
                return None

            logger.info(f"  ➜ 合集 '{library_name}' 本地资源不足 (Emby匹配数: {len(valid_emby_ids)})，尝试使用 TMDB 元数据生成真实封面...")

            # 提取 TMDB ID
            candidates = [i for i in media_info_list if i.get('tmdb_id')]
            
            if not candidates:
                return None

            # 如果是随机模式，洗牌
            if self._sort_by == "Random":
                random.shuffle(candidates)
            
            # 限制数量
            limit = 1 if self._cover_style.startswith('single') else 9
            candidates = candidates[:limit]
            
            # 提取纯 ID 列表
            tmdb_ids = [str(item['tmdb_id']) for item in candidates]
            
            # 从数据库批量查询 poster_path
            metadata_map = queries_db.get_missing_items_metadata(tmdb_ids)
            
            image_paths = []
            
            for tmdb_id in tmdb_ids:
                meta = metadata_map.get(tmdb_id)
                if meta and meta.get('poster_path'):
                    poster_path = meta['poster_path']
                    # 构造完整 URL
                    full_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
                    
                    # 下载
                    save_name = f"tmdb_{tmdb_id}.jpg"
                    local_path = self.__download_external_image(full_url, library_name, save_name)
                    if local_path:
                        image_paths.append(local_path)
            
            if not image_paths:
                logger.warning(f"  ➜ 数据库中未找到有效的 poster_path。")
                return None

            logger.info(f"  ➜ 成功获取到 {len(image_paths)} 张真实海报，正在生成封面...")
            
            # ==================================================================
            # ★★★ 核心修复：清理旧的缓存图片 ★★★
            # 必须删除 1.jpg - 9.jpg，否则 __prepare_multi_images 会复用旧的占位符图片
            # ==================================================================
            subdir = self.covers_path / library_name
            if subdir.exists():
                for i in range(1, 10):
                    old_cache = subdir / f"{i}.jpg"
                    if old_cache.exists():
                        try:
                            old_cache.unlink()
                        except Exception:
                            pass
            # ==================================================================

            return self.__generate_image_from_path(library_name, title, [str(p) for p in image_paths], item_count)

        except Exception as e:
            logger.error(f"  ➜ TMDB 海报兜底流程出错: {e}", exc_info=True)
            return None

    def __download_external_image(self, url: str, library_name: str, filename: str) -> Optional[Path]:
        """通用的外部图片下载方法 (支持代理)"""
        subdir = self.covers_path / library_name
        subdir.mkdir(parents=True, exist_ok=True)
        filepath = subdir / filename
        
        # 简单的缓存机制
        if filepath.exists() and filepath.stat().st_size > 0:
            return filepath

        try:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(max_retries=3)
            session.mount('https://', adapter)
            
            # ★★★ 注入代理 ★★★
            proxies = config_manager.get_proxies_for_requests()
            if proxies:
                session.proxies.update(proxies)
            
            resp = session.get(url, stream=True, timeout=15)
            if resp.status_code == 200:
                with open(filepath, 'wb') as f:
                    shutil.copyfileobj(resp.raw, f)
                return filepath
        except Exception as e:
            logger.warning(f"  ➜ 下载外部图片失败 {url}: {e}")
        return None

    def __generate_from_server(self, server_id: str, library: Dict[str, Any], title: Tuple[str, str], item_count: Optional[int] = None, content_types: Optional[List[str]] = None, custom_collection_data: Optional[Dict] = None) -> bytes:
        required_items_count = 1 if self._cover_style.startswith('single') else 9
        items = self.__get_valid_items_from_library(server_id, library, required_items_count, content_types, custom_collection_data)
        if not items:
            logger.warning(f"  ➜ 在媒体库 '{library['Name']}' 中找不到任何带有可用图片的媒体项。")
            return None
        if self._cover_style.startswith('single'):
            image_url = self.__get_image_url(items[0])
            if not image_url: return None
            image_path = self.__download_image(server_id, image_url, library['Name'], 1)
            if not image_path: return None
            return self.__generate_image_from_path(library['Name'], title, [image_path], item_count)
        else:
            image_paths = []
            for i, item in enumerate(items[:9]):
                image_url = self.__get_image_url(item)
                if image_url:
                    path = self.__download_image(server_id, image_url, library['Name'], i + 1)
                    if path:
                        image_paths.append(path)
            if not image_paths:
                logger.warning(f"  ➜ 为多图模式下载图片失败。")
                return None
            return self.__generate_image_from_path(library['Name'], title, image_paths, item_count)

    def __get_valid_items_from_library(self, server_id: str, library: Dict[str, Any], limit: int, content_types: Optional[List[str]] = None, custom_collection_data: Optional[Dict] = None) -> List[Dict]:
        library_id = library.get("Id") or library.get("ItemId")
        library_name = library.get("Name")
        base_url = config_manager.APP_CONFIG.get('emby_server_url')
        api_key = config_manager.APP_CONFIG.get('emby_api_key')
        user_id = config_manager.APP_CONFIG.get('emby_user_id')

        # ======================================================================
        # ★★★ 0. 统一计算安全分级上限 (Safe Rating Limit) ★★★
        # ======================================================================
        # 1. 获取用户配置的上限 (默认 8/PG-13)
        config_limit = self.config.get('max_safe_rating', 8)
        
        # 2. 判断是否命中白名单 (库名包含 R级/限制/成人 等)
        is_whitelisted_library = any(keyword.lower() in library_name.lower() for keyword in ['R级', '限制', '成人', 'Adult', 'Porn', '18+'])
        
        # 3. 确定最终限制
        safe_rating_limit = None
        if is_whitelisted_library:
            safe_rating_limit = None # 白名单库 -> 无限制
        elif config_limit >= 999:
            safe_rating_limit = None # 用户配置为无限制 -> 无限制
        else:
            safe_rating_limit = config_limit # 应用配置的限制

        if safe_rating_limit is not None:
            logger.trace(f"  🛡️ 媒体库 '{library_name}' 将应用分级限制: 等级 <= {safe_rating_limit}")

        # ======================================================================
        # 策略 A: 实时筛选类合集 (Filter / AI Recommendation)
        # ======================================================================
        if custom_collection_data and custom_collection_data.get('type') in ['filter', 'ai_recommendation']:
            logger.info(f"  ➜ 检测到 '{library_name}' 为实时筛选/推荐合集，正在调用查询引擎...")
            try:
                definition = custom_collection_data.get('definition_json', {})
                rules = definition.get('rules', [])
                
                # 如果规则里显式指定了分级筛选，则信任规则，不强制覆盖
                has_rating_rule = any(r.get('field') == 'unified_rating' for r in rules)
                current_limit = safe_rating_limit if not has_rating_rule else None

                db_sort_by = 'Random' if self._sort_by == 'Random' else 'DateCreated'
                
                items_from_db, _ = queries_db.query_virtual_library_items(
                    rules=rules,
                    logic=definition.get('logic', 'AND'),
                    user_id=user_id,
                    limit=limit,
                    offset=0,
                    sort_by=db_sort_by,
                    item_types=definition.get('item_type', ['Movie']),
                    target_library_ids=definition.get('target_library_ids'),
                    max_rating_override=current_limit # ★ 传入限制
                )
                
                return self.__fetch_emby_items_by_ids(items_from_db, base_url, api_key, user_id, limit)

            except Exception as e:
                logger.error(f"  ➜ 处理实时合集 '{library_name}' 出错: {e}", exc_info=True)

        # ======================================================================
        # 策略 B: 静态/缓存类合集 (List / Global AI)
        # ======================================================================
        custom_collection = custom_collection_data
        if not custom_collection:
            custom_collection = custom_collection_db.get_custom_collection_by_emby_id(library_id)
    
        if custom_collection and custom_collection.get('type') in ['list', 'ai_recommendation_global']:
            # 静态列表通常是用户手动挑选的，一般不应用分级过滤，或者应用后会导致列表变空
            # 这里我们选择：如果不是白名单库，依然应用过滤 (防止手动把 R 级片加到首页推荐)
            # 但由于静态列表没有 SQL 查询过程，我们需要在获取到 Emby Item 后进行过滤 (后置过滤)
            # 为了简单，这里暂不处理静态列表的强过滤，假设用户手动添加即为允许。
            # 如果需要过滤，可以在 __fetch_emby_items_by_ids 后遍历检查 OfficialRating。
            
            logger.info(f"  ➜ 检测到 '{library_name}' 为榜单/全局推荐合集...")
            try:
                media_info_list = custom_collection.get('generated_media_info_json') or []
                if isinstance(media_info_list, str): media_info_list = json.loads(media_info_list)
                    
                valid_emby_ids = [
                    str(item['emby_id']) 
                    for item in media_info_list 
                    if item.get('emby_id') and str(item.get('emby_id')).lower() != 'none'
                ]

                if valid_emby_ids:
                    if self._sort_by == "Random": random.shuffle(valid_emby_ids)
                    # 构造伪对象传给 fetcher
                    items_payload = [{'Id': i} for i in valid_emby_ids[:limit*2]]
                    return self.__fetch_emby_items_by_ids(items_payload, base_url, api_key, user_id, limit)
                
                # Fallback: 现有成员
                fallback_items = emby.get_emby_library_items(
                    base_url=base_url, api_key=api_key, user_id=user_id,
                    library_ids=[library_id],
                    media_type_filter="Movie,Series,Season,Episode", 
                    fields="Id,Name,Type,ImageTags,BackdropImageTags,PrimaryImageTag,PrimaryImageItemId",
                    limit=limit
                )
                return [item for item in fallback_items if self.__get_image_url(item)][:limit]

            except Exception as e:
                logger.error(f"  ➜ 处理自定义合集 '{library_name}' 出错: {e}", exc_info=True)
        
        # ======================================================================
        # 策略 C: 普通媒体库 (Native Library) - ★★★ 核心修改 ★★★
        # ======================================================================
        # 以前是直接调 API，现在改为：优先查 DB (应用分级限制) -> 失败则调 API
        
        # 1. 确定类型
        media_type_to_fetch = None
        if content_types:
            media_type_to_fetch = content_types # List
        else:
            TYPE_MAP = {
                'movies': ['Movie'], 'tvshows': ['Series'], 'music': ['MusicAlbum'],
                'boxsets': ['Movie', 'Series'], 'mixed': ['Movie', 'Series'], 
                'audiobooks': ['AudioBook']
            }
            c_type = library.get('CollectionType')
            media_type_to_fetch = TYPE_MAP.get(c_type, ['Movie', 'Series'])
            
            if library.get('Type') == 'BoxSet':
                media_type_to_fetch = ['Movie'] # 简化处理

        # 2. 确定排序
        db_sort_by = 'Random' if self._sort_by == 'Random' else 'DateCreated'
        
        # 3. ★★★ 尝试从数据库查询 (这是堵住漏洞的关键) ★★★
        # 利用 query_virtual_library_items 的 target_library_ids 功能
        try:
            items_from_db, _ = queries_db.query_virtual_library_items(
                rules=[], # 无额外规则
                logic='AND',
                user_id=None, # 使用管理员视角，但通过 override 限制分级
                limit=limit,
                offset=0,
                sort_by=db_sort_by,
                item_types=media_type_to_fetch,
                target_library_ids=[library_id], # ★ 指定原生库 ID
                max_rating_override=safe_rating_limit # ★ 应用分级限制
            )

            if items_from_db:
                logger.trace(f"  ➜ 原生库 '{library_name}' 通过数据库查询命中 {len(items_from_db)} 个项目 (已过滤分级)。")
                return self.__fetch_emby_items_by_ids(items_from_db, base_url, api_key, user_id, limit)
            else:
                logger.debug(f"  ➜ 原生库 '{library_name}' 数据库查询为空 (可能是新库未同步)，回退到 API 直接调用。")

        except Exception as e:
            logger.warning(f"  ➜ 原生库 '{library_name}' 数据库查询失败: {e}，回退到 API。")

        # 4. API 回退 (兜底逻辑，保持原有行为，但无法精确过滤分级)
        # 如果数据库没数据，说明还没同步，此时只能调 API。
        # API 调用的缺点是无法利用我们的 max_rating_override 逻辑 (除非去解析 OfficialRating 字符串)
        
        api_limit = limit * 5 if limit < 10 else limit * 2 
        str_types = ",".join(media_type_to_fetch)
        
        sort_by_param = "Random" if self._sort_by == "Random" else "DateCreated"
        sort_order_param = "Descending" if sort_by_param == "DateCreated" else None

        all_items = emby.get_emby_library_items(
            base_url=base_url, api_key=api_key, user_id=user_id,
            library_ids=[library_id],
            media_type_filter=str_types,
            fields="Id,Name,Type,ImageTags,BackdropImageTags,DateCreated,PrimaryImageTag,PrimaryImageItemId",
            sort_by=sort_by_param,
            sort_order=sort_order_param,
            limit=api_limit,
            force_user_endpoint=True
        )
        
        if not all_items: return []
        valid_items = [item for item in all_items if self.__get_image_url(item)]
        
        if self._sort_by == "Random":
            random.shuffle(valid_items)
            
        return valid_items[:limit]

    # ★★★ 辅助方法：根据 ID 列表批量获取 Emby 详情 (带图片Tag) ★★★
    def __fetch_emby_items_by_ids(self, items_from_db: List[Dict], base_url: str, api_key: str, user_id: str, limit: int) -> List[Dict]:
        if not items_from_db: return []
        
        target_ids = [item['Id'] for item in items_from_db]
        ids_str = ",".join(target_ids)
        
        url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
        headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
        params = {
            'Ids': ids_str,
            'Fields': "Id,Name,Type,ImageTags,BackdropImageTags,PrimaryImageTag,PrimaryImageItemId",
        }
        
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items_from_emby = data.get('Items', [])
            
            valid_items = [item for item in items_from_emby if self.__get_image_url(item)]
            
            # 如果是随机排序，这里再洗一次牌，因为 API 返回的顺序可能被 ID 顺序影响
            if self._sort_by == "Random":
                random.shuffle(valid_items)
            
            return valid_items[:limit]
        except Exception as e:
            logger.error(f"  ➜ 批量获取 Emby 项目详情失败: {e}")
            return []

    def __get_image_url(self, item: Dict[str, Any]) -> str:
        item_id = item.get("Id")
        if not item_id: return None
        primary_url, backdrop_url = None, None
        primary_tag_in_dict = item.get("ImageTags", {}).get("Primary")
        if primary_tag_in_dict:
            primary_url = f'/emby/Items/{item_id}/Images/Primary?tag={primary_tag_in_dict}'
        else:
            referenced_item_id = item.get("PrimaryImageItemId")
            referenced_tag = item.get("PrimaryImageTag")
            if referenced_item_id and referenced_tag:
                primary_url = f'/emby/Items/{referenced_item_id}/Images/Primary?tag={referenced_tag}'
        backdrop_tags = item.get("BackdropImageTags")
        if backdrop_tags:
            backdrop_url = f'/emby/Items/{item_id}/Images/Backdrop/0?tag={backdrop_tags[0]}'
        
        should_use_primary = (self._cover_style.startswith('single') and self._single_use_primary) or \
                             (self._cover_style.startswith('multi') and self._multi_1_use_primary)

        if should_use_primary:
            return primary_url or backdrop_url
        else:
            return backdrop_url or primary_url

    def __download_image(self, server_id: str, api_path: str, library_name: str, count: int) -> Path:
        subdir = self.covers_path / library_name
        subdir.mkdir(parents=True, exist_ok=True)
        filepath = subdir / f"{count}.jpg"
        try:
            base_url = config_manager.APP_CONFIG.get('emby_server_url')
            api_key = config_manager.APP_CONFIG.get('emby_api_key')
            path_only, _, query_string = api_path.partition('?')
            path_parts = path_only.strip('/').split('/')
            image_tag = None
            if 'tag=' in query_string:
                image_tag = query_string.split('tag=')[1].split('&')[0]
            if len(path_parts) >= 4 and path_parts[1] == 'Items' and path_parts[3] == 'Images':
                item_id = path_parts[2]
                image_type = path_parts[4]
                success = emby.download_emby_image(
                    item_id=item_id, image_type=image_type, image_tag=image_tag,
                    save_path=str(filepath), emby_server_url=base_url, emby_api_key=api_key
                )
                if success: return filepath
            else:
                logger.error(f"  ➜ 无法从API路径解析有效的项目ID和图片类型: {api_path}")
        except Exception as e:
            logger.error(f"  ➜ 下载图片失败 ({api_path}): {e}", exc_info=True)
        return None

    def __generate_image_from_path(self, library_name: str, title: Tuple[str, str], image_paths: List[str], item_count: Optional[int] = None) -> bytes:
        logger.trace(f"  ➜ 正在为 '{library_name}' 从本地路径生成封面...")
        zh_font_size = self.config.get("zh_font_size", 1)
        en_font_size = self.config.get("en_font_size", 1)
        blur_size = self.config.get("blur_size", 50)
        color_ratio = self.config.get("color_ratio", 0.8)
        font_size = (float(zh_font_size), float(en_font_size))
        if self._cover_style == 'single_1':
            return create_style_single_1(str(image_paths[0]), title, (str(self.zh_font_path), str(self.en_font_path)), 
                                         font_size=font_size, blur_size=blur_size, color_ratio=color_ratio,
                                         item_count=item_count, config=self.config)
        elif self._cover_style == 'single_2':
            return create_style_single_2(str(image_paths[0]), title, (str(self.zh_font_path), str(self.en_font_path)), 
                                         font_size=font_size, blur_size=blur_size, color_ratio=color_ratio,
                                         item_count=item_count, config=self.config)
        elif self._cover_style == 'multi_1':
            if self.zh_font_path_multi_1 and self.zh_font_path_multi_1.exists():
                zh_font_path_multi = self.zh_font_path_multi_1
            else:
                logger.warning(f"  ➜ 未找到多图专用中文字体 ({self.zh_font_path_multi_1})，将回退使用单图字体。")
                zh_font_path_multi = self.zh_font_path
            if self.en_font_path_multi_1 and self.en_font_path_multi_1.exists():
                en_font_path_multi = self.en_font_path_multi_1
            else:
                logger.warning(f"  ➜ 未找到多图专用英文字体 ({self.en_font_path_multi_1})，将回退使用单图字体。")
                en_font_path_multi = self.en_font_path
            font_path_multi = (str(zh_font_path_multi), str(en_font_path_multi))
            zh_font_size_multi = self.config.get("zh_font_size_multi_1", 1)
            en_font_size_multi = self.config.get("en_font_size_multi_1", 1)
            font_size_multi = (float(zh_font_size_multi), float(en_font_size_multi))
            blur_size_multi = self.config.get("blur_size_multi_1", 50)
            color_ratio_multi = self.config.get("color_ratio_multi_1", 0.8)
            library_dir = self.covers_path / library_name
            self.__prepare_multi_images(library_dir, image_paths)
            return create_style_multi_1(str(library_dir), title, font_path_multi, 
                                      font_size=font_size_multi, is_blur=self._multi_1_blur, 
                                      blur_size=blur_size_multi, color_ratio=color_ratio_multi,
                                      item_count=item_count, config=self.config)
        return None

    def __set_library_image(self, server_id: str, library: Dict[str, Any], image_data: bytes) -> bool:
        library_id = library.get("Id") or library.get("ItemId")
        base_url = config_manager.APP_CONFIG.get('emby_server_url')
        api_key = config_manager.APP_CONFIG.get('emby_api_key')
        upload_url = f"{base_url.rstrip('/')}/Items/{library_id}/Images/Primary?api_key={api_key}"
        headers = {"Content-Type": "image/jpeg"}
        if self._covers_output:
            try:
                save_path = Path(self._covers_output) / f"{library['Name']}.jpg"
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as f:
                    f.write(image_data)
                logger.info(f"  ➜ 封面已另存到: {save_path}")
            except Exception as e:
                logger.error(f"  ➜ 另存封面失败: {e}")
        try:
            if library_id:
                UPDATING_IMAGES.add(library_id)
                
                def _clear_flag():
                    UPDATING_IMAGES.discard(library_id)
                spawn_later(30, _clear_flag)
            response = requests.post(upload_url, data=image_data, headers=headers, timeout=30)
            response.raise_for_status()
            logger.debug(f"  ➜ 成功上传封面到媒体库 '{library['Name']}'。")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"  ➜ 上传封面到媒体库 '{library['Name']}' 时发生网络错误: {e}")
            if e.response is not None:
                logger.error(f"  ➜ 响应状态: {e.response.status_code}, 响应内容: {e.response.text[:200]}")
            return False

    def __get_library_title_from_yaml(self, library_name: str) -> Tuple[str, str]:
        zh_title, en_title = library_name, ''
        if not self._title_config_str:
            return zh_title, en_title
        try:
            title_config = yaml.safe_load(self._title_config_str)
            if isinstance(title_config, dict) and library_name in title_config:
                titles = title_config[library_name]
                if isinstance(titles, list) and len(titles) >= 2:
                    zh_title, en_title = titles[0], titles[1]
        except yaml.YAMLError as e:
            logger.error(f"  ➜ 解析标题配置失败: {e}")
        return zh_title, en_title

    def __prepare_multi_images(self, library_dir: Path, source_paths: List[str]):
        library_dir.mkdir(parents=True, exist_ok=True)
        for i in range(1, 10):
            target_path = library_dir / f"{i}.jpg"
            if not target_path.exists():
                source_to_copy = random.choice(source_paths)
                shutil.copy(source_to_copy, target_path)

    def __check_custom_image(self, library_name: str) -> List[str]:
        if not self._covers_input: return []
        library_dir = Path(self._covers_input) / library_name
        if not library_dir.is_dir(): return []
        images = sorted([
            str(p) for p in library_dir.iterdir()
            if p.suffix.lower() in [".jpg", ".jpeg", ".png"]
        ])
        return images

    def __download_file(self, url: str, dest_path: Path):
        if dest_path.exists():
            logger.trace(f"  ➜ 字体文件已存在，跳过下载: {dest_path.name}")
            return
        logger.info(f"  ➜ 字体文件不存在，正在从URL下载: {dest_path.name}...")
        try:
            # ★★★ 注入代理 ★★★
            proxies = config_manager.get_proxies_for_requests()
            response = requests.get(url, stream=True, timeout=60, proxies=proxies)
            response.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"  ➜ 字体 '{dest_path.name}' 下载成功。")
        except requests.RequestException as e:
            logger.error(f"  ➜ 下载字体 '{dest_path.name}' 失败: {e}")
            if dest_path.exists():
                dest_path.unlink()

    def __get_fonts(self):
        if self._fonts_checked_and_ready:
            return
        font_definitions = [
            {"target_attr": "zh_font_path", "filename": "zh_font.ttf", "local_key": "zh_font_path_local", "url_key": "zh_font_url"},
            {"target_attr": "en_font_path", "filename": "en_font.ttf", "local_key": "en_font_path_local", "url_key": "en_font_url"},
            {"target_attr": "zh_font_path_multi_1", "filename": "zh_font_multi_1.ttf", "local_key": "zh_font_path_multi_1_local", "url_key": "zh_font_url_multi_1"},
            {"target_attr": "en_font_path_multi_1", "filename": "en_font_multi_1.otf", "local_key": "en_font_path_multi_1_local", "url_key": "en_font_url_multi_1"}
        ]
        for font_def in font_definitions:
            font_path_to_set = None
            expected_font_file = self.font_path / font_def["filename"]
            if expected_font_file.exists():
                font_path_to_set = expected_font_file
            local_path_str = self.config.get(font_def["local_key"])
            if local_path_str:
                local_path = Path(local_path_str)
                if local_path.exists():
                    logger.trace(f"  ➜ 发现并优先使用用户指定的外部字体: {local_path_str}")
                    font_path_to_set = local_path
                else:
                    logger.warning(f"  ➜ 配置的外部字体路径不存在: {local_path_str}，将忽略此配置。")
            if not font_path_to_set:
                url = self.config.get(font_def["url_key"])
                if url:
                    self.__download_file(url, expected_font_file)
                    if expected_font_file.exists():
                        font_path_to_set = expected_font_file
            setattr(self, font_def["target_attr"], font_path_to_set)
        if self.zh_font_path and self.en_font_path:
            logger.trace("  ➜ 核心字体文件已准备就绪。后续任务将不再重复检查。")
            self._fonts_checked_and_ready = True
        else:
            logger.warning("  ➜ 一个或多个核心字体文件缺失且无法下载。请检查UI中的本地路径或下载链接是否有效。")