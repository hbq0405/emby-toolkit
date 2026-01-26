# ai_translator.py
import json
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
import logging
from database import settings_db
import utils
try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)

def _safe_json_loads(text: str) -> Optional[Dict]:
    """
    一个更安全的 JSON 解析函数，能处理一些常见的AI返回错误。
    """
    if not text:
        return None
    
    try:
        # 1. 尝试直接解析
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON直接解析失败: {e}。将尝试进行智能修复...")
        logger.debug(f"  ➜ 待修复的原始文本: ```\n{text}\n```")

        # 2. 尝试从 markdown 代码块中提取 JSON
        match = re.search(r'```(json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if match:
            json_str = match.group(2)
            logger.info("成功从Markdown代码块中提取出JSON内容，正在重新解析...")
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as inner_e:
                logger.error(f"提取出的JSON仍然解析失败: {inner_e}")
                text = json_str 

        # 3. 尝试修复未闭合的 JSON
        last_quote = text.rfind('"')
        last_brace = text.rfind('}')
        
        if last_brace > last_quote:
            fixed_text = text[:last_brace + 1]
        elif last_quote != -1:
            prev_quote = text.rfind('"', 0, last_quote)
            if prev_quote != -1:
                comma_before = text.rfind(',', 0, prev_quote)
                if comma_before != -1:
                    fixed_text = text[:comma_before] + "\n}" 
                else: 
                    fixed_text = "{}"
            else:
                fixed_text = text 
        else:
            fixed_text = text

        if fixed_text != text:
            logger.info("尝试进行截断和补全修复...")
            try:
                result = json.loads(fixed_text)
                logger.info("JSON 修复成功！返回部分解析结果。")
                return result
            except json.JSONDecodeError:
                logger.error("最终修复失败，放弃解析。")
                return None
        
        return None

# --- 动态导入所有需要的 SDK ---
try:
    from openai import OpenAI, APIError, APITimeoutError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from zhipuai import ZhipuAI
    ZHIPUAI_AVAILABLE = True
except ImportError:
    ZHIPUAI_AVAILABLE = False

try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

class AITranslator:
    def __init__(self, config: Dict[str, Any]):
        self.provider = config.get("ai_provider", "openai").lower()
        self.api_key = config.get("ai_api_key")
        self.model = config.get("ai_model_name")
        self.base_url = config.get("ai_base_url")
        self.embedding_model = config.get("ai_embedding_model")
        if not self.api_key:
            raise ValueError("AI Translator: API Key 未配置。")
            
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """根据提供商初始化对应的客户端"""
        try:
            if self.provider == 'openai':
                if not OPENAI_AVAILABLE: raise ImportError("OpenAI SDK 未安装")
                self.client = OpenAI(api_key=self.api_key, base_url=self.base_url if self.base_url else None)
                logger.info(f"  ➜ OpenAI 初始化成功")
            
            elif self.provider == 'zhipuai':
                if not ZHIPUAI_AVAILABLE: raise ImportError("智谱AI SDK 未安装")
                self.client = ZhipuAI(api_key=self.api_key)
                logger.info(f"  ➜ 智谱AI 初始化成功")
            
            elif self.provider == 'gemini':
                if not GEMINI_AVAILABLE: raise ImportError("Google GenAI SDK (google-genai) 未安装")
                self.client = genai.Client(api_key=self.api_key)
                logger.info(f"  ➜ Google Gemini (New SDK) 初始化成功")

            else:
                raise ValueError(f"  ➜ 不支持的AI提供商: {self.provider}")
        except Exception as e:
            logger.error(f"{self.provider.capitalize()} client 初始化失败: {e}")
            raise

    def translate(self, text: str) -> Optional[str]:
        if not text or not text.strip():
            return text
        batch_result = self.batch_translate([text])
        return batch_result.get(text, text)

    def batch_translate(self, 
                        texts: List[str], 
                        mode: str = 'fast',
                        title: Optional[str] = None, 
                        year: Optional[int] = None) -> Dict[str, str]:
        
        if not texts: 
            return {}
        
        unique_texts = list(set(texts))
        
        if mode == 'quality':
            return self._translate_quality_mode(unique_texts, title, year)
        elif mode == 'transliterate':
            return self._translate_transliterate_mode(unique_texts)
        else:
            return self._translate_fast_mode(unique_texts)
        
    def _get_prompt(self, key: str) -> str:
        """
        优先从数据库获取用户自定义提示词，如果没有则使用 utils 中的默认值。
        """
        user_prompts = settings_db.get_setting('ai_user_prompts') or {}
        return user_prompts.get(key, utils.DEFAULT_AI_PROMPTS.get(key, ""))

    def translate_overview(self, overview_text: str, title: str = "") -> Optional[str]:
        """
        专门用于翻译剧情简介。
        """
        if not overview_text or not overview_text.strip():
            return None

        raw_prompt = self._get_prompt("overview_translation")
        system_prompt = raw_prompt.format(title=title, overview=overview_text)
        user_prompt = "Please translate the overview."

        try:
            response_content = ""
            if self.provider == 'openai':
                if not self.client: return None
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.3 
                )
                response_content = resp.choices[0].message.content

            elif self.provider == 'zhipuai':
                 if not self.client: return None
                 resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.3
                )
                 response_content = resp.choices[0].message.content
            
            elif self.provider == 'gemini':
                if not self.client: return None
                config = types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.3,
                    system_instruction=system_prompt
                )
                resp = self.client.models.generate_content(
                    model=self.model,
                    contents=user_prompt,
                    config=config
                )
                response_content = resp.text

            result = _safe_json_loads(response_content)
            if result and "translation" in result:
                return result["translation"]
            return None

        except Exception as e:
            logger.error(f"  ➜ [简介翻译] 翻译失败: {e}")
            return None

    def translate_title(self, title_text: str, media_type: str = "Movie", year: str = "") -> Optional[str]:
        """
        专门用于翻译标题。
        """
        if not title_text or not title_text.strip():
            return None

        raw_prompt = self._get_prompt("title_translation")
        system_prompt = raw_prompt.format(media_type=media_type, title=title_text, year=year)
        user_prompt = "Translate this title."

        try:
            response_content = ""
            if self.provider == 'openai' and self.client:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                    response_format={"type": "json_object"}, temperature=0.3
                )
                response_content = resp.choices[0].message.content
            elif self.provider == 'zhipuai' and self.client:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                    response_format={"type": "json_object"}, temperature=0.3
                )
                response_content = resp.choices[0].message.content
            elif self.provider == 'gemini' and self.client:
                config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.3, system_instruction=system_prompt)
                resp = self.client.models.generate_content(model=self.model, contents=user_prompt, config=config)
                response_content = resp.text

            result = _safe_json_loads(response_content)
            if result and "translation" in result:
                return result["translation"]
            return None

        except Exception as e:
            logger.error(f"  ➜ [标题翻译] 翻译失败: {e}")
            return None

    def _translate_fast_mode(self, texts: List[str]) -> Dict[str, str]:
        CHUNK_SIZE = 50
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        if total_chunks > 1:
            logger.info(f"  ➜ [翻译模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次，每批最多 {CHUNK_SIZE} 个。")
        else:
            logger.info(f"  ➜ [翻译模式] 开始处理 {len(texts)} 个词条...")

        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  ➜ [翻译模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            result_chunk = {}
            if self.provider == 'openai':
                result_chunk = self._fast_openai(chunk)
            elif self.provider == 'zhipuai':
                result_chunk = self._fast_zhipuai(chunk)
            elif self.provider == 'gemini':
                result_chunk = self._fast_gemini(chunk)
            
            if result_chunk:
                all_results.update(result_chunk)
            
            if i < total_chunks - 1:
                logger.debug(f"  ➜ 批次处理完毕，等待 {REQUEST_INTERVAL} 秒...")
                time.sleep(REQUEST_INTERVAL)
        
        return all_results
    
    def _translate_transliterate_mode(self, texts: List[str]) -> Dict[str, str]:
        CHUNK_SIZE = 50
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        if total_chunks > 1:
            logger.info(f"  ➜ [音译模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次。")
        else:
            logger.info(f"  ➜ [音译模式] 开始处理 {len(texts)} 个词条...")

        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  ➜ [音译模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            result_chunk = {}
            if self.provider == 'openai':
                result_chunk = self._transliterate_openai(chunk)
            elif self.provider == 'zhipuai':
                result_chunk = self._transliterate_zhipuai(chunk)
            elif self.provider == 'gemini':
                result_chunk = self._transliterate_gemini(chunk)
            
            if result_chunk:
                all_results.update(result_chunk)
            
            if i < total_chunks - 1:
                time.sleep(REQUEST_INTERVAL)
        
        return all_results

    def _translate_quality_mode(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        CHUNK_SIZE = 30
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        if total_chunks > 1:
            logger.info(f"  ➜ [顾问模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次，每批最多 {CHUNK_SIZE} 个。")
        else:
            logger.info(f"  ➜ [顾问模式] 开始处理 {len(texts)} 个词条 (上下文: '{title}') ...")

        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  ➜ [顾问模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            result_chunk = {}
            if self.provider == 'openai':
                result_chunk = self._quality_openai(chunk, title, year)
            elif self.provider == 'zhipuai':
                result_chunk = self._quality_zhipuai(chunk, title, year)
            elif self.provider == 'gemini':
                result_chunk = self._quality_gemini(chunk, title, year)

            if result_chunk:
                all_results.update(result_chunk)

            if i < total_chunks - 1:
                logger.debug(f"  ➜ 批次处理完毕，等待 {REQUEST_INTERVAL} 秒...")
                time.sleep(REQUEST_INTERVAL)
        
        return all_results

    # --- OpenAI 员工 ---
    def _fast_openai(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("fast_mode")
        user_prompt = json.dumps(texts, ensure_ascii=False)
        try:
            chat_completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
                timeout=300
            )
            response_content = chat_completion.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [翻译模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    def _quality_openai(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("quality_mode")
        user_payload = {"context": {"title": title, "year": year}, "terms": texts}
        user_prompt = json.dumps(user_payload, ensure_ascii=False)
        try:
            chat_completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
                timeout=300
            )
            response_content = chat_completion.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [顾问模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # --- 智谱AI 员工 ---
    def _fast_zhipuai(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("fast_mode") 
        user_prompt = json.dumps(texts, ensure_ascii=False)
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            response_content = response.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [翻译模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    def _quality_zhipuai(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("quality_mode") 
        user_payload = {"context": {"title": title, "year": year}, "terms": texts}
        user_prompt = json.dumps(user_payload, ensure_ascii=False)
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            response_content = response.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [顾问模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # --- Gemini 员工 (新版 SDK) ---
    def _fast_gemini(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("fast_mode") 
        user_prompt = json.dumps(texts, ensure_ascii=False)
        
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.0,
            system_instruction=system_prompt
        )
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=user_prompt,
                config=config
            )
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  ➜ [翻译模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            return {}

    def _quality_gemini(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("quality_mode") # ★★★
        user_payload = {"context": {"title": title, "year": year}, "terms": texts}
        user_prompt = json.dumps(user_payload, ensure_ascii=False)
        
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.0,
            system_instruction=system_prompt
        )
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=user_prompt,
                config=config
            )
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  ➜ [顾问模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            return {}
        
    # ★★★ OpenAI 音译实现 ★★★
    def _transliterate_openai(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("transliterate_mode") # ★★★
        user_prompt = json.dumps(texts, ensure_ascii=False)
        try:
            chat_completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
                timeout=300
            )
            response_content = chat_completion.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [音译模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # ★★★ 智谱AI 音译实现 ★★★
    def _transliterate_zhipuai(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("transliterate_mode") 
        user_prompt = json.dumps(texts, ensure_ascii=False)
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            response_content = response.choices[0].message.content
            return _safe_json_loads(response_content) or {}
        except Exception as e:
            logger.error(f"  ➜ [音译模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # ★★★ Gemini 音译实现 (新版 SDK) ★★★
    def _transliterate_gemini(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = self._get_prompt("transliterate_mode") 
        user_prompt = json.dumps(texts, ensure_ascii=False)
        
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.0,
            system_instruction=system_prompt
        )
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=user_prompt,
                config=config
            )
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  ➜ [音译模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            return {}
        
    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        【核心功能】将文本转化为向量 (Embedding)。
        """
        if not text or not text.strip():
            return None
            
        try:
            if self.provider == 'openai':
                model_to_use = self.embedding_model
                if not model_to_use and self.base_url and "siliconflow" in self.base_url:
                    model_to_use = "BAAI/bge-m3"
                if not model_to_use:
                    model_to_use = "text-embedding-3-small"

                response = self.client.embeddings.create(
                    input=text,
                    model=model_to_use 
                )
                return response.data[0].embedding

            elif self.provider == 'zhipuai':
                model_to_use = self.embedding_model if self.embedding_model else "embedding-2"
                response = self.client.embeddings.create(
                    model=model_to_use,
                    input=text
                )
                return response.data[0].embedding

            elif self.provider == 'gemini':
                model_to_use = self.embedding_model if self.embedding_model else "text-embedding-004"
                response = self.client.models.embed_content(
                    model=model_to_use,
                    contents=text,
                    config=types.EmbedContentConfig(title="Movie Overview")
                )
                return response.embeddings[0].values

        except Exception as e:
            logger.error(f"  ➜ [Embedding] 生成向量失败 ({self.provider}): {e}")
            return None
        
        return None

    def get_recommendations(self, user_history: List[str], user_instruction: str = None, allowed_types: List[str] = None) -> List[Dict[str, Any]]:
        """
        【核心功能】猎手模式：基于用户历史推荐新片。
        """
        if not user_history:
            return []
            
        type_constraint_prompt = ""
        if allowed_types:
            if len(allowed_types) == 1:
                if allowed_types[0] == 'Movie':
                    type_constraint_prompt = "5. **仅推荐电影**：用户只看电影，请不要推荐电视剧。"
                elif allowed_types[0] == 'Series':
                    type_constraint_prompt = "5. **仅推荐电视剧**：用户只看剧集，请不要推荐电影。"
            else:
                type_constraint_prompt = "5. **类型限制**：请推荐电影或电视剧。"

        system_prompt = f"""
你是一位精通中外影视的资深推荐专家。
请根据用户的观影历史，推荐高质量的影视作品。

**【核心铁律 - 违反会导致系统崩溃】**
1. **标题必须是中文**：`title` 字段**必须**是简体中文。
2. **国产剧禁止用英文**：对于中国（大陆/香港/台湾）的影视剧，**绝对禁止**使用英文译名，**必须**使用中文原名。
3. **外语片必须翻译**：对于外语片，必须提供通用的中文译名。

**【防幻觉与准确性规则 - 必须严格遵守】**
1. **禁止推荐具体季号**：只返回剧集的主标题（例如：返回“白夜追凶”，不要返回“白夜追凶 第二季”）。
2. **禁止编造续集**：绝对不要捏造TMDb上不存在的续集（例如：不要编造“隐秘的角落4”）。
3. **禁止重复推荐**：不要推荐同一部剧的多个季，也不要重复推荐同一部剧。
4. **禁止未来年份**：不要推荐年份超过当前年份（{datetime.now().year}）的作品，除非是即将上映的真实作品。
5. **推荐相似作品**：如果用户看了一部剧，请推荐**风格相似的其他剧集**，而不是该剧的下一季。
6. **确保真实存在**：所有推荐的标题必须是真实存在的影视作品。
{type_constraint_prompt}

**JSON 返回格式：**
[
  {{
    "title": "漫长的季节", 
    "original_title": "The Long Season",
    "year": 2023,
    "type": "Series", 
    "reason": "..."
  }}
]
"""
        
        history_str = json.dumps(user_history, ensure_ascii=False)
        user_content = f"用户的高分观影历史: {history_str}\n"
        
        if user_instruction:
            user_content += f"用户的特殊要求: {user_instruction}\n"
        else:
            user_content += "用户要求: 推荐高分、口碑好、且风格相似的作品。\n"
            
        user_content += "\n再次提醒：请务必输出中文标题！国产剧不要输出英文名！"

        logger.info(f"  ➜ [智能推荐] 正在基于 {len(user_history)} 部历史分析用户口味...")

        try:
            response_text = ""
            if self.provider == 'openai':
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    response_format={"type": "json_object"}, 
                    temperature=0.6 
                )
                response_text = resp.choices[0].message.content

            elif self.provider == 'zhipuai':
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.5
                )
                response_text = resp.choices[0].message.content
            
            elif self.provider == 'gemini':
                config = types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.5,
                    system_instruction=system_prompt
                )
                resp = self.client.models.generate_content(
                    model=self.model,
                    contents=user_content,
                    config=config
                )
                response_text = resp.text

            result = _safe_json_loads(response_text)
            
            if isinstance(result, dict):
                for key in result:
                    if isinstance(result[key], list):
                        return result[key]
                return [result]
            elif isinstance(result, list):
                return result
            
            return []

        except Exception as e:
            logger.error(f"  ➜ [智能推荐] 获取推荐失败: {e}", exc_info=True)
            return []