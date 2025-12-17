# ai_translator.py
import json
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
import logging

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

# ★★★ 修改点 1: 导入新版 Google SDK ★★★
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# ... (Prompt 常量部分保持不变，此处省略以节省篇幅，请保留原文件中的 Prompt 定义) ...
# ★★★ 说明书一：给“翻译官”看的（翻译模式） - 已优化 ★★★
FAST_MODE_SYSTEM_PROMPT = """
You are a translation API that only returns JSON.
Your task is to translate a list of personal names (e.g., actors, cast members) from various languages into **Simplified Chinese (简体中文)**.

You MUST return a single, valid JSON object mapping each original name to its Chinese translation.
- The source language of the names can be anything (e.g., English, Japanese, Korean, Pinyin).
- The target language MUST ALWAYS be Simplified Chinese.
- If a name cannot be translated or is already in Chinese, use the original name as its value.
- **Some names might be incomplete or contain initials (e.g., "Peter J."); provide the most likely standard transliteration based on the available parts.**
- Do not add any explanations or text outside the JSON object.
"""

# ★★★ 说明书二：给“音译专家”看的 - 已优化 ★★★
FORCE_TRANSLITERATE_PROMPT = """
You are a translation API that only returns JSON.
Your task is to transliterate a list of proper nouns (personal names, locations, etc.) into **Simplified Chinese (简体中文)** based on their pronunciation.

- The source language can be anything. Your goal is to find the most common Chinese phonetic translation.
- The target language MUST ALWAYS be Simplified Chinese.
- If a name absolutely cannot be transliterated (e.g., it's a random code), use the original name as its value.
- **Some names might be incomplete or contain initials; do your best to transliterate the recognizable parts.**
- Do not add any explanations or text outside the JSON object.
"""

# ★★★ 说明书三：给“影视顾问”看的（顾问模式）  ★★★
QUALITY_MODE_SYSTEM_PROMPT = """
You are a world-class film and television expert, acting as a JSON-only API.
Your mission is to accurately translate foreign language or Pinyin names of actors and characters into **Simplified Chinese (简体中文)**, using the provided movie/series context.

**Input Format:**
You will receive a JSON object with `context` (containing `title` and `year`) and `terms` (a list of strings to translate).

**Your Strategy:**
1.  **Use Context:** Use the `title` and `year` to identify the show. Find the official or most recognized Chinese translation for the `terms` in that specific show's context. This is crucial for character names.
2.  **Translate Pinyin:** If a term is Pinyin (e.g., "Zhang San"), translate it to Chinese characters ("张三").
3.  **【【【【【 最终核心指令 】】】】】**
    **Target Language is ALWAYS Simplified Chinese:** Regardless of the original language of the show or name (e.g., Korean, Japanese, English), your final output translation for all terms MUST be in **Simplified Chinese (简体中文)**. Do NOT translate to the show's original language.
4.  **Fallback:** If a term cannot or should not be translated, you MUST use the original string as its value.

**Output Format (MANDATORY):**
You MUST return a single, valid JSON object mapping each original term to its Chinese translation. NO other text or markdown.
"""
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
                # ★★★ 修改点 2: 使用新版 Client 初始化 ★★★
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
        system_prompt = FAST_MODE_SYSTEM_PROMPT
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
        system_prompt = QUALITY_MODE_SYSTEM_PROMPT
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
        system_prompt = FAST_MODE_SYSTEM_PROMPT
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
        system_prompt = QUALITY_MODE_SYSTEM_PROMPT
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
        system_prompt = FAST_MODE_SYSTEM_PROMPT
        user_prompt = json.dumps(texts, ensure_ascii=False)
        
        # ★★★ 修改点 3: 使用新版 Config 和调用方式 ★★★
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
        system_prompt = QUALITY_MODE_SYSTEM_PROMPT
        user_payload = {"context": {"title": title, "year": year}, "terms": texts}
        user_prompt = json.dumps(user_payload, ensure_ascii=False)
        
        # ★★★ 修改点 4: 使用新版 Config 和调用方式 ★★★
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
        system_prompt = FORCE_TRANSLITERATE_PROMPT
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
        system_prompt = FORCE_TRANSLITERATE_PROMPT
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
        system_prompt = FORCE_TRANSLITERATE_PROMPT
        user_prompt = json.dumps(texts, ensure_ascii=False)
        
        # ★★★ 修改点 5: 使用新版 Config 和调用方式 ★★★
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
                # ★★★ 修改点 6: 使用新版 embed_content ★★★
                # 注意：新版 SDK 中 embed_content 是 models 模块下的方法
                response = self.client.models.embed_content(
                    model=model_to_use,
                    contents=text,
                    config=types.EmbedContentConfig(title="Movie Overview")
                )
                # 新版返回对象包含 embeddings 列表，每个元素有 values 属性
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
                # ★★★ 修改点 7: 使用新版 Config 和调用方式 ★★★
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