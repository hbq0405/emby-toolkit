# ai_translator.py
import json
import re
import time
from typing import Optional, Dict, Any, List
import logging

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
        logger.debug(f"  -> 待修复的原始文本: ```\n{text}\n```")

        # 2. 尝试从 markdown 代码块中提取 JSON
        # AI 经常会返回 ```json ... ``` 这样的格式
        match = re.search(r'```(json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if match:
            json_str = match.group(2)
            logger.info("成功从Markdown代码块中提取出JSON内容，正在重新解析...")
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as inner_e:
                logger.error(f"提取出的JSON仍然解析失败: {inner_e}")
                # 即使提取失败，我们依然可以尝试最后的修复
                text = json_str # 用提取出的内容进行后续修复

        # 3. 尝试修复未闭合的 JSON
        # 找到最后一个 " 或 }，然后截断并尝试补全
        last_quote = text.rfind('"')
        last_brace = text.rfind('}')
        
        if last_brace > last_quote:
            # 如果 } 是最后一个关键字符，说明结构可能没问题，只是被截断了
            # 我们直接截取到最后一个 }
            fixed_text = text[:last_brace + 1]
        elif last_quote != -1:
            # 如果 " 是最后一个，说明一个字符串没闭合
            # 我们找到这个字符串开始的地方，然后把它整个删掉
            prev_quote = text.rfind('"', 0, last_quote)
            if prev_quote != -1:
                # 找到 "key": "value... 这种模式，把它删掉
                comma_before = text.rfind(',', 0, prev_quote)
                if comma_before != -1:
                    fixed_text = text[:comma_before] + "\n}" # 删掉最后半个键值对，并补上结尾
                else: # 如果是第一个键值对
                    fixed_text = "{}"
            else:
                fixed_text = text # 无法修复
        else:
            fixed_text = text

        if fixed_text != text:
            logger.info("尝试进行截断和补全修复...")
            try:
                # 再试一次！
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
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

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
        # 这个prompt现在只用于单文本翻译，作为向后兼容
        
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
                logger.info(f"  -> OpenAI 初始化成功")
            
            elif self.provider == 'zhipuai':
                if not ZHIPUAI_AVAILABLE: raise ImportError("智谱AI SDK 未安装")
                self.client = ZhipuAI(api_key=self.api_key)
                logger.info(f"  -> 智谱AI 初始化成功")
            
            elif self.provider == 'gemini':
                if not GEMINI_AVAILABLE: raise ImportError("Google Gemini SDK 未安装")
                genai.configure(api_key=self.api_key)
                self.client = genai.GenerativeModel(self.model)
                logger.info(f"  -> Google Gemini 初始化成功")

            else:
                raise ValueError(f"  -> 不支持的AI提供商: {self.provider}")
        except Exception as e:
            logger.error(f"{self.provider.capitalize()} client 初始化失败: {e}")
            raise

    # --- 单文本翻译 (保留，但内部可以调用批量方法以统一逻辑) ---
    def translate(self, text: str) -> Optional[str]:
        if not text or not text.strip():
            return text
        
        # 单文本翻译现在可以简单地调用批量翻译，代码更简洁
        # 如果翻译失败，返回原文
        batch_result = self.batch_translate([text])
        return batch_result.get(text, text)

    # --- ✨✨✨ 翻译调度 ✨✨✨ ---
    def batch_translate(self, 
                        texts: List[str], 
                        mode: str = 'fast',
                        title: Optional[str] = None, 
                        year: Optional[int] = None) -> Dict[str, str]:
        
        if not texts: 
            return {}
        
        unique_texts = list(set(texts))
        
        # 调度员开始看指令
        if mode == 'quality':
            # 如果指令是“高质量”，就喊“顾问组”来干活
            return self._translate_quality_mode(unique_texts, title, year)
        # ★★★ 新增调度逻辑 ★★★
        elif mode == 'transliterate':
            # 如果指令是“强制音译”，就喊“音译组”来干活
            return self._translate_transliterate_mode(unique_texts)
        else:
            # 其他所有情况（包括默认的'fast'），都喊“翻译组”来干活
            return self._translate_fast_mode(unique_texts)
    # ★★★ “翻译快做”小组长 (现在负责分批和调度！) ★★★
    def _translate_fast_mode(self, texts: List[str]) -> Dict[str, str]:
        CHUNK_SIZE = 50
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        # ▼▼▼ 只在真正分块时才打印详细日志 ▼▼▼
        if total_chunks > 1:
            logger.info(f"  -> [翻译模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次，每批最多 {CHUNK_SIZE} 个。")
        else:
            logger.info(f"  -> [翻译模式] 开始处理 {len(texts)} 个词条...")

        # 3. 小组长逐个派发任务
        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  -> [翻译模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            # 根据公司（provider）选择不同的员工干活
            result_chunk = {}
            if self.provider == 'openai':
                result_chunk = self._fast_openai(chunk)
            elif self.provider == 'zhipuai':
                result_chunk = self._fast_zhipuai(chunk)
            elif self.provider == 'gemini':
                result_chunk = self._fast_gemini(chunk)
            
            if result_chunk:
                all_results.update(result_chunk)
            
            # 4. 安排休息时间（如果不是最后一批）
            if i < total_chunks - 1:
                logger.debug(f"  -> 批次处理完毕，等待 {REQUEST_INTERVAL} 秒...")
                time.sleep(REQUEST_INTERVAL)
        
        return all_results
    
    # ★★★ “强制音译”小组长 ★★★
    def _translate_transliterate_mode(self, texts: List[str]) -> Dict[str, str]:
        CHUNK_SIZE = 50
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        if total_chunks > 1:
            logger.info(f"  -> [音译模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次。")
        else:
            logger.info(f"  -> [音译模式] 开始处理 {len(texts)} 个词条...")

        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  -> [音译模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            result_chunk = {}
            # 根据提供商选择不同的实现
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

    # ★★★ “顾问精做”小组长 (同样负责分批和调度！) ★★★
    def _translate_quality_mode(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        CHUNK_SIZE = 30
        REQUEST_INTERVAL = 1.5

        all_results = {}
        text_chunks = [texts[i:i + CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
        total_chunks = len(text_chunks)

        # ▼▼▼ 只在真正分块时才打印详细日志 ▼▼▼
        if total_chunks > 1:
            logger.info(f"  -> [顾问模式] 数据量较大，已自动分块。共 {len(texts)} 个词条，分为 {total_chunks} 个批次，每批最多 {CHUNK_SIZE} 个。")
        else:
            # 如果只有一个批次，日志就应该更简洁
            logger.info(f"  -> [顾问模式] 开始处理 {len(texts)} 个词条 (上下文: '{title}') ...")

        # 3. 小组长逐个派发任务
        for i, chunk in enumerate(text_chunks):
            if total_chunks > 1:
                logger.info(f"  -> [顾问模式] 正在处理批次 {i + 1}/{total_chunks}")
            
            result_chunk = {}
            if self.provider == 'openai':
                result_chunk = self._quality_openai(chunk, title, year)
            elif self.provider == 'zhipuai':
                result_chunk = self._quality_zhipuai(chunk, title, year)
            elif self.provider == 'gemini':
                result_chunk = self._quality_gemini(chunk, title, year)

            if result_chunk:
                all_results.update(result_chunk)

            # 4. 安排休息时间
            if i < total_chunks - 1:
                logger.debug(f"  -> 批次处理完毕，等待 {REQUEST_INTERVAL} 秒...")
                time.sleep(REQUEST_INTERVAL)
        
        return all_results
    # --- 底层员工：具体实现各种模式和提供商的组合 ---
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
            return _safe_json_loads(response_content) or {} # 如果抢救失败，返回一个空字典
        except Exception as e:
            logger.error(f"  -> [翻译模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
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
            return _safe_json_loads(response_content) or {} # 如果抢救失败，返回一个空字典
        except Exception as e:
            logger.error(f"  -> [顾问模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
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
            return _safe_json_loads(response_content) or {} # 如果抢救失败，返回一个空字典
        except Exception as e:
            logger.error(f"  -> [翻译模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
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
            return _safe_json_loads(response_content) or {} # 如果抢救失败，返回一个空字典
        except Exception as e:
            logger.error(f"  -> [顾问模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # --- Gemini 员工 ---
    def _fast_gemini(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        # Gemini的System Prompt需要通过GenerationConfig传递
        system_prompt = FAST_MODE_SYSTEM_PROMPT
        user_prompt = json.dumps(texts, ensure_ascii=False)
        # 将 system prompt 和 user prompt 组合成一个列表传递
        full_prompt = [system_prompt, user_prompt]
        
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0
        )
        try:
            # 注意：新版SDK中，system_instruction在GenerativeModel初始化时设置更佳
            response = self.client.generate_content(
                full_prompt,
                generation_config=generation_config,
                request_options={'timeout': 300}
            )
            # Gemini Pro Vision等模型可能返回分块内容，但文本模型通常直接用 .text
            # 另外，Gemini的JSON模式输出非常干净，通常不需要_safe_json_loads，但为了保险起见可以加上
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  -> [翻译模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            # 尝试从错误中提取可解析的部分
            if hasattr(e, 'last_response') and e.last_response:
                logger.info("  -> 尝试从Gemini的错误响应中恢复内容...")
                return _safe_json_loads(e.last_response.text) or {}
            return {}

    def _quality_gemini(self, texts: List[str], title: Optional[str], year: Optional[int]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = QUALITY_MODE_SYSTEM_PROMPT
        user_payload = {"context": {"title": title, "year": year}, "terms": texts}
        user_prompt = json.dumps(user_payload, ensure_ascii=False)
        full_prompt = [system_prompt, user_prompt]
        
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0
        )
        try:
            response = self.client.generate_content(
                full_prompt,
                generation_config=generation_config,
                request_options={'timeout': 300}
            )
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  -> [顾问模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            if hasattr(e, 'last_response') and e.last_response:
                logger.info("  -> 尝试从Gemini的错误响应中恢复内容...")
                return _safe_json_loads(e.last_response.text) or {}
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
            logger.error(f"  -> [音译模式-OpenAI] 翻译时发生错误: {e}", exc_info=True)
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
            logger.error(f"  -> [音译模式-智谱AI] 翻译时发生错误: {e}", exc_info=True)
            return {}

    # ★★★ Gemini 音译实现 ★★★
    def _transliterate_gemini(self, texts: List[str]) -> Dict[str, str]:
        if not self.client: return {}
        system_prompt = FORCE_TRANSLITERATE_PROMPT
        user_prompt = json.dumps(texts, ensure_ascii=False)
        full_prompt = [system_prompt, user_prompt]
        
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0
        )
        try:
            response = self.client.generate_content(
                full_prompt,
                generation_config=generation_config,
                request_options={'timeout': 300}
            )
            return _safe_json_loads(response.text) or {}
        except Exception as e:
            logger.error(f"  -> [音译模式-Gemini] 翻译时发生错误: {e}", exc_info=True)
            if hasattr(e, 'last_response') and e.last_response:
                return _safe_json_loads(e.last_response.text) or {}
            return {}