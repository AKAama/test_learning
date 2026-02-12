import hashlib
import html
import re

import bleach
from pyasn1.codec.ber.decoder import decode

_URL_PATTERN = re.compile(
    r"(https?://\S+)|"  # http(s) URL
    r"(www\.\S+)|"  # www.xxxx
    r"(\S+\.(com|cn|net|org|io|top|vip|xyz)(/|$))",  # 域名
    re.IGNORECASE,
)
_FILE_PATTERN = re.compile(
    r"(/[^ \n]+)|"  # Unix 路径
    r"([a-zA-Z]:\\[^ \n]+)|"  # Windows 路径
    r"(\S+\.(txt|pdf|docx?|xlsx?|jpg|png|mp4|zip|rar|csv|json|xml|html|pptx?|7z|tar|gz?))",  # 常见文件后缀
    re.IGNORECASE,
)

def compute_chinese_ratio(text: str) -> float:
    """计算中文字符（基本汉字区）占比"""
    total = len(text)
    if total == 0:
        return 0.0
    chinese_count = len(re.findall(r'[\u4e00-\u9fff\u3000-\u303f\uff01-\uff5e]', text))
    return chinese_count / total

def is_url_or_filename(text: str) -> bool:
    if _URL_PATTERN.search(text):
        return True

    if _FILE_PATTERN.match(text):
        return True
    return False


def is_low_quality_text(text: str) -> bool:
    # 1. 大量重复字符（如 "aaaaaa" 或 "。。。。"）
    if re.search(r'(.)\1{5,}', text):
        return True

    # 2. 非空白字符中，符号/数字占比过高（比如 >80%）
    non_space = re.sub(r'\s+', '', text)
    if len(non_space) == 0:
        return True
    alnum_or_chinese = len(re.findall(r'[\u4e00-\u9fff\w]', non_space))
    if alnum_or_chinese / len(non_space) < 0.3:
        return True

    # 3. 几乎没有标点（可能是一长串无结构文本）
    punctuation = len(re.findall(r'[，。！？；："''（）【】《》、·…—～.,!?;:]', text))
    if punctuation == 0 and len(text) > 100:
        return True
    return False

def md5(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def plain_text(text: str) -> str:
    clean_text = bleach.clean(text, tags=[], attributes=[], strip=True, strip_comments=True)
    decoded=html.unescape(clean_text)
    result = re.sub(r'\s+', '', decoded.replace('\xa0', ''))
    result = result.strip()
    return result

def is_chinese_char(c):
    return '\u4e00' <= c <= '\u9fff'

def is_chinese_string(s):
    return len(s) > 0 and all(is_chinese_char(c) for c in s)