"""
用于清洗原始中文文本中 HTML 标签的工具函数。
"""

from __future__ import annotations

import html as _html
import re
from typing import Iterable

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None


def _strip_tags_with_bs4(html: str) -> str:
    """如果安装了 BeautifulSoup，则优先使用它来移除 HTML 标签。"""
    if not BeautifulSoup:
        return html
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)


def _strip_tags_fallback(html: str) -> str:
    """当没有 BeautifulSoup 时，使用简单的正则表达式去除 HTML 标签的后备方案。"""
    # 去掉 script/style 代码块
    no_script = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # 去掉剩余的所有标签
    no_tags = re.sub(r"<[^>]+>", " ", no_script)
    # 折叠多余空白
    return re.sub(r"\s+", " ", no_tags).strip()


def clean_html(html: str) -> str:
    """将 HTML 字符串清洗为纯文本。

    - 去除 HTML 标签（优先使用 BeautifulSoup，其次正则）
    - 解码 HTML 实体（如 &nbsp;、&ldquo; 等）
    - 压缩多余空格/换行，只保留单个空格分隔
    """
    if not html:
        return ""

    # 先尝试用 bs4 去标签
    text = _strip_tags_with_bs4(html)
    if text == html:
        # 说明没有安装 BeautifulSoup，或者解析失败，使用正则后备方案
        text = _strip_tags_fallback(html)

    # 解码 HTML 实体（&nbsp;、&ldquo;、&amp; 等）
    text = _html.unescape(text)
    # 再统一压缩所有空白字符为一个空格
    text = re.sub(r"\s+", " ", text).strip()
    return text


def batch_clean_html(html_docs: Iterable[str]) -> list[str]:
    """批量清洗多条 HTML 文本。"""
    return [clean_html(doc) for doc in html_docs]


__all__ = ["clean_html", "batch_clean_html"]


