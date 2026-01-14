import duckdb
import json

# 连接 DuckDB 数据库（本地文件或内存）
con = duckdb.connect(database='/Volumes/Storage/data/test.duckdb', read_only=False)

# 输出 JSONL 文件路径
output_file = '/Volumes/Storage/data/articles_clean.jsonl'

# 查询并清洗文本
query = """
SELECT
    id,
    content
FROM article_content_1
"""

# 执行查询
results = con.execute(query).fetchall()

def clean_text(text: str) -> str:
    if not text:
        return ""
    # 替换换行、回车、制表符、半角空格、非断空格、全角空格
    for c in ['\n', '\r', '\t', '\u00A0', '\u3000']:
        text = text.replace(c, ' ')
    # 压缩连续空格为一个空格，并去掉首尾空格
    text = ' '.join(text.split())
    return text

# 写入干净的 JSONL
with open(output_file, 'w', encoding='utf-8') as f:
    for row in results:
        obj = {
            "id": row[0],
            "content": clean_text(row[1])
        }
        f.write(json.dumps(obj, ensure_ascii=False) + '\n')

print(f"清理后的 JSONL 已生成：{output_file}")
