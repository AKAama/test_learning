import html
import json
import re
from pathlib import Path

import bleach
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    f"mysql+pymysql://root:Sudy.web123@170.18.9.106:3307/corpus_neu?charset=utf8mb4",
    pool_pre_ping=True,
    pool_recycle=3600,
)

# JSONL 输出文件
output_file = Path("/Volumes/Storage/data/东北大学/data.jsonl")
output_file.parent.mkdir(parents=True, exist_ok=True)
batch_size = 100000
table_name = 't_articlecontent'


def plain_text(text: str) -> str:
    clean_text = bleach.clean(text, tags=[], attributes=[], strip=True, strip_comments=True)
    decoded = html.unescape(clean_text)
    result = re.sub(r'\s+', '', decoded.replace('\xa0', ''))
    result = result.strip()
    return result


def is_low_quality_text(text: str) -> bool:
    # 1. 大量重复字符
    if re.search(r'(.)\1{5,}', text):
        return True
    # 2. 符号/数字占比过高
    non_space = re.sub(r'\s+', '', text)
    if len(non_space) == 0:
        return True
    alnum_or_chinese = len(re.findall(r'[\u4e00-\u9fff\w]', non_space))
    if alnum_or_chinese / len(non_space) < 0.3:
        return True
    # 3. 几乎没有标点
    punctuation = len(re.findall(r'[，。！？；："\'（）【】《》、·…—～.,!?;:]', text))
    if punctuation == 0 and len(text) > 100:
        return True
    return False


def compute_chinese_ratio(text: str) -> float:
    total = len(text)
    if total == 0:
        return 0.0
    chinese_count = len(re.findall(r'[\u4e00-\u9fff\u3000-\u303f\uff01-\uff5e]', text))
    return chinese_count / total


def execute():
    last_id = 0
    exec_index = 0
    total_written = 0
    filtered_low_quality = 0
    filtered_low_ratio = 0

    with open(output_file, 'w', encoding='utf-8') as f:
        while True:
            sql = f"SELECT id, content FROM {table_name} WHERE id > {last_id} AND content != '' ORDER BY id LIMIT {batch_size}"
            df = pd.read_sql(sql, engine)

            if df.empty:
                break

            last_id = int(df["id"].max())

            for _, row in df.iterrows():
                content = plain_text(str(row['content']))
                if is_low_quality_text(content):
                    print("低质量")
                    filtered_low_quality += 1
                    continue
                if compute_chinese_ratio(content) < 0.7:
                    print("中文占比低")
                    filtered_low_ratio += 1
                    continue
                obj = {
                    "id": int(row['id']),
                    "content": content
                }
                f.write(json.dumps(obj, ensure_ascii=False) + '\n')
                print(f"id:{int(row['id'])}已写入")
                total_written += 1

            print(f"{exec_index} 处理完成，当前最大 id: {last_id}, 已写入: {total_written}")
            exec_index += 1

            if len(df) < batch_size:
                break

    engine.dispose()
    print(f"\n导出完成!")
    print(f"成功导出: {total_written} 条")
    print(f"过滤低质量: {filtered_low_quality} 条")
    print(f"过滤中文比例过低: {filtered_low_ratio} 条")
    print(f"输出文件: {output_file}")


if __name__ == '__main__':
    execute()
