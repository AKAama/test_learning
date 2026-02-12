"""
直接从 MySQL dump 文件解析 T_ARTICLECONTENT 表数据并导出为 JSONL
无需导入数据库
"""
import json
import re
import bleach
import html

# 文件路径
sql_file = "/Volumes/Storage/data/东北大学/T_ARTICLECONTENT.sql"
output_file = "/Volumes/Storage/data/东北大学/article_content.jsonl"


def unescape_mysql(s: str) -> str:
    """MySQL 转义处理：将 '' 转换为 '"""
    if s is None:
        return ""
    # 两个连续的单引号表示一个单引号
    return s.replace("''", "'")


def parse_sql_file(sql_path: str, output_path: str):
    """解析 SQL 文件并写入 JSONL"""
    count = 0
    error_count = 0
    filtered_low_quality = 0
    filtered_low_ratio = 0

    with open(sql_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:

        insert_buffer = ""
        in_insert = False

        for line in infile:
            stripped = line.strip()

            if 'INSERT INTO `T_ARTICLECONTENT` VALUES' in stripped:
                in_insert = True
                insert_buffer = stripped
            elif in_insert:
                insert_buffer += " " + stripped

            # 遇到分号表示 INSERT 语句结束
            if in_insert and stripped.endswith(';'):
                in_insert = False

                # 提取 VALUES 部分
                prefix = "INSERT INTO `T_ARTICLECONTENT` VALUES"
                if insert_buffer.startswith(prefix):
                    values_str = insert_buffer[len(prefix):].strip()
                else:
                    values_str = insert_buffer
                    idx = insert_buffer.upper().find('VALUES')
                    if idx >= 0:
                        values_str = insert_buffer[idx + 6:].strip()

                # 去掉结尾的分号
                if values_str.endswith(';'):
                    values_str = values_str[:-1]

                # 使用正则表达式匹配每个 VALUES 元组
                # 格式：(id,content,articleId,sort,field1,field2,field3,field4,field5)
                # content 字段可能包含单引号包围的 HTML
                # 使用这个模式：匹配 (, 然后非贪婪匹配内容, 直到遇到完整的 )
                # 由于 VALUES 用逗号分隔，我们需要一个能够正确处理嵌套括号的模式

                # 更简单的方法：使用正则找到所有完整的元组
                # 匹配 (数字,单引号内容,数字,...) 格式
                # 由于 content 可能很长，我们用平衡括号的方式找元组

                pos = 0
                while pos < len(values_str):
                    # 找到 ( 开始
                    if values_str[pos] != '(':
                        pos += 1
                        continue

                    # 找到对应的 ) - 使用深度计数
                    depth = 1
                    end = pos + 1
                    while end < len(values_str) and depth > 0:
                        if values_str[end] == '(':
                            depth += 1
                        elif values_str[end] == ')':
                            depth -= 1
                        elif values_str[end] == "'":
                            # 跳过引号内部的内容
                            end += 1
                            while end < len(values_str) and values_str[end] != "'":
                                if values_str[end] == '\\' and end + 1 < len(values_str):
                                    end += 2  # 跳过转义字符
                                else:
                                    end += 1
                        end += 1

                    if depth == 0:
                        value_tuple = values_str[pos:end].strip()
                        try:
                            # 使用正则分割字段
                            # 字段格式：数字 或 '内容' 或 NULL
                            # 分割时忽略引号内部的逗号
                            fields = []
                            current = ""
                            in_quote = False

                            for i, c in enumerate(value_tuple[1:-1]):  # 去掉首尾括号
                                if c == "'" and not in_quote:
                                    in_quote = True
                                    current += c
                                elif c == "'" and in_quote:
                                    # 检查是否是 ''
                                    if i + 1 < len(value_tuple[1:-1]) and value_tuple[1:-1][i + 1] == "'":
                                        current += "''"
                                        i += 1
                                    else:
                                        in_quote = False
                                        current += c
                                elif c == ',' and not in_quote:
                                    fields.append(current.strip())
                                    current = ""
                                else:
                                    current += c

                            if current.strip():
                                fields.append(current.strip())

                            # 检查字段数量
                            if len(fields) == 9:
                                id_val = fields[0].upper() == 'NULL' and '' or fields[0].strip("'")
                                content_val = unescape_mysql(fields[1].strip("'"))

                                # 应用过滤器
                                plain = plain_text(content_val)
                                if is_low_quality_text(plain):
                                    print("质量低")
                                    filtered_low_quality += 1
                                    continue
                                if compute_chinese_ratio(plain) < 0.7:
                                    print("中文比例不合格")
                                    filtered_low_ratio += 1
                                    continue

                                obj = {
                                    "id": int(id_val) if id_val else 0,
                                    "content": plain
                                }
                                outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')
                                count += 1
                            else:
                                error_count += 1
                                if error_count <= 5:
                                    print(f"字段数错误: 期望 9 个，得到 {len(fields)}")

                        except Exception as e:
                            error_count += 1
                            if error_count <= 5:
                                print(f"解析错误: {e}")

                    pos = end

    print(f"成功导出 {count} 条记录")
    if filtered_low_quality > 0:
        print(f"过滤低质量文本: {filtered_low_quality} 条")
    if filtered_low_ratio > 0:
        print(f"过滤中文比例过低: {filtered_low_ratio} 条")
    if error_count > 0:
        print(f"解析失败 {error_count} 条")

def plain_text(text: str) -> str:
    clean_text = bleach.clean(text, tags=[], attributes=[], strip=True, strip_comments=True)
    decoded=html.unescape(clean_text)
    result = re.sub(r'\s+', '', decoded.replace('\xa0', ''))
    result = result.strip()
    return result


def compute_chinese_ratio(text: str) -> float:
    """计算中文字符（基本汉字区）占比"""
    total = len(text)
    if total == 0:
        return 0.0
    chinese_count = len(re.findall(r'[\u4e00-\u9fff\u3000-\u303f\uff01-\uff5e]', text))
    return chinese_count / total

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

if __name__ == "__main__":
    parse_sql_file(sql_file, output_file)
    print(f"JSONL 文件已生成: {output_file}")
