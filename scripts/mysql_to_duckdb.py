import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote_plus

import duckdb
import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text

# 读取配置文件
config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# Parquet 输出目录（固定到外部盘）
PARQUET_DIR = Path("/Volumes/Storage/data/parquet_file")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# JSONL 输出目录
JSONL_DIR = Path("/Volumes/Storage/data/trains_jsonl")
JSONL_DIR.mkdir(parents=True, exist_ok=True)

# 使用配置文件中的连接信息（SQLAlchemy Engine，pandas 读 SQL 更稳定）
_user = quote_plus(str(config["user"]))
_password = quote_plus(str(config["password"]))
_host = str(config["host"])
_port = int(config["port"])
_db = quote_plus(str(config["database"]))
engine = create_engine(f"mysql+pymysql://{_user}:{_password}@{_host}:{_port}/{_db}?charset=utf8mb4")

# 2️⃣ 分批读取数据并写入 Parquet
batch_size = 100000
table_name = 't_articlecontent'
parquet_files = []


def _write_parquet(df: pd.DataFrame, parquet_path: Path) -> str:
    df.to_parquet(parquet_path, index=False)
    return str(parquet_path)

def execute_jsonl_stream(start_id: int = 0) -> None:
    last_id = int(start_id)
    total = 0
    part_num = 0

    print(f"[jsonl] start -> dir={JSONL_DIR}")

    while True:
        sql = text(f"""
            SELECT id, content
            FROM {table_name}
            WHERE id > :last_id AND content != ''
            ORDER BY id
            LIMIT :limit
        """)

        with engine.connect().execution_options(stream_results=True) as conn:
            result = conn.execute(sql, {
                "last_id": last_id,
                "limit": batch_size
            })

            rows = result.fetchmany(1000)  # 每次只拿1000条进内存
            if not rows:
                break

            jsonl_path = JSONL_DIR / f"data_{part_num}.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                batch_count = 0

                while rows:
                    for row in rows:
                        _id = int(row.id)
                        content = row.content
                        obj = {"id": _id, "content": content}
                        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

                        last_id = _id
                        batch_count += 1
                        total += 1

                    rows = result.fetchmany(1000)

            print(
                f"[jsonl] wrote file={jsonl_path}, "
                f"batch_rows={batch_count}, total={total}, last_id={last_id}"
            )
            part_num += 1

        if batch_count < batch_size:
            break

    engine.dispose()
    print(f"[jsonl] done, total={total}")






def execute_jsonl(start_id: int = 0) -> None:
    """
    直接从 MySQL 导出为 JSONL（不落 parquet）。
    分页方式：id > last_id + LIMIT batch_size
    每一批生成一个 JSONL 文件，放在 JSONL_DIR 下
    """
    last_id = int(start_id)
    total = 0
    part_num = 0

    print(f"[jsonl] start -> dir={JSONL_DIR}")
    while True:
        sql = f"SELECT id, content FROM {table_name} WHERE id > {last_id} AND content != '' ORDER BY id LIMIT {batch_size}"
        df = pd.read_sql(sql, engine)
        if df.empty:
            break

        jsonl_path = JSONL_DIR / f"{table_name}_part{part_num}.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            written_in_batch = 0
            for _id, content in zip(df["id"].tolist(), df["content"].tolist()):
                obj = {"id": int(_id), "content": content}
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                written_in_batch += 1
                total += 1

        last_id = int(df["id"].max())
        print(
            f"[jsonl] wrote file={jsonl_path}, batch_rows={len(df)}, "
            f"kept_rows={written_in_batch}, total={total}, last_id={last_id}"
        )
        part_num += 1

        # 如果返回的数据少于 batch_size，说明已经读完了
        if len(df) < batch_size:
            break

    engine.dispose()
    print(f"[jsonl] done, total={total}")


def execute1():
    # 使用 id > lastId 的方式循环读取数据（比 OFFSET 更高效）
    last_id = 0
    part_num = 0
    futures = []
    max_workers = 8  # 可调：2/4/8；越大越吃 CPU/磁盘
    exec_index = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while True:
            sql = f"SELECT id, content FROM {table_name} WHERE id > {last_id} AND content != '' ORDER BY id LIMIT {batch_size}"
            df = pd.read_sql(sql, engine)

            if df.empty:
                break

            # 记录最后一条的 id，用于下次查询
            last_id = int(df["id"].max())

            # 异步写 parquet（并行提速）
            parquet_path = PARQUET_DIR / f"{table_name}_part{part_num}.parquet"
            futures.append(pool.submit(_write_parquet, df, parquet_path))
            print(f"{exec_index}提交写入 {parquet_path}, 行数: {len(df)}, 最大 id: {last_id}")
            part_num += 1
            exec_index += 1
            # 如果返回的数据少于 batch_size，说明已经读完了
            if len(df) < batch_size:
                break

        # 等待所有 parquet 写完，再给 execute2() 用
        for fut in as_completed(futures):
            parquet_files.append(fut.result())

    engine.dispose()


def execute2():
    # 3️⃣ 将 Parquet 导入 DuckDB
    print("[execute2] start")

    # 如果 parquet_files 为空，则从目录里自动发现（你注释掉 execute1() 时也能导入）
    global parquet_files
    if not parquet_files:
        parquet_files = sorted(str(p) for p in PARQUET_DIR.glob("*.parquet"))
        print(f"[execute2] parquet_files empty, discovered {len(parquet_files)} files from {PARQUET_DIR}")
    else:
        print(f"[execute2] using in-memory parquet_files ({len(parquet_files)})")

    duck_conn = duckdb.connect('/Volumes/Storage/data/test.duckdb')

    # 可以批量导入
    for pq in parquet_files:
        duck_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS article_content_zju AS
            SELECT * FROM read_parquet('{pq}')
        """)
        # 如果表已存在，追加
        duck_conn.execute(f"""
            INSERT INTO article_content_zju
            SELECT * FROM read_parquet('{pq}')
        """)

    duck_conn.close()
    print("[execute2] done")


if __name__ == '__main__':
    # execute1()
    # execute2()
    # execute_jsonl(0)
    execute_jsonl_stream(0)