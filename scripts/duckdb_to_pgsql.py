import duckdb
import psycopg2
from psycopg2.extras import execute_values

# ---------------- 配置区 ----------------
DUCKDB_PATH = "/Volumes/Storage/data/trains.db"
PG_DSN = "host=170.18.9.106 port=5432 dbname=postgres user=postgres password=Sudy.web123"
BATCH_SIZE = 50000
TABLE_NAME = "train_records"  # DuckDB 表名
PG_TABLE = "train_records"    # PG 目标表
# --------------------------------------

# DuckDB 类型 → PostgresSQL 类型映射
DUCKDB_TO_PG = {
    "VARCHAR": "TEXT",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE PRECISION",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMPTZ",
    "DATE": "DATE",
    "JSON": "JSONB",
}

def get_duckdb_schema(duck_cursor, table_name):
    """
    返回 DuckDB 表的列信息 [(name, type)]
    """
    schema_info = duck_cursor.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = [(col[1], DUCKDB_TO_PG.get(col[2].upper(), "TEXT")) for col in schema_info]
    return columns

def create_pg_table(pg_conn, table_name, columns):
    """
    在 PostgresSQL 中创建表
    """
    cols_def = []
    for name, pg_type in columns:
        if name == "id":
            cols_def.append(f"{name} {pg_type} PRIMARY KEY")
        else:
            cols_def.append(f"{name} {pg_type}")
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {', '.join(cols_def)}\n);"
    with pg_conn.cursor() as cur:
        cur.execute(ddl)
    pg_conn.commit()
    print(f"PostgresSQL 表 {table_name} 已创建或存在")

def fetch_batches(cursor, batch_size=BATCH_SIZE):
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

def main():
    # 连接
    duck = duckdb.connect(DUCKDB_PATH)
    pg = psycopg2.connect(PG_DSN)
    pg.autocommit = False

    # 读取 DuckDB 表结构
    duck_cursor = duck.cursor()
    columns = get_duckdb_schema(duck_cursor, TABLE_NAME)
    create_pg_table(pg, PG_TABLE, columns)

    # 批量导入
    duck_cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    total = 0
    for batch in fetch_batches(duck_cursor):
        pg_rows = []
        for r in batch:
            try:
                row = []
                for i, (col_name, col_type) in enumerate(columns):
                    val = r[i]
                    if col_type.upper() == "JSONB":
                        row.append(val if val is not None else [])
                    else:
                        row.append(val)
                pg_rows.append(tuple(row))
            except Exception as e:
                print(f"跳过一行数据: {e}")

        if pg_rows:
            with pg.cursor() as pg_cur:
                execute_values(
                    pg_cur,
                    f"INSERT INTO {PG_TABLE} ({', '.join([c[0] for c in columns])}) VALUES %s",
                    pg_rows
                )
            pg.commit()
            total += len(pg_rows)
            print(f"已导入 {total} 行")

    print("导入完成，总行数:", total)
    duck.close()
    pg.close()

if __name__ == "__main__":
    main()
