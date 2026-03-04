import pandas as pd
import psycopg2
from psycopg2 import sql, extras
import os
import glob

# ================= 配置区域 =================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'quotes_db',  # 请确保这个数据库已存在
    'user': 'postgres',  # 你的用户名
    'password': 'postgres123'  # 你的密码
}


# ===========================================

def init_db(conn):
    """初始化数据库表结构"""
    with conn.cursor() as cur:
        # 1. 创建作者表
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS authors
                    (
                        id
                        SERIAL
                        PRIMARY
                        KEY,
                        name
                        VARCHAR
                    (
                        255
                    ) UNIQUE NOT NULL,
                        slug VARCHAR
                    (
                        255
                    ) UNIQUE,
                        goodreads_link VARCHAR
                    (
                        500
                    )
                        );
                    """)

        # 2. 创建名言表
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS quotes
                    (
                        id
                        SERIAL
                        PRIMARY
                        KEY,
                        text
                        TEXT
                        NOT
                        NULL,
                        author_id
                        INTEGER
                        REFERENCES
                        authors
                    (
                        id
                    ) ON DELETE CASCADE,
                        length INTEGER
                        );
                    """)

        # 3. 创建标签表
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS tags
                    (
                        id
                        SERIAL
                        PRIMARY
                        KEY,
                        name
                        VARCHAR
                    (
                        100
                    ) UNIQUE NOT NULL
                        );
                    """)

        # 4. 创建名言-标签关联表 (多对多关系)
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS quote_tags
                    (
                        quote_id
                        INTEGER
                        REFERENCES
                        quotes
                    (
                        id
                    ) ON DELETE CASCADE,
                        tag_id INTEGER REFERENCES tags
                    (
                        id
                    )
                      ON DELETE CASCADE,
                        PRIMARY KEY
                    (
                        quote_id,
                        tag_id
                    )
                        );
                    """)

        conn.commit()
        print("✅ 数据库表结构初始化完成 (authors, quotes, tags, quote_tags)。")


def get_or_create_author(cur, name, slug=None, link=None):
    """获取作者ID，如果不存在则创建"""
    # 尝试查找
    cur.execute("SELECT id FROM authors WHERE name = %s", (name,))
    result = cur.fetchone()
    if result:
        return result[0]

    # 不存在则插入
    cur.execute("""
                INSERT INTO authors (name, slug, goodreads_link)
                VALUES (%s, %s, %s) RETURNING id
                """, (name, slug, link))
    return cur.fetchone()[0]


def get_or_create_tag(cur, tag_name):
    """获取标签ID，如果不存在则创建"""
    cur.execute("SELECT id FROM tags WHERE name = %s", (tag_name,))
    result = cur.fetchone()
    if result:
        return result[0]

    cur.execute("INSERT INTO tags (name) VALUES (%s) RETURNING id", (tag_name,))
    return cur.fetchone()[0]


def save_to_pg():
    # 1. 查找最新 CSV
    csv_files = glob.glob("quotes_*.csv")
    if not csv_files:
        print("❌ 未找到 CSV 文件，请先运行爬虫脚本。")
        return

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"📂 正在读取: {latest_file}")
    df = pd.read_csv(latest_file)

    # 2. 连接数据库
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"🔗 成功连接到 PostgreSQL 数据库: {DB_CONFIG['database']}")
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        print("💡 提示: 请检查 DB_CONFIG 配置及数据库是否已创建。")
        return

    try:
        with conn:  # 自动处理事务提交/回滚
            with conn.cursor() as cur:
                # 初始化表
                init_db(conn)

                print(f"🚀 开始导入 {len(df)} 条数据...")

                count_quotes = 0
                count_authors = 0
                count_tags = 0

                for _, row in df.iterrows():
                    text = row['名言内容 (Text)']
                    author_name = row['作者 (Author)']
                    tags_str = row['标签 (Tags)']

                    # A. 处理作者
                    # 注意：CSV中没有slug和link，这里暂填None，实际项目中可从原始JSON获取
                    author_id = get_or_create_author(cur, author_name)

                    # B. 插入名言
                    cur.execute("""
                                INSERT INTO quotes (text, author_id, length)
                                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING -- 防止重复插入完全相同的记录（可选）
                        RETURNING id
                                """, (text, author_id, len(text)))

                    # 检查是否插入成功 (ON CONFLICT 可能返回空)
                    res = cur.fetchone()
                    if res:
                        quote_id = res[0]
                        count_quotes += 1
                    else:
                        # 如果冲突了，查询现有的 ID
                        cur.execute("SELECT id FROM quotes WHERE text = %s", (text,))
                        quote_id = cur.fetchone()[0]

                    # C. 处理标签 (多对多)
                    if isinstance(tags_str, str):
                        tag_names = [t.strip() for t in tags_str.split('|')]
                        for tag_name in tag_names:
                            if not tag_name: continue
                            tag_id = get_or_create_tag(cur, tag_name)

                            # 插入关联表 (忽略重复)
                            cur.execute("""
                                        INSERT INTO quote_tags (quote_id, tag_id)
                                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                                        """, (quote_id, tag_id))
                            count_tags += 1

                print(f"✅ 导入完成！")
                print(f"   - 新增/确认名言: {count_quotes} 条")
                print(f"   - 处理标签关联: {count_tags} 次")

                # 验证查询
                print("\n🔍 随机验证 3 条数据 (SQL Query):")
                cur.execute("""
                            SELECT q.text, a.name, string_agg(t.name, ', ') as all_tags
                            FROM quotes q
                                     JOIN authors a ON q.author_id = a.id
                                     JOIN quote_tags qt ON q.id = qt.quote_id
                                     JOIN tags t ON qt.tag_id = t.id
                            GROUP BY q.id, a.name LIMIT 3;
                            """)
                for record in cur.fetchall():
                    print(f"   - [{record[1]}]: {record[0][:50]}... | Tags: {record[2]}")

    except Exception as e:
        print(f"❌ 导入过程中发生错误: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("🔒 数据库连接已关闭。")


if __name__ == "__main__":
    save_to_pg()