import pandas as pd
import os
import glob
from typing import Optional, List
from sqlmodel import SQLModel, Field, Session, create_engine, select
from sqlalchemy import text

# ================= 配置区域 =================
DB_URL = "postgresql://postgres:postgres123@localhost:5432/quotes_db"


# 格式: postgresql://用户名:密码@主机:端口/数据库名
# ===========================================

# ================= 1. 定义数据模型 (Entity) =================
# 这步相当于 Java 中的 @Data @TableName 实体类

class Author(SQLModel, table=True):
    __tablename__ = "authors"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)  # unique=True 对应数据库唯一约束
    slug: Optional[str] = None
    goodreads_link: Optional[str] = None


class Quote(SQLModel, table=True):
    __tablename__ = "quotes"
    id: Optional[int] = Field(default=None, primary_key=True)
    text: str
    author_id: int = Field(foreign_key="authors.id", index=True)  # 外键关联
    length: int


class Tag(SQLModel, table=True):
    __tablename__ = "tags"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)


class QuoteTagLink(SQLModel, table=True):
    """中间表：多对多关系"""
    __tablename__ = "quote_tags"
    quote_id: int = Field(foreign_key="quotes.id", primary_key=True)
    tag_id: int = Field(foreign_key="tags.id", primary_key=True)


# ================= 2. 核心逻辑 =================

def get_or_create_author(session: Session, name: str) -> int:
    """获取作者ID，不存在则创建 (类似 MP 的 saveOrUpdate)"""
    # 查询
    statement = select(Author).where(Author.name == name)
    author = session.exec(statement).first()

    if author:
        return author.id

    # 插入
    new_author = Author(name=name)
    session.add(new_author)
    session.commit()
    session.refresh(new_author)  # 刷新以获取生成的 ID
    return new_author.id


def get_or_create_tag(session: Session, name: str) -> int:
    """获取标签ID，不存在则创建"""
    statement = select(Tag).where(Tag.name == name)
    tag = session.exec(statement).first()

    if tag:
        return tag.id

    new_tag = Tag(name=name)
    session.add(new_tag)
    session.commit()
    session.refresh(new_tag)
    return new_tag.id


def save_to_pg_orm():
    # 1. 查找最新 CSV
    csv_files = glob.glob("quotes_*.csv")
    if not csv_files:
        print("❌ 未找到 CSV 文件。")
        return

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"📂 正在读取: {latest_file}")
    df = pd.read_csv(latest_file)

    # 2. 创建数据库引擎
    # echo=True 可以看到生成的 SQL 语句，调试时很有用
    engine = create_engine(DB_URL, echo=False)

    # 3. 自动建表 (如果不存在)
    # 这步替代了之前的 init_db() 和 CREATE TABLE 语句
    SQLModel.metadata.create_all(engine)
    print("✅ 数据库表结构已检查/初始化 (Authors, Quotes, Tags, Links)。")

    # 4. 开始会话 (Session) - 类似 JDBC Connection 但更高级
    with Session(engine) as session:
        print(f"🚀 开始导入 {len(df)} 条数据...")

        count_quotes = 0

        for _, row in df.iterrows():
            text = row['名言内容 (Text)']
            author_name = row['作者 (Author)']
            tags_str = row['标签 (Tags)']

            # A. 处理作者 (ORM 方式)
            author_id = get_or_create_author(session, author_name)

            # B. 插入名言
            # 检查是否已存在 (避免重复)
            existing_quote = session.exec(select(Quote).where(Quote.text == text)).first()

            if not existing_quote:
                quote = Quote(
                    text=text,
                    author_id=author_id,
                    length=len(text)
                )
                session.add(quote)
                session.commit()
                session.refresh(quote)
                quote_id = quote.id
                count_quotes += 1
            else:
                quote_id = existing_quote.id

            # C. 处理标签关联
            if isinstance(tags_str, str):
                tag_names = [t.strip() for t in tags_str.split('|') if t.strip()]
                for tag_name in tag_names:
                    tag_id = get_or_create_tag(session, tag_name)

                    # 检查关联是否存在
                    link = session.exec(
                        select(QuoteTagLink).where(
                            (QuoteTagLink.quote_id == quote_id) &
                            (QuoteTagLink.tag_id == tag_id)
                        )
                    ).first()

                    if not link:
                        link = QuoteTagLink(quote_id=quote_id, tag_id=tag_id)
                        session.add(link)
                        # 不需要每次都 commit，批量操作效率更高，但为了简单这里每行提交
                        # 实际生产中可以在循环外统一 commit

        session.commit()  # 最后统一提交剩余事务
        print(f"✅ 导入完成！新增名言: {count_quotes} 条")

        # 5. 验证查询 (使用 ORM 语法)
        print("\n🔍 随机验证 3 条数据 (ORM Query):")
        # 联查：Quote -> Author, 并聚合 Tags (稍微复杂点，这里演示简单联查)
        statement = (
            select(Quote, Author)
            .join(Author)
            .limit(3)
        )
        results = session.exec(statement).all()

        for quote, author in results:
            # 获取该名言的所有标签
            tags_stmt = select(Tag).join(QuoteTagLink).where(QuoteTagLink.quote_id == quote.id)
            tags = session.exec(tags_stmt).all()
            tag_names = ", ".join([t.name for t in tags])

            print(f"   - [{author.name}]: {quote.text[:50]}... | Tags: {tag_names}")


if __name__ == "__main__":
    save_to_pg_orm()