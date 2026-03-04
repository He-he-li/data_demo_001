from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from sqlmodel import SQLModel, Field, Session, select, create_engine
from uvicorn import run

# ================= 1. 配置与模型 (复用之前的定义) =================

DB_URL = "postgresql://postgres:postgres123@localhost:5432/quotes_db"


# --- 数据库模型 (Table Models) ---
# 这些类直接映射数据库表，包含 id 等主键
class Author(SQLModel, table=True):
    __tablename__ = "authors"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    slug: Optional[str] = None


class Tag(SQLModel, table=True):
    __tablename__ = "tags"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)


class QuoteTagLink(SQLModel, table=True):
    __tablename__ = "quote_tags"
    quote_id: int = Field(foreign_key="quotes.id", primary_key=True)
    tag_id: int = Field(foreign_key="tags.id", primary_key=True)


class Quote(SQLModel, table=True):
    __tablename__ = "quotes"
    id: Optional[int] = Field(default=None, primary_key=True)
    text: str
    author_id: int = Field(foreign_key="authors.id", index=True)
    length: int


# --- 数据传输对象 (DTO / Pydantic Models) ---
# 这些类用于 API 的输入输出，不包含数据库主键或敏感字段
# 它们可以继承自上面的 Table Models，也可以重新定义

class AuthorRead(SQLModel):
    """返回给前端的作者信息"""
    id: int
    name: str


class TagRead(SQLModel):
    """返回给前端的标签信息"""
    id: int
    name: str


class QuoteRead(SQLModel):
    """返回给前端的名言详情 (包含作者和标签)"""
    id: int
    text: str
    length: int
    author: AuthorRead
    tags: List[TagRead] = []


class QuoteCreate(SQLModel):
    """前端提交新名言时的数据结构 (可选功能)"""
    text: str
    author_name: str  # 简化：直接传作者名，后端自动关联
    tags: List[str] = []


# ================= 2. 数据库连接管理 =================

engine = create_engine(DB_URL, echo=False)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


# ================= 3. 初始化 FastAPI 应用 =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行：确保表存在
    create_db_and_tables()
    yield
    # 关闭时执行：清理资源 (如有)


app = FastAPI(
    title="Quotes API",
    description="基于 SQLModel 和 PostgreSQL 的名言警句 API",
    version="1.0.0",
    lifespan=lifespan
)


# ================= 4. 定义 API 接口 =================

@app.get("/")
def read_root():
    return {"message": "欢迎使用 Quotes API! 访问 /docs 查看文档"}


@app.get("/quotes/", response_model=List[QuoteRead], summary="获取名言列表")
def get_quotes(
        limit: int = Query(10, le=100),
        offset: int = 0,
        author_name: Optional[str] = None
):
    """
    获取名言列表。
    - **limit**: 返回数量 (最大100)
    - **offset**: 分页偏移量
    - **author_name**: (可选) 按作者名筛选
    """
    with Session(engine) as session:
        # 构建基础查询
        statement = select(Quote).offset(offset).limit(limit)

        # 如果有作者筛选，先找到作者ID
        if author_name:
            author_stmt = select(Author).where(Author.name == author_name)
            author = session.exec(author_stmt).first()
            if not author:
                return []  # 或者抛出 HTTPException(404, "Author not found")
            statement = statement.where(Quote.author_id == author.id)

        quotes = session.exec(statement).all()

        results = []
        for q in quotes:
            # 手动组装复杂对象 (因为 SQLModel 默认不自动加载关联关系，除非配置了 relationship)
            # 这里为了演示清晰，手动查询关联数据

            # 获取作者
            author = session.get(Author, q.author_id)

            # 获取标签 (通过中间表联查)
            link_stmt = select(QuoteTagLink).where(QuoteTagLink.quote_id == q.id)
            links = session.exec(link_stmt).all()
            tag_ids = [l.tag_id for l in links]
            tags = []
            if tag_ids:
                tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all()

            results.append(
                QuoteRead(
                    id=q.id,
                    text=q.text,
                    length=q.length,
                    author=AuthorRead(id=author.id, name=author.name),
                    tags=[TagRead(id=t.id, name=t.name) for t in tags]
                )
            )

        return results


@app.get("/quotes/{quote_id}", response_model=QuoteRead, summary="获取名言详情")
def get_quote(quote_id: int):
    """根据 ID 获取单条名言详情"""
    with Session(engine) as session:
        quote = session.get(Quote, quote_id)
        if not quote:
            raise HTTPException(status_code=404, detail="Quote not found")

        author = session.get(Author, quote.author_id)

        link_stmt = select(QuoteTagLink).where(QuoteTagLink.quote_id == quote_id)
        links = session.exec(link_stmt).all()
        tag_ids = [l.tag_id for l in links]
        tags = session.exec(select(Tag).where(Tag.id.in_(tag_ids))).all() if tag_ids else []

        return QuoteRead(
            id=quote.id,
            text=quote.text,
            length=quote.length,
            author=AuthorRead(id=author.id, name=author.name),
            tags=[TagRead(id=t.id, name=t.name) for t in tags]
        )


@app.get("/authors/", response_model=List[AuthorRead], summary="获取作者列表")
def get_authors():
    with Session(engine) as session:
        statement = select(Author)
        return session.exec(statement).all()


# ================= 5. 启动入口 =================

if __name__ == "__main__":
    print("🚀 正在启动 API 服务器...")
    print("📖 文档地址: http://127.0.0.1:8000/docs")
    run(app, host="127.0.0.1", port=8000)