import os
from typing import Dict, List, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool


class PostgresRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or self._build_dsn()
        self.pool = ConnectionPool(self.dsn, kwargs={"row_factory": dict_row, "autocommit": True})
        self._init_db()

    def _build_dsn(self) -> str:
        url = os.getenv("DATABASE_URL")
        if url:
            return url
        user = os.getenv("POSTGRES_USER", "boogle")
        password = os.getenv("POSTGRES_PASSWORD", "boogle")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "boogle")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _init_db(self) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    illustrator TEXT,
                    release_date TEXT,
                    language TEXT,
                    category TEXT,
                    original_publication TEXT,
                    credits TEXT,
                    copyright_status TEXT,
                    downloads TEXT,
                    files JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def upsert_book(self, metadata: Dict) -> None:
        files = metadata.get("files") or []
        values = (
            metadata.get("book_id"),
            metadata.get("url"),
            metadata.get("title"),
            metadata.get("author"),
            metadata.get("illustrator"),
            metadata.get("release_date"),
            metadata.get("language"),
            metadata.get("category"),
            metadata.get("original_publication"),
            metadata.get("credits"),
            metadata.get("copyright_status"),
            metadata.get("downloads"),
            Json(files),
        )
        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO books (
                    book_id, url, title, author, illustrator, release_date,
                    language, category, original_publication, credits,
                    copyright_status, downloads, files
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (book_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    author = EXCLUDED.author,
                    illustrator = EXCLUDED.illustrator,
                    release_date = EXCLUDED.release_date,
                    language = EXCLUDED.language,
                    category = EXCLUDED.category,
                    original_publication = EXCLUDED.original_publication,
                    credits = EXCLUDED.credits,
                    copyright_status = EXCLUDED.copyright_status,
                    downloads = EXCLUDED.downloads,
                    files = EXCLUDED.files,
                    updated_at = NOW()
                """,
                values,
            )

    def get_book(self, book_id: int) -> Optional[Dict]:
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                SELECT
                    book_id, url, title, author, illustrator, release_date,
                    language, category, original_publication, credits,
                    copyright_status, downloads, files
                FROM books
                WHERE book_id = %s
                """,
                (book_id,),
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["files"] = data.get("files") or []
        return data

    def search_books(self, query: str, limit: int = 10) -> List[Dict]:
        term = f"%{query.lower()}%"
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT book_id, title, url
                FROM books
                WHERE lower(coalesce(title, '')) LIKE %s
                   OR lower(coalesce(author, '')) LIKE %s
                ORDER BY title ASC
                LIMIT %s
                """,
                (term, term, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.pool.close()
