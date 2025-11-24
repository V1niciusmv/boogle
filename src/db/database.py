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
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    book_id TEXT NOT NULL,
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
            conn.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS id BIGSERIAL")
            conn.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS source TEXT")
            conn.execute("ALTER TABLE books ALTER COLUMN book_id TYPE TEXT USING book_id::text")
            conn.execute("UPDATE books SET source = 'gutenberg' WHERE source IS NULL")
            conn.execute("ALTER TABLE books DROP CONSTRAINT IF EXISTS books_pkey")
            conn.execute("ALTER TABLE books ADD CONSTRAINT books_pkey PRIMARY KEY (id)")
            conn.execute("ALTER TABLE books ALTER COLUMN source SET NOT NULL")
            conn.execute("ALTER TABLE books ALTER COLUMN book_id SET NOT NULL")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS books_source_book_id_idx ON books (source, book_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS books_title_idx ON books (lower(coalesce(title, '')))"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS books_author_idx ON books (lower(coalesce(author, '')))"
            )

    def upsert_book(self, metadata: Dict) -> None:
        source = metadata.get("source")
        source_book_id = str(metadata.get("book_id"))
        if not source or not source_book_id:
            raise ValueError("source and book_id are required")
        files = metadata.get("files") or []
        values = (
            source,
            source_book_id,
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
                    source, book_id, url, title, author, illustrator, release_date,
                    language, category, original_publication, credits,
                    copyright_status, downloads, files
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (source, book_id) DO UPDATE SET
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

    def get_book(self, source: str, book_id: str) -> Optional[Dict]:
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                SELECT
                    source, book_id, url, title, author, illustrator, release_date,
                    language, category, original_publication, credits,
                    copyright_status, downloads, files
                FROM books
                WHERE source = %s AND book_id = %s
                """,
                (source, book_id),
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["files"] = data.get("files") or []
        return data

    def search_books(self, query: str, limit: int = 10, source: Optional[str] = None) -> List[Dict]:
        term = f"%{query.lower()}%"
        source_filter = "AND source = %s" if source else ""
        params: List[object] = [term, term]
        if source:
            params.append(source)
        params.append(limit)
        with self.pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT source, book_id, title, url
                FROM books
                WHERE (lower(coalesce(title, '')) LIKE %s
                   OR lower(coalesce(author, '')) LIKE %s)
                {source_filter}
                ORDER BY title ASC
                LIMIT %s
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.pool.close()
