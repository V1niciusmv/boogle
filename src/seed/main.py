import os

from src.db import PostgresRepository
from src.seed.service import SeedService
from src.sources import get_sources


def main():
    source = os.getenv("SEED_SOURCE")
    limit_value = os.getenv("SEED_LIMIT")
    limit = int(limit_value) if limit_value else None
    sources = get_sources()
    service = SeedService(PostgresRepository(), sources)
    service.seed(source.lower() if source else None, limit)


if __name__ == "__main__":
    main()
