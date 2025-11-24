from src.scraper.scraper import GutenbergScraper

sources = {"gutenberg": GutenbergScraper()}


def get_sources():
    return sources
