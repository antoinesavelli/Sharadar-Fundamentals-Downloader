"""
Main entry point - just hit play!
"""
from downloader import SharadarDownloader


if __name__ == "__main__":
    downloader = SharadarDownloader()
    downloader.run()
