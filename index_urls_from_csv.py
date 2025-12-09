"""
URL Indexer for Auralis Learning Center

This script reads URLs from a CSV file and indexes them to the Auralis backend.
It implements rate limiting (100 URLs per 5 minutes) and displays progress.
"""

import csv
import time
import requests
from pathlib import Path
from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass
class Config:
    """Configuration for the indexer."""
    api_url: str = "https://ae-backend-dashboard-service-prod.api.auralis.ai/learning-center/index"
    widget_id: str = "d64219b2-eaf8-4599-8c43-4d5155909a0c"
    data_type_id: str = "669b9227-fc26-4f18-b38d-5953883742b7"
    source_type: str = "web"
    batch_size: int = 100
    rate_limit_seconds: int = 180  # 3 minutes

    # Auth token - UPDATE THIS before running
    auth_token: str = "YOUR_AUTH_TOKEN_HERE"


class ProgressBar:
    """Simple terminal progress bar."""

    def __init__(self, total: int, width: int = 50):
        self.total = total
        self.width = width
        self.current = 0
        self.start_time = time.time()

    def update(self, current: int, status: str = "") -> None:
        """Update and display the progress bar."""
        self.current = current
        percentage = (current / self.total) * 100 if self.total > 0 else 0
        filled = int(self.width * current / self.total) if self.total > 0 else 0
        bar = "=" * filled + "-" * (self.width - filled)

        elapsed = time.time() - self.start_time
        eta = (elapsed / current * (self.total - current)) if current > 0 else 0

        print(f"\r[{bar}] {current}/{self.total} ({percentage:.1f}%) | "
              f"Elapsed: {self._format_time(elapsed)} | "
              f"ETA: {self._format_time(eta)} | {status}", end="", flush=True)

    def _format_time(self, seconds: float) -> str:
        """Format seconds into HH:MM:SS."""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def finish(self) -> None:
        """Complete the progress bar."""
        print()


class URLIndexer:
    """Handles indexing URLs to the Auralis backend."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self._setup_session()

    def _setup_session(self) -> None:
        """Configure the HTTP session with required headers."""
        self.session.headers.update({
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "authorization": f"Bearer {self.config.auth_token}",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "origin": "https://app.auralis.ai",
            "pragma": "no-cache",
            "referer": "https://app.auralis.ai/",
            "x-auralis-app": "dashboard",
        })

    def index_url(self, url: str) -> tuple[bool, str]:
        """
        Index a single URL to the backend.

        Returns:
            Tuple of (success: bool, message: str)
        """
        payload = {
            "widget_id": self.config.widget_id,
            "url": [{"url": url, "isForceUpdate": False}],
            "sourceType": self.config.source_type,
            "import_full_website": False,
            "dataTypeIds": [self.config.data_type_id]
        }

        try:
            response = self.session.post(
                self.config.api_url,
                json=payload,
                timeout=30
            )

            if response.status_code in (200, 201):
                return True, "OK"
            elif response.status_code == 401:
                return False, "Auth expired"
            else:
                return False, f"HTTP {response.status_code}"

        except requests.exceptions.Timeout:
            return False, "Timeout"
        except requests.exceptions.RequestException as e:
            return False, str(e)[:50]


def read_urls_from_csv(file_path: Path) -> Iterator[str]:
    """
    Read URLs from CSV file.

    Yields:
        URL strings from the CSV
    """
    with open(file_path, "r", encoding="utf-8") as f:
        # Skip header
        next(f)
        for line in f:
            # Find URL in line - it's between the first ,"" and the next ""
            # Format: "Title,""URL"",""Date"""
            line = line.strip()
            if not line:
                continue

            # Extract URL using pattern matching
            start_marker = ',""http'
            end_marker = '"","'

            start_idx = line.find(',""http')
            if start_idx == -1:
                continue

            # Get from http onwards
            url_start = start_idx + 3  # Skip ,"" to get to http
            remaining = line[url_start:]

            # Find end of URL
            end_idx = remaining.find('""')
            if end_idx == -1:
                continue

            url = remaining[:end_idx]
            if url.startswith("http"):
                yield url


def count_urls(file_path: Path) -> int:
    """Count total URLs in the CSV file."""
    count = 0
    for _ in read_urls_from_csv(file_path):
        count += 1
    return count


def wait_with_countdown(seconds: int, progress: ProgressBar, processed: int) -> None:
    """Display countdown while waiting for rate limit."""
    for remaining in range(seconds, 0, -1):
        progress.update(processed, f"Rate limit - waiting {remaining}s")
        time.sleep(1)


def test_single_url() -> None:
    """Test indexing a single URL for debugging."""
    config = Config(
        auth_token="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InBTVndrVVhxUnlwNmxMLWJidW5ITCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXBwLmF1cmFsaXMuYWkvIiwic3ViIjoiYXV0aDB8NjZkYTIwODc3NTcxYzM5N2E4NGJlMjE3IiwiYXVkIjpbImh0dHBzOi8vYXVyYWxpcy1leGNlbGxlbmNlLWJlLWVjcy1zZXJ2aWNlLXByb2QuenVyby1wcm9kLXZwbi51cy5lMDEuYzAxLmdldHp1cm8uY29tIiwiaHR0cHM6Ly9hdXJhbGlzLWFpLXByb2QudXMuYXV0aDAuY29tL3VzZXJpbmZvIl0sImlhdCI6MTc2NTI1ODI1OSwiZXhwIjoxNzY1MzQ0NjU5LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIG9mZmxpbmVfYWNjZXNzIiwib3JnX2lkIjoib3JnXzFVTFlEc2NPWmFmS0NyY1QiLCJhenAiOiJTMUhQZzNGTWhsZFk2c0JkVU9vY0tnNTZwUVcxS1F0cCIsInBlcm1pc3Npb25zIjpbImNyZWF0ZTpjb252ZXJzYXRpb25zXHQiLCJjcmVhdGU6d2lkZ2V0IiwiZGVsZXRlOmRvY3VtZW50IiwiZGVsZXRlOmxlYXJuaW5nLWNlbnRlciIsImRlbGV0ZTp2aWRlbyIsImRlbGV0ZTp3ZWIiLCJpbmRleDphdWRpbyIsImluZGV4OmRvY3VtZW50IiwiaW5kZXg6cXVlcnkiLCJpbmRleDp0ZXh0IiwiaW5kZXg6dmlkZW8iLCJpbmRleDp3ZWIiLCJpbmRleDp6ZW5kZXNrIiwicmVhZDpjb252ZXJzYXRpb25zIiwicmVhZDppbmRleGVkLXdlYi1kb2N1bWVudHMiLCJyZWFkOmluc2lnaHRzIiwicmVhZDpsZWFybmluZy1jZW50ZXIiLCJyZWFkOndpZGdldCIsInNlYXJjaDpkb2N1bWVudCIsInNlYXJjaDpzYXZlZC1xdWVyeSIsInNlYXJjaDp2aWRlbyIsInNlYXJjaDp3ZWIiLCJzeW5jOmRvYyIsInN5bmM6dmlkZW8iLCJzeW5jOndlYiIsInVwZGF0ZTpjb252ZXJzYXRpb25zIiwidXBkYXRlOmRvYyIsInVwZGF0ZTp3aWRnZXQiLCJ3ZWJzaXRlOmdldGFsbHBhZ2VzIl19.iFAmdhMvCzBcJtW5wApSiLNTCaywGR4TCi3V08_SqvgW8aCE5mDIvk6P1Wz-LG_o0HxzA6iY93s8Bp3u6yGmIft2_XUnkpdT4q3lIrI45SI6UW0iJmVspblrNtStPAkttaXtFzU7oVVAWsI7RLkYFVPhuGg8qtJVLXsdcUebx4eaJYo1ifO0n7ElpWdqj5ucvbtYjvBhEqeLQuMTyFv6pfmSnDLdopjOHxZ1Eo0sVJwkepgQgp4GgWLLL4x4VbMd5HvSb0UGc9-Hv2tuTEESp1YB2yWfRgqJMa6U700morrM3iOAPUDb-cwDFKJGxSiQCCCavSH6j3b1kxh4GCwIxQ"
    )

    # Test URL from the CSV
    test_url = "https://learn.microsoft.com/en-us/dynamics365/finance/finance-welcome"

    print("=" * 60)
    print("DEBUG: Testing single URL indexing")
    print("=" * 60)
    print(f"URL: {test_url}")
    print(f"API: {config.api_url}")
    print()

    indexer = URLIndexer(config)

    # Show the payload being sent
    payload = {
        "widget_id": config.widget_id,
        "url": [{"url": test_url, "isForceUpdate": False}],
        "sourceType": config.source_type,
        "import_full_website": False,
        "dataTypeIds": [config.data_type_id]
    }
    print("Payload:")
    import json
    print(json.dumps(payload, indent=2))
    print()

    print("Sending request...")
    success, message = indexer.index_url(test_url)

    print()
    print("=" * 60)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")
    print(f"Message: {message}")
    print("=" * 60)


def main(limit: Optional[int] = None) -> None:
    """Main entry point for the indexer.

    Args:
        limit: Optional limit on number of URLs to process (for testing)
    """
    # Configuration
    config = Config(
        auth_token="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InBTVndrVVhxUnlwNmxMLWJidW5ITCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXBwLmF1cmFsaXMuYWkvIiwic3ViIjoiYXV0aDB8NjZkYTIwODc3NTcxYzM5N2E4NGJlMjE3IiwiYXVkIjpbImh0dHBzOi8vYXVyYWxpcy1leGNlbGxlbmNlLWJlLWVjcy1zZXJ2aWNlLXByb2QuenVyby1wcm9kLXZwbi51cy5lMDEuYzAxLmdldHp1cm8uY29tIiwiaHR0cHM6Ly9hdXJhbGlzLWFpLXByb2QudXMuYXV0aDAuY29tL3VzZXJpbmZvIl0sImlhdCI6MTc2NTI1ODI1OSwiZXhwIjoxNzY1MzQ0NjU5LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIG9mZmxpbmVfYWNjZXNzIiwib3JnX2lkIjoib3JnXzFVTFlEc2NPWmFmS0NyY1QiLCJhenAiOiJTMUhQZzNGTWhsZFk2c0JkVU9vY0tnNTZwUVcxS1F0cCIsInBlcm1pc3Npb25zIjpbImNyZWF0ZTpjb252ZXJzYXRpb25zXHQiLCJjcmVhdGU6d2lkZ2V0IiwiZGVsZXRlOmRvY3VtZW50IiwiZGVsZXRlOmxlYXJuaW5nLWNlbnRlciIsImRlbGV0ZTp2aWRlbyIsImRlbGV0ZTp3ZWIiLCJpbmRleDphdWRpbyIsImluZGV4OmRvY3VtZW50IiwiaW5kZXg6cXVlcnkiLCJpbmRleDp0ZXh0IiwiaW5kZXg6dmlkZW8iLCJpbmRleDp3ZWIiLCJpbmRleDp6ZW5kZXNrIiwicmVhZDpjb252ZXJzYXRpb25zIiwicmVhZDppbmRleGVkLXdlYi1kb2N1bWVudHMiLCJyZWFkOmluc2lnaHRzIiwicmVhZDpsZWFybmluZy1jZW50ZXIiLCJyZWFkOndpZGdldCIsInNlYXJjaDpkb2N1bWVudCIsInNlYXJjaDpzYXZlZC1xdWVyeSIsInNlYXJjaDp2aWRlbyIsInNlYXJjaDp3ZWIiLCJzeW5jOmRvYyIsInN5bmM6dmlkZW8iLCJzeW5jOndlYiIsInVwZGF0ZTpjb252ZXJzYXRpb25zIiwidXBkYXRlOmRvYyIsInVwZGF0ZTp3aWRnZXQiLCJ3ZWJzaXRlOmdldGFsbHBhZ2VzIl19.iFAmdhMvCzBcJtW5wApSiLNTCaywGR4TCi3V08_SqvgW8aCE5mDIvk6P1Wz-LG_o0HxzA6iY93s8Bp3u6yGmIft2_XUnkpdT4q3lIrI45SI6UW0iJmVspblrNtStPAkttaXtFzU7oVVAWsI7RLkYFVPhuGg8qtJVLXsdcUebx4eaJYo1ifO0n7ElpWdqj5ucvbtYjvBhEqeLQuMTyFv6pfmSnDLdopjOHxZ1Eo0sVJwkepgQgp4GgWLLL4x4VbMd5HvSb0UGc9-Hv2tuTEESp1YB2yWfRgqJMa6U700morrM3iOAPUDb-cwDFKJGxSiQCCCavSH6j3b1kxh4GCwIxQ"
    )

    csv_path = Path(__file__).parent / "ms learn links 09122025.csv"

    # Count total URLs
    print("Counting URLs...")
    total_urls = count_urls(csv_path)

    # Apply limit if specified
    if limit:
        total_urls = min(total_urls, limit)
        print(f"Found URLs - limiting to {total_urls} for testing\n")
    else:
        print(f"Found {total_urls} URLs to index\n")

    if total_urls == 0:
        print("No URLs found. Exiting.")
        return

    # Initialize
    indexer = URLIndexer(config)
    progress = ProgressBar(total_urls)

    # Statistics
    success_count = 0
    error_count = 0
    batch_count = 0

    print("Starting indexing process...\n")

    try:
        for i, url in enumerate(read_urls_from_csv(csv_path), start=1):
            # Stop if we've reached the limit
            if limit and i > limit:
                break

            # Index the URL
            success, message = indexer.index_url(url)

            if success:
                success_count += 1
                progress.update(i, f"OK - {url[:40]}...")
            else:
                error_count += 1
                progress.update(i, f"FAIL ({message}) - {url[:30]}...")

                # Stop on auth errors
                if message == "Auth expired":
                    print("\n\nAuthentication token expired. Please update the token and restart.")
                    break

            batch_count += 1

            # Rate limiting: pause after every 100 URLs
            if batch_count >= config.batch_size and i < total_urls:
                batch_count = 0
                print()  # New line before countdown
                wait_with_countdown(config.rate_limit_seconds, progress, i)

        progress.finish()

    except KeyboardInterrupt:
        progress.finish()
        print("\n\nProcess interrupted by user.")

    # Summary
    print("\n" + "=" * 60)
    print("INDEXING COMPLETE")
    print("=" * 60)
    print(f"Total URLs processed: {success_count + error_count}")
    print(f"Successful: {success_count}")
    print(f"Failed: {error_count}")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_single_url()
    elif len(sys.argv) > 1 and sys.argv[1] == "--limit":
        # Run with a limit for testing (e.g., --limit 10)
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        main(limit=limit)
    else:
        main()
