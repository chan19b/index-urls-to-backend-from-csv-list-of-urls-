# URL Indexer for Auralis Learning Center

A Python script that reads URLs from a CSV file and indexes them to the Auralis backend with rate limiting and progress tracking.

## Features

- Batch processing with rate limiting (100 URLs per 3 minutes)
- Real-time progress bar with ETA
- Automatic retry handling
- CSV parsing for Microsoft Learn URLs

## Requirements

```bash
pip install requests
```

## Setup

1. Update the `auth_token` in the `Config` class with your Auralis authentication token
2. Place your CSV file with URLs in the project directory

## Usage

```python
# Index all URLs from CSV
python index_urls_from_csv.py

# Test with a single URL
# Uncomment test_single_url() in the script
```

## CSV Format

The script expects a CSV with URLs in the format:
```
Title,URL,Date
"Page Title","https://example.com","2025-01-01"
```

## Configuration

Edit the `Config` class to customize:
- `api_url`: Backend API endpoint
- `widget_id`: Your widget identifier
- `batch_size`: URLs per batch (default: 100)
- `rate_limit_seconds`: Wait time between batches (default: 180s)

## License

MIT
