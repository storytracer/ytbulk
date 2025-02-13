# YTBulk Downloader

A robust Python tool for bulk downloading YouTube videos with proxy support, configurable resolution settings, and S3 storage integration.

## Features

- Bulk video download from CSV lists
- Smart proxy management with automatic testing and failover
- Configurable video resolution settings
- Concurrent downloads with thread pooling
- S3 storage integration
- Progress tracking and persistence
- Separate video and audio download options
- Comprehensive error handling and logging

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with the following settings:

```env
YTBULK_MAX_RETRIES=3
YTBULK_MAX_CONCURRENT=5
YTBULK_ERROR_THRESHOLD=10
YTBULK_TEST_VIDEO=<video_id>
YTBULK_PROXY_LIST_URL=<proxy_list_url>
YTBULK_PROXY_MIN_SPEED=1.0
YTBULK_DEFAULT_RESOLUTION=1080p
```

### Configuration Options

- `YTBULK_MAX_RETRIES`: Maximum retry attempts per download
- `YTBULK_MAX_CONCURRENT`: Maximum concurrent downloads
- `YTBULK_ERROR_THRESHOLD`: Error threshold before stopping
- `YTBULK_TEST_VIDEO`: Video ID used for proxy testing
- `YTBULK_PROXY_LIST_URL`: URL to fetch proxy list
- `YTBULK_PROXY_MIN_SPEED`: Minimum acceptable proxy speed (MB/s)
- `YTBULK_DEFAULT_RESOLUTION`: Default video resolution (360p, 480p, 720p, 1080p, 4K)

## Usage

```bash
python -m cli CSV_FILE ID_COLUMN --work-dir WORK_DIR --bucket S3_BUCKET [OPTIONS]
```

### Arguments

- `CSV_FILE`: Path to CSV file containing video IDs
- `ID_COLUMN`: Name of the column containing YouTube video IDs
- `--work-dir`: Working directory for temporary files
- `--bucket`: S3 bucket name for storage
- `--max-resolution`: Maximum video resolution (optional)
- `--video/--no-video`: Enable/disable video download
- `--audio/--no-audio`: Enable/disable audio download

### Example

```bash
python -m cli videos.csv video_id --work-dir ./downloads --bucket my-youtube-bucket --max-resolution 720p
```

## Architecture

### Core Components

1. **YTBulkConfig** (`config.py`)
   - Handles configuration loading and validation
   - Environment variable management
   - Resolution settings

2. **YTBulkProxyManager** (`proxies.py`)
   - Manages proxy pool
   - Tests proxy performance
   - Handles proxy rotation and failover
   - Persists proxy status

3. **YTBulkStorage** (`storage.py`)
   - Manages local and S3 storage
   - Handles file organization
   - Manages metadata
   - Tracks processed videos

4. **YTBulkDownloader** (`download.py`)
   - Core download functionality
   - Video format selection
   - Download process management

5. **YTBulkCLI** (`cli.py`)
   - Command-line interface
   - Progress tracking
   - Concurrent download management

### Proxy Management

The proxy system features:
- Automatic proxy testing
- Speed-based verification
- State persistence
- Automatic failover
- Concurrent proxy usage

### Storage System

Files are organized in the following structure:
```
work_dir/
├── cache/
│   └── proxies.json
└── downloads/
    └── {channel_id}/
        └── {video_id}/
            ├── {video_id}.mp4
            ├── {video_id}.m4a
            └── {video_id}.info.json
```

## Error Handling

- Comprehensive error logging
- Automatic retry mechanism
- Proxy failover
- File integrity verification
- S3 upload confirmation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License

## Dependencies

- yt-dlp: YouTube download functionality
- click: Command line interface
- python-dotenv: Environment configuration
- tqdm: Progress bars
- boto3: AWS S3 integration