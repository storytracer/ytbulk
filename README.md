# YTBulk

YTBulk is a high-performance YouTube bulk video downloader with S3/MinIO storage support. It allows parallel downloading of multiple videos while maintaining resumability and data integrity.

## Features

- **Parallel Processing**: Downloads multiple videos concurrently
- **Resumable**: Automatically skips already downloaded videos
- **S3/MinIO Storage**: Direct upload to S3-compatible storage
- **Format Control**: Separate video and audio downloads with optional merging
- **Resolution Control**: Configurable maximum resolution (4K to 360p)
- **Progress Tracking**: Visual progress bar for bulk downloads
- **Cache Management**: Local cache for optimized performance

## Prerequisites

- Python 3.11 or higher
- ffmpeg (for merging audio and video)
- RapidAPI key for YT-API
- MinIO/S3 access

## Installation

1. Create and activate a virtual environment:
```bash
cd ytbulk
pyenv exec python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in your project directory:

```bash
# API Configuration
YTBULK_API_KEY=your-rapidapi-key

# Download Configuration (optional)
YTBULK_CHUNK_SIZE=1048576
YTBULK_MAX_RETRIES=3
YTBULK_MAX_CONCURRENT=5
YTBULK_ERROR_THRESHOLD=10

# S3/MinIO Configuration
AWS_ACCESS_KEY_ID=your-minio-access-key
AWS_SECRET_ACCESS_KEY=your-minio-secret-key
AWS_ENDPOINT_URL=http://your-minio:9000
```

## Usage

### Basic Usage

```bash
ytbulk video_ids.txt --work-dir /tmp/ytbulk --bucket my-youtube-videos
```

### Video IDs File Format

Create a text file with one YouTube video ID per line:
```
dQw4w9WgXcQ
jNQXAC9IVRw
```

### Advanced Options

Download with specific resolution:
```bash
ytbulk video_ids.txt \
  --work-dir /tmp/ytbulk \
  --bucket my-youtube-videos \
  --max-resolution 720p \
  --max-concurrent 3
```

Download audio only:
```bash
ytbulk video_ids.txt \
  --work-dir /tmp/ytbulk \
  --bucket my-youtube-videos \
  --no-video \
  --audio \
  --no-merge
```

### Available Options

- `--work-dir`: Working directory for temporary files
- `--bucket`: S3/MinIO bucket name
- `--max-resolution`: Maximum video resolution (4K, 1080p, 720p, 480p, 360p)
- `--max-concurrent`: Maximum number of concurrent downloads
- `--video/--no-video`: Enable/disable video download
- `--audio/--no-audio`: Enable/disable audio download
- `--merge/--no-merge`: Enable/disable merging of video and audio

## Storage Structure

Files are organized in the S3 bucket as follows:
```
bucket/
└── downloads/
    └── [channel_id]/
        └── [video_id]/
            ├── [video_id].json       # Video metadata
            ├── [video_id].video.mp4  # Video file (if not merged)
            ├── [video_id].audio.m4a  # Audio file (if not merged)
            └── [video_id].mp4        # Merged file (if merged)
```

## Development

### Project Structure

```
ytbulk/
├── __init__.py
├── cli.py
├── config.py
├── resolutions.py
├── download.py
└── storage.py
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

[Insert License Information]