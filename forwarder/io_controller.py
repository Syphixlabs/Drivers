"""
File management utilities
"""
import os
import time
import asyncio
import mimetypes
from typing import List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class FileSystemController:
    def __init__(self, downloads_dir: str):
        self.downloads_dir = Path(downloads_dir)
        self.supported_extensions = {
            # Documents
            '.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt',
            # Images
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg',
            # Videos
            '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v',
            # Audio
            '.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a',
            # Archives
            '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2',
            # Others
            '.apk', '.exe', '.dmg', '.iso', '.torrent'
        }
        self.temporary_markers = {'.part', '.ytdl', '.temp', '.tmp', '.aria2', '.download'}
    
    def ensure_downloads_dir(self) -> None:
        """Create downloads directory if it doesn't exist"""
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloads directory: {self.downloads_dir}")

    def _sync_get_all_files(self) -> List[Path]:
        """Scan directory synchronously inside a worker thread to prevent event loop lag."""
        if not self.downloads_dir.exists():
            return []
        
        files_with_mtime = []
        now = time.time()

        try:
            for file_path in self.downloads_dir.rglob("*"):
                name = file_path.name
                # Ignore hidden files, temporary download fragments, and active yt-dlp buffers
                if name.startswith('.'):
                    continue
                if any(marker in name.lower() for marker in self.temporary_markers):
                    continue
                if file_path.suffix.lower() not in self.supported_extensions:
                    continue

                try:
                    if not file_path.is_file():
                        continue
                    stat = file_path.stat()
                    if stat.st_size == 0:
                        continue
                    # Skip files actively written within the last 3 seconds (active downloads/streams)
                    if (now - stat.st_mtime) < 3.0:
                        continue
                    files_with_mtime.append((stat.st_mtime, file_path))
                except (OSError, IOError):
                    continue
        except Exception as e:
            logger.warning(f"Error scanning downloads directory: {e}")
            return []

        files_with_mtime.sort(key=lambda x: x[0])
        return [f for _, f in files_with_mtime]

    async def get_all_files_async(self) -> List[Path]:
        """Asynchronously get all supported files without blocking the main event loop."""
        return await asyncio.to_thread(self._sync_get_all_files)

    def get_all_files(self) -> List[Path]:
        """Synchronous file scanner (falls back to _sync_get_all_files)."""
        return self._sync_get_all_files()

    def get_file_info(self, file_path: Path) -> dict:
        """Get file information safely."""
        try:
            stat = file_path.stat()
            mime_type, _ = mimetypes.guess_type(str(file_path))
            
            return {
                'name': file_path.name,
                'path': str(file_path),
                'size': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'modified': stat.st_mtime,
                'mime_type': mime_type or 'application/octet-stream',
                'extension': file_path.suffix.lower()
            }
        except (OSError, IOError) as e:
            logger.error(f"Cannot get file info for {file_path.name}: {e}")
            return {
                'name': file_path.name,
                'path': str(file_path),
                'size': 0,
                'size_mb': 0.0,
                'modified': 0,
                'mime_type': 'application/octet-stream',
                'extension': file_path.suffix.lower()
            }

    def is_file_accessible(self, file_path: Path) -> bool:
        """Check if file is accessible and can be read."""
        try:
            if not file_path.exists() or not os.access(file_path, os.R_OK):
                return False
            if file_path.stat().st_size == 0:
                return False
            with open(file_path, 'rb') as f:
                f.read(1)
            return True
        except (OSError, IOError, PermissionError):
            return False

    async def is_file_accessible_async(self, file_path: Path) -> bool:
        """Non-blocking accessibility check."""
        return await asyncio.to_thread(self.is_file_accessible, file_path)

    async def wait_for_file_stability(self, file_path: Path, timeout: int = 10) -> bool:
        """
        Instant, non-blocking stability check.
        Verifies that file is not actively being written to without freezing the worker loop.
        """
        try:
            stat = file_path.stat()
            if stat.st_size == 0:
                return False
            # If modified within the last 3 seconds, a process is still writing to it
            if (time.time() - stat.st_mtime) < 3.0:
                return False
            return await self.is_file_accessible_async(file_path)
        except OSError:
            return False

    def get_file_stats(self) -> dict:
        """Get statistics about files in downloads directory"""
        if not self.downloads_dir.exists():
            return {'total_files': 0, 'empty_files': 0, 'valid_files': 0, 'total_size_mb': 0}
        total_files = 0
        empty_files = 0
        valid_files = 0
        total_size = 0
        try:
            for file_path in self.downloads_dir.rglob("*"):
                if (file_path.is_file() and 
                    file_path.suffix.lower() in self.supported_extensions and
                    not file_path.name.startswith('.')):
                    total_files += 1
                    try:
                        file_size = file_path.stat().st_size
                        if file_size == 0:
                            empty_files += 1
                        else:
                            valid_files += 1
                            total_size += file_size
                    except OSError:
                        continue
        except Exception as e:
            logger.error(f"Error getting file stats: {e}")
        return {
            'total_files': total_files,
            'empty_files': empty_files,
            'valid_files': valid_files,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }

    def cleanup_empty_files(self, delete: bool = False) -> int:
        """Find and optionally delete empty files"""
        empty_files = []
        if not self.downloads_dir.exists():
            return 0
        try:
            for file_path in self.downloads_dir.rglob("*"):
                if (file_path.is_file() and 
                    file_path.suffix.lower() in self.supported_extensions and
                    not file_path.name.startswith('.')):
                    try:
                        if file_path.stat().st_size == 0:
                            empty_files.append(file_path)
                            if delete:
                                file_path.unlink()
                                logger.info(f"Deleted empty file: {file_path.name}")
                    except OSError as e:
                        logger.warning(f"Cannot process file {file_path.name}: {e}")
                        continue
        except Exception as e:
            logger.error(f"Error during empty files cleanup: {e}")
        if not delete and empty_files:
            logger.info(f"Found {len(empty_files)} empty files (use delete=True to remove them)")
        return len(empty_files)
