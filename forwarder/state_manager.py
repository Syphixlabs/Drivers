import json
import os
import hashlib
import asyncio
import aiofiles
from typing import Dict, Set, Optional, Tuple
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class StateManager:
    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache: Dict[str, dict] = {}
        # In-memory fast lookup caches to prevent repeated disk I/O and SHA-256 computation:
        # file_path -> (st_mtime, st_size, file_hash)
        self._stat_cache: Dict[str, Tuple[float, int, str]] = {}
        # Set of file paths already forwarded
        self._forwarded_paths: Set[str] = set()

    async def load_cache(self) -> None:
        try:
            if os.path.exists(self.cache_file):
                async with aiofiles.open(self.cache_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    self.cache = json.loads(content) if content.strip() else {}
            else:
                self.cache = {}
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            self.cache = {}

        self._forwarded_paths = {
            item['file_path'] for item in self.cache.values() if isinstance(item, dict) and 'file_path' in item
        }
        logger.info(f"[StateManager] Loaded {len(self.cache)} entries from cache ({len(self._forwarded_paths)} unique paths).")

    async def save_cache(self) -> None:
        try:
            dirname = os.path.dirname(self.cache_file)
            if dirname:
                os.makedirs(dirname, exist_ok=True)
            async with aiofiles.open(self.cache_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.cache, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Error saving cache: {e}")

    def _sync_compute_hash(self, file_path: str, stat_info: Optional[os.stat_result] = None) -> str:
        """Compute SHA256 hash in a synchronous worker thread with a 64KB buffer."""
        try:
            hash_obj = hashlib.sha256()
            with open(file_path, 'rb') as f:
                while chunk := f.read(65536):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Error generating hash for {file_path}: {e}")
            stat = stat_info or os.stat(file_path)
            return hashlib.sha256(
                f"{os.path.basename(file_path)}{stat.st_size}{stat.st_mtime}".encode()
            ).hexdigest()

    async def get_file_hash_async(self, file_path: str) -> str:
        """Asynchronously compute file hash off the main event loop thread."""
        try:
            stat = os.stat(file_path)
            if file_path in self._stat_cache:
                mtime, size, h = self._stat_cache[file_path]
                if stat.st_mtime == mtime and stat.st_size == size:
                    return h
            h = await asyncio.to_thread(self._sync_compute_hash, file_path, stat)
            self._stat_cache[file_path] = (stat.st_mtime, stat.st_size, h)
            return h
        except OSError:
            return ""

    def get_file_hash(self, file_path: str) -> str:
        """Synchronous wrapper for backward compatibility."""
        try:
            stat = os.stat(file_path)
            if file_path in self._stat_cache:
                mtime, size, h = self._stat_cache[file_path]
                if stat.st_mtime == mtime and stat.st_size == size:
                    return h
            h = self._sync_compute_hash(file_path, stat)
            self._stat_cache[file_path] = (stat.st_mtime, stat.st_size, h)
            return h
        except OSError:
            return ""

    async def is_file_forwarded(self, file_path: str) -> bool:
        """O(1) memory lookup before falling back to thread-offloaded hashing."""
        try:
            stat = os.stat(file_path)
        except (OSError, FileNotFoundError):
            return False

        # Fast Path 1: Check memory stat cache (instant 0ms)
        if file_path in self._stat_cache:
            mtime, size, cached_hash = self._stat_cache[file_path]
            if stat.st_mtime == mtime and stat.st_size == size:
                return cached_hash in self.cache

        # Fast Path 2: Check known forwarded path if file hasn't changed
        if file_path in self._forwarded_paths:
            # Verify if this path in cache has matching size
            for chash, cinfo in self.cache.items():
                if cinfo.get('file_path') == file_path and cinfo.get('file_size') == stat.st_size:
                    self._stat_cache[file_path] = (stat.st_mtime, stat.st_size, chash)
                    return True

        # Slow Path: Compute hash in a worker thread so asyncio event loop never blocks
        file_hash = await asyncio.to_thread(self._sync_compute_hash, file_path, stat)
        self._stat_cache[file_path] = (stat.st_mtime, stat.st_size, file_hash)
        return file_hash in self.cache

    async def mark_file_forwarded(
        self, file_path: str, message_id: Optional[int] = None, file_hash: Optional[str] = None
    ) -> None:
        """Mark file as forwarded without re-hashing from disk if already known."""
        try:
            stat = os.stat(file_path)
            size = stat.st_size
        except OSError:
            size = 0

        if not file_hash:
            if file_path in self._stat_cache:
                file_hash = self._stat_cache[file_path][2]
            else:
                file_hash = await self.get_file_hash_async(file_path)

        if not file_hash:
            return

        self.cache[file_hash] = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'forwarded_at': datetime.now(timezone.utc).isoformat(),
            'message_id': message_id,
            'file_size': size
        }
        self._forwarded_paths.add(file_path)
        if size > 0:
            self._stat_cache[file_path] = (stat.st_mtime, size, file_hash)
        await self.save_cache()

    async def get_forwarded_files_count(self) -> int:
        return len(self.cache)

    async def cleanup_old_entries(self, days: int = 30) -> int:
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        old_keys = []
        for key, data in self.cache.items():
            try:
                forwarded_at = datetime.fromisoformat(data['forwarded_at'])
                if forwarded_at < cutoff_date:
                    old_keys.append(key)
            except Exception:
                old_keys.append(key)
        for key in old_keys:
            data = self.cache.pop(key, None)
            if data and 'file_path' in data:
                self._forwarded_paths.discard(data['file_path'])
                self._stat_cache.pop(data['file_path'], None)
        if old_keys:
            await self.save_cache()
            logger.info(f"Cleaned up {len(old_keys)} old cache entries")
        return len(old_keys)
