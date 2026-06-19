import json
import os
import hashlib
import aiofiles
from typing import Dict, Set, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class StateManager:
    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache: Dict[str, dict] = {}
        
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
    
    async def save_cache(self) -> None:

        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            async with aiofiles.open(self.cache_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(self.cache, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def get_file_hash(self, file_path: str) -> str:

        try:
            with open(file_path, 'rb') as f:
                hash_obj = hashlib.sha256()
                while chunk := f.read(8192):
                    hash_obj.update(chunk)
                return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Error generating hash for {file_path}: {e}")
            stat = os.stat(file_path)
            return hashlib.sha256(
                f"{os.path.basename(file_path)}{stat.st_size}{stat.st_mtime}".encode()
            ).hexdigest()
    
    async def is_file_forwarded(self, file_path: str) -> bool:

        file_hash = self.get_file_hash(file_path)
        return file_hash in self.cache
    
    async def mark_file_forwarded(self, file_path: str, message_id: Optional[int] = None) -> None:
        """Mark file as forwarded"""
        file_hash = self.get_file_hash(file_path)
        self.cache[file_hash] = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'forwarded_at': datetime.now(timezone.utc).isoformat(),
            'message_id': message_id,
            'file_size': os.path.getsize(file_path)
        }
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
            del self.cache[key]
        if old_keys:
            await self.save_cache()
            logger.info(f"Cleaned up {len(old_keys)} old cache entries")
        return len(old_keys)
