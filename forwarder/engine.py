import re
import asyncio
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))

from pyrogram import filters, Client
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, ChatWriteForbidden, MediaEmpty, 
    FileReferenceExpired, UserNotParticipant, RPCError
)

from stormify import app, config

from .io_controller import FileSystemController
from .state_manager import StateManager

logger = logging.getLogger(__name__)
if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith("drivers.txt") for h in logger.handlers):
    fh = logging.FileHandler("drivers.txt")
    fh.setFormatter(logging.Formatter("[%(asctime)s - %(levelname)s] - %(name)s: %(message)s"))
    logger.addHandler(fh)

TARGET_CHAT_ID = config.TARGET_CHAT_ID
DOWNLOADS_DIR = config.DOWNLOADS_DIR
CACHE_FILE = config.CACHE_FILE
DEFAULT_INTERVAL = config.DEFAULT_INTERVAL
MAX_RETRY_ATTEMPTS = config.MAX_RETRY_ATTEMPTS
MAX_FILE_SIZE_MB = config.MAX_FILE_SIZE_MB

class UploadEngine:
    def __init__(self):
        self.io_controller = FileSystemController(DOWNLOADS_DIR)
        self.state_manager = StateManager(CACHE_FILE)
        self.is_running = False
        self.current_interval = DEFAULT_INTERVAL
        self.forwarding_task: Optional[asyncio.Task] = None
        self.schedule_enabled = True
        self.manual_override = False
        
        self.driver_client: Optional[Client] = None
        if getattr(config, "DRIVER_BOT_TOKEN", None):
            self.driver_client = Client(
                "driver_bot_session",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                bot_token=config.DRIVER_BOT_TOKEN,
            )
        else:
            logger.warning("[SYS] DRIVER_BOT_TOKEN is missing. Uploads will fail!")

        self.stats = {
            'files_forwarded': 0,
            'files_skipped': 0,
            'errors': 0,
            'started_at': None
        }

    async def test_target_chat(self, send_message: bool = False) -> bool:
        """Test if target chat is accessible."""
        if not self.driver_client:
            logger.error("[SYS] DRIVER_BOT_TOKEN is missing. Test failed.")
            return False
            
        try:
            # We must start the client temporarily to test if it's not already running
            temp_started = False
            if not self.driver_client.is_connected:
                await self.driver_client.start()
                temp_started = True
                
            chat = await self.driver_client.get_chat(TARGET_CHAT_ID)
            logger.info(f"Target chat found: {chat.title} (ID: {chat.id}, Type: {chat.type})")
            if send_message:
                test_message = await self.driver_client.send_message(
                    TARGET_CHAT_ID, 
                    "<b> ᴛᴇꜱᴛ ᴍᴇꜱꜱᴀɢᴇ\n\nʙᴏᴛ ᴄᴏɴɴᴇᴄᴛɪᴏɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ! ᴛʜɪꜱ ᴍᴇꜱꜱᴀɢᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ 5 ꜱᴇᴄᴏɴᴅꜱ.</b>"
                )
                logger.info(f"Test message sent successfully: {test_message.id}")
                await asyncio.sleep(5)
                try:
                    await self.driver_client.delete_messages(TARGET_CHAT_ID, test_message.id)
                except Exception as e:
                    logger.warning(f"Could not delete test message: {e}")
                    
            if temp_started:
                await self.driver_client.stop()
                
            return True
        except Exception as e:
            logger.error(f"Target chat test failed: {e}")
            if temp_started:
                try:
                    await self.driver_client.stop()
                except Exception:
                    pass
            return False

    async def start_forwarding(self):
        if self.is_running:
            return
            
        if not self.driver_client:
            logger.error("[SYS] Cannot start UploadEngine: DRIVER_BOT_TOKEN is not configured.")
            return

        logger.info("[SYS] Booting UploadEngine v2.0.0...")
        self.is_running = True
        self.stats['started_at'] = datetime.now(timezone.utc)
        logger.info(f"[SYS] Initializing IO Controller at {DOWNLOADS_DIR}...")
        self.io_controller.ensure_downloads_dir()
        logger.info("[SYS] Linking StateManager to memory cache...")
        await self.state_manager.load_cache()
        
        logger.info("[SYS] Starting secondary driver client...")
        try:
            await self.driver_client.start()
        except Exception as e:
            if "already" not in str(e).lower():
                logger.error(f"[SYS] Failed to start driver client: {e}")
                self.is_running = False
                return
                
        self.forwarding_task = asyncio.create_task(self._forwarding_loop())
        logger.info("[SYS] UploadEngine successfully linked to Stormify Core and is now running.")

    async def stop_forwarding(self):
        if not self.is_running:
            return
        logger.info("[SYS] Sending termination signal to UploadEngine...")
        self.is_running = False
        if self.forwarding_task:
            self.forwarding_task.cancel()
            try:
                await self.forwarding_task
            except asyncio.CancelledError:
                pass
                
        if self.driver_client:
            try:
                await self.driver_client.stop()
                logger.info("[SYS] Secondary driver client stopped.")
            except Exception as e:
                logger.warning(f"[SYS] Error stopping driver client: {e}")
                
        logger.info("[SYS] UploadEngine offline.")

    async def _forwarding_loop(self):
        while self.is_running:
            try:
                await self._process_files()
                await asyncio.sleep(self.current_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in forwarding loop: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(min(self.current_interval, 60))

    async def _process_files(self):
        files = await self.io_controller.get_all_files_async()
        for file_path in files:
            if not self.is_running:
                break
            try:
                # O(1) in-memory check without synchronous disk reads
                if await self.state_manager.is_file_forwarded(str(file_path)):
                    continue
                # Non-blocking stability check (skips files being actively downloaded)
                if not await self.io_controller.wait_for_file_stability(file_path, timeout=0):
                    continue
                file_info = self.io_controller.get_file_info(file_path)
                if file_info['size_mb'] > MAX_FILE_SIZE_MB:
                    await self.state_manager.mark_file_forwarded(str(file_path))
                    self.stats['files_skipped'] += 1
                    continue
                success = await self._forward_file(file_path, file_info)
                if success:
                    self.stats['files_forwarded'] += 1
                else:
                    self.stats['errors'] += 1
                # Small non-blocking pause between file uploads
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error processing file {file_path.name}: {e}")
                self.stats['errors'] += 1
            # Yield control to the asyncio event loop for PyTgCalls & Telegram bot responsiveness
            await asyncio.sleep(0.05)

    async def _forward_file(self, file_path: Path, file_info: dict, retry_count: int = 0) -> bool:
        if not self.driver_client:
            return False
            
        try:
            if file_info['mime_type'].startswith('image/'):
                message = await self.driver_client.send_photo(
                    chat_id=TARGET_CHAT_ID,
                    photo=str(file_path),
                    caption=f"<b> {file_info['name']}\n ꜱɪᴢᴇ: {file_info['size_mb']} ᴍʙ</b>"
                )
            elif file_info['mime_type'].startswith('video/'):
                message = await self.driver_client.send_video(
                    chat_id=TARGET_CHAT_ID,
                    video=str(file_path),
                    caption=f"<b> {file_info['name']}\n ꜱɪᴢᴇ: {file_info['size_mb']} ᴍʙ</b>"
                )
            elif file_info['mime_type'].startswith('audio/'):
                message = await self.driver_client.send_audio(
                    chat_id=TARGET_CHAT_ID,
                    audio=str(file_path),
                    caption=f"<b> {file_info['name']}\n ꜱɪᴢᴇ: {file_info['size_mb']} ᴍʙ</b>"
                )
            else:
                message = await self.driver_client.send_document(
                    chat_id=TARGET_CHAT_ID,
                    document=str(file_path),
                    caption=f"<b> {file_info['name']}\n ꜱɪᴢᴇ: {file_info['size_mb']} ᴍʙ</b>"
                )
            await self.state_manager.mark_file_forwarded(str(file_path), message.id)
            return True
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            if retry_count < MAX_RETRY_ATTEMPTS:
                return await self._forward_file(file_path, file_info, retry_count + 1)
            return False
        except (ChatWriteForbidden, UserNotParticipant):
            return False
        except (MediaEmpty, FileReferenceExpired):
            return False
        except RPCError as e:
            error_message = str(e)
            if "SLOWMODE_WAIT" in error_message:
                wait_match = re.search(r'SLOWMODE_WAIT_(\d+)', error_message)
                wait_time = int(wait_match.group(1)) if wait_match else 60
                await asyncio.sleep(wait_time + 1)
                if retry_count < MAX_RETRY_ATTEMPTS:
                    return await self._forward_file(file_path, file_info, retry_count + 1)
                return False
            elif "PEER_ID_INVALID" in error_message:
                return False
            else:
                if retry_count < MAX_RETRY_ATTEMPTS:
                    await asyncio.sleep(5 * (retry_count + 1))
                    return await self._forward_file(file_path, file_info, retry_count + 1)
                return False
        except Exception as e:
            if "TimeoutError" in str(type(e).__name__) or "Timeout" in str(e):
                logger.warning(f"[UploadEngine] Timeout detected for {file_path.name}. Retrying in background...")
            if retry_count < MAX_RETRY_ATTEMPTS:
                await asyncio.sleep(5 * (retry_count + 1))
                return await self._forward_file(file_path, file_info, retry_count + 1)
            return False

# Plugin instance
engine = UploadEngine()

async def driver_scheduler(engine: UploadEngine):
    """
    Automated Daily Driver Scheduler (3:00 AM IST - 5:00 AM IST).
    Automatically runs the engine from 03:00 to 05:00 IST and stops it at 05:00 IST every day.
    """
    logger.info("[SYS] Automated Daily Driver Scheduler started. Active window: 03:00 AM - 05:00 AM IST daily.")
    while True:
        try:
            if getattr(engine, "schedule_enabled", True):
                now_ist = datetime.now(IST)
                in_window = (3 <= now_ist.hour < 5)

                if in_window:
                    if not engine.is_running:
                        logger.info(
                            f"[SYS] Daily schedule triggered at {now_ist.strftime('%I:%M:%S %p')} IST (window: 3 AM - 5 AM IST). Starting UploadEngine..."
                        )
                        engine.manual_override = False
                        await engine.start_forwarding()
                else:
                    if engine.is_running and not getattr(engine, "manual_override", False):
                        logger.info(
                            f"[SYS] Daily schedule window ended at {now_ist.strftime('%I:%M:%S %p')} IST. Stopping UploadEngine..."
                        )
                        await engine.stop_forwarding()

            await asyncio.sleep(20)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[SYS] Driver scheduler error: {e}")
            await asyncio.sleep(60)

try:
    loop = asyncio.get_running_loop()
    loop.create_task(driver_scheduler(engine))
except RuntimeError:
    pass

@app.on_message(filters.command(["fw_start", "fw_help"]) & filters.private & app.sudoers)
async def start_command(client, message: Message):
    await message.reply(
        "<b> ꜱᴛᴏʀᴍɪꜰʏ ꜱʏꜱᴛᴇᴍ ᴅʀɪᴠᴇʀ (ᴜᴘʟᴏᴀᴅ ᴇɴɢɪɴᴇ)\n\n"
        " ᴄᴏᴍᴍᴀɴᴅꜱ:\n"
        "• /fw_status - ᴇɴɢɪɴᴇ ꜱᴛᴀᴛᴜꜱ\n"
        "• /fw_schedule - ᴅᴀɪʟʏ 3-5 ᴀᴍ ɪꜱᴛ ꜱᴄʜᴇᴅᴜʟᴇ\n"
        "• /fw_run - ɪɴɪᴛɪᴀʟɪᴢᴇ ᴇɴɢɪɴᴇ\n"
        "• /fw_stop - ᴛᴇʀᴍɪɴᴀᴛᴇ ᴇɴɢɪɴᴇ\n"
        "• /fw_interval <ꜱᴇᴄᴏɴᴅꜱ> - ꜱᴇᴛ ɪ/ᴏ ɪɴᴛᴇʀᴠᴀʟ\n"
        "• /fw_stats - ꜱʜᴏᴡ ᴛᴇʟᴇᴍᴇᴛʀʏ\n"
        "• /fw_files - ʟɪꜱᴛ ɪ/ᴏ qᴜᴇᴜᴇ\n"
        "• /fw_cleanup - ᴄʟᴇᴀʀ ꜱᴛᴀᴛᴇ ᴄᴀᴄʜᴇ\n"
        "• /fw_test - ᴛᴇꜱᴛ ᴛᴀʀɢᴇᴛ ᴘɪɴɢ\n\n"
        f" ɪ/ᴏ ᴘᴀᴛʜ: `{DOWNLOADS_DIR}`\n"
        f" ᴛᴀʀɢᴇᴛ: `[HIDDEN FOR SECURITY]`</b>"
    )

@app.on_message(filters.command("fw_status") & filters.private & app.sudoers)
async def status_command(client, message: Message):
    status = " ᴏɴʟɪɴᴇ" if engine.is_running else " ᴏꜰꜰʟɪɴᴇ"
    uptime = ""
    if engine.stats['started_at']:
        uptime_seconds = (datetime.now(timezone.utc) - engine.stats['started_at']).total_seconds()
        hours, remainder = divmod(uptime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime = f"\n ᴜᴘᴛɪᴍᴇ: {int(hours)}ʜ {int(minutes)}ᴍ {int(seconds)}ꜱ"
    
    now_ist = datetime.now(IST)
    in_window = (3 <= now_ist.hour < 5)
    sched_str = " ᴇɴᴀʙʟᴇᴅ (03:00 - 05:00 ᴀᴍ ɪꜱᴛ)" if getattr(engine, "schedule_enabled", True) else " ᴅɪꜱᴀʙʟᴇᴅ"
    win_str = " ɪɴ ᴡɪɴᴅᴏᴡ (ᴀᴄᴛɪᴠᴇ)" if in_window else " ᴏᴜᴛꜱɪᴅᴇ ᴡɪɴᴅᴏᴡ (ɪᴅʟᴇ)"
    
    await message.reply(
        f"<b> ᴇɴɢɪɴᴇ ᴛᴇʟᴇᴍᴇᴛʀʏ\n\n"
        f" ꜱᴛᴀᴛᴜꜱ: {status}\n"
        f" ꜱᴄʜᴇᴅᴜʟᴇ: {sched_str}\n"
        f" ꜱᴄʜᴇᴅᴜʟᴇ ᴡɪɴᴅᴏᴡ: {win_str}\n"
        f" ᴄᴜʀʀᴇɴᴛ ɪꜱᴛ ᴛɪᴍᴇ: {now_ist.strftime('%I:%M:%S %p')} ɪꜱᴛ\n"
        f" ɪ/ᴏ ɪɴᴛᴇʀᴠᴀʟ: {engine.current_interval}ꜱ\n"
        f" ɪ/ᴏ ᴘᴀᴛʜ: `{DOWNLOADS_DIR}`\n"
        f" ᴛᴀʀɢᴇᴛ ᴄʜᴀᴛ: `[HIDDEN]`"
        f"{uptime}</b>"
    )

@app.on_message(filters.command("fw_test") & filters.private & app.sudoers)
async def test_command(client, message: Message):
    await message.reply("<b> ᴘɪɴɢɪɴɢ ᴛᴀʀɢᴇᴛ ᴄʜᴀᴛ...</b>")
    success = await engine.test_target_chat(send_message=True)
    if success:
        await message.reply("<b> ᴘɪɴɢ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ! ᴇɴɢɪɴᴇ ᴄᴀɴ ᴡʀɪᴛᴇ ᴛᴏ ᴛᴀʀɢᴇᴛ.</b>")
    else:
        await message.reply("<b> ᴘɪɴɢ ꜰᴀɪʟᴇᴅ! ᴄʜᴇᴄᴋ ꜱʏꜱᴛᴇᴍ ᴘᴇʀᴍɪꜱꜱɪᴏɴꜱ.</b>")

@app.on_message(filters.command("fw_run") & filters.private & app.sudoers)
async def run_command(client, message: Message):
    if engine.is_running:
        return await message.reply("<b> ᴇɴɢɪɴᴇ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴏɴʟɪɴᴇ!</b>")
    await message.reply("<b> ᴠᴇʀɪꜰʏɪɴɢ ᴛᴀʀɢᴇᴛ ᴀᴄᴄᴇꜱꜱ...</b>")
    if not await engine.test_target_chat(send_message=False):
        return await message.reply("<b> ᴀᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ. ᴄʜᴇᴄᴋ ᴘᴇʀᴍɪꜱꜱɪᴏɴꜱ.</b>")
    engine.manual_override = True
    await engine.start_forwarding()
    await message.reply("<b> ᴜᴘʟᴏᴀᴅ ᴇɴɢɪɴᴇ ɪɴɪᴛɪᴀʟɪᴢᴇᴅ ᴀɴᴅ ᴏɴʟɪɴᴇ (ᴍᴀɴᴜᴀʟ ᴏᴠᴇʀʀɪᴅᴇ)!</b>")

@app.on_message(filters.command("fw_stop") & filters.private & app.sudoers)
async def stop_command(client, message: Message):
    if not engine.is_running:
        return await message.reply("<b> ᴇɴɢɪɴᴇ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴏꜰꜰʟɪɴᴇ!</b>")
    engine.manual_override = False
    await engine.stop_forwarding()
    await message.reply("<b> ᴜᴘʟᴏᴀᴅ ᴇɴɢɪɴᴇ ᴛᴇʀᴍɪɴᴀᴛᴇᴅ!</b>")

@app.on_message(filters.command("fw_schedule") & filters.private & app.sudoers)
async def schedule_command(client, message: Message):
    now_ist = datetime.now(IST)
    in_window = (3 <= now_ist.hour < 5)
    args = message.text.split()
    if len(args) > 1:
        action = args[1].lower()
        if action in ["on", "enable", "true"]:
            engine.schedule_enabled = True
            return await message.reply("<b> ᴅᴀɪʟʏ ꜱᴄʜᴇᴅᴜʟᴇ ᴇɴᴀʙʟᴇᴅ (03:00 ᴀᴍ - 05:00 ᴀᴍ ɪꜱᴛ)!</b>")
        elif action in ["off", "disable", "false"]:
            engine.schedule_enabled = False
            return await message.reply("<b> ᴅᴀɪʟʏ ꜱᴄʜᴇᴅᴜʟᴇ ᴅɪꜱᴀʙʟᴇᴅ!</b>")

    sched_status = " ᴇɴᴀʙʟᴇᴅ" if getattr(engine, "schedule_enabled", True) else " ᴅɪꜱᴀʙʟᴇᴅ"
    window_status = " ɪɴ ᴡɪɴᴅᴏᴡ (ᴀᴄᴛɪᴠᴇ)" if in_window else " ᴏᴜᴛꜱɪᴅᴇ ᴡɪɴᴅᴏᴡ (ɪᴅʟᴇ)"
    await message.reply(
        f"<b> ᴅᴀɪʟʏ ᴅʀɪᴠᴇʀ ꜱᴄʜᴇᴅᴜʟᴇʀ\n\n"
        f"• ꜱᴄʜᴇᴅᴜʟᴇ: `03:00 AM - 05:00 AM IST (Daily)`\n"
        f"• ꜱᴛᴀᴛᴜꜱ: {sched_status}\n"
        f"• ᴡɪɴᴅᴏᴡ: {window_status}\n"
        f"• ᴄᴜʀʀᴇɴᴛ ɪꜱᴛ ᴛɪᴍᴇ: `{now_ist.strftime('%I:%M:%S %p')} IST`\n\n"
        f" ᴛᴏ ᴛᴏɢɢʟᴇ: `/fw_schedule on` ᴏʀ `/fw_schedule off`</b>"
    )

@app.on_message(filters.command("fw_interval") & filters.private & app.sudoers)
async def interval_command(client, message: Message):
    try:
        new_interval = int(message.text.split()[1])
        if new_interval < 10:
            return await message.reply("<b> ᴍɪɴɪᴍᴜᴍ ɪɴᴛᴇʀᴠᴀʟ ɪꜱ 10 ꜱᴇᴄᴏɴᴅꜱ</b>")
        engine.current_interval = new_interval
        await message.reply(f"<b> ɪ/ᴏ ɪɴᴛᴇʀᴠᴀʟ ᴜᴘᴅᴀᴛᴇᴅ ᴛᴏ {new_interval} ꜱᴇᴄᴏɴᴅꜱ</b>")
    except (IndexError, ValueError):
        await message.reply("<b> ᴜꜱᴀɢᴇ: `/fw_interval <ꜱᴇᴄᴏɴᴅꜱ>`</b>")

@app.on_message(filters.command("fw_stats") & filters.private & app.sudoers)
async def stats_command(client, message: Message):
    forwarded_count = await engine.state_manager.get_forwarded_files_count()
    await message.reply(
        f"<b> ᴇɴɢɪɴᴇ ꜱᴛᴀᴛɪꜱᴛɪᴄꜱ\n\n"
        f" ᴘᴀᴄᴋᴇᴛꜱ ꜱᴇɴᴛ: {engine.stats['files_forwarded']}\n"
        f" ᴘᴀᴄᴋᴇᴛꜱ ꜱᴋɪᴘᴘᴇᴅ: {engine.stats['files_skipped']}\n"
        f" ɪ/ᴏ ᴇʀʀᴏʀꜱ: {engine.stats['errors']}\n"
        f" ꜱᴛᴀᴛᴇ ᴄᴀᴄʜᴇ ꜱɪᴢᴇ: {forwarded_count}</b>"
    )

@app.on_message(filters.command("fw_files") & filters.private & app.sudoers)
async def files_command(client, message: Message):
    files = await engine.io_controller.get_all_files_async()
    pending = []
    for f in files[:10]:
        if not await engine.state_manager.is_file_forwarded(str(f)):
            info = engine.io_controller.get_file_info(f)
            pending.append(f" `{info['name']}` ({info['size_mb']} ᴍʙ)")
    if pending:
        msg = "\n".join(pending)
        if len(files) > 10:
            msg += f"\n... ᴀɴᴅ {len(files)-len(pending)} ᴍᴏʀᴇ."
        await message.reply(f"<b> ɪ/ᴏ qᴜᴇᴜᴇ:\n\n{msg}</b>")
    else:
        await message.reply("<b> ɪ/ᴏ qᴜᴇᴜᴇ ɪꜱ ᴇᴍᴘᴛʏ!</b>")

@app.on_message(filters.command("fw_cleanup") & filters.private & app.sudoers)
async def cleanup_command(client, message: Message):
    cleaned = await engine.state_manager.cleanup_old_entries(days=30)
    await message.reply(f"<b> ᴄʟᴇᴀʀᴇᴅ {cleaned} ꜱᴛᴀʟᴇ ᴄᴀᴄʜᴇ ᴇɴᴛʀɪᴇꜱ.</b>")
