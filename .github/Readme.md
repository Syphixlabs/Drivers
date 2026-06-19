# Stormify Drivers

A dynamic, side-loading plugin framework designed specifically for [Stormify](https://github.com/KEX001/Stormify).

This repository contains essential "drivers" (plugins) that seamlessly integrate with your Stormify music bot. The primary driver included in this package is an **Auto Forwarder**, which constantly monitors a specified local directory and automatically uploads and forwards new files directly to a designated Telegram chat. 

## Features
- **Native Integration:** Runs directly on the Stormify Pyrogram `app` client. No secondary bot tokens or messy configurations required.
- **Sudo Controlled:** Automatically respects your main bot's Sudo Users list for maximum security.
- **Auto Forwarding:** Smoothly uploads media and documents to a designated target chat, completely asynchronously.
- **Smart Caching:** Avoids duplicate uploads by maintaining a local cache of forwarded files.

## Automatic Installation

To use this driver, simply ensure your main Stormify `.env` has the following variables set:
```env
DRIVERS=True
DRIVERS_REPO_URL=https://github.com/Syphixlabs/Drivers
```
Stormify will automatically clone this repository, install the plugins, and make them available inside the bot upon startup!

## Configuration
The driver pulls the following configuration parameters directly from your main Stormify `.env` file:
- `TARGET_CHAT_ID`: The ID of the chat/channel where files should be forwarded.
- `DOWNLOADS_DIR`: The local folder path to monitor (default: `./downloads`).
- `CACHE_FILE`: Where the forwarder keeps its history (default: `./drivers/cache/ffiles.json`).
- `DEFAULT_INTERVAL`: How often to scan for new files, in seconds (default: `10`).

## Usage
Control the driver directly inside Telegram using the following Sudo-only commands:
- `/fw_start` / `/fw_help` - Show all available commands
- `/fw_run` - Start the auto-forwarding task
- `/fw_stop` - Stop the auto-forwarding task
- `/fw_status` - Check if the driver is currently running
- `/fw_test` - Run a connection test to the target chat
- `/fw_files` - View a list of files waiting to be uploaded
- `/fw_stats` - View upload statistics and history
