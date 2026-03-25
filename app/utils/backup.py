import os
import shutil
import time
from typing import Iterable, Optional

from app.utils.config import settings
from app.utils.logger import logger


def _ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def _cleanup_old_backups(retention_days: Optional[int] = None) -> None:
    """Remove backup files older than the configured retention window.

    - Uses file modification time to determine age
    - Deletes empty directories after file cleanup
    """
    base_dir = settings.BACKUP_DIR
    if not os.path.isdir(base_dir):
        return

    days = retention_days if retention_days is not None else settings.BACKUP_RETENTION_DAYS
    if days <= 0:
        # Non-positive retention means keep everything
        return

    cutoff_ts = time.time() - days * 24 * 60 * 60

    # Walk tree bottom-up so we can remove empty directories
    for root, dirs, files in os.walk(base_dir, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                mtime = os.path.getmtime(file_path)
            except OSError:
                continue

            if mtime < cutoff_ts:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed expired backup file: {file_path}")
                except OSError as exc:
                    logger.warning(f"Failed to remove backup file {file_path}: {exc}")

        # Remove directory if now empty (and not the base backup dir itself)
        if root != base_dir:
            try:
                if not os.listdir(root):
                    os.rmdir(root)
            except OSError:
                # It's fine if we can't remove it (permissions, race, etc.)
                pass


def _backup_files(filepaths: Iterable[str], subfolder: str) -> None:
    base_dir = settings.BACKUP_DIR
    dest_dir = os.path.join(base_dir, subfolder)
    _ensure_dir(dest_dir)

    for src in filepaths:
        if not src:
            continue
        if not os.path.isfile(src):
            logger.warning(f"Backup skipped, source file not found: {src}")
            continue

        dest_path = os.path.join(dest_dir, os.path.basename(src))
        try:
            # Move file so data/ stays clean once backed up
            shutil.move(src, dest_path)
            logger.info(f"Moved {src} -> {dest_path} (backup)")
        except OSError as exc:
            logger.error(f"Failed to move {src} to backup {dest_path}: {exc}")

    # Always run cleanup after a backup pass
    _cleanup_old_backups()


def backup_daily_files(date_str: str, filepaths: Iterable[str]) -> None:
    """Back up daily job CSVs under backup/daily/<YYYY-MM-DD>/"""
    subfolder = os.path.join("daily", date_str)
    _backup_files(filepaths, subfolder)


def backup_range_files(start_date: str, end_date: str, filepaths: Iterable[str]) -> None:
    """Back up range-based CSVs (e.g. weekly full ETL) under
    backup/range/<start>_to_<end>/
    """
    range_name = f"{start_date}_to_{end_date}"
    subfolder = os.path.join("range", range_name)
    _backup_files(filepaths, subfolder)
