"""
Database concurrency management using file-based locks.

This module provides a file-based locking mechanism to prevent concurrent database
writes that could result in corruption or inconsistent state. The lock manager uses
platform-specific file locking (fcntl on Unix, msvcrt on Windows) combined with
process ID tracking for robust concurrency control.

Key features:
- Cross-platform file locking
- Process ID tracking for stale lock detection
- Configurable timeouts and retry logic
- Context manager support for automatic cleanup
- Integration with existing logging framework
"""

import json
import logging
import os
import platform
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Union

logger = logging.getLogger(__name__)

# Platform-specific imports
if platform.system() == "Windows":
    try:
        import msvcrt
    except ImportError:
        msvcrt = None  # type: ignore[assignment]
else:
    try:
        import fcntl
    except ImportError:
        fcntl = None  # type: ignore[assignment]


class LockError(Exception):
    """Base exception for lock-related errors."""

    pass


class LockTimeoutError(LockError):
    """Raised when lock acquisition times out."""

    pass


class StaleLockError(LockError):
    """Raised when a stale lock is detected."""

    pass


class DatabaseLockManager:
    """
    File-based lock manager for database operations.

    Provides cross-platform file locking to prevent concurrent database writes.
    Uses process ID tracking and timestamps for stale lock detection and cleanup.

    Parameters
    ----------
    database_path : Union[str, Path]
        Path to the database file. Lock file will be created with .lock extension.
    timeout : float, default=30.0
        Maximum time in seconds to wait for lock acquisition.
    retry_interval : float, default=0.1
        Time in seconds to wait between lock acquisition attempts.
    stale_lock_timeout : float, default=300.0
        Time in seconds after which a lock is considered stale (5 minutes).

    Attributes
    ----------
    lock_path : Path
        Path to the lock file.
    timeout : float
        Maximum lock acquisition timeout.
    retry_interval : float
        Retry interval for lock acquisition attempts.
    stale_lock_timeout : float
        Threshold for considering locks stale.
    _lock_file : Optional[Any]
        Handle to the locked file (platform-specific).

    Examples
    --------
    Using as context manager (recommended):

    >>> with DatabaseLockManager("/path/to/db.sqlite") as lock:
    ...     # Perform database operations
    ...     session.commit()

    Manual lock management:

    >>> lock_manager = DatabaseLockManager("/path/to/db.sqlite")
    >>> try:
    ...     lock_manager.acquire()
    ...     # Perform database operations
    ... finally:
    ...     lock_manager.release()
    """

    def __init__(
        self,
        database_path: Union[str, Path],
        timeout: float = 30.0,
        retry_interval: float = 0.1,
        stale_lock_timeout: float = 300.0,
    ):
        self.database_path = Path(database_path)
        self.lock_path = self.database_path.with_suffix(
            self.database_path.suffix + ".lock"
        )
        self.timeout = timeout
        self.retry_interval = retry_interval
        self.stale_lock_timeout = stale_lock_timeout
        self._lock_file: Optional[Any] = None

    def acquire(self) -> None:
        """
        Acquire the database lock.

        Attempts to acquire the lock file with the configured timeout and retry
        interval. Performs stale lock detection and cleanup if necessary.

        Raises
        ------
        LockTimeoutError
            If the lock cannot be acquired within the timeout period.
        LockError
            If there's an error during lock acquisition.
        StaleLockError
            If a stale lock is detected but cannot be cleaned up.
        """
        logger.debug(f"Attempting to acquire lock: {self.lock_path}")
        start_time = time.time()

        while time.time() - start_time < self.timeout:
            try:
                self._attempt_lock_acquisition()
                logger.debug(f"Lock acquired: {self.lock_path}")
                return
            except LockError:
                # Check for stale lock
                if self._is_stale_lock():
                    logger.info(f"Detected stale lock: {self.lock_path}")
                    self._cleanup_stale_lock()
                    continue

                # Wait before retrying
                time.sleep(self.retry_interval)

        raise LockTimeoutError(
            f"Failed to acquire lock {self.lock_path} within {self.timeout} seconds"
        )

    def release(self) -> None:
        """
        Release the database lock.

        Closes the lock file handle and removes the lock file. Safe to call
        multiple times.
        """
        if self._lock_file is not None:
            try:
                self._lock_file.close()
                logger.debug(f"Lock file handle closed: {self.lock_path}")
            except Exception as e:
                logger.warning(f"Error closing lock file handle: {e}")
            finally:
                self._lock_file = None

        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
                logger.debug(f"Lock file removed: {self.lock_path}")
        except Exception as e:
            logger.warning(f"Error removing lock file: {e}")

    def _attempt_lock_acquisition(self) -> None:
        """
        Attempt to acquire the lock file.

        Creates a lock file with process information and applies platform-specific
        file locking.

        Raises
        ------
        LockError
            If the lock cannot be acquired.
        """
        try:
            # Check if lock already exists
            if self.lock_path.exists():
                raise LockError("Lock file already exists")

            # Create lock file with process information
            lock_info = self._create_lock_info()

            # Create the lock file directly (no temp file for simplicity)
            self._lock_file = open(self.lock_path, "w")

            # Apply platform-specific locking
            if not self._apply_file_lock():
                self._lock_file.close()
                self.lock_path.unlink(missing_ok=True)
                self._lock_file = None
                raise LockError("Failed to apply file lock")

            # Write lock information
            json.dump(lock_info, self._lock_file, indent=2)
            self._lock_file.flush()

        except (OSError, IOError) as e:
            if self._lock_file:
                try:
                    self._lock_file.close()
                except Exception:
                    pass
                self._lock_file = None
            self.lock_path.unlink(missing_ok=True)
            raise LockError(f"Failed to acquire lock: {e}")

    def _apply_file_lock(self) -> bool:
        """
        Apply platform-specific file locking.

        Returns
        -------
        bool
            True if lock was successfully applied, False otherwise.
        """
        if platform.system() == "Windows" and msvcrt:
            try:
                assert self._lock_file is not None
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                return True
            except IOError:
                return False
        elif fcntl:
            try:
                assert self._lock_file is not None
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except IOError:
                return False
        else:
            # Fallback to simple existence check (less robust)
            logger.warning("Platform-specific locking not available, using fallback")
            return not self.lock_path.exists()

    def _create_lock_info(self) -> Dict[str, Any]:
        """
        Create lock information dictionary.

        Returns
        -------
        Dict[str, Any]
            Dictionary containing process ID, timestamp, and other metadata.
        """
        return {
            "pid": os.getpid(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database_path": str(self.database_path),
            "hostname": platform.node(),
            "python_version": platform.python_version(),
        }

    def _is_stale_lock(self) -> bool:
        """
        Check if the current lock is stale.

        A lock is considered stale if:
        1. The lock file exists
        2. The process that created it is no longer running
        3. Or the lock is older than the stale timeout

        Returns
        -------
        bool
            True if the lock is stale, False otherwise.
        """
        try:
            if not self.lock_path.exists():
                return False
        except (OSError, PermissionError):
            # If we can't even check if the file exists, assume it's stale
            return True

        try:
            with open(self.lock_path, "r") as f:
                lock_info = json.load(f)

            # Check if the process is still running
            lock_pid = lock_info.get("pid")
            if lock_pid and not self._is_process_running(lock_pid):
                return True

            # Check timestamp
            timestamp_str = lock_info.get("timestamp")
            if timestamp_str:
                lock_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                age = datetime.now(timezone.utc) - lock_time
                if age.total_seconds() > self.stale_lock_timeout:
                    return True

        except (
            json.JSONDecodeError,
            KeyError,
            ValueError,
            OSError,
            PermissionError,
        ) as e:
            logger.warning(f"Invalid or inaccessible lock file: {e}")
            return True

        return False

    def _is_process_running(self, pid: int) -> bool:
        """
        Check if a process with the given PID is running.

        Parameters
        ----------
        pid : int
            Process ID to check.

        Returns
        -------
        bool
            True if the process is running, False otherwise.
        """
        try:
            os.kill(pid, 0)  # Signal 0 doesn't kill, just checks existence
            return True
        except OSError:
            return False

    def _cleanup_stale_lock(self) -> None:
        """
        Clean up a stale lock file.

        Raises
        ------
        StaleLockError
            If the stale lock cannot be cleaned up.
        """
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
                logger.info(f"Cleaned up stale lock: {self.lock_path}")
        except OSError as e:
            raise StaleLockError(f"Failed to clean up stale lock: {e}")

    def __enter__(self) -> "DatabaseLockManager":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.release()


@contextmanager
def database_lock(
    database_path: Union[str, Path],
    timeout: float = 30.0,
    retry_interval: float = 0.1,
) -> Generator[DatabaseLockManager, None, None]:
    """
    Context manager for database locking.

    Convenience function that creates and manages a DatabaseLockManager instance.

    Parameters
    ----------
    database_path : Union[str, Path]
        Path to the database file.
    timeout : float, default=30.0
        Maximum time in seconds to wait for lock acquisition.
    retry_interval : float, default=0.1
        Time in seconds to wait between lock acquisition attempts.

    Yields
    ------
    DatabaseLockManager
        The lock manager instance.

    Examples
    --------
    >>> with database_lock("/path/to/db.sqlite") as lock:
    ...     # Perform database operations
    ...     session.commit()
    """
    lock_manager = DatabaseLockManager(
        database_path=database_path,
        timeout=timeout,
        retry_interval=retry_interval,
    )

    with lock_manager:
        yield lock_manager
