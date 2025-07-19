"""
Tests for the database lock manager.

This module contains comprehensive unit tests for the DatabaseLockManager class,
covering lock acquisition, release, stale lock detection, timeout handling,
and cross-platform compatibility.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from readwise_local_plus.lock_manager import (
    DatabaseLockManager,
    LockError,
    LockTimeoutError,
    StaleLockError,
    database_lock,
)


class TestDatabaseLockManager:
    """Test cases for DatabaseLockManager class."""

    def test_init(self, tmp_path: Path) -> None:
        """Test DatabaseLockManager initialization."""
        db_path = tmp_path / "test.db"

        lock_manager = DatabaseLockManager(db_path)

        assert lock_manager.database_path == db_path
        assert lock_manager.lock_path == db_path.with_suffix(".db.lock")
        assert lock_manager.timeout == 30.0
        assert lock_manager.retry_interval == 0.1
        assert lock_manager.stale_lock_timeout == 300.0
        assert lock_manager._lock_file is None

    def test_init_with_custom_params(self, tmp_path: Path) -> None:
        """Test DatabaseLockManager initialization with custom parameters."""
        db_path = tmp_path / "test.db"

        lock_manager = DatabaseLockManager(
            db_path,
            timeout=60.0,
            retry_interval=0.5,
            stale_lock_timeout=600.0,
        )

        assert lock_manager.timeout == 60.0
        assert lock_manager.retry_interval == 0.5
        assert lock_manager.stale_lock_timeout == 600.0

    def test_acquire_and_release(self, tmp_path: Path) -> None:
        """Test basic lock acquisition and release."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Lock should not exist initially
        assert not lock_manager.lock_path.exists()

        # Acquire lock
        lock_manager.acquire()
        assert lock_manager.lock_path.exists()
        assert lock_manager._lock_file is not None

        # Check lock file content
        with open(lock_manager.lock_path, "r") as f:
            lock_info = json.load(f)

        assert "pid" in lock_info
        assert "timestamp" in lock_info
        assert "database_path" in lock_info
        assert lock_info["pid"] == os.getpid()
        assert lock_info["database_path"] == str(db_path)

        # Release lock
        lock_manager.release()
        assert not lock_manager.lock_path.exists()
        assert lock_manager._lock_file is None

    def test_context_manager(self, tmp_path: Path) -> None:
        """Test lock manager as context manager."""
        db_path = tmp_path / "test.db"

        with DatabaseLockManager(db_path) as lock_manager:
            assert lock_manager.lock_path.exists()
            assert lock_manager._lock_file is not None

        # Lock should be released after context exit
        assert not lock_manager.lock_path.exists()
        assert lock_manager._lock_file is None

    def test_context_manager_exception_handling(self, tmp_path: Path) -> None:
        """Test that lock is released even when exception occurs in context."""
        db_path = tmp_path / "test.db"

        with pytest.raises(ValueError):
            with DatabaseLockManager(db_path) as lock_manager:
                assert lock_manager.lock_path.exists()
                raise ValueError("Test exception")

        # Lock should be released even after exception
        assert not lock_manager.lock_path.exists()

    def test_double_acquire_same_process(self, tmp_path: Path) -> None:
        """Test that same process cannot acquire lock twice."""
        db_path = tmp_path / "test.db"
        lock_manager1 = DatabaseLockManager(db_path, timeout=1.0)
        lock_manager2 = DatabaseLockManager(db_path, timeout=1.0)

        lock_manager1.acquire()

        with pytest.raises(LockTimeoutError):
            lock_manager2.acquire()

        lock_manager1.release()

    def test_multiple_release_safe(self, tmp_path: Path) -> None:
        """Test that multiple release calls are safe."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        lock_manager.acquire()
        lock_manager.release()
        # Second release should not raise error
        lock_manager.release()

    def test_stale_lock_detection_by_pid(self, tmp_path: Path) -> None:
        """Test stale lock detection when process no longer exists."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Create fake stale lock with non-existent PID
        fake_lock_info = {
            "pid": 999999,  # Very unlikely to exist
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database_path": str(db_path),
        }

        with open(lock_manager.lock_path, "w") as f:
            json.dump(fake_lock_info, f)

        # Should detect and clean up stale lock
        assert lock_manager._is_stale_lock()
        lock_manager.acquire()
        assert lock_manager.lock_path.exists()

        lock_manager.release()

    def test_stale_lock_detection_by_age(self, tmp_path: Path) -> None:
        """Test stale lock detection when lock is too old."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path, stale_lock_timeout=0.1)

        # Create old lock
        old_timestamp = datetime.now(timezone.utc)
        old_timestamp = old_timestamp.replace(year=old_timestamp.year - 1)

        fake_lock_info = {
            "pid": os.getpid(),  # Current process
            "timestamp": old_timestamp.isoformat(),
            "database_path": str(db_path),
        }

        with open(lock_manager.lock_path, "w") as f:
            json.dump(fake_lock_info, f)

        assert lock_manager._is_stale_lock()

    def test_invalid_lock_file_format(self, tmp_path: Path) -> None:
        """Test handling of corrupted lock files."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Create corrupted lock file
        with open(lock_manager.lock_path, "w") as f:
            f.write("invalid json content")

        # Should treat as stale lock
        assert lock_manager._is_stale_lock()

        # Should be able to acquire after cleanup
        lock_manager.acquire()
        assert lock_manager.lock_path.exists()

        lock_manager.release()

    def test_lock_timeout(self, tmp_path: Path) -> None:
        """Test lock acquisition timeout."""
        db_path = tmp_path / "test.db"
        lock_manager1 = DatabaseLockManager(db_path)
        lock_manager2 = DatabaseLockManager(db_path, timeout=0.5)

        lock_manager1.acquire()

        start_time = time.time()
        with pytest.raises(LockTimeoutError):
            lock_manager2.acquire()

        elapsed = time.time() - start_time
        assert elapsed >= 0.5
        assert elapsed < 1.0  # Should not take much longer than timeout

        lock_manager1.release()

    def test_unix_file_locking(self, tmp_path: Path) -> None:
        """Test Unix-specific file locking."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        with patch(
            "readwise_local_plus.lock_manager.platform.system", return_value="Linux"
        ):
            with patch("readwise_local_plus.lock_manager.fcntl") as mock_fcntl:
                mock_fcntl.LOCK_EX = 2
                mock_fcntl.LOCK_NB = 4
                mock_fcntl.flock.return_value = None

                with patch("builtins.open", mock_open()) as mock_file:
                    mock_file_instance = mock_file.return_value
                    mock_file_instance.fileno.return_value = 123
                    lock_manager._lock_file = mock_file_instance

                    result = lock_manager._apply_file_lock()
                    assert result is True
                    mock_fcntl.flock.assert_called_once_with(
                        123, 6
                    )  # LOCK_EX | LOCK_NB

    def test_windows_file_locking(self, tmp_path: Path) -> None:
        """Test Windows-specific file locking."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        with patch(
            "readwise_local_plus.lock_manager.platform.system", return_value="Windows"
        ):
            # Mock the msvcrt module at the module level
            mock_msvcrt = MagicMock()
            mock_msvcrt.LK_NBLCK = 1
            mock_msvcrt.locking.return_value = None

            with patch(
                "readwise_local_plus.lock_manager.msvcrt", mock_msvcrt, create=True
            ):
                with patch("builtins.open", mock_open()) as mock_file:
                    mock_file_instance = mock_file.return_value
                    mock_file_instance.fileno.return_value = 123
                    lock_manager._lock_file = mock_file_instance

                    result = lock_manager._apply_file_lock()
                    assert result is True
                    mock_msvcrt.locking.assert_called_once_with(123, 1, 1)

    def test_process_running_check(self, tmp_path: Path) -> None:
        """Test process running detection."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Current process should be running
        assert lock_manager._is_process_running(os.getpid())

        # Non-existent process should not be running
        assert not lock_manager._is_process_running(999999)

    def test_create_lock_info(self, tmp_path: Path) -> None:
        """Test lock info creation."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        lock_info = lock_manager._create_lock_info()

        assert "pid" in lock_info
        assert "timestamp" in lock_info
        assert "database_path" in lock_info
        assert "hostname" in lock_info
        assert "python_version" in lock_info

        assert lock_info["pid"] == os.getpid()
        assert lock_info["database_path"] == str(db_path)

        # Verify timestamp format
        timestamp = datetime.fromisoformat(
            lock_info["timestamp"].replace("Z", "+00:00")
        )
        assert isinstance(timestamp, datetime)

    def test_cleanup_stale_lock_failure(self, tmp_path: Path) -> None:
        """Test stale lock cleanup failure handling."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Create lock file
        lock_manager.lock_path.touch()

        with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
            with pytest.raises(StaleLockError):
                lock_manager._cleanup_stale_lock()

    def test_acquire_file_creation_failure(self, tmp_path: Path) -> None:
        """Test lock acquisition when file creation fails."""
        db_path = tmp_path / "test.db"
        lock_manager = DatabaseLockManager(db_path)

        # Mock open to raise permission error
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            with pytest.raises(LockError):
                lock_manager.acquire()


class TestDatabaseLockFunction:
    """Test cases for database_lock convenience function."""

    def test_database_lock_context_manager(self, tmp_path: Path) -> None:
        """Test database_lock convenience function."""
        db_path = tmp_path / "test.db"

        with database_lock(db_path) as lock_manager:
            assert isinstance(lock_manager, DatabaseLockManager)
            assert lock_manager.lock_path.exists()

        assert not lock_manager.lock_path.exists()

    def test_database_lock_custom_params(self, tmp_path: Path) -> None:
        """Test database_lock with custom parameters."""
        db_path = tmp_path / "test.db"

        with database_lock(db_path, timeout=60.0, retry_interval=0.5) as lock_manager:
            assert lock_manager.timeout == 60.0
            assert lock_manager.retry_interval == 0.5


class TestLockManagerIntegration:
    """Integration tests for lock manager functionality."""

    def test_concurrent_lock_attempts(self, tmp_path: Path) -> None:
        """Test that concurrent lock attempts are properly serialized."""
        db_path = tmp_path / "test.db"

        # This test simulates concurrent access by using nested context managers
        # In practice, this would be across different processes/threads
        with database_lock(db_path):
            # First lock acquired

            # Second lock should timeout
            with pytest.raises(LockTimeoutError):
                with database_lock(db_path, timeout=0.5):
                    pass  # Should not reach here

    def test_lock_file_location(self, tmp_path: Path) -> None:
        """Test that lock file is created in correct location."""
        db_path = tmp_path / "subdir" / "my_database.sqlite"
        db_path.parent.mkdir()

        expected_lock_path = tmp_path / "subdir" / "my_database.sqlite.lock"

        with database_lock(db_path) as lock_manager:
            assert lock_manager.lock_path == expected_lock_path
            assert expected_lock_path.exists()

        assert not expected_lock_path.exists()

    def test_multiple_databases(self, tmp_path: Path) -> None:
        """Test that different databases can be locked simultaneously."""
        db1_path = tmp_path / "db1.sqlite"
        db2_path = tmp_path / "db2.sqlite"

        # Should be able to acquire locks on different databases simultaneously
        with database_lock(db1_path) as lock1:
            with database_lock(db2_path) as lock2:
                assert lock1.lock_path.exists()
                assert lock2.lock_path.exists()
                assert lock1.lock_path != lock2.lock_path

        assert not lock1.lock_path.exists()
        assert not lock2.lock_path.exists()
