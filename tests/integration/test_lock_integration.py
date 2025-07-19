"""
Integration tests for database lock manager with actual database operations.

This module tests the integration between the lock manager and real database
operations, ensuring that concurrent access is properly prevented and that
locks work correctly with the existing pipeline and database functions.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from readwise_local_plus.config import UserConfig
from readwise_local_plus.db_operations import (
    create_database,
    get_session,
    update_readwise_last_fetch,
)
from readwise_local_plus.lock_manager import (
    DatabaseLockManager,
    LockTimeoutError,
    database_lock,
)
from readwise_local_plus.models import ReadwiseLastFetch
from readwise_local_plus.pipeline import run_pipeline_flattened_objects


class TestLockIntegrationWithDatabase:
    """Test lock manager integration with actual database operations."""

    def test_create_database_with_lock(self, tmp_path: Path) -> None:
        """Test that create_database uses lock correctly."""
        db_path = tmp_path / "test.db"

        # Should not exist initially
        assert not db_path.exists()

        create_database(db_path)

        # Database should exist after creation
        assert db_path.exists()

        # Lock file should be cleaned up
        lock_path = db_path.with_suffix(db_path.suffix + ".lock")
        assert not lock_path.exists()

    def test_concurrent_lock_prevention(self, tmp_path: Path) -> None:
        """Test that lock prevents concurrent access to same database."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        # First lock should succeed
        lock1 = DatabaseLockManager(db_path)
        lock1.acquire()

        # Second lock should timeout quickly
        lock2 = DatabaseLockManager(db_path, timeout=0.5)

        start_time = time.time()
        with pytest.raises(LockTimeoutError):
            lock2.acquire()

        elapsed = time.time() - start_time
        assert 0.4 < elapsed < 0.7  # Should timeout around 0.5 seconds

        lock1.release()

    def test_lock_integration_basic(self, tmp_path: Path) -> None:
        """Test basic lock integration with database operations."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        # Test that we can perform database operations with locks
        with database_lock(db_path):
            with get_session(db_path) as session:
                test_time = datetime.now(timezone.utc)
                update_readwise_last_fetch(session, test_time)
                session.commit()

        # Verify the operation worked
        with get_session(db_path) as session:
            last_fetch = session.get(ReadwiseLastFetch, 1)
            assert last_fetch is not None

    def test_update_readwise_last_fetch_works_correctly(self, mem_db) -> None:
        """Test that update_readwise_last_fetch works correctly."""
        session = mem_db.session
        test_time = datetime.now(timezone.utc)

        # Function should work without issues
        update_readwise_last_fetch(session, test_time)
        session.commit()

        # Verify the update worked
        last_fetch = session.get(ReadwiseLastFetch, 1)
        assert last_fetch is not None
        # Compare without timezone info for simplicity
        assert last_fetch.last_successful_fetch.replace(
            tzinfo=None
        ) == test_time.replace(tzinfo=None)

    def test_pipeline_lock_integration(self, tmp_path: Path) -> None:
        """Test that pipeline uses database locks correctly."""
        # Create user config with temporary database
        app_dir = tmp_path / "app"
        config_dir = app_dir / ".config" / "readwise-local-plus"
        config_dir.mkdir(parents=True)

        env_file = config_dir / ".env"
        env_file.write_text("READWISE_API_TOKEN=test_token")

        user_config = UserConfig(app_dir)

        # Create database first
        create_database(user_config.db_path)

        # Mock the API fetch to return empty data
        def mock_fetch(last_fetch):
            start = datetime.now(timezone.utc)
            end = datetime.now(timezone.utc)
            return [], start, end  # Empty data to avoid processing issues

        # Run pipeline - should acquire lock automatically
        run_pipeline_flattened_objects(
            user_config=user_config,
            fetch_func=mock_fetch,
        )

        # Verify database exists and lock is cleaned up
        assert user_config.db_path.exists()
        lock_path = user_config.db_path.with_suffix(
            user_config.db_path.suffix + ".lock"
        )
        assert not lock_path.exists()

    def test_lock_file_cleanup(self, tmp_path: Path) -> None:
        """Test that lock files are properly cleaned up."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        lock_path = db_path.with_suffix(db_path.suffix + ".lock")

        # Use lock and verify cleanup
        with database_lock(db_path):
            # Lock file should exist during operation
            assert lock_path.exists()

        # Lock file should be cleaned up after operation
        assert not lock_path.exists()

    def test_database_lock_protects_writes(self, tmp_path: Path) -> None:
        """Test that database lock protects write operations."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        # Sequential writes with lock should work fine
        for i in range(3):
            with database_lock(db_path):
                with get_session(db_path) as session:
                    test_time = datetime.now(timezone.utc)
                    test_time = test_time.replace(microsecond=i * 1000)

                    update_readwise_last_fetch(session, test_time)
                    session.commit()

        # Database should still be accessible and not corrupted
        with get_session(db_path) as session:
            last_fetch = session.get(ReadwiseLastFetch, 1)
            assert last_fetch is not None
            assert isinstance(last_fetch.last_successful_fetch, datetime)

    def test_lock_timeout_behavior(self, tmp_path: Path) -> None:
        """Test lock timeout behavior."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        # Acquire first lock
        lock1 = DatabaseLockManager(db_path)
        lock1.acquire()

        try:
            # Second lock should timeout
            lock2 = DatabaseLockManager(db_path, timeout=0.5)

            start_time = time.time()
            with pytest.raises(LockTimeoutError):
                lock2.acquire()

            elapsed = time.time() - start_time
            assert 0.4 < elapsed < 0.7  # Should timeout around 0.5 seconds

        finally:
            lock1.release()

    def test_stale_lock_cleanup_with_database_operations(self, tmp_path: Path) -> None:
        """Test that stale locks are cleaned up allowing database operations."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        lock_path = db_path.with_suffix(db_path.suffix + ".lock")

        # Create a stale lock file (process that doesn't exist)
        stale_lock_info = {
            "pid": 999999,  # Non-existent PID
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database_path": str(db_path),
        }

        with open(lock_path, "w") as f:
            json.dump(stale_lock_info, f)

        assert lock_path.exists()

        # Should be able to acquire lock and perform database operation
        with database_lock(db_path):
            with get_session(db_path) as session:
                test_time = datetime.now(timezone.utc)
                update_readwise_last_fetch(session, test_time)
                session.commit()

        # Verify operation succeeded
        with get_session(db_path) as session:
            last_fetch = session.get(ReadwiseLastFetch, 1)
            assert last_fetch is not None
            # Compare without timezone info for consistency
            assert last_fetch.last_successful_fetch.replace(
                tzinfo=None
            ) == test_time.replace(tzinfo=None)

        # Lock should be cleaned up
        assert not lock_path.exists()


class TestLockManagerPerformance:
    """Test performance characteristics of lock manager."""

    def test_lock_acquisition_performance(self, tmp_path: Path) -> None:
        """Test that lock acquisition is fast for uncontended locks."""
        db_path = tmp_path / "test.db"

        # Measure lock acquisition time
        start_time = time.time()

        for _ in range(100):
            with database_lock(db_path):
                pass  # Just acquire and release

        end_time = time.time()
        average_time = (end_time - start_time) / 100

        # Should be very fast (less than 10ms per operation)
        assert average_time < 0.01

    def test_lock_with_database_operations_performance(self, tmp_path: Path) -> None:
        """Test performance impact of locks on database operations."""
        db_path = tmp_path / "test.db"
        create_database(db_path)

        # Measure time with locks
        start_time = time.time()

        for i in range(10):
            with database_lock(db_path):
                with get_session(db_path) as session:
                    test_time = datetime.now(timezone.utc)
                    test_time = test_time.replace(microsecond=i * 1000)
                    update_readwise_last_fetch(session, test_time)
                    session.commit()

        end_time = time.time()
        total_time = end_time - start_time

        # Should complete reasonably quickly (less than 5 seconds)
        assert total_time < 5.0
