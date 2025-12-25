"""
Connection pooling for efficient database connection management.

Provides thread-safe connection pooling with health checks, statistics,
and automatic cleanup for production workloads.
"""

import threading
import time
import logging
from typing import Optional, Callable, Dict, Any, List
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Queue, Empty, Full

from ..base import BaseConnection, ConnectionState
from ..exceptions import (
    ConnectionError,
    PoolExhaustedError,
    ConfigurationError,
)

logger = logging.getLogger(__name__)


class PooledConnection:
    """
    Wrapper for a connection in the pool.

    Tracks metadata about connection usage and health.
    """

    def __init__(self, connection: BaseConnection, pool: 'ConnectionPool'):
        """
        Initialize pooled connection.

        Args:
            connection: Underlying connection instance
            pool: Parent pool instance
        """
        self.connection = connection
        self.pool = pool
        self.created_at = datetime.now()
        self.last_used_at = datetime.now()
        self.use_count = 0
        self.is_healthy = True

    def mark_used(self):
        """Mark connection as used (updates timestamp and counter)."""
        self.last_used_at = datetime.now()
        self.use_count += 1

    def is_stale(self, max_idle_seconds: int) -> bool:
        """
        Check if connection is stale (idle too long).

        Args:
            max_idle_seconds: Maximum idle time in seconds

        Returns:
            True if connection is stale
        """
        idle_duration = (datetime.now() - self.last_used_at).total_seconds()
        return idle_duration > max_idle_seconds

    def is_expired(self, max_lifetime_seconds: int) -> bool:
        """
        Check if connection has exceeded maximum lifetime.

        Args:
            max_lifetime_seconds: Maximum connection lifetime in seconds

        Returns:
            True if connection is expired
        """
        lifetime = (datetime.now() - self.created_at).total_seconds()
        return lifetime > max_lifetime_seconds

    def validate(self) -> bool:
        """
        Validate connection health.

        Returns:
            True if connection is healthy
        """
        try:
            return self.connection.test_connection()
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            self.is_healthy = False
            return False

    def close(self):
        """Close underlying connection."""
        try:
            self.connection.disconnect()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


class ConnectionPool:
    """
    Thread-safe connection pool for database connections.

    Features:
    - Configurable min/max pool size
    - Connection health checks before reuse
    - Automatic cleanup of stale connections
    - Connection lifetime management
    - Pool statistics and monitoring
    - Thread-safe operations
    - Context manager support

    Configuration:
        connection_factory: Callable that creates new connections
        min_size: Minimum number of connections to maintain (default: 2)
        max_size: Maximum number of connections (default: 10)
        max_idle_seconds: Max idle time before cleanup (default: 300)
        max_lifetime_seconds: Max connection lifetime (default: 3600)
        validate_on_checkout: Validate connection health on checkout (default: True)
        wait_timeout: Max seconds to wait for available connection (default: 30)

    Examples:
        >>> def create_conn():
        ...     return BigQueryConnection(config)

        >>> pool = ConnectionPool(
        ...     connection_factory=create_conn,
        ...     min_size=2,
        ...     max_size=10
        ... )

        >>> # Context manager (recommended)
        >>> with pool.get_connection() as conn:
        ...     schema = conn.get_target_schema('users')

        >>> # Manual checkout/checkin
        >>> conn = pool.checkout()
        >>> try:
        ...     schema = conn.get_target_schema('users')
        ... finally:
        ...     pool.checkin(conn)

        >>> # Statistics
        >>> stats = pool.get_stats()
        >>> print(f"Active: {stats['active']}, Idle: {stats['idle']}")

        >>> # Cleanup
        >>> pool.close()
    """

    def __init__(
        self,
        connection_factory: Callable[[], BaseConnection],
        min_size: int = 2,
        max_size: int = 10,
        max_idle_seconds: int = 300,
        max_lifetime_seconds: int = 3600,
        validate_on_checkout: bool = True,
        wait_timeout: int = 30,
    ):
        """
        Initialize connection pool.

        Args:
            connection_factory: Function that creates new connections
            min_size: Minimum pool size
            max_size: Maximum pool size
            max_idle_seconds: Max idle time before cleanup (seconds)
            max_lifetime_seconds: Max connection lifetime (seconds)
            validate_on_checkout: Validate connections before checkout
            wait_timeout: Max wait time for connection (seconds)

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if min_size < 0:
            raise ConfigurationError("min_size must be >= 0")
        if max_size < 1:
            raise ConfigurationError("max_size must be >= 1")
        if min_size > max_size:
            raise ConfigurationError("min_size must be <= max_size")

        self.connection_factory = connection_factory
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_seconds = max_idle_seconds
        self.max_lifetime_seconds = max_lifetime_seconds
        self.validate_on_checkout = validate_on_checkout
        self.wait_timeout = wait_timeout

        # Pool state
        self._pool: Queue = Queue(maxsize=max_size)
        self._all_connections: List[PooledConnection] = []
        self._lock = threading.RLock()
        self._closed = False

        # Statistics
        self._stats = {
            'total_created': 0,
            'total_checkouts': 0,
            'total_checkins': 0,
            'total_validations_failed': 0,
            'total_cleanups': 0,
        }

        # Initialize minimum connections
        self._initialize_pool()

        # Start background cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="ConnectionPool-Cleanup"
        )
        self._cleanup_thread.start()

        logger.info(
            f"Connection pool initialized: min={min_size}, max={max_size}, "
            f"max_idle={max_idle_seconds}s, max_lifetime={max_lifetime_seconds}s"
        )

    def _initialize_pool(self):
        """Initialize pool with minimum connections."""
        for _ in range(self.min_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn, block=False)
            except Exception as e:
                logger.error(f"Failed to initialize pool connection: {e}")

    def _create_connection(self) -> PooledConnection:
        """
        Create a new pooled connection.

        Returns:
            PooledConnection instance

        Raises:
            ConnectionError: If connection creation fails
        """
        try:
            connection = self.connection_factory()
            connection.connect()

            pooled_conn = PooledConnection(connection, self)

            with self._lock:
                self._all_connections.append(pooled_conn)
                self._stats['total_created'] += 1

            logger.debug(f"Created new connection (total: {len(self._all_connections)})")
            return pooled_conn

        except Exception as e:
            raise ConnectionError(
                f"Failed to create pooled connection: {e}",
                platform="pool"
            )

    def checkout(self) -> BaseConnection:
        """
        Get a connection from the pool.

        Returns:
            Connection instance

        Raises:
            PoolExhaustedError: If no connections available within timeout
            ConnectionError: If all connections are unhealthy

        Examples:
            >>> conn = pool.checkout()
            >>> try:
            ...     conn.execute_query("SELECT 1")
            ... finally:
            ...     pool.checkin(conn)
        """
        if self._closed:
            raise ConnectionError("Pool is closed", platform="pool")

        with self._lock:
            self._stats['total_checkouts'] += 1

        start_time = time.time()

        while True:
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self.wait_timeout:
                raise PoolExhaustedError(
                    f"Pool exhausted: no connections available after {self.wait_timeout}s",
                    platform="pool",
                    details={
                        'pool_size': len(self._all_connections),
                        'max_size': self.max_size,
                    }
                )

            # Try to get connection from pool
            try:
                remaining_timeout = self.wait_timeout - elapsed
                pooled_conn = self._pool.get(timeout=min(remaining_timeout, 1.0))

                # Validate connection if configured
                if self.validate_on_checkout:
                    if not pooled_conn.validate():
                        logger.warning("Connection validation failed, creating new connection")
                        with self._lock:
                            self._stats['total_validations_failed'] += 1
                        pooled_conn.close()
                        self._remove_from_all_connections(pooled_conn)
                        continue  # Try again

                # Check if connection is stale or expired
                if pooled_conn.is_stale(self.max_idle_seconds):
                    logger.debug("Connection is stale, creating new connection")
                    pooled_conn.close()
                    self._remove_from_all_connections(pooled_conn)
                    continue

                if pooled_conn.is_expired(self.max_lifetime_seconds):
                    logger.debug("Connection expired, creating new connection")
                    pooled_conn.close()
                    self._remove_from_all_connections(pooled_conn)
                    continue

                # Connection is good, mark as used and return
                pooled_conn.mark_used()
                return pooled_conn.connection

            except Empty:
                # Pool is empty, try to create new connection if under max
                with self._lock:
                    if len(self._all_connections) < self.max_size:
                        try:
                            pooled_conn = self._create_connection()
                            pooled_conn.mark_used()
                            return pooled_conn.connection
                        except Exception as e:
                            logger.error(f"Failed to create new connection: {e}")
                            continue

                # Pool is at max size, wait for connection to become available
                time.sleep(0.1)
                continue

    def checkin(self, connection: BaseConnection):
        """
        Return a connection to the pool.

        Args:
            connection: Connection to return

        Examples:
            >>> pool.checkin(conn)
        """
        if self._closed:
            return

        # Find corresponding pooled connection
        pooled_conn = None
        with self._lock:
            for pc in self._all_connections:
                if pc.connection is connection:
                    pooled_conn = pc
                    break

        if pooled_conn is None:
            logger.warning("Connection not found in pool, ignoring")
            return

        # Return to pool if healthy, otherwise close
        if pooled_conn.is_healthy and connection.is_connected():
            try:
                self._pool.put(pooled_conn, block=False)
                with self._lock:
                    self._stats['total_checkins'] += 1
            except Full:
                # Pool is full, close connection
                logger.debug("Pool is full, closing connection")
                pooled_conn.close()
                self._remove_from_all_connections(pooled_conn)
        else:
            logger.debug("Connection is unhealthy, closing")
            pooled_conn.close()
            self._remove_from_all_connections(pooled_conn)

    @contextmanager
    def get_connection(self):
        """
        Get connection as context manager.

        Automatically checks out and checks in connection.

        Yields:
            Connection instance

        Examples:
            >>> with pool.get_connection() as conn:
            ...     schema = conn.get_target_schema('users')
        """
        conn = self.checkout()
        try:
            yield conn
        finally:
            self.checkin(conn)

    def _cleanup_loop(self):
        """Background cleanup loop (runs in separate thread)."""
        while not self._closed:
            time.sleep(60)  # Run cleanup every minute
            self._cleanup_stale_connections()

    def _cleanup_stale_connections(self):
        """Clean up stale and expired connections."""
        with self._lock:
            to_remove = []

            for pooled_conn in self._all_connections:
                # Check if connection is in use (not in pool)
                try:
                    self._pool.get(block=False)
                    self._pool.put(pooled_conn, block=False)
                except Empty:
                    # Connection is checked out, skip
                    continue
                except Full:
                    # Shouldn't happen, but skip
                    continue

                # Check if stale or expired
                if pooled_conn.is_stale(self.max_idle_seconds) or \
                   pooled_conn.is_expired(self.max_lifetime_seconds):
                    to_remove.append(pooled_conn)

            # Remove stale connections
            for pooled_conn in to_remove:
                try:
                    self._pool.get(block=False)  # Remove from pool
                    pooled_conn.close()
                    self._all_connections.remove(pooled_conn)
                    self._stats['total_cleanups'] += 1
                except Exception as e:
                    logger.error(f"Error cleaning up connection: {e}")

            if to_remove:
                logger.debug(f"Cleaned up {len(to_remove)} stale connections")

    def _remove_from_all_connections(self, pooled_conn: PooledConnection):
        """Remove connection from all_connections list."""
        with self._lock:
            try:
                self._all_connections.remove(pooled_conn)
            except ValueError:
                pass  # Already removed

    def get_stats(self) -> Dict[str, Any]:
        """
        Get pool statistics.

        Returns:
            Dictionary with pool metrics

        Examples:
            >>> stats = pool.get_stats()
            >>> print(f"Active: {stats['active']}, Idle: {stats['idle']}")
        """
        with self._lock:
            total = len(self._all_connections)
            idle = self._pool.qsize()
            active = total - idle

            stats = {
                'total_connections': total,
                'active_connections': active,
                'idle_connections': idle,
                'min_size': self.min_size,
                'max_size': self.max_size,
                'utilization': active / self.max_size if self.max_size > 0 else 0,
                **self._stats
            }

            return stats

    def close(self):
        """
        Close pool and all connections.

        Examples:
            >>> pool.close()
        """
        if self._closed:
            return

        logger.info("Closing connection pool")
        self._closed = True

        # Close all connections
        with self._lock:
            for pooled_conn in self._all_connections:
                try:
                    pooled_conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")

            self._all_connections.clear()

        # Clear pool queue
        while not self._pool.empty():
            try:
                self._pool.get(block=False)
            except Empty:
                break

        logger.info("Connection pool closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    def __repr__(self) -> str:
        """String representation."""
        stats = self.get_stats()
        return (
            f"<ConnectionPool "
            f"total={stats['total_connections']} "
            f"active={stats['active_connections']} "
            f"idle={stats['idle_connections']} "
            f"max={self.max_size}>"
        )
