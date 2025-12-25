"""
Retry logic with exponential backoff for database connections.

Provides decorators and utilities for automatically retrying operations that fail
due to transient errors (network issues, rate limiting, temporary unavailability).
"""

import time
import random
import logging
from functools import wraps
from typing import Callable, Type, Tuple, Optional

from ..exceptions import RetryExhaustedError, is_transient_error

logger = logging.getLogger(__name__)


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True
) -> float:
    """
    Calculate delay for exponential backoff with optional jitter.

    Formula: delay = min(base_delay * (backoff_factor ^ attempt), max_delay)
    Jitter adds randomness: delay * random(0.5, 1.5)

    Args:
        attempt: Current retry attempt (0-indexed)
        base_delay: Initial delay in seconds
        backoff_factor: Multiplier for each attempt
        max_delay: Maximum delay cap
        jitter: Add randomness to prevent thundering herd

    Returns:
        Delay in seconds before next retry

    Examples:
        >>> calculate_backoff(0)  # First retry
        1.0 (±50% with jitter)

        >>> calculate_backoff(1)  # Second retry
        2.0 (±50% with jitter)

        >>> calculate_backoff(3)  # Fourth retry
        8.0 (±50% with jitter)
    """
    # Exponential backoff
    delay = base_delay * (backoff_factor ** attempt)

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter to prevent thundering herd
    if jitter:
        delay *= random.uniform(0.5, 1.5)

    return delay


def retry_on_transient_error(
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    platform: Optional[str] = None
):
    """
    Decorator to retry function on transient errors with exponential backoff.

    Automatically retries operations that fail due to transient errors:
    - Network timeouts
    - Connection resets
    - Rate limiting
    - Temporary service unavailability

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        backoff_factor: Multiplier for each attempt (default: 2.0)
        max_delay: Maximum delay cap in seconds (default: 60.0)
        jitter: Add randomness to delays (default: True)
        exceptions: Exception types to catch (default: all exceptions)
        platform: Platform name for platform-specific error detection

    Returns:
        Decorated function with retry logic

    Examples:
        >>> @retry_on_transient_error(max_retries=3, platform='bigquery')
        ... def connect_to_bigquery():
        ...     # Connection logic
        ...     pass

        >>> @retry_on_transient_error(max_retries=5, base_delay=0.5)
        ... def execute_query(query):
        ...     # Query execution
        ...     pass

    Usage in class methods:
        >>> class BigQueryConnection:
        ...     @retry_on_transient_error(max_retries=3, platform='bigquery')
        ...     def connect(self):
        ...         # Connect logic
        ...         pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            func_platform = platform

            # Try to get platform from self if available (for class methods)
            if not func_platform and args and hasattr(args[0], 'platform_name'):
                try:
                    func_platform = args[0].platform_name()
                except Exception:
                    pass

            for attempt in range(max_retries + 1):
                try:
                    # Execute function
                    return func(*args, **kwargs)

                except exceptions as e:
                    last_error = e

                    # Check if error is transient
                    if not is_transient_error(e, func_platform or 'unknown'):
                        # Non-transient error, don't retry
                        logger.error(
                            f"{func.__name__} failed with non-transient error: {e}"
                        )
                        raise

                    # Check if we have retries left
                    if attempt >= max_retries:
                        # Exhausted all retries
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries. "
                            f"Last error: {e}"
                        )
                        raise RetryExhaustedError(
                            f"Operation failed after {max_retries} retry attempts",
                            attempts=max_retries,
                            last_error=e,
                            platform=func_platform
                        )

                    # Calculate backoff delay
                    delay = calculate_backoff(
                        attempt=attempt,
                        base_delay=base_delay,
                        backoff_factor=backoff_factor,
                        max_delay=max_delay,
                        jitter=jitter
                    )

                    logger.warning(
                        f"{func.__name__} failed with transient error (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )

                    # Wait before retry
                    time.sleep(delay)

            # Should never reach here, but handle edge case
            if last_error:
                raise last_error

        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for manual retry control.

    Useful when you need fine-grained control over retry logic without decorators.

    Examples:
        >>> retry_ctx = RetryContext(max_retries=3, platform='bigquery')
        >>> with retry_ctx as ctx:
        ...     while ctx.should_retry():
        ...         try:
        ...             result = risky_operation()
        ...             ctx.success()
        ...             return result
        ...         except Exception as e:
        ...             ctx.record_error(e)
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        platform: Optional[str] = None
    ):
        """
        Initialize retry context.

        Args:
            max_retries: Maximum number of retries
            base_delay: Initial delay in seconds
            backoff_factor: Multiplier for each attempt
            max_delay: Maximum delay cap
            jitter: Add randomness to delays
            platform: Platform name for error detection
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter = jitter
        self.platform = platform

        self.attempt = 0
        self.last_error = None
        self._success = False

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        if exc_type and not self._success:
            # Unhandled exception
            if self.last_error:
                # We have a recorded error from retries
                raise RetryExhaustedError(
                    f"Operation failed after {self.attempt} attempts",
                    attempts=self.attempt,
                    last_error=self.last_error,
                    platform=self.platform
                )
        return False

    def should_retry(self) -> bool:
        """
        Check if should continue retrying.

        Returns:
            True if retries available, False otherwise
        """
        return self.attempt <= self.max_retries

    def record_error(self, error: Exception) -> None:
        """
        Record error and handle retry logic.

        Args:
            error: Exception to record

        Raises:
            Exception: If error is non-transient or retries exhausted
        """
        self.last_error = error

        # Check if transient
        if not is_transient_error(error, self.platform or 'unknown'):
            logger.error(f"Non-transient error: {error}")
            raise error

        # Check retries
        if self.attempt >= self.max_retries:
            raise RetryExhaustedError(
                f"Operation failed after {self.max_retries} retries",
                attempts=self.max_retries,
                last_error=error,
                platform=self.platform
            )

        # Calculate and apply backoff
        delay = calculate_backoff(
            attempt=self.attempt,
            base_delay=self.base_delay,
            backoff_factor=self.backoff_factor,
            max_delay=self.max_delay,
            jitter=self.jitter
        )

        logger.warning(
            f"Transient error (attempt {self.attempt + 1}/{self.max_retries}): {error}. "
            f"Retrying in {delay:.2f}s..."
        )

        time.sleep(delay)
        self.attempt += 1

    def success(self) -> None:
        """Mark operation as successful."""
        self._success = True
