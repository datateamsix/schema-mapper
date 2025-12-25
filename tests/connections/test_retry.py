"""Tests for schema_mapper.connections.utils.retry module."""

import time
import pytest
from unittest.mock import Mock, patch, MagicMock

from schema_mapper.connections.utils.retry import (
    calculate_backoff,
    retry_on_transient_error,
    RetryContext
)
from schema_mapper.connections.exceptions import (
    RetryExhaustedError,
    NetworkError,
    ConfigurationError
)


class TestCalculateBackoff:
    """Test calculate_backoff function."""

    def test_backoff_basic(self):
        """Test basic exponential backoff calculation."""
        # Attempt 0: 1 * (2^0) = 1
        delay = calculate_backoff(0, base_delay=1.0, backoff_factor=2.0, jitter=False)
        assert delay == 1.0

        # Attempt 1: 1 * (2^1) = 2
        delay = calculate_backoff(1, base_delay=1.0, backoff_factor=2.0, jitter=False)
        assert delay == 2.0

        # Attempt 2: 1 * (2^2) = 4
        delay = calculate_backoff(2, base_delay=1.0, backoff_factor=2.0, jitter=False)
        assert delay == 4.0

        # Attempt 3: 1 * (2^3) = 8
        delay = calculate_backoff(3, base_delay=1.0, backoff_factor=2.0, jitter=False)
        assert delay == 8.0

    def test_backoff_custom_base_delay(self):
        """Test backoff with custom base delay."""
        delay = calculate_backoff(0, base_delay=0.5, backoff_factor=2.0, jitter=False)
        assert delay == 0.5

        delay = calculate_backoff(1, base_delay=0.5, backoff_factor=2.0, jitter=False)
        assert delay == 1.0

        delay = calculate_backoff(2, base_delay=0.5, backoff_factor=2.0, jitter=False)
        assert delay == 2.0

    def test_backoff_custom_factor(self):
        """Test backoff with custom backoff factor."""
        # Factor of 3
        delay = calculate_backoff(0, base_delay=1.0, backoff_factor=3.0, jitter=False)
        assert delay == 1.0

        delay = calculate_backoff(1, base_delay=1.0, backoff_factor=3.0, jitter=False)
        assert delay == 3.0

        delay = calculate_backoff(2, base_delay=1.0, backoff_factor=3.0, jitter=False)
        assert delay == 9.0

    def test_backoff_max_delay_cap(self):
        """Test that backoff is capped at max_delay."""
        # Without cap, attempt 5 would be 1 * (2^5) = 32
        delay = calculate_backoff(5, base_delay=1.0, backoff_factor=2.0, max_delay=10.0, jitter=False)
        assert delay == 10.0

        # Attempt 6 would be 64, still capped at 10
        delay = calculate_backoff(6, base_delay=1.0, backoff_factor=2.0, max_delay=10.0, jitter=False)
        assert delay == 10.0

    def test_backoff_with_jitter(self):
        """Test that jitter adds randomness within expected range."""
        base_delay = 2.0
        expected_min = base_delay * 0.5  # 1.0
        expected_max = base_delay * 1.5  # 3.0

        # Run multiple times to test randomness
        for _ in range(10):
            delay = calculate_backoff(0, base_delay=base_delay, jitter=True)
            assert expected_min <= delay <= expected_max

    def test_backoff_without_jitter_deterministic(self):
        """Test that without jitter, backoff is deterministic."""
        delays = [calculate_backoff(2, jitter=False) for _ in range(5)]
        # All delays should be identical
        assert len(set(delays)) == 1
        assert delays[0] == 4.0


class TestRetryOnTransientError:
    """Test retry_on_transient_error decorator."""

    def test_successful_execution_no_retries(self):
        """Test successful execution without any failures."""
        call_count = 0

        @retry_on_transient_error(max_retries=3)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_function()
        assert result == "success"
        assert call_count == 1  # Called only once

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_retry_on_transient_error_succeeds(self, mock_sleep, mock_is_transient):
        """Test retry on transient error that eventually succeeds."""
        mock_is_transient.return_value = True
        call_count = 0

        @retry_on_transient_error(max_retries=3, base_delay=0.1)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NetworkError("Temporary network issue")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert call_count == 3  # Failed twice, succeeded on third
        assert mock_sleep.call_count == 2  # Slept between retries

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    def test_non_transient_error_no_retry(self, mock_is_transient):
        """Test that non-transient errors are not retried."""
        mock_is_transient.return_value = False
        call_count = 0

        @retry_on_transient_error(max_retries=3)
        def failing_function():
            nonlocal call_count
            call_count += 1
            raise ConfigurationError("Invalid configuration")

        with pytest.raises(ConfigurationError):
            failing_function()

        assert call_count == 1  # Only called once, no retries

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_retry_exhausted_raises_error(self, mock_sleep, mock_is_transient):
        """Test that retry exhaustion raises RetryExhaustedError."""
        mock_is_transient.return_value = True
        call_count = 0

        @retry_on_transient_error(max_retries=3)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise NetworkError("Always fails")

        with pytest.raises(RetryExhaustedError) as exc_info:
            always_fails()

        assert call_count == 4  # Initial + 3 retries
        assert "after 3 retry attempts" in str(exc_info.value)
        assert exc_info.value.attempts == 3

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_retry_with_custom_parameters(self, mock_sleep, mock_is_transient):
        """Test retry with custom retry parameters."""
        mock_is_transient.return_value = True
        call_count = 0

        @retry_on_transient_error(
            max_retries=5,
            base_delay=0.5,
            backoff_factor=3.0
        )
        def custom_retry_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NetworkError("Temporary issue")
            return "success"

        result = custom_retry_function()
        assert result == "success"
        assert call_count == 3

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_retry_with_platform_parameter(self, mock_sleep, mock_is_transient):
        """Test retry with platform parameter."""
        mock_is_transient.return_value = True

        @retry_on_transient_error(max_retries=2, platform='bigquery')
        def platform_function():
            raise NetworkError("Rate limited")

        with pytest.raises(RetryExhaustedError) as exc_info:
            platform_function()

        # Verify platform was passed to is_transient_error
        assert any('bigquery' in str(call) for call in mock_is_transient.call_args_list)

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_retry_detects_platform_from_self(self, mock_sleep, mock_is_transient):
        """Test retry automatically detects platform from class method."""
        mock_is_transient.return_value = True

        class MockConnection:
            def platform_name(self):
                return 'snowflake'

            @retry_on_transient_error(max_retries=2)
            def connect(self):
                raise NetworkError("Connection failed")

        conn = MockConnection()
        with pytest.raises(RetryExhaustedError):
            conn.connect()

        # Verify platform was detected and used
        assert any('snowflake' in str(call) for call in mock_is_transient.call_args_list)

    @patch('time.sleep')
    def test_retry_respects_exception_filter(self, mock_sleep):
        """Test retry only catches specified exception types."""
        call_count = 0

        @retry_on_transient_error(max_retries=3, exceptions=(ValueError,))
        def specific_exception_function():
            nonlocal call_count
            call_count += 1
            raise TypeError("Not in exceptions list")

        # Should raise immediately without retry
        with pytest.raises(TypeError):
            specific_exception_function()

        assert call_count == 1  # No retries

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_retry_backoff_timing(self, mock_sleep, mock_is_transient):
        """Test that retry uses correct backoff timing."""
        mock_is_transient.return_value = True

        @retry_on_transient_error(
            max_retries=3,
            base_delay=1.0,
            backoff_factor=2.0,
            jitter=False
        )
        def timing_test():
            raise NetworkError("Timeout")

        with pytest.raises(RetryExhaustedError):
            timing_test()

        # Verify sleep was called with correct delays
        # Attempt 0: 1.0, Attempt 1: 2.0, Attempt 2: 4.0
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == 1.0
        assert sleep_calls[1] == 2.0
        assert sleep_calls[2] == 4.0


class TestRetryContext:
    """Test RetryContext class."""

    def test_context_manager_basic(self):
        """Test basic context manager usage."""
        with RetryContext(max_retries=3) as ctx:
            assert ctx.should_retry() is True
            ctx.success()

    def test_should_retry_tracks_attempts(self):
        """Test should_retry respects max_retries."""
        ctx = RetryContext(max_retries=2)

        assert ctx.should_retry() is True  # Attempt 0
        ctx.attempt = 1
        assert ctx.should_retry() is True  # Attempt 1
        ctx.attempt = 2
        assert ctx.should_retry() is True  # Attempt 2
        ctx.attempt = 3
        assert ctx.should_retry() is False  # Exceeded

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_record_error_transient(self, mock_sleep, mock_is_transient):
        """Test recording transient error."""
        mock_is_transient.return_value = True

        ctx = RetryContext(max_retries=2, base_delay=0.1)
        error = NetworkError("Timeout")

        ctx.record_error(error)
        assert ctx.last_error == error
        assert ctx.attempt == 1
        assert mock_sleep.called

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    def test_record_error_non_transient(self, mock_is_transient):
        """Test recording non-transient error raises immediately."""
        mock_is_transient.return_value = False

        ctx = RetryContext(max_retries=2)
        error = ConfigurationError("Invalid config")

        with pytest.raises(ConfigurationError):
            ctx.record_error(error)

        assert ctx.attempt == 0  # No retry attempted

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_record_error_exhausted(self, mock_sleep, mock_is_transient):
        """Test retry exhaustion in context."""
        mock_is_transient.return_value = True

        ctx = RetryContext(max_retries=1)
        error = NetworkError("Timeout")

        # First error is ok
        ctx.record_error(error)
        assert ctx.attempt == 1

        # Second error exhausts retries
        with pytest.raises(RetryExhaustedError) as exc_info:
            ctx.record_error(error)

        assert exc_info.value.attempts == 1

    def test_success_marks_operation(self):
        """Test success() marks operation as successful."""
        ctx = RetryContext(max_retries=3)
        assert ctx._success is False

        ctx.success()
        assert ctx._success is True

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_context_manager_with_retries(self, mock_sleep, mock_is_transient):
        """Test full context manager workflow with retries."""
        mock_is_transient.return_value = True
        call_count = 0

        with RetryContext(max_retries=3, platform='bigquery') as ctx:
            while ctx.should_retry():
                try:
                    call_count += 1
                    if call_count < 3:
                        raise NetworkError("Temporary issue")
                    ctx.success()
                    result = "success"
                    break
                except NetworkError as e:
                    ctx.record_error(e)

        assert call_count == 3
        assert result == "success"

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_context_exit_with_unhandled_error(self, mock_sleep, mock_is_transient):
        """Test context manager exit with unhandled errors."""
        mock_is_transient.return_value = True

        with pytest.raises(RetryExhaustedError):
            with RetryContext(max_retries=2) as ctx:
                while ctx.should_retry():
                    error = NetworkError("Always fails")
                    ctx.record_error(error)


class TestIntegration:
    """Integration tests for retry functionality."""

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_decorator_with_class_method(self, mock_sleep, mock_is_transient):
        """Test decorator works correctly with class methods."""
        mock_is_transient.return_value = True

        class DatabaseConnection:
            def __init__(self):
                self.connect_count = 0

            def platform_name(self):
                return 'postgresql'

            @retry_on_transient_error(max_retries=3)
            def connect(self):
                self.connect_count += 1
                if self.connect_count < 3:
                    raise NetworkError("Connection refused")
                return "connected"

        conn = DatabaseConnection()
        result = conn.connect()

        assert result == "connected"
        assert conn.connect_count == 3

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    @patch('time.sleep')
    def test_nested_retry_decorators(self, mock_sleep, mock_is_transient):
        """Test nested functions with retry decorators."""
        mock_is_transient.return_value = True
        inner_count = 0
        outer_count = 0

        @retry_on_transient_error(max_retries=2)
        def outer():
            nonlocal outer_count
            outer_count += 1
            return inner()

        @retry_on_transient_error(max_retries=2)
        def inner():
            nonlocal inner_count
            inner_count += 1
            if inner_count < 2:
                raise NetworkError("Temporary issue")
            return "success"

        result = outer()
        assert result == "success"
        assert inner_count == 2
        assert outer_count == 1

    @patch('schema_mapper.connections.utils.retry.is_transient_error')
    def test_decorator_preserves_function_metadata(self, mock_is_transient):
        """Test that decorator preserves function name and docstring."""
        @retry_on_transient_error(max_retries=3)
        def my_function():
            """This is my function."""
            return "result"

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "This is my function."
