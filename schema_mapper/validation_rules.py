"""
Validation Rules - Comprehensive validation patterns for common data types.

This module provides predefined validation rules, formatters, and custom
rule builders for validating and standardizing common data patterns.
"""

import re
from typing import Callable, Optional, List, Dict, Any, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ValidationRules:
    """
    Predefined validation rules for common data types.

    Provides validators and fixers for:
    - Email addresses
    - Phone numbers (US and international)
    - ZIP codes (US and international)
    - URLs
    - IP addresses
    - Credit cards (basic validation)
    - SSN (Social Security Numbers)
    - Dates
    - Custom patterns

    Example:
        >>> from schema_mapper.validation_rules import ValidationRules
        >>>
        >>> # Validate email
        >>> ValidationRules.validate_email("user@example.com")
        True
        >>>
        >>> # Fix and standardize phone
        >>> ValidationRules.standardize_us_phone("5551234567")
        '(555) 123-4567'
        >>>
        >>> # Create custom validator
        >>> age_validator = ValidationRules.create_range_validator(0, 120)
        >>> age_validator(25)
        True
    """

    # ========================================
    # EMAIL VALIDATION
    # ========================================

    # Comprehensive email regex (RFC 5322 simplified)
    EMAIL_REGEX = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )

    # Common email domain typos
    EMAIL_DOMAIN_FIXES = {
        'gmial.com': 'gmail.com',
        'gmai.com': 'gmail.com',
        'gmil.com': 'gmail.com',
        'yahooo.com': 'yahoo.com',
        'yaho.com': 'yahoo.com',
        'hotmial.com': 'hotmail.com',
        'hotmai.com': 'hotmail.com',
        'outlok.com': 'outlook.com',
    }

    @staticmethod
    def validate_email(value: str) -> bool:
        """
        Validate email format.

        Args:
            value: Email address to validate

        Returns:
            True if valid email format, False otherwise

        Example:
            >>> ValidationRules.validate_email("user@example.com")
            True
            >>> ValidationRules.validate_email("invalid.email")
            False
        """
        if not isinstance(value, str):
            return False

        value = value.strip()
        if not value:
            return False

        return bool(ValidationRules.EMAIL_REGEX.match(value))

    @staticmethod
    def fix_email(value: str, fix_common_typos: bool = True) -> str:
        """
        Fix and standardize email address.

        Common fixes:
        - Convert to lowercase
        - Strip whitespace
        - Fix common domain typos (gmail, yahoo, hotmail)

        Args:
            value: Email address to fix
            fix_common_typos: Whether to fix common domain typos

        Returns:
            Fixed email address

        Example:
            >>> ValidationRules.fix_email("  User@EXAMPLE.COM  ")
            'user@example.com'
            >>> ValidationRules.fix_email("user@gmial.com")
            'user@gmail.com'
        """
        if not isinstance(value, str):
            return value

        # Lowercase and trim
        email = value.lower().strip()

        # Fix common domain typos
        if fix_common_typos and '@' in email:
            local, domain = email.rsplit('@', 1)
            if domain in ValidationRules.EMAIL_DOMAIN_FIXES:
                email = f"{local}@{ValidationRules.EMAIL_DOMAIN_FIXES[domain]}"
                logger.debug(f"Fixed email domain: {value} → {email}")

        return email

    # ========================================
    # PHONE NUMBER VALIDATION
    # ========================================

    # US phone number patterns
    US_PHONE_REGEX = re.compile(
        r'^(?:\+?1[-.\s]?)?'  # Optional country code
        r'\(?([0-9]{3})\)?[-.\s]?'  # Area code
        r'([0-9]{3})[-.\s]?'  # Exchange
        r'([0-9]{4})$'  # Line number
    )

    @staticmethod
    def validate_us_phone(value: str) -> bool:
        """
        Validate US phone number.

        Accepts formats:
        - (555) 123-4567
        - 555-123-4567
        - 555.123.4567
        - 5551234567
        - +1 555 123 4567

        Args:
            value: Phone number to validate

        Returns:
            True if valid US phone format, False otherwise

        Example:
            >>> ValidationRules.validate_us_phone("(555) 123-4567")
            True
            >>> ValidationRules.validate_us_phone("555-123-4567")
            True
            >>> ValidationRules.validate_us_phone("123")
            False
        """
        if not isinstance(value, str):
            return False

        value = value.strip()
        return bool(ValidationRules.US_PHONE_REGEX.match(value))

    @staticmethod
    def standardize_us_phone(value: str, format: str = 'dash') -> str:
        """
        Standardize US phone number to consistent format.

        Args:
            value: Phone number to standardize
            format: Output format ('dash', 'dot', 'paren', 'plain')
                   - 'dash': 555-123-4567
                   - 'dot': 555.123.4567
                   - 'paren': (555) 123-4567
                   - 'plain': 5551234567

        Returns:
            Standardized phone number

        Example:
            >>> ValidationRules.standardize_us_phone("5551234567")
            '555-123-4567'
            >>> ValidationRules.standardize_us_phone("5551234567", format='paren')
            '(555) 123-4567'
        """
        if not isinstance(value, str):
            return value

        # Extract digits only
        digits = re.sub(r'\D', '', value)

        # Remove leading 1 if present
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]

        # Must be exactly 10 digits
        if len(digits) != 10:
            logger.warning(f"Invalid phone number length: {value}")
            return value

        area, exchange, line = digits[:3], digits[3:6], digits[6:]

        if format == 'dash':
            return f"{area}-{exchange}-{line}"
        elif format == 'dot':
            return f"{area}.{exchange}.{line}"
        elif format == 'paren':
            return f"({area}) {exchange}-{line}"
        elif format == 'plain':
            return digits
        else:
            raise ValueError(f"Unknown format: {format}")

    @staticmethod
    def validate_international_phone(value: str) -> bool:
        """
        Validate international phone number (E.164 format).

        Accepts: +[country code][number]
        Example: +1-555-123-4567, +44-20-1234-5678

        Args:
            value: Phone number to validate

        Returns:
            True if valid international format, False otherwise

        Example:
            >>> ValidationRules.validate_international_phone("+1-555-123-4567")
            True
            >>> ValidationRules.validate_international_phone("+44-20-1234-5678")
            True
        """
        if not isinstance(value, str):
            return False

        # E.164 format: + followed by 7-15 digits
        pattern = re.compile(r'^\+[1-9]\d{6,14}$')
        cleaned = re.sub(r'[-.\s()]', '', value.strip())

        return bool(pattern.match(cleaned))

    # ========================================
    # ZIP CODE VALIDATION
    # ========================================

    US_ZIP_REGEX = re.compile(r'^\d{5}(?:-\d{4})?$')

    @staticmethod
    def validate_us_zip(value: str) -> bool:
        """
        Validate US ZIP code (5 or 9 digit).

        Accepts:
        - 12345
        - 12345-6789

        Args:
            value: ZIP code to validate

        Returns:
            True if valid US ZIP format, False otherwise

        Example:
            >>> ValidationRules.validate_us_zip("12345")
            True
            >>> ValidationRules.validate_us_zip("12345-6789")
            True
            >>> ValidationRules.validate_us_zip("123")
            False
        """
        if not isinstance(value, str):
            return False

        value = value.strip()
        return bool(ValidationRules.US_ZIP_REGEX.match(value))

    @staticmethod
    def standardize_us_zip(value: str, include_plus4: bool = False) -> str:
        """
        Standardize US ZIP code format.

        Args:
            value: ZIP code to standardize
            include_plus4: Whether to include +4 extension if present

        Returns:
            Standardized ZIP code

        Example:
            >>> ValidationRules.standardize_us_zip("12345-6789")
            '12345'
            >>> ValidationRules.standardize_us_zip("12345-6789", include_plus4=True)
            '12345-6789'
        """
        if not isinstance(value, str):
            return value

        digits = re.sub(r'\D', '', value.strip())

        if len(digits) == 5:
            return digits
        elif len(digits) == 9:
            if include_plus4:
                return f"{digits[:5]}-{digits[5:]}"
            else:
                return digits[:5]
        else:
            logger.warning(f"Invalid ZIP code length: {value}")
            return value

    @staticmethod
    def validate_postal_code(value: str, country: str = 'US') -> bool:
        """
        Validate postal code for various countries.

        Supported countries:
        - US: 12345 or 12345-6789
        - UK: SW1A 1AA
        - CA: K1A 0B1

        Args:
            value: Postal code to validate
            country: Country code (US, UK, CA)

        Returns:
            True if valid postal code for country, False otherwise

        Example:
            >>> ValidationRules.validate_postal_code("12345", "US")
            True
            >>> ValidationRules.validate_postal_code("SW1A 1AA", "UK")
            True
        """
        if not isinstance(value, str):
            return False

        value = value.strip().upper()

        patterns = {
            'US': r'^\d{5}(?:-\d{4})?$',
            'UK': r'^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$',
            'CA': r'^[A-Z]\d[A-Z]\s?\d[A-Z]\d$',
        }

        if country not in patterns:
            raise ValueError(f"Unsupported country: {country}")

        return bool(re.match(patterns[country], value))

    # ========================================
    # URL VALIDATION
    # ========================================

    URL_REGEX = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$',
        re.IGNORECASE
    )

    @staticmethod
    def validate_url(value: str) -> bool:
        """
        Validate URL format.

        Args:
            value: URL to validate

        Returns:
            True if valid URL format, False otherwise

        Example:
            >>> ValidationRules.validate_url("https://example.com")
            True
            >>> ValidationRules.validate_url("http://localhost:8080/path")
            True
            >>> ValidationRules.validate_url("not-a-url")
            False
        """
        if not isinstance(value, str):
            return False

        return bool(ValidationRules.URL_REGEX.match(value.strip()))

    @staticmethod
    def fix_url(value: str) -> str:
        """
        Fix common URL issues.

        - Add http:// if missing
        - Remove trailing whitespace

        Args:
            value: URL to fix

        Returns:
            Fixed URL

        Example:
            >>> ValidationRules.fix_url("example.com")
            'http://example.com'
        """
        if not isinstance(value, str):
            return value

        url = value.strip()

        # Add http:// if missing
        if not url.startswith(('http://', 'https://')):
            url = f"http://{url}"

        return url

    # ========================================
    # IP ADDRESS VALIDATION
    # ========================================

    IP_V4_REGEX = re.compile(
        r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    )

    @staticmethod
    def validate_ipv4(value: str) -> bool:
        """
        Validate IPv4 address.

        Args:
            value: IP address to validate

        Returns:
            True if valid IPv4 format, False otherwise

        Example:
            >>> ValidationRules.validate_ipv4("192.168.1.1")
            True
            >>> ValidationRules.validate_ipv4("256.1.1.1")
            False
        """
        if not isinstance(value, str):
            return False

        return bool(ValidationRules.IP_V4_REGEX.match(value.strip()))

    # ========================================
    # CREDIT CARD VALIDATION
    # ========================================

    @staticmethod
    def validate_credit_card(value: str) -> bool:
        """
        Validate credit card number using Luhn algorithm.

        Args:
            value: Credit card number to validate

        Returns:
            True if valid credit card number, False otherwise

        Example:
            >>> ValidationRules.validate_credit_card("4532015112830366")
            True
            >>> ValidationRules.validate_credit_card("1234567890123456")
            False
        """
        if not isinstance(value, str):
            return False

        # Remove spaces and dashes
        digits = re.sub(r'[\s-]', '', value.strip())

        # Must be all digits and 13-19 characters
        if not digits.isdigit() or not (13 <= len(digits) <= 19):
            return False

        # Luhn algorithm
        def luhn_checksum(card_num):
            def digits_of(n):
                return [int(d) for d in str(n)]

            digits = digits_of(card_num)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return checksum % 10

        return luhn_checksum(digits) == 0

    @staticmethod
    def get_credit_card_type(value: str) -> Optional[str]:
        """
        Identify credit card type from number.

        Args:
            value: Credit card number

        Returns:
            Card type ('visa', 'mastercard', 'amex', 'discover') or None

        Example:
            >>> ValidationRules.get_credit_card_type("4532015112830366")
            'visa'
            >>> ValidationRules.get_credit_card_type("378282246310005")
            'amex'
        """
        if not isinstance(value, str):
            return None

        digits = re.sub(r'[\s-]', '', value.strip())

        if not digits.isdigit():
            return None

        # Card type patterns
        if re.match(r'^4', digits):
            return 'visa'
        elif re.match(r'^5[1-5]', digits):
            return 'mastercard'
        elif re.match(r'^3[47]', digits):
            return 'amex'
        elif re.match(r'^6(?:011|5)', digits):
            return 'discover'
        else:
            return None

    # ========================================
    # SSN VALIDATION
    # ========================================

    SSN_REGEX = re.compile(r'^\d{3}-?\d{2}-?\d{4}$')

    @staticmethod
    def validate_ssn(value: str) -> bool:
        """
        Validate US Social Security Number format.

        Accepts:
        - 123-45-6789
        - 123456789

        Args:
            value: SSN to validate

        Returns:
            True if valid SSN format, False otherwise

        Example:
            >>> ValidationRules.validate_ssn("123-45-6789")
            True
            >>> ValidationRules.validate_ssn("123456789")
            True
        """
        if not isinstance(value, str):
            return False

        value = value.strip()

        # Basic format check
        if not ValidationRules.SSN_REGEX.match(value):
            return False

        # Extract digits
        digits = re.sub(r'\D', '', value)

        # Invalid SSNs (known invalid patterns)
        if digits == '000000000':
            return False
        if digits[:3] == '000' or digits[:3] == '666' or digits[:3].startswith('9'):
            return False
        if digits[3:5] == '00':
            return False
        if digits[5:] == '0000':
            return False

        return True

    @staticmethod
    def mask_ssn(value: str) -> str:
        """
        Mask SSN for display (show only last 4 digits).

        Args:
            value: SSN to mask

        Returns:
            Masked SSN (XXX-XX-1234)

        Example:
            >>> ValidationRules.mask_ssn("123-45-6789")
            'XXX-XX-6789'
        """
        if not isinstance(value, str):
            return value

        digits = re.sub(r'\D', '', value.strip())

        if len(digits) != 9:
            return value

        return f"XXX-XX-{digits[-4:]}"

    # ========================================
    # DATE VALIDATION
    # ========================================

    @staticmethod
    def validate_date(value: str, format: str = '%Y-%m-%d') -> bool:
        """
        Validate date string against format.

        Args:
            value: Date string to validate
            format: Expected date format (default: YYYY-MM-DD)

        Returns:
            True if valid date in format, False otherwise

        Example:
            >>> ValidationRules.validate_date("2024-01-15")
            True
            >>> ValidationRules.validate_date("01/15/2024", format='%m/%d/%Y')
            True
            >>> ValidationRules.validate_date("invalid")
            False
        """
        if not isinstance(value, str):
            return False

        try:
            datetime.strptime(value.strip(), format)
            return True
        except ValueError:
            return False

    @staticmethod
    def standardize_date(value: str, input_format: str = None, output_format: str = '%Y-%m-%d') -> str:
        """
        Standardize date to consistent format.

        Args:
            value: Date string to standardize
            input_format: Expected input format (None = auto-detect)
            output_format: Desired output format (default: YYYY-MM-DD)

        Returns:
            Standardized date string

        Example:
            >>> ValidationRules.standardize_date("01/15/2024", input_format='%m/%d/%Y')
            '2024-01-15'
        """
        if not isinstance(value, str):
            return value

        value = value.strip()

        # Try to parse with provided format
        if input_format:
            try:
                dt = datetime.strptime(value, input_format)
                return dt.strftime(output_format)
            except ValueError:
                logger.warning(f"Failed to parse date '{value}' with format '{input_format}'")
                return value

        # Auto-detect common formats
        common_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%m-%d-%Y',
            '%d-%m-%Y',
        ]

        for fmt in common_formats:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime(output_format)
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {value}")
        return value

    # ========================================
    # CUSTOM RULE BUILDERS
    # ========================================

    @staticmethod
    def create_regex_validator(pattern: str, flags: int = 0) -> Callable[[str], bool]:
        """
        Create custom validator from regex pattern.

        Args:
            pattern: Regex pattern string
            flags: Regex flags (e.g., re.IGNORECASE)

        Returns:
            Validator function

        Example:
            >>> # Create validator for 3-letter state codes
            >>> state_validator = ValidationRules.create_regex_validator(r'^[A-Z]{3}$')
            >>> state_validator("CAL")
            True
            >>> state_validator("CA")
            False
        """
        regex = re.compile(pattern, flags)

        def validator(value: str) -> bool:
            if not isinstance(value, str):
                return False
            return bool(regex.match(value.strip()))

        return validator

    @staticmethod
    def create_range_validator(
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        inclusive: bool = True
    ) -> Callable[[Union[int, float]], bool]:
        """
        Create numeric range validator.

        Args:
            min_val: Minimum value (None = no minimum)
            max_val: Maximum value (None = no maximum)
            inclusive: Whether to include endpoints

        Returns:
            Validator function

        Example:
            >>> # Create age validator (0-120)
            >>> age_validator = ValidationRules.create_range_validator(0, 120)
            >>> age_validator(25)
            True
            >>> age_validator(150)
            False
        """
        def validator(value: Union[int, float]) -> bool:
            if not isinstance(value, (int, float)):
                return False

            if min_val is not None:
                if inclusive and value < min_val:
                    return False
                if not inclusive and value <= min_val:
                    return False

            if max_val is not None:
                if inclusive and value > max_val:
                    return False
                if not inclusive and value >= max_val:
                    return False

            return True

        return validator

    @staticmethod
    def create_length_validator(
        min_len: Optional[int] = None,
        max_len: Optional[int] = None
    ) -> Callable[[str], bool]:
        """
        Create string length validator.

        Args:
            min_len: Minimum length (None = no minimum)
            max_len: Maximum length (None = no maximum)

        Returns:
            Validator function

        Example:
            >>> # Create username validator (3-20 characters)
            >>> username_validator = ValidationRules.create_length_validator(3, 20)
            >>> username_validator("john")
            True
            >>> username_validator("ab")
            False
        """
        def validator(value: str) -> bool:
            if not isinstance(value, str):
                return False

            length = len(value)

            if min_len is not None and length < min_len:
                return False

            if max_len is not None and length > max_len:
                return False

            return True

        return validator

    @staticmethod
    def create_enum_validator(allowed_values: List[Any], case_sensitive: bool = True) -> Callable:
        """
        Create enum/whitelist validator.

        Args:
            allowed_values: List of allowed values
            case_sensitive: Whether comparison is case-sensitive

        Returns:
            Validator function

        Example:
            >>> # Create status validator
            >>> status_validator = ValidationRules.create_enum_validator(['active', 'inactive', 'pending'])
            >>> status_validator('active')
            True
            >>> status_validator('deleted')
            False
        """
        if not case_sensitive:
            allowed_values = [str(v).lower() for v in allowed_values]

        def validator(value: Any) -> bool:
            check_val = value
            if not case_sensitive and isinstance(value, str):
                check_val = value.lower()

            return check_val in allowed_values

        return validator

    @staticmethod
    def create_composite_validator(*validators: Callable) -> Callable:
        """
        Create validator that combines multiple validators (AND logic).

        Args:
            *validators: Validator functions to combine

        Returns:
            Composite validator function

        Example:
            >>> # Create validator for 5-10 character alphanumeric strings
            >>> length_val = ValidationRules.create_length_validator(5, 10)
            >>> pattern_val = ValidationRules.create_regex_validator(r'^[a-zA-Z0-9]+$')
            >>> username_val = ValidationRules.create_composite_validator(length_val, pattern_val)
            >>> username_val('user123')
            True
            >>> username_val('ab')
            False
        """
        def composite_validator(value: Any) -> bool:
            return all(validator(value) for validator in validators)

        return composite_validator


# ========================================
# CONVENIENCE FUNCTIONS
# ========================================

def apply_validation_rule(
    series: 'pd.Series',
    validator: Callable,
    return_mask: bool = False
) -> Union['pd.Series', Dict[str, Any]]:
    """
    Apply validation rule to pandas Series.

    Args:
        series: Pandas Series to validate
        validator: Validator function
        return_mask: If True, return boolean mask; if False, return summary

    Returns:
        Boolean mask or validation summary dict

    Example:
        >>> import pandas as pd
        >>> emails = pd.Series(['valid@example.com', 'invalid', 'another@test.com'])
        >>> result = apply_validation_rule(emails, ValidationRules.validate_email)
        >>> print(result)
        {'valid_count': 2, 'invalid_count': 1, 'valid_percentage': 66.67}
    """
    import pandas as pd

    mask = series.apply(validator)

    if return_mask:
        return mask

    valid_count = mask.sum()
    total_count = len(series)
    invalid_count = total_count - valid_count

    return {
        'valid_count': int(valid_count),
        'invalid_count': int(invalid_count),
        'total_count': int(total_count),
        'valid_percentage': round((valid_count / total_count) * 100, 2) if total_count > 0 else 0,
        'invalid_indices': series[~mask].index.tolist()
    }
