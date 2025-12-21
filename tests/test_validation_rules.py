"""
Test suite for validation_rules.py

Comprehensive tests for all validation patterns and custom rule builders.
"""

import pytest
import pandas as pd
from schema_mapper import ValidationRules
from schema_mapper.validation_rules import apply_validation_rule


class TestEmailValidation:
    """Test email validation and fixing."""

    def test_validate_email_valid(self):
        """Test valid email addresses."""
        valid_emails = [
            'user@example.com',
            'test.user@example.com',
            'user+tag@example.co.uk',
            'user_name@example.org',
            'user123@test-domain.com',
        ]

        for email in valid_emails:
            assert ValidationRules.validate_email(email), f"Failed for {email}"

    def test_validate_email_invalid(self):
        """Test invalid email addresses."""
        invalid_emails = [
            'invalid',
            '@example.com',
            'user@',
            'user @example.com',
            'user@.com',
            '',
            None,
            123,
        ]

        for email in invalid_emails:
            assert not ValidationRules.validate_email(email), f"Should be invalid: {email}"

    def test_fix_email(self):
        """Test email fixing and standardization."""
        assert ValidationRules.fix_email('  User@EXAMPLE.COM  ') == 'user@example.com'
        assert ValidationRules.fix_email('USER@TEST.COM') == 'user@test.com'

    def test_fix_email_common_typos(self):
        """Test fixing common email domain typos."""
        assert ValidationRules.fix_email('user@gmial.com') == 'user@gmail.com'
        assert ValidationRules.fix_email('user@gmai.com') == 'user@gmail.com'
        assert ValidationRules.fix_email('user@yahooo.com') == 'user@yahoo.com'
        assert ValidationRules.fix_email('user@hotmial.com') == 'user@hotmail.com'

    def test_fix_email_preserve_valid(self):
        """Test that valid emails are preserved."""
        email = 'user@example.com'
        assert ValidationRules.fix_email(email) == email


class TestPhoneValidation:
    """Test phone number validation and standardization."""

    def test_validate_us_phone_valid(self):
        """Test valid US phone numbers."""
        valid_phones = [
            '(555) 123-4567',
            '555-123-4567',
            '555.123.4567',
            '5551234567',
            '+1 555 123 4567',
            '+1-555-123-4567',
        ]

        for phone in valid_phones:
            assert ValidationRules.validate_us_phone(phone), f"Failed for {phone}"

    def test_validate_us_phone_invalid(self):
        """Test invalid US phone numbers."""
        invalid_phones = [
            '123',
            '555-123',
            'abcdefghij',
            '',
            None,
        ]

        for phone in invalid_phones:
            assert not ValidationRules.validate_us_phone(phone), f"Should be invalid: {phone}"

    def test_standardize_us_phone_dash(self):
        """Test phone standardization to dash format."""
        assert ValidationRules.standardize_us_phone('5551234567') == '555-123-4567'
        assert ValidationRules.standardize_us_phone('(555) 123-4567') == '555-123-4567'
        assert ValidationRules.standardize_us_phone('+1 555 123 4567') == '555-123-4567'

    def test_standardize_us_phone_paren(self):
        """Test phone standardization to parentheses format."""
        assert ValidationRules.standardize_us_phone('5551234567', format='paren') == '(555) 123-4567'

    def test_standardize_us_phone_dot(self):
        """Test phone standardization to dot format."""
        assert ValidationRules.standardize_us_phone('5551234567', format='dot') == '555.123.4567'

    def test_standardize_us_phone_plain(self):
        """Test phone standardization to plain format."""
        assert ValidationRules.standardize_us_phone('(555) 123-4567', format='plain') == '5551234567'

    def test_validate_international_phone(self):
        """Test international phone validation."""
        assert ValidationRules.validate_international_phone('+1-555-123-4567')
        assert ValidationRules.validate_international_phone('+44-20-1234-5678')
        assert ValidationRules.validate_international_phone('+81-3-1234-5678')
        assert not ValidationRules.validate_international_phone('555-123-4567')


class TestZipCodeValidation:
    """Test ZIP code validation and standardization."""

    def test_validate_us_zip_valid(self):
        """Test valid US ZIP codes."""
        valid_zips = [
            '12345',
            '12345-6789',
        ]

        for zip_code in valid_zips:
            assert ValidationRules.validate_us_zip(zip_code), f"Failed for {zip_code}"

    def test_validate_us_zip_invalid(self):
        """Test invalid US ZIP codes."""
        invalid_zips = [
            '123',
            '12345-67',
            'abcde',
            '',
            None,
        ]

        for zip_code in invalid_zips:
            assert not ValidationRules.validate_us_zip(zip_code), f"Should be invalid: {zip_code}"

    def test_standardize_us_zip_5_digit(self):
        """Test ZIP code standardization without +4."""
        assert ValidationRules.standardize_us_zip('12345-6789') == '12345'
        assert ValidationRules.standardize_us_zip('12345') == '12345'

    def test_standardize_us_zip_with_plus4(self):
        """Test ZIP code standardization with +4."""
        assert ValidationRules.standardize_us_zip('123456789', include_plus4=True) == '12345-6789'

    def test_validate_postal_code_uk(self):
        """Test UK postal code validation."""
        assert ValidationRules.validate_postal_code('SW1A 1AA', 'UK')
        assert ValidationRules.validate_postal_code('EC1A1BB', 'UK')
        assert not ValidationRules.validate_postal_code('12345', 'UK')

    def test_validate_postal_code_ca(self):
        """Test Canadian postal code validation."""
        assert ValidationRules.validate_postal_code('K1A 0B1', 'CA')
        assert ValidationRules.validate_postal_code('K1A0B1', 'CA')
        assert not ValidationRules.validate_postal_code('12345', 'CA')


class TestURLValidation:
    """Test URL validation and fixing."""

    def test_validate_url_valid(self):
        """Test valid URLs."""
        valid_urls = [
            'https://example.com',
            'http://example.com',
            'http://example.com/path',
            'https://example.com:8080',
            'http://localhost:3000',
            'http://192.168.1.1',
        ]

        for url in valid_urls:
            assert ValidationRules.validate_url(url), f"Failed for {url}"

    def test_validate_url_invalid(self):
        """Test invalid URLs."""
        invalid_urls = [
            'not-a-url',
            'example.com',  # Missing protocol
            '',
            None,
        ]

        for url in invalid_urls:
            assert not ValidationRules.validate_url(url), f"Should be invalid: {url}"

    def test_fix_url(self):
        """Test URL fixing."""
        assert ValidationRules.fix_url('example.com') == 'http://example.com'
        assert ValidationRules.fix_url('https://example.com') == 'https://example.com'


class TestIPValidation:
    """Test IP address validation."""

    def test_validate_ipv4_valid(self):
        """Test valid IPv4 addresses."""
        valid_ips = [
            '192.168.1.1',
            '10.0.0.1',
            '172.16.0.1',
            '255.255.255.255',
            '0.0.0.0',
        ]

        for ip in valid_ips:
            assert ValidationRules.validate_ipv4(ip), f"Failed for {ip}"

    def test_validate_ipv4_invalid(self):
        """Test invalid IPv4 addresses."""
        invalid_ips = [
            '256.1.1.1',
            '1.1.1',
            '1.1.1.1.1',
            'not-an-ip',
            '',
            None,
        ]

        for ip in invalid_ips:
            assert not ValidationRules.validate_ipv4(ip), f"Should be invalid: {ip}"


class TestCreditCardValidation:
    """Test credit card validation."""

    def test_validate_credit_card_valid(self):
        """Test valid credit card numbers (test numbers)."""
        # These are known valid test credit card numbers
        valid_cards = [
            '4532015112830366',  # Visa
            '5425233430109903',  # Mastercard
            '378282246310005',   # Amex
            '6011111111111117',  # Discover
        ]

        for card in valid_cards:
            assert ValidationRules.validate_credit_card(card), f"Failed for {card}"

    def test_validate_credit_card_invalid(self):
        """Test invalid credit card numbers."""
        invalid_cards = [
            '1234567890123456',
            '123',
            'abcdefghijklmnop',
            '',
            None,
        ]

        for card in invalid_cards:
            assert not ValidationRules.validate_credit_card(card), f"Should be invalid: {card}"

    def test_validate_credit_card_with_spaces(self):
        """Test credit card with spaces."""
        assert ValidationRules.validate_credit_card('4532 0151 1283 0366')

    def test_get_credit_card_type(self):
        """Test credit card type detection."""
        assert ValidationRules.get_credit_card_type('4532015112830366') == 'visa'
        assert ValidationRules.get_credit_card_type('5425233430109903') == 'mastercard'
        assert ValidationRules.get_credit_card_type('378282246310005') == 'amex'
        assert ValidationRules.get_credit_card_type('6011111111111117') == 'discover'
        assert ValidationRules.get_credit_card_type('invalid') is None


class TestSSNValidation:
    """Test SSN validation."""

    def test_validate_ssn_valid(self):
        """Test valid SSN formats."""
        valid_ssns = [
            '123-45-6789',
            '123456789',
        ]

        for ssn in valid_ssns:
            assert ValidationRules.validate_ssn(ssn), f"Failed for {ssn}"

    def test_validate_ssn_invalid(self):
        """Test invalid SSNs."""
        invalid_ssns = [
            '000-00-0000',  # All zeros
            '666-12-3456',  # Starts with 666
            '900-12-3456',  # Starts with 9
            '123-00-4567',  # Middle is 00
            '123-45-0000',  # Last 4 are 0000
            '123',
            '',
            None,
        ]

        for ssn in invalid_ssns:
            assert not ValidationRules.validate_ssn(ssn), f"Should be invalid: {ssn}"

    def test_mask_ssn(self):
        """Test SSN masking."""
        assert ValidationRules.mask_ssn('123-45-6789') == 'XXX-XX-6789'
        assert ValidationRules.mask_ssn('123456789') == 'XXX-XX-6789'


class TestDateValidation:
    """Test date validation and standardization."""

    def test_validate_date_valid(self):
        """Test valid dates."""
        assert ValidationRules.validate_date('2024-01-15')
        assert ValidationRules.validate_date('01/15/2024', format='%m/%d/%Y')
        assert ValidationRules.validate_date('15-01-2024', format='%d-%m-%Y')

    def test_validate_date_invalid(self):
        """Test invalid dates."""
        assert not ValidationRules.validate_date('invalid')
        assert not ValidationRules.validate_date('2024-13-01')  # Invalid month
        assert not ValidationRules.validate_date('01/15/2024')  # Wrong format
        assert not ValidationRules.validate_date('')
        assert not ValidationRules.validate_date(None)

    def test_standardize_date(self):
        """Test date standardization."""
        assert ValidationRules.standardize_date('01/15/2024', input_format='%m/%d/%Y') == '2024-01-15'
        assert ValidationRules.standardize_date('2024-01-15') == '2024-01-15'

    def test_standardize_date_auto_detect(self):
        """Test date standardization with auto-detection."""
        assert ValidationRules.standardize_date('01/15/2024') == '2024-01-15'
        assert ValidationRules.standardize_date('2024/01/15') == '2024-01-15'


class TestCustomRuleBuilders:
    """Test custom validation rule builders."""

    def test_create_regex_validator(self):
        """Test custom regex validator creation."""
        # Create validator for 3-letter state codes
        state_validator = ValidationRules.create_regex_validator(r'^[A-Z]{3}$')

        assert state_validator('CAL')
        assert state_validator('NYC')
        assert not state_validator('CA')
        assert not state_validator('california')

    def test_create_range_validator(self):
        """Test custom range validator creation."""
        # Create age validator (0-120)
        age_validator = ValidationRules.create_range_validator(0, 120)

        assert age_validator(25)
        assert age_validator(0)
        assert age_validator(120)
        assert not age_validator(-1)
        assert not age_validator(150)

    def test_create_range_validator_min_only(self):
        """Test range validator with only minimum."""
        min_validator = ValidationRules.create_range_validator(min_val=0)

        assert min_validator(0)
        assert min_validator(100)
        assert not min_validator(-1)

    def test_create_range_validator_max_only(self):
        """Test range validator with only maximum."""
        max_validator = ValidationRules.create_range_validator(max_val=100)

        assert max_validator(50)
        assert max_validator(100)
        assert not max_validator(101)

    def test_create_length_validator(self):
        """Test custom length validator creation."""
        # Create username validator (3-20 characters)
        username_validator = ValidationRules.create_length_validator(3, 20)

        assert username_validator('john')
        assert username_validator('a' * 20)
        assert not username_validator('ab')
        assert not username_validator('a' * 21)

    def test_create_enum_validator(self):
        """Test custom enum validator creation."""
        # Create status validator
        status_validator = ValidationRules.create_enum_validator(['active', 'inactive', 'pending'])

        assert status_validator('active')
        assert status_validator('pending')
        assert not status_validator('deleted')
        assert not status_validator('Active')  # Case sensitive

    def test_create_enum_validator_case_insensitive(self):
        """Test case-insensitive enum validator."""
        status_validator = ValidationRules.create_enum_validator(
            ['active', 'inactive'],
            case_sensitive=False
        )

        assert status_validator('active')
        assert status_validator('ACTIVE')
        assert status_validator('Active')

    def test_create_composite_validator(self):
        """Test composite validator creation."""
        # Create validator for 5-10 character alphanumeric strings
        length_val = ValidationRules.create_length_validator(5, 10)
        pattern_val = ValidationRules.create_regex_validator(r'^[a-zA-Z0-9]+$')
        username_val = ValidationRules.create_composite_validator(length_val, pattern_val)

        assert username_val('user123')
        assert username_val('abcde')
        assert not username_val('ab')  # Too short
        assert not username_val('user_name')  # Has underscore


class TestApplyValidationRule:
    """Test applying validation rules to pandas Series."""

    def test_apply_validation_rule_summary(self):
        """Test validation summary on Series."""
        emails = pd.Series([
            'valid@example.com',
            'invalid',
            'another@test.com',
            'bad-email'
        ])

        result = apply_validation_rule(emails, ValidationRules.validate_email)

        assert result['valid_count'] == 2
        assert result['invalid_count'] == 2
        assert result['total_count'] == 4
        assert result['valid_percentage'] == 50.0
        assert len(result['invalid_indices']) == 2

    def test_apply_validation_rule_mask(self):
        """Test returning boolean mask."""
        emails = pd.Series(['valid@example.com', 'invalid', 'another@test.com'])

        mask = apply_validation_rule(emails, ValidationRules.validate_email, return_mask=True)

        assert isinstance(mask, pd.Series)
        assert mask.dtype == bool
        assert mask.sum() == 2  # 2 valid emails

    def test_apply_validation_rule_phones(self):
        """Test validation on phone numbers."""
        phones = pd.Series([
            '(555) 123-4567',
            '555-123-4567',
            'invalid',
            '5551234567'
        ])

        result = apply_validation_rule(phones, ValidationRules.validate_us_phone)

        assert result['valid_count'] == 3
        assert result['invalid_count'] == 1


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_validate_with_none(self):
        """Test validators handle None gracefully."""
        assert not ValidationRules.validate_email(None)
        assert not ValidationRules.validate_us_phone(None)
        assert not ValidationRules.validate_us_zip(None)
        assert not ValidationRules.validate_url(None)
        assert not ValidationRules.validate_ipv4(None)
        assert not ValidationRules.validate_credit_card(None)
        assert not ValidationRules.validate_ssn(None)
        assert not ValidationRules.validate_date(None)

    def test_validate_with_non_string(self):
        """Test validators handle non-string types gracefully."""
        assert not ValidationRules.validate_email(123)
        assert not ValidationRules.validate_us_phone(456)
        assert not ValidationRules.validate_url(['not', 'a', 'string'])

    def test_fix_with_non_string(self):
        """Test fixers handle non-string types gracefully."""
        assert ValidationRules.fix_email(123) == 123
        assert ValidationRules.mask_ssn(None) is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
