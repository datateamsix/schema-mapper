"""
Validation Rules Demo - Comprehensive validation and data cleaning

Demonstrates the ValidationRules class capabilities:
- Email, phone, ZIP code, URL, IP, credit card validation
- Data standardization and fixing
- Custom validation rules
- Applying rules to pandas DataFrames
"""

import pandas as pd
from schema_mapper import ValidationRules
from schema_mapper.validation_rules import apply_validation_rule

print("=" * 80)
print("VALIDATION RULES DEMO")
print("=" * 80)

# ========================================
# 1. EMAIL VALIDATION & FIXING
# ========================================
print("\n" + "=" * 80)
print("1. EMAIL VALIDATION & FIXING")
print("=" * 80)

emails = [
    '  User@EXAMPLE.COM  ',  # Needs trimming and lowercasing
    'user@gmial.com',        # Common typo
    'valid@test.com',        # Valid
    'invalid-email',          # Invalid
    'user@yahooo.com'        # Common typo
]

print("\nOriginal Emails:")
for email in emails:
    valid = ValidationRules.validate_email(email)
    fixed = ValidationRules.fix_email(email)
    print(f"  {repr(email):30} -> Valid: {valid:5} -> Fixed: {fixed}")

# ========================================
# 2. PHONE NUMBER STANDARDIZATION
# ========================================
print("\n" + "=" * 80)
print("2. PHONE NUMBER STANDARDIZATION")
print("=" * 80)

phones = [
    '5551234567',
    '(555) 123-4567',
    '+1 555 123 4567',
    '555.123.4567',
]

print("\nStandardizing to different formats:")
for phone in phones:
    print(f"\nOriginal: {phone}")
    print(f"  Dash:   {ValidationRules.standardize_us_phone(phone, format='dash')}")
    print(f"  Paren:  {ValidationRules.standardize_us_phone(phone, format='paren')}")
    print(f"  Dot:    {ValidationRules.standardize_us_phone(phone, format='dot')}")
    print(f"  Plain:  {ValidationRules.standardize_us_phone(phone, format='plain')}")

# ========================================
# 3. ZIP CODE VALIDATION
# ========================================
print("\n" + "=" * 80)
print("3. ZIP CODE VALIDATION")
print("=" * 80)

zip_codes = [
    '12345',
    '12345-6789',
    '123',  # Invalid
]

print("\nValidating ZIP codes:")
for zip_code in zip_codes:
    valid = ValidationRules.validate_us_zip(zip_code)
    standardized = ValidationRules.standardize_us_zip(zip_code)
    print(f"  {zip_code:15} -> Valid: {valid:5} -> Standardized: {standardized}")

# ========================================
# 4. URL VALIDATION & FIXING
# ========================================
print("\n" + "=" * 80)
print("4. URL VALIDATION & FIXING")
print("=" * 80)

urls = [
    'example.com',  # Missing protocol
    'https://example.com',
    'http://localhost:8080',
]

print("\nValidating and fixing URLs:")
for url in urls:
    valid = ValidationRules.validate_url(url)
    fixed = ValidationRules.fix_url(url)
    print(f"  {url:30} -> Valid: {valid:5} -> Fixed: {fixed}")

# ========================================
# 5. IP ADDRESS VALIDATION
# ========================================
print("\n" + "=" * 80)
print("5. IP ADDRESS VALIDATION")
print("=" * 80)

ip_addresses = [
    '192.168.1.1',
    '10.0.0.1',
    '256.1.1.1',  # Invalid
]

print("\nValidating IP addresses:")
for ip in ip_addresses:
    valid = ValidationRules.validate_ipv4(ip)
    print(f"  {ip:15} -> Valid: {valid}")

# ========================================
# 6. CREDIT CARD VALIDATION
# ========================================
print("\n" + "=" * 80)
print("6. CREDIT CARD VALIDATION")
print("=" * 80)

credit_cards = [
    '4532015112830366',  # Visa (test number)
    '5425233430109903',  # Mastercard (test number)
    '378282246310005',   # Amex (test number)
    '1234567890123456',  # Invalid
]

print("\nValidating credit cards:")
for card in credit_cards:
    valid = ValidationRules.validate_credit_card(card)
    card_type = ValidationRules.get_credit_card_type(card)
    print(f"  {card} -> Valid: {valid:5} -> Type: {card_type}")

# ========================================
# 7. SSN VALIDATION & MASKING
# ========================================
print("\n" + "=" * 80)
print("7. SSN VALIDATION & MASKING")
print("=" * 80)

ssns = [
    '123-45-6789',
    '123456789',
    '000-00-0000',  # Invalid
]

print("\nValidating and masking SSNs:")
for ssn in ssns:
    valid = ValidationRules.validate_ssn(ssn)
    masked = ValidationRules.mask_ssn(ssn)
    print(f"  {ssn:15} -> Valid: {valid:5} -> Masked: {masked}")

# ========================================
# 8. DATE STANDARDIZATION
# ========================================
print("\n" + "=" * 80)
print("8. DATE STANDARDIZATION")
print("=" * 80)

dates = [
    ('01/15/2024', '%m/%d/%Y'),
    ('2024-01-15', None),  # Auto-detect
    ('15-01-2024', '%d-%m-%Y'),
]

print("\nStandardizing dates to YYYY-MM-DD:")
for date, fmt in dates:
    standardized = ValidationRules.standardize_date(date, input_format=fmt)
    print(f"  {date:15} (format: {str(fmt):12}) -> {standardized}")

# ========================================
# 9. CUSTOM VALIDATION RULES
# ========================================
print("\n" + "=" * 80)
print("9. CUSTOM VALIDATION RULES")
print("=" * 80)

print("\n9a. Custom Regex Validator (3-letter state codes):")
state_validator = ValidationRules.create_regex_validator(r'^[A-Z]{3}$')

states = ['CAL', 'NYC', 'TEX', 'CA', 'texas']
for state in states:
    valid = state_validator(state)
    print(f"  {state:10} -> Valid: {valid}")

print("\n9b. Custom Range Validator (age 0-120):")
age_validator = ValidationRules.create_range_validator(0, 120)

ages = [25, 0, 120, -5, 150]
for age in ages:
    valid = age_validator(age)
    print(f"  {age:5} -> Valid: {valid}")

print("\n9c. Custom Length Validator (username 3-20 chars):")
username_validator = ValidationRules.create_length_validator(3, 20)

usernames = ['john', 'ab', 'verylongusernamethatexceedslimit']
for username in usernames:
    valid = username_validator(username)
    print(f"  {username:35} -> Valid: {valid}")

print("\n9d. Custom Enum Validator (status values):")
status_validator = ValidationRules.create_enum_validator(
    ['active', 'inactive', 'pending'],
    case_sensitive=False
)

statuses = ['active', 'ACTIVE', 'pending', 'deleted']
for status in statuses:
    valid = status_validator(status)
    print(f"  {status:10} -> Valid: {valid}")

print("\n9e. Composite Validator (5-10 char alphanumeric):")
length_val = ValidationRules.create_length_validator(5, 10)
pattern_val = ValidationRules.create_regex_validator(r'^[a-zA-Z0-9]+$')
username_val = ValidationRules.create_composite_validator(length_val, pattern_val)

test_usernames = ['user123', 'ab', 'user_name', 'verylongusername']
for username in test_usernames:
    valid = username_val(username)
    print(f"  {username:20} -> Valid: {valid}")

# ========================================
# 10. APPLYING RULES TO PANDAS DATAFRAMES
# ========================================
print("\n" + "=" * 80)
print("10. APPLYING RULES TO PANDAS DATAFRAMES")
print("=" * 80)

# Create sample DataFrame
df = pd.DataFrame({
    'email': [
        'user1@example.com',
        'user2@gmial.com',
        'invalid-email',
        'user3@test.com',
        'bad@',
    ],
    'phone': [
        '(555) 123-4567',
        '5551234567',
        'invalid',
        '555-123-4567',
        '123',
    ],
    'age': [25, 30, 150, 28, -5],  # 150 and -5 are invalid
})

print("\nOriginal DataFrame:")
print(df)

# Apply email validation
print("\n10a. Email Validation Results:")
email_results = apply_validation_rule(df['email'], ValidationRules.validate_email)
print(f"  Valid emails: {email_results['valid_count']}/{email_results['total_count']}")
print(f"  Valid percentage: {email_results['valid_percentage']:.1f}%")
print(f"  Invalid indices: {email_results['invalid_indices']}")

# Apply phone validation
print("\n10b. Phone Validation Results:")
phone_results = apply_validation_rule(df['phone'], ValidationRules.validate_us_phone)
print(f"  Valid phones: {phone_results['valid_count']}/{phone_results['total_count']}")
print(f"  Valid percentage: {phone_results['valid_percentage']:.1f}%")
print(f"  Invalid indices: {phone_results['invalid_indices']}")

# Apply age validation
print("\n10c. Age Validation Results (0-120):")
age_validator = ValidationRules.create_range_validator(0, 120)
age_results = apply_validation_rule(df['age'], age_validator)
print(f"  Valid ages: {age_results['valid_count']}/{age_results['total_count']}")
print(f"  Valid percentage: {age_results['valid_percentage']:.1f}%")
print(f"  Invalid indices: {age_results['invalid_indices']}")

# Get boolean masks for filtering
print("\n10d. Filtering DataFrame with Validation Masks:")
email_mask = apply_validation_rule(df['email'], ValidationRules.validate_email, return_mask=True)
phone_mask = apply_validation_rule(df['phone'], ValidationRules.validate_us_phone, return_mask=True)
age_mask = apply_validation_rule(df['age'], age_validator, return_mask=True)

# Combined validation
valid_rows = email_mask & phone_mask & age_mask
print(f"\nRows with ALL valid data:")
print(df[valid_rows])

print(f"\nRows with ANY invalid data:")
print(df[~valid_rows])

# Fix emails in DataFrame
print("\n10e. Fixing Emails in DataFrame:")
df_fixed = df.copy()
df_fixed['email'] = df_fixed['email'].apply(lambda x: ValidationRules.fix_email(x) if isinstance(x, str) else x)
print("\nFixed emails:")
for i, (orig, fixed) in enumerate(zip(df['email'], df_fixed['email'])):
    if orig != fixed:
        print(f"  Row {i}: {orig:25} -> {fixed}")

# Standardize phones
print("\n10f. Standardizing Phone Numbers:")
df_fixed['phone_standardized'] = df['phone'].apply(
    lambda x: ValidationRules.standardize_us_phone(x) if ValidationRules.validate_us_phone(x) else x
)
print("\nStandardized phones:")
print(df_fixed[['phone', 'phone_standardized']])

# ========================================
# SUMMARY
# ========================================
print("\n" + "=" * 80)
print("VALIDATION RULES DEMO COMPLETE")
print("=" * 80)

print(f"""
Summary of Capabilities Demonstrated:
  ✓ Email validation and fixing (including common typos)
  ✓ Phone number validation and multi-format standardization
  ✓ ZIP code validation (US and international)
  ✓ URL validation and fixing
  ✓ IP address validation
  ✓ Credit card validation (Luhn algorithm) and type detection
  ✓ SSN validation and masking
  ✓ Date validation and standardization
  ✓ Custom validators (regex, range, length, enum, composite)
  ✓ Applying validation rules to pandas DataFrames
  ✓ Filtering and cleaning DataFrames based on validation

The ValidationRules module provides production-ready validation
and standardization for common data types!
""")
