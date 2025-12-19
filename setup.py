"""Setup configuration for schema-mapper package."""

from setuptools import setup, find_packages
import os

# Read version from __version__.py
version = {}
version_file = os.path.join('schema_mapper', '__version__.py')
with open(version_file) as f:
    exec(f.read(), version)

# Read long description from README
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

# Read requirements
with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='schema-mapper',
    version=version['__version__'],
    author=version['__author__'],
    author_email=version['__email__'],
    description=version['__description__'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/datateamsix/schema-mapper',
    project_urls={
        'Bug Tracker': 'https://github.com/datateamsix/schema-mapper/issues',
        'Documentation': 'https://github.com/datateamsix/schema-mapper#readme',
        'Source Code': 'https://github.com/datateamsix/schema-mapper',
    },
    packages=find_packages(exclude=['tests', 'tests.*', 'docs']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ],
    python_requires='>=3.8',
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'mypy>=1.0.0',
            'isort>=5.12.0',
        ],
        'bigquery': ['google-cloud-bigquery>=3.0.0', 'pandas-gbq>=0.19.0'],
        'snowflake': ['snowflake-connector-python>=3.0.0'],
        'redshift': ['psycopg2-binary>=2.9.0'],
        'sqlserver': ['pyodbc>=4.0.0'],
        'postgresql': ['psycopg2-binary>=2.9.0'],
        'all': [
            'google-cloud-bigquery>=3.0.0',
            'pandas-gbq>=0.19.0',
            'snowflake-connector-python>=3.0.0',
            'psycopg2-binary>=2.9.0',
            'pyodbc>=4.0.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'schema-mapper=schema_mapper.cli:main',
        ],
    },
    keywords=[
        'database',
        'schema',
        'bigquery',
        'snowflake',
        'redshift',
        'sql-server',
        'postgresql',
        'data-engineering',
        'etl',
        'data-migration',
    ],
    include_package_data=True,
    zip_safe=False,
)
