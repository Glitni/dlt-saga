"""Test suite for dlt-saga package.

This package contains unit and integration tests for the dlt-based data ingestion framework.

Test organization:
- unit/: Unit tests that don't require external dependencies
- integration/: Integration tests that may require external services (GCP, databases, etc.)

Run tests with:
    pytest                              # Run all tests
    pytest -m unit                      # Run only unit tests
    pytest -m integration               # Run only integration tests
    pytest --cov                        # Run with coverage report
"""
