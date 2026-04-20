# Test Suite Documentation

This document describes the test infrastructure for the saga-dlt package and provides guidance for future test development.

## Current Test Coverage

### Implemented Tests (79 tests, all passing)

#### 1. Selector System Tests ([unit/test_selectors.py](unit/test_selectors.py))
**Coverage: 99% (76/77 statements)**

Tests for the dbt-style pipeline selector system that filters pipeline configurations.

- **Tag Selection**: `tag:daily`, `tag:critical`
- **Type Selection**: `type:google_sheets`, `pipeline-type:api`
- **Glob Patterns**: `*balance*`, `google_sheets__*`
- **Union Logic**: Space-separated selectors (OR)
- **Intersection Logic**: Comma-separated selectors (AND)
- **Complex Combinations**: Mixed union and intersection
- **Edge Cases**: Empty configs, no matches, deduplication
- **Formatting**: Config list display with tags and disabled pipelines

#### 2. Naming Utilities Tests ([unit/test_naming.py](unit/test_naming.py))
**Coverage: 100% (36/36 statements)**

Tests for environment-aware dataset and table naming conventions.

- **Environment Detection**: dev vs prod from env vars and profiles
- **Dataset Naming**:
  - Prod: Separate dataset per pipeline type (`dlt_google_sheets`, `dlt_filesystem`)
  - Dev: Shared dataset for all types (`dlt_dev`, `dlt_john`)
- **Table Naming**:
  - Prod: No pipeline type prefix (`asm__salgsmal`)
  - Dev: With pipeline type prefix (`google_sheets__asm__salgsmal`)
- **Priority Order**: Config override > Profile > Env var > Default
- **Execution Plan Datasets**: Prod (`dlt_orchestration`) vs Dev (shared)
- **Integration Tests**: Naming consistency across dev and prod environments

#### 3. File Configuration Tests ([unit/test_file_config.py](unit/test_file_config.py))
**Coverage: 71% (137/192 statements)**

Tests for file-based configuration discovery and hierarchical resolution.

- **Base Table Name Derivation**: Extract from config paths
  - Simple: `configs/google_sheets/data.yml` → `data`
  - Nested: `configs/google_sheets/asm/salgsmal.yml` → `asm__salgsmal`
  - Multiple levels: `configs/api/region/norway/oslo.yml` → `region__norway__oslo`
- **Pipeline Type Extraction**: From config file paths
- **Path Segment Parsing**: For hierarchical config resolution
- **Value Resolution**: Inheritance vs override logic
  - `+key:` syntax for inheritance (merge lists/dicts)
  - `key:` syntax for override (replace completely)
- **Hierarchical Config Resolution**: Project → Folder → File
- **YAML Loading**: Valid files, invalid YAML, missing files
- **Config Application**: Project defaults, folder hierarchy, file overrides

### Test Organization

```
tests/
├── README.md              # This file
├── __init__.py            # Test suite overview
├── conftest.py            # Shared pytest fixtures
├── unit/                  # Unit tests (no external dependencies)
│   ├── __init__.py
│   ├── test_selectors.py  # Pipeline selector tests (24 tests)
│   ├── test_naming.py     # Naming utilities tests (20 tests)
│   └── test_file_config.py # Config parsing tests (35 tests)
└── integration/           # Integration tests (requires GCP, databases, etc.)
    └── __init__.py        # Placeholder for future tests
```

## Running Tests

### Basic Commands

```bash
# Run all tests with coverage
pytest

# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Verbose output
pytest -v

# Quiet output (less verbose)
pytest -q

# Stop at first failure
pytest -x

# Run specific test file
pytest tests/unit/test_selectors.py

# Run specific test class
pytest tests/unit/test_selectors.py::TestPipelineSelector

# Run specific test method
pytest tests/unit/test_selectors.py::TestPipelineSelector::test_select_by_tag
```

### Coverage Reports

```bash
# Terminal coverage report (default)
pytest

# HTML coverage report (see htmlcov/index.html)
pytest --cov-report=html

# Skip coverage reporting
pytest --no-cov
```

### CI/CD Integration

Tests run automatically in GitHub Actions CI pipeline ([.github/workflows/ci.yml](../.github/workflows/ci.yml)):
- Triggered on push to main/master and pull requests
- Runs `pytest -m unit` to execute all unit tests
- Unit tests must pass before merge

## Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.unit
def test_something_isolated():
    """Tests that don't require external dependencies."""
    pass

@pytest.mark.integration
def test_something_with_gcp():
    """Tests that require GCP, databases, or network access."""
    pass
```

## Shared Fixtures

Common test fixtures are defined in [conftest.py](conftest.py):

- `sample_configs`: Sample pipeline configurations organized by type
- `empty_configs`: Empty configuration dict for edge case testing

## Future Test Improvements

### High Priority

#### 1. Base Pipeline Tests
**File:** `tests/unit/test_base_pipeline.py`

Test the core pipeline execution logic in [pipelines/base_pipeline.py](../pipelines/base_pipeline.py):
- Pipeline initialization with config
- Phase execution (init, extract, load, finalize)
- Timing tracking per phase
- Error handling and logging
- Resource cleanup
- Mock dlt pipeline interactions

**Rationale:** Base pipeline is the foundation for all pipeline types. Bugs here affect everything.

#### 2. BigQuery Destination Tests
**Files:**
- `tests/unit/test_bigquery_destination.py` - Destination logic
- `tests/unit/test_bigquery_access.py` - IAM management

Test BigQuery-specific functionality:
- Dataset creation and proactive race condition prevention
- Table schema generation with hints
- Partitioning and clustering configuration
- IAM policy parsing and application
- Access management (users, groups, service accounts)
- Dataset access updates (avoid repeated updates)

**Rationale:** BigQuery is the primary destination. Access management is complex and error-prone.

#### 3. Pipeline Registry Tests
**File:** `tests/unit/test_registry.py`

Test the polymorphic loading system in [pipelines/registry.py](../pipelines/registry.py):
- Pipeline class loading with fallback hierarchy
- Class name derivation from pipeline type
- Import error handling
- Module path resolution (config-specific → subdirectory → base)

**Rationale:** Registry is critical for pipeline discovery. Failures prevent pipelines from running.

### Medium Priority

#### 4. Integration Tests for Source Types
**Files:**
- `tests/integration/test_google_sheets_pipeline.py`
- `tests/integration/test_filesystem_pipeline.py`
- `tests/integration/test_database_pipeline.py`
- `tests/integration/test_api_pipeline.py`

End-to-end tests that:
- Use mock data sources (mock Google Sheets API, mock GCS, etc.)
- Verify data extraction and transformation
- Test incremental loading
- Validate schema generation
- Check error handling (API failures, missing files, etc.)

**Rationale:** Source-specific logic is where most data quality issues occur.

#### 5. Profile Loading Tests
**File:** `tests/unit/test_profiles.py`

Test profile configuration in [utility/cli/profiles.py](../utility/cli/profiles.py):
- Profile file discovery and parsing
- Target selection
- Service account impersonation
- Environment variable interpolation
- Profile inheritance and overrides

**Rationale:** Profiles determine authentication and destination. Misconfiguration breaks everything.

#### 6. Execution Context Tests
**File:** `tests/unit/test_context.py`

Test execution context in [utility/cli/context.py](../utility/cli/context.py):
- Context initialization
- Profile loading and caching
- Environment and dataset retrieval
- Thread safety for parallel execution

**Rationale:** Context is used throughout the codebase. Thread safety is critical for parallel execution.

### Low Priority (Nice to Have)

#### 7. Orchestration Tests
**File:** `tests/unit/test_execution_plan.py`

Test Cloud Run orchestration:
- Execution plan generation
- Pipeline grouping by type
- Task distribution across workers
- Plan serialization and deserialization

#### 8. Secret Resolution Tests
**File:** `tests/unit/test_secrets.py`

Test secret resolution from GCP Secret Manager and environment variables.

#### 9. Client Pool Tests
**File:** `tests/unit/test_client_pool.py`

Test database connection pooling for parallel execution.

#### 10. Validation Tests
**File:** `tests/unit/test_validation.py`

Test configuration validation patterns:
- Required fields validation
- Type checking
- Enum validation
- Cross-field validation

## Writing Good Tests

### Test Structure

Follow the Arrange-Act-Assert pattern:

```python
def test_something(self):
    """Test description explaining what and why."""
    # Arrange: Set up test data
    config = FilePipelineConfig()

    # Act: Execute the code under test
    result = config._derive_base_table_name("configs/google_sheets/data.yml")

    # Assert: Verify the result
    assert result == "data"
```

### Mocking External Dependencies

Use `unittest.mock` to avoid external dependencies in unit tests:

```python
from unittest.mock import MagicMock, patch

def test_with_mocked_gcp(self):
    """Test GCP interaction without actual GCP calls."""
    with patch("google.cloud.bigquery.Client") as mock_client:
        mock_client.return_value.get_dataset.return_value = MagicMock()

        # Test code that uses BigQuery client
        result = some_function_that_uses_bq()

        # Verify mock was called correctly
        mock_client.return_value.get_dataset.assert_called_once()
```

### Test Naming Conventions

- Test files: `test_<module_name>.py`
- Test classes: `Test<ClassName>` or `Test<Functionality>`
- Test methods: `test_<what_is_being_tested>`
- Use descriptive names: `test_select_by_tag` not `test_select_1`

### Docstrings

Every test should have a docstring explaining what it tests:

```python
def test_select_by_tag(self):
    """Test selecting pipeline configs by tag filter."""
    # Test implementation
```

### Edge Cases

Always test edge cases:
- Empty inputs
- None values
- Invalid inputs
- Boundary conditions
- Error conditions

### Assertions

Use specific assertions:
- `assert result == expected` not `assert result`
- `assert len(result) == 5` not `assert result`
- `assert "error" in result` for substring matching
- `with pytest.raises(ValueError, match="error message")` for exceptions

## Code Coverage Goals

Current coverage by module:
- ✅ `utility/naming.py`: 100%
- ✅ `utility/cli/selectors.py`: 99%
- ⚠️ `pipeline_config/file_config.py`: 71%
- ❌ Most other modules: 0%

**Target:** 80% overall code coverage for core modules (utility/, pipeline_config/, pipelines/base_*.py, destinations/base.py)

**Strategy:**
1. Start with high-value, frequently-used modules
2. Focus on critical paths and error handling
3. Don't aim for 100% on everything - focus on testable, valuable code
4. Integration tests can help cover complex interactions that are hard to unit test

## Continuous Improvement

As the codebase evolves:
1. Add tests for new features before implementation (TDD)
2. Add tests for bug fixes to prevent regression
3. Refactor tests when refactoring code
4. Keep tests maintainable - avoid over-mocking or brittle tests
5. Review test failures in CI - don't ignore flaky tests

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- Project coding rules: [.claude/CLAUDE.md](../.claude/CLAUDE.md)
