# Scheduler Worker Tests

## Overview

Fast, isolated unit tests for all scheduler workers using minimal synthetic data and comprehensive mocking.

## Philosophy

### Minimal Data, Maximum Coverage

Instead of testing with thousands of rows:
- **Use 1-3 rows** that cover essential cases
- Focus on **logic validation**, not data volume
- Test **edge cases** with minimal examples
- Execute in **< 1 second** per test suite

### Synthetic Data Strategy

Each test uses **carefully crafted minimal data** that:
1. Matches the exact SQL output schema
2. Covers different branches in logic
3. Tests edge cases (empty, null, single item)
4. Validates data transformations

### Full Isolation

- ✅ **No real database** - All DB calls mocked
- ✅ **No real Redis** - In-memory mock tracking
- ✅ **No network I/O** - Pure logic testing
- ✅ **No file I/O** - Except reading SQL files
- ✅ **Deterministic** - Same results every time

## Test Structure

```
tests/scheduler/workers/
├── README.md (this file)
├── channels_by_dma/
│   ├── README.md
│   └── test_channels_by_dma_worker.py
├── dummy/
│   ├── README.md
│   └── test_dummy_worker.py
└── weekly_stats/
    ├── README.md
    └── test_weekly_stats_worker.py
```

## Running Tests

### All Scheduler Tests
```bash
pytest tests/scheduler/ -v
```

### Specific Worker
```bash
pytest tests/scheduler/workers/channels_by_dma/ -v
pytest tests/scheduler/workers/dummy/ -v
pytest tests/scheduler/workers/weekly_stats/ -v
```

### With Coverage
```bash
pytest tests/scheduler/ --cov=scheduler.workers --cov-report=html
```

### Specific Test
```bash
pytest tests/scheduler/workers/dummy/test_dummy_worker.py::TestDummyWorker::test_dma_grouping -v
```

### Fast Mode (Parallel)
```bash
pytest tests/scheduler/ -n auto  # Requires pytest-xdist
```

## Performance

| Worker | Tests | Execution Time | Data Rows |
|--------|-------|----------------|-----------|
| channels_by_dma | 8 | ~0.5s | 3 per test |
| dummy | 10 | ~0.6s | 3 per test |
| weekly_stats | 10 | ~0.8s | 1-2 per statement |
| **Total** | **28** | **< 2s** | **Minimal** |

## What We Test

### For Each Worker

1. **Initialization** - Worker setup, paths, connections
2. **Data Processing** - Correct transformation logic
3. **Redis Storage** - Keys, values, TTL, JSON structure
4. **SQL Validation** - Query files load and have expected structure
5. **Edge Cases** - Empty results, single items, unusual data
6. **Error Handling** - Database failures, Redis failures
7. **Data Structure** - Output format matches expectations

### Common Edge Cases

- ✅ Empty results from database
- ✅ Single item results
- ✅ Null/None values
- ✅ Database connection errors
- ✅ Redis connection errors
- ✅ Invalid data types
- ✅ Missing required fields

## Mocking Patterns

### Basic Redis Mock
```python
@pytest.fixture
def mock_redis():
    redis_mock = Mock()
    redis_mock.stored_data = {}

    def mock_setex(key, ttl, value):
        redis_mock.stored_data[key] = (value, ttl)

    redis_mock.setex = mock_setex
    return redis_mock
```

### Basic Database Mock
```python
with patch('scheduler.workers.your_worker.RedshiftConnection') as mock_conn:
    mock_conn_instance = MagicMock()
    mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
    mock_conn_instance.__exit__ = Mock(return_value=None)
    mock_conn_instance.execute_query = Mock(return_value=test_data)
    mock_conn.return_value = mock_conn_instance

    result = worker.process()
```

### Advanced Mock (Different Results per Query)
```python
def execute_query_side_effect(statement):
    if "QUERY TYPE A" in statement.upper():
        return [{"field1": "value1"}]
    elif "QUERY TYPE B" in statement.upper():
        return [{"field2": "value2"}]
    return []

mock_conn_instance.execute_query = Mock(side_effect=execute_query_side_effect)
```

## Synthetic Data Examples

### channels_by_dma
```python
[
    {"dma_name": "New York", "dma_id": 501, "channelcount": 10},
    {"dma_name": "Los Angeles", "dma_id": 803, "channelcount": 8},
]
```

### dummy
```python
[
    {"content_type": "channel", "content_id": 123, "title": "Channel 1", "dma_id": 501},
    {"content_type": "vod", "content_id": 789, "title": "Movie 1", "dma_id": None},
]
```

### weekly_stats
```python
{
    "Live Summarized": [
        {"friendly_call_sign": "WABC", "viewers": 100, "sessions": 50}
    ]
}
```

## Benefits

### Speed
- **< 2 seconds** for all tests
- No database startup
- No Redis startup
- Parallel execution possible

### Reliability
- **Deterministic** - Same input = same output
- **No flaky tests** - No network timeouts
- **No race conditions** - Pure logic testing

### Maintainability
- **Clear synthetic data** - Easy to understand
- **Focused tests** - One thing per test
- **Good error messages** - Clear failures

### Coverage
- **Logic branches** - All code paths
- **Edge cases** - Minimal examples
- **Error paths** - Exception handling

## Adding New Worker Tests

### 1. Create Test Directory
```bash
mkdir -p tests/scheduler/workers/new_worker
```

### 2. Create Test File
```python
# tests/scheduler/workers/new_worker/test_new_worker.py
import pytest
from unittest.mock import Mock, patch, MagicMock

@pytest.fixture
def mock_redis():
    # Your mock setup
    pass

@pytest.fixture
def minimal_db_results():
    # Your minimal synthetic data (1-3 rows)
    return [
        {"field1": "value1", "field2": 123},
    ]

class TestNewWorker:
    def test_process_with_minimal_data(self, mock_redis, minimal_db_results):
        # Your test
        pass
```

### 3. Create README
Document:
- Data structure
- Edge cases covered
- Running instructions
- Mocking strategy

### 4. Update This README
Add worker to the table and examples.

## Integration Tests

For full end-to-end testing with real databases:
- Use `tests/integration/` (separate directory)
- Docker Compose with test database
- Minimal test data (10-20 rows per table)
- Run separately from unit tests
- Slower execution (acceptable for integration tests)

## Common Issues

### Import Errors
```python
# Add paths at top of test file
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "scheduler"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))
```

### Mock Not Working
```python
# Use MagicMock for context managers
mock_conn_instance = MagicMock()
mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
mock_conn_instance.__exit__ = Mock(return_value=None)
```

### JSON Parsing Errors
```python
# Ensure mock returns proper JSON
import json
stored_value, ttl = mock_redis.stored_data[key]
data = json.loads(stored_value)  # Should work
```

## Best Practices

1. **One assertion per test** (when possible)
2. **Clear test names** - Describe what is being tested
3. **Minimal data** - Only what's needed
4. **Document fixtures** - Explain the data structure
5. **Test edge cases** - Empty, null, single item
6. **Mock at boundaries** - Database and Redis only
7. **Fast execution** - Under 1 second per suite

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
- [pytest fixtures guide](https://docs.pytest.org/en/stable/fixture.html)
