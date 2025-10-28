# Validator Test Refactoring Guide: From aiohttp.ClientSession Mocking to URL-Specific Mocking

## Problem Statement

The current validator tests mock the entire `aiohttp.ClientSession` class, which creates several maintainability issues:

1. **Tight coupling**: All HTTP requests go through the same mock, making it difficult to add new HTTP endpoints
2. **Fragility**: Adding any new HTTP request will break existing tests that need to juggle the mock to handle both manifests and new requests
3. **Limited flexibility**: Hard to test different response scenarios for different endpoints
4. **Unrealistic testing**: Mocking the entire session doesn't reflect real HTTP behavior

## Recommended Solution

Replace the current approach with **URL-specific mocking** using the `aioresponses` library.

## Benefits of URL-Specific Mocking

### 1. **Granular Control**
Each URL can be mocked independently with different responses, timeouts, and error conditions.

### 2. **Future-Proof**
Adding new endpoints (like `/v0.1/hotkey`, `/v0.1/status`) won't break existing tests.

### 3. **Better Test Isolation** 
Tests don't interfere with each other since each mocks only the URLs it needs.

### 4. **More Realistic Testing**
Uses actual `aiohttp.ClientSession` with just the responses mocked.

### 5. **Easier Debugging**
Mocking is explicit per URL, making it clear what each test is doing.

## Implementation Guide

### Step 1: Add aioresponses Dependency

Add to `validator/pyproject.toml`:
```toml
[dependency-groups]
test = [
    # ... existing dependencies ...
    "aioresponses>=0.7.6",
]
```

### Step 2: Identify Affected Files

The following files currently mock `aiohttp.ClientSession`:

1. `validator/app/src/compute_horde_validator/validator/tests/helpers.py`
2. `validator/app/src/compute_horde_validator/validator/tests/test_other/test_manifest_polling.py`
3. `validator/app/src/compute_horde_validator/validator/tests/test_other/test_utils.py`
4. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_single_miner.py`
5. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_multiple_miners.py`
6. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_batch.py`
7. `validator/app/src/compute_horde_validator/validator/allowance/tests/mockchain.py`

### Step 3: Migration Patterns

#### Before (Current - Problematic)
```python
# Mock entire aiohttp.ClientSession
async with mock_aiohttp_client_session(manifest_data):
    # ALL HTTP requests are mocked through the same session
    result = await some_function_that_makes_http_requests()
```

#### After (Recommended)
```python
# Mock specific URLs only
with aioresponses() as m:
    m.get("http://192.168.1.1:8080/v0.1/manifest", 
          payload={"manifest": manifest_data})
    m.get("http://192.168.1.2:8080/v0.1/manifest", 
          status=404, payload={"error": "Miner offline"})
    
    # Only manifest endpoints are mocked
    result = await some_function_that_makes_http_requests()
```

## Current HTTP Endpoints in Validator

Based on the codebase analysis, the following HTTP endpoints are currently used:

1. **`/v0.1/manifest`** - Get miner executor manifest
2. **`/v0.1/hotkey`** - Get miner hotkey (in scoring module)

Future endpoints might include:
- `/v0.1/status` - Miner status information
- `/v0.1/health` - Health check endpoint
- `/v0.1/metrics` - Performance metrics

## Refactoring Examples

### Example 1: Manifest Polling Test

**Before:**
```python
@asynccontextmanager
async def mock_organic_miner_client(miner_configs: list[MinerConfig]):
    mock_session = create_mock_http_session(miner_configs)
    with patch("aiohttp.ClientSession", return_value=mock_session):
        yield

async with mock_organic_miner_client(miner_configs):
    manifests_dict = await get_manifests_from_miners(miners, timeout=5)
```

**After:**
```python
def mock_manifest_endpoints(miner_configs: list[MinerConfig]):
    mock = aioresponses()
    for config in miner_configs:
        url = f"http://{config['address']}:{config['port']}/v0.1/manifest"
        if config.get('status', 200) == 200:
            mock.get(url, payload={"manifest": config['manifest']})
        else:
            mock.get(url, status=config['status'], 
                    payload={"error": config.get('error', 'Miner unavailable')})
    return mock

with mock_manifest_endpoints(miner_configs):
    manifests_dict = await get_manifests_from_miners(miners, timeout=5)
```

### Example 2: Synthetic Job Test with Multiple Miners

**Before:**
```python
async with mock_aiohttp_client_session(manifest_message):
    await execute_synthetic_batch_run(miners, [], batch.id)
```

**After:**
```python
with aioresponses() as m:
    # Different responses for different miners
    m.get("http://127.0.0.1:8080/v0.1/manifest", 
          payload={"manifest": {ExecutorClass.always_on__gpu_24gb: 2}})
    m.get("http://127.0.0.1:8081/v0.1/manifest", 
          payload={"manifest": {ExecutorClass.always_on__llm__a6000: 1}})
    m.get("http://127.0.0.1:8082/v0.1/manifest", 
          status=404, payload={"error": "Miner offline"})
    
    await execute_synthetic_batch_run(miners, [], batch.id)
```

### Example 3: Testing Timeout Scenarios

**Before (complex mock session logic):**
```python
class MockSession:
    def __init__(self, manifest, wait_before):
        self.wait_before = wait_before
    
    async def get(self, url):
        await asyncio.sleep(self.wait_before)  # Global delay
        return MockResponse(200, {"manifest": self.manifest})
```

**After (simple and clear):**
```python
with aioresponses() as m:
    # Specific timeout for specific miner
    m.get("http://192.168.1.1:8080/v0.1/manifest", 
          exception=asyncio.TimeoutError())
    
    # Normal response for other miners
    m.get("http://192.168.1.2:8080/v0.1/manifest", 
          payload={"manifest": manifest_data})
```

## Migration Strategy

### Phase 1: Add Dependencies and Utilities
1. Add `aioresponses` to test dependencies
2. Create URL-specific mocking utility functions
3. Create backward-compatibility bridge functions

### Phase 2: Gradual Migration
1. Update `helpers.py` with new mocking functions
2. Migrate high-impact tests first (manifest polling, synthetic jobs)
3. Keep backward compatibility during transition

### Phase 3: Complete Migration
1. Update all test files to use URL-specific mocking
2. Remove old session mocking functions
3. Remove backward compatibility code

### Phase 4: Documentation and Best Practices
1. Update test documentation
2. Create examples for new HTTP endpoint testing
3. Establish testing patterns for future endpoints

## Utility Functions for Common Patterns

### Basic Manifest Mocking
```python
def mock_manifest_endpoints(miner_configs: List[Dict[str, Any]]):
    with aioresponses() as m:
        for config in miner_configs:
            url = f"http://{config['address']}:{config['port']}/v0.1/manifest"
            status = config.get('status', 200)
            if status == 200:
                m.get(url, payload={"manifest": config['manifest']})
            else:
                m.get(url, status=status, payload={"error": config.get('error')})
        yield m
```

### Timeout and Delay Testing
```python
def mock_manifest_with_delays(miner_configs: List[Dict[str, Any]]):
    with aioresponses() as m:
        for config in miner_configs:
            url = f"http://{config['address']}:{config['port']}/v0.1/manifest"
            if config.get('timeout'):
                m.get(url, exception=asyncio.TimeoutError())
            elif 'delay' in config:
                async def delayed_response(*args, **kwargs):
                    await asyncio.sleep(config['delay'])
                    return aioresponses.Response(200, payload={"manifest": config['manifest']})
                m.get(url, callback=delayed_response)
            else:
                m.get(url, payload={"manifest": config['manifest']})
        yield m
```

### Mixed Endpoint Testing
```python
def mock_multiple_endpoints(endpoint_configs: Dict[str, Dict]):
    with aioresponses() as m:
        for endpoint_type, configs in endpoint_configs.items():
            for config in configs:
                if endpoint_type == 'manifest':
                    url = f"http://{config['address']}:{config['port']}/v0.1/manifest"
                    m.get(url, payload={"manifest": config['data']})
                elif endpoint_type == 'hotkey':
                    url = f"http://{config['address']}:{config['port']}/v0.1/hotkey"
                    m.get(url, payload={"hotkey": config['data']})
        yield m
```

## Best Practices

### 1. Be Explicit About URLs
Always use full URLs in mocks rather than patterns when possible:
```python
# Good
m.get("http://192.168.1.1:8080/v0.1/manifest", payload={...})

# Avoid if possible
m.get(re.compile(r".*manifest"), payload={...})
```

### 2. Use Fixtures for Common Scenarios
```python
@pytest.fixture
def mock_healthy_miners():
    configs = [
        {'address': '192.168.1.1', 'port': 8080, 'manifest': {...}},
        {'address': '192.168.1.2', 'port': 8080, 'manifest': {...}},
    ]
    with mock_manifest_endpoints(configs) as m:
        yield m
```

### 3. Test Error Conditions Explicitly
```python
def test_miner_offline_scenario():
    with aioresponses() as m:
        m.get("http://192.168.1.1:8080/v0.1/manifest", 
              status=404, payload={"error": "Miner offline"})
        
        # Test how system handles offline miners
```

### 4. Document Mock Scenarios
```python
def test_complex_scenario():
    """
    Test scenario where:
    - Miner 1: Returns valid manifest
    - Miner 2: Times out
    - Miner 3: Returns HTTP 500 error
    """
    with aioresponses() as m:
        m.get("http://miner1:8080/v0.1/manifest", payload={...})
        m.get("http://miner2:8080/v0.1/manifest", exception=asyncio.TimeoutError())
        m.get("http://miner3:8080/v0.1/manifest", status=500)
        
        # Test logic
```

## Timeline for Implementation

### Week 1: Setup and Planning
- Add aioresponses dependency
- Create utility functions
- Create refactored examples

### Week 2: Core Infrastructure
- Migrate `helpers.py`
- Create backward compatibility layer
- Update manifest polling tests

### Week 3: Synthetic Job Tests
- Migrate synthetic job test files
- Test complex multi-miner scenarios
- Update allowance tests

### Week 4: Completion and Documentation
- Remove old mocking code
- Update documentation
- Create guidelines for future HTTP endpoint testing

## Files to Modify

### High Priority (Core Infrastructure)
1. `validator/pyproject.toml` - Add aioresponses dependency
2. `validator/app/src/compute_horde_validator/validator/tests/helpers.py` - Add new mocking utilities

### Medium Priority (Frequently Used Tests)
3. `validator/app/src/compute_horde_validator/validator/tests/test_other/test_manifest_polling.py`
4. `validator/app/src/compute_horde_validator/validator/tests/test_other/test_utils.py`

### Lower Priority (Specific Feature Tests)
5. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_single_miner.py`
6. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_multiple_miners.py`
7. `validator/app/src/compute_horde_validator/validator/tests/test_synthetic_jobs/test_batch.py`
8. `validator/app/src/compute_horde_validator/validator/allowance/tests/mockchain.py`

## Conclusion

This refactoring addresses the reviewer's concerns by:

1. **Eliminating session-wide mocking**: Each URL is mocked independently
2. **Future-proofing tests**: New endpoints won't break existing tests
3. **Improving maintainability**: Tests are more isolated and easier to understand
4. **Enabling better testing**: Different response scenarios per endpoint
5. **Providing migration path**: Backward compatibility during transition

The URL-specific mocking approach using `aioresponses` is a best practice in the Python async HTTP testing community and will make the validator test suite much more maintainable and robust.

