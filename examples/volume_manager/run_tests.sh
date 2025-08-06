#!/bin/bash
set -e
set -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Parse command line arguments
TEST_INTEGRATION=false
DEPLOY_VOLUME_MANAGER=false

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -i, --integration  Test complete job integration (default: test endpoints only)"
    echo "  -d, --deploy       Deploy volume manager example"
    echo "  -h, --help         Show this help message"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--integration)
            TEST_INTEGRATION=true
            shift
            ;;
        -d|--deploy)
            DEPLOY_VOLUME_MANAGER=true
            shift
            ;;
        -h|--help)
            show_usage
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            ;;
    esac
done

# Use the newer 'docker compose' command
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE_COMMAND="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_COMMAND="docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' is available on this system."
    exit 1
fi

if [ "$DEPLOY_VOLUME_MANAGER" = true ]; then
    echo "Starting volume manager with docker-compose..."
    uv sync
    $DOCKER_COMPOSE_COMMAND up -d

    echo "Waiting for volume manager to be ready..."
    timeout 60s bash -c "until curl -f http://localhost:${VOLUME_MANAGER_PORT:-8001}/health; do sleep 2; done"
else
    echo "Assuming volume manager is already running..."
fi

echo "Running tests..."
if [ "$TEST_INTEGRATION" = true ]; then
    echo "Testing integration..."
    uv run python test_integration.py
    PYTHON_EXIT_CODE=$?
    
    # Check cache status immediately after the test
    CACHE_RESPONSE=$(curl -s http://localhost:8001/cache/status 2>/dev/null || echo '{"cached_volumes_count":0,"cached_volumes":[]}')
    CACHE_COUNT=$(echo "$CACHE_RESPONSE" | grep -o '"cached_volumes_count":[0-9]*' | grep -o '[0-9]*' || echo "0")
    CACHED_MODELS=$(echo "$CACHE_RESPONSE" | grep -o '"cached_volumes":\[[^]]*\]' | sed 's/"cached_volumes":\[//' | sed 's/\]//' | sed 's/"//g' || echo "[]")
else
    echo "Testing endpoints..."
    uv run python test_endpoints.py
    PYTHON_EXIT_CODE=$?
fi

# Test Review
echo ""
echo "=== TEST REVIEW ==="
if [ $PYTHON_EXIT_CODE -eq 0 ]; then
    if [ "$TEST_INTEGRATION" = true ] && [ "$CACHE_COUNT" -gt 0 ]; then
        echo "✅ PASSED - Volume manager integration working"
        echo "📊 Cache has $CACHE_COUNT volumes"
        echo "🔍 Cached models: $CACHED_MODELS"
    elif [ "$TEST_INTEGRATION" = true ] && [ "$CACHE_COUNT" -eq 0 ]; then
        echo "❌ FAILED - Volume manager not used (cache empty)"
        echo "⚠️  Cache is empty - integration test failed"
        exit 1
    else
        echo "✅ PASSED - Endpoint test completed"
    fi
else
    echo "❌ FAILED"
fi
echo "=================="
echo ""

if [ "$DEPLOY_VOLUME_MANAGER" = true ]; then
    echo "Stopping volume manager..."
    $DOCKER_COMPOSE_COMMAND down
fi