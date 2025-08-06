# Volume Manager Example

An example implementation of a volume manager for Compute Horde. This demonstrates how to build an external volume management service that can download, cache, and return mount options for volumes used in compute jobs.

## Quick Start

### Prerequisites
- Docker (required for running the service)
- Python 3.11+ and UV package manager (only needed for local development)

### Usage
```bash
# Start the volume manager with Docker Compose 
docker-compose up -d

# Check logs
docker-compose logs -f volume-manager

# Stop the service
docker-compose down
```

**Note:** The port where the volume manager service listens on can be configured using environment variables:

```bash
# Set env variable directly
export VOLUME_MANAGER_PORT=8123

# Or use it with docker-compose
VOLUME_MANAGER_PORT=8123 docker-compose up -d
```

## Monitoring

```bash
# Check health
curl http://localhost:${VOLUME_MANAGER_PORT:-8001}/health

# Check cache status
curl http://localhost:${VOLUME_MANAGER_PORT:-8001}/cache/status
```

## Local Testing

After setting everything up with [`local_stack/run_in_screen.sh`](https://github.com/backend-developers-ltd/ComputeHorde/blob/master/local_stack/run_in_screen.sh), test the volume manager with:

```bash
./run_tests.sh

# Test with custom port
VOLUME_MANAGER_PORT=9000 ./run_tests.sh

# Deploy the volume manager example
./run_tests.sh -d

# Test the whole ComputeHorde integration instead of endpoints only
./run_tests.sh -i
```