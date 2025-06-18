# EigenLayer Sidecar - Claude.ai Development Guide

## Project Overview

The EigenLayer Sidecar is an open source, permissionless, verified indexer enabling anyone (AVS, operator, etc) to access EigenLayer's protocol rewards in real-time. It's a sophisticated Go-based application that indexes Ethereum blockchain data, calculates rewards, and provides comprehensive APIs for accessing EigenLayer protocol state.

**Key Capabilities:**
- Real-time blockchain indexing and state management
- Complex rewards calculation engine
- RESTful and gRPC API services
- Database snapshot creation and restoration
- Multi-network support (mainnet, holesky, testnet, preprod)

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Ethereum      │────│   Sidecar        │────│   PostgreSQL    │
│   RPC Node      │    │   Indexer        │    │   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              │
                    ┌─────────────────────┐
                    │   API Services      │
                    │   (gRPC + HTTP)     │
                    │   Port 7100/7101    │
                    └─────────────────────┘
```

### Core Components

- **Indexer**: Fetches and processes blockchain data from Ethereum nodes
- **State Manager**: Manages EigenLayer protocol state (operators, stakers, rewards)
- **Rewards Engine**: Complex calculation system for reward distribution
- **API Server**: gRPC and HTTP endpoints for data access
- **Database**: PostgreSQL with extensive migration system
- **Snapshot System**: Database backup/restore functionality

## Project Structure

```
/
├── cmd/                    # CLI commands and main entry points
├── pkg/                    # Core application packages
│   ├── eigenState/         # Protocol state management
│   ├── rewards/            # Rewards calculation engine
│   ├── rpcServer/          # API server implementation
│   ├── indexer/            # Blockchain indexing logic
│   ├── postgres/           # Database layer and migrations
│   ├── clients/            # External service clients (Ethereum, Etherscan)
│   └── sidecar/            # Main application orchestration
├── docs/                   # Docusaurus documentation site
├── examples/               # Usage examples and client code
├── scripts/                # Development and deployment scripts
├── snapshots/              # Database snapshot configurations
├── charts/                 # Kubernetes Helm charts
└── internal/               # Internal utilities and configuration
```

## Technology Stack

- **Language**: Go 1.23.6
- **Database**: PostgreSQL with GORM ORM
- **APIs**: gRPC with Protocol Buffers, HTTP/REST
- **Blockchain**: Ethereum RPC integration
- **Documentation**: Docusaurus with OpenAPI specs
- **Containerization**: Docker and Docker Compose
- **Deployment**: Kubernetes with Helm charts

## Development Setup

### Prerequisites
- Go 1.23.6+
- PostgreSQL 16+
- Docker and Docker Compose
- Node.js 18+ (for documentation)

### Quick Start
```bash
# Install dependencies
make deps

# Start PostgreSQL with Docker
docker-compose up postgres -d

# Build the application
make build

# Run database migrations
./bin/sidecar database

# Start the sidecar (requires Ethereum RPC URL)
./bin/sidecar run \
  --ethereum.rpc-url=<YOUR_ETH_RPC_URL> \
  --chain=mainnet
```

### Environment Variables
Key configuration via environment variables (prefix: `SIDECAR_`):

```bash
SIDECAR_ETHEREUM_RPC_URL=http://localhost:8545
SIDECAR_CHAIN=mainnet                    # mainnet, holesky, testnet, preprod
SIDECAR_DATABASE_HOST=localhost
SIDECAR_DATABASE_PORT=5432
SIDECAR_DATABASE_USER=sidecar
SIDECAR_DATABASE_PASSWORD=sidecar
SIDECAR_DATABASE_DB_NAME=sidecar
SIDECAR_DEBUG=false
```

## Key Commands

### Building and Testing
```bash
make build                    # Build the sidecar binary
make test                     # Run all tests
make test-file FILE=path.go   # Run specific test file
make lint                     # Run linter
make fmt                      # Format code
make fmtcheck                 # Check code formatting
```

### Database Operations
```bash
./bin/sidecar database           # Run migrations
./bin/sidecar createSnapshot     # Create database snapshot
./bin/sidecar restoreSnapshot    # Restore from snapshot
```

### Running Services
```bash
./bin/sidecar run               # Full indexer + API server
./bin/sidecar rpc               # API server only
```

### Development Tools
```bash
make docs/dev                   # Start documentation site
make docker-buildx-self         # Build Docker image locally
```

## API Endpoints

### gRPC Services (Port 7100)
- **Events**: Block and state change streaming
- **Rewards**: Reward calculation and distribution data
- **Protocol**: General protocol state queries
- **Health**: Service health checks

### HTTP API (Port 7101)
RESTful endpoints with OpenAPI documentation available at `/docs`

Key endpoint categories:
- `/v1/rewards/` - Reward-related queries
- `/v1/protocol/` - Protocol state queries  
- `/v1/health/` - Health checks
- `/v1/events/` - Event streaming

## Database Schema

The application uses PostgreSQL with an extensive migration system. Key tables:

- **State Tables**: `blocks`, `transactions`, `transaction_logs`
- **Protocol State**: `operators`, `stakers`, `avs_operators`, `staker_delegations`
- **Rewards**: `reward_submissions`, `submitted_distribution_roots`, `gold_*` tables
- **Snapshots**: Various `*_snapshots` tables for historical data

Migrations are located in `pkg/postgres/migrations/` with timestamp-based naming.

## Configuration

Configuration is handled via:
1. Environment variables (prefixed with `SIDECAR_`)
2. Command line flags
3. Configuration files (YAML/JSON)

See `internal/config/config.go` for all available options.

## Testing

### Test Types
- **Unit Tests**: Package-level tests (`*_test.go`)
- **Integration Tests**: Database integration tests (`*Integration_test.go`)
- **Regression Tests**: Rewards calculation validation
- **Contract Tests**: Smart contract interaction tests

### Running Tests
```bash
make test                      # All tests
make test-rewards             # Rewards-specific tests
TEST_REWARDS=true make test   # Full rewards regression tests
```

### Test Configuration
Tests use environment variables for database connection:
- `SIDECAR_DATABASE_HOST`
- `SIDECAR_DATABASE_USER`  
- `SIDECAR_DATABASE_PASSWORD`
- `TESTING=true` (enables test mode)

## Deployment

### Docker
```bash
# Build image
make docker-buildx-self

# Run with Docker Compose
docker-compose up
```

### Kubernetes
Helm charts available in `charts/sidecar/`:
```bash
helm install sidecar ./charts/sidecar
```

## Development Workflow

### Code Standards
- **Linting**: golangci-lint with custom configuration
- **Formatting**: gofmt with automatic formatting checks
- **Commit Messages**: Conventional commits with commitlint
- **PR Requirements**: Branch must be up-to-date with master

### Contributing
1. Fork and create feature branch
2. Make changes with tests
3. Ensure all checks pass (`make lint`, `make test`, `make fmtcheck`)
4. Submit PR with conventional commit format

### Git Hooks
- Commit message linting via commitlint
- Format checking on push
- Branch update requirements for PRs

## Monitoring and Observability

### Metrics
- **Prometheus**: Metrics export on port 2112
- **DataDog**: StatsD integration with custom metrics
- **Health Checks**: `/health` and `/ready` endpoints

### Logging
- **Structured Logging**: Using zap logger
- **Debug Mode**: Extensive debug output available
- **APM**: DataDog tracing integration

## Snapshots and Data Management

The sidecar includes sophisticated snapshot functionality:

### Snapshot Types
- **slim**: Essential data only
- **full**: Complete state data (default)
- **archive**: Full historical data

### Snapshot Operations
```bash
# Create snapshot
./bin/sidecar createSnapshot --output=snapshot.dump --kind=full

# Restore snapshot
./bin/sidecar restoreSnapshot --input=snapshot.dump

# Remote snapshot restore
./bin/sidecar restoreSnapshot --manifest-url=https://sidecar.eigenlayer.xyz/snapshots/manifest.json
```

## Troubleshooting

### Common Issues
1. **Database Connection**: Verify PostgreSQL is running and credentials are correct
2. **Ethereum RPC**: Ensure RPC URL is accessible and supports required methods
3. **Memory Usage**: Rewards calculations can be memory-intensive
4. **Sync Time**: Initial sync can take several hours depending on chain

### Debug Mode
Enable with `--debug=true` or `SIDECAR_DEBUG=true` for verbose logging.

### Performance Tuning
- Adjust `ethereum.rpc-contract-call-batch-size` for RPC batching
- Configure PostgreSQL connection pool settings
- Use appropriate snapshot type for faster startup

## Documentation

Full documentation available at: https://sidecar-docs.eigenlayer.xyz/

### Local Documentation
```bash
cd docs
yarn install
yarn start    # Starts dev server on http://localhost:3000
```

## API Examples

See `examples/` directory for:
- **eventSubscriber**: Real-time event streaming
- **rewardsData**: Rewards data queries
- **rewardsProofs**: Merkle proof generation

## Support and Community

- **GitHub Issues**: Bug reports and feature requests
- **Documentation**: Comprehensive guides and API docs
- **Examples**: Working code samples in multiple languages

This project follows semantic versioning and maintains backward compatibility in API design.