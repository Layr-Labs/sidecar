#!/bin/bash

# ============================================================================
# Rewards V2.2 Sidecar E2E Tests
# ============================================================================
# This script runs sidecar Go tests against an Anvil fork that has already
# been seeded with test data using eigenlayer-contracts.
#
# Prerequisites:
#   1. Anvil running with mainnet fork
#   2. Test data seeded via eigenlayer-contracts (using zeus)
#   3. PostgreSQL running locally
#
# Usage:
#   ./run_sidecar_tests.sh [test_pattern]
#
# Examples:
#   ./run_sidecar_tests.sh                    # Run all E2E Anvil tests
#   ./run_sidecar_tests.sh Test_E2E_Anvil_SSS_1  # Run specific test
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
ANVIL_RPC="${ANVIL_RPC:-http://localhost:8545}"
TEST_PATTERN="${1:-Test_E2E_Anvil}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# PostgreSQL configuration
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

# Sidecar configuration
export TEST_E2E_ANVIL=true
export SIDECAR_CHAIN=mainnet
export SIDECAR_ETHEREUM_RPC_URL="$ANVIL_RPC"
export SIDECAR_REWARDS_V2_2_ENABLED=true

# ============================================================================
# Pre-flight Checks
# ============================================================================

check_anvil() {
    log_info "Checking Anvil connection at $ANVIL_RPC..."
    
    if ! command -v cast &> /dev/null; then
        log_error "cast not found. Install Foundry first."
        exit 1
    fi
    
    if ! cast block-number --rpc-url "$ANVIL_RPC" &>/dev/null; then
        log_error "Cannot connect to Anvil at $ANVIL_RPC"
        log_error ""
        log_error "Make sure Anvil is running with mainnet fork:"
        log_error "  anvil --fork-url <MAINNET_RPC_URL> --port 8545"
        exit 1
    fi
    
    BLOCK_NUM=$(cast block-number --rpc-url "$ANVIL_RPC")
    log_success "Connected to Anvil at block $BLOCK_NUM"
}

check_postgres() {
    log_info "Checking PostgreSQL connection..."
    
    if ! command -v psql &> /dev/null; then
        log_warn "psql not found - skipping database check"
        return 0
    fi
    
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -c "SELECT 1;" &>/dev/null; then
        log_success "PostgreSQL connection OK"
    else
        log_warn "Cannot connect to PostgreSQL - tests may fail"
    fi
}

# ============================================================================
# Run Tests
# ============================================================================

run_tests() {
    log_info "Running sidecar E2E tests..."
    log_info "Test pattern: $TEST_PATTERN"
    
    cd "$PROJECT_ROOT"
    
    # Run Go tests
    go test -v ./pkg/rewards \
        -run "$TEST_PATTERN" \
        -timeout 30m
    
    log_success "Tests completed"
}

# ============================================================================
# Main
# ============================================================================

print_usage() {
    echo ""
    echo "Rewards V2.2 Sidecar E2E Tests"
    echo "=============================="
    echo ""
    echo "Usage: $0 [test_pattern]"
    echo ""
    echo "This script runs sidecar Go tests against an Anvil mainnet fork."
    echo ""
    echo "Prerequisites:"
    echo "  1. Anvil running with mainnet fork"
    echo "  2. Test data seeded via eigenlayer-contracts (using zeus)"
    echo ""
    echo "Test Patterns:"
    echo "  Test_E2E_Anvil              - All E2E Anvil tests (default)"
    echo "  Test_E2E_Anvil_Connection   - Just connection test"
    echo "  Test_E2E_Anvil_SSS_1        - SSS scenario 1"
    echo "  Test_E2E_Anvil_Full         - Full E2E test"
    echo ""
    echo "Environment Variables:"
    echo "  ANVIL_RPC         - Anvil RPC URL (default: http://localhost:8545)"
    echo "  POSTGRES_HOST     - PostgreSQL host (default: localhost)"
    echo "  POSTGRES_PORT     - PostgreSQL port (default: 5432)"
    echo "  POSTGRES_USER     - PostgreSQL user (default: postgres)"
    echo "  POSTGRES_PASSWORD - PostgreSQL password (default: postgres)"
    echo ""
}

main() {
    if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
        print_usage
        exit 0
    fi
    
    echo ""
    echo "========================================"
    echo "   Sidecar E2E Tests (Mainnet Fork)    "
    echo "========================================"
    echo ""
    
    check_anvil
    check_postgres
    run_tests
}

main "$@"

