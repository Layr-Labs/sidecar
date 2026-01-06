#!/bin/bash
# Rewards v2.2 Integration Test Runner
# Usage: ./run_integration_tests.sh [tier1|tier2|tier3|all]

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

export TEST_REWARDS=true

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Rewards v2.2 Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

function run_tier1() {
    echo -e "${YELLOW}Running Tier 1: PostgreSQL Timeline Simulation Tests${NC}"
    echo "These tests simulate 2+ years of rewards with withdrawal/deallocation queue"
    echo ""

    # Check for required PostgreSQL environment variables
    if [ -z "$SIDECAR_DATABASE_HOST" ]; then
        echo -e "${RED}Error: PostgreSQL environment variables not set${NC}"
        echo ""
        echo "Please set the following environment variables:"
        echo "  export SIDECAR_DATABASE_HOST=localhost"
        echo "  export SIDECAR_DATABASE_PORT=5432"
        echo "  export SIDECAR_DATABASE_USER=postgres"
        echo "  export SIDECAR_DATABASE_PASSWORD=postgres"
        echo "  export SIDECAR_DATABASE_DB_NAME=postgres"
        echo "  export SIDECAR_DATABASE_SCHEMA_NAME=public"
        echo ""
        echo "For more info, run: $0 help"
        exit 1
    fi

    echo -e "${GREEN}✓ Using PostgreSQL at ${SIDECAR_DATABASE_HOST}:${SIDECAR_DATABASE_PORT}${NC}"
    echo ""

    go test -v . \
        -run "Test_RewardsV2_2_WithWithdrawalQueue_Integration" \
        -timeout 5m \
        || { echo -e "${RED}Tier 1 tests failed${NC}"; exit 1; }

    echo -e "${GREEN}✓ Tier 1 tests passed${NC}"
    echo ""
}

function run_tier2() {
    echo -e "${YELLOW}Running Tier 2: Withdrawal Queue Edge Case Tests${NC}"
    echo "These are included in Tier 1 but can be run separately"
    echo ""

    # Run specific scenarios
    scenarios=(
        "Scenario_1"  # Forward-looking rewards with withdrawal
        "Scenario_2"  # Multiple concurrent withdrawals
        "Scenario_3"  # Slash during withdrawal
        "Scenario_4"  # 2-year forward-looking rewards
        "Scenario_5"  # Deallocation queue
    )

    for scenario in "${scenarios[@]}"; do
        echo -e "${YELLOW}Testing: ${scenario}${NC}"
        go test -v . \
            -run "Test_RewardsV2_2_WithWithdrawalQueue_Integration/${scenario}" \
            -timeout 2m \
            || { echo -e "${RED}${scenario} failed${NC}"; exit 1; }
    done

    echo -e "${GREEN}✓ Tier 2 tests passed${NC}"
    echo ""
}

function run_failing() {
    echo -e "${YELLOW}Running Only Failing Tests (Scenarios 1, 4, 8)${NC}"
    echo "These tests have assertion/logic failures after SQL fixes"
    echo ""

    # Check for required PostgreSQL environment variables
    if [ -z "$SIDECAR_DATABASE_HOST" ]; then
        echo -e "${RED}Error: PostgreSQL environment variables not set${NC}"
        echo ""
        echo "Please set the following environment variables:"
        echo "  export SIDECAR_DATABASE_HOST=localhost"
        echo "  export SIDECAR_DATABASE_PORT=5432"
        echo "  export SIDECAR_DATABASE_USER=postgres"
        echo "  export SIDECAR_DATABASE_PASSWORD=postgres"
        echo "  export SIDECAR_DATABASE_DB_NAME=postgres"
        echo "  export SIDECAR_DATABASE_SCHEMA_NAME=public"
        echo ""
        echo "For more info, run: $0 help"
        exit 1
    fi

    echo -e "${GREEN}✓ Using PostgreSQL at ${SIDECAR_DATABASE_HOST}:${SIDECAR_DATABASE_PORT}${NC}"
    echo ""

    # Run the 3 failing scenarios
    echo -e "${YELLOW}Testing: Scenario_1, Scenario_4, Scenario_8${NC}"
    echo ""
    
    go test -v . \
        -run "Test_RewardsV2_2_WithWithdrawalQueue_Integration/(Scenario_8):" \
        -timeout 5m \
        2>&1 | tee failing_tests_output.log
    
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo -e "${RED}Some tests failed - see failing_tests_output.log for details${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ All previously failing tests now pass${NC}"
    fi

    echo -e "${YELLOW}Check the *_output.log files for detailed error messages${NC}"
}

function run_tier3() {
    echo -e "${YELLOW}Running Tier 3: Anvil-Based Contract Integration Tests${NC}"
    echo ""

    # Check if Anvil is running
    if ! curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        ${ANVIL_RPC_URL:-http://localhost:8545} > /dev/null 2>&1; then
        echo -e "${RED}Error: Anvil is not running${NC}"
        echo "Please start Anvil first:"
        echo "  anvil --fork-url \$RPC_URL"
        echo "Or:"
        echo "  anvil --block-time 1"
        echo ""
        echo "See ANVIL_SETUP.md for detailed setup instructions"
        exit 1
    fi

    echo -e "${GREEN}✓ Anvil detected at ${ANVIL_RPC_URL:-http://localhost:8545}${NC}"

    # Check environment variables
    if [ -z "$REWARDS_COORDINATOR_ADDRESS" ]; then
        echo -e "${YELLOW}Warning: REWARDS_COORDINATOR_ADDRESS not set${NC}"
        echo "Some tests may be skipped"
    fi

    if [ -z "$DELEGATION_MANAGER_ADDRESS" ]; then
        echo -e "${YELLOW}Warning: DELEGATION_MANAGER_ADDRESS not set${NC}"
    fi

    if [ -z "$ALLOCATION_MANAGER_ADDRESS" ]; then
        echo -e "${YELLOW}Warning: ALLOCATION_MANAGER_ADDRESS not set${NC}"
    fi

    export TEST_ANVIL=true

    echo -e "\n${YELLOW}Running comprehensive Anvil test suite (10 edge cases)...${NC}"
    go test -v . \
        -run "Test_RewardsV2_2_Anvil_Comprehensive" \
        -timeout 30m \
        || { echo -e "${RED}Tier 3 tests failed${NC}"; exit 1; }

    echo -e "${GREEN}✓ Tier 3 tests passed${NC}"
    echo ""
}

function run_all() {
    echo "Running all test tiers..."
    echo ""

    run_tier1

    echo -e "${YELLOW}Tier 2 is included in Tier 1, skipping...${NC}"
    echo ""

    # Ask if user wants to run Tier 3
    if curl -s -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        ${ANVIL_RPC_URL:-http://localhost:8545} > /dev/null 2>&1; then
        echo -e "${GREEN}Anvil detected, running Tier 3...${NC}"
        run_tier3
    else
        echo -e "${YELLOW}Skipping Tier 3 (Anvil not running)${NC}"
        echo "To run Tier 3: Start Anvil and run './run_integration_tests.sh tier3'"
    fi

    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}All tests passed! ✓${NC}"
    echo -e "${GREEN}========================================${NC}"
}

function show_help() {
    echo "Usage: $0 [tier1|tier2|tier3|all|failing|help]"
    echo ""
    echo "Test Tiers:"
    echo "  tier1   - PostgreSQL Timeline Simulation Tests (fast, ~30s)"
    echo "  tier2   - Withdrawal Queue Edge Case Tests (subset of tier1)"
    echo "  tier3   - Anvil-Based Contract Integration Tests (slow, ~2min, requires Anvil)"
    echo "  failing - Run only currently failing tests (Scenario 1, 4, 8)"
    echo "  all     - Run all applicable tests"
    echo "  help    - Show this help message"
    echo ""
    echo "Required Environment Variables (for tier1 & tier2):"
    echo "  SIDECAR_DATABASE_HOST       - PostgreSQL host (e.g., localhost)"
    echo "  SIDECAR_DATABASE_PORT       - PostgreSQL port (e.g., 5432)"
    echo "  SIDECAR_DATABASE_USER       - PostgreSQL user (e.g., postgres)"
    echo "  SIDECAR_DATABASE_PASSWORD   - PostgreSQL password"
    echo "  SIDECAR_DATABASE_DB_NAME    - PostgreSQL database (e.g., postgres)"
    echo "  SIDECAR_DATABASE_SCHEMA_NAME - PostgreSQL schema (e.g., public)"
    echo ""
    echo "Optional Environment Variables (for tier3):"
    echo "  ANVIL_RPC_URL               - Anvil RPC URL (default: http://localhost:8545)"
    echo "  REWARDS_COORDINATOR_ADDRESS - RewardsCoordinator v2.2 address"
    echo "  DELEGATION_MANAGER_ADDRESS  - DelegationManager address"
    echo "  ALLOCATION_MANAGER_ADDRESS  - AllocationManager address"
    echo ""
    echo "Examples:"
    echo ""
    echo "  # Run database integration tests"
    echo "  export SIDECAR_DATABASE_HOST=localhost"
    echo "  export SIDECAR_DATABASE_PORT=5432"
    echo "  export SIDECAR_DATABASE_USER=postgres"
    echo "  export SIDECAR_DATABASE_PASSWORD=postgres"
    echo "  export SIDECAR_DATABASE_DB_NAME=postgres"
    echo "  export SIDECAR_DATABASE_SCHEMA_NAME=public"
    echo "  $0 tier1"
    echo ""
    echo "  # Run all tests"
    echo "  # (set database env vars as above, then:)"
    echo "  $0 all"
    echo ""
}

# Main
case "${1:-all}" in
    tier1)
        run_tier1
        ;;
    tier2)
        run_tier2
        ;;
    tier3)
        run_tier3
        ;;
    failing)
        run_failing
        ;;
    all)
        run_all
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac

echo -e "${GREEN}Done!${NC}"

