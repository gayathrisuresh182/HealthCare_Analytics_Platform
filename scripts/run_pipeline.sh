#!/bin/bash
# Linux/Mac Shell Script for Data Pipeline
# Use with cron or systemd

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project directory
cd "$PROJECT_ROOT"

# Log file
LOG_FILE="$PROJECT_ROOT/logs/pipeline_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$PROJECT_ROOT/logs"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "========================================"
log "Starting Healthcare Analytics Pipeline"
log "========================================"

# Activate virtual environment (if using)
# source venv/bin/activate

# Run the enhanced pipeline script
log "Running pipeline script..."
python scripts/run_pipeline_enhanced.py 2>&1 | tee -a "$LOG_FILE"

# Check exit code
EXIT_CODE=${PIPESTATUS[0]}
if [ $EXIT_CODE -ne 0 ]; then
    log "Pipeline FAILED with exit code $EXIT_CODE"
    exit $EXIT_CODE
else
    log "Pipeline completed successfully"
    exit 0
fi

