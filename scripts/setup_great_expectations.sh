#!/bin/bash
# Setup script for Great Expectations in Healthcare Analytics Platform

set -e

echo "🚀 Setting up Great Expectations for Healthcare Analytics Platform"
echo ""

# Check if GX is installed
if ! command -v great_expectations &> /dev/null; then
    echo "📦 Installing Great Expectations..."
    pip install great-expectations
else
    echo "✅ Great Expectations already installed"
fi

# Initialize GX project (if not already initialized)
if [ ! -d "great_expectations" ]; then
    echo "🔧 Initializing Great Expectations project..."
    great_expectations init --no-view
    echo "✅ Great Expectations project initialized"
else
    echo "✅ Great Expectations project already exists"
fi

echo ""
echo "📝 Next steps:"
echo "1. Configure Snowflake datasource:"
echo "   great_expectations datasource new"
echo ""
echo "2. Create expectation suite for marts:"
echo "   great_expectations suite new"
echo ""
echo "3. Create checkpoint:"
echo "   great_expectations checkpoint new"
echo ""
echo "4. Run validation:"
echo "   great_expectations checkpoint run <checkpoint_name>"
echo ""
echo "5. Build data docs:"
echo "   great_expectations docs build"
echo ""
echo "✅ Setup complete!"

