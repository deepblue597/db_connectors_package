#!/bin/bash
set -e

echo "🚀 Updating db-connectors package..."

# Clean build artifacts
echo "🧹 Cleaning build artifacts..."
rm -rf dist/ build/

# Build package
echo "📦 Building package..."
uv build

# Check package
echo "✅ Checking package..."
uv run twine check dist/*

# Upload to Test PyPI
echo "🧪 Uploading to Test PyPI..."
uv run twine upload --repository testpypi dist/*

echo "✅ Package uploaded to Test PyPI!"
echo "Test with: pip install --index-url https://test.pypi.org/simple/ db-connectors"
echo ""
echo "If everything works, upload to production with:"
echo "uv run twine upload dist/*"