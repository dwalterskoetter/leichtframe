#!/bin/bash
set -e # Exits as soon as any command fails

echo "🚀 [1/4] Building Solution (Release)..."
# Builds Core and IO, updates XMLs and DLLs
dotnet build -c Release --verbosity quiet

echo "🧹 [2/4] Cleaning & Preparing website directories..."
# Ensure target directories exist
mkdir -p website/docs/api
mkdir -p website/docs/guides

# Delete old API documentation (so deleted classes also disappear here)
rm -rf website/docs/api/*

echo "📂 [3/4] Copying manual documentation..."
# This is where the magic happens: We copy PROFILING.md into the website folder
if [ -f "docs/PROFILING.md" ]; then
    cp docs/PROFILING.md website/docs/guides/profiling.md
    echo "   -> Copied PROFILING.md to website/docs/guides/"
else
    echo "⚠️ Warning: docs/PROFILING.md not found!"
fi

echo "📝 [4/4] Generating API Reference (MdDocs)..."
# Generates the API documentation from the DLLs
dotnet tool run mddocs apireference \
  --assemblies "src/LeichtFrame.Core/bin/Release/net8.0/LeichtFrame.Core.dll" "src/LeichtFrame.IO/bin/Release/net8.0/LeichtFrame.IO.dll" \
  --outdir "website/docs/api"

echo "✅ Documentation generation complete!"