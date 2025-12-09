#!/bin/bash
set -e

echo "🚀 [1/3] Building Solution (Release)..."
dotnet build -c Release --verbosity quiet

echo "🔨 [2/3] Building DocGen Tool..."
dotnet build tools/LeichtFrame.DocGen/LeichtFrame.DocGen.csproj -c Release --verbosity quiet

echo "📝 [3/3] Running DocGen..."
dotnet run --project tools/LeichtFrame.DocGen/LeichtFrame.DocGen.csproj -c Release

echo "✅ Documentation complete!"