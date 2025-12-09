#!/bin/bash
set -e

rm -rf artifacts/doc_bin

echo "🚀 [1/3] Publishing Libraries to Central Artifacts Folder..."
dotnet publish src/LeichtFrame.Core/LeichtFrame.Core.csproj -c Release -o artifacts/doc_bin --verbosity quiet
dotnet publish src/LeichtFrame.IO/LeichtFrame.IO.csproj -c Release -o artifacts/doc_bin --verbosity quiet

echo "🔨 [2/3] Building DocGen Tool..."
dotnet build tools/LeichtFrame.DocGen/LeichtFrame.DocGen.csproj -c Release --verbosity quiet

echo "📝 [3/3] Running DocGen..."
dotnet run --project tools/LeichtFrame.DocGen/LeichtFrame.DocGen.csproj -c Release

echo "✅ Documentation complete!"