#!/bin/bash

# ==========================================
# NEXO DEPLOYMENT GUIDE (EXECUTABLE)
# ==========================================
# This script documents the "Deploy Everything" strategy.
# It assumes you have already run: npm run release

VERSION=$(grep '^version =' Cargo.toml | cut -d '"' -f 2)

echo "🚀 Deploying Nexo v$VERSION..."

# 1. SERVER (Docker)
# ------------------
echo "🐳 1. Building & Pushing SERVER..."
# docker build -t nexo/server:$VERSION .
# docker push nexo/server:$VERSION
# docker tag nexo/server:$VERSION nexo/server:latest
# docker push nexo/server:latest

# 2. SDK (NPM)
# ------------
echo "📦 2. Publishing SDK..."
cd sdk/ts
# npm publish --access public
cd ../..

# 3. DOCS (Vercel/Netlify)
# ------------------------
echo "📄 3. Deploying Docs..."
# Usually handled automatically by Vercel/Netlify via Git Hooks
# But if manual:
# cd nexo-docs-hub && vercel --prod

echo "✅ Deployment steps completed (Dry Run)."
echo "   Real commands are commented out in scripts/deploy.sh for safety."
