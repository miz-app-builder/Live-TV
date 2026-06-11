#!/bin/bash
set -e

npm install --legacy-peer-deps
cp server.js live-tv/server.js

if [ -n "$GITHUB_TOKEN" ]; then
  git remote set-url origin https://$GITHUB_TOKEN@github.com/miz-app-builder/Live-TV.git 2>/dev/null || \
    git remote add origin https://$GITHUB_TOKEN@github.com/miz-app-builder/Live-TV.git
  git config user.email "replit-agent@users.noreply.github.com"
  git config user.name "Replit Agent"
  git add -A
  git commit -m "Auto-push: $(date -u '+%Y-%m-%d %H:%M UTC')" || true
  git push origin main
else
  echo "GITHUB_TOKEN not set — skipping GitHub push"
fi
