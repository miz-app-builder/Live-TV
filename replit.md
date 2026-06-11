# MIZ Live TV

## Project Overview

A Live HLS Streaming Web App built with Node.js + Express. Channels are stored in Supabase and served via a backend API. Supports guest and authenticated users, admin panel, HLS proxy, and real-time channel management.

- **Server:** `server.js` (Express, port 5000)
- **Database:** Supabase (channels, profiles, app_config tables)
- **Auth:** Supabase Auth with JWT verification
- **GitHub Repo:** https://github.com/miz-app-builder/Live-TV

## Automatic GitHub Push (Post-Merge)

After every task merge, `scripts/post-merge.sh` automatically:
1. Runs `npm install`
2. Copies `server.js` → `live-tv/server.js`
3. Commits any staged changes and pushes to `main` on GitHub

This triggers Railway to auto-deploy — no separate push task is needed.

**Required secret:** `GITHUB_TOKEN` must be set as a Replit environment secret with push access to `miz-app-builder/Live-TV`.

## User Preferences

- After every task is complete, changes are automatically pushed to GitHub via `scripts/post-merge.sh` so Railway auto-deploys.
- No manual push task is needed — the post-merge hook handles it.
