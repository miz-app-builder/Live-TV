# MIZ Live TV

## Project Overview

A Live HLS Streaming Web App built with Node.js + Express. Channels are stored in Supabase and served via a backend API. Supports guest and authenticated users, admin panel, HLS proxy, and real-time channel management.

- **Server:** `server.js` (Express, port 5000)
- **Database:** Supabase (channels, private_channels, profiles, app_config tables)
- **Auth:** Supabase Auth with JWT verification
- **GitHub Repo:** https://github.com/miz-app-builder/Live-TV
- **Live URL:** https://masudiz.work (hosted on Render.com)

## Automatic GitHub Push (Post-Merge)

After every task merge, `scripts/post-merge.sh` automatically:
1. Runs `npm install`
2. Copies `server.js` → `live-tv/server.js`
3. Pushes updated files to GitHub via Python + GitHub API

This triggers Render to auto-deploy — no separate push task is needed.

**Required secret:** `GITHUB_TOKEN` must be set as a Replit environment secret with push access to `miz-app-builder/Live-TV`.

## Deployment

- **Platform:** Render.com (free tier)
- **Auto-deploy:** Enabled — triggers on every GitHub push to `main`
- **Domain:** masudiz.work
- **Uptime:** UptimeRobot pings every 5 minutes to prevent sleep

## User Preferences

- After every task is complete, changes are automatically pushed to GitHub via `scripts/post-merge.sh` so Render auto-deploys.
- No manual push task is needed — the post-merge hook handles it.
- Use Python + GitHub API for all GitHub pushes (not git CLI commands).
