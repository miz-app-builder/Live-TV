# MIZ Live TV

## Project Overview

A Live HLS Streaming Web App built with Node.js + Express. Channels are stored in Supabase and served via a backend API. Supports guest and authenticated users, admin panel, HLS proxy, and real-time channel management.

- **Server:** `server.js` (Express, port 5000)
- **Database:** Supabase (channels, profiles, app_config tables)
- **Auth:** Supabase Auth with JWT verification
- **GitHub Repo:** https://github.com/miz-app-builder/Live-TV

## User Preferences

- After every task is complete, push all changes to the GitHub repo so Railway auto-deploys the update.
- Git push must be done via a **Project Task** (Replit restricts direct git operations from the main agent).
- The push command to use inside the Project Task:
  ```bash
  git remote set-url origin https://$GITHUB_TOKEN@github.com/miz-app-builder/Live-TV.git 2>/dev/null || git remote add origin https://$GITHUB_TOKEN@github.com/miz-app-builder/Live-TV.git
  git add -A
  git commit -m "<short description of what changed>" || true
  git push origin main
  ```
