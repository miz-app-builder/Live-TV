# MIZ Live TV

A Node.js + Express live streaming app. All HTML is server-side rendered as template literals in `server.js`.

## Stack
- **Backend**: Node.js + Express (`server.js`)
- **Database**: Supabase (PostgreSQL)
- **Auth**: Supabase Auth
- **Streaming**: HLS proxy built into Express

## Running
```
node server.js
```
Runs on port 5000.

## Key Files
- `server.js` — entire app (~2600 lines), all routes and HTML templates
- `supabase-setup.sql` — Supabase DB schema reference
- `scripts/` — migration scripts (one-time use)
- `package.json` — dependencies

## Supabase Tables
- `profiles` — user roles (admin / member)
- `app_config` — key/value settings (e.g. guest_limit_minutes)
- `channels` — all 618+ channels (name, URL, category, visible_to_guests)

---

## ⚠️ CRITICAL: Database Rules — NEVER Break These

### DATABASE_URL / pg package = Replit's internal DB (NOT Supabase)
| Env var | Points to | Use for |
|---|---|---|
| `DATABASE_URL` | Replit internal PostgreSQL (`heliumdb` on host `helium`) | ❌ NEVER use for app data |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` | Same Replit internal DB | ❌ NEVER use |
| `SUPABASE_URL` | Supabase project REST endpoint | ✅ All app DB operations |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role JWT | ✅ Admin/server DB access |
| `SUPABASE_ANON_KEY` | Supabase anon JWT | ✅ Client-side access |
| `SUPABASE_ACCESS_TOKEN` | Supabase Management API token | ✅ DDL (CREATE TABLE etc.) |

### Rules
1. **Always use `supabaseAdmin` (supabase-js client) for all DB reads/writes**
2. **For CREATE TABLE / DDL**: use `SUPABASE_ACCESS_TOKEN` → Management API → `POST https://api.supabase.com/v1/projects/{ref}/database/query`
3. **Never `require('pg')` or use `DATABASE_URL`** for app data — that goes to Replit's own DB which is completely separate from Supabase
4. The `pg` package was already used by mistake — the `scripts/` migration scripts exist to fix this
5. Supabase project ref: extract from `SUPABASE_URL` → `https://[REF].supabase.co`

---

## Admin Panel
- Route: `/admin`
- Tabs: Dashboard, Users, Channels, Settings
- Auth: Supabase token required, role must be `admin` in `profiles` table

## Guest Timer
- Configurable via admin Settings tab
- Stored in `app_config` table, key `guest_limit_minutes`
- Default: 5 minutes

## User Preferences
- All DB operations go through Supabase (supabase-js), never Replit's internal DB
