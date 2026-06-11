---
name: Supabase vs Replit DB
description: DATABASE_URL and pg env vars point to Replit's internal DB, not Supabase. Always use supabaseAdmin (supabase-js) for all app data operations.
---

## Rule
Never use `DATABASE_URL`, `PGHOST`, `PGPASSWORD`, `PGUSER`, or the `pg` package for app data in this project. These connect to **Replit's internal PostgreSQL** (`heliumdb` on host `helium`), which is completely separate from Supabase.

**Why:** This was discovered after mistakenly creating the `channels` table and inserting 618 rows into Replit's internal DB instead of Supabase. The Supabase client (`supabaseAdmin`) couldn't find the table, causing `Could not find the table 'public.channels' in the schema cache` errors on every startup.

## Correct Approach

| Operation | How to do it |
|---|---|
| Read / Write data | `supabaseAdmin.from('table').select/insert/update/delete()` |
| Create tables (DDL) | `SUPABASE_ACCESS_TOKEN` → `POST https://api.supabase.com/v1/projects/{ref}/database/query` |
| Auth operations | `supabaseAdmin.auth.admin.*` |

## Env Vars

| Var | Points to |
|---|---|
| `DATABASE_URL` | ❌ Replit internal DB (`helium`) — DO NOT USE |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | ❌ Same Replit internal DB |
| `SUPABASE_URL` | ✅ Supabase REST endpoint |
| `SUPABASE_SERVICE_ROLE_KEY` | ✅ Supabase service role |
| `SUPABASE_ANON_KEY` | ✅ Supabase anon key |
| `SUPABASE_ACCESS_TOKEN` | ✅ Supabase Management API (for DDL) |

## How to apply
Before any DB operation: ask "am I using supabaseAdmin?" If the answer is no (e.g., using pg/Client/Pool), stop and switch to supabaseAdmin. For DDL, use the Management API script pattern in `scripts/migrate-to-supabase.js`.
