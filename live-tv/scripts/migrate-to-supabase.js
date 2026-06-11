const { Client } = require('pg');

const PROJECT_REF = (process.env.SUPABASE_URL || '').match(/https:\/\/([^.]+)\.supabase\.co/)?.[1];
const ACCESS_TOKEN = process.env.SUPABASE_ACCESS_TOKEN;
const SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;

if (!PROJECT_REF || !ACCESS_TOKEN) {
  console.error('Missing SUPABASE_URL or SUPABASE_ACCESS_TOKEN'); process.exit(1);
}

async function execSQL(query) {
  const res = await fetch(`https://api.supabase.com/v1/projects/${PROJECT_REF}/database/query`, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}

async function supabaseInsert(rows) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/channels`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${SERVICE_ROLE}`,
      'apikey': SERVICE_ROLE,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(rows)
  });
  if (!res.ok) { const err = await res.text(); throw new Error(err); }
}

async function migrate() {
  console.log('Project ref:', PROJECT_REF);

  // Step 1: Read channels from Replit's DB (where old migration stored them)
  console.log('\n1. Reading channels from Replit DB...');
  const pgClient = new Client({ connectionString: process.env.DATABASE_URL, ssl: false });
  await pgClient.connect();
  const { rows: channels } = await pgClient.query('SELECT * FROM channels ORDER BY id');
  await pgClient.end();
  console.log(`   Found ${channels.length} channels in Replit DB`);

  if (!channels.length) { console.error('No channels found in Replit DB!'); process.exit(1); }

  // Step 2: Create channels table in Supabase
  console.log('\n2. Creating channels table in Supabase...');
  await execSQL(`
    CREATE TABLE IF NOT EXISTS public.channels (
      id                INTEGER PRIMARY KEY,
      channel_name      TEXT NOT NULL,
      stream_url        TEXT NOT NULL,
      category          TEXT NOT NULL DEFAULT 'International',
      status            TEXT NOT NULL DEFAULT 'Online',
      visible_to_guests BOOLEAN NOT NULL DEFAULT TRUE,
      created_at        TIMESTAMPTZ DEFAULT NOW(),
      updated_at        TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  console.log('   Table created.');

  // Step 3: RLS
  console.log('\n3. Setting up RLS...');
  await execSQL(`ALTER TABLE public.channels ENABLE ROW LEVEL SECURITY;`);
  await execSQL(`
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='channels' AND policyname='Anyone can read channels') THEN
        CREATE POLICY "Anyone can read channels" ON public.channels FOR SELECT USING (TRUE);
      END IF;
    END $$;
  `);
  await execSQL(`
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='channels' AND policyname='Service role manages channels') THEN
        CREATE POLICY "Service role manages channels" ON public.channels FOR ALL USING (TRUE);
      END IF;
    END $$;
  `);
  console.log('   RLS enabled.');

  // Step 4: Insert channels into Supabase
  console.log('\n4. Inserting channels into Supabase...');
  const rows = channels.map(ch => ({
    id: ch.id, channel_name: ch.channel_name, stream_url: ch.stream_url,
    category: ch.category, status: ch.status, visible_to_guests: ch.visible_to_guests
  }));
  const batchSize = 100;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    await supabaseInsert(rows.slice(i, i + batchSize));
    inserted += Math.min(batchSize, rows.length - i);
    process.stdout.write(`\r   Inserted ${inserted}/${rows.length}`);
  }
  console.log('\n   All channels inserted!');

  // Step 5: Verify
  console.log('\n5. Verifying...');
  const countRes = await execSQL('SELECT COUNT(*) as cnt FROM public.channels');
  console.log(`   Supabase channels table: ${countRes[0]?.cnt} rows`);

  const catRes = await execSQL(`SELECT category, COUNT(*) as cnt FROM public.channels GROUP BY category ORDER BY cnt DESC`);
  console.log('   Categories:');
  catRes.forEach(r => console.log(`     ${r.category}: ${r.cnt}`));

  console.log('\nMigration to Supabase DONE!');
}

migrate().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
