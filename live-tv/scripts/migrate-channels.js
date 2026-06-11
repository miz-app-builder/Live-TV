const { Client } = require('pg');
const fs = require('fs');

const CAT_RULES = [
  { key: 'Bangla', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga','ntv','dbc','ekushey','deshi','sangeet','atn','channel i','channel 9','channel 24','sa tv','btv','sangsad','independent tv','star news','kolkata tv','tv9 bangla','r bangla','enter10','jalsha','g-series','aakash aath','ananda tv'] },
  { key: 'News',   words: ['news','ndtv','republic','wion','cnn','bbc news','dw ','france 24','sky news','al-jazeera','aljazeera','india today','cgtn','times now','zee news','fox news','abc news','cnbc'] },
  { key: 'Movies', words: ['movie','cinema','film','bollywood','hollywood','goldmines','afriwood','artflix','biz cinema','classique','filmrise','moviesphere','grand cinema'] },
  { key: 'Music',  words: ['music','beats','9xm','joo music','8xm','dhoom music','atn music','yrfmusic'] },
  { key: 'Kids',   words: ['kids','cartoon','junior','motu','doraemon','pbs','zoo moo','tom &','jungle book','cbeebies'] },
  { key: 'Sports', words: ['sport','dd sport','cricket','football','soccer','tennis','basketball','wrestling','racing','olympic'] },
];
function categorize(name) {
  const n = name.toLowerCase();
  for (const rule of CAT_RULES) {
    if (rule.words.some(w => n.includes(w))) return rule.key;
  }
  return 'International';
}

const serverContent = fs.readFileSync('./server.js', 'utf8');
const lines = serverContent.split('\n');
const si = lines.findIndex(l => l.trimStart().startsWith('const channels = ['));
let depth = 0, ei = -1;
for (let i = si; i < lines.length; i++) {
  for (const c of lines[i]) {
    if (c === '[') depth++;
    else if (c === ']') { depth--; if (depth === 0) { ei = i; break; } }
  }
  if (ei >= 0) break;
}
if (si < 0 || ei < 0) { console.error('Could not locate channels array'); process.exit(1); }

const arrayCode = lines.slice(si, ei + 1).join('\n');
const fn = new Function(arrayCode + '\nreturn channels;');
const channels = fn();
console.log(`Parsed ${channels.length} channels from server.js`);

async function migrate() {
  const client = new Client({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
  await client.connect();
  console.log('Connected to DB');

  await client.query(`
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
  await client.query(`ALTER TABLE public.channels ENABLE ROW LEVEL SECURITY;`);
  await client.query(`
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='channels' AND policyname='Anyone can read channels') THEN
        CREATE POLICY "Anyone can read channels" ON public.channels FOR SELECT USING (TRUE);
      END IF;
    END $$;
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='channels' AND policyname='Service role manages channels') THEN
        CREATE POLICY "Service role manages channels" ON public.channels FOR ALL USING (TRUE);
      END IF;
    END $$;
  `);
  console.log('Table created with RLS policies');

  let blockedIds = new Set();
  try {
    const visRes = await client.query(`SELECT channel_id FROM public.channel_visibility WHERE visible_to_guests = false`);
    visRes.rows.forEach(r => blockedIds.add(r.channel_id));
    console.log(`Migrating ${blockedIds.size} blocked channels from channel_visibility`);
  } catch (_) { console.log('channel_visibility: not found or empty — defaulting all visible'); }

  let urlOverrides = {};
  try {
    const ovRes = await client.query(`SELECT channel_id, stream_url FROM public.channel_url_overrides`);
    ovRes.rows.forEach(r => { urlOverrides[r.channel_id] = r.stream_url; });
    console.log(`Migrating ${Object.keys(urlOverrides).length} URL overrides from channel_url_overrides`);
  } catch (_) { console.log('channel_url_overrides: not found or empty'); }

  const batchSize = 50;
  let inserted = 0;
  for (let i = 0; i < channels.length; i += batchSize) {
    const batch = channels.slice(i, i + batchSize);
    const vph = [], params = [];
    let p = 1;
    for (const ch of batch) {
      const url = urlOverrides[ch.id] || ch.stream_url;
      const visible = !blockedIds.has(ch.id);
      vph.push(`($${p},$${p+1},$${p+2},$${p+3},$${p+4},$${p+5})`);
      params.push(ch.id, ch.channel_name, url, categorize(ch.channel_name), ch.status || 'Online', visible);
      p += 6;
    }
    await client.query(
      `INSERT INTO public.channels (id,channel_name,stream_url,category,status,visible_to_guests)
       VALUES ${vph.join(',')}
       ON CONFLICT (id) DO UPDATE SET
         channel_name=EXCLUDED.channel_name, stream_url=EXCLUDED.stream_url,
         category=EXCLUDED.category, status=EXCLUDED.status, updated_at=NOW()`,
      params
    );
    inserted += batch.length;
    process.stdout.write(`\rInserted ${inserted}/${channels.length}`);
  }

  console.log('\nAll channels inserted!');
  const countRes = await client.query('SELECT COUNT(*) FROM public.channels');
  console.log(`DB total: ${countRes.rows[0].count} channels`);

  const catRes = await client.query(`SELECT category, COUNT(*) as cnt FROM public.channels GROUP BY category ORDER BY cnt DESC`);
  console.log('Category breakdown:');
  catRes.rows.forEach(r => console.log(`  ${r.category}: ${r.cnt}`));

  await client.end();
  console.log('Migration complete!');
}

migrate().catch(e => { console.error('Migration failed:', e.message); process.exit(1); });
