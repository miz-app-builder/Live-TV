const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { realtime: { transport: ws } }
);

const CAT_RULES = [
  { key: 'Bangla', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga','ntv','dbc','ekushey','deshi','sangeet','atn','channel i','channel 9','channel 24','sa tv','btv','sangsad','independent tv','star jalsha','jalsha','ruposhi','mohona','nagorik','tara bangla','zee bangla','gaan bangla','banglavision','gazi','duronto','shomoy','mytvbd','my tv','shopno'] },
  { key: 'News',   words: ['news','ndtv','republic','wion','cnn','bbc','dw','france 24','sky news','al-jazeera','aljazeera','al jazeera','india today','cgtn','times now','zee news','fox news','abc news','cnbc','geo news','ary news','hum news','nbcnews','nbcnews'] },
  { key: 'Movies', words: ['movie','cinema','film','bollywood','hollywood','goldmines','afriwood','artflix','biz cinema','classique','filmrise','moviesphere','grand cinema','star gold','zee cinema','zee bollywood','zee classic','b4u movies','sony max','sony pix','bflix','romedy','movies now','manoranjan'] },
  { key: 'Music',  words: ['music','beats','9xm','joo music','8xm','dhoom music','atn music','yrfmusic','zoom','zing','mtv','b4u music','e 24','sangeet'] },
  { key: 'Kids',   words: ['kids','cartoon','junior','motu','doraemon','pbs','zoo moo','tom &','jungle book','cbeebies','cheebies','nick','disney','sony yay'] },
  { key: 'Sports', words: ['sport','sports','cricket','football','soccer','tennis','basketball','wrestling','racing','olympic','dd sport','d sport','dsports','euro sport','ptv sport','geo super','sony six','sony ten','star sports','edge sports','fox sports'] },
];

function categorize(name) {
  const n = name.toLowerCase();
  for (const rule of CAT_RULES) {
    if (rule.words.some(w => n.includes(w))) return rule.key;
  }
  return 'International';
}

function cleanName(raw) {
  let name = raw.trim();
  name = name.replace(/^WoW:\s*/i, '');
  name = name.replace(/\s*\[Extra\]\s*$/i, '');
  name = name.replace(/^\d+\.\s*/, '');
  name = name.replace(/\s+/g, ' ').trim();
  return name;
}

function parseM3U(filePath) {
  const lines = fs.readFileSync(filePath, 'utf8').split('\n').map(l => l.trim()).filter(Boolean);
  const entries = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line.startsWith('#EXTINF')) {
      const commaIdx = line.indexOf(',');
      if (commaIdx === -1) continue;
      const rawName = line.slice(commaIdx + 1).trim();
      if (!rawName) continue;
      const urlLine = lines[i + 1];
      if (!urlLine || urlLine.startsWith('#')) continue;
      if (!urlLine.startsWith('http')) continue;
      const name = cleanName(rawName);
      if (!name) continue;
      entries.push({ name, url: urlLine });
      i++;
    }
  }
  return entries;
}

async function getMaxId() {
  const { data } = await supabase.from('channels').select('id').order('id', { ascending: false }).limit(1);
  return data && data.length > 0 ? data[0].id : 0;
}

async function getExistingUrls() {
  let allUrls = new Set();
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await supabase.from('channels').select('stream_url').range(from, from + pageSize - 1);
    if (error || !data || data.length === 0) break;
    data.forEach(r => allUrls.add(r.stream_url));
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return allUrls;
}

async function main() {
  const filePath = path.join(__dirname, '../attached_assets/Pasted--EXTM3U-EXTINF-1-Gazi-tv-http-itpolly-iptv-digijadoo-ne_1781155035664.txt');
  console.log('Parsing M3U file...');
  const entries = parseM3U(filePath);
  console.log(`Parsed ${entries.length} entries`);

  console.log('Fetching existing URLs from DB...');
  const existingUrls = await getExistingUrls();
  console.log(`Found ${existingUrls.size} existing URLs`);

  const newEntries = entries.filter(e => !existingUrls.has(e.url));
  console.log(`${newEntries.length} new entries to insert`);

  if (newEntries.length === 0) {
    console.log('Nothing to insert.');
    return;
  }

  let maxId = await getMaxId();
  console.log(`Current max ID: ${maxId}`);

  const rows = newEntries.map((e, idx) => ({
    id: maxId + idx + 1,
    channel_name: e.name,
    stream_url: e.url,
    category: categorize(e.name),
    status: 'Online',
    visible_to_guests: true,
  }));

  const BATCH = 200;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase.from('channels').insert(batch);
    if (error) {
      console.error(`Batch ${Math.floor(i/BATCH)+1} error:`, error.message);
    } else {
      inserted += batch.length;
      console.log(`Inserted ${inserted}/${rows.length}...`);
    }
  }

  console.log(`Done! ${inserted} channels inserted.`);
}

main().catch(console.error);
