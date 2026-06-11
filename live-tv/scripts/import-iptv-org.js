const https = require('https');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

const supabase = createClient(
  process.env.SUPABASE_URL || '',
  process.env.SUPABASE_SERVICE_ROLE_KEY || '',
  { realtime: { transport: ws } }
);

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchText(res.headers.location).then(resolve).catch(reject);
      }
      let raw = '';
      res.on('data', chunk => raw += chunk);
      res.on('end', () => resolve(raw));
    }).on('error', reject);
  });
}

function parseM3U(text) {
  const lines = text.split('\n');
  const channels = [];
  let meta = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line.startsWith('#EXTINF')) {
      const nameMatch = line.match(/,(.+)$/);
      const groupMatch = line.match(/group-title="([^"]*)"/i);
      const countryMatch = line.match(/tvg-country="([^"]*)"/i);
      const langMatch = line.match(/tvg-language="([^"]*)"/i);

      meta = {
        name: nameMatch ? nameMatch[1].trim() : 'Unknown',
        category: groupMatch ? groupMatch[1].trim() : 'General',
        country: countryMatch ? countryMatch[1].trim() : '',
        language: langMatch ? langMatch[1].trim() : '',
      };
    } else if (line.startsWith('http') && meta) {
      channels.push({
        name: meta.name,
        stream_url: line.trim(),
        category: meta.category || 'General',
        description: [
          meta.country ? `Country: ${meta.country}` : '',
          meta.language ? `Language: ${meta.language}` : '',
        ].filter(Boolean).join(' | ') || '',
      });
      meta = null;
    } else if (!line.startsWith('#') && line.length > 0 && meta) {
      meta = null;
    }
  }
  return channels;
}

async function clearExisting() {
  console.log('🗑️  Clearing existing iptv-org channels...');
  const { error } = await supabase
    .from('private_channels')
    .delete()
    .like('description', '%iptv-org%');
  if (error) console.log('Clear note:', error.message);
}

async function insertBatch(batch) {
  const { error } = await supabase.from('private_channels').insert(batch);
  if (error) throw new Error(error.message);
}

async function main() {
  console.log('📥 Fetching iptv-org full channel list...');
  const m3uText = await fetchText('https://iptv-org.github.io/iptv/index.m3u');
  console.log(`✅ Downloaded ${Math.round(m3uText.length / 1024)} KB`);

  const channels = parseM3U(m3uText);
  console.log(`📺 Parsed ${channels.length} channels`);

  // Tag each with source
  const tagged = channels.map(ch => ({
    ...ch,
    description: ch.description ? ch.description + ' | iptv-org' : 'iptv-org',
  }));

  // Group summary
  const catCount = {};
  const countryCount = {};
  tagged.forEach(ch => {
    catCount[ch.category] = (catCount[ch.category] || 0) + 1;
    const c = ch.description.match(/Country: ([^ |]+)/)?.[1] || 'Unknown';
    countryCount[c] = (countryCount[c] || 0) + 1;
  });

  console.log('\n📊 Category breakdown:');
  Object.entries(catCount).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => {
    console.log(`   ${k}: ${v}`);
  });

  console.log('\n🌍 Top 20 countries:');
  Object.entries(countryCount).sort((a, b) => b[1] - a[1]).slice(0, 20).forEach(([k, v]) => {
    console.log(`   ${k}: ${v}`);
  });

  // Insert in batches of 500
  const BATCH = 500;
  let inserted = 0;
  let failed = 0;

  console.log(`\n🚀 Inserting ${tagged.length} channels in batches of ${BATCH}...`);

  for (let i = 0; i < tagged.length; i += BATCH) {
    const batch = tagged.slice(i, i + BATCH);
    try {
      await insertBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\r   Progress: ${inserted}/${tagged.length} (${Math.round(inserted/tagged.length*100)}%)`);
    } catch (e) {
      console.error(`\n   Batch ${i}-${i+BATCH} failed: ${e.message}`);
      failed += batch.length;
    }
  }

  console.log(`\n\n✅ Done! Inserted: ${inserted}, Failed: ${failed}`);
  console.log(`📺 Total private channels now includes ${inserted} iptv-org channels`);
}

main().catch(e => {
  console.error('❌ Fatal error:', e.message);
  process.exit(1);
});
