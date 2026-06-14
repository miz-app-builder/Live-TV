const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const https = require('https');
const http = require('http');
const { spawn } = require('child_process');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

// Prevent server crashes from unhandled errors (e.g. ECONNRESET from client disconnect)
process.on('uncaughtException', (err) => {
  if (err.code === 'ECONNRESET' || err.code === 'EPIPE' || err.code === 'ECONNABORTED') return;
  console.error('[uncaughtException]', err.message);
});
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason && reason.message ? reason.message : reason);
});

const supabaseAdmin = createClient(
  process.env.SUPABASE_URL || '',
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || '',
  { realtime: { transport: ws } }
);

let channels = [];
let guestBlockedChannels = new Set();
let privateChannels = [];

async function loadPrivateChannelsFromDB() {
  try {
    let all = [], from = 0;
    while (true) {
      const { data, error } = await supabaseAdmin.from('private_channels').select('*').order('id').range(from, from + 999);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all = all.concat(data);
      if (data.length < 1000) break;
      from += 1000;
    }
    privateChannels = all.map(ch => ({ ...ch, status: ch.status || 'Online' }));
    console.log(`Loaded ${privateChannels.length} private channels from DB`);
  } catch(e) { console.error('loadPrivateChannelsFromDB failed:', e.message); }
}

function rebuildBlockedSet() {
  guestBlockedChannels = new Set(channels.filter(ch => !ch.visible_to_guests).map(ch => ch.id));
}

async function loadChannelsFromDB() {
  try {
    let all = [];
    const PAGE = 1000;
    let from = 0;
    while (true) {
      const { data, error } = await supabaseAdmin.from('channels').select('*').order('id').range(from, from + PAGE - 1);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all = all.concat(data);
      if (data.length < PAGE) break;
      from += PAGE;
    }
    channels = all;
    rebuildBlockedSet();
    console.log(`Loaded ${channels.length} channels from DB`);
  } catch(e) { console.error('loadChannelsFromDB failed:', e.message); }
}

/* ── Stream Health Checker ───────────────────────────────── */
const HEALTH_BATCH     = 25;      // channels per batch
const HEALTH_DELAY_MS  = 1500;    // delay between batches
const HEALTH_TIMEOUT_MS= 5000;    // per-stream timeout
const HEALTH_INTERVAL  = 2 * 60 * 60 * 1000; // 2 hours
let   _healthRunning   = false;

function _checkStreamUrl(url) {
  return new Promise(resolve => {
    try {
      const mod = url.startsWith('https') ? https : http;
      const timer = setTimeout(() => { try { req.destroy(); } catch(_) {} resolve(false); }, HEALTH_TIMEOUT_MS);
      const req = mod.request(url, { method: 'GET', timeout: HEALTH_TIMEOUT_MS,
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', 'Range': 'bytes=0-511' } }, res => {
        clearTimeout(timer);
        if (res.statusCode < 200 || res.statusCode >= 400) { req.destroy(); resolve(false); return; }
        let buf = '';
        res.setEncoding('utf8');
        res.on('data', chunk => {
          buf += chunk;
          if (buf.length >= 64) { req.destroy(); }
        });
        res.on('close', () => {
          const trimmed = buf.trimStart();
          if (trimmed.startsWith('#EXTM3U') || trimmed.startsWith('#EXT-X')) {
            resolve(true);
          } else if (buf.length > 0 && !url.toLowerCase().includes('.m3u8')) {
            resolve(true);
          } else {
            resolve(false);
          }
        });
        res.on('error', () => resolve(false));
      });
      req.on('error', () => { clearTimeout(timer); resolve(false); });
      req.on('timeout', () => { clearTimeout(timer); req.destroy(); resolve(false); });
      req.end();
    } catch(_) { resolve(false); }
  });
}

function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function _runHealthCheckForList(arr, table) {
  const updates = [];
  for (let i = 0; i < arr.length; i += HEALTH_BATCH) {
    const batch = arr.slice(i, i + HEALTH_BATCH);
    const results = await Promise.all(batch.map(ch => _checkStreamUrl(ch.stream_url)));
    results.forEach((ok, idx) => {
      const ch = batch[idx];
      const newStatus = ok ? 'Online' : 'Offline';
      if (ch.status !== newStatus) updates.push({ id: ch.id, status: newStatus });
      ch.status = newStatus;
    });
    if (i + HEALTH_BATCH < arr.length) await _sleep(HEALTH_DELAY_MS);
  }
  if (updates.length) {
    const onIds  = updates.filter(u => u.status === 'Online').map(u => u.id);
    const offIds = updates.filter(u => u.status === 'Offline').map(u => u.id);
    try { if (onIds.length)  await supabaseAdmin.from(table).update({ status: 'Online'  }).in('id', onIds); } catch(_) {}
    try { if (offIds.length) await supabaseAdmin.from(table).update({ status: 'Offline' }).in('id', offIds); } catch(_) {}
    console.log('[Health]', table, '—', onIds.length, 'Online,', offIds.length, 'Offline updated');
  } else {
    console.log('[Health]', table, '— no status changes');
  }
}

async function runStreamHealthCheck() {
  if (_healthRunning) return;
  _healthRunning = true;
  console.log('[Health] Stream check started — channels:', channels.length, '| private:', privateChannels.length);
  await _runHealthCheckForList(channels, 'channels');
  await _runHealthCheckForList(privateChannels, 'private_channels');
  _healthRunning = false;
}

/* Start: first check 5 min after startup, then every 2 hours */
setTimeout(() => {
  runStreamHealthCheck();
  setInterval(runStreamHealthCheck, HEALTH_INTERVAL);
}, 5 * 60 * 1000);

let appConfig = { guest_limit_minutes: '5' };
const channelViews = new Map();
const activeUsers = new Map();
const ACTIVE_THRESHOLD_MS = 5 * 60 * 1000;
const channelViewers = new Map(); // chId → Map(viewerKey → lastSeenMs)
const VIEWER_TIMEOUT_MS = 60 * 1000;
const userCurrentChannel = new Map(); // userId → {chId, chName, startedAt, sessionId}
setInterval(() => {
  const now = Date.now();
  channelViewers.forEach((viewers, chId) => {
    viewers.forEach((ts, key) => { if (now - ts > VIEWER_TIMEOUT_MS) viewers.delete(key); });
    if (viewers.size === 0) channelViewers.delete(chId);
  });
}, 30000);

async function loadAppConfig() {
  try {
    const { data } = await supabaseAdmin.from('app_config').select('*');
    (data || []).forEach(r => { appConfig[r.key] = r.value; });
  } catch(_) {}
}

const CAT_RULES_SRV = [
  { key: 'Bangla', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga','ntv','dbc','ekushey','deshi','sangeet','atn','channel i','channel 9','channel 24','sa tv','btv','sangsad','independent tv','star news','kolkata tv','tv9 bangla','r bangla','enter10','jalsha','g-series','aakash aath','ananda tv'] },
  { key: 'News',   words: ['news','ndtv','republic','wion','cnn','bbc news','dw ','france 24','sky news','al-jazeera','aljazeera','india today','cgtn','times now','zee news','fox news','abc news','cnbc'] },
  { key: 'Movies', words: ['movie','cinema','film','bollywood','hollywood','goldmines','afriwood','artflix','biz cinema','classique','filmrise','moviesphere','grand cinema'] },
  { key: 'Music',  words: ['music','beats','9xm','joo music','8xm','dhoom music','atn music','yrfmusic'] },
  { key: 'Kids',   words: ['kids','cartoon','junior','motu','doraemon','pbs','zoo moo','tom &','jungle book','cbeebies'] },
  { key: 'Sports', words: ['sport','dd sport','cricket','football','soccer','tennis','basketball','wrestling','racing','olympic'] },
];
function categorizeChannel(name) {
  const n = name.toLowerCase();
  for (const rule of CAT_RULES_SRV) {
    if (rule.words.some(w => n.includes(w))) return rule.key;
  }
  return 'International';
}

const COUNTRY_RULES_SRV = [
  { code: 'BD', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga','dbc news','ekushey','sa tv','btv','sangsad','rtv ','my tv','banglavision','independent tv','channel i','atn bangla','desh tv','nagorik','news24 bd','channel 9','channel24','ntvbd','ntv bd','channel s'] },
  { code: 'IN', words: ['ndtv','zee tv','star plus','star gold','sony liv','colors tv','sun tv','vijay tv','asianet','gemini tv','etv','tv9 telugu','news18','republic tv','mirror now','times now','india today','dd national','dd news','lok sabha tv','rajya sabha','doordarshan','aaj tak','india tv','cnbctv18','abp news','news nation','india news','zee 24','colors marathi','star vijay','zee marathi','star suvarna','zee kannada','maa tv','star jalsha'] },
  { code: 'GB', words: ['bbc one','bbc two','bbc world','bbc news','sky news','sky sports','itv1','itv2','channel 4 uk','channel 5 uk','united kingdom','britain tv'] },
  { code: 'US', words: ['cnn','fox news','msnbc','abc news','nbc news','cbs news','espn ','hbo ','discovery us','nat geo','history channel','cartoon network','nickelodeon','disney channel','amc ','pbs ','bloomberg tv','cnbc ','usa network','fox sports','nfl network','nba tv'] },
  { code: 'PK', words: ['pakistan','geo tv','geo news','ary news','ary digital','hum tv','hum news','dunya tv','express news','aaj tv','bol news','92 news','24 news pk','dawn news','such tv','a sports pk','ten sports pk'] },
  { code: 'AE', words: ['dubai tv','abu dhabi tv','uae tv','al arabiya','sama dubai','al aan','sharjah tv'] },
  { code: 'SA', words: ['saudi tv','mbc ksa','rotana','al ekhbariya','iqraa','sbc tv','ain tv','ksa sports'] },
  { code: 'QA', words: ['qatar tv','al jazeera','aljazeera','bein sports','bein sport','bein connect'] },
  { code: 'TR', words: ['trt world','trt haber','turkish tv','ntv türk','cnn türk','show tv','kanal d','habertürk','tv8 türk','teve2','a haber'] },
  { code: 'FR', words: ['tf1','m6 fr','bfm tv','arte fr','canal+','france 24','france 2','france 3','lci fr','cnews fr','france tv'] },
  { code: 'DE', words: ['ard ','zdf ','rtl germany','pro7','dw tv','sat.1','kabel eins','n-tv','welt tv','phoenix tv'] },
  { code: 'RU', words: ['rt news','rossiya','ntv russia','russia today','ren tv','tvc russia','zvezda tv','russia 1','russia 24'] },
  { code: 'IR', words: ['irib','press tv','iran international','gem tv','manoto','varzesh iran','moein tv','iran tv'] },
  { code: 'EG', words: ['nile tv','cbc egypt','mbc masr','on tv','sada el balad','el mehwar','nile cinema'] },
  { code: 'AF', words: ['ariana tv','tolo tv','1tv afghanistan','shamshad tv','lemar tv','afghan tv'] },
  { code: 'NP', words: ['kantipur tv','image channel nepal','avenues tv','himalaya tv','news24 nepal','nepal tv','nepal1'] },
  { code: 'LK', words: ['derana','sirasa','siyatha','rupavahini','hiru tv','swarnawahini','tvone lk','shakthi tv'] },
  { code: 'MM', words: ['mrtv myanmar','skynet myanmar','mntv','myawaddy tv'] },
  { code: 'JP', words: ['nhk world','nhk japan','fuji tv','tv tokyo','tbs japan','tv asahi','j sports','nhk bs'] },
  { code: 'CN', words: ['cctv','cgtn','phoenix tv','dragon tv','iqiyi','youku','china tv'] },
  { code: 'KR', words: ['kbs world','mbc korea','sbs tv','jtbc','ytn news','arirang tv','tvn korea'] },
  { code: 'ID', words: ['rcti','sctv','indosiar','trans tv','metro tv','tvone','net tv indonesia','kompas tv'] },
  { code: 'MY', words: ['rtm malaysia','astro tv','tv3 malaysia','ntv7','8tv malaysia','bernama tv'] },
  { code: 'TH', words: ['pptv thailand','true4u','one31','gmm25','ch3 thailand','ch7 thailand'] },
  { code: 'IT', words: ['rai uno','rai due','rai tre','rai news','mediaset','canale 5','italia 1','rete 4','sky italia','la7'] },
  { code: 'ES', words: ['tve ','tve1','antena 3','telecinco','cuatro es','la sexta','rtve','tv3 spain'] },
  { code: 'NG', words: ['channels tv','arise tv','nta nigeria','silverbird tv','africa magic nigeria'] },
  { code: 'GH', words: ['ghone tv','joy news ghana','adom tv','tv3 ghana','e.tv ghana'] },
];
function detectCountry(name) {
  const n = name.toLowerCase();
  for (const rule of COUNTRY_RULES_SRV) {
    if (rule.words.some(w => n.includes(w))) return rule.code;
  }
  return '';
}

async function verifyUser(req) {
  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return null;
  try {
    const { data } = await supabaseAdmin.auth.getUser(token);
    return data?.user || null;
  } catch(_) { return null; }
}

async function getUserRole(userId) {
  try {
    const { data } = await supabaseAdmin
      .from('profiles')
      .select('role')
      .eq('id', userId)
      .single();
    return data?.role || 'member';
  } catch(_) { return 'member'; }
}

const LOGO_SVG = `<svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" style="flex-shrink:0"><rect width="32" height="32" rx="8" fill="#e00"/><rect x="4" y="8" width="24" height="16" rx="3" fill="#fff" fill-opacity="0.15"/><rect x="4" y="8" width="24" height="16" rx="3" stroke="#fff" stroke-opacity="0.4" stroke-width="1"/><polygon points="13,12 13,20 21,16" fill="#fff"/><rect x="11" y="25" width="4" height="3" rx="1" fill="#e00" fill-opacity="0.7"/><rect x="17" y="25" width="4" height="3" rx="1" fill="#e00" fill-opacity="0.7"/><rect x="9" y="27" width="14" height="1.5" rx="0.75" fill="#e00" fill-opacity="0.5"/></svg>`;
const LOGO_FULL_HTML = `${LOGO_SVG}<span style="font-size:18px;font-weight:700;letter-spacing:1px;color:#fff">MIZ <span style="color:#e00">Live</span> TV</span>`;

const SB_URL = process.env.SUPABASE_URL || '';
const SB_KEY = process.env.SUPABASE_ANON_KEY || '';

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());


// HLS Proxy — fetches external stream server-side, avoids browser CORS issues
function fetchUrl(url, extraHeaders = {}, _redirects = 0) {
  return new Promise((resolve, reject) => {
    if (_redirects > 5) return reject(new Error('Too many redirects'));
    const lib = url.startsWith('https') ? https : http;
    const options = { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', ...extraHeaders }, timeout: 15000 };
    const req = lib.get(url, options, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return fetchUrl(res.headers.location, extraHeaders, _redirects + 1).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', d => chunks.push(d));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    req.on('error', reject);
  });
}

// Rewrite manifest URLs to route through proxy
function _rewriteManifest(bodyStr, baseUrl) {
  const base = baseUrl.substring(0, baseUrl.lastIndexOf('/') + 1);
  let manifest = bodyStr;
  manifest = manifest.replace(/,?\s*CODECS="[^"]*"/gi, '');
  manifest = manifest.replace(/(URI=")([^"]+)(")/g, (_, open, uri, close) => {
    if (uri.startsWith('/proxy?url=')) return open + uri + close;
    const abs = uri.startsWith('http') ? uri : base + uri;
    return open + '/proxy?url=' + encodeURIComponent(abs) + close;
  });
  manifest = manifest.replace(/^(?!#)(.+)$/gm, (match) => {
    const t = match.trim();
    if (!t) return match;
    if (t.startsWith('/proxy?url=')) return match;
    if (t.startsWith('http')) return '/proxy?url=' + encodeURIComponent(t);
    return '/proxy?url=' + encodeURIComponent(base + t);
  });
  return manifest;
}

// Pipe upstream response directly to client (no buffering — low latency for segments)
function _pipeUpstream(url, extraHeaders, res, _redirects) {
  if (_redirects === undefined) _redirects = 0;
  return new Promise((resolve) => {
    if (_redirects > 5) {
      if (!res.headersSent) res.status(502).send('Too many redirects');
      return resolve();
    }
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', ...extraHeaders },
      timeout: 15000
    }, (upstream) => {
      if (upstream.statusCode >= 300 && upstream.statusCode < 400 && upstream.headers.location) {
        upstream.resume();
        return _pipeUpstream(upstream.headers.location, extraHeaders, res, _redirects + 1).then(resolve);
      }
      if (upstream.statusCode >= 400) {
        upstream.resume();
        if (!res.headersSent) res.status(502).send('Upstream error ' + upstream.statusCode);
        return resolve();
      }
      const ct = upstream.headers['content-type'] || '';
      // If server says it's a manifest despite no .m3u8 in URL — buffer and rewrite
      if (ct.includes('mpegurl') || ct.includes('x-mpegURL')) {
        const chunks = [];
        upstream.on('data', d => chunks.push(d));
        upstream.on('end', () => {
          const bodyStr = Buffer.concat(chunks).toString('utf8');
          const manifest = _rewriteManifest(bodyStr, url);
          res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
          res.setHeader('Cache-Control', 'no-cache');
          res.send(manifest);
          resolve();
        });
        upstream.on('error', () => { if (!res.headersSent) res.status(502).send('Proxy stream error'); resolve(); });
        return;
      }
      // Binary segment — pipe directly for lowest latency
      res.setHeader('Content-Type', ct || 'video/MP2T');
      res.setHeader('Cache-Control', 'no-cache');
      if (upstream.headers['content-length']) res.setHeader('Content-Length', upstream.headers['content-length']);
      // Prevent ECONNRESET crash when client disconnects mid-stream
      res.on('error', () => { try { upstream.destroy(); } catch(_) {} resolve(); });
      res.on('close', () => { try { upstream.destroy(); } catch(_) {} resolve(); });
      upstream.pipe(res);
      upstream.on('end', resolve);
      upstream.on('error', () => { if (!res.headersSent) { try { res.status(502).send('Proxy stream error'); } catch(_) {} } resolve(); });
    });
    req.on('timeout', () => { req.destroy(); if (!res.headersSent) res.status(502).send('Proxy timeout'); resolve(); });
    req.on('error', () => { if (!res.headersSent) res.status(502).send('Proxy error'); resolve(); });
  });
}

// Proxy route: /proxy?url=<encoded_url>
app.get('/proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).send('Missing url param');

  try {
    const decodedUrl = decodeURIComponent(targetUrl);
    let streamOrigin = '';
    try { const u = new URL(decodedUrl); streamOrigin = u.origin; } catch(_) {}
    const extraHeaders = streamOrigin
      ? { 'Referer': streamOrigin + '/', 'Origin': streamOrigin }
      : {};

    // Manifest (.m3u8): buffer fully so we can rewrite segment URLs
    const isManifestUrl = decodedUrl.toLowerCase().split('?')[0].includes('.m3u8');
    if (isManifestUrl) {
      const result = await fetchUrl(decodedUrl, extraHeaders);
      if (result.status >= 400) return res.status(502).send('Upstream error ' + result.status);
      const bodyStr = result.body.toString('utf8');
      const ct = result.headers['content-type'] || '';
      const trimmed = bodyStr.trimStart();
      const isHls = trimmed.startsWith('#EXTM3U') || trimmed.startsWith('#EXT-X') ||
                    ct.includes('mpegurl') || ct.includes('x-mpegURL');
      if (isHls) {
        const manifest = _rewriteManifest(bodyStr, decodedUrl);
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.setHeader('Cache-Control', 'no-cache');
        return res.send(manifest);
      }
      res.setHeader('Content-Type', ct || 'application/octet-stream');
      res.setHeader('Cache-Control', 'no-cache');
      return res.send(result.body);
    }

    // Segments & binary: pipe directly — no buffering, low latency
    await _pipeUpstream(decodedUrl, extraHeaders, res);

  } catch (err) {
    console.error('Proxy error:', err.message);
    if (!res.headersSent) res.status(502).send('Proxy fetch failed');
  }
});

app.use('/video', (req, res, next) => {
  const filePath = path.join(__dirname, 'video', req.path);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'Stream file not found' });
  next();
});

app.use('/video', express.static(path.join(__dirname, 'video'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.m3u8')) {
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    } else if (filePath.endsWith('.ts')) {
      res.setHeader('Content-Type', 'video/MP2T');
    }
  }
}));

app.use('/public', express.static(path.join(__dirname, 'public')));

app.get('/status', (req, res) => res.json({ status: 'live' }));

/* ── IPTV-org logo map ─────────────────────────────────── */
let logoMap = {}; // normalizedName → logoUrl

function normalizeName(name) {
  return name.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let raw = '';
      res.on('data', chunk => raw += chunk);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(e); } });
    }).on('error', reject);
  });
}

/* ── Domain guesser for Clearbit / Google Favicon ─────── */
const KNOWN_DOMAINS = {
  'aljazeera': 'aljazeera.com', 'bbc': 'bbc.com', 'cnn': 'cnn.com',
  'dw': 'dw.com', 'jamunatv': 'jamunatv.com', 'jamuna': 'jamunatv.com',
  'somoytv': 'somoynews.tv', 'somoy': 'somoynews.tv',
  'channel24': 'channel24bd.tv', 'ntvbd': 'ntvbd.com', 'ntv': 'ntvbd.com',
  'ekattortv': 'ekattortv.com', 'ekattor': 'ekattortv.com',
  'atnnews': 'atnnews.net', 'news24': 'news24bd.tv',
  'btv': 'btv.gov.bd', 'deeptotv': 'deeptotv.com', 'deepto': 'deeptotv.com',
  'maasrangatv': 'maasrangatv.com', 'maasranga': 'maasrangatv.com',
  'channelionline': 'channelionline.com', 'channeli': 'channelionline.com',
  'dbcnews': 'dbcnews.tv', 'channel9': 'channel9bd.com',
  'ekusheytelevision': 'ekusheytelevision.com', 'ekushey': 'ekusheytelevision.com',
  'satv': 'satv.com.bd', 'banglavision': 'banglavision.tv',
  'independenttv': 'independent.com.bd', 'boishakhi': 'boishakhitv.com',
  'ananda': 'anandastar.in', 'tv9bangla': 'tv9bangla.com',
  'rbangla': 'rbangla.in', 'sangeetbangla': 'sangeetbangla.com',
  'starnews': 'starnews.in', 'sangsad': 'sangsadtv.gov.bd',
  'deshitv': 'deshitv24.net', 'kolkatatv': 'kolkatatv.in',
};

function guessDomain(name) {
  const key = normalizeName(name);
  for (const [k, v] of Object.entries(KNOWN_DOMAINS)) {
    if (key.includes(normalizeName(k))) return v;
  }
  const domain = name.toLowerCase()
    .replace(/\b(tv|news|channel|hd|bangla|bd|uk|live|the)\b/gi, '')
    .replace(/[^a-z0-9]/g, '').trim();
  return domain ? domain + '.com' : null;
}

async function fetchLogoMap() {
  try {
    const [channelsData, logosData] = await Promise.all([
      fetchJson('https://iptv-org.github.io/api/channels.json'),
      fetchJson('https://iptv-org.github.io/api/logos.json')
    ]);

    // Source 2: channels.json logo field (lower priority base)
    channelsData.forEach(ch => {
      if (ch.logo && ch.name) {
        const key = normalizeName(ch.name);
        if (!logoMap[key]) logoMap[key] = ch.logo;
        (ch.alt_names || []).forEach(a => {
          const aKey = normalizeName(a);
          if (!logoMap[aKey]) logoMap[aKey] = ch.logo;
        });
      }
    });

    // Build id → {name, alt_names} map
    const idToNames = {};
    channelsData.forEach(ch => {
      if (ch.id && ch.name) idToNames[ch.id] = { name: ch.name, alt_names: ch.alt_names || [] };
    });

    // Source 1: logos.json (higher priority — overwrites channels.json)
    logosData.forEach(entry => {
      if (!entry.channel || !entry.url || !entry.in_use) return;
      const info = idToNames[entry.channel];
      if (!info) return;
      logoMap[normalizeName(info.name)] = entry.url;
      info.alt_names.forEach(a => { logoMap[normalizeName(a)] = entry.url; });
    });

    console.log('Logo map loaded (src 1+2):', Object.keys(logoMap).length, 'entries');
  } catch(e) {
    console.log('Logo map fetch failed:', e.message);
  }
}

/* ── Source 5: SportsDB background fetch (non-blocking) ─ */
async function fetchSportsDBLogos() {
  const missing = channels.filter(ch => !logoMap[normalizeName(ch.channel_name)]);
  let found = 0;
  for (const ch of missing) {
    try {
      const data = await fetchJson(
        `https://www.thesportsdb.com/api/v1/json/3/search_tv.php?t=${encodeURIComponent(ch.channel_name)}`
      );
      if (data && data.channels && data.channels[0]) {
        const logo = data.channels[0].strLogo || data.channels[0].strFanart;
        if (logo) { logoMap[normalizeName(ch.channel_name)] = logo; found++; }
      }
    } catch(_) {}
    await new Promise(r => setTimeout(r, 150));
  }
  if (found) console.log('SportsDB logos added:', found);
}

fetchLogoMap().then(() => fetchSportsDBLogos());
loadChannelsFromDB();
loadPrivateChannelsFromDB();
loadAppConfig();

async function ensurePrivatePinColumn() {
  try {
    const ref = (process.env.SUPABASE_URL || '').match(/https?:\/\/([^.]+)/)?.[1];
    const token = process.env.SUPABASE_ACCESS_TOKEN;
    if (!ref || !token) return;
    await fetch(`https://api.supabase.com/v1/projects/${ref}/database/query`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: `ALTER TABLE public.profiles ADD COLUMN IF NOT EXISTS private_pin TEXT DEFAULT NULL;` })
    });
    console.log('private_pin column ensured');
  } catch(e) { console.error('ensurePrivatePinColumn:', e.message); }
}
ensurePrivatePinColumn();

async function ensureActivityTables() {
  try {
    const ref = (process.env.SUPABASE_URL || '').match(/https?:\/\/([^.]+)/)?.[1];
    const tok = process.env.SUPABASE_ACCESS_TOKEN;
    if (!ref || !tok) return;
    const sql = `
      CREATE TABLE IF NOT EXISTS public.user_login_logs (
        id BIGSERIAL PRIMARY KEY,
        user_id UUID NOT NULL,
        logged_in_at TIMESTAMPTZ DEFAULT NOW(),
        ip TEXT
      );
      CREATE TABLE IF NOT EXISTS public.user_watch_sessions (
        id BIGSERIAL PRIMARY KEY,
        user_id UUID NOT NULL,
        channel_id INTEGER NOT NULL,
        channel_name TEXT,
        started_at TIMESTAMPTZ DEFAULT NOW(),
        ended_at TIMESTAMPTZ,
        duration_seconds INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_login_logs_user ON public.user_login_logs(user_id, logged_in_at DESC);
      CREATE INDEX IF NOT EXISTS idx_watch_sessions_user ON public.user_watch_sessions(user_id, started_at DESC);
    `;
    await fetch(`https://api.supabase.com/v1/projects/${ref}/database/query`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${tok}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: sql })
    });
    console.log('Activity tables ensured');
  } catch(e) { console.error('ensureActivityTables:', e.message); }
}
ensureActivityTables();

/* ── Logo fallback API (SportsDB on-demand) ───────────── */
app.get('/api/logo-fallback', async (req, res) => {
  const name = (req.query.name || '').trim();
  if (!name) return res.json({ logo: null });
  const cached = logoMap[normalizeName(name)];
  if (cached) return res.json({ logo: cached });
  try {
    const data = await fetchJson(
      `https://www.thesportsdb.com/api/v1/json/3/search_tv.php?t=${encodeURIComponent(name)}`
    );
    if (data && data.channels && data.channels[0]) {
      const logo = data.channels[0].strLogo || data.channels[0].strFanart || null;
      if (logo) logoMap[normalizeName(name)] = logo;
      return res.json({ logo: logo || null });
    }
  } catch(_) {}
  res.json({ logo: null });
});

app.get('/channels', async (req, res) => {
  const user = await verifyUser(req);
  if (user) {
    activeUsers.set(user.id, Date.now());
  } else {
    activeUsers.set('g_' + (req.headers['x-forwarded-for'] || req.ip || 'unknown'), Date.now());
  }
  const withLogos = channels.map(ch => ({
    ...ch,
    logo: logoMap[normalizeName(ch.channel_name)] || null,
    domain: guessDomain(ch.channel_name),
    country: ch.country || detectCountry(ch.channel_name) || ''
  }));
  const filtered = user
    ? withLogos
    : withLogos.filter(ch => !guestBlockedChannels.has(ch.id));
  const groupMap = new Map();
  filtered.forEach(ch => {
    const key = ch.channel_name.toLowerCase().trim();
    if (!groupMap.has(key)) {
      groupMap.set(key, { ...ch, servers: [{ id: ch.id, url: ch.stream_url }] });
    } else {
      groupMap.get(key).servers.push({ id: ch.id, url: ch.stream_url });
    }
  });
  res.json({ channels: Array.from(groupMap.values()), isGuest: !user });
});

/* ── Auth & Admin API ───────────────────────────────────── */
app.get('/api/auth/me', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.json({ user: null });
  const role = await getUserRole(user.id);
  res.json({ user: { id: user.id, email: user.email, role } });
});

app.get('/api/admin/channels', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  res.json({ channels: channels.map(ch => ({ id: ch.id, name: ch.channel_name, stream_url: ch.stream_url, category: ch.category, status: ch.status, blocked: !ch.visible_to_guests, is_highlighted: !!ch.is_highlighted })) });
});

/* ── Admin: Add new channel — MUST be before /:id ──────── */
app.post('/api/admin/channels/new', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { channel_name, stream_url, category, country } = req.body;
  if (!channel_name || !stream_url) return res.status(400).json({ error: 'channel_name and stream_url required' });
  const maxId = channels.length > 0 ? Math.max(...channels.map(c => c.id)) : 0;
  const newId = maxId + 1;
  const newChannel = {
    id: newId, channel_name: channel_name.trim(), stream_url: stream_url.trim(),
    category: category || categorizeChannel(channel_name),
    country: country || detectCountry(channel_name) || null,
    status: 'Online', visible_to_guests: true, is_highlighted: false,
    created_at: new Date().toISOString(), updated_at: new Date().toISOString()
  };
  const { error } = await supabaseAdmin.from('channels').insert([newChannel]);
  if (error) return res.status(500).json({ error: error.message });
  channels.push(newChannel);
  rebuildBlockedSet();
  res.json({ success: true, channel: newChannel });
});

/* ── Admin: Bulk block/unblock — MUST be before /:id ───── */
app.post('/api/admin/channels/bulk', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { action, category } = req.body;
  const visible = action !== 'block';
  let targetIds = channels.map(c => c.id);
  if (category && category !== 'All') {
    targetIds = channels.filter(ch => ch.category === category).map(c => c.id);
  }
  try {
    const { error } = await supabaseAdmin.from('channels').update({ visible_to_guests: visible, updated_at: new Date().toISOString() }).in('id', targetIds);
    if (error) throw error;
    targetIds.forEach(id => {
      const ch = channels.find(c => c.id === id);
      if (ch) ch.visible_to_guests = visible;
    });
    rebuildBlockedSet();
    res.json({ success: true, affected: targetIds.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/channels/:id', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const channelId = parseInt(req.params.id);
  const { blocked } = req.body;
  const { error } = await supabaseAdmin.from('channels').update({ visible_to_guests: !blocked, updated_at: new Date().toISOString() }).eq('id', channelId);
  if (error) return res.status(500).json({ error: error.message });
  const ch = channels.find(c => c.id === channelId);
  if (ch) { ch.visible_to_guests = !blocked; rebuildBlockedSet(); }
  res.json({ success: true });
});

/* ── Admin: Highlight toggle ────────────────────────────── */
app.post('/api/admin/channels/:id/highlight', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const channelId = parseInt(req.params.id);
  const { highlighted } = req.body;
  const { error } = await supabaseAdmin.from('channels').update({ is_highlighted: !!highlighted, updated_at: new Date().toISOString() }).eq('id', channelId);
  if (error) return res.status(500).json({ error: error.message });
  const ch = channels.find(c => c.id === channelId);
  if (ch) ch.is_highlighted = !!highlighted;
  res.json({ success: true });
});

/* ── View tracking ──────────────────────────────────────── */
app.post('/api/track/view', (req, res) => {
  const { ch } = req.body;
  if (ch && typeof ch === 'number') channelViews.set(ch, (channelViews.get(ch) || 0) + 1);
  res.json({ ok: true });
});

/* ── Login tracking ─────────────────────────────────────── */
app.post('/api/track/login', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.json({ ok: false });
  try {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || null;
    await supabaseAdmin.from('user_login_logs').insert({ user_id: user.id, ip });
  } catch(_) {}
  res.json({ ok: true });
});

/* ── Viewer heartbeat (auth or guest by IP) ─────────────── */
app.post('/api/track/heartbeat', async (req, res) => {
  const { ch, chName } = req.body;
  if (!ch || typeof ch !== 'number') return res.json({ ok: false });
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const authUser = await verifyUser(req);
  const viewerKey = authUser ? 'u:' + authUser.id : 'ip:' + ip;

  if (!channelViewers.has(ch)) channelViewers.set(ch, new Map());
  channelViewers.get(ch).set(viewerKey, Date.now());

  if (authUser) {
    activeUsers.set(authUser.id, Date.now());
    const prev = userCurrentChannel.get(authUser.id);
    const channelDisplayName = chName || channels.find(c => c.id === ch)?.channel_name || ('Channel ' + ch);
    if (!prev || prev.chId !== ch) {
      if (prev && prev.sessionId) {
        const dur = Math.round((Date.now() - prev.startedAt) / 1000);
        supabaseAdmin.from('user_watch_sessions').update({ ended_at: new Date().toISOString(), duration_seconds: dur }).eq('id', prev.sessionId).then(()=>{}).catch(()=>{});
      }
      try {
        const { data } = await supabaseAdmin.from('user_watch_sessions').insert({ user_id: authUser.id, channel_id: ch, channel_name: channelDisplayName }).select('id').single();
        userCurrentChannel.set(authUser.id, { chId: ch, chName: channelDisplayName, startedAt: Date.now(), sessionId: data?.id || null });
      } catch(_) {
        userCurrentChannel.set(authUser.id, { chId: ch, chName: channelDisplayName, startedAt: Date.now(), sessionId: null });
      }
    }
  }
  res.json({ ok: true });
});

/* ── Presence ping (logged-in users browsing, no channel) ── */
app.post('/api/track/presence', async (req, res) => {
  const authUser = await verifyUser(req);
  if (authUser) {
    activeUsers.set(authUser.id, Date.now());
  } else {
    const guestKey = 'g_' + (req.headers['x-forwarded-for'] || req.ip || 'unknown');
    activeUsers.set(guestKey, Date.now());
  }
  res.json({ ok: true });
});

/* ── Admin: live viewer count for a channel ─────────────── */
app.get('/api/admin/viewers/:chId', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const chId = parseInt(req.params.chId);
  if (isNaN(chId)) return res.status(400).json({ error: 'Invalid channel id' });
  const viewers = channelViewers.get(chId);
  const now = Date.now();
  let count = 0;
  if (viewers) viewers.forEach((ts) => { if (now - ts <= VIEWER_TIMEOUT_MS) count++; });
  res.json({ count });
});

/* ── Public config ──────────────────────────────────────── */
app.get('/api/config/guest-limit', (req, res) => {
  res.json({ minutes: parseInt(appConfig.guest_limit_minutes) || 5 });
});

/* ── Admin: Stats ───────────────────────────────────────── */
app.get('/api/admin/stats', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  try {
    const { data: { users } } = await supabaseAdmin.auth.admin.listUsers({ perPage: 1000 });
    const total = users.length;
    const today = new Date(); today.setHours(0,0,0,0);
    const todaySignups = users.filter(u => new Date(u.created_at) >= today).length;
    const now = Date.now();
    activeUsers.forEach((t, id) => { if (now - t > ACTIVE_THRESHOLD_MS) activeUsers.delete(id); });
    const activeCount = activeUsers.size;
    const topChannels = Array.from(channelViews.entries())
      .sort((a, b) => b[1] - a[1]).slice(0, 10)
      .map(([id, views]) => { const ch = channels.find(c => c.id === id); return { id, name: ch?.channel_name || 'Unknown', views }; });
    const blockedCount = channels.filter(ch => !ch.visible_to_guests).length;
    res.json({ total, todaySignups, activeCount, topChannels, blockedChannels: blockedCount, totalChannels: channels.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Admin: Users ───────────────────────────────────────── */
app.get('/api/admin/users', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  try {
    const { data: { users } } = await supabaseAdmin.auth.admin.listUsers({ perPage: 1000 });
    const { data: profiles } = await supabaseAdmin.from('profiles').select('id, role');
    const profileMap = {};
    (profiles || []).forEach(p => { profileMap[p.id] = p.role; });
    const now = Date.now();
    activeUsers.forEach((t, id) => { if (now - t > ACTIVE_THRESHOLD_MS) activeUsers.delete(id); });
    const list = users.map(u => {
      const isOnline = activeUsers.has(u.id);
      const watching = userCurrentChannel.get(u.id);
      return {
        id: u.id, email: u.email, role: profileMap[u.id] || 'member',
        created_at: u.created_at,
        banned: !!(u.banned_until && new Date(u.banned_until) > new Date()),
        is_online: isOnline,
        last_seen: activeUsers.get(u.id) || null,
        watching_channel: (isOnline && watching) ? { id: watching.chId, name: watching.chName } : null,
      };
    });
    res.json({ users: list });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Admin: User activity history ───────────────────────── */
app.get('/api/admin/users/:id/activity', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const uid = req.params.id;
  try {
    const [{ data: logins }, { data: sessions }] = await Promise.all([
      supabaseAdmin.from('user_login_logs').select('id,logged_in_at,ip').eq('user_id', uid).order('logged_in_at', { ascending: false }).limit(200),
      supabaseAdmin.from('user_watch_sessions').select('id,channel_id,channel_name,started_at,ended_at,duration_seconds').eq('user_id', uid).order('started_at', { ascending: false }).limit(500),
    ]);
    const events = [];
    (logins || []).forEach(l => events.push({ type: 'login', at: l.logged_in_at, ip: l.ip }));
    (sessions || []).forEach(s => events.push({ type: 'watch', at: s.started_at, channel_id: s.channel_id, channel_name: s.channel_name, ended_at: s.ended_at, duration_seconds: s.duration_seconds }));
    events.sort((a, b) => new Date(b.at) - new Date(a.at));
    res.json({ activity: events });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users/:id/role', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  if (user.id === req.params.id) return res.status(400).json({ error: 'Cannot change own role' });
  const { role: newRole } = req.body;
  if (!['admin', 'member'].includes(newRole)) return res.status(400).json({ error: 'Invalid role' });
  try {
    await supabaseAdmin.from('profiles').upsert({ id: req.params.id, role: newRole });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users/:id/ban', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  if (user.id === req.params.id) return res.status(400).json({ error: 'Cannot ban yourself' });
  const { ban } = req.body;
  try {
    await supabaseAdmin.auth.admin.updateUserById(req.params.id, { ban_duration: ban ? '876000h' : 'none' });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Admin: Channel URL update (legacy route kept for compat) ── */
app.post('/api/admin/channels/:id/url', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const channelId = parseInt(req.params.id);
  const { stream_url } = req.body;
  if (!stream_url) return res.status(400).json({ error: 'stream_url required' });
  const ch = channels.find(c => c.id === channelId);
  if (!ch) return res.status(404).json({ error: 'Channel not found' });
  const { error } = await supabaseAdmin.from('channels').update({ stream_url, updated_at: new Date().toISOString() }).eq('id', channelId);
  if (error) return res.status(500).json({ error: error.message });
  ch.stream_url = stream_url;
  res.json({ success: true });
});

/* ── Admin: Full channel update (PUT) ───────────────────── */
app.put('/api/admin/channels/:id', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const channelId = parseInt(req.params.id);
  const { channel_name, stream_url, category, country, status, visible_to_guests } = req.body;
  const updates = { updated_at: new Date().toISOString() };
  if (channel_name !== undefined) updates.channel_name = channel_name.trim();
  if (stream_url !== undefined) updates.stream_url = stream_url.trim();
  if (category !== undefined) updates.category = category;
  if (country !== undefined) updates.country = country || null;
  if (status !== undefined) updates.status = status;
  if (visible_to_guests !== undefined) updates.visible_to_guests = visible_to_guests;
  const { error } = await supabaseAdmin.from('channels').update(updates).eq('id', channelId);
  if (error) return res.status(500).json({ error: error.message });
  const ch = channels.find(c => c.id === channelId);
  if (ch) { Object.assign(ch, updates); rebuildBlockedSet(); }
  res.json({ success: true });
});

/* ── Admin: Delete channel ──────────────────────────────── */
app.delete('/api/admin/channels/:id', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const channelId = parseInt(req.params.id);
  const { error } = await supabaseAdmin.from('channels').delete().eq('id', channelId);
  if (error) return res.status(500).json({ error: error.message });
  const idx = channels.findIndex(c => c.id === channelId);
  if (idx >= 0) { channels.splice(idx, 1); rebuildBlockedSet(); }
  res.json({ success: true });
});

/* ── Admin: App config ──────────────────────────────────── */
app.post('/api/admin/config', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  const role = await getUserRole(user.id);
  if (role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { key, value } = req.body;
  if (!key) return res.status(400).json({ error: 'key required' });
  appConfig[key] = String(value);
  try {
    await supabaseAdmin.from('app_config').upsert({ key, value: String(value), updated_at: new Date().toISOString() });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── FFmpeg Transcoding ────────────────────────────────── */
const transcodeSessions = new Map(); // sessionId → { proc, dir, timer }

const QUALITY_PRESETS = {
  '1080': { height: 1080, vb: '4000k', ab: '192k' },
  '720':  { height: 720,  vb: '2500k', ab: '128k' },
  '480':  { height: 480,  vb: '1200k', ab: '96k'  },
  '360':  { height: 360,  vb: '600k',  ab: '64k'  },
  '240':  { height: 240,  vb: '300k',  ab: '48k'  },
};

function killSession(sessionId) {
  const s = transcodeSessions.get(sessionId);
  if (!s) return;
  clearTimeout(s.timer);
  try { s.proc.kill('SIGKILL'); } catch(_) {}
  try { fs.rmSync(s.dir, { recursive: true, force: true }); } catch(_) {}
  transcodeSessions.delete(sessionId);
}

app.get('/transcode/start', async (req, res) => {
  const { url, height } = req.query;
  if (!url || !QUALITY_PRESETS[height]) return res.status(400).json({ error: 'Bad params' });

  const preset  = QUALITY_PRESETS[height];
  const sid     = crypto.randomBytes(8).toString('hex');
  const dir     = path.join('/tmp', 'tc_' + sid);
  const outFile = path.join(dir, 'index.m3u8');
  const proxyUrl = 'http://localhost:' + PORT + '/proxy?url=' + encodeURIComponent(url);

  fs.mkdirSync(dir, { recursive: true });

  const args = [
    '-y',
    '-re',
    '-i', proxyUrl,
    '-c:v', 'libx264',
    '-preset', 'ultrafast',
    '-tune', 'zerolatency',
    '-vf', 'scale=-2:' + preset.height,
    '-b:v', preset.vb,
    '-maxrate', preset.vb,
    '-bufsize', '2M',
    '-c:a', 'aac',
    '-b:a', preset.ab,
    '-f', 'hls',
    '-hls_time', '2',
    '-hls_list_size', '5',
    '-hls_flags', 'delete_segments+append_list',
    '-hls_segment_filename', path.join(dir, 'seg%05d.ts'),
    outFile
  ];

  const proc = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });

  // Auto-kill after 30 min
  const timer = setTimeout(() => killSession(sid), 30 * 60 * 1000);
  transcodeSessions.set(sid, { proc, dir, timer });

  proc.on('exit', () => { killSession(sid); });

  // Wait for first playlist to appear (up to 15s)
  let waited = 0;
  const check = setInterval(() => {
    waited += 300;
    if (fs.existsSync(outFile)) {
      clearInterval(check);
      return res.json({ sessionId: sid });
    }
    if (waited >= 15000) {
      clearInterval(check);
      killSession(sid);
      return res.status(504).json({ error: 'Transcode timeout' });
    }
  }, 300);
});

app.get('/transcode/stop/:sid', (req, res) => {
  killSession(req.params.sid);
  res.json({ ok: true });
});

app.get('/transcode/:sid/:file', (req, res) => {
  const { sid, file } = req.params;
  const s = transcodeSessions.get(sid);
  if (!s) return res.status(404).send('Session not found');

  // Refresh auto-kill timer on activity
  clearTimeout(s.timer);
  s.timer = setTimeout(() => killSession(sid), 30 * 60 * 1000);

  const filePath = path.join(s.dir, file);
  if (!fs.existsSync(filePath)) return res.status(404).send('Not ready');

  if (file.endsWith('.m3u8')) {
    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache');
  } else {
    res.setHeader('Content-Type', 'video/MP2T');
  }
  res.sendFile(filePath);
});

const SHARED_HEAD_STYLES = `
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0a0a0a; color: #fff;
      font-family: 'Segoe UI', Arial, sans-serif;
      min-height: 100vh; display: flex; flex-direction: column;
      align-items: center; padding: 16px 12px 60px;
    }
    header {
      width: 100%; max-width: 960px;
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    header h1 { font-size: 18px; font-weight: 700; letter-spacing: 1px; display:flex; align-items:center; gap:8px; }
    .open-btn {
      background: #222; color: #aaa; border: 1px solid #333;
      border-radius: 6px; padding: 6px 12px; font-size: 12px;
      cursor: pointer; text-decoration: none;
      transition: background 0.2s, color 0.2s;
    }
    .open-btn:hover { background: #e00; color: #fff; border-color: #e00; }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.3;transform:scale(.6)} }
    @keyframes spin { to { transform: rotate(360deg); } }
`;

/* ── Auth Pages ─────────────────────────────────────────── */
const AUTH_PAGE_STYLE = `
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0a0a0a; color: #fff; font-family: 'Segoe UI', Arial, sans-serif;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center;
    justify-content: center; padding: 16px; }
  .logo { display: flex; align-items: center; gap: 10px; margin-bottom: 32px; cursor: pointer; }
  .logo span { font-size: 20px; font-weight: 700; letter-spacing: .5px; }
  .card { background: #141414; border: 1px solid #222; border-radius: 14px;
    padding: 32px; width: 100%; max-width: 400px; }
  h2 { font-size: 20px; font-weight: 700; margin-bottom: 24px; text-align: center; }
  label { display: block; font-size: 12px; color: #888; margin-bottom: 6px; font-weight: 600; letter-spacing: .5px; }
  input[type=email], input[type=password], input[type=text] {
    width: 100%; background: #0a0a0a; border: 1px solid #2a2a2a;
    border-radius: 8px; padding: 12px 14px; color: #ddd;
    font-size: 14px; margin-bottom: 16px; outline: none; transition: border-color .2s; }
  input:focus { border-color: #e00; }
  .btn { width: 100%; background: #e00; color: #fff; border: none;
    border-radius: 8px; padding: 13px; font-size: 15px; font-weight: 700;
    cursor: pointer; transition: background .15s; margin-top: 4px; }
  .btn:hover { background: #c00; }
  .btn:disabled { background: #600; cursor: not-allowed; }
  .switch { text-align: center; margin-top: 20px; font-size: 13px; color: #666; }
  .switch a { color: #e00; text-decoration: none; }
  .switch a:hover { text-decoration: underline; }
  .msg { font-size: 13px; text-align: center; margin-top: 14px; min-height: 20px; }
  .msg.err { color: #f66; } .msg.ok { color: #4c4; }
`;

const AUTH_SHARED_JS = (sbUrl, sbKey) => `
  const _sb = supabase.createClient('${sbUrl}', '${sbKey}');
  function getToken() { return localStorage.getItem('miz_token'); }
`;

app.get('/login', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" /><meta name="viewport" content="width=device-width,initial-scale=1.0" />
  <title>Login — MIZ Live TV</title>
  <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.min.js"></script>
  <style>${AUTH_PAGE_STYLE}</style>
</head>
<body>
  <div class="logo" onclick="location.href='/'">
    ${LOGO_FULL_HTML}
  </div>
  <div class="card">
    <h2>🔑 Login</h2>
    <label>EMAIL</label>
    <input type="email" id="email" placeholder="your@email.com" autocomplete="email" />
    <label>PASSWORD</label>
    <input type="password" id="pass" placeholder="••••••••" autocomplete="current-password" />
    <button class="btn" id="btn">Login</button>
    <div class="msg" id="msg"></div>
    <div class="switch">Account নেই? <a href="/signup">Sign Up করুন</a></div>
  </div>
  <script>
    ${AUTH_SHARED_JS(SB_URL, SB_KEY)}
    const btn = document.getElementById('btn');
    const msg = document.getElementById('msg');
    btn.addEventListener('click', async () => {
      const email = document.getElementById('email').value.trim();
      const pass = document.getElementById('pass').value;
      if (!email || !pass) { msg.className='msg err'; msg.textContent='Email ও Password দিন।'; return; }
      btn.disabled = true; btn.textContent = 'Loading...';
      msg.className='msg'; msg.textContent='';
      const { data, error } = await _sb.auth.signInWithPassword({ email, password: pass });
      if (error) { msg.className='msg err'; msg.textContent=error.message; btn.disabled=false; btn.textContent='Login'; return; }
      localStorage.setItem('miz_token', data.session.access_token);
      localStorage.setItem('miz_refresh', data.session.refresh_token);
      localStorage.removeItem('miz_guest_time');
      fetch('/api/track/login', { method: 'POST', headers: { Authorization: 'Bearer ' + data.session.access_token } }).catch(()=>{});
      msg.className='msg ok'; msg.textContent='✅ Login সফল! Redirect হচ্ছে...';
      setTimeout(() => window.location.href = '/', 700);
    });
    document.addEventListener('keydown', e => { if (e.key==='Enter') btn.click(); });
    if (getToken()) window.location.href = '/';
  </script>
</body></html>`);
});

app.get('/signup', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" /><meta name="viewport" content="width=device-width,initial-scale=1.0" />
  <title>Sign Up — MIZ Live TV</title>
  <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.min.js"></script>
  <style>${AUTH_PAGE_STYLE}</style>
</head>
<body>
  <div class="logo" onclick="location.href='/'">
    ${LOGO_FULL_HTML}
  </div>
  <div class="card">
    <h2>📝 Sign Up</h2>
    <label>EMAIL</label>
    <input type="email" id="email" placeholder="your@email.com" autocomplete="email" />
    <label>PASSWORD</label>
    <input type="password" id="pass" placeholder="কমপক্ষে ৬ অক্ষর" autocomplete="new-password" />
    <label>CONFIRM PASSWORD</label>
    <input type="password" id="pass2" placeholder="আবার টাইপ করুন" autocomplete="new-password" />
    <button class="btn" id="btn">Sign Up</button>
    <div class="msg" id="msg"></div>
    <div class="switch">Account আছে? <a href="/login">Login করুন</a></div>
  </div>
  <script>
    ${AUTH_SHARED_JS(SB_URL, SB_KEY)}
    const btn = document.getElementById('btn');
    const msg = document.getElementById('msg');
    btn.addEventListener('click', async () => {
      const email = document.getElementById('email').value.trim();
      const pass = document.getElementById('pass').value;
      const pass2 = document.getElementById('pass2').value;
      if (!email || !pass) { msg.className='msg err'; msg.textContent='সব field পূরণ করুন।'; return; }
      if (pass.length < 6) { msg.className='msg err'; msg.textContent='Password কমপক্ষে ৬ অক্ষর হতে হবে।'; return; }
      if (pass !== pass2) { msg.className='msg err'; msg.textContent='Password মিলছে না।'; return; }
      btn.disabled=true; btn.textContent='Creating account...';
      msg.className='msg'; msg.textContent='';
      const { data, error } = await _sb.auth.signUp({ email, password: pass });
      if (error) { msg.className='msg err'; msg.textContent=error.message; btn.disabled=false; btn.textContent='Sign Up'; return; }
      msg.className='msg ok'; msg.textContent='✅ Account তৈরি হয়েছে! Email confirm করুন তারপর Login করুন।';
      btn.textContent='Done!';
      setTimeout(() => window.location.href = '/login', 3000);
    });
    if (getToken()) window.location.href = '/';
  </script>
</body></html>`);
});

/* ── Admin: Private Channels ─────────────────────────────── */
app.get('/api/admin/private-channels', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  try {
    let allPc = [], pcFrom = 0;
    while(true) {
      const { data, error } = await supabaseAdmin.from('private_channels').select('*').order('name', { ascending: true }).range(pcFrom, pcFrom + 999);
      if (error) throw error;
      if (!data || !data.length) break;
      allPc = allPc.concat(data);
      pcFrom += 1000;
      if (data.length < 1000) break;
    }
    res.json({ channels: allPc });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/private-channels', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { name, stream_url, category, country, description } = req.body;
  if (!name || !stream_url) return res.status(400).json({ error: 'name and stream_url required' });
  try {
    const { data, error } = await supabaseAdmin.from('private_channels').insert([{
      name: name.trim(), stream_url: stream_url.trim(),
      category: category || 'Private', description: description || '',
      country: country || detectCountry(name) || null
    }]).select().single();
    if (error) throw error;
    res.json({ success: true, channel: data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/admin/private-channels/:id', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const id = parseInt(req.params.id);
  const { name, stream_url, category, country, description } = req.body;
  const updates = { updated_at: new Date().toISOString() };
  if (name !== undefined) updates.name = name.trim();
  if (stream_url !== undefined) updates.stream_url = stream_url.trim();
  if (category !== undefined) updates.category = category;
  if (country !== undefined) updates.country = country || null;
  if (description !== undefined) updates.description = description;
  try {
    const { error } = await supabaseAdmin.from('private_channels').update(updates).eq('id', id);
    if (error) throw error;
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/admin/private-channels/:id', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const id = parseInt(req.params.id);
  try {
    const { error } = await supabaseAdmin.from('private_channels').delete().eq('id', id);
    if (error) throw error;
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/private-channels/:id/make-public', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const id = parseInt(req.params.id);
  try {
    const { data: ch, error: fetchErr } = await supabaseAdmin.from('private_channels').select('*').eq('id', id).single();
    if (fetchErr) throw fetchErr;
    const maxId = channels.length > 0 ? Math.max(...channels.map(c => c.id)) : 0;
    const newId = maxId + 1;
    const newChannel = {
      id: newId, channel_name: ch.name, stream_url: ch.stream_url,
      category: ch.category === 'Private' || ch.category === 'VIP' || ch.category === 'Premium'
        ? categorizeChannel(ch.name) : ch.category,
      status: 'Online', visible_to_guests: true,
      created_at: new Date().toISOString(), updated_at: new Date().toISOString()
    };
    const { error: insertErr } = await supabaseAdmin.from('channels').insert([newChannel]);
    if (insertErr) throw insertErr;
    channels.push(newChannel);
    rebuildBlockedSet();
    res.json({ success: true, publicId: newId });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/private-channels/:id/access', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const id = parseInt(req.params.id);
  try {
    const { data: ch } = await supabaseAdmin.from('private_channels').select('category').eq('id', id).single();
    const [{ data: userAccess }, { data: catAccess }] = await Promise.all([
      supabaseAdmin.from('private_channel_user_access').select('id, user_id, created_at').eq('channel_id', id),
      supabaseAdmin.from('private_channel_category_access').select('id, user_id, created_at').eq('category', ch?.category || 'Private')
    ]);
    const { data: { users } } = await supabaseAdmin.auth.admin.listUsers({ perPage: 1000 });
    const emailMap = {};
    users.forEach(u => { emailMap[u.id] = u.email; });
    res.json({
      channelAccess: (userAccess || []).map(a => ({ ...a, email: emailMap[a.user_id] || a.user_id })),
      categoryAccess: (catAccess || []).map(a => ({ ...a, email: emailMap[a.user_id] || a.user_id })),
      category: ch?.category || 'Private'
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/private-channels/access', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { user_id, channel_id, category, type } = req.body;
  if (!user_id) return res.status(400).json({ error: 'user_id required' });
  try {
    if (type === 'category') {
      if (!category) return res.status(400).json({ error: 'category required' });
      const { error } = await supabaseAdmin.from('private_channel_category_access').upsert({ user_id, category });
      if (error) throw error;
    } else {
      if (!channel_id) return res.status(400).json({ error: 'channel_id required' });
      const { error } = await supabaseAdmin.from('private_channel_user_access').upsert({ user_id, channel_id });
      if (error) throw error;
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/admin/private-channels/access/:accessId', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(user.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const { type } = req.query;
  const id = parseInt(req.params.accessId);
  try {
    const table = type === 'category' ? 'private_channel_category_access' : 'private_channel_user_access';
    const { error } = await supabaseAdmin.from(table).delete().eq('id', id);
    if (error) throw error;
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/user/private-channels', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const role = await getUserRole(user.id);
    console.log('[PRIV-API] user=' + user.id.slice(0,8) + ' role=' + role);
    if (role === 'admin') {
      const gMapA = new Map();
      privateChannels.forEach(ch => {
        const key = (ch.name || '').toLowerCase().trim();
        if (!gMapA.has(key)) gMapA.set(key, { ...ch, country: ch.country || detectCountry(ch.name) || '', servers: [{ id: ch.id, url: ch.stream_url }] });
        else {
          gMapA.get(key).servers.push({ id: ch.id, url: ch.stream_url });
          if (ch.status === 'Online') gMapA.get(key).status = 'Online';
        }
      });
      const adminResult = Array.from(gMapA.values());
      console.log('[PRIV-API] admin path: groups=' + adminResult.length + ' firstId=' + (adminResult[0] && adminResult[0].id));
      return res.json({ channels: adminResult });
    }
    const [{ data: chanAccess }, { data: catAccess }] = await Promise.all([
      supabaseAdmin.from('private_channel_user_access').select('channel_id').eq('user_id', user.id),
      supabaseAdmin.from('private_channel_category_access').select('category').eq('user_id', user.id)
    ]);
    const channelIds = (chanAccess || []).map(a => a.channel_id);
    const categories = (catAccess || []).map(a => a.category);
    console.log('[PRIV-API] user path: channelIds=' + channelIds.length + ' categories=' + categories.length);
    if (channelIds.length === 0 && categories.length === 0) return res.json({ channels: [] });
    const conditions = [];
    if (channelIds.length > 0) conditions.push(`id.in.(${channelIds.join(',')})`);
    if (categories.length > 0) conditions.push(`category.in.(${categories.map(c => '"' + c + '"').join(',')})`);
    const { data, error } = await supabaseAdmin.from('private_channels').select('*').or(conditions.join(',')).order('name', { ascending: true }).order('id', { ascending: true });
    if (error) throw error;
    const pcStatusMapM = new Map(privateChannels.map(pc => [pc.id, pc.status || 'Offline']));
    const withCountryM = (data || []).map(ch => ({ ...ch, country: ch.country || detectCountry(ch.name) || '', status: pcStatusMapM.get(ch.id) || ch.status || 'Offline' }));
    const gMapM = new Map();
    withCountryM.forEach(ch => {
      const key = ch.name.toLowerCase().trim();
      if (!gMapM.has(key)) gMapM.set(key, { ...ch, servers: [{ id: ch.id, url: ch.stream_url }] });
      else {
        gMapM.get(key).servers.push({ id: ch.id, url: ch.stream_url });
        if (ch.status === 'Online') gMapM.get(key).status = 'Online';
      }
    });
    res.json({ channels: Array.from(gMapM.values()) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Private PIN: verify ─────────────────────────────── */
app.post('/api/private/verify-pin', async (req, res) => {
  const user = await verifyUser(req);
  if (!user) return res.status(401).json({ ok: false });
  const { pin } = req.body;
  if (!pin || !/^\d{6}$/.test(String(pin))) return res.json({ ok: false });
  try {
    const { data } = await supabaseAdmin.from('profiles').select('private_pin').eq('id', user.id).single();
    if (!data || !data.private_pin) return res.json({ ok: false });
    const hashed = crypto.createHash('sha256').update(user.id + ':' + pin).digest('hex');
    return res.json({ ok: hashed === data.private_pin });
  } catch(_) { return res.json({ ok: false }); }
});

/* ── Admin: set user PIN ─────────────────────────────── */
app.post('/api/admin/users/:id/pin', async (req, res) => {
  const admin = await verifyUser(req);
  if (!admin) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(admin.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  const pin = String(req.body.pin || '').replace(/\D/g, '');
  if (pin.length !== 6) return res.status(400).json({ error: 'PIN must be 6 digits' });
  const userId = req.params.id;
  try {
    const hashed = crypto.createHash('sha256').update(userId + ':' + pin).digest('hex');
    const { error } = await supabaseAdmin
      .from('profiles')
      .update({ private_pin: hashed })
      .eq('id', userId);
    if (error) throw error;
    console.log('PIN set for user:', userId);
    res.json({ success: true });
  } catch(e) {
    console.error('PIN set error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

/* ── Admin: clear user PIN ───────────────────────────── */
app.delete('/api/admin/users/:id/pin', async (req, res) => {
  const admin = await verifyUser(req);
  if (!admin) return res.status(401).json({ error: 'Unauthorized' });
  if (await getUserRole(admin.id) !== 'admin') return res.status(403).json({ error: 'Forbidden' });
  try {
    const { error } = await supabaseAdmin.from('profiles').update({ private_pin: null }).eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/admin', async (req, res) => {
  const guestMin = parseInt(appConfig.guest_limit_minutes) || 5;
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" /><meta name="viewport" content="width=device-width,initial-scale=1.0" />
  <title>Admin Panel — MIZ Live TV</title>
  <style>
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{background:#0a0a0a;color:#fff;font-family:'Segoe UI',Arial,sans-serif;min-height:100vh;padding:0 0 60px}
    header{width:100%;background:#111;border-bottom:1px solid #1e1e1e;padding:14px 24px;display:flex;align-items:center;justify-content:space-between}
    .logo{display:flex;align-items:center;gap:8px;text-decoration:none;color:#fff;cursor:pointer}
    .logo span{font-size:18px;font-weight:700}
    .back-btn{background:#1a1a1a;color:#ccc;border:1px solid #333;border-radius:8px;padding:7px 14px;font-size:13px;cursor:pointer;text-decoration:none}
    .back-btn:hover{background:#2a2a2a}
    .container{max-width:960px;margin:0 auto;padding:24px 20px}
    .tabs{display:flex;gap:4px;margin-bottom:24px;background:#111;border:1px solid #1e1e1e;border-radius:10px;padding:4px}
    .tab-btn{flex:1;background:none;border:none;color:#555;font-size:13px;font-weight:600;padding:9px 12px;border-radius:7px;cursor:pointer;transition:all .15s;white-space:nowrap;position:relative}
    .tab-btn:hover{color:#ccc;background:#161616}
    .tab-btn.active{background:#1e1e1e;color:#fff;box-shadow:inset 0 -2px 0 #e00}
    .stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:24px}
    .stat-card{background:#141414;border:1px solid #1e1e1e;border-radius:10px;padding:16px 18px;border-left:3px solid #e00;transition:border-color .15s,box-shadow .15s}
    .stat-card:hover{box-shadow:0 4px 16px rgba(0,0,0,.4);border-color:#2a2a2a;border-left-color:#e00}
    .stat-card .num{font-size:30px;font-weight:800;color:#e00;line-height:1}
    .stat-card .lbl{font-size:11px;color:#666;margin-top:6px;text-transform:uppercase;letter-spacing:.5px}
    .u-avatar{width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:800;color:#fff;flex-shrink:0;position:relative}
    .u-avatar .av-dot{position:absolute;bottom:0;right:0;width:10px;height:10px;border-radius:50%;border:2px solid #141414}
    .u-avatar .av-dot.online{background:#33dd77;box-shadow:0 0 4px #33dd77}
    .u-avatar .av-dot.offline{background:#444}
    .section-title{font-size:14px;font-weight:700;color:#888;letter-spacing:.5px;text-transform:uppercase;margin-bottom:12px}
    .top-row{display:flex;align-items:center;gap:10px;padding:10px 14px;background:#141414;border:1px solid #1e1e1e;border-radius:8px;margin-bottom:5px}
    .top-rank{font-size:12px;color:#555;width:24px;flex-shrink:0}
    .top-name{flex:1;font-size:13px;color:#ccc}
    .top-views{font-size:12px;color:#e00;font-weight:700}
    .search-bar{width:100%;background:#141414;border:1px solid #2a2a2a;border-radius:8px;padding:10px 16px;color:#ddd;font-size:13px;margin-bottom:14px;outline:none;transition:border-color .2s}
    .search-bar:focus{border-color:#e00}
    .user-row{background:#141414;border:1px solid #1e1e1e;border-radius:8px;padding:12px 16px;display:flex;flex-direction:column;gap:10px;margin-bottom:6px}
    .u-email{font-size:13px;color:#ddd;font-weight:500;margin-bottom:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%}
    .u-meta{font-size:11px;color:#555;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
    .u-actions{display:flex;gap:6px;flex-wrap:wrap;width:100%}
    .u-actions .act-btn{flex:1;min-width:70px;text-align:center;justify-content:center}
    .badge{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:700;display:inline-block}
    .badge-admin{background:#3a1a3a;color:#d9a}
    .badge-member{background:#1a1a2a;color:#77a}
    .badge-banned{background:#3a1a1a;color:#f66}
    .badge-on{background:#1a3a1a;color:#4c4}
    .badge-off{background:#3a1a1a;color:#f66}
    .online-dot{display:inline-block;width:9px;height:9px;border-radius:50%;flex-shrink:0}
    .online-dot.online{background:#33ff77;box-shadow:0 0 5px #33ff77}
    .online-dot.offline{background:#555}
    #activity-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:9999;align-items:center;justify-content:center}
    #activity-modal.open{display:flex}
    #activity-box{background:#1a1a2e;border:1px solid #2a2a4a;border-radius:14px;width:min(700px,96vw);max-height:85vh;display:flex;flex-direction:column;overflow:hidden}
    #activity-box-header{padding:16px 20px;border-bottom:1px solid #2a2a4a;display:flex;justify-content:space-between;align-items:center}
    #activity-box-header h3{margin:0;font-size:16px;color:#ddd}
    #act-close{background:none;border:none;color:#888;font-size:22px;cursor:pointer;line-height:1}
    #activity-body{overflow-y:auto;padding:16px 20px;flex:1}
    .act-event{display:flex;gap:12px;padding:10px 0;border-bottom:1px solid #1e1e38}
    .act-event:last-child{border-bottom:none}
    .act-icon{font-size:20px;min-width:28px;text-align:center}
    .act-detail{flex:1}
    .act-title{font-size:13px;font-weight:600;color:#ccc}
    .act-time{font-size:11px;color:#666;margin-top:2px}
    .act-dur{font-size:11px;color:#9a8;margin-top:2px}
    .act-btn{background:#1e1e1e;border:1px solid #2a2a2a;color:#ccc;font-size:11px;padding:5px 10px;border-radius:6px;cursor:pointer;transition:all .15s;white-space:nowrap}
    .act-btn:hover{background:#2a2a2a;color:#fff}
    .act-blue{border-color:#1a3a6a;color:#7af}
    .act-blue:hover{background:#1a2a4a}
    .act-red{border-color:#5a1a1a;color:#f66}
    .act-red:hover{background:#3a1a1a}
    .act-green{border-color:#1a4a1a;color:#4c4}
    .act-green:hover{background:#1a3a1a}
    .act-url{border-color:#3a2a1a;color:#fa0}
    .act-url:hover{background:#2a1a0a}
    .ch-list{display:flex;flex-direction:column;gap:5px}
    .ch-row{background:#141414;border:1px solid #1e1e1e;border-radius:8px;padding:10px 14px;display:flex;flex-direction:column;gap:8px;transition:border-color .15s;margin-bottom:5px}
    .ch-row:hover{border-color:#2a2a2a}
    .ch-row-top{display:flex;align-items:center;gap:8px;min-width:0;width:100%}
    .ch-row-btns{display:flex;align-items:center;gap:6px;flex-wrap:wrap;width:100%}
    .ch-row-btns .act-btn{flex:1;min-width:60px;text-align:center}
    .ch-info{display:flex;align-items:center;gap:8px;min-width:0}
    .ch-id{font-size:11px;color:#444;width:30px;flex-shrink:0}
    .ch-name{font-size:13px;color:#ccc;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}
    .toggle{position:relative;width:44px;height:24px;cursor:pointer;flex-shrink:0}
    .toggle input{opacity:0;width:0;height:0}
    .slider{position:absolute;inset:0;background:#333;border-radius:24px;transition:.3s}
    .slider::before{content:'';position:absolute;height:18px;width:18px;left:3px;top:3px;background:#fff;border-radius:50%;transition:.3s}
    input:checked+.slider{background:#2a2}
    input:checked+.slider::before{transform:translateX(20px)}
    .blocked .slider{background:#e00!important}
    .bulk-bar{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;align-items:center}
    .bulk-label{font-size:12px;color:#555;margin-right:4px}
    .cat-select{background:#141414;border:1px solid #2a2a2a;color:#ccc;font-size:12px;padding:6px 10px;border-radius:6px;outline:none}
    .cat-select:focus{border-color:#e00}
    .settings-row{background:#141414;border:1px solid #1e1e1e;border-radius:10px;padding:20px 22px;margin-bottom:14px}
    .settings-row label{font-size:13px;color:#aaa;display:block;margin-bottom:10px;font-weight:600}
    .settings-row .hint{font-size:11px;color:#555;margin-top:6px}
    .range-wrap{display:flex;align-items:center;gap:14px}
    input[type=range]{flex:1;accent-color:#e00}
    .range-val{font-size:22px;font-weight:700;color:#e00;min-width:40px;text-align:center}
    .save-btn{background:#e00;color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:13px;font-weight:600;cursor:pointer;transition:background .15s;margin-top:12px}
    .save-btn:hover{background:#c00}
    .url-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:9000;align-items:center;justify-content:center}
    .url-modal.open{display:flex}
    #confirm-modal{z-index:99999}
    .confirm-box{background:#1a1a1a;border:1px solid #2e2e2e;border-radius:16px;padding:32px 28px 24px;width:90%;max-width:400px;text-align:center;box-shadow:0 24px 60px rgba(0,0,0,.7)}
    .confirm-icon{font-size:42px;margin-bottom:14px;display:block}
    .confirm-title{font-size:15px;font-weight:700;color:#f0f0f0;margin-bottom:8px}
    .confirm-msg{font-size:13px;color:#999;line-height:1.5;margin-bottom:24px}
    .confirm-btns{display:flex;gap:10px;justify-content:center}
    .confirm-btns .act-btn{min-width:110px;padding:10px 20px;font-size:13px;font-weight:600;border-radius:9px}
    .act-btn-cancel{background:#2a2a2a;color:#aaa;border:1px solid #333}
    .act-btn-cancel:hover{background:#333;color:#eee}
    .url-box{background:#171717;border:1px solid #2a2a2a;border-radius:12px;padding:24px;width:90%;max-width:560px}
    .url-box h4{font-size:15px;font-weight:700;margin-bottom:14px}
    .url-input{width:100%;background:#1e1e1e;border:1px solid #333;border-radius:7px;padding:10px 14px;color:#ddd;font-size:12px;outline:none;margin-bottom:12px;font-family:monospace}
    .url-input:focus{border-color:#e00}
    .url-btns{display:flex;gap:8px;justify-content:flex-end}
    #msg{position:fixed;bottom:20px;right:20px;background:#222;border:1px solid #333;border-radius:8px;padding:10px 16px;font-size:13px;opacity:0;transition:opacity .3s;pointer-events:none;z-index:99999}
    #msg.show{opacity:1}
    @media(max-width:600px){
      .container{padding:16px 12px}
      .tabs{overflow-x:auto;-webkit-overflow-scrolling:touch;flex-wrap:nowrap;border-radius:8px}
      .tab-btn{flex:0 0 auto;font-size:11px;padding:8px 10px;white-space:nowrap}
      .stat-grid{grid-template-columns:repeat(2,1fr)!important;gap:8px}
      .stat-card{padding:12px 14px}
      .stat-card .num{font-size:24px}
      .ch-name{max-width:none!important;flex:1}
      .bulk-bar{gap:6px}
      .bulk-bar .act-btn{flex:1}
      header{padding:10px 14px}
      .logo span{font-size:15px}
      .back-btn{font-size:11px;padding:5px 10px}
    }
  </style>
</head>
<body>
<header>
  <div class="logo" onclick="location.href='/'">
    ${LOGO_FULL_HTML}
  </div>
  <a href="/" class="back-btn">← Back to App</a>
</header>
<div class="container">
  <div class="tabs">
    <button class="tab-btn active" data-tab="dashboard">📊 Dashboard</button>
    <button class="tab-btn" data-tab="users">👥 Users</button>
    <button class="tab-btn" data-tab="channels">📺 Channels</button>
    <button class="tab-btn" data-tab="settings">⚙️ Settings</button>
    <button class="tab-btn" data-tab="private">🔒 Private</button>
  </div>
  <div id="tab-dashboard">
    <div class="stat-grid">
      <div class="stat-card"><div class="num" id="st-users">—</div><div class="lbl">Total Users</div></div>
      <div class="stat-card"><div class="num" id="st-today">—</div><div class="lbl">Today's Signups</div></div>
      <div class="stat-card"><div class="num" id="st-active">—</div><div class="lbl">Active Now</div></div>
      <div class="stat-card"><div class="num" id="st-total-ch">—</div><div class="lbl">Total Channels</div></div>
      <div class="stat-card"><div class="num" id="st-blocked">—</div><div class="lbl">Blocked Channels</div></div>
    </div>
    <div class="section-title">🔥 Most Watched Channels</div>
    <div id="top-channels"><div style="color:#444;text-align:center;padding:20px">Loading...</div></div>
  </div>
  <div id="tab-users" hidden>
    <input class="search-bar" id="user-search" placeholder="🔍 Email দিয়ে খোঁজো..." />
    <div id="users-list"><div style="color:#444;text-align:center;padding:20px">Loading...</div></div>
  </div>
  <div id="tab-channels" hidden>
    <div class="stat-grid" style="grid-template-columns:repeat(3,1fr)">
      <div class="stat-card"><div class="num" id="total-count">—</div><div class="lbl">Total Channels</div></div>
      <div class="stat-card"><div class="num" id="blocked-count">—</div><div class="lbl">Blocked</div></div>
      <div class="stat-card"><div class="num" id="visible-count">—</div><div class="lbl">Visible to Guests</div></div>
    </div>
    <div class="bulk-bar">
      <span class="bulk-label">⚡ Bulk:</span>
      <select class="cat-select" id="bulk-cat">
        <option value="All">🔍 All Categories</option>
        <option value="Bangla">🇧🇩 Bangla</option>
        <option value="News">📰 News</option>
        <option value="Movies">🎬 Movies</option>
        <option value="Music">🎵 Music</option>
        <option value="Kids">👶 Kids</option>
        <option value="Sports">⚽ Sports</option>
        <option value="International">🌍 International</option>
      </select>
      <button class="act-btn act-red" onclick="bulkAction('block')">🚫 Block All</button>
      <button class="act-btn act-green" onclick="bulkAction('unblock')">✅ Unblock All</button>
      <button class="act-btn act-green" style="margin-left:auto" onclick="openAddModal()">➕ Add Channel</button>
    </div>
    <input class="search-bar" id="ch-search" placeholder="🔍 Channel খোঁজো (name/category)..." />
    <div class="ch-list" id="ch-list"><div style="color:#444;text-align:center;padding:20px">Loading...</div></div>
  </div>
  <div id="tab-settings" hidden>
    <div class="settings-row">
      <label>⏱ Guest Free Watch Time</label>
      <div class="range-wrap">
        <input type="range" id="guest-range" min="1" max="60" value="${guestMin}" oninput="document.getElementById('guest-val').textContent=this.value" />
        <div class="range-val" id="guest-val">${guestMin}</div>
        <span style="color:#555;font-size:13px">min</span>
      </div>
      <div class="hint">Guest users এই সময়ের পর Login করতে বলা হবে। এখন: <strong id="cur-limit">${guestMin} minutes</strong></div>
      <button class="save-btn" onclick="saveGuestLimit()">💾 Save</button>
    </div>
  </div>
  <div id="tab-private" hidden>
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap">
      <div class="stat-grid" style="grid-template-columns:repeat(2,1fr);margin:0;flex:1;min-width:200px">
        <div class="stat-card"><div class="num" id="pc-total">—</div><div class="lbl">Private Channels</div></div>
        <div class="stat-card"><div class="num" id="pc-access-count">—</div><div class="lbl">Access Grants</div></div>
      </div>
      <button class="act-btn act-green" onclick="openPcAdd()" style="white-space:nowrap">➕ Add Private Channel</button>
    </div>
    <input class="search-bar" id="pc-search" placeholder="🔍 Private channel খোঁজো (name/category)..." />
    <div class="ch-list" id="pc-list"><div style="color:#444;text-align:center;padding:20px">Loading...</div></div>
    <div id="pc-pagination"></div>
  </div>
</div>

<!-- URL Edit Modal -->
<div class="url-modal" id="url-modal">
  <div class="url-box">
    <h4>✏️ Edit Stream URL — <span id="url-ch-name"></span></h4>
    <input class="url-input" id="url-input" placeholder="https://example.com/stream/index.m3u8" />
    <div class="url-btns">
      <button class="act-btn" onclick="document.getElementById('url-modal').classList.remove('open')">Cancel</button>
      <button class="act-btn act-green" onclick="saveUrl()">💾 Save URL</button>
    </div>
  </div>
</div>

<!-- Edit Channel Modal -->
<div class="url-modal" id="edit-modal">
  <div class="url-box" style="max-width:600px">
    <h4>✏️ Edit Channel — <span id="edit-ch-name-title"></span></h4>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Channel Name</label>
    <input class="url-input" id="edit-name" placeholder="Channel Name" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Stream URL</label>
    <input class="url-input" id="edit-url" placeholder="https://example.com/stream/index.m3u8" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Category</label>
    <select class="cat-select" id="edit-cat" style="width:100%;margin-bottom:12px;padding:10px 14px">
      <option value="Bangla">🇧🇩 Bangla</option>
      <option value="News">📰 News</option>
      <option value="Movies">🎬 Movies</option>
      <option value="Music">🎵 Music</option>
      <option value="Kids">👶 Kids</option>
      <option value="Sports">⚽ Sports</option>
      <option value="International">🌍 International</option>
    </select>
    <div class="url-btns">
      <button class="act-btn" onclick="document.getElementById('edit-modal').classList.remove('open')">Cancel</button>
      <button class="act-btn act-green" onclick="saveEdit()">💾 Save Changes</button>
    </div>
  </div>
</div>

<!-- Add Channel Modal -->
<div class="url-modal" id="add-modal">
  <div class="url-box" style="max-width:600px">
    <h4>➕ Add New Channel</h4>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Channel Name *</label>
    <input class="url-input" id="add-name" placeholder="e.g. Jamuna TV" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Stream URL *</label>
    <input class="url-input" id="add-url" placeholder="https://example.com/stream/index.m3u8" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Category</label>
    <select class="cat-select" id="add-cat" style="width:100%;margin-bottom:10px;padding:10px 14px">
      <option value="">Auto-detect</option>
      <option value="Bangla">🇧🇩 Bangla</option>
      <option value="News">📰 News</option>
      <option value="Movies">🎬 Movies</option>
      <option value="Music">🎵 Music</option>
      <option value="Kids">👶 Kids</option>
      <option value="Sports">⚽ Sports</option>
      <option value="International">🌍 International</option>
    </select>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Country</label>
    <select class="cat-select" id="add-country" style="width:100%;margin-bottom:12px;padding:10px 14px">
      <option value="">Auto-detect</option>
      <option value="BD">🇧🇩 Bangladesh</option>
      <option value="IN">🇮🇳 India</option>
      <option value="GB">🇬🇧 UK</option>
      <option value="US">🇺🇸 USA</option>
      <option value="PK">🇵🇰 Pakistan</option>
      <option value="AE">🇦🇪 UAE</option>
      <option value="SA">🇸🇦 Saudi Arabia</option>
      <option value="QA">🇶🇦 Qatar</option>
      <option value="TR">🇹🇷 Turkey</option>
      <option value="FR">🇫🇷 France</option>
      <option value="DE">🇩🇪 Germany</option>
      <option value="RU">🇷🇺 Russia</option>
      <option value="IR">🇮🇷 Iran</option>
      <option value="EG">🇪🇬 Egypt</option>
      <option value="AF">🇦🇫 Afghanistan</option>
      <option value="NP">🇳🇵 Nepal</option>
      <option value="LK">🇱🇰 Sri Lanka</option>
      <option value="MM">🇲🇲 Myanmar</option>
      <option value="JP">🇯🇵 Japan</option>
      <option value="CN">🇨🇳 China</option>
      <option value="KR">🇰🇷 Korea</option>
      <option value="ID">🇮🇩 Indonesia</option>
      <option value="MY">🇲🇾 Malaysia</option>
      <option value="TH">🇹🇭 Thailand</option>
      <option value="IT">🇮🇹 Italy</option>
      <option value="ES">🇪🇸 Spain</option>
      <option value="NG">🇳🇬 Nigeria</option>
      <option value="GH">🇬🇭 Ghana</option>
    </select>
    <div class="url-btns">
      <button class="act-btn" onclick="document.getElementById('add-modal').classList.remove('open')">Cancel</button>
      <button class="act-btn act-green" onclick="addChannel()">➕ Add Channel</button>
    </div>
  </div>
</div>

<!-- Private Channel Add/Edit Modal -->
<div class="url-modal" id="pc-add-modal">
  <div class="url-box" style="max-width:600px">
    <h4 id="pc-modal-title">➕ Add Private Channel</h4>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Channel Name *</label>
    <input class="url-input" id="pc-name" placeholder="e.g. VIP Sports HD" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Stream URL *</label>
    <input class="url-input" id="pc-url" placeholder="https://example.com/stream/index.m3u8" style="margin-bottom:10px" />
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Category</label>
    <select class="cat-select" id="pc-cat" style="width:100%;margin-bottom:10px;padding:10px 14px">
      <option value="Private">🔒 Private</option>
      <option value="VIP">⭐ VIP</option>
      <option value="Premium">💎 Premium</option>
      <option value="Bangla">🇧🇩 Bangla</option>
      <option value="News">📰 News</option>
      <option value="Movies">🎬 Movies</option>
      <option value="Music">🎵 Music</option>
      <option value="Sports">⚽ Sports</option>
      <option value="International">🌍 International</option>
    </select>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Country</label>
    <select class="cat-select" id="pc-country" style="width:100%;margin-bottom:10px;padding:10px 14px">
      <option value="">Auto-detect</option>
      <option value="BD">🇧🇩 Bangladesh</option>
      <option value="IN">🇮🇳 India</option>
      <option value="GB">🇬🇧 UK</option>
      <option value="US">🇺🇸 USA</option>
      <option value="PK">🇵🇰 Pakistan</option>
      <option value="AE">🇦🇪 UAE</option>
      <option value="SA">🇸🇦 Saudi Arabia</option>
      <option value="QA">🇶🇦 Qatar</option>
      <option value="TR">🇹🇷 Turkey</option>
      <option value="FR">🇫🇷 France</option>
      <option value="DE">🇩🇪 Germany</option>
      <option value="RU">🇷🇺 Russia</option>
      <option value="IR">🇮🇷 Iran</option>
      <option value="EG">🇪🇬 Egypt</option>
      <option value="AF">🇦🇫 Afghanistan</option>
      <option value="NP">🇳🇵 Nepal</option>
      <option value="LK">🇱🇰 Sri Lanka</option>
      <option value="MM">🇲🇲 Myanmar</option>
      <option value="JP">🇯🇵 Japan</option>
      <option value="CN">🇨🇳 China</option>
      <option value="KR">🇰🇷 Korea</option>
      <option value="ID">🇮🇩 Indonesia</option>
      <option value="MY">🇲🇾 Malaysia</option>
      <option value="TH">🇹🇭 Thailand</option>
      <option value="IT">🇮🇹 Italy</option>
      <option value="ES">🇪🇸 Spain</option>
      <option value="NG">🇳🇬 Nigeria</option>
      <option value="GH">🇬🇭 Ghana</option>
    </select>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Description (optional)</label>
    <input class="url-input" id="pc-desc" placeholder="Short description..." style="margin-bottom:12px" />
    <div class="url-btns">
      <button class="act-btn" onclick="document.getElementById('pc-add-modal').classList.remove('open')">Cancel</button>
      <button class="act-btn act-green" onclick="savePcChannel()">💾 Save</button>
    </div>
  </div>
</div>

<!-- PIN Set Modal -->
<div class="url-modal" id="pin-set-modal">
  <div class="url-box" style="max-width:380px">
    <h4>🔑 Private PIN — <span id="pin-set-email" style="color:#b8a;font-size:13px"></span></h4>
    <label style="font-size:12px;color:#888;display:block;margin-bottom:6px;margin-top:12px">৬-সংখ্যার PIN</label>
    <input class="url-input" id="pin-set-input" type="tel" inputmode="numeric" maxlength="6" placeholder="000000" style="letter-spacing:8px;font-size:22px;text-align:center;margin-bottom:14px" oninput="this.value=this.value.replace(/\D/g,'')" />
    <div class="url-btns">
      <button class="act-btn act-red" onclick="clearPinSet()">🗑 Clear PIN</button>
      <button class="act-btn" onclick="document.getElementById('pin-set-modal').classList.remove('open')">Cancel</button>
      <button class="act-btn act-green" onclick="savePinSet()">💾 Save PIN</button>
    </div>
  </div>
</div>

<!-- Private Channel Access Modal -->
<div class="url-modal" id="pc-access-modal">
  <div class="url-box" style="max-width:580px;max-height:85vh;overflow-y:auto">
    <h4 id="pc-access-title">👥 Access Management</h4>
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;padding:14px;margin:14px 0">
      <div style="font-size:11px;color:#666;font-weight:700;letter-spacing:.5px;margin-bottom:10px">➕ GRANT ACCESS</div>
      <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">User নির্বাচন করো</label>
      <select class="cat-select" id="pc-grant-user" style="width:100%;padding:9px 12px;margin-bottom:8px">
        <option value="">Loading...</option>
      </select>
      <label style="font-size:12px;color:#888;display:block;margin-bottom:4px">Access Type</label>
      <select class="cat-select" id="pc-grant-type" style="width:100%;padding:9px 12px;margin-bottom:10px">
        <option value="channel">🔒 This channel only</option>
        <option value="category">📁 Entire category (এই category-র সব private channels)</option>
      </select>
      <button class="act-btn act-green" onclick="grantPcAccess()" style="width:100%;padding:9px">✅ Grant Access</button>
    </div>
    <div style="font-size:11px;color:#666;font-weight:700;letter-spacing:.5px;margin-bottom:8px">🔒 CHANNEL-LEVEL ACCESS (শুধু এই channel)</div>
    <div id="pc-ch-access-list" style="margin-bottom:16px"></div>
    <div style="font-size:11px;color:#666;font-weight:700;letter-spacing:.5px;margin-bottom:8px">📁 CATEGORY-LEVEL ACCESS — <span id="pc-cat-label" style="color:#8af"></span></div>
    <div id="pc-cat-access-list" style="margin-bottom:14px"></div>
    <div style="text-align:right">
      <button class="act-btn" onclick="document.getElementById('pc-access-modal').classList.remove('open')">Close</button>
    </div>
  </div>
</div>

<div class="url-modal" id="confirm-modal">
  <div class="confirm-box">
    <span class="confirm-icon" id="confirm-icon">⚠️</span>
    <div class="confirm-title" id="confirm-title">Are you sure?</div>
    <div class="confirm-msg" id="confirm-msg"></div>
    <div class="confirm-btns">
      <button class="act-btn act-btn-cancel" id="confirm-cancel">Cancel</button>
      <button class="act-btn" id="confirm-ok">Confirm</button>
    </div>
  </div>
</div>

<div id="msg"></div>
<script>
  const token = localStorage.getItem('miz_token');
  if (!token) window.location.href = '/login';

  let tabLoaded = {};
  let _activeTab = localStorage.getItem('admin_active_tab') || 'dashboard';
  let _autoRefreshTimer = null;

  function startAutoRefresh(tab) {
    clearInterval(_autoRefreshTimer);
    if (tab === 'dashboard') {
      _autoRefreshTimer = setInterval(() => loadDashboard(), 20000);
    } else if (tab === 'users') {
      _autoRefreshTimer = setInterval(() => { tabLoaded.users = false; loadUsers(); }, 30000);
    } else {
      _autoRefreshTimer = null;
    }
  }

  function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('[id^="tab-"]').forEach(p => p.hidden = true);
    const btn = document.querySelector('.tab-btn[data-tab="' + tabName + '"]');
    if (btn) btn.classList.add('active');
    const panel = document.getElementById('tab-' + tabName);
    if (panel) panel.hidden = false;
    _activeTab = tabName;
    localStorage.setItem('admin_active_tab', tabName);
    if (!tabLoaded[tabName]) { tabLoaded[tabName] = true; loadTab(tabName); }
    startAutoRefresh(tabName);
  }

  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  switchTab(_activeTab);

  function loadTab(t) {
    if (t === 'dashboard') loadDashboard();
    if (t === 'users') loadUsers();
    if (t === 'channels') loadChTab();
    if (t === 'private') loadPrivateTab();
  }

  function showMsg(text, ok) {
    const el = document.getElementById('msg');
    el.textContent = text; el.style.color = ok ? '#4c4' : '#f66';
    el.classList.add('show'); setTimeout(() => el.classList.remove('show'), 2500);
  }

  /* ─── Dashboard ─── */
  async function loadDashboard() {
    const r = await fetch('/api/admin/stats', { headers: { Authorization: 'Bearer ' + token } });
    if (r.status === 403) { alert('Admin only!'); location.href = '/'; return; }
    const d = await r.json();
    document.getElementById('st-users').textContent = d.total;
    document.getElementById('st-today').textContent = d.todaySignups;
    document.getElementById('st-active').textContent = d.activeCount;
    document.getElementById('st-total-ch').textContent = d.totalChannels;
    document.getElementById('st-blocked').textContent = d.blockedChannels;
    const topEl = document.getElementById('top-channels');
    if (!d.topChannels || !d.topChannels.length) {
      topEl.innerHTML = '<div style="color:#444;text-align:center;padding:16px">এখনো কোনো view tracked হয়নি।</div>';
    } else {
      topEl.innerHTML = d.topChannels.map((ch, i) => \`
        <div class="top-row"><span class="top-rank">#\${i+1}</span><span class="top-name">\${ch.name}</span><span class="top-views">\${ch.views} views</span></div>
      \`).join('');
    }
  }

  /* ─── Users ─── */
  let allUsers = [];
  async function loadUsers() {
    document.getElementById('users-list').innerHTML = '<div style="color:#444;text-align:center;padding:20px">Loading...</div>';
    const r = await fetch('/api/admin/users', { headers: { Authorization: 'Bearer ' + token } });
    const d = await r.json();
    allUsers = d.users || [];
    renderUsers(allUsers);
  }
  function timeSince(ms) {
    const s = Math.floor((Date.now() - ms) / 1000);
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s/60) + 'm ago';
    if (s < 86400) return Math.floor(s/3600) + 'h ago';
    return Math.floor(s/86400) + 'd ago';
  }
  function _avColor(email) { const c=['#c0392b','#8e44ad','#2980b9','#16a085','#d35400','#1a5276','#6c3483','#1e8449']; let h=0; for(let i=0;i<email.length;i++) h=(h*31+email.charCodeAt(i))&0xffff; return c[h%c.length]; }
  function _avInitial(email) { return (email||'?')[0].toUpperCase(); }
  function renderUsers(list) {
    const el = document.getElementById('users-list');
    if (!list.length) { el.innerHTML = '<div style="color:#444;text-align:center;padding:20px">কোনো user নেই।</div>'; return; }
    el.innerHTML = list.map(u => \`
      <div class="user-row">
        <div style="display:flex;align-items:center;gap:10px;min-width:0;width:100%">
          <div class="u-avatar" style="background:\${_avColor(u.email)};flex-shrink:0">
            \${_avInitial(u.email)}
            <span class="av-dot \${u.is_online ? 'online' : 'offline'}"></span>
          </div>
          <div style="min-width:0;flex:1">
            <div class="u-email" title="\${u.email}">\${u.email}</div>
            <div class="u-meta">
              <span>Joined \${new Date(u.created_at).toLocaleDateString()}</span>
              <span class="badge badge-\${u.role}">\${u.role === 'admin' ? '⭐ Admin' : '👤 Member'}</span>
              \${u.banned ? '<span class="badge badge-banned">🚫 Banned</span>' : ''}
              \${u.is_online ? '<span style="font-size:11px;color:#3f3;font-weight:600">🟢 Online</span>' : (u.last_seen ? '<span style="font-size:11px;color:#666">⚫ ' + timeSince(u.last_seen) + '</span>' : '')}
              \${u.watching_channel ? '<span style="font-size:11px;color:#e88;font-weight:600">📺 ' + u.watching_channel.name + '</span>' : ''}
            </div>
          </div>
        </div>
        <div class="u-actions">
          <button class="act-btn" style="border-color:#1a3a5a;color:#6af" onclick="openActivity('\${u.id}','\${u.email.replace(/'/g,'&apos;')}')">📋 Details</button>
          \${u.role === 'admin'
            ? \`<button class="act-btn" onclick="setRole('\${u.id}','member')">👤 Member</button>\`
            : \`<button class="act-btn act-blue" onclick="setRole('\${u.id}','admin')">⭐ Admin</button>\`}
          \${u.banned
            ? \`<button class="act-btn act-green" onclick="setBan('\${u.id}',false)">✅ Unban</button>\`
            : \`<button class="act-btn act-red" onclick="setBan('\${u.id}',true)">🚫 Ban</button>\`}
          <button class="act-btn" style="border-color:#3a2a5a;color:#b8a" onclick="openPinSet('\${u.id}','\${u.email}')">🔑 PIN</button>
        </div>
      </div>
    \`).join('');
  }
  document.getElementById('user-search').addEventListener('input', e => {
    const q = e.target.value.toLowerCase();
    renderUsers(q ? allUsers.filter(u => u.email.toLowerCase().includes(q)) : allUsers);
  });
  async function setRole(id, role) {
    const r = await fetch('/api/admin/users/' + id + '/role', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ role })
    });
    const d = await r.json();
    if (d.success) { tabLoaded.users = false; loadUsers(); showMsg('Role updated!', true); }
    else showMsg(d.error || 'Error', false);
  }
  async function setBan(id, ban) {
    const r = await fetch('/api/admin/users/' + id + '/ban', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ ban })
    });
    const d = await r.json();
    if (d.success) { tabLoaded.users = false; loadUsers(); showMsg(ban ? 'User banned!' : 'User unbanned!', true); }
    else showMsg(d.error || 'Error', false);
  }
  let pinTargetId = '';
  function openPinSet(id, email) {
    pinTargetId = id;
    document.getElementById('pin-set-email').textContent = email;
    document.getElementById('pin-set-input').value = '';
    document.getElementById('pin-set-modal').classList.add('open');
  }
  async function savePinSet() {
    try {
      const rawVal = document.getElementById('pin-set-input').value;
      const pin = rawVal.replace(/\D/g, '');
      if (pin.length !== 6) { showMsg('PIN must be exactly 6 digits', false); return; }
      if (!pinTargetId) { showMsg('User ID missing', false); return; }
      const r = await fetch('/api/admin/users/' + pinTargetId + '/pin', {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
        body: JSON.stringify({ pin })
      });
      const d = await r.json();
      document.getElementById('pin-set-modal').classList.remove('open');
      if (d.success) showMsg('✅ PIN set successfully!', true);
      else showMsg('❌ ' + (d.error || 'Error setting PIN'), false);
    } catch(e) {
      document.getElementById('pin-set-modal').classList.remove('open');
      showMsg('❌ Failed: ' + e.message, false);
    }
  }
  async function clearPinSet() {
    try {
      const r = await fetch('/api/admin/users/' + pinTargetId + '/pin', {
        method: 'DELETE', headers: { Authorization: 'Bearer ' + token }
      });
      const d = await r.json();
      document.getElementById('pin-set-modal').classList.remove('open');
      if (d.success) showMsg('✅ PIN cleared!', true);
      else showMsg('❌ ' + (d.error || 'Error'), false);
    } catch(e) {
      document.getElementById('pin-set-modal').classList.remove('open');
      showMsg('❌ Failed: ' + e.message, false);
    }
  }

  /* ─── Channels ─── */
  const CAT_EMOJI = { Bangla:'🇧🇩', News:'📰', Movies:'🎬', Music:'🎵', Kids:'👶', Sports:'⚽', International:'🌍' };
  let allChannels = [];
  async function loadChTab() {
    document.getElementById('ch-list').innerHTML = '<div style="color:#444;text-align:center;padding:20px">Loading...</div>';
    const r = await fetch('/api/admin/channels', { headers: { Authorization: 'Bearer ' + token } });
    if (r.status === 401 || r.status === 403) { alert('Access denied!'); location.href='/'; return; }
    const d = await r.json();
    allChannels = d.channels;
    updateChStats();
    renderChannels(allChannels);
  }
  function updateChStats() {
    const bl = allChannels.filter(c => c.blocked).length;
    document.getElementById('total-count').textContent = allChannels.length;
    document.getElementById('blocked-count').textContent = bl;
    document.getElementById('visible-count').textContent = allChannels.length - bl;
  }
  function renderChannels(list) {
    const el = document.getElementById('ch-list');
    if (!list.length) { el.innerHTML = '<div style="color:#444;text-align:center;padding:20px">কোনো channel পাওয়া যায়নি।</div>'; return; }
    el.innerHTML = list.map(ch => \`
      <div class="ch-row" id="row-\${ch.id}">
        <div class="ch-row-top">
          <span class="ch-id">#\${ch.id}</span>
          \${ch.is_highlighted ? '<span style="font-size:10px;font-weight:700;color:#fff;background:#e00;border-radius:4px;padding:2px 6px;flex-shrink:0;letter-spacing:.5px">🔴 LIVE</span>' : ''}
          <span class="ch-name" title="\${ch.name}" style="flex:1">\${ch.name}</span>
          <span style="font-size:11px;color:#555;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:4px;padding:2px 6px;flex-shrink:0">\${CAT_EMOJI[ch.category]||'🌍'} \${ch.category||'?'}</span>
          <span class="badge \${ch.blocked ? 'badge-off' : 'badge-on'}" id="badge-\${ch.id}" style="flex-shrink:0">\${ch.blocked ? '🚫' : '✅'}</span>
          <label class="toggle \${ch.blocked ? 'blocked' : ''}" style="flex-shrink:0">
            <input type="checkbox" \${ch.blocked ? '' : 'checked'} onchange="toggleCh(\${ch.id}, this)" />
            <span class="slider"></span>
          </label>
        </div>
        <div class="ch-row-btns">
          <button class="act-btn \${ch.is_highlighted ? 'act-red' : 'act-green'}" id="hl-btn-\${ch.id}" onclick="toggleHighlight(\${ch.id})">\${ch.is_highlighted ? '🔴 Live ON' : '⭐ Highlight'}</button>
          <button class="act-btn act-url" onclick="openEditModal(\${ch.id})">✏️ Edit</button>
          <button class="act-btn act-red" onclick="deleteCh(\${ch.id},'\${ch.name.replace(/'/g,'&apos;')}')">🗑 Delete</button>
        </div>
      </div>
    \`).join('');
  }
  document.getElementById('ch-search').addEventListener('input', e => {
    const q = e.target.value.toLowerCase().trim();
    renderChannels(q ? allChannels.filter(c => c.name.toLowerCase().includes(q) || (c.category||'').toLowerCase().includes(q)) : allChannels);
  });
  async function toggleCh(id, el) {
    const newBlocked = !el.checked;
    el.disabled = true;
    try {
      const r = await fetch('/api/admin/channels/' + id, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
        body: JSON.stringify({ blocked: newBlocked })
      });
      const d = await r.json();
      if (d.success) {
        const ch = allChannels.find(c => c.id === id);
        if (ch) ch.blocked = newBlocked;
        document.getElementById('badge-' + id).className = 'badge ' + (newBlocked ? 'badge-off' : 'badge-on');
        document.getElementById('badge-' + id).textContent = newBlocked ? '🚫 Blocked' : '✅ Visible';
        document.getElementById('row-' + id).querySelector('.toggle').className = 'toggle ' + (newBlocked ? 'blocked' : '');
        updateChStats();
        showMsg(newBlocked ? 'Channel blocked' : 'Channel visible', true);
      }
    } catch(_) { showMsg('Error!', false); el.checked = !newBlocked; }
    el.disabled = false;
  }
  function showConfirm(msg, isDanger, icon) {
    return new Promise(resolve => {
      document.getElementById('confirm-msg').textContent = msg;
      document.getElementById('confirm-icon').textContent = icon || (isDanger ? '🗑️' : '⚠️');
      document.getElementById('confirm-title').textContent = isDanger ? 'Are you sure?' : 'Confirm Action';
      const okBtn = document.getElementById('confirm-ok');
      okBtn.className = 'act-btn ' + (isDanger ? 'act-red' : 'act-green');
      okBtn.textContent = isDanger ? 'Delete' : 'Confirm';
      const modal = document.getElementById('confirm-modal');
      modal.classList.add('open');
      const cleanup = () => { modal.classList.remove('open'); okBtn.onclick = null; document.getElementById('confirm-cancel').onclick = null; };
      okBtn.onclick = () => { cleanup(); resolve(true); };
      document.getElementById('confirm-cancel').onclick = () => { cleanup(); resolve(false); };
    });
  }

  async function bulkAction(action) {
    const category = document.getElementById('bulk-cat').value;
    const label = category === 'All' ? 'all channels' : category + ' channels';
    if (!await showConfirm((action === 'block' ? '🚫 Block' : '✅ Unblock') + ' ' + label + ' for guests?', false, action === 'block' ? '🚫' : '✅')) return;
    const r = await fetch('/api/admin/channels/bulk', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ action, category })
    });
    const d = await r.json();
    if (d.success) { tabLoaded.channels = false; loadChTab(); showMsg(d.affected + ' channels ' + (action === 'block' ? 'blocked' : 'unblocked') + '!', true); }
    else showMsg(d.error || 'Error', false);
  }
  async function toggleHighlight(id) {
    const ch = allChannels.find(c => c.id === id);
    if (!ch) return;
    const newVal = !ch.is_highlighted;
    const btn = document.getElementById('hl-btn-' + id);
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/admin/channels/' + id + '/highlight', {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
        body: JSON.stringify({ highlighted: newVal })
      });
      const d = await r.json();
      if (d.success) {
        ch.is_highlighted = newVal;
        renderChannels(allChannels);
        showMsg(newVal ? '🔴 Channel highlighted as LIVE!' : '✅ Highlight removed', true);
      } else showMsg(d.error || 'Error', false);
    } catch(_) { showMsg('Error!', false); }
    if (btn) btn.disabled = false;
  }

  async function deleteCh(id, name) {
    if (!await showConfirm('Delete "' + name + '"? This cannot be undone.', true, '🗑️')) return;
    const r = await fetch('/api/admin/channels/' + id, {
      method: 'DELETE', headers: { Authorization: 'Bearer ' + token }
    });
    const d = await r.json();
    if (d.success) {
      allChannels = allChannels.filter(c => c.id !== id);
      document.getElementById('row-' + id)?.remove();
      updateChStats();
      showMsg('Channel deleted!', true);
    } else showMsg(d.error || 'Error', false);
  }

  /* ─── Edit Channel Modal ─── */
  let _editChId = null;
  function openEditModal(id) {
    const ch = allChannels.find(c => c.id === id);
    if (!ch) return;
    _editChId = id;
    document.getElementById('edit-ch-name-title').textContent = ch.name;
    document.getElementById('edit-name').value = ch.name;
    document.getElementById('edit-url').value = ch.stream_url || '';
    document.getElementById('edit-cat').value = ch.category || 'International';
    document.getElementById('edit-modal').classList.add('open');
  }
  async function saveEdit() {
    const channel_name = document.getElementById('edit-name').value.trim();
    const stream_url = document.getElementById('edit-url').value.trim();
    const category = document.getElementById('edit-cat').value;
    if (!channel_name || !stream_url) { showMsg('Name and URL required', false); return; }
    const r = await fetch('/api/admin/channels/' + _editChId, {
      method: 'PUT', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ channel_name, stream_url, category })
    });
    const d = await r.json();
    if (d.success) {
      const ch = allChannels.find(c => c.id === _editChId);
      if (ch) { ch.name = channel_name; ch.stream_url = stream_url; ch.category = category; }
      document.getElementById('edit-modal').classList.remove('open');
      renderChannels(allChannels);
      showMsg('Channel updated!', true);
    } else showMsg(d.error || 'Error', false);
  }

  /* ─── URL Edit (legacy) ─── */
  let _urlChId = null;
  function openUrlEdit(id) {
    const ch = allChannels.find(c => c.id === id);
    if (!ch) return;
    _urlChId = id;
    document.getElementById('url-ch-name').textContent = ch.name;
    document.getElementById('url-input').value = ch.stream_url || '';
    document.getElementById('url-modal').classList.add('open');
  }
  async function saveUrl() {
    const url = document.getElementById('url-input').value.trim();
    if (!url) { showMsg('URL cannot be empty', false); return; }
    const r = await fetch('/api/admin/channels/' + _urlChId + '/url', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ stream_url: url })
    });
    const d = await r.json();
    if (d.success) {
      const ch = allChannels.find(c => c.id === _urlChId);
      if (ch) ch.stream_url = url;
      document.getElementById('url-modal').classList.remove('open');
      showMsg('Stream URL updated!', true);
    } else showMsg(d.error || 'Error', false);
  }
  ['url-modal','edit-modal','add-modal','pc-add-modal','pc-access-modal'].forEach(id => {
    document.getElementById(id).addEventListener('click', e => {
      if (e.target.id === id) document.getElementById(id).classList.remove('open');
    });
  });

  /* ─── Add Channel Modal ─── */
  function openAddModal() {
    document.getElementById('add-name').value = '';
    document.getElementById('add-url').value = '';
    document.getElementById('add-cat').value = '';
    document.getElementById('add-country').value = '';
    document.getElementById('add-modal').classList.add('open');
  }
  async function addChannel() {
    const channel_name = document.getElementById('add-name').value.trim();
    const stream_url = document.getElementById('add-url').value.trim();
    const category = document.getElementById('add-cat').value || undefined;
    const country = document.getElementById('add-country').value || undefined;
    if (!channel_name || !stream_url) { showMsg('Name and URL required', false); return; }
    const r = await fetch('/api/admin/channels/new', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ channel_name, stream_url, category, country })
    });
    const d = await r.json();
    if (d.success) {
      allChannels.push({ id: d.channel.id, name: d.channel.channel_name, stream_url: d.channel.stream_url, category: d.channel.category, blocked: false, is_highlighted: false });
      document.getElementById('add-modal').classList.remove('open');
      renderChannels(allChannels);
      updateChStats();
      showMsg('Channel added! ID: ' + d.channel.id, true);
    } else showMsg(d.error || 'Error', false);
  }

  /* ─── Settings ─── */
  async function saveGuestLimit() {
    const val = document.getElementById('guest-range').value;
    const r = await fetch('/api/admin/config', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ key: 'guest_limit_minutes', value: val })
    });
    const d = await r.json();
    if (d.success) {
      document.getElementById('cur-limit').textContent = val + ' minutes';
      showMsg('Guest timer updated to ' + val + ' minutes!', true);
    } else showMsg(d.error || 'Error', false);
  }

  /* ─── Private Channels Tab ─── */
  let allPrivateChannels = [];
  let pcCurrentList = [];
  let pcPage = 1;
  const PC_PER_PAGE = 100;

  async function loadPrivateTab() {
    document.getElementById('pc-list').innerHTML = '<div style="color:#444;text-align:center;padding:20px">Loading...</div>';
    document.getElementById('pc-pagination').innerHTML = '';
    const r = await fetch('/api/admin/private-channels', { headers: { Authorization: 'Bearer ' + token } });
    const d = await r.json();
    allPrivateChannels = d.channels || [];
    document.getElementById('pc-total').textContent = allPrivateChannels.length;
    document.getElementById('pc-access-count').textContent = '—';
    renderPrivateChannels(allPrivateChannels);
  }

  function renderPrivateChannels(list) {
    pcCurrentList = list;
    pcPage = 1;
    renderPcPage();
  }

  function renderPcPage() {
    const el = document.getElementById('pc-list');
    if (!pcCurrentList.length) {
      el.innerHTML = '<div style="color:#444;text-align:center;padding:24px">কোনো private channel নেই। ➕ Add করো।</div>';
      document.getElementById('pc-pagination').innerHTML = '';
      return;
    }
    const totalPages = Math.ceil(pcCurrentList.length / PC_PER_PAGE);
    const start = (pcPage - 1) * PC_PER_PAGE;
    const pageItems = pcCurrentList.slice(start, start + PC_PER_PAGE);
    el.innerHTML = pageItems.map(ch => \`
      <div class="ch-row" id="pc-row-\${ch.id}">
        <div class="ch-row-top">
          <span class="ch-name" title="\${ch.name}" style="flex:1">\${ch.name}</span>
          <span style="font-size:11px;color:#8af;background:#0a1a2a;border:1px solid #1a3a5a;border-radius:4px;padding:2px 8px;flex-shrink:0">🔒 \${ch.category}</span>
          \${ch.description ? '<span style="font-size:11px;color:#555;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100px">' + ch.description + '</span>' : ''}
        </div>
        <div class="ch-row-btns">
          <button class="act-btn act-blue" onclick="openPcAccess(\${ch.id},'\${ch.name.replace(/'/g,'&apos;')}','\${ch.category}')">👥 Access</button>
          <button class="act-btn act-url" onclick="openPcEdit(\${ch.id})">✏️ Edit</button>
          <button class="act-btn act-green" onclick="pcMakePublic(\${ch.id},'\${ch.name.replace(/'/g,'&apos;')}')">📺 Public</button>
          <button class="act-btn act-red" onclick="pcDelete(\${ch.id},'\${ch.name.replace(/'/g,'&apos;')}')">🗑 Delete</button>
        </div>
      </div>
    \`).join('');
    renderPcPagination(totalPages);
  }

  function renderPcPagination(totalPages) {
    const el = document.getElementById('pc-pagination');
    if (totalPages <= 1) { el.innerHTML = ''; return; }
    const pStyle = 'background:#1a1a1a;border:1px solid #2a2a2a;color:#888;padding:5px 10px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;transition:all .15s';
    const pActive = 'background:#e00;border-color:#e00;color:#fff';
    const pDis = 'opacity:0.35;cursor:default';
    let pages = [];
    for (let i = 1; i <= totalPages; i++) {
      if (i === 1 || i === totalPages || (i >= pcPage - 2 && i <= pcPage + 2)) pages.push(i);
      else if (pages[pages.length - 1] !== '...') pages.push('...');
    }
    const pageBtns = pages.map(p =>
      p === '...' ? \`<span style="color:#444;padding:0 3px">…</span>\` :
      \`<button style="\${pStyle};\${p===pcPage?pActive:''}" onclick="pcGoPage(\${p})">\${p}</button>\`
    ).join('');
    el.innerHTML = \`<div style="display:flex;align-items:center;justify-content:center;gap:6px;padding:14px 4px;flex-wrap:wrap">
      <span style="color:#555;font-size:11px;margin-right:4px">Page \${pcPage}/\${totalPages} · \${pcCurrentList.length} channels</span>
      <button style="\${pStyle};\${pcPage===1?pDis:''}" onclick="pcGoPage(\${pcPage-1})" \${pcPage===1?'disabled':''}>← Prev</button>
      \${pageBtns}
      <button style="\${pStyle};\${pcPage===totalPages?pDis:''}" onclick="pcGoPage(\${pcPage+1})" \${pcPage===totalPages?'disabled':''}>Next →</button>
    </div>\`;
  }

  function pcGoPage(p) {
    const total = Math.ceil(pcCurrentList.length / PC_PER_PAGE);
    if (p < 1 || p > total || p === pcPage) return;
    pcPage = p;
    renderPcPage();
    document.getElementById('pc-list').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  document.getElementById('pc-search').addEventListener('input', e => {
    const q = e.target.value.toLowerCase().trim();
    renderPrivateChannels(q ? allPrivateChannels.filter(c => c.name.toLowerCase().includes(q) || (c.category||'').toLowerCase().includes(q)) : allPrivateChannels);
  });

  /* ─── Private Channel CRUD ─── */
  let _pcEditId = null;
  function openPcAdd() {
    _pcEditId = null;
    document.getElementById('pc-modal-title').textContent = '➕ Add Private Channel';
    document.getElementById('pc-name').value = '';
    document.getElementById('pc-url').value = '';
    document.getElementById('pc-cat').value = 'Private';
    document.getElementById('pc-country').value = '';
    document.getElementById('pc-desc').value = '';
    document.getElementById('pc-add-modal').classList.add('open');
  }
  function openPcEdit(id) {
    const ch = allPrivateChannels.find(c => c.id === id);
    if (!ch) return;
    _pcEditId = id;
    document.getElementById('pc-modal-title').textContent = '✏️ Edit — ' + ch.name;
    document.getElementById('pc-name').value = ch.name;
    document.getElementById('pc-url').value = ch.stream_url;
    document.getElementById('pc-cat').value = ch.category || 'Private';
    document.getElementById('pc-country').value = ch.country || '';
    document.getElementById('pc-desc').value = ch.description || '';
    document.getElementById('pc-add-modal').classList.add('open');
  }
  async function savePcChannel() {
    const name = document.getElementById('pc-name').value.trim();
    const stream_url = document.getElementById('pc-url').value.trim();
    const category = document.getElementById('pc-cat').value;
    const country = document.getElementById('pc-country').value || undefined;
    const description = document.getElementById('pc-desc').value.trim();
    if (!name || !stream_url) { showMsg('Name and URL required', false); return; }
    const method = _pcEditId ? 'PUT' : 'POST';
    const url = _pcEditId ? '/api/admin/private-channels/' + _pcEditId : '/api/admin/private-channels';
    const r = await fetch(url, {
      method, headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ name, stream_url, category, country, description })
    });
    const d = await r.json();
    if (d.success || d.channel) {
      document.getElementById('pc-add-modal').classList.remove('open');
      tabLoaded.private = false; loadPrivateTab();
      showMsg(_pcEditId ? 'Channel updated!' : 'Private channel added!', true);
    } else showMsg(d.error || 'Error', false);
  }
  async function pcDelete(id, name) {
    if (!await showConfirm('Delete "' + name + '"? This cannot be undone.', true, '🗑️')) return;
    const r = await fetch('/api/admin/private-channels/' + id, { method: 'DELETE', headers: { Authorization: 'Bearer ' + token } });
    const d = await r.json();
    if (d.success) {
      allPrivateChannels = allPrivateChannels.filter(c => c.id !== id);
      document.getElementById('pc-row-' + id)?.remove();
      document.getElementById('pc-total').textContent = allPrivateChannels.length;
      showMsg('Private channel deleted!', true);
    } else showMsg(d.error || 'Error', false);
  }
  async function pcMakePublic(id, name) {
    if (!await showConfirm('"' + name + '" — main public channels-এ add করবো?', false, '📺')) return;
    const r = await fetch('/api/admin/private-channels/' + id + '/make-public', { method: 'POST', headers: { Authorization: 'Bearer ' + token } });
    const d = await r.json();
    if (d.success) showMsg('"' + name + '" public channels-এ add হয়েছে! (ID: ' + d.publicId + ')', true);
    else showMsg(d.error || 'Error', false);
  }

  /* ─── Private Channel Access ─── */
  let _pcAccessId = null, _pcAccessCat = null;
  async function openPcAccess(id, name, category) {
    try {
      _pcAccessId = id; _pcAccessCat = category;
      document.getElementById('pc-access-title').textContent = '👥 Access — ' + name;
      document.getElementById('pc-access-modal').classList.add('open');
      const sel = document.getElementById('pc-grant-user');
      sel.innerHTML = '<option value="">⏳ Loading...</option>';
      const r = await fetch('/api/admin/users', { headers: { Authorization: 'Bearer ' + token } });
      const d = await r.json();
      allUsers = d.users || [];
      const members = allUsers.filter(u => u.role !== 'admin');
      if (members.length === 0) {
        sel.innerHTML = '<option value="">— কোনো member user নেই —</option>';
      } else {
        sel.innerHTML = '<option value="">— User নির্বাচন করো —</option>' +
          members.map(u => \`<option value="\${u.id}">\${u.email}</option>\`).join('');
      }
      await loadPcAccess(id);
    } catch(e) {
      showMsg('Error: ' + e.message, false);
    }
  }
  async function loadPcAccess(id) {
    try {
      const r = await fetch('/api/admin/private-channels/' + id + '/access', { headers: { Authorization: 'Bearer ' + token } });
      const d = await r.json();
      document.getElementById('pc-cat-label').textContent = d.category || 'Private';
      const chEl = document.getElementById('pc-ch-access-list');
      const catEl = document.getElementById('pc-cat-access-list');
      chEl.innerHTML = (d.channelAccess || []).length
        ? (d.channelAccess || []).map(a => \`
          <div style="display:flex;align-items:center;justify-content:space-between;padding:7px 12px;background:#1a1a1a;border-radius:6px;margin-bottom:4px">
            <span style="font-size:12px;color:#ccc">\${a.email}</span>
            <button class="act-btn act-red" style="font-size:10px;padding:3px 8px" onclick="revokeAccess(\${a.id},'channel')">✕ Remove</button>
          </div>
        \`).join('')
        : '<div style="font-size:12px;color:#444;padding:6px 0">কোনো channel-level access নেই।</div>';
      catEl.innerHTML = (d.categoryAccess || []).length
        ? (d.categoryAccess || []).map(a => \`
          <div style="display:flex;align-items:center;justify-content:space-between;padding:7px 12px;background:#1a1a1a;border-radius:6px;margin-bottom:4px">
            <span style="font-size:12px;color:#ccc">\${a.email}</span>
            <button class="act-btn act-red" style="font-size:10px;padding:3px 8px" onclick="revokeAccess(\${a.id},'category')">✕ Remove</button>
          </div>
        \`).join('')
        : '<div style="font-size:12px;color:#444;padding:6px 0">কোনো category-level access নেই।</div>';
    } catch(e) {
      showMsg('Access load error: ' + e.message, false);
    }
  }
  async function grantPcAccess() {
    const user_id = document.getElementById('pc-grant-user').value;
    const type = document.getElementById('pc-grant-type').value;
    if (!user_id) { showMsg('User নির্বাচন করো', false); return; }
    const body = { user_id, type };
    if (type === 'channel') body.channel_id = _pcAccessId;
    else body.category = _pcAccessCat;
    const r = await fetch('/api/admin/private-channels/access', {
      method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (d.success) { await loadPcAccess(_pcAccessId); showMsg('Access granted!', true); }
    else showMsg(d.error || 'Error', false);
  }
  async function revokeAccess(accessId, type) {
    const r = await fetch('/api/admin/private-channels/access/' + accessId + '?type=' + type, {
      method: 'DELETE', headers: { Authorization: 'Bearer ' + token }
    });
    const d = await r.json();
    if (d.success) { await loadPcAccess(_pcAccessId); showMsg('Access removed!', true); }
    else showMsg(d.error || 'Error', false);
  }
  /* ── Activity Modal ── */
  function fmtDur(secs) {
    if (!secs) return '';
    if (secs < 60) return secs + 's';
    if (secs < 3600) return Math.floor(secs/60) + 'm ' + (secs%60) + 's';
    return Math.floor(secs/3600) + 'h ' + Math.floor((secs%3600)/60) + 'm';
  }
  function fmtDT(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  }
  async function openActivity(uid, email) {
    const modal = document.getElementById('activity-modal');
    const body = document.getElementById('activity-body');
    document.getElementById('activity-user-label').textContent = email;
    body.innerHTML = '<div style="text-align:center;color:#555;padding:20px">Loading...</div>';
    modal.classList.add('open');
    try {
      const r = await fetch('/api/admin/users/' + uid + '/activity', { headers: { Authorization: 'Bearer ' + token } });
      const d = await r.json();
      const events = d.activity || [];
      if (!events.length) { body.innerHTML = '<div style="text-align:center;color:#555;padding:30px">কোনো activity নেই।</div>'; return; }
      body.innerHTML = events.map(ev => {
        if (ev.type === 'login') return \`
          <div class="act-event">
            <div class="act-icon">🔑</div>
            <div class="act-detail">
              <div class="act-title">Login</div>
              <div class="act-time">\${fmtDT(ev.at)}</div>
              \${ev.ip ? '<div class="act-dur">IP: ' + ev.ip + '</div>' : ''}
            </div>
          </div>\`;
        if (ev.type === 'watch') return \`
          <div class="act-event">
            <div class="act-icon">📺</div>
            <div class="act-detail">
              <div class="act-title">\${ev.channel_name || 'Unknown Channel'}</div>
              <div class="act-time">\${fmtDT(ev.at)}</div>
              \${ev.duration_seconds ? '<div class="act-dur">⏱ ' + fmtDur(ev.duration_seconds) + '</div>' : (ev.ended_at ? '' : '<div class="act-dur" style="color:#3f3">🟢 Watching now</div>')}
            </div>
          </div>\`;
        return '';
      }).join('');
    } catch(e) {
      body.innerHTML = '<div style="color:#f66;padding:20px">Error loading activity.</div>';
    }
  }
  document.getElementById('act-close').addEventListener('click', () => {
    document.getElementById('activity-modal').classList.remove('open');
  });
  document.getElementById('activity-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.classList.remove('open');
  });
</script>
<div id="activity-modal">
  <div id="activity-box">
    <div id="activity-box-header">
      <h3>📋 Activity — <span id="activity-user-label"></span></h3>
      <button id="act-close">✕</button>
    </div>
    <div id="activity-body"></div>
  </div>
</div>
</body></html>`);
});

app.get('/googledbd050bd9f076437.html', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send('google-site-verification: googledbd050bd9f076437.html');
});

app.get('/', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MIZ Live TV</title>
  <style>
    ${SHARED_HEAD_STYLES}
    #grid-view { width: 100%; max-width: 960px; }
    .grid-search {
      width: 100%; background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 10px 16px;
      color: #ddd; font-size: 13px; margin-bottom: 16px;
      outline: none; transition: border-color .2s;
    }
    .grid-search::placeholder { color: #444; }
    .grid-search:focus { border-color: #e00; }
    .grid-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    .grid-header h2 { font-size: 13px; font-weight: 600; color: #555; letter-spacing: 1px; text-transform: uppercase; }
    .grid-count { font-size: 12px; color: #444; }
    .grid-channels {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(110px, 1fr));
      gap: 10px;
    }
    .grid-card {
      background: #141414; border: 1px solid #1e1e1e;
      border-radius: 12px; padding: 16px 10px 13px;
      display: flex; flex-direction: column; align-items: center; gap: 10px;
      cursor: pointer; transition: border-color .2s, background .2s, transform .15s, box-shadow .2s;
      text-align: center; position: relative;
    }
    .grid-card:hover { background: #1c1c1c; border-color: #e00; transform: translateY(-3px); box-shadow: 0 6px 22px rgba(220,0,0,.18); }
    .grid-logo {
      width: 56px; height: 56px; border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: 16px; font-weight: 800; color: #fff;
      flex-shrink: 0; letter-spacing: .5px;
    }
    .grid-logo-img {
      width: 56px; height: 56px; border-radius: 50%;
      object-fit: contain; background: #1e1e1e; flex-shrink: 0;
    }
    .grid-name {
      font-size: 11px; font-weight: 600; color: #ccc;
      line-height: 1.3; word-break: break-word;
      display: -webkit-box; -webkit-line-clamp: 2;
      -webkit-box-orient: vertical; overflow: hidden;
    }
    .grid-status-dot {
      position: absolute; top: 10px; right: 10px;
      width: 9px; height: 9px; border-radius: 50%; border: 2px solid #141414;
    }
    .grid-status-dot.online  { background: #33dd77; box-shadow: 0 0 5px #33dd77; }
    .grid-status-dot.offline { background: #444; }
    .no-results { color: #444; font-size: 13px; padding: 16px; text-align: center; }
    /* ── LIVE NOW section ── */
    #live-section {
      width: 100%; max-width: 960px;
      margin-bottom: 22px; display: none;
      background: linear-gradient(135deg, #1a0505 0%, #0f0f0f 100%);
      border: 1px solid #3a0a0a; border-radius: 14px;
      padding: 18px 20px 20px;
    }
    .live-header {
      display: flex; align-items: center; gap: 10px; margin-bottom: 16px;
    }
    .live-pulse {
      width: 9px; height: 9px; border-radius: 50%; background: #e00; flex-shrink: 0;
      animation: livePulse 1.1s ease-in-out infinite;
      box-shadow: 0 0 6px #e00;
    }
    @keyframes livePulse {
      0%, 100% { opacity: 1; transform: scale(1); box-shadow: 0 0 6px #e00; }
      50% { opacity: .4; transform: scale(1.5); box-shadow: 0 0 12px #e00; }
    }
    .live-title {
      font-size: 12px; font-weight: 800; color: #ff3333;
      letter-spacing: 2px; text-transform: uppercase;
    }
    .live-count {
      margin-left: auto; font-size: 11px; color: #555;
    }
    .live-grid {
      display: flex; gap: 10px; flex-wrap: nowrap;
      overflow-x: auto; padding-bottom: 6px;
      scrollbar-width: none; -ms-overflow-style: none;
    }
    .live-grid::-webkit-scrollbar { display: none; }
    .live-card {
      background: #140000; border: 1.5px solid #2a0808;
      border-radius: 12px; padding: 12px 10px 10px;
      display: flex; flex-direction: column; align-items: center; gap: 7px;
      cursor: pointer; transition: border-color .15s, background .15s, transform .15s;
      text-align: center; position: relative;
      width: 95px; min-width: 85px; flex-shrink: 0;
    }
    .live-card:hover { background: #1e0505; border-color: #e00; transform: translateY(-3px); box-shadow: 0 4px 20px rgba(220,0,0,.25); }
    .live-card-badge {
      position: absolute; top: 6px; right: 6px;
      background: #e00; color: #fff; font-size: 8px; font-weight: 900;
      letter-spacing: .8px; border-radius: 4px; padding: 2px 5px;
      line-height: 1.4;
    }
    /* Category pills */
    .cat-pills { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }
    .cat-pill { background:#141414; border:1px solid #1e1e1e; border-radius:20px; padding:7px 15px; color:#666; font-size:12px; font-weight:600; cursor:pointer; transition:all .15s; white-space:nowrap; outline:none; }
    .cat-pill:hover { border-color:#444; color:#ccc; background:#1c1c1c; }
    .cat-pill.active { background:#e00; border-color:#e00; color:#fff; box-shadow:0 2px 10px rgba(220,0,0,.35); }
    /* Country filter */
    .filter-bar {
      display: flex; gap: 10px; margin-bottom: 16px; align-items: center; flex-wrap: wrap;
    }
    .filter-select {
      background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 8px 32px 8px 12px;
      color: #ccc; font-size: 13px; font-weight: 600;
      cursor: pointer; outline: none; flex: 1; min-width: 140px;
      appearance: none; -webkit-appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='%23888'%3E%3Cpath d='M7 10l5 5 5-5z'/%3E%3C/svg%3E");
      background-repeat: no-repeat; background-position: right 10px center;
      transition: border-color .15s;
    }
    .filter-select:focus, .filter-select:hover { border-color: #e00; color: #fff; }
    .filter-select option { background: #1a1a1a; color: #ccc; }
    .auth-btn {
      background: #1a1a1a; color: #ccc; border: 1px solid #333;
      border-radius: 8px; padding: 8px 14px; font-size: 12px; font-weight: 600;
      cursor: pointer; text-decoration: none; display: flex; align-items: center; gap: 6px;
      transition: all .15s; white-space: nowrap;
    }
    .auth-btn:hover { background: #e00; color: #fff; border-color: #e00; }
    .auth-btn.red { background: #e00; color: #fff; border-color: #e00; }
    .user-menu { position: relative; }
    .user-drop {
      position: absolute; right: 0; top: calc(100% + 8px);
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 8px; min-width: 185px; z-index: 200; overflow: hidden; display: none;
    }
    .user-drop.open { display: block; }
    .user-email-line { padding: 10px 14px; font-size: 11px; color: #666; border-bottom: 1px solid #2a2a2a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .user-item {
      padding: 10px 14px; font-size: 13px; color: #ccc;
      cursor: pointer; transition: background .1s; display: block; text-decoration: none;
    }
    .user-item:hover { background: #2a2a2a; color: #fff; }
    .user-item.danger:hover { background: #e00; }
    .g-overlay {
      display: none; position: fixed; inset: 0;
      background: rgba(0,0,0,.88); z-index: 9000;
      align-items: center; justify-content: center;
    }
    .g-overlay.show { display: flex; }
    .g-modal {
      background: #141414; border: 1px solid #2a2a2a;
      border-radius: 16px; padding: 36px 28px; max-width: 360px;
      width: 90%; text-align: center;
    }
    .g-modal h3 { font-size: 20px; font-weight: 700; margin-bottom: 10px; }
    .g-modal p { color: #888; font-size: 13px; margin-bottom: 24px; line-height: 1.65; }
    .g-btns { display: flex; gap: 10px; justify-content: center; }
    .g-btn {
      padding: 11px 22px; border-radius: 8px; font-size: 14px;
      font-weight: 700; cursor: pointer; border: none; text-decoration: none; display: inline-block;
    }
    .g-login { background: #e00; color: #fff; }
    .g-login:hover { background: #c00; }
    .g-signup { background: #222; color: #ccc; border: 1px solid #333 !important; }
    .g-signup:hover { background: #2a2a2a; }
  </style>
</head>
<body>
  <header>
    <h1 id="miz-logo" style="cursor:pointer;user-select:none;-webkit-user-select:none;display:flex;align-items:center;gap:8px">${LOGO_FULL_HTML}</h1>
    <div id="auth-area"></div>
  </header>
  <div id="live-section">
    <div class="live-header">
      <span class="live-pulse"></span>
      <span class="live-title">Live Now</span>
      <span class="live-count" id="live-count"></span>
    </div>
    <div class="live-grid" id="live-grid"></div>
  </div>
  <div id="grid-view">
    <input class="grid-search" id="grid-search" type="text" placeholder="🔍  Search channels..." autocomplete="off" />
    <div id="cat-pills" class="cat-pills"></div>
    <div class="filter-bar">
      <select class="filter-select" id="country-select"></select>
    </div>
    <div class="grid-header">
      <h2 id="grid-cat-label">Channels</h2>
      <span class="grid-count" id="grid-count"></span>
    </div>
    <div class="grid-channels" id="grid-channels"></div>
  </div>
  <script>
    const gridChannels = document.getElementById('grid-channels');
    const gridSearch   = document.getElementById('grid-search');
    const gridCount    = document.getElementById('grid-count');
    let allChannels = [];
    let activeCategory = 'All';
    let activeCountry = 'All';

    const CATEGORIES = [
      { key: 'All',           label: '🔍 All' },
      { key: 'Bangla',        label: '🇧🇩 Bangla' },
      { key: 'News',          label: '📰 News' },
      { key: 'Movies',        label: '🎬 Movies' },
      { key: 'Music',         label: '🎵 Music' },
      { key: 'Kids',          label: '👶 Kids' },
      { key: 'Sports',        label: '⚽ Sports' },
      { key: 'International', label: '🌍 International' },
    ];

    const CAT_RULES = [
      { key: 'Bangla', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga',' ntv','dbc','ekushey','deshi','sangeet','atn','channel i','channel 9','channel 24','sa tv','btv','sangsad','independent tv','star news','rongeen','dd bangla','republic bangla','kolkata tv','tv9 bangla','abp ananda','zee 24 ghanta','news18 bangla','r bangla','enter10','colors bangla','sony aath','jalsha','g-series','g-serise','aakash aath','ananda tv','protidin','24 ghanta','24 ghonto','24 kalak','24 taas','jago','dhoom','baalle','baallee','srk tv','bengali beats','gopal bhar'] },
      { key: 'News',    words: ['news','ndtv','republic','wion','cnn','bbc news','dw ','france 24','rt now','rtnews','rtd','sky news','cna','nhk','trt','press tv','al-jazeera','aljazeera','india today','mirror now','cgtn','times now','zee news','india tv','times of india','breaking news','oannews','oan ','channel s','fox news','abc news','global news','t global','cnbc','iran international','argus','kalinga','nandighosha','sudarshan','jan tv','jan tv','jantantra','network 10','news 1','news18','news 11','news max','newsmax','abn','akash news','independent'] },
      { key: 'Movies',  words: ['movie','cinema','film','bollywood','hollywood','goldmines','afriwood','artflix','biz cinema','cine ','classique','filmrise','moviesphere','mytime movie','persiana cinema','runtime','zylo','ifilm','grand cinema','gold star','afra film','home plus','meta film','sl 1','sl 2','sl one','sl two','maverick','south movies','hindi movies','zee action','zee anmol cinema','manoranjan movies','star movies','manoranjan prime','manoranjan grend','b4u kadak','sheemaroo','big magic'] },
      { key: 'Music',   words: ['music','beats','9xm','joo music','8xm','dhoom music','atn music','yrfmusic','yrf music','hindi hits','e24','music india','party universe'] },
      { key: 'Kids',    words: ['kids','cartoon','junior','motu','doraemon','pbs','zoo moo','tom &','jungle book','gopal bhar','cbeebies','happykids','buddy','funny junior','lucky family','smarty','nikki','joy','screem','buddy star'] },
      { key: 'Sports',  words: ['sport','dd sport','cricket','football','soccer','tennis','basketball','wrestling','racing','olympic'] },
    ];

    function categorize(name) {
      const n = name.toLowerCase();
      for (const rule of CAT_RULES) {
        if (rule.words.some(w => n.includes(w))) return rule.key;
      }
      return 'International';
    }

    const LOGO_COLORS = ['#c0392b','#8e44ad','#2980b9','#16a085','#d35400','#c0392b','#1a5276','#6c3483','#1e8449','#b7950b'];
    function logoColor(name) {
      let h = 0;
      for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) & 0xffff;
      return LOGO_COLORS[h % LOGO_COLORS.length];
    }
    function initials(name) {
      return name.split(/\\s+/).slice(0,2).map(w => w[0]||'').join('').toUpperCase() || '?';
    }
    function buildLogoSources(ch) {
      const srcs = [];
      if (ch.logo) srcs.push({ type: 'url', url: ch.logo });
      if (ch.domain) srcs.push({ type: 'url', url: 'https://logo.clearbit.com/' + ch.domain });
      srcs.push({ type: 'api', url: '/api/logo-fallback?name=' + encodeURIComponent(ch.channel_name) });
      return srcs;
    }
    function attachLogo(img, fallbackEl, ch) {
      const srcs = buildLogoSources(ch);
      let idx = 0;
      async function tryNext() {
        if (idx >= srcs.length) { img.style.display = 'none'; fallbackEl.style.display = 'flex'; return; }
        const src = srcs[idx++];
        if (src.type === 'url') { img.src = src.url; }
        else {
          try {
            const r = await fetch(src.url); const d = await r.json();
            if (d.logo) { img.src = d.logo; }
            else { img.style.display = 'none'; fallbackEl.style.display = 'flex'; }
          } catch(_) { img.style.display = 'none'; fallbackEl.style.display = 'flex'; }
        }
      }
      img.onerror = tryNext;
      fallbackEl.style.display = 'none';
      tryNext();
    }

    function renderGrid(list) {
      gridChannels.innerHTML = '';
      if (!list.length) {
        const empty = document.createElement('div');
        empty.className = 'no-results';
        empty.style.gridColumn = '1/-1';
        empty.textContent = 'No channels found.';
        gridChannels.appendChild(empty);
        return;
      }
      gridCount.textContent = list.length + ' channels';
      const frag = document.createDocumentFragment();
      list.forEach(ch => {
        const card = document.createElement('div');
        card.className = 'grid-card';
        const dot = document.createElement('span');
        dot.className = 'grid-status-dot ' + (ch.status === 'Online' ? 'online' : 'offline');
        dot.title = ch.status || 'Unknown';
        card.appendChild(dot);
        const fallback = document.createElement('div');
        fallback.className = 'grid-logo';
        fallback.style.background = logoColor(ch.channel_name);
        fallback.textContent = initials(ch.channel_name);
        const img = document.createElement('img');
        img.className = 'grid-logo-img';
        img.alt = ch.channel_name;
        card.appendChild(img);
        card.appendChild(fallback);
        attachLogo(img, fallback, ch);
        const nameEl = document.createElement('div');
        nameEl.className = 'grid-name';
        nameEl.textContent = ch.channel_name;
        card.appendChild(nameEl);
        card.addEventListener('click', () => {
          window.location.href = '/watch?ch=' + ch.id;
        });
        frag.appendChild(card);
      });
      gridChannels.appendChild(frag);
    }

    function countryFlag(code) {
      if (!code || code.length !== 2) return '🌍';
      const b = 0x1F1E6 - 65;
      return String.fromCodePoint(code.toUpperCase().charCodeAt(0)+b, code.toUpperCase().charCodeAt(1)+b);
    }
    const COUNTRY_NAMES = {BD:'Bangladesh',IN:'India',GB:'UK',US:'USA',PK:'Pakistan',AE:'UAE',SA:'Saudi Arabia',QA:'Qatar',TR:'Turkey',FR:'France',DE:'Germany',RU:'Russia',IR:'Iran',EG:'Egypt',AF:'Afghanistan',NP:'Nepal',LK:'Sri Lanka',MM:'Myanmar',JP:'Japan',CN:'China',KR:'Korea',ID:'Indonesia',MY:'Malaysia',TH:'Thailand',IT:'Italy',ES:'Spain',NG:'Nigeria',GH:'Ghana'};

    function buildCountrySelect(list) {
      const sel = document.getElementById('country-select');
      if (!sel) return;
      const prev = activeCountry;
      sel.innerHTML = '';
      const allOpt = document.createElement('option');
      allOpt.value = 'All'; allOpt.textContent = '🌍 All Countries';
      sel.appendChild(allOpt);
      const counts = {};
      list.forEach(c => { if (c.country) counts[c.country] = (counts[c.country]||0)+1; });
      Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,30).forEach(([code, count]) => {
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = countryFlag(code) + ' ' + (COUNTRY_NAMES[code]||code) + ' (' + count + ')';
        sel.appendChild(opt);
      });
      sel.value = (prev !== 'All' && sel.querySelector('option[value="'+prev+'"]')) ? prev : 'All';
      activeCountry = sel.value;
      if (!sel._hasListener) {
        sel._hasListener = true;
        sel.addEventListener('change', () => {
          activeCountry = sel.value;
          renderGrid(getFiltered());
        });
      }
    }

    function getFiltered() {
      const q = gridSearch.value.toLowerCase().trim();
      let list = allChannels;
      if (activeCategory !== 'All') {
        list = list.filter(c => categorize(c.channel_name) === activeCategory);
      }
      if (activeCountry !== 'All') {
        list = list.filter(c => c.country === activeCountry);
      }
      if (q) {
        list = list.filter(c => c.channel_name.toLowerCase().includes(q));
      }
      list = list.slice().sort((a, b) => (a.locked ? 1 : 0) - (b.locked ? 1 : 0));
      return list;
    }

    function updateCatLabel() {
      const label = CATEGORIES.find(c => c.key === activeCategory)?.label || 'Channels';
      document.getElementById('grid-cat-label').textContent =
        activeCategory === 'All' ? 'CHANNELS' : label.replace(/^\S+\s/, '').toUpperCase();
    }

    function buildCategoryTabs() {
      const container = document.getElementById('cat-pills');
      if (!container) return;
      container.innerHTML = '';
      CATEGORIES.forEach(cat => {
        const btn = document.createElement('button');
        btn.className = 'cat-pill' + (cat.key === 'All' ? ' active' : '');
        btn.dataset.cat = cat.key;
        btn.textContent = cat.label;
        btn.addEventListener('click', () => {
          container.querySelectorAll('.cat-pill').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          activeCategory = cat.key;
          activeCountry = 'All';
          const catFiltered = activeCategory === 'All' ? allChannels : allChannels.filter(c => categorize(c.channel_name) === activeCategory);
          buildCountrySelect(catFiltered);
          updateCatLabel();
          renderGrid(getFiltered());
        });
        container.appendChild(btn);
      });
    }

    gridSearch.addEventListener('input', () => {
      renderGrid(getFiltered());
    });

    function renderLiveSection(channels) {
      const section = document.getElementById('live-section');
      const grid = document.getElementById('live-grid');
      const countEl = document.getElementById('live-count');
      const liveChannels = channels.filter(ch => ch.is_highlighted);
      if (!liveChannels.length) { section.style.display = 'none'; return; }
      section.style.display = 'block';
      if (countEl) countEl.textContent = liveChannels.length + ' channel' + (liveChannels.length > 1 ? 's' : '');
      grid.innerHTML = '';
      const frag = document.createDocumentFragment();
      liveChannels.forEach(ch => {
        const card = document.createElement('div');
        card.className = 'live-card';
        const badge = document.createElement('div');
        badge.className = 'live-card-badge';
        badge.textContent = 'LIVE';
        const fallback = document.createElement('div');
        fallback.className = 'grid-logo';
        fallback.style.background = logoColor(ch.channel_name);
        fallback.textContent = initials(ch.channel_name);
        const img = document.createElement('img');
        img.className = 'grid-logo-img';
        img.alt = ch.channel_name;
        const nameEl = document.createElement('div');
        nameEl.className = 'grid-name';
        nameEl.textContent = ch.channel_name;
        card.appendChild(badge);
        card.appendChild(img);
        card.appendChild(fallback);
        card.appendChild(nameEl);
        attachLogo(img, fallback, ch);
        card.addEventListener('click', () => { window.location.href = '/watch?ch=' + ch.id; });
        frag.appendChild(card);
      });
      grid.appendChild(frag);
    }

    async function loadChannels() {
      try {
        const tok = localStorage.getItem('miz_token');
        const headers = tok ? { Authorization: 'Bearer ' + tok } : {};
        const r = await fetch('/channels', { headers });
        const data = await r.json();
        allChannels = data.channels;
        renderLiveSection(allChannels);
        buildCategoryTabs();
        buildCountrySelect(allChannels);
        updateCatLabel();
        renderGrid(getFiltered());
      } catch(e) {
        gridChannels.innerHTML = '<div class="no-results" style="grid-column:1/-1;color:#a33;">Failed to load channels.</div>';
      }
    }

    /* ── Auth & Guest Timer ────────────────────── */
    const GUEST_LIMIT = ${(parseInt(appConfig.guest_limit_minutes) || 5) * 60 * 1000};
    const GUEST_MIN_LABEL = '${parseInt(appConfig.guest_limit_minutes) || 5}';
    let _guestTimer = null;
    let _guestUsed = parseInt(localStorage.getItem('miz_guest_time') || '0');
    let _isAuth = false;

    function showGuestModal() { document.getElementById('g-overlay').classList.add('show'); }
    function startGuestTimer() {
      if (_isAuth || _guestTimer) return;
      const start = Date.now() - _guestUsed;
      _guestTimer = setInterval(() => {
        _guestUsed = Date.now() - start;
        localStorage.setItem('miz_guest_time', _guestUsed);
        if (_guestUsed >= GUEST_LIMIT) { clearInterval(_guestTimer); showGuestModal(); }
      }, 1000);
    }
    function renderAuthUI(user, role) {
      const area = document.getElementById('auth-area');
      if (!area) return;
      if (user) {
        _isAuth = true;
        const isAdmin = role === 'admin';
        area.innerHTML = '<div class="user-menu">' +
          '<button class="auth-btn" id="user-btn">👤 ' + user.email.split('@')[0] + ' ▾</button>' +
          '<div class="user-drop" id="user-drop">' +
          '<div class="user-email-line">' + user.email + '</div>' +
          (isAdmin ? '<a href="/admin" class="user-item">⚙️ Admin Panel</a>' : '') +
          '<div class="user-item danger" id="logout-btn">🚪 Logout</div></div></div>';
        document.getElementById('user-btn').addEventListener('click', e => {
          e.stopPropagation();
          document.getElementById('user-drop').classList.toggle('open');
        });
        document.addEventListener('click', () => { const d = document.getElementById('user-drop'); if(d) d.classList.remove('open'); });
        document.getElementById('logout-btn').addEventListener('click', () => {
          localStorage.removeItem('miz_token'); localStorage.removeItem('miz_user');
          localStorage.removeItem('miz_refresh'); localStorage.removeItem('miz_guest_time');
          window.location.reload();
        });
      } else {
        area.innerHTML = '<a href="/login" class="auth-btn red">🔑 Login / Sign Up</a>';
        startGuestTimer();
      }
    }
    async function initAuth() {
      const token = localStorage.getItem('miz_token');
      let user = null; let role = 'member';
      if (token) {
        try {
          const r = await fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } });
          const d = await r.json();
          if (d.user) { user = d.user; role = d.user.role; }
          else { localStorage.removeItem('miz_token'); }
        } catch(_) {}
      }
      renderAuthUI(user, role);
      const tok = localStorage.getItem('miz_token');
      function sendPresence() {
        fetch('/api/track/presence', {
          method: 'POST',
          headers: tok ? { Authorization: 'Bearer ' + tok } : {}
        }).catch(()=>{});
      }
      sendPresence();
      setInterval(sendPresence, 60000);
    }
    initAuth();
    loadChannels();
  </script>

  <!-- Guest limit modal -->
  <div class="g-overlay" id="g-overlay">
    <div class="g-modal">
      <h3>⏱ ${parseInt(appConfig.guest_limit_minutes)||5} মিনিট শেষ!</h3>
      <p>Guest হিসেবে মাত্র ${parseInt(appConfig.guest_limit_minutes)||5} মিনিট দেখতে পারবেন।<br>আরো দেখতে Login বা Sign Up করুন — সম্পূর্ণ বিনামূল্যে!</p>
      <div class="g-btns">
        <a href="/login" class="g-btn g-login">🔑 Login</a>
        <a href="/signup" class="g-btn g-signup">📝 Sign Up</a>
      </div>
    </div>
  </div>

  <!-- Private PIN modal -->
  <div id="pin-overlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.92);z-index:99999;align-items:center;justify-content:center">
    <div id="pin-box" style="background:#111;border:1px solid #222;border-radius:20px;padding:36px 32px;width:90%;max-width:300px;text-align:center">
      <div style="font-size:13px;color:#666;margin-bottom:16px;letter-spacing:1px">🔒 ENTER PIN</div>
      <div id="pin-dots" style="display:flex;gap:12px;justify-content:center;margin-bottom:10px">
        <div class="pdot"></div><div class="pdot"></div><div class="pdot"></div><div class="pdot"></div><div class="pdot"></div><div class="pdot"></div>
      </div>
      <div id="pin-msg" style="height:20px;font-size:12px;color:#e44;margin-bottom:16px;transition:opacity .2s;opacity:0"></div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;max-width:220px;margin:0 auto">
        <button class="pkey" data-k="1">1</button>
        <button class="pkey" data-k="2">2</button>
        <button class="pkey" data-k="3">3</button>
        <button class="pkey" data-k="4">4</button>
        <button class="pkey" data-k="5">5</button>
        <button class="pkey" data-k="6">6</button>
        <button class="pkey" data-k="7">7</button>
        <button class="pkey" data-k="8">8</button>
        <button class="pkey" data-k="9">9</button>
        <button class="pkey" style="visibility:hidden" data-k=""></button>
        <button class="pkey" data-k="0">0</button>
        <button class="pkey" data-k="del">⌫</button>
      </div>
    </div>
  </div>
  <style>
    .pdot{width:14px;height:14px;border-radius:50%;background:#2a2a2a;border:2px solid #444;transition:background .15s}
    .pdot.filled{background:#e00;border-color:#e00}
    .pdot.wrong{background:#e44;border-color:#e44}
    .pkey{background:#1a1a1a;border:1px solid #2a2a2a;color:#ddd;font-size:20px;font-weight:600;border-radius:12px;padding:14px 0;cursor:pointer;transition:background .1s}
    .pkey:hover{background:#2a2a2a}
    .pkey:active{background:#333}
    @keyframes pinShake{0%,100%{transform:translateX(0)}20%{transform:translateX(-8px)}40%{transform:translateX(8px)}60%{transform:translateX(-6px)}80%{transform:translateX(6px)}}
    .pin-shake{animation:pinShake .4s ease}
  </style>
  <script>
    (function() {
      const overlay = document.getElementById('pin-overlay');
      overlay.style.display = 'flex';
      overlay.style.display = 'none';
      let pin = '';
      let busy = false;
      let longPressTimer = null;
      const LONG_MS = 1500;

      function startLongPress() {
        longPressTimer = setTimeout(() => { openPinModal(); }, LONG_MS);
      }
      function cancelLongPress() { clearTimeout(longPressTimer); }

      const logo = document.getElementById('miz-logo');
      logo.addEventListener('mousedown', startLongPress);
      logo.addEventListener('mouseup', cancelLongPress);
      logo.addEventListener('mouseleave', cancelLongPress);
      logo.addEventListener('touchstart', e => { e.preventDefault(); startLongPress(); }, { passive: false });
      logo.addEventListener('touchend', cancelLongPress);
      logo.addEventListener('touchcancel', cancelLongPress);

      function openPinModal() {
        const tok = localStorage.getItem('miz_token');
        if (!tok) return;
        pin = ''; busy = false;
        updateDots();
        showMsg('');
        overlay.style.display = 'flex';
      }
      function closePinModal() { overlay.style.display = 'none'; pin = ''; busy = false; updateDots(); showMsg(''); }

      function showMsg(txt) {
        const el = document.getElementById('pin-msg');
        el.textContent = txt;
        el.style.opacity = txt ? '1' : '0';
      }

      function updateDots(wrong) {
        document.querySelectorAll('.pdot').forEach((d, i) => {
          d.classList.toggle('filled', !wrong && i < pin.length);
          d.classList.toggle('wrong', !!wrong && i < 6);
        });
      }

      function shakeAndReset(msg) {
        updateDots(true);
        const box = document.getElementById('pin-box');
        box.classList.add('pin-shake');
        showMsg(msg || '❌ Wrong PIN!');
        setTimeout(() => {
          box.classList.remove('pin-shake');
          pin = '';
          updateDots();
        }, 500);
      }

      async function submitPin() {
        if (busy) return;
        const tok = localStorage.getItem('miz_token');
        if (!tok) { closePinModal(); return; }
        busy = true;
        showMsg('⏳ Verifying...');
        try {
          const r = await fetch('/api/private/verify-pin', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tok },
            body: JSON.stringify({ pin })
          });
          if (r.status === 401) {
            closePinModal();
            alert('Session expired. Please login again.');
            window.location.href = '/login';
            return;
          }
          const d = await r.json();
          if (d.ok) {
            sessionStorage.setItem('miz_private_ok', '1');
            closePinModal();
            window.location.href = '/private-tv';
          } else {
            busy = false;
            shakeAndReset('❌ Wrong PIN! Try again.');
          }
        } catch(_) { busy = false; shakeAndReset('❌ Connection error!'); }
      }

      document.querySelectorAll('.pkey').forEach(btn => {
        btn.addEventListener('click', () => {
          if (busy) return;
          const k = btn.dataset.k;
          if (k === 'del') { pin = pin.slice(0, -1); showMsg(''); updateDots(); }
          else if (k !== '' && pin.length < 6) {
            pin += k; updateDots();
            if (pin.length === 6) submitPin();
          }
        });
      });

      overlay.addEventListener('click', e => { if (e.target === overlay) closePinModal(); });
    })();
  </script>
</body>
</html>`);
});

app.get('/private-tv', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MIZ Live TV</title>
  <style>
    ${SHARED_HEAD_STYLES}
    #grid-view { width: 100%; max-width: 960px; }
    .grid-search {
      width: 100%; background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 10px 16px;
      color: #ddd; font-size: 13px; margin-bottom: 16px;
      outline: none; transition: border-color .2s;
    }
    .grid-search::placeholder { color: #444; }
    .grid-search:focus { border-color: #e00; }
    .grid-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    .grid-header h2 { font-size: 13px; font-weight: 600; color: #555; letter-spacing: 1px; text-transform: uppercase; }
    .grid-count { font-size: 12px; color: #444; }
    .grid-channels {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(110px, 1fr));
      gap: 10px;
    }
    .grid-card {
      background: #141414; border: 1px solid #1e1e1e;
      border-radius: 12px; padding: 16px 10px 13px;
      display: flex; flex-direction: column; align-items: center; gap: 10px;
      cursor: pointer; transition: border-color .2s, background .2s, transform .15s, box-shadow .2s;
      text-align: center; position: relative;
    }
    .grid-card:hover { background: #1c1c1c; border-color: #e00; transform: translateY(-3px); box-shadow: 0 6px 22px rgba(220,0,0,.18); }
    .grid-logo {
      width: 56px; height: 56px; border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: 16px; font-weight: 800; color: #fff;
      flex-shrink: 0; letter-spacing: .5px;
    }
    .grid-logo-img {
      width: 56px; height: 56px; border-radius: 50%;
      object-fit: contain; background: #1e1e1e; flex-shrink: 0;
    }
    .grid-name {
      font-size: 11px; font-weight: 600; color: #ccc;
      line-height: 1.3; word-break: break-word;
      display: -webkit-box; -webkit-line-clamp: 2;
      -webkit-box-orient: vertical; overflow: hidden;
    }
    .grid-status-dot {
      position: absolute; top: 10px; right: 10px;
      width: 9px; height: 9px; border-radius: 50%; border: 2px solid #141414;
    }
    .grid-status-dot.online  { background: #33dd77; box-shadow: 0 0 5px #33dd77; }
    .grid-status-dot.offline { background: #444; }
    .no-results { color: #444; font-size: 13px; padding: 16px; text-align: center; }
    /* Category pills */
    .cat-pills { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }
    .cat-pill { background:#141414; border:1px solid #1e1e1e; border-radius:20px; padding:7px 15px; color:#666; font-size:12px; font-weight:600; cursor:pointer; transition:all .15s; white-space:nowrap; outline:none; }
    .cat-pill:hover { border-color:#444; color:#ccc; background:#1c1c1c; }
    .cat-pill.active { background:#e00; border-color:#e00; color:#fff; box-shadow:0 2px 10px rgba(220,0,0,.35); }
    /* Country filter */
    .filter-bar {
      display: flex; gap: 10px; margin-bottom: 16px; align-items: center; flex-wrap: wrap;
    }
    .filter-select {
      background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 8px 32px 8px 12px;
      color: #ccc; font-size: 13px; font-weight: 600;
      cursor: pointer; outline: none; flex: 1; min-width: 140px;
      appearance: none; -webkit-appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='%23888'%3E%3Cpath d='M7 10l5 5 5-5z'/%3E%3C/svg%3E");
      background-repeat: no-repeat; background-position: right 10px center;
      transition: border-color .15s;
    }
    .filter-select:focus, .filter-select:hover { border-color: #e00; color: #fff; }
    .filter-select option { background: #1a1a1a; color: #ccc; }
    .auth-btn {
      background: #1a1a1a; color: #ccc; border: 1px solid #333;
      border-radius: 8px; padding: 8px 14px; font-size: 12px; font-weight: 600;
      cursor: pointer; text-decoration: none; display: flex; align-items: center; gap: 6px;
      transition: all .15s; white-space: nowrap;
    }
    .auth-btn:hover { background: #e00; color: #fff; border-color: #e00; }
    .user-menu { position: relative; }
    .user-drop {
      position: absolute; right: 0; top: calc(100% + 8px);
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 8px; min-width: 185px; z-index: 200; overflow: hidden; display: none;
    }
    .user-drop.open { display: block; }
    .user-email-line { padding: 10px 14px; font-size: 11px; color: #666; border-bottom: 1px solid #2a2a2a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .user-item { padding: 10px 14px; font-size: 13px; color: #ccc; cursor: pointer; transition: background .1s; display: block; text-decoration: none; }
    .user-item:hover { background: #2a2a2a; color: #fff; }
    .user-item.danger:hover { background: #e00; }
  </style>
</head>
<body>
  <header>
    <h1 style="display:flex;align-items:center;gap:8px">${LOGO_FULL_HTML}</h1>
    <div id="auth-area"></div>
  </header>
  <div id="grid-view">
    <input class="grid-search" id="grid-search" type="text" placeholder="🔍  Search channels..." autocomplete="off" />
    <div id="cat-pills" class="cat-pills"></div>
    <div class="filter-bar">
      <select class="filter-select" id="country-select"></select>
    </div>
    <div class="grid-header">
      <h2 id="grid-cat-label">Channels</h2>
      <span class="grid-count" id="grid-count"></span>
    </div>
    <div class="grid-channels" id="grid-channels"></div>
    <div id="grid-pagination"></div>
  </div>
  <script>
    (function() {
      /* ── Security gate ── */
      const tok = localStorage.getItem('miz_token');
      if (!tok) { window.location.replace('/'); throw new Error('blocked'); }
    })();

    /* ── Grid ── */
    const gridChannels = document.getElementById('grid-channels');
    const gridSearch   = document.getElementById('grid-search');
    const gridCount    = document.getElementById('grid-count');
    let allChannels = [];
    let activeCategory = 'All';
    let activeCountry = 'All';
    let tvPage = 1;
    const TV_PER_PAGE = 100;
    let tvFilteredList = [];

    const LOGO_COLORS = ['#c0392b','#8e44ad','#2980b9','#16a085','#d35400','#c0392b','#1a5276','#6c3483','#1e8449','#b7950b'];
    function logoColor(name) {
      let h = 0;
      for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) & 0xffff;
      return LOGO_COLORS[h % LOGO_COLORS.length];
    }
    function initials(name) {
      return name.split(/\\s+/).slice(0,2).map(w => w[0]||'').join('').toUpperCase() || '?';
    }
    function attachLogo(img, fallbackEl, ch) {
      const srcs = [];
      if (ch.logo) srcs.push({ type: 'url', url: ch.logo });
      srcs.push({ type: 'api', url: '/api/logo-fallback?name=' + encodeURIComponent(ch.name) });
      let idx = 0;
      async function tryNext() {
        if (idx >= srcs.length) { img.style.display = 'none'; fallbackEl.style.display = 'flex'; return; }
        const src = srcs[idx++];
        if (src.type === 'url') { img.src = src.url; }
        else {
          try {
            const r = await fetch(src.url); const d = await r.json();
            if (d.logo) { img.src = d.logo; }
            else { img.style.display = 'none'; fallbackEl.style.display = 'flex'; }
          } catch(_) { img.style.display = 'none'; fallbackEl.style.display = 'flex'; }
        }
      }
      img.onerror = tryNext;
      fallbackEl.style.display = 'none';
      tryNext();
    }

    function renderGrid(list) {
      tvFilteredList = list;
      tvPage = 1;
      renderTvPage();
    }

    function renderTvPage() {
      gridChannels.innerHTML = '';
      const pgEl = document.getElementById('grid-pagination');
      if (!tvFilteredList.length) {
        const empty = document.createElement('div');
        empty.className = 'no-results';
        empty.style.gridColumn = '1/-1';
        empty.textContent = 'No channels found.';
        gridChannels.appendChild(empty);
        if (pgEl) pgEl.innerHTML = '';
        return;
      }
      const totalPages = Math.ceil(tvFilteredList.length / TV_PER_PAGE);
      const start = (tvPage - 1) * TV_PER_PAGE;
      const pageItems = tvFilteredList.slice(start, start + TV_PER_PAGE);
      gridCount.textContent = tvFilteredList.length + ' channels · Page ' + tvPage + '/' + totalPages;
      const frag = document.createDocumentFragment();
      pageItems.forEach(ch => {
        const card = document.createElement('div');
        card.className = 'grid-card';
        const dot = document.createElement('span');
        dot.className = 'grid-status-dot ' + (ch.status === 'Online' ? 'online' : 'offline');
        dot.title = ch.status || 'Unknown';
        card.appendChild(dot);
        const fallback = document.createElement('div');
        fallback.className = 'grid-logo';
        fallback.style.background = logoColor(ch.name);
        fallback.textContent = initials(ch.name);
        const img = document.createElement('img');
        img.className = 'grid-logo-img';
        img.alt = ch.name;
        card.appendChild(img);
        card.appendChild(fallback);
        attachLogo(img, fallback, ch);
        const nameEl = document.createElement('div');
        nameEl.className = 'grid-name';
        nameEl.textContent = ch.name;
        card.appendChild(nameEl);
        card.addEventListener('click', () => { console.log('[PTV] clicking channel id=' + ch.id + ' type=' + typeof ch.id + ' name=' + ch.name); window.location.href = '/private-watch?ch=' + ch.id; });
        frag.appendChild(card);
      });
      gridChannels.appendChild(frag);
      renderTvPagination(totalPages);
    }

    function renderTvPagination(totalPages) {
      const pgEl = document.getElementById('grid-pagination');
      if (!pgEl) return;
      if (totalPages <= 1) { pgEl.innerHTML = ''; return; }
      const pStyle = 'background:#1a1a1a;border:1px solid #2a2a2a;color:#888;padding:7px 12px;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;transition:all .15s';
      const pActive = 'background:#e00;border-color:#e00;color:#fff';
      const pDis = 'opacity:0.35;cursor:default;pointer-events:none';
      let pages = [];
      for (let i = 1; i <= totalPages; i++) {
        if (i === 1 || i === totalPages || (i >= tvPage - 2 && i <= tvPage + 2)) pages.push(i);
        else if (pages[pages.length - 1] !== '...') pages.push('...');
      }
      const pageBtns = pages.map(p =>
        p === '...' ? '<span style="color:#444;padding:0 4px">…</span>' :
        '<button style="' + pStyle + ';' + (p === tvPage ? pActive : '') + '" onclick="tvGoPage(' + p + ')">' + p + '</button>'
      ).join('');
      pgEl.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;gap:8px;padding:20px 8px;flex-wrap:wrap">' +
        '<button style="' + pStyle + ';' + (tvPage === 1 ? pDis : '') + '" onclick="tvGoPage(' + (tvPage - 1) + ')"' + (tvPage === 1 ? ' disabled' : '') + '>← Prev</button>' +
        pageBtns +
        '<button style="' + pStyle + ';' + (tvPage === totalPages ? pDis : '') + '" onclick="tvGoPage(' + (tvPage + 1) + ')"' + (tvPage === totalPages ? ' disabled' : '') + '>Next →</button>' +
        '</div>';
    }

    function tvGoPage(p) {
      const total = Math.ceil(tvFilteredList.length / TV_PER_PAGE);
      if (p < 1 || p > total || p === tvPage) return;
      tvPage = p;
      renderTvPage();
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function getCategories(list) {
      const cats = [...new Set(list.map(c => c.category).filter(Boolean))].sort();
      return cats;
    }

    function buildCategoryTabs(list) {
      const pillsEl = document.getElementById('cat-pills');
      if (!pillsEl) return;
      pillsEl.innerHTML = '';
      const cats = ['All', ...getCategories(list)];
      cats.forEach(cat => {
        const btn = document.createElement('button');
        btn.className = 'cat-pill' + (cat === 'All' ? ' active' : '');
        btn.textContent = cat === 'All' ? '\uD83D\uDD0D All' : cat;
        btn.dataset.cat = cat;
        btn.addEventListener('click', () => {
          pillsEl.querySelectorAll('.cat-pill').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          activeCategory = cat;
          activeCountry = 'All';
          document.getElementById('grid-cat-label').textContent = activeCategory === 'All' ? 'CHANNELS' : activeCategory.toUpperCase();
          const catFiltered = activeCategory === 'All' ? allChannels : allChannels.filter(c => c.category === activeCategory);
          buildCountrySelect(catFiltered);
          renderGrid(getFiltered());
        });
        pillsEl.appendChild(btn);
      });
    }

    function countryFlag(code) {
      if (!code || code.length !== 2) return '🌍';
      const b = 0x1F1E6 - 65;
      return String.fromCodePoint(code.toUpperCase().charCodeAt(0)+b, code.toUpperCase().charCodeAt(1)+b);
    }
    const COUNTRY_NAMES = {BD:'Bangladesh',IN:'India',GB:'UK',US:'USA',PK:'Pakistan',AE:'UAE',SA:'Saudi Arabia',QA:'Qatar',TR:'Turkey',FR:'France',DE:'Germany',RU:'Russia',IR:'Iran',EG:'Egypt',AF:'Afghanistan',NP:'Nepal',LK:'Sri Lanka',MM:'Myanmar',JP:'Japan',CN:'China',KR:'Korea',ID:'Indonesia',MY:'Malaysia',TH:'Thailand',IT:'Italy',ES:'Spain',NG:'Nigeria',GH:'Ghana'};

    function buildCountrySelect(list) {
      const sel = document.getElementById('country-select');
      if (!sel) return;
      const prev = activeCountry;
      sel.innerHTML = '';
      const allOpt = document.createElement('option');
      allOpt.value = 'All'; allOpt.textContent = '🌍 All Countries';
      sel.appendChild(allOpt);
      const counts = {};
      list.forEach(c => { if (c.country) counts[c.country] = (counts[c.country]||0)+1; });
      Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,30).forEach(([code, count]) => {
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = countryFlag(code) + ' ' + (COUNTRY_NAMES[code]||code) + ' (' + count + ')';
        sel.appendChild(opt);
      });
      sel.value = (prev !== 'All' && sel.querySelector('option[value="'+prev+'"]')) ? prev : 'All';
      activeCountry = sel.value;
      if (!sel._hasListener) {
        sel._hasListener = true;
        sel.addEventListener('change', () => {
          activeCountry = sel.value;
          renderGrid(getFiltered());
        });
      }
    }

    function getFiltered() {
      const q = gridSearch.value.toLowerCase().trim();
      let list = allChannels;
      if (activeCategory !== 'All') list = list.filter(c => c.category === activeCategory);
      if (activeCountry !== 'All') list = list.filter(c => c.country === activeCountry);
      if (q) list = list.filter(c => (c.name || c.channel_name || '').toLowerCase().includes(q));
      return list;
    }

    gridSearch.addEventListener('input', () => renderGrid(getFiltered()));

    async function loadChannels() {
      try {
        const tok = localStorage.getItem('miz_token');
        const r = await fetch('/api/user/private-channels', { headers: { Authorization: 'Bearer ' + tok } });
        if (r.status === 401) { window.location.replace('/'); return; }
        const data = await r.json();
        allChannels = data.channels || [];
        if (!allChannels.length) {
          gridChannels.innerHTML = '<div class="no-results" style="grid-column:1/-1">Access denied or no private channels assigned.</div>';
          return;
        }
        buildCategoryTabs(allChannels);
        buildCountrySelect(allChannels);
        document.getElementById('grid-cat-label').textContent = 'CHANNELS';
        renderGrid(allChannels);
      } catch(e) {
        gridChannels.innerHTML = '<div class="no-results" style="grid-column:1/-1;color:#a33;">Failed to load channels.</div>';
      }
    }

    function renderAuthUI(user, role) {
      const area = document.getElementById('auth-area');
      if (!area) return;
      if (user) {
        const isAdmin = role === 'admin';
        area.innerHTML = '<div class="user-menu">' +
          '<button class="auth-btn" id="user-btn">👤 ' + user.email.split('@')[0] + ' ▾</button>' +
          '<div class="user-drop" id="user-drop">' +
          '<div class="user-email-line">' + user.email + '</div>' +
          (isAdmin ? '<a href="/admin" class="user-item">⚙️ Admin Panel</a>' : '') +
          '<a href="/" class="user-item">🏠 Main Page</a>' +
          '<div class="user-item danger" id="logout-btn">🚪 Logout</div></div></div>';
        document.getElementById('user-btn').addEventListener('click', e => {
          e.stopPropagation();
          document.getElementById('user-drop').classList.toggle('open');
        });
        document.addEventListener('click', () => { const d = document.getElementById('user-drop'); if(d) d.classList.remove('open'); });
        document.getElementById('logout-btn').addEventListener('click', () => {
          localStorage.removeItem('miz_token'); localStorage.removeItem('miz_user');
          localStorage.removeItem('miz_refresh');
          window.location.replace('/');
        });
      }
    }
    async function initAuth() {
      const token = localStorage.getItem('miz_token');
      if (!token) { window.location.replace('/'); return; }
      try {
        const r = await fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } });
        const d = await r.json();
        if (d.user) { renderAuthUI(d.user, d.user.role); }
        else { localStorage.removeItem('miz_token'); window.location.replace('/'); return; }
      } catch(_) {}
    }
    initAuth();
    loadChannels();
  </script>
</body>
</html>`);
});

app.get('/private-watch', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MIZ Live TV</title>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0a0a0a; color: #fff;
      font-family: 'Segoe UI', Arial, sans-serif;
      min-height: 100vh; display: flex; flex-direction: column;
      align-items: center; padding: 16px 12px 60px;
    }
    header {
      width: 100%; max-width: 960px;
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    header h1 { font-size: 18px; font-weight: 700; letter-spacing: 1px; display:flex; align-items:center; gap:8px; }
    .player-wrapper {
      position: relative; width: 100%; max-width: 960px;
      background: #000; border-radius: 10px; overflow: hidden;
      box-shadow: 0 0 50px rgba(220,0,0,0.18); cursor: pointer;
    }
    .player-wrapper:fullscreen, .player-wrapper:-webkit-full-screen { border-radius: 0; max-width: 100%; }
    video { width: 100%; display: block; background: #000; aspect-ratio: 16/9; }
    .center-icon {
      position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
      width: 64px; height: 64px; border-radius: 50%; background: rgba(0,0,0,0.55);
      display: flex; align-items: center; justify-content: center;
      z-index: 12; opacity: 0; pointer-events: none; transition: opacity 0.25s;
    }
    .center-icon svg { width: 30px; height: 30px; fill: #fff; }
    .center-icon.show { opacity: 1; }
    .controls-bar {
      position: absolute; bottom: 0; left: 0; right: 0;
      background: linear-gradient(to top, rgba(0,0,0,0.85) 0%, transparent 100%);
      padding: 28px 14px 10px; display: flex; flex-direction: column; gap: 6px;
      z-index: 10; opacity: 0; transition: opacity 0.3s; pointer-events: none;
    }
    .player-wrapper.controls-visible .controls-bar { opacity: 1; pointer-events: auto; }
    .player-wrapper.controls-hidden * { cursor: none; }
    .viewer-badge {
      position: absolute; top: 12px; right: 12px;
      background: rgba(0,0,0,0.65); color: #fff;
      font-size: 12px; font-weight: 600; padding: 4px 9px;
      border-radius: 20px; pointer-events: none;
      display: flex; align-items: center; gap: 5px;
      backdrop-filter: blur(4px); z-index: 20;
      opacity: 0; transition: opacity 0.3s;
    }
    .player-wrapper.controls-visible .viewer-badge { opacity: 1; }
    .progress-row { display: flex; align-items: center; gap: 8px; }
    .live-line { flex: 1; height: 4px; background: rgba(255,255,255,0.2); border-radius: 2px; position: relative; overflow: hidden; }
    .live-line-fill { position: absolute; left: 0; top: 0; bottom: 0; width: 100%; background: #e00; border-radius: 2px; animation: live-pulse 2s ease-in-out infinite; }
    @keyframes live-pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
    .live-tag { font-size: 10px; font-weight: 700; color: #e00; letter-spacing: 1px; flex-shrink: 0; }
    .btns-row { display: flex; align-items: center; gap: 4px; }
    .ctrl-btn {
      background: none; border: none; cursor: pointer; color: #fff; padding: 5px;
      border-radius: 4px; display: flex; align-items: center; justify-content: center;
      transition: background 0.15s, transform 0.1s; flex-shrink: 0;
    }
    .ctrl-btn:hover { background: rgba(255,255,255,0.15); }
    .ctrl-btn:active { transform: scale(0.9); }
    .ctrl-btn svg { width: 20px; height: 20px; fill: #fff; display: block; }
    .vol-wrap { display: flex; align-items: center; gap: 6px; }
    .vol-slider {
      -webkit-appearance: none; appearance: none; width: 70px; height: 4px;
      border-radius: 2px; outline: none; cursor: pointer;
      background: linear-gradient(to right, #fff 0%, #fff var(--vol,100%), rgba(255,255,255,0.3) var(--vol,100%), rgba(255,255,255,0.3) 100%);
    }
    .vol-slider::-webkit-slider-thumb { -webkit-appearance: none; width: 12px; height: 12px; border-radius: 50%; background: #fff; cursor: pointer; }
    .spacer { flex: 1; }
    .kbps-label { font-size: 11px; color: rgba(255,255,255,0.5); flex-shrink: 0; }
    .quality-wrap { position: relative; flex-shrink: 0; }
    .quality-btn {
      background: none; border: none; cursor: pointer; color: #fff;
      font-size: 11px; font-weight: 700; padding: 5px 8px;
      border-radius: 4px; display: flex; align-items: center; gap: 4px;
      transition: background 0.15s; white-space: nowrap;
    }
    .quality-btn:hover { background: rgba(255,255,255,0.15); }
    .quality-btn svg { width: 16px; height: 16px; fill: #fff; flex-shrink: 0; }
    .quality-menu {
      position: fixed; background: rgba(20,20,20,0.97); border: 1px solid #333;
      border-radius: 8px; overflow-y: auto; overflow-x: hidden;
      z-index: 9999; display: none; flex-direction: column;
      min-width: 140px; max-height: 260px; box-shadow: 0 6px 24px rgba(0,0,0,0.7);
      scrollbar-width: thin; scrollbar-color: #444 transparent;
    }
    .quality-menu::-webkit-scrollbar { width: 4px; }
    .quality-menu::-webkit-scrollbar-track { background: transparent; }
    .quality-menu::-webkit-scrollbar-thumb { background: #444; border-radius: 2px; }
    .quality-menu.open { display: flex; }
    .quality-menu-title { padding: 8px 14px 6px; font-size: 10px; font-weight: 700; color: #666; text-transform: uppercase; letter-spacing: 1px; border-bottom: 1px solid #252525; }
    .quality-option {
      padding: 9px 14px; font-size: 13px; color: #ccc; cursor: pointer;
      border-bottom: 1px solid #1e1e1e; transition: background 0.12s, color 0.12s;
      display: flex; align-items: center; justify-content: space-between; gap: 10px;
    }
    .quality-option:last-child { border-bottom: none; }
    .quality-option:hover { background: rgba(255,255,255,0.08); color: #fff; }
    .quality-option.active { color: #fff; font-weight: 700; }
    .quality-option .q-check { color: #e00; font-size: 14px; line-height: 1; }
    #loading-msg {
      display: none; position: absolute; inset: 0; background: rgba(0,0,0,0.72);
      align-items: center; justify-content: center;
      flex-direction: column; gap: 10px; z-index: 15; pointer-events: none;
    }
    #loading-msg.visible { display: flex; }
    .spinner { width: 38px; height: 38px; border: 3px solid #333; border-top-color: #e00; border-radius: 50%; animation: spin .8s linear infinite; }
    #error-msg {
      display: none; position: absolute; inset: 0; background: rgba(0,0,0,0.85);
      color: #bbb; font-size: 14px; align-items: center; justify-content: center;
      flex-direction: column; gap: 12px; text-align: center; padding: 24px; z-index: 20;
    }
    #error-msg.visible { display: flex; }
    #error-msg .icon { font-size: 36px; }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.3;transform:scale(.6)} }
    @keyframes spin { to { transform: rotate(360deg); } }
    .status-bar {
      width: 100%; max-width: 960px;
      display: flex; align-items: center; justify-content: space-between;
      padding: 8px 2px; font-size: 12px; color: #555;
    }
    .status-indicator { display: flex; align-items: center; gap: 6px; }
    .status-dot { width: 7px; height: 7px; border-radius: 50%; background: #2a2; }
    .status-dot.offline { background: #a33; }
    .status-dot.loading { background: #fa0; animation: pulse 1s infinite; }
    .channel-section { width: 100%; max-width: 960px; margin-top: 18px; }
    .ch-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
    .ch-header h2 { font-size: 13px; font-weight: 600; color: #555; letter-spacing: 1px; text-transform: uppercase; }
    .ch-count { font-size: 12px; color: #444; }
    .search-bar {
      width: 100%; background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 9px 14px; color: #ddd; font-size: 13px;
      margin-bottom: 10px; outline: none; transition: border-color .2s;
    }
    .search-bar::placeholder { color: #444; }
    .search-bar:focus { border-color: #e00; }
    .channel-list {
      display: flex; flex-direction: column; gap: 6px;
      max-height: 520px; overflow-y: auto;
      scrollbar-width: thin; scrollbar-color: #333 transparent;
    }
    .channel-list::-webkit-scrollbar { width: 5px; }
    .channel-list::-webkit-scrollbar-track { background: transparent; }
    .channel-list::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
    .channel-item {
      display: flex; align-items: center; justify-content: space-between;
      background: #141414; border: 1px solid #1e1e1e;
      border-radius: 7px; padding: 10px 14px;
      cursor: pointer; transition: border-color .15s, background .15s;
    }
    .channel-item:hover { background: #1c1c1c; border-color: #c00; }
    .channel-item.active { background: #1a0000; border-color: #e00; }
    .channel-item.ch-failed { opacity: 0.45; }
    .channel-item.ch-failed .ch-name::after { content: ' ⚠'; font-size: 11px; color: #a55; }
    .hide-toggle {
      background: none; border: 1px solid #333; border-radius: 5px;
      color: #555; font-size: 11px; font-weight: 600; padding: 3px 9px;
      cursor: pointer; transition: all .15s; white-space: nowrap;
    }
    .hide-toggle:hover { border-color: #e00; color: #ccc; }
    .hide-toggle.active { border-color: #e00; color: #e00; background: #1a0000; }
    .channel-left { display: flex; align-items: center; gap: 10px; min-width: 0; }
    .ch-number { font-size: 11px; font-weight: 700; color: #444; min-width: 26px; flex-shrink: 0; }
    .ch-name { font-size: 13px; font-weight: 500; color: #ddd; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ch-right { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
    .play-btn {
      background: #e00; color: #fff; border: none; border-radius: 5px;
      padding: 5px 12px; font-size: 11px; font-weight: 700;
      cursor: pointer; transition: background .15s; white-space: nowrap;
    }
    .play-btn:hover { background: #c00; }
    .no-results { color: #444; font-size: 13px; padding: 16px; text-align: center; }
    .ch-logo-thumb { width: 32px; height: 32px; border-radius: 50%; object-fit: contain; background: #1e1e1e; flex-shrink: 0; }
    .ch-logo-fb { width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 800; color: #fff; flex-shrink: 0; }
    .fav-btn { background: none; border: none; font-size: 14px; cursor: pointer; padding: 2px 4px; line-height: 1; opacity: 0.5; transition: opacity .15s; flex-shrink: 0; }
    .fav-btn:hover, .fav-btn.active { opacity: 1; }
    .ch-badge { font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 4px; flex-shrink: 0; white-space: nowrap; }
    .ch-badge.online { background: rgba(40,180,80,.15); color: #4d4; }
    .ch-badge.offline { background: rgba(100,40,40,.2); color: #955; }
    .hd-badge { font-size: 9px; font-weight: 800; background: #1a3a1a; color: #4d4; border: 1px solid #2d6b2d; padding: 1px 5px; border-radius: 3px; flex-shrink: 0; }
    .resume-bar { display:none; width:100%; background:#141414; border:1px solid #2a2a2a; border-radius:8px; padding:10px 14px; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; }
    .resume-bar.show { display:flex; }
    #resume-label { font-size:13px; color:#ccc; }
    #resume-yes { background:#e00; color:#fff; border:none; border-radius:5px; padding:5px 12px; font-size:12px; font-weight:700; cursor:pointer; }
    #resume-yes:hover { background:#c00; }
    #resume-dismiss { background:none; border:1px solid #333; color:#666; border-radius:5px; padding:5px 10px; font-size:12px; cursor:pointer; }
    #resume-dismiss:hover { border-color:#666; color:#aaa; }
    .recent-section { width:100%; margin-bottom:8px; }
    .recent-label { font-size:11px; font-weight:700; color:#444; text-transform:uppercase; letter-spacing:.5px; margin-bottom:6px; }
    .recent-row { display:flex; gap:6px; flex-wrap:wrap; }
    .recent-chip { background:#141414; border:1px solid #2a2a2a; border-radius:20px; padding:5px 13px; font-size:11px; color:#888; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .recent-chip:hover { border-color:#e00; color:#ddd; background:#1c1c1c; }
    .auth-btn {
      background: #1a1a1a; color: #ccc; border: 1px solid #333;
      border-radius: 8px; padding: 7px 13px; font-size: 12px; font-weight: 600;
      cursor: pointer; text-decoration: none; display: flex; align-items: center; gap: 6px;
      transition: all .15s; white-space: nowrap; flex-shrink: 0;
    }
    .auth-btn:hover { background: #e00; color: #fff; border-color: #e00; }
    .user-menu { position: relative; }
    .user-drop {
      position: absolute; right: 0; top: calc(100% + 8px);
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 8px; min-width: 185px; z-index: 200; overflow: hidden; display: none;
    }
    .user-drop.open { display: block; }
    .user-email-line { padding: 10px 14px; font-size: 11px; color: #666; border-bottom: 1px solid #2a2a2a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .user-item { padding: 10px 14px; font-size: 13px; color: #ccc; cursor: pointer; transition: background .1s; display: block; text-decoration: none; }
    .user-item:hover { background: #2a2a2a; color: #fff; }
    .user-item.danger:hover { background: #e00; }
  </style>
</head>
<body>
  <header>
    <h1><a href="/" style="display:flex;align-items:center;gap:8px;text-decoration:none;color:inherit">${LOGO_FULL_HTML}</a></h1>
    <div id="auth-area"></div>
  </header>

  <div class="player-wrapper" id="player-wrapper">
    <video id="video" autoplay muted playsinline autopictureinpicture></video>
    <div class="center-icon" id="center-icon">
      <svg id="center-svg" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
    </div>
    <div class="controls-bar" id="controls-bar">
      <div class="progress-row">
        <div class="live-line"><div class="live-line-fill"></div></div>
        <span class="live-tag">LIVE</span>
      </div>
      <div class="btns-row">
        <button class="ctrl-btn" id="btn-play" title="Play/Pause">
          <svg id="play-icon" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>
        </button>
        <div class="vol-wrap">
          <button class="ctrl-btn" id="btn-mute" title="Mute/Unmute">
            <svg id="vol-icon" viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0 0 14 7.97v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/></svg>
          </button>
          <input class="vol-slider" id="vol-slider" type="range" min="0" max="1" step="0.02" value="1" />
        </div>
        <div class="spacer"></div>
        <span class="kbps-label" id="kbps-label"></span>
        <div class="quality-wrap">
          <button class="quality-btn" id="quality-btn" title="Quality">
            <svg viewBox="0 0 24 24"><path d="M19.14 12.94c.04-.3.06-.61.06-.94s-.02-.64-.07-.94l2.03-1.58a.49.49 0 0 0 .12-.61l-1.92-3.32a.49.49 0 0 0-.59-.22l-2.39.96a7.02 7.02 0 0 0-1.62-.94l-.36-2.54a.484.484 0 0 0-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96a.47.47 0 0 0-.59.22L2.74 8.87a.47.47 0 0 0 .12.61l2.03 1.58c-.05.3-.07.62-.07.94s.02.64.07.94l-2.03 1.58a.49.49 0 0 0-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.57 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32a.47.47 0 0 0-.12-.61l-2.03-1.58zM12 15.6a3.6 3.6 0 1 1 0-7.2 3.6 3.6 0 0 1 0 7.2z"/></svg>
            <span id="quality-label">Auto</span>
          </button>
          <div class="quality-menu" id="quality-menu">
            <div class="quality-menu-title">Quality</div>
          </div>
        </div>
        <button class="ctrl-btn" id="btn-pip" title="Picture in Picture" style="display:none">
          <svg id="pip-icon" viewBox="0 0 24 24"><path d="M19 7H9c-1.1 0-2 .9-2 2v6c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V9c0-1.1-.9-2-2-2zm0 8H9V9h10v6zM3 5v14h2V5H3zm4-2v2h12V3H7z"/></svg>
        </button>
        <button class="ctrl-btn" id="btn-fs" title="Fullscreen">
          <svg id="fs-icon" viewBox="0 0 24 24"><path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/></svg>
        </button>
      </div>
    </div>
    <div id="loading-msg" class="visible">
      <div class="spinner"></div>
      <span style="color:#777;font-size:12px;">Loading stream...</span>
    </div>
    <div class="viewer-badge" id="viewer-badge" style="display:none">👁 <span id="viewer-count">—</span></div>
    <div id="error-msg">
      <span class="icon">📡</span>
      <strong>Stream Unavailable</strong>
      <span id="error-detail">Could not load this stream.</span>
    </div>
  </div>

  <div class="status-bar" id="status-bar-wrap">
    <div class="status-indicator">
      <span class="status-dot loading" id="dot"></span>
      <span id="status-text">Connecting...</span>
    </div>
    <span id="stream-info"></span>
  </div>

  <div class="server-bar" id="server-bar" style="display:none"></div>

  <div id="_dbg" style="display:block;width:100%;max-width:960px;background:#1a1a1a;color:#ffcc00;font-size:11px;padding:6px 10px;border-radius:6px;margin-bottom:6px;word-break:break-all;min-height:20px">Loading...</div>

  <div class="channel-section" id="channel-section">
    <div class="ch-header">
      <h2>Private Channels</h2>
      <div style="display:flex;align-items:center;gap:8px;">
        <span class="ch-count" id="ch-count"></span>
        <button class="hide-toggle" id="hide-toggle">Hide unavailable</button>
      </div>
    </div>
    <input class="search-bar" id="search" type="text" placeholder="&#128269;  Search channels..." autocomplete="off" />
    <div id="resume-bar" class="resume-bar">
      <span id="resume-label">&#128250; Resume watching?</span>
      <div style="display:flex;gap:6px">
        <button id="resume-yes">&#9654; Resume</button>
        <button id="resume-dismiss">&#10005;</button>
      </div>
    </div>
    <div class="recent-section" id="recent-section" style="display:none">
      <div class="recent-label">&#128336; Recently Watched</div>
      <div class="recent-row" id="recent-row"></div>
    </div>
    <div class="channel-list" id="channel-list">
      <div style="color:#444;font-size:13px;padding:14px;">Loading channels...</div>
    </div>
  </div>

  <style>
    .server-bar {
      width: 100%; max-width: 960px;
      display: flex; flex-wrap: wrap; align-items: center; gap: 8px;
      padding: 6px 0 2px;
    }
    .server-bar-label {
      font-size: 11px; font-weight: 700; color: #555;
      text-transform: uppercase; letter-spacing: .5px;
      margin-right: 2px; white-space: nowrap;
    }
    .server-btn {
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 6px; padding: 5px 13px;
      font-size: 11px; font-weight: 600; color: #888;
      cursor: pointer; transition: all .15s; white-space: nowrap;
    }
    .server-btn:hover { border-color: #e00; color: #ddd; background: #1e1e1e; }
    .server-btn.active { background: #e00; border-color: #e00; color: #fff; }
  </style>
  <script>
    (function() {
      const tok = localStorage.getItem('miz_token');
      if (!tok) { window.location.replace('/'); throw new Error('blocked'); }
    })();

    const video        = document.getElementById('video');
    const playerWrap   = document.getElementById('player-wrapper');
    const dot          = document.getElementById('dot');
    const statusText   = document.getElementById('status-text');
    const errorMsg     = document.getElementById('error-msg');
    const errorDetail  = document.getElementById('error-detail');
    const loadingMsg   = document.getElementById('loading-msg');
    const channelList  = document.getElementById('channel-list');
    const searchInput  = document.getElementById('search');
    const chCount      = document.getElementById('ch-count');
    const qualityBtn   = document.getElementById('quality-btn');
    const qualityMenu  = document.getElementById('quality-menu');
    const qualityLabel = document.getElementById('quality-label');
    const kbpsLabel    = document.getElementById('kbps-label');
    const btnPlay      = document.getElementById('btn-play');
    const playIcon     = document.getElementById('play-icon');
    const btnMute      = document.getElementById('btn-mute');
    const volIcon      = document.getElementById('vol-icon');
    const volSlider    = document.getElementById('vol-slider');
    const btnFs        = document.getElementById('btn-fs');
    const fsIcon       = document.getElementById('fs-icon');
    const btnPip       = document.getElementById('btn-pip');
    const centerIcon   = document.getElementById('center-icon');
    const centerSvg    = document.getElementById('center-svg');
    const serverBar    = document.getElementById('server-bar');

    let currentHls  = null;
    let activeId    = null;
    let allChannels = [];
    let isAutoMode  = true;
    let hideTimer   = null;
    let currentServers = [];
    let currentActiveServerIdx = 0;

    const FAILED_KEY = 'miz_priv_failed_v1';
    let failedIds = new Set(JSON.parse(localStorage.getItem(FAILED_KEY) || '[]'));
    let hideUnavailable = false;

    function markFailed(id) {
      if (!id) return;
      failedIds.add(id);
      localStorage.setItem(FAILED_KEY, JSON.stringify([...failedIds]));
      document.querySelectorAll('.channel-item[data-id="' + id + '"]').forEach(el => el.classList.add('ch-failed'));
    }

    function isGeoName(name) {
      return /\[geo.?block/i.test(name) || /\[not.?avail/i.test(name);
    }

    const hideToggleBtn = document.getElementById('hide-toggle');
    hideToggleBtn.addEventListener('click', () => {
      hideUnavailable = !hideUnavailable;
      hideToggleBtn.classList.toggle('active', hideUnavailable);
      hideToggleBtn.textContent = hideUnavailable ? 'Show all' : 'Hide unavailable';
      applyFilter();
    });

    function renderServerBar() {
      if (!serverBar) return;
      if (currentServers.length <= 1) {
        serverBar.style.display = 'none';
        serverBar.innerHTML = '';
        return;
      }
      serverBar.style.display = 'flex';
      let html = '<span class="server-bar-label">Server:</span>';
      currentServers.forEach((srv, idx) => {
        const active = idx === currentActiveServerIdx ? ' active' : '';
        html += '<button class="server-btn' + active + '" data-idx="' + idx + '">S' + (idx + 1) + '</button>';
      });
      serverBar.innerHTML = html;
      serverBar.querySelectorAll('.server-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const idx = parseInt(btn.dataset.idx);
          if (idx === currentActiveServerIdx) return;
          currentActiveServerIdx = idx;
          const url = currentServers[idx].url;
          playFromUrl(url);
          serverBar.querySelectorAll('.server-btn').forEach((b, i) => b.classList.toggle('active', i === idx));
        });
      });
    }

    function playFromUrl(url) {
      if (currentHls) { currentHls.destroy(); currentHls = null; }
      currentStreamUrl = url;
      resetQuality();
      kbpsLabel.textContent = '';
      errorMsg.classList.remove('visible');
      loadingMsg.classList.add('visible');
      setStatus('loading', 'Connecting...');
      const proxyUrl = '/proxy?url=' + encodeURIComponent(url);
      if (Hls.isSupported()) {
        let _triedDirect = false;
        let _connectTimer = null;
        const _clearConnect = () => { if (_connectTimer) { clearTimeout(_connectTimer); _connectTimer = null; } };
        function _startHls(srcUrl) {
          if (currentHls) { currentHls.destroy(); currentHls = null; }
          const hls = new Hls({ liveSyncDurationCount:3, liveMaxLatencyDurationCount:6, enableWorker:true });
          currentHls = hls;
          hls.loadSource(srcUrl);
          hls.attachMedia(video);
          _clearConnect();
          _connectTimer = setTimeout(() => {
            if (currentHls) { currentHls.destroy(); currentHls = null; }
            if (!_triedDirect && srcUrl === proxyUrl) {
              _triedDirect = true;
              setStatus('loading', 'Retrying direct...');
              loadingMsg.classList.add('visible');
              errorMsg.classList.remove('visible');
              _startHls(url);
            } else {
              loadingMsg.classList.remove('visible');
              setStatus('offline', 'Stream not responding');
              errorDetail.innerHTML = 'Could not connect to stream. The stream may be offline or unreachable.';
              errorMsg.classList.add('visible');
            }
          }, 15000);
          let _stallTimer = null;
          const _clearStall = () => { if (_stallTimer) { clearTimeout(_stallTimer); _stallTimer = null; } };
          function _onTimeUpdate() {
            if (video.currentTime > 0) { _clearStall(); video.removeEventListener('timeupdate', _onTimeUpdate); }
          }
          video.addEventListener('timeupdate', _onTimeUpdate, { passive: true });
          hls.on(Hls.Events.MANIFEST_PARSED, () => {
            _clearConnect();
            loadingMsg.classList.remove('visible');
            setStatus('live', 'Streaming');
            video.muted = true; updateVolIcon();
            video.play().catch(()=>{});
            buildQualityMenu(); updatePlayIcon();
            _stallTimer = setTimeout(() => {
              video.removeEventListener('timeupdate', _onTimeUpdate);
              if (video.currentTime === 0) {
                setStatus('offline', 'Stream not responding');
                errorDetail.innerHTML = 'Stream loaded but no video frames received. Try another server or channel.';
                errorMsg.classList.add('visible');
              }
            }, 12000);
          });
          hls.on(Hls.Events.LEVEL_LOADED, () => {
            if (hls.bandwidthEstimate > 0) kbpsLabel.textContent = Math.round(hls.bandwidthEstimate/1000) + ' kbps';
          });
          hls.on(Hls.Events.ERROR, (_, data) => {
            if (data.fatal) {
              _clearConnect();
              _clearStall();
              video.removeEventListener('timeupdate', _onTimeUpdate);
              hls.destroy();
              if (!_triedDirect && srcUrl === proxyUrl) {
                _triedDirect = true;
                setStatus('loading', 'Retrying direct...');
                loadingMsg.classList.add('visible');
                errorMsg.classList.remove('visible');
                _startHls(url);
                return;
              }
              loadingMsg.classList.remove('visible');
              const isCodec = data.details && data.details.toLowerCase().includes('codec');
              if (isCodec) { setStatus('offline','Codec not supported'); showCodecError(); }
              else {
                markFailed(activeId);
                const isGeo = activeId && allChannels.find(c=>c.id===activeId) &&
                              isGeoName(allChannels.find(c=>c.id===activeId).name || '');
                setStatus('offline', isGeo ? 'Region restricted' : 'Stream unavailable');
                errorDetail.innerHTML = isGeo
                  ? 'This channel is not available in your region.'
                  : 'Could not load this stream. Try another server or channel.';
                errorMsg.classList.add('visible');
              }
            }
          });
        }
        _startHls(proxyUrl);
      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = proxyUrl;
        video.addEventListener('loadedmetadata', () => {
          loadingMsg.classList.remove('visible'); setStatus('live','Streaming');
          video.play().catch(()=>{}); updatePlayIcon();
        }, { once:true });
        video.addEventListener('error', () => {
          loadingMsg.classList.remove('visible'); setStatus('offline','Stream unavailable');
          errorMsg.classList.add('visible');
        }, { once:true });
      } else {
        loadingMsg.classList.remove('visible');
        setStatus('offline','HLS not supported');
        errorDetail.textContent = 'Your browser does not support HLS playback.';
        errorMsg.classList.add('visible');
      }
    }

    const SVG = {
      play:    'M8 5v14l11-7z',
      pause:   'M6 19h4V5H6v14zm8-14v14h4V5h-4z',
      volFull: 'M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0 0 14 7.97v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z',
      volMute: 'M16.5 12A4.5 4.5 0 0 0 14 7.97v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51A8.796 8.796 0 0 0 21 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06A8.99 8.99 0 0 0 17.73 18 16.4 16.4 0 0 0 19.73 20L21 18.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z',
      fsEnter: 'M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z',
      fsExit:  'M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z'
    };

    function showControls() {
      playerWrap.classList.add('controls-visible');
      playerWrap.classList.remove('controls-hidden');
      clearTimeout(hideTimer);
      hideTimer = setTimeout(() => {
        if (!video.paused) {
          playerWrap.classList.remove('controls-visible');
          playerWrap.classList.add('controls-hidden');
        }
      }, 3000);
    }
    playerWrap.addEventListener('mousemove', showControls);
    playerWrap.addEventListener('touchstart', showControls, { passive: true });
    playerWrap.addEventListener('touchmove', showControls, { passive: true });

    function updatePlayIcon() {
      playIcon.querySelector('path').setAttribute('d', video.paused ? SVG.play : SVG.pause);
    }
    function flashCenter(paused) {
      centerSvg.querySelector('path').setAttribute('d', paused ? SVG.play : SVG.pause);
      centerIcon.classList.add('show');
      setTimeout(() => centerIcon.classList.remove('show'), 600);
    }
    btnPlay.addEventListener('click', (e) => {
      e.stopPropagation();
      flashCenter(video.paused);
      if (video.paused) { video.play().catch(()=>{}); } else { video.pause(); }
    });
    video.addEventListener('play',  () => { updatePlayIcon(); showControls(); });
    video.addEventListener('pause', () => { updatePlayIcon(); playerWrap.classList.add('controls-visible'); });
    playerWrap.addEventListener('click', (e) => {
      if (e.target === playerWrap || e.target === video) {
        if (playerWrap.classList.contains('controls-visible')) {
          clearTimeout(hideTimer);
          playerWrap.classList.remove('controls-visible');
          playerWrap.classList.add('controls-hidden');
        } else {
          showControls();
        }
      }
    });

    function updateVolIcon() {
      volIcon.querySelector('path').setAttribute('d', video.muted || video.volume === 0 ? SVG.volMute : SVG.volFull);
      const pct = video.muted ? 0 : video.volume * 100;
      volSlider.style.setProperty('--vol', pct + '%');
      volSlider.value = video.muted ? 0 : video.volume;
    }
    btnMute.addEventListener('click', (e) => { e.stopPropagation(); video.muted = !video.muted; updateVolIcon(); });
    volSlider.addEventListener('input', (e) => {
      e.stopPropagation();
      video.volume = parseFloat(volSlider.value);
      video.muted  = video.volume === 0;
      updateVolIcon();
    });
    volSlider.addEventListener('click', e => e.stopPropagation());

    function updateFsIcon() {
      const isFs = !!(document.fullscreenElement || document.webkitFullscreenElement);
      fsIcon.querySelector('path').setAttribute('d', isFs ? SVG.fsExit : SVG.fsEnter);
    }
    btnFs.addEventListener('click', (e) => {
      e.stopPropagation();
      if (!document.fullscreenElement && !document.webkitFullscreenElement) {
        const fsPromise = (playerWrap.requestFullscreen || playerWrap.webkitRequestFullscreen).call(playerWrap);
        if (fsPromise && fsPromise.then) {
          fsPromise.then(() => {
            if (screen.orientation && screen.orientation.lock) {
              screen.orientation.lock('landscape').catch(() => {});
            }
          }).catch(() => {});
        }
      } else {
        (document.exitFullscreen || document.webkitExitFullscreen).call(document);
        if (screen.orientation && screen.orientation.unlock) screen.orientation.unlock();
      }
    });
    document.addEventListener('fullscreenchange', updateFsIcon);
    document.addEventListener('webkitfullscreenchange', updateFsIcon);

    const pipSupported = !!document.pictureInPictureEnabled;
    if (pipSupported) {
      btnPip.style.display = '';
      btnPip.addEventListener('click', async (e) => {
        e.stopPropagation();
        try {
          if (document.pictureInPictureElement) { await document.exitPictureInPicture(); }
          else { await video.requestPictureInPicture(); }
        } catch(err) {}
      });
      video.addEventListener('enterpictureinpicture', () => {
        btnPip.title = 'Exit Picture in Picture';
        btnPip.querySelector('path').setAttribute('d', 'M21 3H3c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H3V5h18v14zm-10-7h8v5h-8z');
      });
      video.addEventListener('leavepictureinpicture', () => {
        btnPip.title = 'Picture in Picture';
        btnPip.querySelector('path').setAttribute('d', 'M19 7H9c-1.1 0-2 .9-2 2v6c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V9c0-1.1-.9-2-2-2zm0 8H9V9h10v6zM3 5v14h2V5H3zm4-2v2h12V3H7z');
      });
      video.addEventListener('play', () => {
        try { video.autoPictureInPicture = true; } catch(e) {}
      });
      video.addEventListener('pause', () => {
        try { video.autoPictureInPicture = false; } catch(e) {}
      });

      document.addEventListener('visibilitychange', async () => {
        if (!currentHls && !video.src) return;
        if (document.hidden) {
          if (!video.paused && !document.pictureInPictureElement && video.readyState >= 2) {
            try { await video.requestPictureInPicture(); } catch(e) {}
          }
        } else {
          if (document.pictureInPictureElement === video) {
            try { await document.exitPictureInPicture(); } catch(e) {}
          }
        }
      });
    }

    function setStatus(state, label) {
      dot.className = 'status-dot' + (state==='live' ? '' : state==='loading' ? ' loading' : ' offline');
      statusText.textContent = label;
    }

    const QUALITY_OPTIONS = [
      { label: '1080p', height: '1080', note: 'HD' },
      { label: '720p',  height: '720',  note: 'HD' },
      { label: '480p',  height: '480',  note: '' },
      { label: '360p',  height: '360',  note: '' },
      { label: '240p',  height: '240',  note: 'Low' },
    ];
    let currentTranscodeSession = null;
    let currentStreamUrl = null;

    async function stopTranscode() {
      if (currentTranscodeSession) {
        fetch('/transcode/stop/' + currentTranscodeSession).catch(()=>{});
        currentTranscodeSession = null;
      }
    }
    function updateMenuActive(activeKey) {
      qualityMenu.querySelectorAll('.quality-option').forEach(opt => {
        const isActive = opt.dataset.key === activeKey;
        opt.classList.toggle('active', isActive);
        let check = opt.querySelector('.q-check');
        if (isActive && !check) { check = document.createElement('span'); check.className = 'q-check'; check.textContent = '✓'; opt.appendChild(check); }
        else if (!isActive && check) { check.remove(); }
      });
    }
    async function selectTranscodeQuality(height, label) {
      qualityMenu.classList.remove('open');
      qualityLabel.textContent = '⏳ ' + label;
      await stopTranscode();
      try {
        const r = await fetch('/transcode/start?url=' + encodeURIComponent(currentStreamUrl) + '&height=' + height);
        const data = await r.json();
        if (!data.sessionId) throw new Error('No session');
        currentTranscodeSession = data.sessionId;
        const tcUrl = '/transcode/' + data.sessionId + '/index.m3u8';
        if (currentHls) { currentHls.destroy(); currentHls = null; }
        const hls = new Hls({ liveSyncDurationCount:3, liveMaxLatencyDurationCount:6, enableWorker:true });
        currentHls = hls;
        hls.loadSource(tcUrl);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          loadingMsg.classList.remove('visible');
          setStatus('live', 'Streaming · ' + label);
          video.play().catch(()=>{}); updatePlayIcon();
        });
        hls.on(Hls.Events.ERROR, (_, d) => {
          if (d.fatal) { setStatus('offline', 'Transcode error'); loadingMsg.classList.remove('visible'); hls.destroy(); }
        });
        qualityLabel.textContent = label;
        updateMenuActive(height);
      } catch(e) {
        qualityLabel.textContent = 'Auto'; updateMenuActive('auto'); setStatus('live', 'Streaming');
      }
    }
    function selectAutoQuality() {
      qualityMenu.classList.remove('open');
      stopTranscode();
      qualityLabel.textContent = 'Auto';
      updateMenuActive('auto');
      if (!currentStreamUrl) return;
      playFromUrl(currentStreamUrl);
    }
    function buildQualityMenu() {
      qualityMenu.innerHTML = '<div class="quality-menu-title">Quality</div>';
      const autoOpt = document.createElement('div');
      autoOpt.className = 'quality-option active'; autoOpt.dataset.key = 'auto';
      autoOpt.innerHTML = '<span>Auto</span><span class="q-check">✓</span>';
      autoOpt.addEventListener('click', (e) => { e.stopPropagation(); selectAutoQuality(); });
      qualityMenu.appendChild(autoOpt);
      QUALITY_OPTIONS.forEach(q => {
        const opt = document.createElement('div');
        opt.className = 'quality-option'; opt.dataset.key = q.height;
        const noteHtml = q.note ? ' <span style="font-size:10px;color:#666;margin-left:4px;">' + q.note + '</span>' : '';
        opt.innerHTML = '<span>' + q.label + noteHtml + '</span>';
        opt.addEventListener('click', (e) => { e.stopPropagation(); selectTranscodeQuality(q.height, q.label); });
        qualityMenu.appendChild(opt);
      });
      qualityLabel.textContent = 'Auto';
    }
    function resetQuality() {
      stopTranscode(); qualityMenu.classList.remove('open');
      buildQualityMenu(); qualityLabel.textContent = 'Auto'; isAutoMode = true;
    }
    function positionQualityMenu() {
      const rect = qualityBtn.getBoundingClientRect();
      const menuH = Math.min(260, qualityMenu.scrollHeight);
      const spaceAbove = rect.top; const spaceBelow = window.innerHeight - rect.bottom;
      if (spaceAbove >= menuH + 8 || spaceAbove > spaceBelow) {
        qualityMenu.style.top = (rect.top - menuH - 8) + 'px';
      } else { qualityMenu.style.top = (rect.bottom + 8) + 'px'; }
      const right = window.innerWidth - rect.right;
      qualityMenu.style.right = right + 'px'; qualityMenu.style.left = 'auto';
    }
    qualityBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const willOpen = !qualityMenu.classList.contains('open');
      qualityMenu.classList.toggle('open');
      if (willOpen) positionQualityMenu();
    });
    document.addEventListener('click', () => qualityMenu.classList.remove('open'));
    buildQualityMenu();

    function showCodecError() {
      errorMsg.classList.add('visible');
      errorDetail.innerHTML = \`Preview sandbox doesn't support H.264.<br>
        <a href="#" target="_blank" rel="noopener noreferrer" onclick="this.href=location.href"
           style="color:#ff6666;font-weight:700;text-decoration:underline;cursor:pointer;font-size:13px;">
          &#8599; Open in Full Browser Tab to Watch
        </a>\`;
    }

    function playStream(channel) {
      activeId = channel.id;
      document.querySelectorAll('.channel-item').forEach(el => {
        el.classList.toggle('active', String(el.dataset.id) === String(channel.id));
      });
      currentServers = channel.servers && channel.servers.length > 0
        ? channel.servers
        : [{ id: channel.id, url: channel.stream_url }];
      currentActiveServerIdx = 0;
      renderServerBar();
      playFromUrl(currentServers[0].url);
      if (typeof window._startViewerTracking === 'function') window._startViewerTracking(channel.id, channel.name || channel.channel_name || null);
      saveRecent(channel);
      localStorage.setItem('miz_last_priv_ch', String(channel.id));
      renderRecent();
    }

    /* ── Favourites ── */
    function getFavs() { try { return JSON.parse(localStorage.getItem('miz_priv_favorites')||'[]'); } catch(e) { return []; } }
    function saveFavs(a) { localStorage.setItem('miz_priv_favorites', JSON.stringify(a)); }
    function isFav(id) { return getFavs().includes(String(id)); }
    function toggleFav(id, ev) {
      if (ev) ev.stopPropagation();
      const favs = getFavs();
      const key = String(id);
      const idx = favs.indexOf(key);
      if (idx >= 0) favs.splice(idx, 1); else favs.push(key);
      saveFavs(favs);
      document.querySelectorAll('.fav-btn[data-id="' + id + '"]').forEach(b => {
        b.textContent = favs.includes(key) ? '\u2764\uFE0F' : '\uD83E\uDD0D';
        b.classList.toggle('active', favs.includes(key));
      });
    }

    /* ── Recently Watched ── */
    const RECENT_KEY_P = 'miz_priv_recent';
    function getRecent() { try { return JSON.parse(localStorage.getItem(RECENT_KEY_P)||'[]'); } catch(e) { return []; } }
    function saveRecent(ch) {
      let r = getRecent().filter(x => String(x.id) !== String(ch.id));
      r.unshift({ id: String(ch.id), name: ch.name || ch.channel_name || '' });
      if (r.length > 10) r = r.slice(0, 10);
      localStorage.setItem(RECENT_KEY_P, JSON.stringify(r));
    }
    function renderRecent() {
      const recentSec = document.getElementById('recent-section');
      const recentRow = document.getElementById('recent-row');
      if (!recentSec || !recentRow) return;
      const r = getRecent();
      if (!r.length) { recentSec.style.display = 'none'; return; }
      recentSec.style.display = '';
      recentRow.innerHTML = r.map(c =>
        '<div class="recent-chip" data-id="' + c.id + '">' + c.name + '</div>'
      ).join('');
      recentRow.querySelectorAll('.recent-chip').forEach(chip => {
        chip.addEventListener('click', () => {
          const ch = allChannels.find(x => String(x.id) === chip.dataset.id);
          if (ch) { history.pushState({ chId: ch.id }, '', '/private-watch?ch=' + ch.id); playStream(ch); }
        });
      });
    }

    /* ── Logo / HD helpers ── */
    const _P_LOGO_COLORS = ['#c0392b','#8e44ad','#2980b9','#16a085','#d35400','#1a5276','#6c3483','#1e8449'];
    function _pLogoColor(name) { let h=0; for(let i=0;i<name.length;i++) h=(h*31+name.charCodeAt(i))&0xffff; return _P_LOGO_COLORS[h%_P_LOGO_COLORS.length]; }
    function _pInitials(name) { return (name||'?').split(/\s+/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase()||'?'; }
    function isHD_p(name) { return /\b(HD|FHD|4K|1080[pi]?|720p)\b/i.test(name||''); }

    function renderChannels(list) {
      channelList.innerHTML = '';
      if (!list.length) {
        channelList.innerHTML = '<div class="no-results">No channels found.</div>';
        return;
      }
      const frag = document.createDocumentFragment();
      const favs = getFavs();
      list.forEach(ch => {
        const isFailed = failedIds.has(String(ch.id)) || isGeoName(ch.name || '');
        const item = document.createElement('div');
        item.className = 'channel-item' + (String(ch.id) === String(activeId) ? ' active' : '') + (isFailed ? ' ch-failed' : '');
        item.dataset.id = ch.id;
        const hdBadge = isHD_p(ch.name) ? '<span class="hd-badge">HD</span>' : '';
        const favActive = favs.includes(String(ch.id));
        item.innerHTML =
          '<div class="channel-left">' +
            '<img class="ch-logo-thumb" alt="" />' +
            '<div class="ch-logo-fb" style="background:' + _pLogoColor(ch.name || '?') + ';display:none">' + _pInitials(ch.name) + '</div>' +
            '<span class="ch-name">' + (ch.name || '') + '</span>' +
            hdBadge +
          '</div>' +
          '<div class="ch-right">' +
            '<button class="fav-btn' + (favActive?' active':'') + '" data-id="' + ch.id + '" title="Favourite">' + (favActive?'\u2764\uFE0F':'\uD83E\uDD0D') + '</button>' +
            '<span class="ch-badge ' + (ch.status==='Online'?'online':'offline') + '">' + (ch.status||'') + '</span>' +
            '<button class="play-btn">&#9654;</button>' +
          '</div>';
        const imgEl = item.querySelector('.ch-logo-thumb');
        const fbEl  = item.querySelector('.ch-logo-fb');
        if (ch.logo) {
          imgEl.src = ch.logo;
          imgEl.onerror = () => { imgEl.style.display='none'; fbEl.style.display='flex'; };
        } else {
          imgEl.style.display = 'none'; fbEl.style.display = 'flex';
        }
        const switchChannel = () => {
          history.pushState({ chId: ch.id }, '', '/private-watch?ch=' + ch.id);
          playStream(ch);
        };
        item.querySelector('.fav-btn').addEventListener('click', e => toggleFav(ch.id, e));
        item.querySelector('.play-btn').addEventListener('click', e => { e.stopPropagation(); switchChannel(); });
        item.addEventListener('click', switchChannel);
        frag.appendChild(item);
      });
      channelList.appendChild(frag);
    }

    function applyFilter() {
      const q = searchInput.value.toLowerCase().trim();
      let list = q ? allChannels.filter(c => (c.name || '').toLowerCase().includes(q)) : allChannels;
      if (hideUnavailable) list = list.filter(c => !failedIds.has(String(c.id)) && !isGeoName(c.name || ''));
      chCount.textContent = list.length + ' channels';
      renderChannels(list);
    }

    searchInput.addEventListener('input', applyFilter);

    window.addEventListener('popstate', (e) => {
      const idStr = e.state && e.state.chId
        ? String(e.state.chId)
        : new URLSearchParams(window.location.search).get('ch');
      if (idStr) {
        const ch = allChannels.find(c => String(c.id) === idStr);
        if (ch) playStream(ch);
      } else {
        window.location.href = '/private-tv';
      }
    });

    async function loadChannels() {
      try {
        const tok = localStorage.getItem('miz_token');
        const r = await fetch('/api/user/private-channels', { headers: { Authorization: 'Bearer ' + tok } });
        if (r.status === 401) { window.location.replace('/'); return; }
        const data = await r.json();
        allChannels = data.channels || [];
        const dbg = document.getElementById('_dbg');
        if (dbg) dbg.textContent = 'API OK: ' + allChannels.length + ' groups | firstId=' + (allChannels[0] && allChannels[0].id);
        console.log('[PW] loadChannels: total channels =', allChannels.length);
        renderRecent();
        applyFilter();
        const urlChStr = new URLSearchParams(window.location.search).get('ch');
        console.log('[PW] urlChStr =', urlChStr, '| first channel id =', allChannels[0] && allChannels[0].id, '| type =', allChannels[0] && typeof allChannels[0].id);
        if (urlChStr) {
          const ch = allChannels.find(c => String(c.id) === urlChStr);
          console.log('[PW] channel found?', !!ch, ch && ch.name, '| stream_url =', ch && (ch.servers && ch.servers[0] && ch.servers[0].url || ch.stream_url));
          if (ch) {
            if (dbg) dbg.textContent = 'FOUND ch=' + urlChStr + ': ' + ch.name + ' | URL: ' + (ch.servers && ch.servers[0] && ch.servers[0].url || ch.stream_url || '(none)').slice(0,60);
            playStream(ch); return;
          }
          if (dbg) dbg.textContent = 'NOT FOUND ch=' + urlChStr + ' in ' + allChannels.length + ' groups. firstId=' + (allChannels[0] && allChannels[0].id) + ' — playing first channel instead';
          console.warn('[PW] channel NOT found — playing first channel');
          if (allChannels.length) { history.replaceState({}, '', '/private-watch?ch=' + allChannels[0].id); playStream(allChannels[0]); return; }
        }
        /* Resume watching */
        const lastId = localStorage.getItem('miz_last_priv_ch');
        if (lastId) {
          const lastCh = allChannels.find(c => String(c.id) === lastId);
          if (lastCh) {
            const resumeBar = document.getElementById('resume-bar');
            const resumeLabel = document.getElementById('resume-label');
            if (resumeBar && resumeLabel) {
              resumeLabel.textContent = '\uD83D\uDCFA Resume: ' + (lastCh.name || '') + '?';
              resumeBar.classList.add('show');
              document.getElementById('resume-yes').onclick = () => {
                resumeBar.classList.remove('show');
                history.pushState({ chId: lastCh.id }, '', '/private-watch?ch=' + lastCh.id);
                playStream(lastCh);
              };
              document.getElementById('resume-dismiss').onclick = () => resumeBar.classList.remove('show');
              return;
            }
          }
        }
        if (allChannels.length) { playStream(allChannels[0]); }
      } catch(e) {
        console.error('[PW] loadChannels error:', e);
        channelList.innerHTML = '<div style="color:#a33;font-size:13px;padding:14px;">Failed to load channels.</div>';
      }
    }

    function renderAuthUI(user, role) {
      const area = document.getElementById('auth-area');
      if (!area) return;
      if (user) {
        const isAdmin = role === 'admin';
        area.innerHTML = '<div class="user-menu">' +
          '<button class="auth-btn" id="user-btn">👤 ' + user.email.split('@')[0] + ' ▾</button>' +
          '<div class="user-drop" id="user-drop">' +
          '<div class="user-email-line">' + user.email + '</div>' +
          (isAdmin ? '<a href="/admin" class="user-item">⚙️ Admin Panel</a>' : '') +
          '<a href="/" class="user-item">🏠 Main Page</a>' +
          '<a href="/private-tv" class="user-item">🔒 Secret TV</a>' +
          '<div class="user-item danger" id="logout-btn">🚪 Logout</div></div></div>';
        document.getElementById('user-btn').addEventListener('click', e => {
          e.stopPropagation();
          document.getElementById('user-drop').classList.toggle('open');
        });
        document.addEventListener('click', () => { const d = document.getElementById('user-drop'); if(d) d.classList.remove('open'); });
        document.getElementById('logout-btn').addEventListener('click', () => {
          localStorage.removeItem('miz_token'); localStorage.removeItem('miz_user');
          localStorage.removeItem('miz_refresh');
          window.location.replace('/');
        });
        const tok = localStorage.getItem('miz_token');
        let _currentChId = null, _currentChName = null, _hbTimer = null;
        function sendHeartbeat() {
          if (!_currentChId) return;
          fetch('/api/track/heartbeat', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tok }, body: JSON.stringify({ ch: _currentChId, chName: _currentChName }) }).catch(()=>{});
        }
        if (isAdmin) {
          const badge = document.getElementById('viewer-badge');
          const countEl = document.getElementById('viewer-count');
          let _pollTimer = null;
          function pollViewers() {
            if (!_currentChId) return;
            fetch('/api/admin/viewers/' + _currentChId, { headers: { 'Authorization': 'Bearer ' + tok } })
              .then(r => r.json()).then(d => { if (countEl) countEl.textContent = d.count ?? '—'; }).catch(()=>{});
          }
          window._startViewerTracking = function(chId, chName) {
            _currentChId = chId; _currentChName = chName || null;
            if (badge) badge.style.display = 'flex';
            if (countEl) countEl.textContent = '—';
            clearInterval(_hbTimer); clearInterval(_pollTimer);
            sendHeartbeat(); pollViewers();
            _hbTimer = setInterval(sendHeartbeat, 30000);
            _pollTimer = setInterval(pollViewers, 30000);
          };
        } else {
          window._startViewerTracking = function(chId, chName) {
            _currentChId = chId; _currentChName = chName || null;
            clearInterval(_hbTimer);
            sendHeartbeat();
            _hbTimer = setInterval(sendHeartbeat, 30000);
          };
        }
      }
    }
    async function initAuth() {
      const token = localStorage.getItem('miz_token');
      if (!token) { window.location.replace('/'); return; }
      try {
        const r = await fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } });
        const d = await r.json();
        if (d.user) { renderAuthUI(d.user, d.user.role); }
        else { localStorage.removeItem('miz_token'); window.location.replace('/'); return; }
      } catch(_) {}
    }
    initAuth().then(() => loadChannels());
  </script>
</body>
</html>`);
});

app.get('/watch', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MIZ Live TV</title>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0a0a0a; color: #fff;
      font-family: 'Segoe UI', Arial, sans-serif;
      min-height: 100vh; display: flex; flex-direction: column;
      align-items: center; padding: 16px 12px 60px;
    }
    header {
      width: 100%; max-width: 960px;
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    header h1 { font-size: 18px; font-weight: 700; letter-spacing: 1px; display:flex; align-items:center; gap:8px; }
    header h1 a { color: inherit; text-decoration: none; display:flex; align-items:center; gap:8px; }
    header h1 a:hover { opacity: 0.8; }
    .open-btn {
      background: #222; color: #aaa; border: 1px solid #333;
      border-radius: 6px; padding: 6px 12px; font-size: 12px;
      cursor: pointer; text-decoration: none;
      transition: background 0.2s, color 0.2s;
    }
    .open-btn:hover { background: #e00; color: #fff; border-color: #e00; }

    /* Player */
    .player-wrapper {
      position: relative; width: 100%; max-width: 960px;
      background: #000; border-radius: 10px; overflow: hidden;
      box-shadow: 0 0 50px rgba(220,0,0,0.18);
      cursor: pointer;
    }
    .player-wrapper:fullscreen,
    .player-wrapper:-webkit-full-screen {
      border-radius: 0; max-width: 100%;
    }
    video {
      width: 100%; display: block; background: #000;
      aspect-ratio: 16/9;
    }

    /* Center play/pause ripple */
    .center-icon {
      position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
      width: 64px; height: 64px; border-radius: 50%;
      background: rgba(0,0,0,0.55);
      display: flex; align-items: center; justify-content: center;
      z-index: 12; opacity: 0; pointer-events: none;
      transition: opacity 0.25s;
    }
    .center-icon svg { width: 30px; height: 30px; fill: #fff; }
    .center-icon.show { opacity: 1; }

    /* Bottom controls bar */
    .controls-bar {
      position: absolute; bottom: 0; left: 0; right: 0;
      background: linear-gradient(to top, rgba(0,0,0,0.85) 0%, transparent 100%);
      padding: 28px 14px 10px;
      display: flex; flex-direction: column; gap: 6px;
      z-index: 10;
      opacity: 0; transition: opacity 0.3s;
      pointer-events: none;
    }
    .player-wrapper.controls-visible .controls-bar { opacity: 1; pointer-events: auto; }
    .player-wrapper.controls-hidden * { cursor: none; }
    .viewer-badge {
      position: absolute; top: 12px; right: 12px;
      background: rgba(0,0,0,0.65); color: #fff;
      font-size: 12px; font-weight: 600; padding: 4px 9px;
      border-radius: 20px; pointer-events: none;
      display: flex; align-items: center; gap: 5px;
      backdrop-filter: blur(4px); z-index: 20;
      opacity: 0; transition: opacity 0.3s;
    }
    .player-wrapper.controls-visible .viewer-badge { opacity: 1; }

    /* Progress / live bar */
    .progress-row {
      display: flex; align-items: center; gap: 8px;
    }
    .live-line {
      flex: 1; height: 4px; background: rgba(255,255,255,0.2);
      border-radius: 2px; position: relative; overflow: hidden;
    }
    .live-line-fill {
      position: absolute; left: 0; top: 0; bottom: 0;
      width: 100%; background: #e00; border-radius: 2px;
      animation: live-pulse 2s ease-in-out infinite;
    }
    @keyframes live-pulse {
      0%,100% { opacity: 1; }
      50%      { opacity: 0.5; }
    }
    .live-tag {
      font-size: 10px; font-weight: 700; color: #e00;
      letter-spacing: 1px; flex-shrink: 0;
    }

    /* Buttons row */
    .btns-row {
      display: flex; align-items: center; gap: 4px;
    }
    .ctrl-btn {
      background: none; border: none; cursor: pointer;
      color: #fff; padding: 5px; border-radius: 4px;
      display: flex; align-items: center; justify-content: center;
      transition: background 0.15s, transform 0.1s;
      flex-shrink: 0;
    }
    .ctrl-btn:hover { background: rgba(255,255,255,0.15); }
    .ctrl-btn:active { transform: scale(0.9); }
    .ctrl-btn svg { width: 20px; height: 20px; fill: #fff; display: block; }

    /* Volume slider */
    .vol-wrap { display: flex; align-items: center; gap: 6px; }
    .vol-slider {
      -webkit-appearance: none; appearance: none;
      width: 70px; height: 4px; border-radius: 2px; outline: none; cursor: pointer;
      background: linear-gradient(to right, #fff 0%, #fff var(--vol,100%), rgba(255,255,255,0.3) var(--vol,100%), rgba(255,255,255,0.3) 100%);
    }
    .vol-slider::-webkit-slider-thumb {
      -webkit-appearance: none; width: 12px; height: 12px;
      border-radius: 50%; background: #fff; cursor: pointer;
    }

    .spacer { flex: 1; }
    .kbps-label { font-size: 11px; color: rgba(255,255,255,0.5); flex-shrink: 0; }

    /* Quality menu */
    .quality-wrap { position: relative; flex-shrink: 0; }
    .quality-btn {
      background: none; border: none; cursor: pointer; color: #fff;
      font-size: 11px; font-weight: 700; padding: 5px 8px;
      border-radius: 4px; display: flex; align-items: center; gap: 4px;
      transition: background 0.15s;
      white-space: nowrap;
    }
    .quality-btn:hover { background: rgba(255,255,255,0.15); }
    .quality-btn svg { width: 16px; height: 16px; fill: #fff; flex-shrink: 0; }
    .quality-menu {
      position: fixed;
      background: rgba(20,20,20,0.97); border: 1px solid #333;
      border-radius: 8px; overflow-y: auto; overflow-x: hidden;
      z-index: 9999; display: none; flex-direction: column;
      min-width: 140px; max-height: 260px;
      box-shadow: 0 6px 24px rgba(0,0,0,0.7);
      scrollbar-width: thin; scrollbar-color: #444 transparent;
    }
    .quality-menu::-webkit-scrollbar { width: 4px; }
    .quality-menu::-webkit-scrollbar-track { background: transparent; }
    .quality-menu::-webkit-scrollbar-thumb { background: #444; border-radius: 2px; }
    .quality-menu.open { display: flex; }
    .quality-menu-title {
      padding: 8px 14px 6px; font-size: 10px; font-weight: 700;
      color: #666; text-transform: uppercase; letter-spacing: 1px;
      border-bottom: 1px solid #252525;
    }
    .quality-option {
      padding: 9px 14px; font-size: 13px; color: #ccc;
      cursor: pointer; border-bottom: 1px solid #1e1e1e;
      transition: background 0.12s, color 0.12s;
      display: flex; align-items: center; justify-content: space-between; gap: 10px;
    }
    .quality-option:last-child { border-bottom: none; }
    .quality-option:hover { background: rgba(255,255,255,0.08); color: #fff; }
    .quality-option.active { color: #fff; font-weight: 700; }
    .quality-option .q-check { color: #e00; font-size: 14px; line-height: 1; }

    /* Loading / Error overlays */
    #loading-msg {
      display: none; position: absolute; inset: 0;
      background: rgba(0,0,0,0.72);
      align-items: center; justify-content: center;
      flex-direction: column; gap: 10px; z-index: 15; pointer-events: none;
    }
    #loading-msg.visible { display: flex; }
    .spinner {
      width: 38px; height: 38px;
      border: 3px solid #333; border-top-color: #e00;
      border-radius: 50%; animation: spin .8s linear infinite;
    }
    #error-msg {
      display: none; position: absolute; inset: 0;
      background: rgba(0,0,0,0.85); color: #bbb; font-size: 14px;
      align-items: center; justify-content: center;
      flex-direction: column; gap: 12px; text-align: center; padding: 24px; z-index: 20;
    }
    #error-msg.visible { display: flex; }
    #error-msg .icon { font-size: 36px; }

    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.3;transform:scale(.6)} }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* Status bar */
    .status-bar {
      width: 100%; max-width: 960px;
      display: flex; align-items: center; justify-content: space-between;
      padding: 8px 2px; font-size: 12px; color: #555;
    }
    .status-indicator { display: flex; align-items: center; gap: 6px; }
    .status-dot { width: 7px; height: 7px; border-radius: 50%; background: #2a2; }
    .status-dot.offline { background: #a33; }
    .status-dot.loading { background: #fa0; animation: pulse 1s infinite; }

    /* Server bar */
    .server-bar {
      width: 100%; max-width: 960px;
      display: flex; flex-wrap: wrap; align-items: center; gap: 8px;
      padding: 6px 0 2px;
    }
    .server-bar-label {
      font-size: 11px; font-weight: 700; color: #555;
      text-transform: uppercase; letter-spacing: .5px;
      margin-right: 2px; white-space: nowrap;
    }
    .server-btn {
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 6px; padding: 5px 13px;
      font-size: 11px; font-weight: 600; color: #888;
      cursor: pointer; transition: all .15s; white-space: nowrap;
    }
    .server-btn:hover { border-color: #e00; color: #ddd; background: #1e1e1e; }
    .server-btn.active { background: #e00; border-color: #e00; color: #fff; }

    /* Channel section */
    .channel-section { width: 100%; max-width: 960px; margin-top: 18px; }
    .ch-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 10px;
    }
    .ch-header h2 { font-size: 13px; font-weight: 600; color: #555; letter-spacing: 1px; text-transform: uppercase; }
    .ch-count { font-size: 12px; color: #444; }
    .search-bar {
      width: 100%; background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 9px 14px;
      color: #ddd; font-size: 13px; margin-bottom: 10px;
      outline: none; transition: border-color .2s;
    }
    .search-bar::placeholder { color: #444; }
    .search-bar:focus { border-color: #e00; }
    .channel-list {
      display: flex; flex-direction: column; gap: 6px;
      max-height: 520px; overflow-y: auto;
      scrollbar-width: thin; scrollbar-color: #333 transparent;
    }
    .channel-list::-webkit-scrollbar { width: 5px; }
    .channel-list::-webkit-scrollbar-track { background: transparent; }
    .channel-list::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
    .channel-item {
      display: flex; align-items: center; justify-content: space-between;
      background: #141414; border: 1px solid #1e1e1e;
      border-radius: 8px; padding: 9px 12px;
      cursor: pointer; transition: border-color .15s, background .15s, box-shadow .15s;
      border-left: 3px solid transparent;
    }
    .channel-item:hover { background: #1c1c1c; border-color: #333; border-left-color: #c00; }
    .channel-item.active { background: #1a0000; border-color: #e00; border-left-color: #e00; box-shadow: 0 0 10px rgba(220,0,0,.12); }
    .channel-left { display: flex; align-items: center; gap: 9px; min-width: 0; }
    .ch-logo-thumb { width: 30px; height: 30px; border-radius: 50%; object-fit: contain; background: #1e1e1e; flex-shrink: 0; }
    .ch-logo-fb { width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 800; color: #fff; flex-shrink: 0; }
    .ch-number { font-size: 10px; font-weight: 700; color: #444; min-width: 22px; flex-shrink: 0; }
    .ch-name { font-size: 13px; font-weight: 500; color: #ddd; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ch-right { display: flex; align-items: center; gap: 7px; flex-shrink: 0; }
    .ch-badge { font-size: 9px; font-weight: 700; padding: 2px 6px; border-radius: 3px; text-transform: uppercase; letter-spacing: .5px; }
    .ch-badge.online  { background:#1a3a1a; color:#4c4; border:1px solid #2a5a2a; }
    .ch-badge.offline { background:#3a1a1a; color:#c44; border:1px solid #5a2a2a; }
    .play-btn {
      background: #e00; color: #fff; border: none;
      border-radius: 5px; padding: 5px 11px;
      font-size: 11px; font-weight: 700; cursor: pointer;
      transition: background .15s; white-space: nowrap;
    }
    .play-btn:hover { background: #c00; }
    .no-results { color: #444; font-size: 13px; padding: 16px; text-align: center; }

    /* Category filters */
    .cat-filters { display:flex; gap:5px; flex-wrap:wrap; margin-bottom:10px; }
    .cat-btn { background:#141414; border:1px solid #1e1e1e; color:#666; font-size:11px; font-weight:600; padding:5px 10px; border-radius:20px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cat-btn:hover { border-color:#555; color:#ccc; }
    .cat-btn.active { background:#e00; border-color:#e00; color:#fff; }

    /* Fav button */
    .fav-btn { background:none; border:none; font-size:15px; cursor:pointer; padding:0 4px; line-height:1; opacity:.5; transition:opacity .15s, transform .1s; flex-shrink:0; }
    .fav-btn:hover { opacity:1; }
    .fav-btn.active { opacity:1; }

    /* HD badge */
    .hd-badge { font-size:8px; font-weight:800; background:#1a2a4a; color:#7af; border:1px solid #2a4a7a; border-radius:3px; padding:1px 4px; letter-spacing:.5px; flex-shrink:0; }

    /* Recently Watched */
    .recent-section { margin-bottom:10px; }
    .recent-label { font-size:10px; font-weight:700; color:#555; letter-spacing:.8px; text-transform:uppercase; margin-bottom:6px; }
    .recent-row { display:flex; gap:6px; overflow-x:auto; padding-bottom:4px; scrollbar-width:none; }
    .recent-row::-webkit-scrollbar { display:none; }
    .recent-chip { flex-shrink:0; background:#141414; border:1px solid #1e1e1e; border-radius:8px; padding:5px 10px; font-size:11px; color:#aaa; cursor:pointer; white-space:nowrap; transition:all .15s; }
    .recent-chip:hover { border-color:#e00; color:#fff; }

    /* Share toast */
    #share-toast { position:fixed; bottom:80px; left:50%; transform:translateX(-50%) translateY(20px); background:#222; border:1px solid #333; color:#ccc; font-size:12px; padding:8px 18px; border-radius:20px; z-index:9999; opacity:0; transition:opacity .25s, transform .25s; pointer-events:none; }
    #share-toast.show { opacity:1; transform:translateX(-50%) translateY(0); }

    /* Keyboard shortcut hint */
    #kb-hint { position:fixed; bottom:80px; right:16px; background:#111; border:1px solid #222; border-radius:10px; padding:10px 14px; font-size:11px; color:#555; z-index:9998; opacity:0; transition:opacity .3s; pointer-events:none; line-height:1.8; }
    #kb-hint.show { opacity:1; }
    #kb-hint span { color:#888; }

    /* Resume prompt */
    #resume-bar { display:none; background:#1a1a1a; border:1px solid #2a2a2a; border-radius:10px; padding:10px 14px; margin-bottom:10px; font-size:12px; color:#aaa; align-items:center; justify-content:space-between; gap:10px; }
    #resume-bar.show { display:flex; }
    #resume-bar button { background:#e00; border:none; color:#fff; font-size:11px; font-weight:700; padding:5px 12px; border-radius:6px; cursor:pointer; }
    #resume-dismiss { background:#222 !important; color:#888 !important; }

    /* Grid view */
    #grid-view {
      width: 100%; max-width: 960px;
    }
    .grid-search {
      width: 100%; background: #141414; border: 1px solid #2a2a2a;
      border-radius: 8px; padding: 10px 16px;
      color: #ddd; font-size: 13px; margin-bottom: 16px;
      outline: none; transition: border-color .2s;
    }
    .grid-search::placeholder { color: #444; }
    .grid-search:focus { border-color: #e00; }
    .grid-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    .grid-header h2 { font-size: 13px; font-weight: 600; color: #555; letter-spacing: 1px; text-transform: uppercase; }
    .grid-count { font-size: 12px; color: #444; }
    .grid-channels {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(110px, 1fr));
      gap: 10px;
    }
    .grid-card {
      background: #141414; border: 1px solid #1e1e1e;
      border-radius: 10px; padding: 14px 10px 12px;
      display: flex; flex-direction: column; align-items: center; gap: 10px;
      cursor: pointer; transition: border-color .15s, background .15s, transform .1s;
      text-align: center;
    }
    .grid-card:hover { background: #1c1c1c; border-color: #e00; transform: translateY(-2px); }
    .grid-logo {
      width: 52px; height: 52px; border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: 15px; font-weight: 800; color: #fff;
      flex-shrink: 0; letter-spacing: .5px;
    }
    .grid-logo-img {
      width: 52px; height: 52px; border-radius: 50%;
      object-fit: contain; background: #1e1e1e;
      flex-shrink: 0;
    }
    .grid-name {
      font-size: 11px; font-weight: 500; color: #ccc;
      line-height: 1.3; word-break: break-word;
      display: -webkit-box; -webkit-line-clamp: 2;
      -webkit-box-orient: vertical; overflow: hidden;
    }
    .auth-btn {
      background: #1a1a1a; color: #ccc; border: 1px solid #333;
      border-radius: 8px; padding: 7px 13px; font-size: 12px; font-weight: 600;
      cursor: pointer; text-decoration: none; display: flex; align-items: center; gap: 6px;
      transition: all .15s; white-space: nowrap; flex-shrink: 0;
    }
    .auth-btn:hover { background: #e00; color: #fff; border-color: #e00; }
    .auth-btn.red { background: #e00; color: #fff; border-color: #e00; }
    .user-menu { position: relative; }
    .user-drop {
      position: absolute; right: 0; top: calc(100% + 8px);
      background: #1a1a1a; border: 1px solid #2a2a2a;
      border-radius: 8px; min-width: 185px; z-index: 200; overflow: hidden; display: none;
    }
    .user-drop.open { display: block; }
    .user-email-line { padding: 10px 14px; font-size: 11px; color: #666; border-bottom: 1px solid #2a2a2a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .user-item {
      padding: 10px 14px; font-size: 13px; color: #ccc;
      cursor: pointer; transition: background .1s; display: block; text-decoration: none;
    }
    .user-item:hover { background: #2a2a2a; color: #fff; }
    .user-item.danger:hover { background: #e00; }
    .g-overlay {
      display: none; position: fixed; inset: 0;
      background: rgba(0,0,0,.88); z-index: 9000;
      align-items: center; justify-content: center;
    }
    .g-overlay.show { display: flex; }
    .g-modal {
      background: #141414; border: 1px solid #2a2a2a;
      border-radius: 16px; padding: 36px 28px; max-width: 360px;
      width: 90%; text-align: center;
    }
    .g-modal h3 { font-size: 20px; font-weight: 700; margin-bottom: 10px; }
    .g-modal p { color: #888; font-size: 13px; margin-bottom: 24px; line-height: 1.65; }
    .g-btns { display: flex; gap: 10px; justify-content: center; }
    .g-btn {
      padding: 11px 22px; border-radius: 8px; font-size: 14px;
      font-weight: 700; cursor: pointer; border: none; text-decoration: none; display: inline-block;
    }
    .g-login { background: #e00; color: #fff; }
    .g-login:hover { background: #c00; }
    .g-signup { background: #222; color: #ccc; border: 1px solid #333 !important; }
    .g-signup:hover { background: #2a2a2a; }
  </style>
</head>
<body>

  <header>
    <h1><a href="/" style="display:flex;align-items:center;gap:8px;text-decoration:none;color:inherit">${LOGO_FULL_HTML}</a></h1>
    <div id="auth-area"></div>
  </header>

  <div class="player-wrapper" id="player-wrapper">
    <!-- Top overlay -->
    <!-- Video -->
    <video id="video" autoplay muted playsinline autopictureinpicture></video>

    <!-- Center icon (play/pause feedback) -->
    <div class="center-icon" id="center-icon">
      <svg id="center-svg" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
    </div>

    <!-- Bottom controls -->
    <div class="controls-bar" id="controls-bar">
      <!-- Live progress bar -->
      <div class="progress-row">
        <div class="live-line"><div class="live-line-fill"></div></div>
        <span class="live-tag">LIVE</span>
      </div>
      <!-- Buttons -->
      <div class="btns-row">
        <!-- Play/Pause -->
        <button class="ctrl-btn" id="btn-play" title="Play/Pause">
          <svg id="play-icon" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>
        </button>
        <!-- Volume -->
        <div class="vol-wrap">
          <button class="ctrl-btn" id="btn-mute" title="Mute/Unmute">
            <svg id="vol-icon" viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0 0 14 7.97v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/></svg>
          </button>
          <input class="vol-slider" id="vol-slider" type="range" min="0" max="1" step="0.02" value="1" />
        </div>

        <div class="spacer"></div>

        <!-- kbps -->
        <span class="kbps-label" id="kbps-label"></span>

        <!-- Quality -->
        <div class="quality-wrap">
          <button class="quality-btn" id="quality-btn" title="Quality">
            <svg viewBox="0 0 24 24"><path d="M19.14 12.94c.04-.3.06-.61.06-.94s-.02-.64-.07-.94l2.03-1.58a.49.49 0 0 0 .12-.61l-1.92-3.32a.49.49 0 0 0-.59-.22l-2.39.96a7.02 7.02 0 0 0-1.62-.94l-.36-2.54a.484.484 0 0 0-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96a.47.47 0 0 0-.59.22L2.74 8.87a.47.47 0 0 0 .12.61l2.03 1.58c-.05.3-.07.62-.07.94s.02.64.07.94l-2.03 1.58a.49.49 0 0 0-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.57 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32a.47.47 0 0 0-.12-.61l-2.03-1.58zM12 15.6a3.6 3.6 0 1 1 0-7.2 3.6 3.6 0 0 1 0 7.2z"/></svg>
            <span id="quality-label">Auto</span>
          </button>
          <div class="quality-menu" id="quality-menu">
            <div class="quality-menu-title">Quality</div>
          </div>
        </div>

        <!-- Share -->
        <button class="ctrl-btn" id="btn-share" title="Share Channel">
          <svg viewBox="0 0 24 24"><path d="M18 16.08c-.76 0-1.44.3-1.96.77L8.91 12.7c.05-.23.09-.46.09-.7s-.04-.47-.09-.7l7.05-4.11c.54.5 1.25.81 2.04.81 1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3c0 .24.04.47.09.7L8.04 9.81C7.5 9.31 6.79 9 6 9c-1.66 0-3 1.34-3 3s1.34 3 3 3c.79 0 1.5-.31 2.04-.81l7.12 4.16c-.05.21-.08.43-.08.65 0 1.61 1.31 2.92 2.92 2.92s2.92-1.31 2.92-2.92-1.31-2.92-2.92-2.92z"/></svg>
        </button>

        <!-- Picture-in-Picture -->
        <button class="ctrl-btn" id="btn-pip" title="Picture in Picture" style="display:none">
          <svg id="pip-icon" viewBox="0 0 24 24"><path d="M19 7H9c-1.1 0-2 .9-2 2v6c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V9c0-1.1-.9-2-2-2zm0 8H9V9h10v6zM3 5v14h2V5H3zm4-2v2h12V3H7z"/></svg>
        </button>

        <!-- Fullscreen -->
        <button class="ctrl-btn" id="btn-fs" title="Fullscreen">
          <svg id="fs-icon" viewBox="0 0 24 24"><path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/></svg>
        </button>
      </div>
    </div>

    <!-- Loading -->
    <div id="loading-msg" class="visible">
      <div class="spinner"></div>
      <span style="color:#777;font-size:12px;">Loading stream...</span>
    </div>
    <div class="viewer-badge" id="viewer-badge" style="display:none">👁 <span id="viewer-count">—</span></div>

    <!-- Error -->
    <div id="error-msg">
      <span class="icon">📡</span>
      <strong>Stream Unavailable</strong>
      <span id="error-detail">Could not load this stream.</span>
    </div>
  </div>

  <div class="status-bar" id="status-bar-wrap">
    <div class="status-indicator">
      <span class="status-dot loading" id="dot"></span>
      <span id="status-text">Connecting...</span>
    </div>
    <span id="stream-info"></span>
  </div>

  <div class="server-bar" id="server-bar" style="display:none"></div>

  <div class="channel-section" id="channel-section">
    <div class="ch-header">
      <h2>Channels</h2>
      <span class="ch-count" id="ch-count"></span>
    </div>
    <input class="search-bar" id="search" type="text" placeholder="&#128269;  Search channels..." autocomplete="off" />
    <div class="cat-filters" id="cat-filters">
      <button class="cat-btn active" data-cat="all">🔴 All</button>
      <button class="cat-btn" data-cat="favourites">❤️ Favs</button>
      <button class="cat-btn" data-cat="bangla" id="cnt-bangla">🇧🇩 Bangla</button>
      <button class="cat-btn" data-cat="news" id="cnt-news">📰 News</button>
      <button class="cat-btn" data-cat="movies" id="cnt-movies">🎬 Movies</button>
      <button class="cat-btn" data-cat="music" id="cnt-music">🎵 Music</button>
      <button class="cat-btn" data-cat="kids" id="cnt-kids">👶 Kids</button>
      <button class="cat-btn" data-cat="sports" id="cnt-sports">⚽ Sports</button>
      <button class="cat-btn" data-cat="international" id="cnt-intl">🌍 Intl</button>
    </div>
    <div id="resume-bar">
      <span id="resume-label">📺 Resume watching?</span>
      <div style="display:flex;gap:6px">
        <button id="resume-yes">▶ Resume</button>
        <button id="resume-dismiss">✕</button>
      </div>
    </div>
    <div class="recent-section" id="recent-section" style="display:none">
      <div class="recent-label">🕐 Recently Watched</div>
      <div class="recent-row" id="recent-row"></div>
    </div>
    <div class="channel-list" id="channel-list">
      <div style="color:#444;font-size:13px;padding:14px;">Loading channels...</div>
    </div>
  </div>
  <div id="share-toast">🔗 Link copied!</div>
  <div id="kb-hint">
    <span>Space</span> Play/Pause &nbsp; <span>M</span> Mute &nbsp; <span>F</span> Fullscreen<br>
    <span>↑↓</span> Volume &nbsp; <span>S</span> Share
  </div>

  <script>
    const video        = document.getElementById('video');
    const playerWrap   = document.getElementById('player-wrapper');
    const dot          = document.getElementById('dot');
    const statusText   = document.getElementById('status-text');
    const errorMsg     = document.getElementById('error-msg');
    const errorDetail  = document.getElementById('error-detail');
    const loadingMsg   = document.getElementById('loading-msg');
    const channelList  = document.getElementById('channel-list');
    const searchInput  = document.getElementById('search');
    const chCount      = document.getElementById('ch-count');

    const qualityBtn   = document.getElementById('quality-btn');
    const qualityMenu  = document.getElementById('quality-menu');
    const qualityLabel = document.getElementById('quality-label');
    const kbpsLabel    = document.getElementById('kbps-label');
    const btnPlay      = document.getElementById('btn-play');
    const playIcon     = document.getElementById('play-icon');
    const btnMute      = document.getElementById('btn-mute');
    const volIcon      = document.getElementById('vol-icon');
    const volSlider    = document.getElementById('vol-slider');
    const btnFs        = document.getElementById('btn-fs');
    const fsIcon       = document.getElementById('fs-icon');
    const btnPip       = document.getElementById('btn-pip');
    const centerIcon   = document.getElementById('center-icon');
    const centerSvg    = document.getElementById('center-svg');
    const serverBar    = document.getElementById('server-bar');
    const btnShare     = document.getElementById('btn-share');
    const shareToast   = document.getElementById('share-toast');
    const kbHint       = document.getElementById('kb-hint');
    const resumeBar    = document.getElementById('resume-bar');
    const resumeLabel  = document.getElementById('resume-label');
    const recentSec    = document.getElementById('recent-section');
    const recentRow    = document.getElementById('recent-row');

    let currentHls     = null;
    let activeId       = null;
    let allChannels    = [];
    let activeCategory = 'all';
    let isAutoMode     = true;
    let hideTimer      = null;
    let currentServers = [];
    let currentActiveServerIdx = 0;

    /* ── Favorites ───────────────────────────── */
    function getFavs() { try { return JSON.parse(localStorage.getItem('miz_favorites')||'[]'); } catch(e) { return []; } }
    function saveFavs(a) { localStorage.setItem('miz_favorites', JSON.stringify(a)); }
    function isFav(id) { return getFavs().includes(id); }
    function toggleFav(id, ev) {
      if (ev) ev.stopPropagation();
      const favs = getFavs();
      const idx = favs.indexOf(id);
      if (idx >= 0) favs.splice(idx, 1); else favs.push(id);
      saveFavs(favs);
      document.querySelectorAll('.fav-btn[data-id="' + id + '"]').forEach(b => {
        b.textContent = favs.includes(id) ? '❤️' : '🤍';
        b.classList.toggle('active', favs.includes(id));
      });
      if (activeCategory === 'favourites') applyCategory();
    }

    /* ── Recently Watched ────────────────────── */
    function getRecent() { try { return JSON.parse(localStorage.getItem('miz_recent')||'[]'); } catch(e) { return []; } }
    function saveRecent(ch) {
      let r = getRecent().filter(x => x.id !== ch.id);
      r.unshift({ id: ch.id, name: ch.channel_name });
      if (r.length > 10) r = r.slice(0, 10);
      localStorage.setItem('miz_recent', JSON.stringify(r));
    }
    function renderRecent() {
      const r = getRecent();
      if (!r.length) { recentSec.style.display = 'none'; return; }
      recentSec.style.display = '';
      recentRow.innerHTML = r.map(c =>
        '<div class="recent-chip" data-id="' + c.id + '">' + c.name + '</div>'
      ).join('');
      recentRow.querySelectorAll('.recent-chip').forEach(chip => {
        chip.addEventListener('click', () => {
          const ch = allChannels.find(x => x.id === parseInt(chip.dataset.id));
          if (ch) { history.pushState({ chId: ch.id }, '', '/watch?ch=' + ch.id); playStream(ch); }
        });
      });
    }

    /* ── HD Detection ────────────────────────── */
    function isHD(name) { return /\b(HD|FHD|4K|1080[pi]?|720p)\b/i.test(name); }

    /* ── Category filter ─────────────────────── */
    function getCatForChannel(ch) {
      const cat = (ch.category || '').toLowerCase();
      const name = (ch.channel_name || '').toLowerCase();
      if (cat === 'bangla' || /bangla|bengali|বাংলা/.test(name)) return 'bangla';
      if (cat === 'news' || /news|সংবাদ|খবর/.test(name)) return 'news';
      if (cat === 'movies' || /movie|cinema|film|বিনোদন/.test(name)) return 'movies';
      if (cat === 'music' || /music|গান|সঙ্গীত/.test(name)) return 'music';
      if (cat === 'kids' || /kids|children|cartoon|baby|শিশু/.test(name)) return 'kids';
      if (cat === 'sports' || /sport|cricket|football|খেলা/.test(name)) return 'sports';
      if (cat === 'international') return 'international';
      return 'other';
    }
    function applyCategory() {
      const q = searchInput.value.toLowerCase().trim();
      let list = q ? allChannels.filter(c => c.channel_name.toLowerCase().includes(q)) : allChannels;
      if (activeCategory === 'favourites') {
        const favs = getFavs();
        list = list.filter(c => favs.includes(c.id));
      } else if (activeCategory !== 'all') {
        list = list.filter(c => getCatForChannel(c) === activeCategory);
      }
      chCount.textContent = list.length + ' channels';
      renderChannels(list);
    }
    document.querySelectorAll('.cat-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.cat-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeCategory = btn.dataset.cat;
        applyCategory();
      });
    });
    function updateCategoryCounts() {
      const cats = { bangla:0, news:0, movies:0, music:0, kids:0, sports:0, international:0 };
      allChannels.forEach(c => { const k = getCatForChannel(c); if (cats[k] !== undefined) cats[k]++; });
      const idMap = { bangla:'cnt-bangla', news:'cnt-news', movies:'cnt-movies', music:'cnt-music', kids:'cnt-kids', sports:'cnt-sports', international:'cnt-intl' };
      Object.entries(idMap).forEach(([cat, elId]) => {
        const el = document.getElementById(elId);
        if (el && cats[cat]) { const base = el.textContent.split(' (')[0]; el.textContent = base + ' (' + cats[cat] + ')'; }
      });
    }

    /* ── Share ───────────────────────────────── */
    let _shareTimer = null;
    function shareChannel() {
      if (!activeId) return;
      const url = location.origin + '/watch?ch=' + activeId;
      function _done() {
        shareToast.classList.add('show');
        clearTimeout(_shareTimer);
        _shareTimer = setTimeout(() => shareToast.classList.remove('show'), 2000);
      }
      if (navigator.clipboard) {
        navigator.clipboard.writeText(url).then(_done).catch(() => { _copyFallback(url); _done(); });
      } else { _copyFallback(url); _done(); }
    }
    function _copyFallback(text) {
      const ta = document.createElement('textarea');
      ta.value = text; ta.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta);
    }
    btnShare && btnShare.addEventListener('click', e => { e.stopPropagation(); shareChannel(); });

    /* ── Keyboard Shortcuts ──────────────────── */
    let _kbTimer = null;
    document.addEventListener('keydown', e => {
      const tag = document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      let handled = true;
      if (e.key === ' ' || e.code === 'Space') {
        e.preventDefault();
        if (video.paused) video.play().catch(()=>{}); else video.pause();
      } else if (e.key === 'm' || e.key === 'M') {
        video.muted = !video.muted; volSlider.value = video.muted ? 0 : video.volume;
      } else if (e.key === 'f' || e.key === 'F') {
        btnFs.click();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault(); video.volume = Math.min(1, video.volume + 0.1); video.muted = false; volSlider.value = video.volume;
      } else if (e.key === 'ArrowDown') {
        e.preventDefault(); video.volume = Math.max(0, video.volume - 0.1); volSlider.value = video.volume;
      } else if (e.key === 's' || e.key === 'S') {
        shareChannel();
      } else { handled = false; }
      if (handled) {
        kbHint.classList.add('show');
        clearTimeout(_kbTimer);
        _kbTimer = setTimeout(() => kbHint.classList.remove('show'), 1800);
      }
    });

    function renderServerBar() {
      if (!serverBar) return;
      if (currentServers.length <= 1) {
        serverBar.style.display = 'none';
        serverBar.innerHTML = '';
        return;
      }
      serverBar.style.display = 'flex';
      let html = '<span class="server-bar-label">Server:</span>';
      currentServers.forEach((srv, idx) => {
        const active = idx === currentActiveServerIdx ? ' active' : '';
        html += '<button class="server-btn' + active + '" data-idx="' + idx + '">S' + (idx + 1) + '</button>';
      });
      serverBar.innerHTML = html;
      serverBar.querySelectorAll('.server-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const idx = parseInt(btn.dataset.idx);
          if (idx === currentActiveServerIdx) return;
          currentActiveServerIdx = idx;
          playFromUrl(currentServers[idx].url);
          serverBar.querySelectorAll('.server-btn').forEach((b, i) => b.classList.toggle('active', i === idx));
        });
      });
    }

    function playFromUrl(url) {
      if (currentHls) { currentHls.destroy(); currentHls = null; }
      currentStreamUrl = url;
      resetQuality();
      kbpsLabel.textContent = '';
      errorMsg.classList.remove('visible');
      loadingMsg.classList.add('visible');
      setStatus('loading', 'Connecting...');
      const proxyUrl = '/proxy?url=' + encodeURIComponent(url);
      if (Hls.isSupported()) {
        let _triedDirect = false;
        let _connectTimer = null;
        const _clearConnect = () => { if (_connectTimer) { clearTimeout(_connectTimer); _connectTimer = null; } };
        function _startHls(srcUrl) {
          if (currentHls) { currentHls.destroy(); currentHls = null; }
          const hls = new Hls({ liveSyncDurationCount:3, liveMaxLatencyDurationCount:6, enableWorker:true });
          currentHls = hls;
          hls.loadSource(srcUrl);
          hls.attachMedia(video);
          _clearConnect();
          _connectTimer = setTimeout(() => {
            if (currentHls) { currentHls.destroy(); currentHls = null; }
            if (!_triedDirect && srcUrl === proxyUrl) {
              _triedDirect = true;
              setStatus('loading', 'Retrying direct...');
              loadingMsg.classList.add('visible');
              errorMsg.classList.remove('visible');
              _startHls(url);
            } else {
              loadingMsg.classList.remove('visible');
              setStatus('offline', 'Stream not responding');
              errorDetail.innerHTML = 'Could not connect to stream. The stream may be offline or unreachable.';
              errorMsg.classList.add('visible');
            }
          }, 15000);
          let _stallTimer = null;
          const _clearStall = () => { if (_stallTimer) { clearTimeout(_stallTimer); _stallTimer = null; } };
          function _onTimeUpdate() {
            if (video.currentTime > 0) { _clearStall(); video.removeEventListener('timeupdate', _onTimeUpdate); }
          }
          video.addEventListener('timeupdate', _onTimeUpdate, { passive: true });
          hls.on(Hls.Events.MANIFEST_PARSED, () => {
            _clearConnect();
            loadingMsg.classList.remove('visible');
            setStatus('live', 'Streaming');
            video.muted = true; updateVolIcon();
            video.play().catch(()=>{});
            buildQualityMenu(); updatePlayIcon();
            _stallTimer = setTimeout(() => {
              video.removeEventListener('timeupdate', _onTimeUpdate);
              if (video.currentTime === 0) {
                setStatus('offline', 'Stream not responding');
                errorDetail.innerHTML = 'Stream loaded but no video frames received. Try another server or channel.';
                errorMsg.classList.add('visible');
              }
            }, 12000);
          });
          hls.on(Hls.Events.LEVEL_LOADED, () => {
            if (hls.bandwidthEstimate > 0) kbpsLabel.textContent = Math.round(hls.bandwidthEstimate/1000) + ' kbps';
          });
          hls.on(Hls.Events.ERROR, (_, data) => {
            if (data.fatal) {
              _clearConnect();
              _clearStall();
              video.removeEventListener('timeupdate', _onTimeUpdate);
              hls.destroy();
              if (!_triedDirect && srcUrl === proxyUrl) {
                _triedDirect = true;
                setStatus('loading', 'Retrying direct...');
                loadingMsg.classList.add('visible');
                errorMsg.classList.remove('visible');
                _startHls(url);
                return;
              }
              loadingMsg.classList.remove('visible');
              const isCodec = data.details && data.details.toLowerCase().includes('codec');
              if (isCodec) { setStatus('offline','Codec not supported'); showCodecError(); }
              else {
                markFailed(activeId);
                const isGeo = activeId && allChannels.find(c=>c.id===activeId) &&
                              isGeoName(allChannels.find(c=>c.id===activeId).name || '');
                setStatus('offline', isGeo ? 'Region restricted' : 'Stream unavailable');
                errorDetail.innerHTML = isGeo
                  ? 'This channel is not available in your region.'
                  : 'Could not load this stream. Try another server or channel.';
                errorMsg.classList.add('visible');
              }
            }
          });
        }
        _startHls(proxyUrl);
      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = proxyUrl;
        video.addEventListener('loadedmetadata', () => {
          loadingMsg.classList.remove('visible'); setStatus('live','Streaming');
          video.play().catch(()=>{}); updatePlayIcon();
        }, { once:true });
        video.addEventListener('error', () => {
          loadingMsg.classList.remove('visible'); setStatus('offline','Stream unavailable');
          errorMsg.classList.add('visible');
        }, { once:true });
      } else {
        loadingMsg.classList.remove('visible');
        setStatus('offline','HLS not supported');
        errorDetail.textContent = 'Your browser does not support HLS playback.';
        errorMsg.classList.add('visible');
      }
    }

    /* ── SVG paths ─────────────────────────────── */
    const SVG = {
      play:    'M8 5v14l11-7z',
      pause:   'M6 19h4V5H6v14zm8-14v14h4V5h-4z',
      volFull: 'M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0 0 14 7.97v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z',
      volMute: 'M16.5 12A4.5 4.5 0 0 0 14 7.97v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51A8.796 8.796 0 0 0 21 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06A8.99 8.99 0 0 0 17.73 18 16.4 16.4 0 0 0 19.73 20L21 18.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z',
      fsEnter: 'M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z',
      fsExit:  'M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z'
    };

    /* ── Controls auto-hide ──────────────────── */
    function showControls() {
      playerWrap.classList.add('controls-visible');
      playerWrap.classList.remove('controls-hidden');
      clearTimeout(hideTimer);
      hideTimer = setTimeout(() => {
        if (!video.paused) {
          playerWrap.classList.remove('controls-visible');
          playerWrap.classList.add('controls-hidden');
        }
      }, 3000);
    }
    playerWrap.addEventListener('mousemove', showControls);
    playerWrap.addEventListener('touchstart', showControls, { passive: true });
    playerWrap.addEventListener('touchmove', showControls, { passive: true });

    /* ── Play / Pause ────────────────────────── */
    function updatePlayIcon() {
      playIcon.querySelector('path').setAttribute('d', video.paused ? SVG.play : SVG.pause);
    }
    function flashCenter(paused) {
      centerSvg.querySelector('path').setAttribute('d', paused ? SVG.play : SVG.pause);
      centerIcon.classList.add('show');
      setTimeout(() => centerIcon.classList.remove('show'), 600);
    }
    btnPlay.addEventListener('click', (e) => {
      e.stopPropagation();
      flashCenter(video.paused);
      if (video.paused) { video.play().catch(()=>{}); } else { video.pause(); }
    });
    video.addEventListener('play',  () => { updatePlayIcon(); showControls(); });
    video.addEventListener('pause', () => { updatePlayIcon(); playerWrap.classList.add('controls-visible'); });
    playerWrap.addEventListener('click', (e) => {
      if (e.target === playerWrap || e.target === video) {
        if (playerWrap.classList.contains('controls-visible')) {
          clearTimeout(hideTimer);
          playerWrap.classList.remove('controls-visible');
          playerWrap.classList.add('controls-hidden');
        } else {
          showControls();
        }
      }
    });

    /* ── Volume ──────────────────────────────── */
    function updateVolIcon() {
      volIcon.querySelector('path').setAttribute('d', video.muted || video.volume === 0 ? SVG.volMute : SVG.volFull);
      const pct = video.muted ? 0 : video.volume * 100;
      volSlider.style.setProperty('--vol', pct + '%');
      volSlider.value = video.muted ? 0 : video.volume;
    }
    btnMute.addEventListener('click', (e) => {
      e.stopPropagation();
      video.muted = !video.muted;
      updateVolIcon();
    });
    volSlider.addEventListener('input', (e) => {
      e.stopPropagation();
      video.volume = parseFloat(volSlider.value);
      video.muted  = video.volume === 0;
      updateVolIcon();
    });
    volSlider.addEventListener('click', e => e.stopPropagation());

    /* ── Fullscreen ──────────────────────────── */
    function updateFsIcon() {
      const isFs = !!(document.fullscreenElement || document.webkitFullscreenElement);
      fsIcon.querySelector('path').setAttribute('d', isFs ? SVG.fsExit : SVG.fsEnter);
    }
    btnFs.addEventListener('click', (e) => {
      e.stopPropagation();
      if (!document.fullscreenElement && !document.webkitFullscreenElement) {
        const fsPromise = (playerWrap.requestFullscreen || playerWrap.webkitRequestFullscreen).call(playerWrap);
        if (fsPromise && fsPromise.then) {
          fsPromise.then(() => {
            if (screen.orientation && screen.orientation.lock) {
              screen.orientation.lock('landscape').catch(() => {});
            }
          }).catch(() => {});
        }
      } else {
        (document.exitFullscreen || document.webkitExitFullscreen).call(document);
        if (screen.orientation && screen.orientation.unlock) screen.orientation.unlock();
      }
    });
    document.addEventListener('fullscreenchange', updateFsIcon);
    document.addEventListener('webkitfullscreenchange', updateFsIcon);

    /* ── Picture-in-Picture ──────────────────── */
    const pipSupported = !!document.pictureInPictureEnabled;

    if (pipSupported) {
      btnPip.style.display = '';

      btnPip.addEventListener('click', async (e) => {
        e.stopPropagation();
        try {
          if (document.pictureInPictureElement) {
            await document.exitPictureInPicture();
          } else {
            await video.requestPictureInPicture();
          }
        } catch(err) {}
      });

      video.addEventListener('enterpictureinpicture', () => {
        btnPip.title = 'Exit Picture in Picture';
        btnPip.querySelector('path').setAttribute('d', 'M21 3H3c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H3V5h18v14zm-10-7h8v5h-8z');
      });

      video.addEventListener('leavepictureinpicture', () => {
        btnPip.title = 'Picture in Picture';
        btnPip.querySelector('path').setAttribute('d', 'M19 7H9c-1.1 0-2 .9-2 2v6c0 1.1.9 2 2 2h10c1.1 0 2-.9 2-2V9c0-1.1-.9-2-2-2zm0 8H9V9h10v6zM3 5v14h2V5H3zm4-2v2h12V3H7z');
      });

      video.addEventListener('play', () => {
        try { video.autoPictureInPicture = true; } catch(e) {}
      });
      video.addEventListener('pause', () => {
        try { video.autoPictureInPicture = false; } catch(e) {}
      });

      document.addEventListener('visibilitychange', async () => {
        if (!currentHls && !video.src) return;
        if (document.hidden) {
          if (!video.paused && !document.pictureInPictureElement && video.readyState >= 2) {
            try { await video.requestPictureInPicture(); } catch(e) {}
          }
        } else {
          if (document.pictureInPictureElement === video) {
            try { await document.exitPictureInPicture(); } catch(e) {}
          }
        }
      });
    }

    /* ── Status bar ──────────────────────────── */
    function setStatus(state, label) {
      dot.className = 'status-dot' + (state==='live' ? '' : state==='loading' ? ' loading' : ' offline');
      statusText.textContent = label;
    }

    /* ── Quality ─────────────────────────────── */
    const QUALITY_OPTIONS = [
      { label: '1080p', height: '1080', note: 'HD' },
      { label: '720p',  height: '720',  note: 'HD' },
      { label: '480p',  height: '480',  note: '' },
      { label: '360p',  height: '360',  note: '' },
      { label: '240p',  height: '240',  note: 'Low' },
    ];

    let currentTranscodeSession = null;
    let currentStreamUrl = null;

    async function stopTranscode() {
      if (currentTranscodeSession) {
        fetch('/transcode/stop/' + currentTranscodeSession).catch(()=>{});
        currentTranscodeSession = null;
      }
    }

    function updateMenuActive(activeKey) {
      qualityMenu.querySelectorAll('.quality-option').forEach(opt => {
        const isActive = opt.dataset.key === activeKey;
        opt.classList.toggle('active', isActive);
        let check = opt.querySelector('.q-check');
        if (isActive && !check) {
          check = document.createElement('span');
          check.className = 'q-check'; check.textContent = '✓';
          opt.appendChild(check);
        } else if (!isActive && check) { check.remove(); }
      });
    }

    async function selectTranscodeQuality(height, label) {
      qualityMenu.classList.remove('open');
      qualityLabel.textContent = '⏳ ' + label;

      await stopTranscode();

      try {
        const r = await fetch('/transcode/start?url=' + encodeURIComponent(currentStreamUrl) + '&height=' + height);
        const data = await r.json();
        if (!data.sessionId) throw new Error('No session');

        currentTranscodeSession = data.sessionId;
        const tcUrl = '/transcode/' + data.sessionId + '/index.m3u8';

        if (currentHls) { currentHls.destroy(); currentHls = null; }
        const hls = new Hls({ liveSyncDurationCount:3, liveMaxLatencyDurationCount:6, enableWorker:true });
        currentHls = hls;
        hls.loadSource(tcUrl);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          loadingMsg.classList.remove('visible');
          setStatus('live', 'Streaming · ' + label);
          video.play().catch(()=>{});
          updatePlayIcon();
        });
        hls.on(Hls.Events.ERROR, (_, d) => {
          if (d.fatal) {
            setStatus('offline', 'Transcode error');
            loadingMsg.classList.remove('visible');
            hls.destroy();
          }
        });

        qualityLabel.textContent = label;
        updateMenuActive(height);
      } catch(e) {
        qualityLabel.textContent = 'Auto';
        updateMenuActive('auto');
        setStatus('live', 'Streaming');
      }
    }

    function selectAutoQuality() {
      qualityMenu.classList.remove('open');
      stopTranscode();
      qualityLabel.textContent = 'Auto';
      updateMenuActive('auto');

      if (!currentStreamUrl) return;
      if (currentHls) { currentHls.destroy(); currentHls = null; }
      loadingMsg.classList.add('visible');
      setStatus('loading', 'Connecting...');

      const proxyUrl = '/proxy?url=' + encodeURIComponent(currentStreamUrl);
      const hls = new Hls({ liveSyncDurationCount:3, liveMaxLatencyDurationCount:6, enableWorker:true });
      currentHls = hls;
      hls.loadSource(proxyUrl);
      hls.attachMedia(video);
      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        loadingMsg.classList.remove('visible');
        setStatus('live', 'Streaming');
        video.play().catch(()=>{});
        updatePlayIcon();
      });
      hls.on(Hls.Events.LEVEL_LOADED, () => {
        if (hls.bandwidthEstimate > 0)
          kbpsLabel.textContent = Math.round(hls.bandwidthEstimate/1000) + ' kbps';
      });
      hls.on(Hls.Events.ERROR, (_, d) => {
        if (d.fatal) { loadingMsg.classList.remove('visible'); setStatus('offline','Stream error'); hls.destroy(); }
      });
    }

    function buildQualityMenu() {
      qualityMenu.innerHTML = '<div class="quality-menu-title">Quality</div>';

      const autoOpt = document.createElement('div');
      autoOpt.className = 'quality-option active';
      autoOpt.dataset.key = 'auto';
      autoOpt.innerHTML = '<span>Auto</span><span class="q-check">✓</span>';
      autoOpt.addEventListener('click', (e) => { e.stopPropagation(); selectAutoQuality(); });
      qualityMenu.appendChild(autoOpt);

      QUALITY_OPTIONS.forEach(q => {
        const opt = document.createElement('div');
        opt.className = 'quality-option';
        opt.dataset.key = q.height;
        const noteHtml = q.note ? ' <span style="font-size:10px;color:#666;margin-left:4px;">' + q.note + '</span>' : '';
        opt.innerHTML = '<span>' + q.label + noteHtml + '</span>';
        opt.addEventListener('click', (e) => { e.stopPropagation(); selectTranscodeQuality(q.height, q.label); });
        qualityMenu.appendChild(opt);
      });

      qualityLabel.textContent = 'Auto';
    }

    function resetQuality() {
      stopTranscode();
      qualityMenu.classList.remove('open');
      buildQualityMenu();
      qualityLabel.textContent = 'Auto';
      isAutoMode = true;
    }

    function positionQualityMenu() {
      const rect = qualityBtn.getBoundingClientRect();
      const menuH = Math.min(260, qualityMenu.scrollHeight);
      const spaceAbove = rect.top;
      const spaceBelow = window.innerHeight - rect.bottom;
      if (spaceAbove >= menuH + 8 || spaceAbove > spaceBelow) {
        qualityMenu.style.top = (rect.top - menuH - 8) + 'px';
      } else {
        qualityMenu.style.top = (rect.bottom + 8) + 'px';
      }
      const right = window.innerWidth - rect.right;
      qualityMenu.style.right = right + 'px';
      qualityMenu.style.left = 'auto';
    }

    qualityBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const willOpen = !qualityMenu.classList.contains('open');
      qualityMenu.classList.toggle('open');
      if (willOpen) positionQualityMenu();
    });
    document.addEventListener('click', () => qualityMenu.classList.remove('open'));

    buildQualityMenu();

    /* ── showCodecError ──────────────────────── */
    function showCodecError() {
      errorMsg.classList.add('visible');
      errorDetail.innerHTML = \`
        Preview sandbox doesn't support H.264.<br>
        <a href="#" target="_blank" rel="noopener noreferrer" onclick="this.href=location.href"
           style="color:#ff6666;font-weight:700;text-decoration:underline;cursor:pointer;font-size:13px;">
          &#8599; Open in Full Browser Tab to Watch
        </a>
      \`;
    }

    /* ── playStream ──────────────────────────── */
    function playStream(channel) {
      activeId = channel.id;
      document.querySelectorAll('.channel-item').forEach(el => {
        el.classList.toggle('active', parseInt(el.dataset.id) === channel.id);
      });
      currentServers = channel.servers && channel.servers.length > 0
        ? channel.servers
        : [{ id: channel.id, url: channel.stream_url }];
      currentActiveServerIdx = 0;
      renderServerBar();
      playFromUrl(currentServers[0].url);
      if (typeof window._startViewerTracking === 'function') window._startViewerTracking(channel.id, channel.channel_name || channel.name || null);
      saveRecent(channel);
      localStorage.setItem('miz_last_ch', channel.id);
      renderRecent();
    }

    /* ── renderChannels ──────────────────────── */
    const LOGO_COLORS_W = ['#c0392b','#8e44ad','#2980b9','#16a085','#d35400','#1a5276','#6c3483','#1e8449'];
    function _wLogoColor(name) { let h=0; for(let i=0;i<name.length;i++) h=(h*31+name.charCodeAt(i))&0xffff; return LOGO_COLORS_W[h%LOGO_COLORS_W.length]; }
    function _wInitials(name) { return name.split(/\s+/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase()||'?'; }

    function renderChannels(list) {
      channelList.innerHTML = '';
      if (!list.length) {
        channelList.innerHTML = '<div class="no-results">No channels found.</div>';
        return;
      }
      const frag = document.createDocumentFragment();
      const favs = getFavs();
      list.forEach(ch => {
        const item = document.createElement('div');
        item.className = 'channel-item' + (ch.id === activeId ? ' active' : '');
        item.dataset.id = ch.id;
        const hdBadge = isHD(ch.channel_name) ? '<span class="hd-badge">HD</span>' : '';
        const favActive = favs.includes(ch.id);
        item.innerHTML =
          '<div class="channel-left">' +
            '<img class="ch-logo-thumb" data-chid="' + ch.id + '" alt="" />' +
            '<div class="ch-logo-fb" data-fbid="' + ch.id + '" style="background:' + _wLogoColor(ch.channel_name) + ';display:none">' + _wInitials(ch.channel_name) + '</div>' +
            '<span class="ch-name">' + ch.channel_name + '</span>' +
            hdBadge +
          '</div>' +
          '<div class="ch-right">' +
            '<button class="fav-btn' + (favActive?' active':'') + '" data-id="' + ch.id + '" title="Favourite">' + (favActive?'❤️':'🤍') + '</button>' +
            '<span class="ch-badge ' + (ch.status==='Online'?'online':'offline') + '">' + ch.status + '</span>' +
            '<button class="play-btn">&#9654;</button>' +
          '</div>';
        const imgEl = item.querySelector('.ch-logo-thumb');
        const fbEl  = item.querySelector('.ch-logo-fb');
        if (ch.logo) {
          imgEl.src = ch.logo;
          imgEl.onerror = () => { imgEl.style.display='none'; fbEl.style.display='flex'; };
        } else {
          imgEl.style.display = 'none'; fbEl.style.display = 'flex';
        }
        const switchChannel = () => {
          history.pushState({ chId: ch.id }, '', '/watch?ch=' + ch.id);
          playStream(ch);
        };
        item.querySelector('.fav-btn').addEventListener('click', e => toggleFav(ch.id, e));
        item.querySelector('.play-btn').addEventListener('click', e => { e.stopPropagation(); switchChannel(); });
        item.addEventListener('click', switchChannel);
        frag.appendChild(item);
      });
      channelList.appendChild(frag);
    }

    searchInput.addEventListener('input', () => { applyCategory(); });

    window.addEventListener('popstate', (e) => {
      const id = e.state && e.state.chId
        ? e.state.chId
        : parseInt(new URLSearchParams(window.location.search).get('ch'));
      if (id) {
        const ch = allChannels.find(c => c.id === id);
        if (ch) playStream(ch);
      } else {
        window.location.href = '/';
      }
    });

    async function loadChannels() {
      try {
        const tok = localStorage.getItem('miz_token');
        const headers = tok ? { Authorization: 'Bearer ' + tok } : {};
        const r = await fetch('/channels', { headers });
        const data = await r.json();
        allChannels = data.channels;
        chCount.textContent = allChannels.length + ' channels';
        updateCategoryCounts();
        renderRecent();
        renderChannels(allChannels);
        const urlChId = parseInt(new URLSearchParams(window.location.search).get('ch'));
        if (urlChId) {
          const ch = allChannels.find(c => c.id === urlChId);
          if (ch) { playStream(ch); return; }
        }
        /* ── Resume watching ── */
        const lastId = parseInt(localStorage.getItem('miz_last_ch') || '0');
        if (lastId) {
          const lastCh = allChannels.find(c => c.id === lastId);
          if (lastCh) {
            resumeLabel.textContent = '📺 Resume: ' + lastCh.channel_name + '?';
            resumeBar.classList.add('show');
            document.getElementById('resume-yes').onclick = () => {
              resumeBar.classList.remove('show');
              history.pushState({ chId: lastCh.id }, '', '/watch?ch=' + lastCh.id);
              playStream(lastCh);
            };
            document.getElementById('resume-dismiss').onclick = () => resumeBar.classList.remove('show');
            return;
          }
        }
        window.location.href = '/';
      } catch(e) {
        channelList.innerHTML = '<div style="color:#a33;font-size:13px;padding:14px;">Failed to load channels.</div>';
      }
    }

    /* ── Auth & Guest Timer ────────────────────── */
    const GUEST_LIMIT = ${(parseInt(appConfig.guest_limit_minutes) || 5) * 60 * 1000};
    const GUEST_MIN_LABEL = '${parseInt(appConfig.guest_limit_minutes) || 5}';
    let _guestTimer = null;
    let _guestUsed = parseInt(localStorage.getItem('miz_guest_time') || '0');
    let _isAuth = false;

    function showGuestModal() { document.getElementById('g-overlay').classList.add('show'); }
    function startGuestTimer() {
      if (_isAuth || _guestTimer) return;
      const start = Date.now() - _guestUsed;
      _guestTimer = setInterval(() => {
        _guestUsed = Date.now() - start;
        localStorage.setItem('miz_guest_time', _guestUsed);
        if (_guestUsed >= GUEST_LIMIT) {
          clearInterval(_guestTimer);
          video.pause();
          showGuestModal();
        }
      }, 1000);
    }

    video.addEventListener('play', () => { if (!_isAuth) startGuestTimer(); });
    video.addEventListener('play', () => {
      if (activeId) fetch('/api/track/view', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ch: activeId }) }).catch(()=>{});
    });

    function renderAuthUI(user, role) {
      const area = document.getElementById('auth-area');
      if (!area) return;
      if (user) {
        _isAuth = true;
        if (_guestTimer) { clearInterval(_guestTimer); _guestTimer = null; }
        localStorage.removeItem('miz_guest_time');
        _guestUsed = 0;
        const isAdmin = role === 'admin';
        area.innerHTML = '<div class="user-menu">' +
          '<button class="auth-btn" id="user-btn">👤 ' + user.email.split('@')[0] + ' ▾</button>' +
          '<div class="user-drop" id="user-drop">' +
          '<div class="user-email-line">' + user.email + '</div>' +
          (isAdmin ? '<a href="/admin" class="user-item">⚙️ Admin Panel</a>' : '') +
          '<div class="user-item danger" id="logout-btn">🚪 Logout</div></div></div>';
        document.getElementById('user-btn').addEventListener('click', e => {
          e.stopPropagation();
          document.getElementById('user-drop').classList.toggle('open');
        });
        document.addEventListener('click', () => { const d = document.getElementById('user-drop'); if(d) d.classList.remove('open'); });
        document.getElementById('logout-btn').addEventListener('click', () => {
          localStorage.removeItem('miz_token'); localStorage.removeItem('miz_user');
          localStorage.removeItem('miz_refresh'); localStorage.removeItem('miz_guest_time');
          window.location.reload();
        });
        const tok = localStorage.getItem('miz_token');
        let _currentChId = null, _currentChName = null, _hbTimer = null;
        function sendHeartbeat() {
          if (!_currentChId) return;
          fetch('/api/track/heartbeat', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tok }, body: JSON.stringify({ ch: _currentChId, chName: _currentChName }) }).catch(()=>{});
        }
        if (isAdmin) {
          const badge = document.getElementById('viewer-badge');
          const countEl = document.getElementById('viewer-count');
          let _pollTimer = null;
          function pollViewers() {
            if (!_currentChId) return;
            fetch('/api/admin/viewers/' + _currentChId, { headers: { 'Authorization': 'Bearer ' + tok } })
              .then(r => r.json()).then(d => { if (countEl) countEl.textContent = d.count ?? '—'; }).catch(()=>{});
          }
          window._startViewerTracking = function(chId, chName) {
            _currentChId = chId; _currentChName = chName || null;
            if (badge) badge.style.display = 'flex';
            if (countEl) countEl.textContent = '—';
            clearInterval(_hbTimer); clearInterval(_pollTimer);
            sendHeartbeat(); pollViewers();
            _hbTimer = setInterval(sendHeartbeat, 30000);
            _pollTimer = setInterval(pollViewers, 30000);
          };
        } else {
          window._startViewerTracking = function(chId, chName) {
            _currentChId = chId; _currentChName = chName || null;
            clearInterval(_hbTimer);
            sendHeartbeat();
            _hbTimer = setInterval(sendHeartbeat, 30000);
          };
        }
      } else {
        area.innerHTML = '<a href="/login" class="auth-btn red">🔑 Login / Sign Up</a>';
      }
    }
    async function initAuth() {
      const token = localStorage.getItem('miz_token');
      let user = null; let role = 'member';
      if (token) {
        try {
          const r = await fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } });
          const d = await r.json();
          if (d.user) { user = d.user; role = d.user.role; }
          else { localStorage.removeItem('miz_token'); }
        } catch(_) {}
      }
      renderAuthUI(user, role);
    }
    initAuth().then(() => loadChannels());
  </script>

  <!-- Guest limit modal -->
  <div class="g-overlay" id="g-overlay">
    <div class="g-modal">
      <h3>⏱ ${parseInt(appConfig.guest_limit_minutes)||5} মিনিট শেষ!</h3>
      <p>Guest হিসেবে মাত্র ${parseInt(appConfig.guest_limit_minutes)||5} মিনিট দেখতে পারবেন।<br>আরো দেখতে Login বা Sign Up করুন — সম্পূর্ণ বিনামূল্যে!</p>
      <div class="g-btns">
        <a href="/login" class="g-btn g-login">🔑 Login</a>
        <a href="/signup" class="g-btn g-signup">📝 Sign Up</a>
      </div>
    </div>
  </div>
</body>
</html>`);
});

app.listen(PORT, '0.0.0.0', () => {
  console.log('Live streaming server running on port ' + PORT);
});
