const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { realtime: { transport: ws } }
);

const COUNTRY_RULES = [
  { code: 'BD', words: ['bangla','boishakhi','jamuna','somoy','ekattor','deepto','maasranga','dbc news','ekushey','sa tv','btv','sangsad','rtv ','my tv','banglavision','independent tv','channel i','atn bangla','desh tv','nagorik','news24 bd','channel 9','channel24','ntvbd','ntv bd','channel s'] },
  { code: 'IN', words: ['ndtv','zee tv','star plus','star gold','sony liv','colors tv','sun tv','vijay tv','asianet','gemini tv','etv','tv9 telugu','news18','republic tv','mirror now','times now','india today','dd national','dd news','lok sabha tv','rajya sabha','doordarshan','aaj tak','india tv','cnbctv18','abp news','news nation','india news','zee 24','colors marathi','star vijay','zee marathi','star suvarna','zee kannada','maa tv','star jalsha'] },
  { code: 'GB', words: ['bbc one','bbc two','bbc world','bbc news','sky news','sky sports','itv1','itv2','channel 4 uk','channel 5 uk','united kingdom','britain tv'] },
  { code: 'US', words: ['cnn','fox news','msnbc','abc news','nbc news','cbs news','espn ','hbo ','discovery us','nat geo','history channel','cartoon network','nickelodeon','disney channel','bloomberg tv','cnbc ','pbs '] },
  { code: 'PK', words: ['pakistan','geo tv','geo news','ary news','ary digital','hum tv','hum news','dunya tv','express news','aaj tv','bol news','92 news','24 news pk','dawn news','such tv'] },
  { code: 'AE', words: ['dubai tv','abu dhabi tv','uae tv','al arabiya','sama dubai','al aan','sharjah tv'] },
  { code: 'SA', words: ['saudi tv','mbc ksa','rotana','al ekhbariya','iqraa','sbc tv','ksa sports'] },
  { code: 'QA', words: ['qatar tv','al jazeera','aljazeera','bein sports','bein sport','bein connect'] },
  { code: 'TR', words: ['trt world','trt haber','turkish tv','ntv turk','cnn turk','show tv','kanal d','haberturk','tv8 turk','teve2','a haber'] },
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
  { code: 'CN', words: ['cctv','cgtn','dragon tv','china tv'] },
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
  const n = (name || '').toLowerCase();
  for (const rule of COUNTRY_RULES) {
    if (rule.words.some(w => n.includes(w))) return rule.code;
  }
  return null;
}

async function populate(table, nameField) {
  console.log(`\n--- Populating ${table}.country ---`);
  let page = 0, total = 0, updated = 0;
  while (true) {
    const { data, error } = await supabaseAdmin.from(table).select('id,' + nameField).is('country', null).range(page * 1000, page * 1000 + 999);
    if (error) { console.error('Fetch error:', error.message); break; }
    if (!data || data.length === 0) break;
    total += data.length;
    const toUpdate = data.map(r => ({ id: r.id, country: detectCountry(r[nameField]) })).filter(r => r.country);
    for (const row of toUpdate) {
      const { error: ue } = await supabaseAdmin.from(table).update({ country: row.country }).eq('id', row.id);
      if (ue) console.error('Update error id=' + row.id, ue.message);
      else updated++;
    }
    console.log(`  Page ${page}: fetched ${data.length}, updated ${toUpdate.length}`);
    if (data.length < 1000) break;
    page++;
  }
  console.log(`  Total fetched: ${total}, updated with country: ${updated}`);
}

(async () => {
  await populate('channels', 'channel_name');
  await populate('private_channels', 'name');
  console.log('\nDone!');
  process.exit(0);
})();
