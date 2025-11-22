const fs = require('fs');
const dir = 'storage/datasets/default';
if (!fs.existsSync(dir)) { console.log('No dataset dir'); process.exit(0); }
const files = fs.readdirSync(dir).filter(f => f.endsWith('.json')).sort();
const map = {};
files.forEach(f => {
  const j = JSON.parse(fs.readFileSync(dir + '/' + f));
  const k = (j.artistName || '') + '||' + (j.eventDateRaw || '');
  map[k] = (map[k] || 0) + 1;
});
const dup = Object.entries(map).filter(([k, v]) => v > 1);
if (!dup.length) console.log('No exact duplicates (artist+date) found');
else { console.log('Duplicates found:'); dup.forEach(([k, v]) => console.log(k, '->', v)); }
