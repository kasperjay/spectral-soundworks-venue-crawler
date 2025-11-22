import { chromium } from 'playwright';

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();

    const matches = [];

    page.on('response', async (res) => {
        try {
            const url = res.url();
            const keywords = ['event', 'events', 'calendar', 'timely', 'collection', 'api', 'json', 'items'];
            if (!keywords.some(k => url.toLowerCase().includes(k))) return;
            const ct = res.headers()['content-type'] || '';
            let text = '';
            if (ct.includes('application/json') || url.endsWith('.json')) {
                try { text = JSON.stringify(await res.json()); } catch (e) { text = await res.text().catch(()=>''); }
            } else {
                text = await res.text().catch(()=>'');
            }

            matches.push({ url, ct, snippet: text.slice(0, 2000) });
        } catch (e) {
            // ignore
        }
    });

    console.log('Visiting: https://continentalclub.com/austin/');
    await page.goto('https://continentalclub.com/austin/', { waitUntil: 'networkidle' });
    await page.waitForTimeout(4000);

    if (!matches.length) {
        console.log('No matching network responses captured.');
    } else {
        for (const m of matches) {
            console.log('\n--- MATCH ---');
            console.log('URL:', m.url);
            console.log('Content-Type:', m.ct);
            console.log('Snippet:\n', m.snippet);
        }
    }

    await browser.close();
})();