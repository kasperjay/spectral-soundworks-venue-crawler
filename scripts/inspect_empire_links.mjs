import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
await page.goto('https://empireatx.com/calendar/', { waitUntil: 'networkidle', timeout: 30000 });

const links = await page.$$eval('a[href*="/events/"]', (as) => {
    return as.slice(0, 8).map(a => ({ 
        text: a.textContent?.trim(), 
        href: a.href 
    }));
});

console.log(JSON.stringify(links, null, 2));
await browser.close();
