import { chromium } from 'playwright';

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto('https://continentalclub.com/austin/', { waitUntil: 'networkidle' });
    await page.waitForTimeout(3000);

    const selectors = ['.timely-event', '.event-excerpt', '.event-item', '.sqs-block-calendar', '.events-collection-list', '.event', '.event-list', '.event-listing', '.sqs-block-events', '.event-excerpts'];

    for (const sel of selectors) {
        const count = await page.$$eval(sel, els => els.length).catch(() => 0);
        console.log(sel, count);
        if (count > 0) {
            const sample = await page.$$eval(sel, els => els.slice(0,3).map(e => e.textContent.trim()));
            console.log('  sample:', sample);
        }
    }

    const navCount = await page.$$eval('nav a, .main-nav a, #desktopNav a', els => els.length).catch(() => 0);
    console.log('nav links', navCount);

    const bodyText = await page.$eval('body', b => b.innerText.slice(0,1000)).catch(()=>null);
    console.log('bodyTextPreview:', bodyText ? bodyText.replace(/\n/g,' | ') : null);

    await browser.close();
})();