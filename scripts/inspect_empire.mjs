import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();

await page.goto('https://empireatx.com/calendar/', { waitUntil: 'networkidle', timeout: 30000 });

await page.waitForTimeout(2000);

const data = await page.evaluate(() => {
    const results = {
        pageTitle: document.title,
        selectors: {},
        sampleText: document.body.innerText.slice(0, 1500),
    };

    // Check various selectors
    const selectors = [
        '.event', '.event-item', '.event-card', '.event-listing',
        '[class*="event"]', '[class*="calendar"]', '[class*="listing"]',
        '.calendar-event', '.show', '.gig',
        'article', '.entry', '.post',
        '[class*="show"]', '[class*="performance"]'
    ];

    for (const sel of selectors) {
        const count = document.querySelectorAll(sel).length;
        if (count > 0) {
            results.selectors[sel] = { count, sample: document.querySelector(sel)?.outerHTML?.slice(0, 300) };
        }
    }

    // Check for event links
    const eventLinks = Array.from(document.querySelectorAll('a')).filter(a => {
        const href = (a.href || '').toLowerCase();
        const text = (a.textContent || '').toLowerCase();
        return href.includes('event') || text.includes('event') || href.includes('show') || text.includes('show');
    }).slice(0, 5);

    results.eventLinks = eventLinks.map(a => ({ text: a.textContent?.trim(), href: a.href }));

    return results;
});

console.log(JSON.stringify(data, null, 2));

await browser.close();
