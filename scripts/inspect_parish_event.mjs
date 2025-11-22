import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();

// Test a Parish event page
await page.goto('https://parishaustin.com/events/satsang/', { waitUntil: 'networkidle', timeout: 30000 });

const data = await page.evaluate(() => {
    const heading = document.querySelector('h1, .entry-title, .post-title, .event-title');
    const headingText = heading ? (heading.textContent || '').trim() : null;

    const text = document.body.innerText || '';
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);

    // Show first 50 lines
    const sample = lines.slice(0, 50);

    // Find lines with "with", "featuring", etc.
    const interestingLines = lines.filter((l, i) => {
        return /^(with|featuring|feat\.?|w\/|Featuring|featuring)/i.test(l) || 
               (i > 0 && /^(with|featuring|feat\.?|w\/)/i.test(lines[i-1])) ||
               /,\s*(with|feat)/i.test(l);
    });

    return {
        heading: headingText,
        sampleLines: sample,
        interestingLines: interestingLines.slice(0, 20),
        totalLines: lines.length,
    };
});

console.log(JSON.stringify(data, null, 2));

await browser.close();
