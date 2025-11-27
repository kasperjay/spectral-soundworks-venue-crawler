import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';

Actor.main(async () => {
    const rawInput = (await Actor.getInput()) || {};

    // Allow overriding input via VENUES_JSON environment variable when running locally.
    // This helps when the local KeyValue store isn't picked up as expected.
    let input = rawInput;
    if ((!input.venues || !input.venues.length) && process.env.VENUES_JSON) {
        try {
            const parsed = JSON.parse(process.env.VENUES_JSON);
            if (parsed && typeof parsed === 'object') input = parsed;
        } catch (e) {
            // ignore parse errors and fall back to rawInput
        }
    }

    const {
        venues = [
                {
                    "id": "mohawkAustin",
                    "startUrl": "https://mohawkaustin.com/"
                },
                {
                    "id": "comeAndTakeIt",
                    "startUrl": "https://comeandtakeitproductions.com/calendar/",
                    "parserId": "comeAndTakeItEvent"
                },
                {
                    "id": "continentalClubAustin",
                    "startUrl": "https://continentalclub.com/austin/",
                    "parserId": "continentalClubEvent"
                },
                {
                    "id": "parishAustin",
                    "startUrl": "https://parishaustin.com/calendar/",
                    "parserId": "parishAustinEvent"
                },
                {
                    "id": "empireAtAustin",
                    "startUrl": "https://empireatx.com/calendar/",
                    "parserId": "empireAtAustin"
                },  
                {
                    "id": "stubbsAustin",
                    "startUrl": "https://stubbsaustin.com/concert-listings/",
                    "parserId": "stubbsAustinEvent"
                },
                {
                    "id": "emosAustin",
                    "startUrl": "https://www.emosaustin.com/shows/"
                },
                {
                    "id": "scootInn",
                    "startUrl": "https://www.scootinnaustin.com/shows/"
                },
                {
                    "id": "antones",
                    "startUrl": "https://antonesnightclub.com/calendar/"
                }
        ],
        proxyConfiguration: proxyConfigInput,
        maxConcurrency = 5,
    } = input;

    if (!venues.length) {
        log.warning('No venues provided in "venues" array. Nothing to do.');
        return;
    }

    // Build proxy configuration from input and environment variables
    const proxyOptions = {
        ...(proxyConfigInput || {}),
        ...(process.env.APIFY_PROXY_PASSWORD ? { password: process.env.APIFY_PROXY_PASSWORD } : {}),
        ...(process.env.APIFY_PROXY_GROUPS ? { groups: process.env.APIFY_PROXY_GROUPS.split(',') } : {}),
        ...(process.env.APIFY_PROXY_URL ? { proxyUrls: [process.env.APIFY_PROXY_URL] } : {}),
    };
    if (!proxyOptions.groups) proxyOptions.groups = ['AUTO'];

    const proxyConfiguration = await Actor.createProxyConfiguration(proxyOptions);

    // ------------------------------------------------------------------------
    // Venue-specific parsers
    // Each parser returns either:
    //   - [{ headliner, supportingActs, eventDateRaw, sourceUrl }]
    //   - or an empty array (used for calendar pages that only queue detail URLs)
    // ------------------------------------------------------------------------
    const venueParsers = {
        // ------------------------------------------------------------
        // Mohawk Austin – calendar page has headliner + supports
        // Extracts event cards from .list-view-details
        // ------------------------------------------------------------
        mohawkAustin: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('.list-view-details', (cards) => {
                const results = [];
                const seen = new Set();

                for (const card of cards) {
                    // Get headliner from .event-name.headliners
                    const headliner = card
                        .querySelector('.event-name.headliners a, .event-name.headliners')
                        ?.textContent?.trim();

                    if (!headliner) continue;

                    // Get supporting acts from .event-name.supports
                    const supportsEl = card.querySelector('.event-name.supports');
                    const supportsText = supportsEl ? (supportsEl.textContent || '').trim() : '';

                    // Get event URL
                    const linkEl = card.querySelector('a[href]');
                    const url = linkEl ? linkEl.href : null;

                    // Deduplicate by headliner + URL
                    const key = `${url || ''}|${headliner}`;
                    if (seen.has(key)) continue;
                    seen.add(key);

                    // Try to extract date from card
                    const dateEl = card.querySelector('.event-date, time, .date');
                    const dateText = dateEl ? (dateEl.getAttribute('datetime') || (dateEl.textContent || '').trim()) : null;

                    results.push({
                        headliner,
                        supportsText,
                        url,
                        dateText,
                    });
                }

                return results;
            });

            if (!events || !events.length) {
                return [];
            }

            // Parse supporting acts from the supportsText field
            const parseSupports = (text) => {
                if (!text) return [];
                return text
                    .split(/,|\s+and\s+|\s+&\s+|\s+with\s+|\s*\/\s*/i)
                    .map(s => s.trim())
                    .filter(Boolean);
            };

            const normalized = events.map((ev) => ({
                headliner: ev.headliner || null,
                supportingActs: parseSupports(ev.supportsText),
                eventDateRaw: ev.dateText || null,
                sourceUrl: ev.url || sourceUrl,
            }));

            return normalized;
        },

        // ------------------------------------------------------------
        // Come and Take It – EVENT PAGE
        // Uses "Come and Take It Productions presents" block to get lineup
        // ------------------------------------------------------------
        comeAndTakeItEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const { lineup, fullDate, headingTitle } = await page.evaluate(() => {
                const text = document.body.innerText || '';
                const allLines = text
                    .split('\n')
                    .map((t) => t.trim())
                    .filter(Boolean);

                const heading = document.querySelector('h1, .entry-title');
                const headingTitle = heading?.textContent?.trim() || null;

                let eventDateFull = null;
                for (const line of allLines) {
                    if (/^[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2}$/.test(line)) {
                        eventDateFull = line;
                        break;
                    }
                }

                const startIndex = allLines.findIndex((line) =>
                    /Come and Take It Productions presents/i.test(line),
                );

                const stopPattern = /^(tickets|event details|details|venue info|time:|doors|show:|ages|admission|onsale)/i;

                const lineupLines = [];
                if (startIndex !== -1) {
                    for (let i = startIndex + 1; i < allLines.length; i++) {
                        const line = allLines[i];
                        if (!line) continue;

                        if (/^www\./i.test(line)) break;
                        if (stopPattern.test(line)) break;
                        if (line.length > 80) break;

                        lineupLines.push(line);

                        // Prevent walking too far down unrelated sections
                        if (lineupLines.length >= 8) break;
                    }
                }

                return {
                    lineup: lineupLines,
                    fullDate: eventDateFull,
                    headingTitle,
                };
            });

            const normalizeName = (name) => name.replace(/\s+/g, ' ').replace(/^[–-]\s*/, '').trim();

            const candidateNames = [];
            for (const rawLine of lineup || []) {
                let line = normalizeName(rawLine);
                if (!line) continue;

                if (/^(with)\s+/i.test(line)) {
                    line = line.replace(/^with\s+/i, '');
                }

                const parts = line
                    .split(/,| & | and |\+/i)
                    .map((p) => normalizeName(p))
                    .filter(Boolean);

                for (const part of parts) {
                    const wordCount = part.split(/\s+/).length;
                    if (wordCount > 8) continue;
                    if (/^(tickets|event details|details|venue info|time:|doors|show:|ages|admission)/i.test(part)) continue;
                    if (/[0-9]:[0-9]{2}\s*(am|pm)/i.test(part)) continue;
                    candidateNames.push(part);
                }
            }

            const uniqueNames = Array.from(new Set(candidateNames));

            let headliner = uniqueNames[0] || null;
            const supportingActs = uniqueNames.slice(1);

            if (!headliner && headingTitle) {
                headliner = headingTitle.replace(/Come and Take It Productions presents:?\s*/i, '').trim();
            }

            return [
                {
                    eventDateRaw: fullDate || null,
                    headliner,
                    supportingActs,
                    sourceUrl,
                },
            ];
        },

        // ------------------------------------------------------------
        // Continental Club Austin – calendar page (prefers detail pages)
        // ------------------------------------------------------------
        continentalClubAustin: async ({ page, request, context }) => {
            const sourceUrl = request.loadedUrl || request.url;

            try {
                await page.waitForSelector('body, .timely-event, .event-excerpt, .event-item, .sqs-block-calendar, .events-collection-list, .event, .event-list', { timeout: 7000 });
            } catch (e) {
                // ignore
            }

            try {
                const now = Math.floor(Date.now() / 1000);
                const oneYear = 365 * 24 * 3600;
                const end = now + oneYear;

                const apiUrl = `https://timelyapp.time.ly/api/calendars/54714987/events?group_by_date=1&venues=678194628&timezone=America/Chicago&view=month&start_date_utc=${now}&end_date_utc=${end}&per_page=1000&page=1`;

                const resp = await page.request.get(apiUrl, {
                    headers: { Accept: 'application/json, text/javascript, */*; q=0.01' },
                });

                if (resp && (resp.status ? resp.status() === 200 : resp.ok())) {
                    const payload = await resp.json();
                    if (payload && payload.data && payload.data.items) {
                        const items = payload.data.items || {};
                        const rows = [];
                        for (const dateKey of Object.keys(items)) {
                            const dayItems = items[dateKey] || [];
                            for (const it of dayItems) {
                                const title = it.title || null;
                                const start = it.start_datetime || it.start_utc_datetime || null;
                                const customUrl = it.custom_url || null;
                                const id = it.id || null;

                                let timelyEventUrl = null;
                                try {
                                    if (customUrl) {
                                        timelyEventUrl = `https://events.timely.fun/74avt53i/event/${customUrl}`;
                                    } else if (id) {
                                        timelyEventUrl = `https://events.timely.fun/74avt53i/event/${id}`;
                                    }
                                } catch (e) {
                                    timelyEventUrl = null;
                                }

                                rows.push({
                                    eventDateRaw: start || dateKey || null,
                                    headliner: title || null,
                                    supportingActs: [],
                                    sourceUrl: timelyEventUrl || sourceUrl,
                                });
                            }
                        }

                        if (rows.length) {
                            let queuedDetails = false;
                            if (context?.crawler) {
                                const detailReqs = [];
                                for (const dateKey of Object.keys(items)) {
                                    for (const it of items[dateKey] || []) {
                                        const customUrl = it.custom_url || null;
                                        const id = it.id || null;
                                        let timelyEventUrl = null;
                                        if (customUrl) timelyEventUrl = `https://events.timely.fun/74avt53i/event/${customUrl}`;
                                        else if (id) timelyEventUrl = `https://events.timely.fun/74avt53i/event/${id}`;
                                        if (!timelyEventUrl) continue;
                                        detailReqs.push({
                                            url: timelyEventUrl,
                                            userData: {
                                                venueId: request.userData && request.userData.venueId,
                                                parserId: 'continentalClubEvent',
                                                calendarDateText: dateKey,
                                                calendarTitle: it.title || null,
                                            },
                                        });
                                    }
                                }

                                if (detailReqs.length) {
                                    await context.crawler.addRequests(detailReqs);
                                    queuedDetails = true;
                                }
                            }

                            if (queuedDetails) return [];
                            return rows;
                        }
                    }
                }
            } catch (err) {
                console.warn('Timely API fetch failed for continentalClubAustin:', err && err.message ? err.message : err);
            }

            try {
                const timelyId = '74avt53i';
                const venuesId = '678194628';
                const timelyUrl = `https://events.timely.fun/${timelyId}/month?venues=${venuesId}&nofilters=1&timely_id=timely-iframe-embed-0`;

                const timelyPage = await page.context().newPage();
                await timelyPage.goto(timelyUrl, { waitUntil: 'networkidle', timeout: 15000 });

                const timelyRows = await timelyPage.$$eval('.timely-event', (nodes, timelyUrlParam) => {
                    const rows = [];

                    function parseTimeToMinutes(timeStr) {
                        if (!timeStr) return null;
                        const s = timeStr.trim().toLowerCase();
                        const m = s.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
                        if (!m) return null;
                        let hour = parseInt(m[1], 10);
                        const minutes = m[2] ? parseInt(m[2], 10) : 0;
                        const meridiem = m[3];
                        if (meridiem === 'pm' && hour !== 12) hour += 12;
                        if (meridiem === 'am' && hour === 12) hour = 0;
                        return hour * 60 + minutes;
                    }

                    nodes.forEach((node, idx) => {
                        const titleEl = node.querySelector('.timely-event-title-text');
                        if (!titleEl) return;
                        const timeEl = titleEl.querySelector('.timely-event-time');
                        const timeText = timeEl ? (timeEl.textContent || '').trim() : null;
                        const clone = titleEl.cloneNode(true);
                        const cloneTime = clone.querySelector('.timely-event-time'); if (cloneTime) cloneTime.remove();
                        const title = (clone.textContent || '').trim();
                        if (!title) return;

                        let eventDateRaw = null;
                        const aria = node.getAttribute('aria-label') || '';
                        if (aria) {
                            const parts = aria.split(',').map(p => p.trim()).filter(Boolean);
                            const last = parts[parts.length - 1];
                            if (/\d{4}$/.test(last)) eventDateRaw = last;
                        }

                        const linkEl = node.querySelector('a[href*="/event/"]') || node.querySelector('a');
                        const detailUrl = linkEl ? (linkEl.href || null) : null;

                        rows.push({ eventDateRaw, title: title, headliner: title, supportingActs: [], sourceUrl: timelyUrlParam, detailUrl, timeText, minutes: parseTimeToMinutes(timeText), domIndex: idx });
                    });

                    const byDate = new Map();
                    rows.forEach((r) => {
                        const key = r.eventDateRaw || 'unknown';
                        if (!byDate.has(key)) byDate.set(key, []);
                        byDate.get(key).push(r);
                    });

                    const out = [];
                    for (const [k, arr] of byDate.entries()) {
                        arr.sort((a, b) => {
                            const ma = a.minutes; const mb = b.minutes;
                            if (ma == null && mb == null) return a.domIndex - b.domIndex;
                            if (ma == null) return -1; if (mb == null) return 1; return ma - mb;
                        });
                        const head = arr[arr.length - 1];
                        for (const item of arr) out.push({ eventDateRaw: item.eventDateRaw, headliner: item.headliner, supportingActs: [], sourceUrl: item.sourceUrl });
                    }

                    return out;
                }, timelyUrl);

                await timelyPage.close();

                if (timelyRows && timelyRows.length) {
                    let queuedDetails = false;
                    if (context?.crawler) {
                        const detailReqs = [];
                        for (const it of timelyRows) {
                            if (it.detailUrl) {
                                detailReqs.push({
                                    url: it.detailUrl,
                                    userData: {
                                        venueId: request.userData && request.userData.venueId,
                                        parserId: 'continentalClubEvent',
                                        calendarDateText: it.eventDateRaw || null,
                                        calendarTitle: it.title || null,
                                    },
                                });
                            }
                        }
                        if (detailReqs.length) {
                            await context.crawler.addRequests(detailReqs);
                            queuedDetails = true;
                        }
                    }

                    const normalize = (s) => (s || '').replace(/\s*@\s*\d.*$/,'').replace(/\s*\(.*?\)\s*/g,'').trim();
                    const splitArtists = (s) => {
                        const cleaned = normalize(s);
                        const parts = cleaned.split(/,|\s+with\s+|\s+feat\.?\s+|\s+featuring\s+|\s+&\s+|\s+and\s+|\s*\/\s*|\s*\+\s*/i).map(p => (p||'').trim()).filter(Boolean);
                        if (parts.length === 0) return { headliner: cleaned || null, supports: [] };
                        return { headliner: parts[0], supports: parts.slice(1) };
                    };

                    const rowsOut = timelyRows.map((r) => {
                        const parts = splitArtists(r.title || r.headliner || '');
                        return { eventDateRaw: r.eventDateRaw || null, headliner: parts.headliner || null, supportingActs: parts.supports, sourceUrl: r.sourceUrl || sourceUrl };
                    });

                    if (queuedDetails) return [];
                    return rowsOut;
                }
            } catch (e) {
                console.warn('Timely page parse fallback failed:', e && e.message ? e.message : e);
            }

            const rows = await page.evaluate(() => {
                const results = [];

                function pushIfValid(title, url, dateText) {
                    if (!title) return;
                    const key = `${url || ''}|${title}`;
                    results.push({ title: title.trim(), url: url || null, dateText: dateText || null, key });
                }

                const timely = Array.from(document.querySelectorAll('.timely-event'));
                if (timely.length) {
                    timely.forEach((node) => {
                        if (node.closest && node.closest('nav, .menu, .site-navigation, .main-nav')) return;
                        const titleEl = node.querySelector('.timely-event-title-text') || node.querySelector('.title') || node.querySelector('h2, h3, h4');
                        let title = titleEl ? (titleEl.textContent || '').trim() : null;
                        const timeEl = titleEl ? titleEl.querySelector('.timely-event-time') : null;
                        if (timeEl) {
                            const t = (timeEl.textContent || '').trim();
                            title = title ? title.replace(t, '').trim() : title;
                        }

                        const linkEl = node.querySelector('a[href]');
                        const url = linkEl ? linkEl.href : null;

                        let dateText = null;
                        const aria = node.getAttribute && node.getAttribute('aria-label');
                        if (aria) {
                            const parts = aria.split(',').map(p => p.trim()).filter(Boolean);
                            const last = parts[parts.length - 1];
                            if (/\d{4}$/.test(last)) dateText = last;
                        }

                        const timeElement = node.querySelector('time');
                        if (!dateText && timeElement) dateText = timeElement.getAttribute('datetime') || (timeElement.textContent || '').trim();

                        pushIfValid(title, url, dateText);
                    });
                    const map = new Map();
                    results.forEach(r => map.set(r.key, r));
                    return Array.from(map.values()).map(({title, url, dateText}) => ({ title, url, dateText }));
                }

                const candidates = Array.from(document.querySelectorAll('article, li, .event, .event-item, .listing-item, .show, .post'));
                if (candidates.length) {
                    candidates.forEach((node) => {
                        if (node.closest && node.closest('nav, .menu, .site-navigation, .main-nav')) return;
                        const titleEl = node.querySelector('h1, h2, h3, h4, .title, .entry-title, .event-title, a.event-title') || node.querySelector('a');
                        const title = titleEl ? (titleEl.textContent || '').trim() : null;
                        const linkEl = (titleEl && titleEl.tagName === 'A') ? titleEl : node.querySelector('a[href]');
                        const url = linkEl ? linkEl.href : null;

                        const timeEl = node.querySelector('time, .date, .event-date, .posted-on');
                        const dateText = timeEl ? (timeEl.getAttribute('datetime') || (timeEl.textContent || '').trim()) : null;

                        const tLow = (title || '').toLowerCase();
                        const deny = ['about', 'contact', 'gallery', 'shop', 'welcome', 'home', 'contact us', 'austin shop', 'houston shop', 'austin tickets'];
                        if (tLow && deny.includes(tLow)) return;

                        pushIfValid(title, url, dateText);
                    });
                    const map = new Map();
                    results.forEach(r => map.set(r.key, r));
                    return Array.from(map.values()).map(({title, url, dateText}) => ({ title, url, dateText }));
                }

                const text = document.body ? (document.body.innerText || '') : '';
                if (text) {
                    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
                    for (let i = 0; i < lines.length; i++) {
                        const line = lines[i];
                        if (/\b\d{4}$/.test(line)) {
                            const next = lines[i+1] || null;
                            if (next && next.length > 0 && next.length < 80) {
                                pushIfValid(next, null, line);
                            }
                        }
                    }
                }

                const map = new Map();
                results.forEach(r => map.set(r.key, r));
                return Array.from(map.values()).map(({title, url, dateText}) => ({ title, url, dateText }));
            });

            const withUrls = (rows || []).filter(r => r.url);
            const { venueId } = request.userData || {};

            const likelyEvent = (url) => {
                if (!url) return false;
                try {
                    const u = new URL(url);
                    const path = (u.pathname || '').toLowerCase();
                    const deny = ['/', '/about', '/contact', '/gallery', '/shop', '/austintickets', '/houston', '/bigtop'];
                    if (deny.includes(path)) return false;
                    if (/tm-event|\/event(?:s)?\/|\bshow\b|ticket|tickets|performance|gig|lineup/i.test(path + (u.search||''))) return true;
                    if (/\b\d{4}\b/.test(url) || /\b\d{1,2}[-\/]\d{1,2}\b/.test(url)) return true;
                    if (path.includes('/austin/') && path.split('/').filter(Boolean).length > 2) return true;
                    return false;
                } catch (e) {
                    return false;
                }
            };

            const filtered = withUrls.filter((ev) => likelyEvent(ev.url));

            let queuedDetails = false;
            if (filtered.length && context?.crawler) {
                const detailRequests = filtered.map((ev) => ({
                    url: ev.url,
                    userData: {
                        venueId,
                        parserId: 'continentalClubEvent',
                        calendarDateText: ev.dateText || null,
                        calendarTitle: ev.title || null,
                    },
                }));

                if (detailRequests.length) {
                    await context.crawler.addRequests(detailRequests);
                    queuedDetails = true;
                }
            }

            if (queuedDetails) return [];

            return (rows || []).flatMap((r) => {
                if (!r.title) return [];
                return [{ eventDateRaw: r.dateText || null, headliner: r.title, supportingActs: [], sourceUrl }];
            });
        },

        // ------------------------------------------------------------
        // Continental Club – EVENT DETAIL PAGE
        // ------------------------------------------------------------
        continentalClubEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;
            const { calendarDateText, calendarTitle } = request.userData || {};

            const { headingTitle, allLines, isTimely } = await page.evaluate(() => {
                const heading = document.querySelector('h1, .entry-title, .post-title');
                const headingTitle = heading?.textContent?.trim() || null;
                const text = document.body ? (document.body.innerText || '') : '';
                const allLines = text.split('\n').map(l => l.trim()).filter(Boolean);
                const isTimely = location.host.includes('timely.fun') || location.host.includes('time.ly') || !!document.querySelector('.timely-event') || !!document.querySelector('.timely-iframe');
                return { headingTitle, allLines, isTimely };
            });

            const stopPattern = /^(tickets|event details|details|venue info|time:|doors|show:|ages|admission|onsale)/i;

            let startIndex = -1;
            if (headingTitle) startIndex = allLines.findIndex(l => l.includes(headingTitle));
            if (startIndex === -1) startIndex = allLines.findIndex(l => /\b(with|featuring|feat\.?|presented by)\b/i.test(l));

            const candidateLines = [];
            if (startIndex !== -1) {
                for (let i = startIndex + 1; i < Math.min(allLines.length, startIndex + 20); i++) {
                    const line = allLines[i];
                    if (!line) continue;
                    if (stopPattern.test(line)) break;
                    if (/^www\./i.test(line)) break;
                    if (line.length > 200) break;
                    candidateLines.push(line);
                    if (candidateLines.length >= 12) break;
                }
            } else {
                for (let i = 0; i < Math.min(allLines.length, 40); i++) {
                    const line = allLines[i];
                    if (!line) continue;
                    if (stopPattern.test(line)) break;
                    if (/\b(with|featuring|feat\.?|presented by)\b/i.test(line) || /,/.test(line)) candidateLines.push(line);
                    if (candidateLines.length >= 12) break;
                }
            }

            const normalizeName = (name) => name.replace(/^[–-]\s*/, '').replace(/\s+/g, ' ').trim();
            const candidateNames = [];
            for (const raw of candidateLines) {
                let line = normalizeName(raw);
                if (!line) continue;
                if (/^(with)\s+/i.test(line)) line = line.replace(/^(with)\s+/i, '');
                const parts = line.split(/,| & | and |\+/i).map(p => normalizeName(p)).filter(Boolean);
                for (const part of parts) {
                    if (/^(tickets|event details|details|venue info|time:|doors|show:|ages|admission)/i.test(part)) continue;
                    if (/[0-9]:[0-9]{2}\s*(am|pm)/i.test(part)) continue;
                    if (part.split(/\s+/).length > 10) continue;
                    candidateNames.push(part);
                }
            }

            if ((isTimely) && (!candidateNames.length || !headingTitle)) {
                const timelyCandidates = [];
                for (const l of allLines.slice(0, 40)) {
                    if (/^(featuring|feats?|with|presented by)\b/i.test(l) || /,\s*with\b/i.test(l) || /feat\.?/i.test(l)) {
                        timelyCandidates.push(l);
                    }
                }
                for (const rawLine of timelyCandidates) {
                    let line = normalizeName(rawLine.replace(/^(featuring|feats?|with|presented by)\s*/i, ''));
                    if (!line) continue;
                    const parts = line.split(/,| & | and |\+/i).map(p => normalizeName(p)).filter(Boolean);
                    for (const p of parts) if (p) candidateNames.push(p);
                }
                if (!candidateNames.length && headingTitle) {
                    candidateNames.push(headingTitle);
                }
            }

            const unique = Array.from(new Set(candidateNames));
            let headliner = calendarTitle || (unique[0] || null) || headingTitle || null;
            let supportingActs = unique.slice(1);
            if (calendarTitle && headliner && headliner !== calendarTitle) {
                headliner = calendarTitle;
                supportingActs = unique.filter(n => n !== calendarTitle);
            }

            return [{ eventDateRaw: calendarDateText || null, headliner, supportingActs, sourceUrl }];
        },

        // ------------------------------------------------------------
        // The Parish Austin – CALENDAR PAGE
        // Grabs event links from Modern Events Calendar (MEC) and queues them
        // ------------------------------------------------------------
        parishAustin: async ({ page, request, context }) => {
            const { venueId } = request.userData || {};
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href*="/events/"]', (anchors) => {
                const seen = new Set();
                const result = [];

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    const href = a.getAttribute('href');

                    if (!text || !href) continue;

                    if (!/[a-z0-9]/i.test(text) || text.length < 3) continue;

                    const url = a.href;
                    const key = `${url}|${text}`;
                    if (seen.has(key)) continue;
                    seen.add(key);

                    result.push({ title: text, url });
                }

                return result;
            });

            if (!events || !events.length) {
                return [];
            }

            const detailRequests = events.map((ev) => ({
                url: ev.url,
                userData: {
                    venueId,
                    parserId: 'parishAustinEvent',
                    calendarTitle: ev.title || null,
                },
            }));

            if (context?.crawler && detailRequests.length > 0) {
                await context.crawler.addRequests(detailRequests);
                return [];
            }

            return events.map(ev => ({ eventDateRaw: null, headliner: ev.title, supportingActs: [], sourceUrl: ev.url }));
        },

        // ------------------------------------------------------------
        // The Parish Austin – EVENT DETAIL PAGE
        // Extracts artist lineup from event page
        // ------------------------------------------------------------
        parishAustinEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;
            const { calendarTitle } = request.userData || {};

            const { eventText, headingText } = await page.evaluate(() => {
                const heading = document.querySelector('h1, .entry-title, .post-title, .event-title');
                const headingText = heading ? (heading.textContent || '').trim() : null;

                const text = document.body.innerText || '';
                const eventText = text;

                return { eventText, headingText };
            });

            const lines = eventText.split('\n').map(l => l.trim()).filter(Boolean);

            let headliner = calendarTitle || headingText || null;
            const supportingActs = [];

            if (headliner) {
                const withMatch = headliner.match(/(.+?)\s+(?:w\/|with)\s+(.+?)(?:\s+at\s+|$)/i);
                if (withMatch) {
                    const mainArtist = withMatch[1].trim();
                    const supports = withMatch[2].trim();
                    
                    headliner = mainArtist;
                    
                    const supportParts = supports
                        .split(/,|\s+and\s+|\s+&\s+|\s*\/\s*/i)
                        .map(p => p.replace(/\s+at\s+.*/i, '').trim())
                        .filter(p => p.length > 2 && p.length < 100);
                    
                    for (const part of supportParts) {
                        supportingActs.push(part);
                    }
                }
            }

            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];

                if (/^(with|featuring|feat\.?|w\/)\s+/i.test(line)) {
                    let artistStr = line.replace(/^(with|featuring|feat\.?|w\/)\s+/i, '').trim();
                    artistStr = artistStr.replace(/\s*on\s+.*/i, '').replace(/\s+at\s+.*/i, '');

                    if (artistStr) {
                        const parts = artistStr
                            .split(/,|\s+and\s+|\s+&\s+/i)
                            .map(p => p.trim())
                            .filter(Boolean);

                        for (const part of parts) {
                            if (part.length > 2 && part.length < 100 && !supportingActs.includes(part)) {
                                supportingActs.push(part);
                            }
                        }
                    }
                }
            }

            if (headliner) {
                headliner = headliner
                    .replace(/\s+\d{1,2}:\d{2}\s*(am|pm)?/i, '')
                    .replace(/\s*[\-–]\s*.*$/, '')
                    .replace(/\s+at\s+.*/i, '')
                    .trim();
            }

            const uniqueSupports = Array.from(new Set(supportingActs));

            return [
                {
                    eventDateRaw: null,
                    headliner,
                    supportingActs: uniqueSupports,
                    sourceUrl,
                },
            ];
        },

        // ------------------------------------------------------------
        // Empire at Austin – CALENDAR PAGE
        // Grabs event links from Modern Events Calendar (MEC) and queues them
        // ------------------------------------------------------------
        empireAtAustin: async ({ page, request, context }) => {
            const { venueId } = request.userData || {};
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href*="/events/"]', (anchors) => {
                const seen = new Set();
                const result = [];

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    const href = a.getAttribute('href');

                    if (!text || !href) continue;

                    if (!/[a-z0-9]/i.test(text) || text.length < 3) continue;

                    const url = a.href;
                    const key = `${url}|${text}`;
                    if (seen.has(key)) continue;
                    seen.add(key);

                    result.push({ title: text, url });
                }

                return result;
            });

            if (!events || !events.length) {
                return [];
            }

            const detailRequests = events.map((ev) => ({
                url: ev.url,
                userData: {
                    venueId,
                    parserId: 'empireAtAustinEvent',
                    calendarTitle: ev.title || null,
                },
            }));

            if (context?.crawler && detailRequests.length > 0) {
                await context.crawler.addRequests(detailRequests);
                return [];
            }

            return events.map(ev => ({ eventDateRaw: null, headliner: ev.title, supportingActs: [], sourceUrl: ev.url }));
        },

        // ------------------------------------------------------------
        // Empire at Austin – EVENT DETAIL PAGE
        // Extracts artist lineup from event page
        // ------------------------------------------------------------
        empireAtAustinEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;
            const { calendarTitle } = request.userData || {};

            const { eventText, headingText } = await page.evaluate(() => {
                const heading = document.querySelector('h1, .entry-title, .post-title, .event-title');
                const headingText = heading ? (heading.textContent || '').trim() : null;

                const text = document.body.innerText || '';
                const eventText = text;

                return { eventText, headingText };
            });

            const lines = eventText.split('\n').map(l => l.trim()).filter(Boolean);

            let headliner = calendarTitle || headingText || null;
            const supportingActs = [];

            if (headliner) {
                const withMatch = headliner.match(/(.+?)\s+(?:w\/|with)\s+(.+?)(?:\s+(?:at|in)\s+|$)/i);
                if (withMatch) {
                    const mainArtist = withMatch[1].trim();
                    const supports = withMatch[2].trim();
                    
                    headliner = mainArtist;
                    
                    const supportParts = supports
                        .split(/,|\s+and\s+|\s+&\s+|\s*\/\s*/i)
                        .map(p => p.replace(/\s+(?:at|in)\s+.*/i, '').trim())
                        .filter(p => p.length > 2 && p.length < 100);
                    
                    for (const part of supportParts) {
                        supportingActs.push(part);
                    }
                }
            }

            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];

                if (/^(with|featuring|feat\.?|w\/)\s+/i.test(line)) {
                    let artistStr = line.replace(/^(with|featuring|feat\.?|w\/)\s+/i, '').trim();
                    artistStr = artistStr.replace(/\s*on\s+.*/i, '').replace(/\s+at\s+.*/i, '').replace(/\s+in\s+.*/i, '');

                    if (artistStr) {
                        const parts = artistStr
                            .split(/,|\s+and\s+|\s+&\s+/i)
                            .map(p => p.trim())
                            .filter(Boolean);

                        for (const part of parts) {
                            if (part.length > 2 && part.length < 100 && !supportingActs.includes(part)) {
                                supportingActs.push(part);
                            }
                        }
                    }
                }
            }

            if (headliner) {
                headliner = headliner
                    .replace(/\s+\d{1,2}:\d{2}\s*(am|pm)?/i, '')  // remove times
                    .replace(/\s*[\-–]\s*.*$/, '') // remove trailing dash descriptions
                    .replace(/\s+at\s+.*/i, '') // remove venue/location
                    .replace(/\s+in\s+.*/i, '') // remove "in the Garage/Control Room"
                    .replace(/\s+\d{1,2}:\d{2}\s*(am|pm)?/i, '')
                    .replace(/\s*[\-–]\s*.*$/, '')
                    .replace(/\s+at\s+.*/i, '')
                    .replace(/\s+in\s+.*/i, '')
                    .trim();
            }

            const uniqueSupports = Array.from(new Set(supportingActs));

            return [
                {
                    eventDateRaw: null,
                    headliner,
                    supportingActs: uniqueSupports,
                    sourceUrl,
                },
            ];
        },

        // ------------------------------------------------------------
        // Emo's Austin – Shows page (Next.js, JSON-LD first, DOM fallback)
        // ------------------------------------------------------------
        emosAustin: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            try {
                await page.waitForSelector('body', { timeout: 10000 });
            } catch (e) {
                // ignore
            }

            const events = await page.evaluate(() => {
                const rows = [];
                const seen = new Set();

                const pushRow = (name, url, dateRaw) => {
                    if (!name) return;
                    const key = `${name.toLowerCase()}|${url || ''}|${dateRaw || ''}`;
                    if (seen.has(key)) return;
                    seen.add(key);
                    rows.push({
                        headliner: name.trim(),
                        supportingActs: [],
                        eventDateRaw: dateRaw || null,
                        sourceUrl: url || null,
                    });
                };

                const extractFromObject = (obj) => {
                    if (!obj || typeof obj !== 'object') return;
                    if (Array.isArray(obj)) {
                        obj.forEach(extractFromObject);
                        return;
                    }

                    if (obj['@type'] === 'MusicEvent' && obj.name) {
                        pushRow(obj.name, obj.url || obj.sameAs || null, obj.startDate || obj.date || null);
                    }

                    if (obj['@graph']) extractFromObject(obj['@graph']);
                };

                const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                for (const script of scripts) {
                    try {
                        const text = script.textContent || '';
                        const parsed = JSON.parse(text);
                        extractFromObject(parsed);
                    } catch (e) {
                        // ignore malformed JSON-LD
                    }
                }

                if (!rows.length) {
                    const cardSelectors = [
                        '[data-automation="event-card"]',
                        '[data-automation="show-card"]',
                        'a[href*="/event/"]',
                        'a[href*="ticketmaster.com"]',
                    ];

                    const cards = Array.from(document.querySelectorAll(cardSelectors.join(',')));
                    for (const card of cards) {
                        const titleEl = card.querySelector('h3, h4, h2, .chakra-heading, .title') || card;
                        const title = titleEl ? (titleEl.textContent || '').trim() : null;

                        const linkEl = card.tagName === 'A' ? card : card.querySelector('a[href]');
                        const url = linkEl ? linkEl.href : null;

                        const dateEl = card.querySelector('time');
                        const dateRaw = dateEl ? (dateEl.getAttribute('datetime') || (dateEl.textContent || '').trim()) : null;

                        pushRow(title, url, dateRaw);
                    }
                }

                return rows;
            });

            return (events || []).map((ev) => ({
                headliner: ev.headliner || null,
                supportingActs: ev.supportingActs || [],
                eventDateRaw: ev.eventDateRaw || null,
                sourceUrl: ev.sourceUrl || sourceUrl,
            }));
        },

        // ------------------------------------------------------------
        // Scoot Inn – Shows page (Next.js, JSON-LD first, DOM fallback)
        // ------------------------------------------------------------
        scootInn: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            try {
                await page.waitForSelector('body', { timeout: 10000 });
            } catch (e) {
                // ignore
            }

            const events = await page.evaluate(() => {
                const rows = [];
                const seen = new Set();

                const pushRow = (name, url, dateRaw) => {
                    if (!name) return;
                    const key = `${name.toLowerCase()}|${url || ''}|${dateRaw || ''}`;
                    if (seen.has(key)) return;
                    seen.add(key);
                    rows.push({
                        headliner: name.trim(),
                        supportingActs: [],
                        eventDateRaw: dateRaw || null,
                        sourceUrl: url || null,
                    });
                };

                const extractFromObject = (obj) => {
                    if (!obj || typeof obj !== 'object') return;
                    if (Array.isArray(obj)) {
                        obj.forEach(extractFromObject);
                        return;
                    }

                    if (obj['@type'] === 'MusicEvent' && obj.name) {
                        pushRow(obj.name, obj.url || obj.sameAs || null, obj.startDate || obj.date || null);
                    }

                    if (obj['@graph']) extractFromObject(obj['@graph']);
                };

                const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                for (const script of scripts) {
                    try {
                        const text = script.textContent || '';
                        const parsed = JSON.parse(text);
                        extractFromObject(parsed);
                    } catch (e) {
                        // ignore malformed JSON-LD
                    }
                }

                if (!rows.length) {
                    const cardSelectors = [
                        '[data-automation="event-card"]',
                        '[data-automation="show-card"]',
                        'a[href*="/event/"]',
                        'a[href*="ticketmaster.com"]',
                    ];

                    const cards = Array.from(document.querySelectorAll(cardSelectors.join(',')));
                    for (const card of cards) {
                        const titleEl = card.querySelector('h3, h4, h2, .chakra-heading, .title') || card;
                        const title = titleEl ? (titleEl.textContent || '').trim() : null;

                        const linkEl = card.tagName === 'A' ? card : card.querySelector('a[href]');
                        const url = linkEl ? linkEl.href : null;

                        const dateEl = card.querySelector('time');
                        const dateRaw = dateEl ? (dateEl.getAttribute('datetime') || (dateEl.textContent || '').trim()) : null;

                        pushRow(title, url, dateRaw);
                    }
                }

                return rows;
            });

            return (events || []).map((ev) => ({
                headliner: ev.headliner || null,
                supportingActs: ev.supportingActs || [],
                eventDateRaw: ev.eventDateRaw || null,
                sourceUrl: ev.sourceUrl || sourceUrl,
            }));
        },

        // Grabs all /tm-event/ links and queues them
        // ------------------------------------------------------------
        stubbsAustin: async ({ page, request, context }) => {
            const { venueId } = request.userData || {};
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href*="/tm-event/"]', (anchors) => {
                const seen = new Set();
                const result = [];

                for (const a of anchors) {
                    const title = (a.textContent || '').trim();
                    const url = a.href;

                    if (!title || !url) continue;

                    const key = `${url}|${title}`;
                    if (seen.has(key)) continue;
                    seen.add(key);

                    result.push({ title, url });
                }

                return result;
            });

            if (!events || !events.length) {
                return [];
            }

            const detailRequests = events.map((ev) => ({
                url: ev.url,
                userData: {
                    venueId,
                    parserId: 'stubbsAustinEvent',
                    calendarDateText: null,
                    calendarTitle: ev.title || null,
                },
            }));

            if (context?.crawler && detailRequests.length > 0) {
                await context.crawler.addRequests(detailRequests);
                return [];
            }

            return events.map(ev => ({ eventDateRaw: null, headliner: ev.title || ev.url, supportingActs: [], sourceUrl: ev.url }));
        },

        // ------------------------------------------------------------
        // Stubb's Austin – TM EVENT PAGE
        // Uses "with ..." to get openers. Headliner comes from calendarTitle.
        // ------------------------------------------------------------
        stubbsAustinEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;
            const { calendarDateText, calendarTitle } = request.userData || {};

            const { withLine, titleText } = await page.evaluate(() => {
                const text = document.body.innerText || '';
                const lines = text
                    .split('\n')
                    .map((t) => t.trim())
                    .filter(Boolean);

                const wLine = lines.find((l) => /^with\s+/i.test(l)) || null;

                const heading =
                    document.querySelector('h1, .entry-title, .post-title') || null;
                const hText = heading ? heading.textContent.trim() : null;

                return {
                    withLine: wLine,
                    titleText: hText,
                };
            });

            let headliner = calendarTitle || titleText || null;
            let supportingActs = [];

            if (withLine) {
                let namesStr = withLine.replace(/^with\s+/i, '').trim();
                namesStr = namesStr.replace(/\.$/, '');

                const parts = namesStr
                    .split(/,| & | and /i)
                    .map((s) => s.trim())
                    .filter(Boolean);

                if (parts.length > 0) {
                    supportingActs = parts;
                }
            }

            return [
                {
                    eventDateRaw: calendarDateText || null,
                    headliner,
                    supportingActs,
                    sourceUrl,
                },
            ];
        }

    };

    // ------------------------------------------------------------------------
    // Build initial requests from input.venues
    // ------------------------------------------------------------------------
    const normalizeVenue = (venue) => {
        const v = { ...venue };
        if (v.id && v.startUrl) {
            if (['emosAustin', 'scootInn'].includes(v.id) && /\/shows\/calendar\/?$/i.test(v.startUrl)) {
                v.startUrl = v.startUrl.replace(/\/shows\/calendar\/?$/i, '/shows/');
            }
        }
        return v;
    };

    const startRequests = venues.map((venue) => {
        const v = normalizeVenue(venue);
        return {
            url: v.startUrl,
        userData: {
                venueId: v.id,
                parserId: v.parserId || v.id,
        },
        };
    });
    
    // ------------------------------------------------------------------------
    // Playwright crawler
    // ------------------------------------------------------------------------
    const crawler = new PlaywrightCrawler({
        maxConcurrency: Math.min(maxConcurrency || 1, 3),
        proxyConfiguration,
        navigationTimeoutSecs: 90,
        requestHandlerTimeoutSecs: 180,

        async requestHandler(context) {
            const { request, page } = context;
            const { venueId, parserId } = request.userData || {};
            const parserKey = parserId || venueId;

            log.info(
                `Handling URL: ${request.url} | venueId=${venueId || 'N/A'} | parserKey=${parserKey || 'N/A'}`,
            );

            const parser = venueParsers[parserKey];

            if (!parser) {
                log.error(
                    `No parser found for parserId="${parserKey}" (venueId="${venueId}") on ${request.url}.`,
                );
                return;
            }

            log.info(`Using parser "${parserKey}" for ${request.url}`);

            const rawItems = await parser({ page, request, context, log });

            if (!rawItems || !rawItems.length) {
                log.warning(
                    `Parser "${parserKey}" returned no items for ${request.url}.`,
                );
                return;
            }

            const normalizedRows = rawItems.flatMap((item) => {
                const base = {
                    venueId,
                    venueParserId: parserKey,
                    sourceUrl: item.sourceUrl || request.loadedUrl || request.url,
                    eventDateRaw: item.eventDateRaw ?? null,
                    scrapedAt: new Date().toISOString(),
                };

                const cleanArtist = (s) => {
                    if (!s) return null;
                    let v = String(s || '');
                    v = v.replace(/^[^:]+\s+(?:Presents?|Pres\.):\s*/i, '');
                    v = v.replace(/@\s*\d{1,2}(:\d{2})?\s*(am|pm)?/i, '');
                    v = v.replace(/\bat\s+\d{1,2}(:\d{2})?\s*(am|pm)?\b/i, '');
                    v = v.replace(/[\-–—]\s*[^,()]+$/g, '');
                    v = v.replace(/\(.*?\)/g, '');
                    v = v.replace(/:\s*[^:]*\bTour\b.*$/i, ''); // drop trailing ": XYZ Tour"
                    v = v.replace(/[\u2018\u2019\u201C\u201D]/g, "'");
                    v = v.replace(/[\s\u00A0]+/g, ' ').trim();
                    v = v.replace(/^[,;:\s]+|[,;:\s]+$/g, '');
                    if (!v) return null;
                    return v;
                };

                if (item.headliner) {
                    const rows = [];

                    const normalizeTitle = (s) => (s || '').replace(/\s*@\s*\d.*$/,'').replace(/\s*\(.*?\)\s*/g,'').trim();
                    const splitParts = (s) => (normalizeTitle(s).split(/,|\s+with\s+|\s+feat\.?\s+|\s+featuring\s+|\s+&\s+|\s+and\s+|\s*\/\s*|\s*\+\s*/i).map(p => p.trim()).filter(Boolean));

                    const headParts = splitParts(item.headliner);
                    const mainHeadlinerRaw = headParts[0] || item.headliner;
                    const mainHeadliner = cleanArtist(mainHeadlinerRaw) || mainHeadlinerRaw;

                    rows.push({ ...base, role: 'headliner', artistName: mainHeadliner });

                    const supports = new Set();
                    if (Array.isArray(item.supportingActs)) {
                        for (const s of item.supportingActs) if (s) supports.add(cleanArtist(s) || s);
                    }
                    for (let i = 1; i < headParts.length; i++) supports.add(cleanArtist(headParts[i]) || headParts[i]);

                    for (const name of Array.from(supports)) {
                        if (!name) continue;
                        rows.push({ ...base, role: 'support', artistName: name });
                    }

                    return rows;
                }

                if (item.artistName) {
                    const cleaned = cleanArtist(item.artistName) || item.artistName;
                    return [
                        {
                            ...base,
                            role: item.role || 'unknown',
                            artistName: cleaned,
                        },
                    ];
                }

                log.warning(
                    `Item from parser "${parserKey}" had no headliner or artistName on ${request.url}.`,
                );
                return [];
            });

            const dedupeKey = (r) => `${(r.artistName||'').toLowerCase().replace(/\s+/g,' ')}`;
            const seen = new Set();
            const uniqueRows = [];
            for (const r of normalizedRows) {
                const k = dedupeKey(r);
                if (seen.has(k)) continue;
                seen.add(k);
                uniqueRows.push(r);
            }

            if (!uniqueRows.length) {
                log.warning(
                    `No normalized rows produced by parser "${parserKey}" for ${request.url}.`,
                );
                return;
            }

            log.info(
                `Pushing ${uniqueRows.length} row(s) to dataset from ${request.url} (parser="${parserKey}")`,
            );

            await Actor.pushData(uniqueRows);
        },

        async failedRequestHandler({ request, log }) {
            log.error(`Request ${request.url} failed too many times.`);
        },
    });

        await crawler.run(startRequests);
    });
