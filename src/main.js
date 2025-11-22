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
                    "startUrl": "https://comeandtakeitproductions.com/calendar/"
                },
                {
                    "id": "continentalClubAustin",
                    "startUrl": "https://continentalclub.com/austin/"
                },
                {
                    "id": "parishAustin",
                    "startUrl": "https://parishaustin.com/calendar/"
                },
                {
                    "id": "empireAtAustin",
                    "startUrl": "https://empireatx.com/calendar/"
                },
                {
                    "id": "stubbsAustin",
                    "startUrl": "https://stubbsaustin.com/concert-listings/"
                },
                {
                    "id": "emosAustin",
                    "startUrl": "https://www.emosaustin.com/shows/calendar/"
                },
                {
                    "id": "scootInn",
                    "startUrl": "https://www.scootinnaustin.com/shows/calendar"
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

    const proxyConfiguration = await Actor.createProxyConfiguration(proxyConfigInput);

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
                // Wait for the calendar/event widgets to render (Squarespace loads events via JS)
                await page.waitForSelector('body, .timely-event, .event-excerpt, .event-item, .sqs-block-calendar, .events-collection-list, .event, .event-list', { timeout: 7000 });
            } catch (e) {
                // ignore
            }

            // --- Timely API fallback ---
            // The Continental Club embeds a Timely calendar. We can directly call the Timely
            // JSON API to get structured events for this venue (more reliable than scanning DOM).
            try {
                const now = Math.floor(Date.now() / 1000);
                const oneYear = 365 * 24 * 3600;
                const end = now + oneYear;

                // Calendar/venue IDs were observed in network capture. These are specific to
                // The Continental Club - Austin embed and should be stable for this site.
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

                                // Construct a likely timely event URL when possible
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
                            // Queue detail pages for richer parsing (optional)
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

                                if (detailReqs.length) await context.crawler.addRequests(detailReqs);
                            }

                            // Emit rows found from the API directly
                            return rows;
                        }
                    }
                }
            } catch (err) {
                // network/JSON parsing errors are non-fatal; fall back to DOM parsing
                // eslint-disable-next-line no-console
                console.warn('Timely API fetch failed for continentalClubAustin:', err && err.message ? err.message : err);
            }

            // If API and local DOM parsing didn't return events, try loading the Timely
            // month page in a new Playwright page (this runs in-browser and bypasses the
            // direct API 403 that blocks non-browser requests).
            try {
                // Avoid navigating away from the current page; open a new page in the same context
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

                        // Try to find a detail link on the timely node
                        const linkEl = node.querySelector('a[href*="/event/"]') || node.querySelector('a');
                        const detailUrl = linkEl ? (linkEl.href || null) : null;

                        rows.push({ eventDateRaw, title: title, headliner: title, supportingActs: [], sourceUrl: timelyUrlParam, detailUrl, timeText, minutes: parseTimeToMinutes(timeText), domIndex: idx });
                    });

                    // group by date and try to mark headliners
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
                    // Queue detail pages found on the Timely page for richer parsing
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
                        if (detailReqs.length) await context.crawler.addRequests(detailReqs);
                    }

                    // Normalize titles into headliner + supportingActs here (split multi-act titles)
                    const normalize = (s) => (s || '').replace(/\s*@\s*\d.*$/,'').replace(/\s*\(.*?\)\s*/g,'').trim();
                    const splitArtists = (s) => {
                        const cleaned = normalize(s);
                        // split on commas, ' with ', ' feat', 'featuring', '&', ' and ', '/', ' + '
                        const parts = cleaned.split(/,|\s+with\s+|\s+feat\.?\s+|\s+featuring\s+|\s+&\s+|\s+and\s+|\s*\/\s*|\s*\+\s*/i).map(p => (p||'').trim()).filter(Boolean);
                        if (parts.length === 0) return { headliner: cleaned || null, supports: [] };
                        return { headliner: parts[0], supports: parts.slice(1) };
                    };

                    const rowsOut = timelyRows.map((r) => {
                        const parts = splitArtists(r.title || r.headliner || '');
                        return { eventDateRaw: r.eventDateRaw || null, headliner: parts.headliner || null, supportingActs: parts.supports, sourceUrl: r.sourceUrl || sourceUrl };
                    });

                    return rowsOut;
                }
            } catch (e) {
                // non-fatal; fall back to existing DOM parsing
                // eslint-disable-next-line no-console
                console.warn('Timely page parse fallback failed:', e && e.message ? e.message : e);
            }

            const rows = await page.evaluate(() => {
                const results = [];

                function pushIfValid(title, url, dateText) {
                    if (!title) return;
                    const key = `${url || ''}|${title}`;
                    results.push({ title: title.trim(), url: url || null, dateText: dateText || null, key });
                }

                // Try Timely widget-like nodes first
                const timely = Array.from(document.querySelectorAll('.timely-event'));
                if (timely.length) {
                    timely.forEach((node) => {
                        // skip navigation/menu items
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

                // Generic candidates
                const candidates = Array.from(document.querySelectorAll('article, li, .event, .event-item, .listing-item, .show, .post'));
                if (candidates.length) {
                    candidates.forEach((node) => {
                        // skip navigation/menu items
                        if (node.closest && node.closest('nav, .menu, .site-navigation, .main-nav')) return;
                        const titleEl = node.querySelector('h1, h2, h3, h4, .title, .entry-title, .event-title, a.event-title') || node.querySelector('a');
                        const title = titleEl ? (titleEl.textContent || '').trim() : null;
                        const linkEl = (titleEl && titleEl.tagName === 'A') ? titleEl : node.querySelector('a[href]');
                        const url = linkEl ? linkEl.href : null;

                        const timeEl = node.querySelector('time, .date, .event-date, .posted-on');
                        const dateText = timeEl ? (timeEl.getAttribute('datetime') || (timeEl.textContent || '').trim()) : null;

                        // skip obvious non-event section titles
                        const tLow = (title || '').toLowerCase();
                        const deny = ['about', 'contact', 'gallery', 'shop', 'welcome', 'home', 'contact us', 'austin shop', 'houston shop', 'austin tickets'];
                        if (tLow && deny.includes(tLow)) return;

                        pushIfValid(title, url, dateText);
                    });
                    const map = new Map();
                    results.forEach(r => map.set(r.key, r));
                    return Array.from(map.values()).map(({title, url, dateText}) => ({ title, url, dateText }));
                }

                // Fallback: text-scan
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

                if (detailRequests.length) await context.crawler.addRequests(detailRequests);
                // Continue and also emit calendar rows below so we get both calendar summaries
                // and detailed rows produced by event pages queued above.
            }

            // Otherwise emit calendar rows directly
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

            // If this is a Timely event page, try additional heuristics: the page often
            // contains a single heading with the full title and then lines like "Featuring...".
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
                // If still empty, attempt to use headingTitle as single headliner
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

                    // Filter to only event links (avoid navigation/other links)
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
                // Emit lightweight calendar summary rows in addition to queuing details.
                return events.map(ev => ({
                    eventDateRaw: null,
                    headliner: ev.title,
                    supportingActs: [],
                    sourceUrl: ev.url,
                }));
            }

            // If not running in crawler context, emit summary rows so callers
            // (or tests) get immediate results.
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

            // Extract supporting acts from heading/title using "w/" or "with" patterns
            if (headliner) {
                // Match patterns like "Satsang w/ Tim Snider" or "Artist with Support Act"
                const withMatch = headliner.match(/(.+?)\s+(?:w\/|with)\s+(.+?)(?:\s+at\s+|$)/i);
                if (withMatch) {
                    const mainArtist = withMatch[1].trim();
                    const supports = withMatch[2].trim();
                    
                    headliner = mainArtist;
                    
                    // Parse multiple supports if separated by "and", ",", "&", etc.
                    const supportParts = supports
                        .split(/,|\s+and\s+|\s+&\s+|\s*\/\s*/i)
                        .map(p => p.replace(/\s+at\s+.*/i, '').trim())
                        .filter(p => p.length > 2 && p.length < 100);
                    
                    for (const part of supportParts) {
                        supportingActs.push(part);
                    }
                }
            }

            // Also look for standalone lines that match "with X" or "featuring X" patterns
            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];

                // Match "with X" or "featuring X" patterns at start of line
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

            // Clean up headliner to remove extra text
            if (headliner) {
                headliner = headliner
                    .replace(/\s+\d{1,2}:\d{2}\s*(am|pm)?/i, '')  // remove times
                    .replace(/\s*[\-–]\s*.*$/, '') // remove trailing dash descriptions
                    .replace(/\s+at\s+.*/i, '') // remove venue/location
                    .trim();
            }

            // Dedupe supportingActs
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

                    // Filter to only event links (avoid navigation/other links)
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
                // Emit lightweight calendar summary rows in addition to queuing details.
                return events.map(ev => ({
                    eventDateRaw: null,
                    headliner: ev.title,
                    supportingActs: [],
                    sourceUrl: ev.url,
                }));
            }

            // If not running in crawler context, emit summary rows so callers
            // (or tests) get immediate results.
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

            // Extract supporting acts from heading/title using "w/" or "with" patterns
            if (headliner) {
                // Match patterns like "Artist w/ Support Act" or "Artist with Support Act"
                const withMatch = headliner.match(/(.+?)\s+(?:w\/|with)\s+(.+?)(?:\s+(?:at|in)\s+|$)/i);
                if (withMatch) {
                    const mainArtist = withMatch[1].trim();
                    const supports = withMatch[2].trim();
                    
                    headliner = mainArtist;
                    
                    // Parse multiple supports if separated by "and", ",", "&", etc.
                    const supportParts = supports
                        .split(/,|\s+and\s+|\s+&\s+|\s*\/\s*/i)
                        .map(p => p.replace(/\s+(?:at|in)\s+.*/i, '').trim())
                        .filter(p => p.length > 2 && p.length < 100);
                    
                    for (const part of supportParts) {
                        supportingActs.push(part);
                    }
                }
            }

            // Also look for standalone lines that match "with X" or "featuring X" patterns
            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];

                // Match "with X" or "featuring X" patterns at start of line
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

            // Clean up headliner to remove extra text
            if (headliner) {
                headliner = headliner
                    .replace(/\s+\d{1,2}:\d{2}\s*(am|pm)?/i, '')  // remove times
                    .replace(/\s*[\-–]\s*.*$/, '') // remove trailing dash descriptions
                    .replace(/\s+at\s+.*/i, '') // remove venue/location
                    .replace(/\s+in\s+.*/i, '') // remove "in the Garage/Control Room"
                    .trim();
            }

            // Dedupe supportingActs
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
                    calendarDateText: null,          // not pairing dates for now
                    calendarTitle: ev.title || null, // used as headliner fallback
                },
            }));

            if (context?.crawler && detailRequests.length > 0) {
                await context.crawler.addRequests(detailRequests);
                // Emit lightweight calendar summary rows in addition to queuing details.
                return events.map(ev => ({
                    eventDateRaw: null,
                    headliner: ev.title || ev.url,
                    supportingActs: [],
                    sourceUrl: ev.url,
                }));
            }

            // Listings page itself does not emit rows when not in crawler context,
            // but return summaries for tests and callers.
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

                // Treat all "with ..." names as supports;
                // keep headliner from calendarTitle/titleText
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
        },

        // ------------------------------------------------------------
        // Emos Austin – CALENDAR PAGE
        // Extracts artist names and times from event links
        // Link text format: "Artist Name [and Support Act] HH:MMPM"
        // ------------------------------------------------------------
        emosAustin: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href*="ticketmaster.com"]', (anchors) => {
                const results = [];
                const seen = new Set();

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    if (!text) continue;

                    // Skip if URL doesn't look like a Ticketmaster event
                    const href = a.href || '';
                    if (!/ticketmaster\.com/.test(href)) continue;

                    // De-duplicate by href
                    if (seen.has(href)) continue;
                    seen.add(href);

                    results.push({ text, url: href });
                }

                return results;
            });

            if (!events || !events.length) {
                return [];
            }

            // Parse each event: extract artist name and time from the link text
            const parseEvent = (linkText) => {
                // Remove "MOVED TO X - " prefix that some shows have
                let text = linkText.replace(/^MOVED\s+TO\s+[A-Z\s'-]+-?\s*/i, '');
                
                // Format: "Artist Name [and Support Act]HH:MMPM"
                // Time pattern: digits followed by optional minutes and AM/PM
                const timeMatch = text.match(/(\d{1,2}):?(\d{2})?\s*(am|pm)/i);
                
                let artistText = text;
                let timeText = null;

                if (timeMatch) {
                    timeText = timeMatch[0].trim();
                    // Remove time from artist text
                    artistText = text.replace(timeMatch[0], '').trim();
                }

                // Remove tour names: "Artist Name - Tour Name" -> "Artist Name"
                artistText = artistText.replace(/\s*-\s*(tour|leg).*$/i, '').trim();
                // Also remove any remaining trailing dash and cleanup
                artistText = artistText.replace(/[\s-–—]+$/, '').trim();

                if (!artistText) return null;

                // Split artists by "and", " w/ ", " with ", "&", commas
                const artistParts = artistText
                    .split(/\s+(?:and|w\/|with)\s+|,|\s+&\s+/i)
                    .map(p => p.trim())
                    .filter(Boolean);

                if (artistParts.length === 0) return null;

                return {
                    headliner: artistParts[0] || null,
                    supportingActs: artistParts.slice(1),
                    eventDateRaw: null, // Calendar doesn't show explicit dates in visible text
                    timeText,
                };
            };

            const normalizedEvents = [];
            for (const ev of events) {
                const parsed = parseEvent(ev.text);
                if (parsed) {
                    normalizedEvents.push({
                        ...parsed,
                        sourceUrl: ev.url,
                    });
                }
            }

            return normalizedEvents;
        },

        // ------------------------------------------------------------
        // Scoot Inn – CALENDAR PAGE
        // Extracts artist names and times from event links (identical to Emos Austin)
        // Link text format: "Artist Name [and Support Act] HH:MMPM"
        // ------------------------------------------------------------
        scootInn: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href*="ticketmaster.com"]', (anchors) => {
                const results = [];
                const seen = new Set();

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    if (!text) continue;

                    // Skip if URL doesn't look like a Ticketmaster event
                    const href = a.href || '';
                    if (!/ticketmaster\.com/.test(href)) continue;

                    // De-duplicate by href
                    if (seen.has(href)) continue;
                    seen.add(href);

                    results.push({ text, url: href });
                }

                return results;
            });

            if (!events || !events.length) {
                return [];
            }

            // Parse each event: extract artist name and time from the link text
            const parseEvent = (linkText) => {
                // Remove "MOVED TO X - " prefix that some shows have
                let text = linkText.replace(/^MOVED\s+TO\s+[A-Z\s'-]+-?\s*/i, '');
                
                // Format: "Artist Name [and Support Act]HH:MMPM"
                // Time pattern: digits followed by optional minutes and AM/PM
                const timeMatch = text.match(/(\d{1,2}):?(\d{2})?\s*(am|pm)/i);
                
                let artistText = text;
                let timeText = null;

                if (timeMatch) {
                    timeText = timeMatch[0].trim();
                    // Remove time from artist text
                    artistText = text.replace(timeMatch[0], '').trim();
                }

                // Remove tour names: "Artist Name - Tour Name" -> "Artist Name"
                artistText = artistText.replace(/\s*-\s*(tour|leg).*$/i, '').trim();
                // Also remove any remaining trailing dash and cleanup
                artistText = artistText.replace(/[\s-–—]+$/, '').trim();

                if (!artistText) return null;

                // Split artists by "and", " w/ ", " with ", "&", commas
                const artistParts = artistText
                    .split(/\s+(?:and|w\/|with)\s+|,|\s+&\s+/i)
                    .map(p => p.trim())
                    .filter(Boolean);

                if (artistParts.length === 0) return null;

                return {
                    headliner: artistParts[0] || null,
                    supportingActs: artistParts.slice(1),
                    eventDateRaw: null, // Calendar doesn't show explicit dates in visible text
                    timeText,
                };
            };

            const normalizedEvents = [];
            for (const ev of events) {
                const parsed = parseEvent(ev.text);
                if (parsed) {
                    normalizedEvents.push({
                        ...parsed,
                        sourceUrl: ev.url,
                    });
                }
            }

            return normalizedEvents;
        },

        // ------------------------------------------------------------
        // Antone's Nightclub – CALENDAR PAGE
        // FullCalendar daygrid with in-page event anchors (href like "#tw-event-dialog-<id>")
        // Link text includes artist and times, e.g. "Smallpools w/ Kevian Kraemer & The Romance Doors: 7:00pm Show: 8:00pm"
        // ------------------------------------------------------------
        antones: async ({ page, request, context }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a[href^="#tw-event-dialog-"], .fc-daygrid-event', (anchors) => {
                const results = [];
                const seen = new Set();

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    if (!text) continue;

                    const href = a.getAttribute('href') || a.href || '';
                    const key = href.startsWith('#') ? href : href;
                    if (seen.has(key)) continue;
                    seen.add(key);

                    let date = null;
                    try {
                        const dayEl = a.closest('[data-date], .fc-daygrid-day');
                        if (dayEl) {
                            date = dayEl.getAttribute('data-date') || null;
                            if (!date) {
                                const num = dayEl.querySelector('.fc-daygrid-day-number');
                                if (num) date = (num.textContent || '').trim();
                            }
                        }
                    } catch (e) {
                        date = null;
                    }

                    results.push({ text, url: href, calendarDate: date });
                }

                return results;
            });

            if (!events || !events.length) return [];

            // Helper to parse the visible anchor text into a lightweight summary.
            const parseEvent = (text) => {
                const s = (text || '').replace(/\s+/g, ' ').replace(/Doors\s*:/i, 'Doors:').replace(/Show\s*:/i, 'Show:').trim();
                const doorsMatch = s.match(/Doors:\s*(\d{1,2}:?\d{0,2}\s*(am|pm))/i);
                const showMatch = s.match(/Show:\s*(\d{1,2}:?\d{0,2}\s*(am|pm))/i);
                const doorsTime = doorsMatch ? doorsMatch[1].trim() : null;
                const showTime = showMatch ? showMatch[1].trim() : null;
                let titlePart = s.replace(/Doors:\s*\d{1,2}:?\d{0,2}\s*(am|pm)/i, '').replace(/Show:\s*\d{1,2}:?\d{0,2}\s*(am|pm)/i, '').trim();
                const parts = titlePart.split(/\s+w\/?\s+|\s+with\s+|,|\s+&\s+|\s+and\s+/i).map(p => p.trim()).filter(Boolean);
                return { headliner: parts[0]||null, supportingActs: parts.slice(1), eventDateRaw: null, doorsTime, showTime };
            };

            // Queue modal/dialog detail requests if crawler context is present
            if (context?.crawler) {
                const base = (request.loadedUrl || request.url).split('#')[0];
                const detailReqs = events.map((ev) => {
                    const dialogId = (ev.url || '').replace(/^#/, '') || null;
                    const url = base + (ev.url && ev.url.startsWith('#') ? ev.url : ev.url || '');
                    return {
                        url,
                        userData: {
                            venueId: request.userData && request.userData.venueId,
                            parserId: 'antonesEvent',
                            dialogId,
                            calendarDate: ev.calendarDate || null,
                        },
                    };
                });

                if (detailReqs.length) await context.crawler.addRequests(detailReqs);
                // Emit lightweight summary rows in addition to queuing detail pages.
                return events.map((ev) => {
                    const parsed = parseEvent(ev.text);
                    return { ...parsed, eventDateRaw: ev.calendarDate || parsed.eventDateRaw || null, sourceUrl: ev.url || sourceUrl };
                });
            }

            // Not running in crawler context: return parsed summary rows
            return events.map((ev) => {
                const parsed = parseEvent(ev.text);
                return { ...parsed, eventDateRaw: ev.calendarDate || parsed.eventDateRaw || null, sourceUrl: ev.url || sourceUrl };
            });
        },

        // ------------------------------------------------------------
        // Antone's – EVENT DIALOG PARSER
        // Parses event modal/dialog content referenced by calendar anchors
        // ------------------------------------------------------------
        antonesEvent: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;
            const dialogId = request.userData && request.userData.dialogId;
            const calendarDate = request.userData && request.userData.calendarDate;

            if (!dialogId) return [];

            const { heading, lines } = await page.evaluate((id) => {
                const el = document.getElementById(id);
                if (!el) return { heading: null, lines: [] };
                const heading = el.querySelector('h1, h2, h3, .tw-event-title')?.textContent?.trim() || null;
                const text = el.innerText || '';
                const all = text.split('\n').map(s => s.trim()).filter(Boolean);
                return { heading, lines: all };
            }, dialogId);

            let candidateLines = lines || [];
            if (!candidateLines.length) {
                const anchorText = await page.evaluate((id) => {
                    const a = document.querySelector(`a[href="#${id}"]`);
                    return a ? (a.textContent || '').trim() : null;
                }, dialogId);
                if (anchorText) candidateLines = [anchorText];
            }

            let headliner = heading || null;
            const supportingActs = [];
            let doorsTime = null;
            let showTime = null;

            for (const line of candidateLines) {
                if (!line) continue;
                const l = line.replace(/\s+/g,' ').trim();
                const dMatch = l.match(/Doors:\s*(\d{1,2}:?\d{0,2}\s*(am|pm))/i);
                const sMatch = l.match(/Show:\s*(\d{1,2}:?\d{0,2}\s*(am|pm))/i);
                if (dMatch) doorsTime = dMatch[1].trim();
                if (sMatch) showTime = sMatch[1].trim();

                if (/\bw\/?\b|\bwith\b|,|&| and /i.test(l)) {
                    const parts = l.split(/\s+w\/?\s+|\s+with\s+|,|\s+&\s+|\s+and\s+/i).map(p=>p.trim()).filter(Boolean);
                    if (!headliner && parts.length) headliner = parts[0];
                    for (let i=1;i<parts.length;i++) supportingActs.push(parts[i]);
                }

                if (!headliner && l.length>0 && l.length < 80 && !/Doors:|Show:|ticket/i.test(l)) {
                    headliner = l;
                }
            }

            const clean = (s) => (s||'').replace(/\s+at\s+.*/i,'').replace(/\s*-\s*Tour.*$/i,'').replace(/\(.*?\)/g,'').trim() || null;
            const head = clean(headliner);
            const supports = Array.from(new Set(supportingActs.map(clean).filter(Boolean)));

            return [ { eventDateRaw: calendarDate || null, headliner: head, supportingActs: supports, sourceUrl } ];
        },
    };

    // ------------------------------------------------------------------------
    // Build initial requests from input.venues
    // ------------------------------------------------------------------------
    const startRequests = venues.map((venue) => ({
        url: venue.startUrl,
        userData: {
            venueId: venue.id,
            parserId: venue.parserId || venue.id,
        },
    }));

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
                    // Remove "XYZ Presents:" patterns at the start
                    v = v.replace(/^[^:]+\s+(?:Presents?|Pres\.):\s*/i, '');
                    // Remove explicit time mentions like "@10pm", "@ 10:30 pm", "at 10pm"
                    v = v.replace(/@\s*\d{1,2}(:\d{2})?\s*(am|pm)?/i, '');
                    v = v.replace(/\bat\s+\d{1,2}(:\d{2})?\s*(am|pm)?\b/i, '');
                    // Remove trailing dash/en dash and anything after (often extra descriptors)
                    v = v.replace(/[\-–—]\s*[^,()]+$/g, '');
                    // Remove parenthetical notes
                    v = v.replace(/\(.*?\)/g, '');
                    // Normalize whitespace and punctuation
                    v = v.replace(/[\u2018\u2019\u201C\u201D]/g, "'");
                    v = v.replace(/[\s\u00A0]+/g, ' ').trim();
                    // Remove trailing commas or spurious punctuation
                    v = v.replace(/^[,;:\s]+|[,;:\s]+$/g, '');
                    if (!v) return null;
                    return v;
                };

                if (item.headliner) {
                    const rows = [];

                    // Split headliner string when it looks like multiple artists (comma, ' with ', 'feat', '&', 'and', '/').
                    const normalizeTitle = (s) => (s || '').replace(/\s*@\s*\d.*$/,'').replace(/\s*\(.*?\)\s*/g,'').trim();
                    const splitParts = (s) => (normalizeTitle(s).split(/,|\s+with\s+|\s+feat\.?\s+|\s+featuring\s+|\s+&\s+|\s+and\s+|\s*\/\s*|\s*\+\s*/i).map(p => p.trim()).filter(Boolean));

                    const headParts = splitParts(item.headliner);
                    const mainHeadlinerRaw = headParts[0] || item.headliner;
                    const mainHeadliner = cleanArtist(mainHeadlinerRaw) || mainHeadlinerRaw;

                    rows.push({ ...base, role: 'headliner', artistName: mainHeadliner });

                    // supportingActs from parser + parsed tail parts
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

            // Deduplicate normalized rows by artistName only (ignore role/date/sourceUrl)
            // This makes the dataset contain one row per artist (no repeats across dates).
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


