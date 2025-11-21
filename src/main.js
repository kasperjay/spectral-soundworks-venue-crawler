import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';

Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        venues = [],
        proxyConfiguration: proxyConfigInput,
        maxConcurrency = 2,
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
        // ------------------------------------------------------------
        mohawkAustin: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('.list-view-details', (cards, sourceUrlInner) => {
                const results = [];

                for (const card of cards) {
                    const headliner = card
                        .querySelector('.event-name.headliners a, .event-name.headliners')
                        ?.textContent?.trim();

                    const supportsText = card
                        .querySelector('.supports a, .supports')
                        ?.textContent?.trim() || '';

                    const supportingActs = supportsText
                        .split(',')
                        .map((s) => s.trim())
                        .filter(Boolean);

                    const dateText =
                        card.querySelector('.date, .event-date, time')?.textContent?.trim() ??
                        null;

                    if (!headliner) continue;

                    results.push({
                        eventDateRaw: dateText,
                        headliner,
                        supportingActs,
                        sourceUrl: sourceUrlInner,
                    });
                }

                return results;
            }, sourceUrl);

            return events || [];
        },

        // ------------------------------------------------------------
        // Come and Take It – CALENDAR PAGE
        // Finds "More Info" links and queues event pages
        // ------------------------------------------------------------
        comeAndTakeIt: async ({ page, request, context }) => {
            const { venueId } = request.userData || {};
            const sourceUrl = request.loadedUrl || request.url;

            const events = await page.$$eval('a', (anchors) => {
                const items = [];

                for (const a of anchors) {
                    const text = (a.textContent || '').trim();
                    if (!/More Info/i.test(text)) continue;

                    const href = a.getAttribute('href');
                    if (!href) continue;

                    const url = a.href;
                    items.push({ url });
                }

                return items;
            });

            if (!events || !events.length) {
                return [];
            }

            const detailRequests = events.map((ev) => ({
                url: ev.url,
                userData: {
                    venueId,
                    parserId: 'comeAndTakeItEvent',
                    // you could pass calendar date/title later if needed
                },
            }));

            if (context?.crawler && detailRequests.length > 0) {
                await context.crawler.addRequests(detailRequests);
            }

            // Calendar itself does not emit rows; event pages will.
            return [];
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
        // Continental Club Austin – calendar page entries with times
        // ------------------------------------------------------------
        continentalClubAustin: async ({ page, request }) => {
            const sourceUrl = request.loadedUrl || request.url;

            const rows = await page.$$eval('.timely-event', (nodes) => {
                // Step 1: collect raw events with date + time + artist
                const rawEvents = [];
                let unknownDateCounter = 0;

                function parseTimeToMinutes(timeStr) {
                    if (!timeStr) return null;

                    const s = timeStr.trim().toLowerCase();
                    // Handles "9:30pm", "6:00 am", "10pm"
                    const m = s.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
                    if (!m) return null;

                    let hour = parseInt(m[1], 10);
                    const minutes = m[2] ? parseInt(m[2], 10) : 0;
                    const meridiem = m[3];

                    if (meridiem === 'pm' && hour !== 12) hour += 12;
                    if (meridiem === 'am' && hour === 12) hour = 0;

                    return hour * 60 + minutes;
                }

                nodes.forEach((node, domIndex) => {
                    const titleSpan = node.querySelector('.timely-event-title-text');
                    if (!titleSpan) return;

                    // Time text, e.g. "2:30pm"
                    const timeEl = titleSpan.querySelector('.timely-event-time');
                    const timeText = timeEl ? timeEl.textContent || '' : '';

                    // Clone to safely remove time and get just the artist
                    const clone = titleSpan.cloneNode(true);
                    const cloneTimeEl = clone.querySelector('.timely-event-time');
                    if (cloneTimeEl) cloneTimeEl.remove();

                    const artistName = (clone.textContent || '').trim();
                    if (!artistName) return;

                    // Date from aria-label: "2:30pm - 11:59pm, Marshall Hood, no location, 2 November 2025"
                    let eventDateRaw = null;
                    const aria = node.getAttribute('aria-label') || '';
                    if (aria) {
                        const parts = aria.split(',').map((p) => p.trim()).filter(Boolean);
                        if (parts.length) {
                            const last = parts[parts.length - 1];
                            if (/\d{4}$/.test(last)) {
                                eventDateRaw = last; // "2 November 2025"
                            }
                        }
                    }

                    const minutes = parseTimeToMinutes(timeText);

                    rawEvents.push({
                        eventDateRaw,
                        artistName,
                        minutes,
                        domIndex,
                        groupKey: eventDateRaw || `unknown-${unknownDateCounter++}`,
                    });
                });

                // Step 2: group by date and assign roles
                const byDate = new Map();

                for (const ev of rawEvents) {
                    const key = ev.groupKey;
                    if (!byDate.has(key)) byDate.set(key, []);
                    byDate.get(key).push(ev);
                }

                const rowsInner = [];

                for (const [dateKey, eventsOnDate] of byDate.entries()) {
                    if (!eventsOnDate.length) continue;

                    // Sort by start time; nulls treated as very early
                    eventsOnDate.sort((a, b) => {
                        const ma = a.minutes;
                        const mb = b.minutes;

                        if (ma == null && mb == null) {
                            return a.domIndex - b.domIndex;
                        }

                        if (ma == null) return -1;
                        if (mb == null) return 1;

                        return ma - mb;
                    });

                    const headlinerEvent = eventsOnDate[eventsOnDate.length - 1];

                    for (const ev of eventsOnDate) {
                        rowsInner.push({
                            eventDateRaw: ev.eventDateRaw,
                            artistName: ev.artistName,
                            role: ev === headlinerEvent ? 'headliner' : 'support',
                        });
                    }
                }

                return rowsInner;
            });

            // Attach sourceUrl back in Node context
            return (rows || []).map((row) => ({
                ...row,
                sourceUrl,
            }));
        },

        // ------------------------------------------------------------
        // Stubb's Austin – CONCERT LISTINGS PAGE
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
            }

            // Listings page itself does not emit rows; /tm-event pages will.
            return [];
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

                if (item.headliner) {
                    const rows = [
                        {
                            ...base,
                            role: 'headliner',
                            artistName: item.headliner,
                        },
                    ];

                    if (Array.isArray(item.supportingActs)) {
                        for (const name of item.supportingActs) {
                            if (!name) continue;
                            rows.push({
                                ...base,
                                role: 'support',
                                artistName: name,
                            });
                        }
                    }

                    return rows;
                }

                if (item.artistName) {
                    return [
                        {
                            ...base,
                            role: item.role || 'unknown',
                            artistName: item.artistName,
                        },
                    ];
                }

                log.warning(
                    `Item from parser "${parserKey}" had no headliner or artistName on ${request.url}.`,
                );
                return [];
            });

            if (!normalizedRows.length) {
                log.warning(
                    `No normalized rows produced by parser "${parserKey}" for ${request.url}.`,
                );
                return;
            }

            log.info(
                `Pushing ${normalizedRows.length} row(s) to dataset from ${request.url} (parser="${parserKey}")`,
            );

            await Actor.pushData(normalizedRows);
        },

        async failedRequestHandler({ request, log }) {
            log.error(`Request ${request.url} failed too many times.`);
        },
    });

    await crawler.run(startRequests);
});


