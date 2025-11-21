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

            const { lineup, fullDate } = await page.evaluate(() => {
                const text = document.body.innerText || '';
                const allLines = text
                    .split('\n')
                    .map((t) => t.trim())
                    .filter(Boolean);

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

                const lineupLines = [];
                if (startIndex !== -1) {
                    for (let i = startIndex + 1; i < allLines.length; i++) {
                        const line = allLines[i];
                        if (!line) continue;

                        if (/^www\./i.test(line)) break;
                        if (/Venue info/i.test(line)) break;

                        lineupLines.push(line);
                    }
                }

                return {
                    lineup: lineupLines,
                    fullDate: eventDateFull,
                };
            });

            const cleanedLineup = Array.from(
                new Set(
                    (lineup || []).map((name) =>
                        name.replace(/\s+/g, ' ').trim(),
                    ),
                ),
            ).filter(Boolean);

            const headliner = cleanedLineup[0] || null;
            const supportingActs = cleanedLineup.slice(1);

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