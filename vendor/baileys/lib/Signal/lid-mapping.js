import { LRUCache } from 'lru-cache';
import { isHostedPnUser, isLidUser, isPnUser, jidDecode, jidNormalizedUser, WAJIDDomains } from '../WABinary/index.js';
export class LIDMappingStore {
    constructor(keys, logger, pnToLIDFunc) {
        this.mappingCache = new LRUCache({
            ttl: 3 * 24 * 60 * 60 * 1000, // 7 days
            ttlAutopurge: true,
            updateAgeOnGet: true
        });
        this.inflightLIDLookups = new Map();
        this.inflightPNLookups = new Map();
        this.keys = keys;
        this.pnToLIDFunc = pnToLIDFunc;
        this.logger = logger;
    }
    async storeLIDPNMappings(pairs) {
        if (pairs.length === 0)
            return;
        const validatedPairs = [];
        for (const { lid, pn } of pairs) {
            if (!((isLidUser(lid) && isPnUser(pn)) || (isPnUser(lid) && isLidUser(pn)))) {
                this.logger.warn(`Invalid LID-PN mapping: ${lid}, ${pn}`);
                continue;
            }
            const lidDecoded = jidDecode(lid);
            const pnDecoded = jidDecode(pn);
            // isLidUser / isPnUser only check the suffix: a jid without a user part ('@lid') is no mapping,
            // and storing it would overwrite (so wipe) the mapping already learned for that number
            if (!lidDecoded?.user || !pnDecoded?.user) {
                this.logger.warn(`Invalid LID-PN mapping: ${lid}, ${pn}`);
                continue;
            }
            validatedPairs.push({ pnUser: pnDecoded.user, lidUser: lidDecoded.user });
        }
        if (validatedPairs.length === 0)
            return;
        const cacheMissSet = new Set();
        const existingMappings = new Map();
        for (const { pnUser } of validatedPairs) {
            const cached = this.mappingCache.get(`pn:${pnUser}`);
            if (cached) {
                existingMappings.set(pnUser, cached);
            }
            else {
                cacheMissSet.add(pnUser);
            }
        }
        if (cacheMissSet.size > 0) {
            const cacheMisses = [...cacheMissSet];
            this.logger.trace(`Batch fetching ${cacheMisses.length} LID mappings from database`);
            const stored = await this.keys.get('lid-mapping', cacheMisses);
            for (const pnUser of cacheMisses) {
                const existingLidUser = stored[pnUser];
                if (existingLidUser) {
                    existingMappings.set(pnUser, existingLidUser);
                    this.mappingCache.set(`pn:${pnUser}`, existingLidUser);
                    this.mappingCache.set(`lid:${existingLidUser}`, pnUser);
                }
            }
        }
        const pairMap = {};
        for (const { pnUser, lidUser } of validatedPairs) {
            const existingLidUser = existingMappings.get(pnUser);
            if (existingLidUser === lidUser) {
                this.logger.debug({ pnUser, lidUser }, 'LID mapping already exists, skipping');
                continue;
            }
            pairMap[pnUser] = lidUser;
        }
        if (Object.keys(pairMap).length === 0)
            return;
        this.logger.trace({ pairMap }, `Storing ${Object.keys(pairMap).length} pn mappings`);
        const batchData = {};
        for (const [pnUser, lidUser] of Object.entries(pairMap)) {
            batchData[pnUser] = lidUser;
            batchData[`${lidUser}_reverse`] = pnUser;
            // LID reassignment: when this PN previously mapped to a different LID, delete
            // the stale reverse row so getPNForLID(oldLID) stops returning this PN.
            const oldLidUser = existingMappings.get(pnUser);
            if (oldLidUser && oldLidUser !== lidUser) {
                batchData[`${oldLidUser}_reverse`] = null;
            }
        }
        await this.keys.transaction(async () => {
            await this.keys.set({ 'lid-mapping': batchData });
        }, 'lid-mapping');
        // Update cache after successful DB write
        for (const [pnUser, lidUser] of Object.entries(pairMap)) {
            this.mappingCache.set(`pn:${pnUser}`, lidUser);
            this.mappingCache.set(`lid:${lidUser}`, pnUser);
            // drop the stale reverse cache entry for a reassigned LID
            const oldLidUser = existingMappings.get(pnUser);
            if (oldLidUser && oldLidUser !== lidUser) {
                this.mappingCache.delete(`lid:${oldLidUser}`);
            }
        }
    }
    async getLIDForPN(pn) {
        return (await this.getLIDsForPNs([pn]))?.[0]?.lid || null;
    }
    async getLIDsForPNs(pns) {
        if (pns.length === 0)
            return null;
        const sortedPns = [...new Set(pns)].sort();
        const cacheKey = sortedPns.join(',');
        const inflight = this.inflightLIDLookups.get(cacheKey);
        if (inflight) {
            this.logger.trace(`Coalescing getLIDsForPNs request for ${sortedPns.length} PNs`);
            return inflight;
        }
        const promise = this._getLIDsForPNsImpl(pns);
        this.inflightLIDLookups.set(cacheKey, promise);
        try {
            return await promise;
        }
        finally {
            this.inflightLIDLookups.delete(cacheKey);
        }
    }
    /**
     * The LIDs we already know for these PNs, from the cache and the DB only: never a USync, never null. A PN with no
     * stored LID is left out. The device is kept, device 99 going to the hosted LID server, like getLIDsForPNs.
     * WA Web maps a status audience this way (WAWebLidMigrationUtils.toUserLid, a local lookup)
     */
    async getKnownLIDsForPNs(pns) {
        const pnUsers = new Map();
        for (const pn of pns) {
            const user = (isPnUser(pn) || isHostedPnUser(pn)) && jidDecode(pn)?.user;
            if (user) {
                pnUsers.set(pn, user);
            }
        }
        const uncached = [...new Set(pnUsers.values())].filter(user => !this.mappingCache.get(`pn:${user}`));
        const stored = uncached.length ? await this.keys.get('lid-mapping', uncached) : {};
        for (const user of uncached) {
            const lidUser = stored[user]?.toString();
            if (lidUser) {
                this.mappingCache.set(`pn:${user}`, lidUser);
                this.mappingCache.set(`lid:${lidUser}`, user);
            }
        }
        const pairs = [];
        for (const [pn, user] of pnUsers) {
            const lidUser = this.mappingCache.get(`pn:${user}`);
            if (!lidUser)
                continue;
            const decoded = jidDecode(pn);
            const device = decoded.device || 0;
            const server = decoded.server === 'hosted' || device === 99 ? 'hosted.lid' : 'lid';
            pairs.push({ pn, lid: `${lidUser}${device ? `:${device}` : ''}@${server}` });
        }
        return pairs;
    }
    async _getLIDsForPNsImpl(pns) {
        const usyncFetch = {};
        const successfulPairs = {};
        for (const pn of pns) {
            if (!isPnUser(pn) && !isHostedPnUser(pn))
                continue;
            const decoded = jidDecode(pn);
            if (!decoded)
                continue;
            const pnUser = decoded.user;
            let lidUser = this.mappingCache.get(`pn:${pnUser}`);
            if (!lidUser) {
                // Cache miss - check database
                const stored = await this.keys.get('lid-mapping', [pnUser]);
                lidUser = stored[pnUser];
                if (lidUser) {
                    this.mappingCache.set(`pn:${pnUser}`, lidUser);
                    this.mappingCache.set(`lid:${lidUser}`, pnUser);
                }
                else {
                    this.logger.trace(`No LID mapping found for PN user ${pnUser}; batch getting from USync`);
                    const device = decoded.device || 0;
                    let normalizedPn = jidNormalizedUser(pn);
                    if (isHostedPnUser(normalizedPn)) {
                        normalizedPn = `${pnUser}@s.whatsapp.net`;
                    }
                    if (!usyncFetch[normalizedPn]) {
                        usyncFetch[normalizedPn] = [device];
                    }
                    else {
                        usyncFetch[normalizedPn]?.push(device);
                    }
                    continue;
                }
            }
            lidUser = lidUser.toString();
            if (!lidUser) {
                this.logger.warn(`Invalid or empty LID user for PN ${pn}: lidUser = "${lidUser}"`);
                return null;
            }
            // Push the PN device ID to the LID to maintain device separation
            const pnDevice = decoded.device !== undefined ? decoded.device : 0;
            const deviceSpecificLid = `${lidUser}${!!pnDevice ? `:${pnDevice}` : ``}@${decoded.server === 'hosted' ? 'hosted.lid' : 'lid'}`;
            this.logger.trace(`getLIDForPN: ${pn} → ${deviceSpecificLid} (user mapping with device ${pnDevice})`);
            successfulPairs[pn] = { lid: deviceSpecificLid, pn };
        }
        if (Object.keys(usyncFetch).length > 0) {
            const result = await this.pnToLIDFunc?.(Object.keys(usyncFetch)); // this function already adds LIDs to mapping
            if (result && result.length > 0) {
                await this.storeLIDPNMappings(result);
                for (const pair of result) {
                    const pnDecoded = jidDecode(pair.pn);
                    const pnUser = pnDecoded?.user;
                    if (!pnUser)
                        continue;
                    const lidUser = jidDecode(pair.lid)?.user;
                    if (!lidUser)
                        continue;
                    for (const device of usyncFetch[pair.pn]) {
                        const deviceSpecificLid = `${lidUser}${!!device ? `:${device}` : ``}@${device === 99 ? 'hosted.lid' : 'lid'}`;
                        this.logger.trace(`getLIDForPN: USYNC success for ${pair.pn} → ${deviceSpecificLid} (user mapping with device ${device})`);
                        const deviceSpecificPn = `${pnUser}${!!device ? `:${device}` : ``}@${device === 99 ? 'hosted' : 's.whatsapp.net'}`;
                        successfulPairs[deviceSpecificPn] = { lid: deviceSpecificLid, pn: deviceSpecificPn };
                    }
                }
            }
            else {
                return null;
            }
        }
        return Object.values(successfulPairs);
    }
    /**
     * Get PNs for LIDs - USER LEVEL with device construction
     */
    async getPNsForLIDs(lids) {
        const result = [];
        const missingLids = [];
        for (const lid of lids) {
            if (!isLidUser(lid)) {
                continue;
            }
            const decoded = jidDecode(lid);
            if (!decoded) {
                continue;
            }
            const lidUser = decoded.user;
            const pnUser = this.mappingCache.get(`lid:${lidUser}`);
            if (!pnUser || typeof pnUser !== 'string') {
                missingLids.push(lidUser);
            }
            else {
                const lidDevice = decoded.device !== undefined ? decoded.device : 0;
                const domain = decoded.domainType === WAJIDDomains.HOSTED_LID ? 'hosted' : 's.whatsapp.net';
                const pnJid = `${pnUser}${!!lidDevice ? `:${lidDevice}` : ``}@${domain}`;
                result.push({ lid, pn: pnJid });
            }
        }
        if (missingLids.length > 0) {
            const keys = missingLids.map(l => `${l}_reverse`);
            const stored = await this.keys.get('lid-mapping', keys);
            for (const lidUser of missingLids) {
                const pnUser = stored[`${lidUser}_reverse`];
                if (pnUser && typeof pnUser === 'string') {
                    this.mappingCache.set(`lid:${lidUser}`, pnUser);
                    // Match on the decoded user, NOT a string prefix: `startsWith` would map
                    // e.g. "4471@lid" to "447"'s PN (wrong recipient) since "4471" starts with "447".
                    for (const lid of lids) {
                        const decoded = jidDecode(lid);
                        if (decoded && decoded.user === lidUser) {
                            const lidDevice = decoded.device !== undefined ? decoded.device : 0;
                            const domain = decoded.domainType === WAJIDDomains.HOSTED_LID ? 'hosted' : 's.whatsapp.net';
                            const pnJid = `${pnUser}${!!lidDevice ? `:${lidDevice}` : ``}@${domain}`;
                            result.push({ lid, pn: pnJid });
                        }
                    }
                }
            }
        }
        return result;
    }
    async getPNForLID(lid) {
        const results = await this.getPNsForLIDs([lid]);
        return results?.[0]?.pn || null;
    }
}
//# sourceMappingURL=lid-mapping.js.map