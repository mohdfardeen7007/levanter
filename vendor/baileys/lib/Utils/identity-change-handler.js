import { getBinaryNodeChild, areJidsSameUser, jidDecode } from '../WABinary/index.js';
export const handleIdentityChange = async (node, { meId, meLid, validateSession, assertSessions, debounceCache, logger }) => {
    const from = node.attrs.from;
    const isOffline = !!node.attrs.offline;
    const identityNode = getBinaryNodeChild(node, 'identity');
    if (!from) {
        logger.warn({ node }, 'identity change notification without from JID');
        return { action: 'no_identity_node' };
    }
    if (!identityNode) {
        return { action: 'no_identity_node' };
    }
    if (isOffline) {
        logger.debug({ jid: from }, 'skipping identity change (offline)');
        return { action: 'skipped_offline' };
    }
    const { device } = jidDecode(from);
    if (device && device !== 0) {
        logger.debug({ jid: from }, 'skipping identity change (companion device)');
        return { action: 'skipped_companion' };
    }
    if ((meId && areJidsSameUser(from, meId)) || (meLid && areJidsSameUser(from, meLid))) {
        logger.debug({ jid: from }, 'skipping identity change (self)');
        return { action: 'skipped_self' };
    }
    if (debounceCache.get(from)) {
        logger.debug({ jid: from }, 'skipping identity change (debounced)');
        return { action: 'debounced' };
    }
    debounceCache.set(from, true);
    const { exists } = await validateSession(from);
    if (!exists) {
        logger.debug({ jid: from }, 'skipping identity change (no session)');
        return { action: 'no_session' };
    }
    try {
        logger.info({ jid: from }, 'identity changed, refreshing session');
        await assertSessions([from], true);
        return { action: 'session_refreshed' };
    }
    catch (error) {
        logger.warn({ error, jid: from }, 'failed to assert sessions after identity change');
        return { action: 'session_refreshed' };
    }
};
//# sourceMappingURL=identity-change-handler.js.map