/** WA Web WAWebUsernameConstants */
export const USERNAME_MIN_LENGTH = 3;
export const USERNAME_MAX_LENGTH = 35;
export const USERNAME_KEY_LENGTH = 4;
const ALLOWED_CHARACTERS = /^[A-Za-z0-9_.]*$/;
const HAS_LETTER = /[A-Za-z]/;
const DOMAIN_SUFFIXES = [
    '.com',
    '.org',
    '.net',
    '.int',
    '.edu',
    '.gov',
    '.mil',
    '.arpa',
    '.html',
    '.htm',
    '.txt',
    '.xml'
];
const RESERVED_WORDS = ['whatsapp', 'instagram', 'facebook', 'oculus'];
/** WA Web WAWebUsernameValidationUtils.validateUsernameLocally — same checks, same order */
export const validateUsername = (username) => {
    const lower = username.toLowerCase();
    if (!ALLOWED_CHARACTERS.test(username)) {
        return { isValid: false, errorType: 'INVALID_CHARACTER' };
    }
    if (username.length < USERNAME_MIN_LENGTH || username.length > USERNAME_MAX_LENGTH) {
        return { isValid: false, errorType: 'INVALID_LENGTH' };
    }
    if (!HAS_LETTER.test(username)) {
        return { isValid: false, errorType: 'INVALID_NO_LETTERS' };
    }
    if (username.startsWith('.') || username.endsWith('.') || username.includes('..')) {
        return { isValid: false, errorType: 'INVALID_PERIODS' };
    }
    if (lower.startsWith('www.')) {
        return { isValid: false, errorType: 'INVALID_WWW_PREFIX' };
    }
    if (DOMAIN_SUFFIXES.some(suffix => lower.endsWith(suffix))) {
        return { isValid: false, errorType: 'INVALID_DOMAIN_SUFFIX' };
    }
    if (RESERVED_WORDS.some(word => lower.includes(word))) {
        return { isValid: false, errorType: 'INVALID_WORD' };
    }
    return { isValid: true };
};
/** WA Web WAWebUsernameTypes.isUsernameKey: exactly 4 digits */
export const isValidUsernameKey = (key) => key.length === USERNAME_KEY_LENGTH && /^[0-9]{4}$/.test(key);
/**
 * WA Web WAWebUsernameTypes.asMaybeUsername: empty -> undefined, one leading '@' stripped.
 * Unlike WA Web, a bare '@' also becomes undefined rather than ''.
 */
export const normalizeUsername = (username) => {
    if (!username) {
        return undefined;
    }
    return (username.startsWith('@') ? username.slice(1) : username) || undefined;
};
/**
 * WA Web useWAWebDebouncedUsernameLookup: `@name:1234` carries a key (kept only when valid),
 * a bare name has none; names failing local validation are not looked up.
 */
export const parseUsernameHandle = (handle) => {
    const hasAt = handle.startsWith('@');
    let username = hasAt ? handle.slice(1) : handle;
    let usernameKey;
    if (hasAt) {
        const [name = '', key] = username.split(':');
        username = name;
        if (key !== undefined && isValidUsernameKey(key)) {
            usernameKey = key;
        }
    }
    if (!validateUsername(username).isValid) {
        return undefined;
    }
    return { username, ...(usernameKey ? { usernameKey } : {}) };
};
/**
 * WA Web WAWebBlockUserJob.getBlocklistIdentifier + WASmaxOutBlocklistsBlocklistIds (zapo 471ca18a):
 * blocking a LID sends exactly one identifier — username, pn_jid, display_name or unknown_identifier.
 */
export const pickBlocklistIdentifier = (pnJid, identity = {}) => {
    const username = normalizeUsername(identity.username);
    if (pnJid && username) {
        return identity.name ? { pn_jid: pnJid } : { username };
    }
    if (username) {
        return { username };
    }
    if (pnJid) {
        return { pn_jid: pnJid };
    }
    if (identity.displayName) {
        return { display_name: identity.displayName };
    }
    return { unknown_identifier: 'true' };
};
//# sourceMappingURL=username.js.map