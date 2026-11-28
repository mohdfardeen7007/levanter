/** WA Web WAWebUsernameConstants */
export declare const USERNAME_MIN_LENGTH = 3;
export declare const USERNAME_MAX_LENGTH = 35;
export declare const USERNAME_KEY_LENGTH = 4;
export type UsernameValidationError = 'INVALID_CHARACTER' | 'INVALID_LENGTH' | 'INVALID_NO_LETTERS' | 'INVALID_PERIODS' | 'INVALID_WWW_PREFIX' | 'INVALID_DOMAIN_SUFFIX' | 'INVALID_WORD';
/** WA Web WAWebUsernameValidationUtils.validateUsernameLocally — same checks, same order */
export declare const validateUsername: (username: string) => {
    isValid: true;
} | {
    isValid: false;
    errorType: UsernameValidationError;
};
/** WA Web WAWebUsernameTypes.isUsernameKey: exactly 4 digits */
export declare const isValidUsernameKey: (key: string) => boolean;
/**
 * WA Web WAWebUsernameTypes.asMaybeUsername: empty -> undefined, one leading '@' stripped.
 * Unlike WA Web, a bare '@' also becomes undefined rather than ''.
 */
export declare const normalizeUsername: (username?: string | null) => string | undefined;
/**
 * WA Web useWAWebDebouncedUsernameLookup: `@name:1234` carries a key (kept only when valid),
 * a bare name has none; names failing local validation are not looked up.
 */
export declare const parseUsernameHandle: (handle: string) => {
    username: string;
    usernameKey?: string;
} | undefined;
export type BlocklistIdentity = {
    /** the contact's username handle */
    username?: string;
    /** saved address-book name; WA Web then prefers pn_jid over the username */
    name?: string;
    /** display name shown on the LID chat */
    displayName?: string;
};
/**
 * WA Web WAWebBlockUserJob.getBlocklistIdentifier + WASmaxOutBlocklistsBlocklistIds (zapo 471ca18a):
 * blocking a LID sends exactly one identifier — username, pn_jid, display_name or unknown_identifier.
 */
export declare const pickBlocklistIdentifier: (pnJid: string | undefined, identity?: BlocklistIdentity) => Record<string, string>;
//# sourceMappingURL=username.d.ts.map