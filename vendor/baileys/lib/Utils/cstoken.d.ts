/**
 * Whether a cstoken may be sent to this JID. Mirrors whatsmeow's shouldSendCsToken:
 * only real user JIDs (PN or LID), never the PSA sender or bots.
 */
export declare const shouldSendCsToken: (jid: string) => boolean;
/**
 * Derive a cstoken for a recipient: `HMAC-SHA256(nctSalt, recipientLID)`.
 * The token is keyed to the recipient's LID, so a PN JID is resolved to its LID
 * first; if no LID is known the token can't be derived. Mirrors whatsmeow's
 * generateCsToken. Returns undefined when a cstoken shouldn't/can't be produced.
 */
export declare const generateCsToken: ({ jid, nctSalt, getLIDForPN }: {
    jid: string;
    nctSalt?: Uint8Array;
    getLIDForPN: (pn: string) => Promise<string | null | undefined>;
}) => Promise<Uint8Array | undefined>;
//# sourceMappingURL=cstoken.d.ts.map