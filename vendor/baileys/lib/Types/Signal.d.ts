import { proto } from '../../WAProto/index.js';
import type { LIDMappingStore } from '../Signal/lid-mapping.js';
type DecryptGroupSignalOpts = {
    group: string;
    authorJid: string;
    msg: Uint8Array;
};
type ProcessSenderKeyDistributionMessageOpts = {
    item: proto.Message.ISenderKeyDistributionMessage;
    authorJid: string;
};
/**
 * A 1:1 session kept apart from the default one (WA Web WAWebSessionScope): `status` is the session a sender marks
 * with `<meta session_scope="status">` on a status, stored at the default address + `_status` (WAWebSignalAddress)
 */
export type SessionScope = 'status';
type DecryptSignalProtoOpts = {
    jid: string;
    type: 'pkmsg' | 'msg';
    ciphertext: Uint8Array;
    /** `<enc type="pkmsg" state="false">`: decrypt without persisting the resulting session (whatsmeow f376da26) */
    stateless?: boolean;
    /** decrypt with this scope's session instead of the default one */
    scope?: SessionScope;
};
type EncryptMessageOpts = {
    jid: string;
    data: Uint8Array;
};
type EncryptGroupMessageOpts = {
    group: string;
    data: Uint8Array;
    meId: string;
};
type GetSenderKeyDistributionMessageOpts = {
    group: string;
    meId: string;
};
type PreKey = {
    keyId: number;
    publicKey: Uint8Array;
};
type SignedPreKey = PreKey & {
    signature: Uint8Array;
};
type E2ESession = {
    registrationId: number;
    identityKey: Uint8Array;
    signedPreKey: SignedPreKey;
    preKey?: PreKey;
};
type E2ESessionOpts = {
    jid: string;
    session: E2ESession;
};
export type SignalRepository = {
    decryptGroupMessage(opts: DecryptGroupSignalOpts): Promise<Uint8Array>;
    processSenderKeyDistributionMessage(opts: ProcessSenderKeyDistributionMessageOpts): Promise<void>;
    decryptMessage(opts: DecryptSignalProtoOpts): Promise<Uint8Array>;
    encryptMessage(opts: EncryptMessageOpts): Promise<{
        type: 'pkmsg' | 'msg';
        ciphertext: Uint8Array;
    }>;
    encryptGroupMessage(opts: EncryptGroupMessageOpts): Promise<{
        senderKeyDistributionMessage: Uint8Array;
        ciphertext: Uint8Array;
    }>;
    getSenderKeyDistributionMessage(opts: GetSenderKeyDistributionMessageOpts): Promise<Uint8Array>;
    hasSenderKey(opts: GetSenderKeyDistributionMessageOpts): Promise<boolean>;
    getSessionInfo(jid: string): Promise<{
        baseKey: Uint8Array;
        registrationId: number;
    } | null>;
    injectE2ESession(opts: E2ESessionOpts): Promise<void>;
    validateSession(jid: string): Promise<{
        exists: boolean;
        reason?: string;
    }>;
    jidToSignalProtocolAddress(jid: string): string;
    migrateSession(fromJid: string, toJid: string): Promise<{
        migrated: number;
        skipped: number;
        total: number;
    }>;
    /** deletes the sessions of `jids` in every scope (WA Web deleteRemoteSession) */
    deleteSession(jids: string[]): Promise<void>;
    deleteIdentity(jids: string[]): Promise<void>;
    validateSessions(jids: string[]): Promise<Record<string, {
        exists: boolean;
        reason?: string;
    }>>;
};
export interface SignalRepositoryWithLIDStore extends SignalRepository {
    lidMapping: LIDMappingStore;
}
export {};
//# sourceMappingURL=Signal.d.ts.map