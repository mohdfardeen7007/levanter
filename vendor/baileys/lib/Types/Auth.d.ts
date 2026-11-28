import type { proto } from '../../WAProto/index.js';
import type { Contact } from './Contact.js';
import type { MinimalMessage } from './Message.js';
export type KeyPair = {
    public: Uint8Array;
    private: Uint8Array;
};
export type SignedKeyPair = {
    keyPair: KeyPair;
    signature: Uint8Array;
    keyId: number;
    timestampS?: number;
};
export type ProtocolAddress = {
    name: string;
    deviceId: number;
};
export type SignalIdentity = {
    identifier: ProtocolAddress;
    identifierKey: Uint8Array;
};
export type LIDMapping = {
    pn: string;
    lid: string;
};
export type LTHashState = {
    version: number;
    hash: Buffer;
    indexValueMap: {
        [indexMacBase64: string]: {
            valueMac: Uint8Array | Buffer;
        };
    };
};
export type SignalCreds = {
    readonly signedIdentityKey: KeyPair;
    readonly signedPreKey: SignedKeyPair;
    readonly registrationId: number;
};
export type AccountSettings = {
    /** unarchive chats when a new message is received */
    unarchiveChats: boolean;
    /** the default mode to start new conversations with */
    defaultDisappearingMode?: Pick<proto.IConversation, 'ephemeralExpiration' | 'ephemeralSettingTimestamp'>;
};
export type AuthenticationCreds = SignalCreds & {
    readonly noiseKey: KeyPair;
    readonly pairingEphemeralKeyPair: KeyPair;
    advSecretKey: string;
    me?: Contact;
    account?: proto.IADVSignedDeviceIdentity;
    signalIdentities?: SignalIdentity[];
    myAppStateKeyId?: string;
    firstUnuploadedPreKeyId: number;
    nextPreKeyId: number;
    lastAccountSyncTimestamp?: number;
    platform?: string;
    /**
     * NCT salt for deriving cstokens, delivered via the `nct_salt_sync` app-state
     * collection. Used as the privacy-token fallback on 1:1 sends when no tctoken
     * is available (avoids the 463 reach-out rate-lock). See [[Utils/cstoken]].
     */
    nctSalt?: Uint8Array;
    /**
     * Companion meta nonce (HistorySync field 16), captured on the initial bootstrap.
     * Sent as the `Companion_User_Secret` header when deleting media the companion
     * uploaded. Mirrors whatsmeow's Store.CompanionMetaNonce (6723e55).
     */
    companionMetaNonce?: string;
    /**
     * WA Web LOGIN_COUNTER: reset to 0 when pairing, +1 on every successful stream open,
     * sent as ClientPayload.lc on login (WAWebClientPayload / WAWebStartBackend).
     */
    loginCounter?: number;
    /**
     * Server-issued attributes captured from the post-login `<success>` stanza
     * (account creation time, edge location, props/abprops catalogue versions, and
     * the companion static key). Informational / used for AB-prop version tracking
     * and diagnostics; persisted only when a value actually changes.
     */
    accountCreationTs?: number;
    connectionLocation?: string;
    propsVersion?: string;
    abPropsVersion?: string;
    companionEncStatic?: Uint8Array;
    processedHistoryMessages: MinimalMessage[];
    /** number of times history & app state has been synced */
    accountSyncCounter: number;
    accountSettings: AccountSettings;
    registered: boolean;
    pairingCode: string | undefined;
    lastPropHash: string | undefined;
    routingInfo: Buffer | undefined;
    additionalData?: any | undefined;
    /**
     * Deat of the LID migration
     */
    lidMigrationTimestamp?: number;
};
export type SignalDataTypeMap = {
    'pre-key': KeyPair;
    session: Uint8Array;
    'sender-key': Uint8Array;
    'sender-key-memory': {
        [jid: string]: boolean;
    };
    'app-state-sync-key': proto.Message.IAppStateSyncKeyData;
    'app-state-sync-version': LTHashState;
    'lid-mapping': string;
    'device-list': string[];
    'identity-key': Uint8Array;
    tctoken: {
        token?: Buffer;
        timestamp?: string;
        senderTimestamp?: number;
    };
};
export type SignalDataSet = {
    [T in keyof SignalDataTypeMap]?: {
        [id: string]: SignalDataTypeMap[T] | null;
    };
};
type Awaitable<T> = T | Promise<T>;
export type SignalKeyStore = {
    get<T extends keyof SignalDataTypeMap>(type: T, ids: string[]): Awaitable<{
        [id: string]: SignalDataTypeMap[T];
    }>;
    set(data: SignalDataSet): Awaitable<void>;
    /** clear all the data in the store */
    clear?(): Awaitable<void>;
};
export type SignalKeyStoreWithTransaction = SignalKeyStore & {
    isInTransaction: () => boolean;
    transaction<T>(exec: () => Promise<T>, key: string): Promise<T>;
    /**
     * Run `work` OUTSIDE any active transaction context so its reads/writes go straight to
     * the underlying store. Needed for fire-and-forget writes scheduled inside a transaction
     * body but executed AFTER that transaction has committed — without this they land in the
     * already-flushed transaction context and are silently dropped.
     */
    exitTransaction<T>(work: () => Promise<T>): Promise<T>;
};
export type TransactionCapabilityOptions = {
    maxCommitRetries: number;
    delayBetweenTriesMs: number;
};
export type SignalAuthState = {
    creds: SignalCreds;
    keys: SignalKeyStore | SignalKeyStoreWithTransaction;
};
export type AuthenticationState = {
    creds: AuthenticationCreds;
    keys: SignalKeyStore;
};
export {};
//# sourceMappingURL=Auth.d.ts.map