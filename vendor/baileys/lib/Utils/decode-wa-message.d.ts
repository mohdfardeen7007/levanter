import type { SocketConfig, WAMessage } from '../Types/index.js';
import type { SignalRepositoryWithLIDStore } from '../Types/Signal.js';
import { type BinaryNode } from '../WABinary/index.js';
export declare const getDecryptionJid: (sender: string, repository: SignalRepositoryWithLIDStore) => Promise<string>;
export declare const NO_MESSAGE_FOUND_ERROR_TEXT = "Message absent from node";
export declare const MISSING_KEYS_ERROR_TEXT = "Key used already or never filled";
export declare const DECRYPTION_RETRY_CONFIG: {
    maxRetries: number;
    baseDelayMs: number;
    sessionRecordErrors: string[];
};
export declare const NACK_REASONS: {
    ParsingError: number;
    UnrecognizedStanza: number;
    UnrecognizedStanzaClass: number;
    UnrecognizedStanzaType: number;
    InvalidProtobuf: number;
    InvalidHostedCompanionStanza: number;
    MissingMessageSecret: number;
    SignalErrorOldCounter: number;
    MessageDeletedOnPeer: number;
    UnhandledError: number;
    UnsupportedAdminRevoke: number;
    UnsupportedLIDGroup: number;
    DBOperationFailed: number;
    NackInvalidHostedCompanionStanza: number;
    NackUnhandledError: number;
    NackUnsupportedAdminRevoke: number;
    NackUnsupportedLIDGroup: number;
    NackDBOperationFailed: number;
};
/**
 * Server-side error codes returned in ack stanzas (server → client).
 * These are distinct from the client-side NackReason enum
 * (WAWebCreateNackFromStanza) which covers client→server nack codes.
 * 421 and 475 happen to overlap numerically, but 463 and 479 are
 * server-specific codes not present in the client enum.
 */
export declare const SERVER_ERROR_CODES: {
    /** Group addressing mode is stale — re-query group metadata */
    readonly StaleGroupAddressingMode: "421";
    /**
     * 1:1 message missing privacy token (tctoken). Usually means the account is
     * restricted: WhatsApp blocks starting new chats but preserves existing ones,
     * since established chats already carry a tctoken.
     */
    readonly MessageAccountRestriction: "463";
    /** New chat messages rate limited */
    readonly NewChatMessagesCapped: "475";
    /** Stanza validation failure (SMAX_INVALID) — likely stale device session */
    readonly SmaxInvalid: "479";
};
export declare const extractAddressingContext: (stanza: BinaryNode) => {
    addressingMode: string;
    senderAlt: string | undefined;
    recipientAlt: string | undefined;
};
/**
 * Decode the received node as a message.
 * @note this will only parse the message, not decrypt it
 */
export declare function decodeMessageNode(stanza: BinaryNode, meId: string, meLid: string): {
    fullMessage: WAMessage;
    author: string;
    sender: string;
};
export declare const decryptMessageNode: (stanza: BinaryNode, meId: string, meLid: string, repository: SignalRepositoryWithLIDStore, config: SocketConfig) => {
    fullMessage: WAMessage;
    category: string | undefined;
    author: string;
    sessionScope: "status" | undefined;
    decrypt(): Promise<void>;
};
//# sourceMappingURL=decode-wa-message.d.ts.map