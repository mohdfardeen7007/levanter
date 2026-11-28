import { proto } from '../../WAProto/index.js';
import type { AuthenticationCreds } from '../Types/index.js';
import { type BinaryNode } from '../WABinary/index.js';
import type { ILogger } from './logger.js';
/** Signs the server's WebAuthn request options; the credential source stays outside the library. */
export type ShortcakeAssertionSigner = (requestOptions: Uint8Array) => Promise<{
    readonly credentialId: Uint8Array;
    readonly webauthnAssertion: Uint8Array;
}>;
export interface ShortcakeFlowOptions {
    readonly logger: ILogger;
    /** Send an IQ and await its (error-free) result. */
    readonly query: (node: BinaryNode, timeoutMs?: number) => Promise<BinaryNode>;
    readonly signAssertion: ShortcakeAssertionSigner;
    readonly getCreds: () => AuthenticationCreds;
    /** Persist a credentials patch (rotates the ADV secret on a prologue). */
    readonly updateCreds: (patch: Partial<AuthenticationCreds>) => void;
    /** Companion platform reported in the ephemeral identity. */
    readonly deviceType?: proto.DeviceProps.PlatformType;
    readonly emitVerificationCode?: (code: string) => void;
    readonly emitPrologueSent?: () => void;
}
/** Drives the companion side of the "Shortcake" passkey-linking handshake (md IQ exchange + commit/reveal ECDH). */
export declare const makeShortcakeFlow: (opts: ShortcakeFlowOptions) => {
    handleIncomingNotification: (node: BinaryNode) => Promise<boolean>;
    executePrologue: (args?: {
        readonly requestOptions?: Uint8Array;
        readonly pairingHandoffProof?: Uint8Array;
    }) => Promise<void>;
    confirmVerificationCode: () => Promise<void>;
    hasSession: () => boolean;
    getVerificationCode: () => string | null;
    clearSession: () => void;
};
export type ShortcakeFlow = ReturnType<typeof makeShortcakeFlow>;
//# sourceMappingURL=shortcake.d.ts.map