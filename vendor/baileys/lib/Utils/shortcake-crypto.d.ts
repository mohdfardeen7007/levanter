import { proto } from '../../WAProto/index.js';
import type { KeyPair } from '../Types/index.js';
/** The primary device's ephemeral X25519 public key + 32B nonce (used for the verification code). */
export interface ShortcakePrimaryEphemeralIdentity {
    readonly publicKey: Uint8Array;
    readonly nonce: Uint8Array;
}
/** The companion's ephemeral X25519 keypair + committed nonce; keep in memory for the handshake only. */
export interface ShortcakeCompanionEphemeralIdentity {
    readonly keyPair: KeyPair;
    /** 32 random bytes committed up-front, revealed only after the primary replies. */
    readonly companionNonce: Uint8Array;
    readonly companionEphemeralIdentityBytes: Uint8Array;
    /** `SHA-256(companionEphemeralIdentity ‖ companionNonce)`. */
    readonly commitmentHash: Uint8Array;
    readonly prologuePayloadBytes: Uint8Array;
}
/** Generate the companion's ephemeral identity + nonce commitment for the prologue. */
export declare function generateCompanionEphemeralIdentity(args: {
    readonly ref: string;
    readonly deviceType: proto.DeviceProps.PlatformType;
}): ShortcakeCompanionEphemeralIdentity;
/** Parses + validates a `PrimaryEphemeralIdentity` proto from the primary. */
export declare function decodePrimaryEphemeralIdentity(bytes: Uint8Array): ShortcakePrimaryEphemeralIdentity;
/** Verification code: `Crockford32( primaryNonce[0..5] XOR SHA-256(companionNonce ‖ primaryPubKey)[0..5] )`. */
export declare function deriveVerificationCode(companionNonce: Uint8Array, primary: ShortcakePrimaryEphemeralIdentity): string;
/** AES-GCM key for the pairing request: `HKDF( X25519(companionPriv, primaryPub), salt, ENCRYPTION_KEY_INFO )`. */
export declare function deriveEncryptionKey(args: {
    readonly companionPrivKey: Uint8Array;
    readonly primaryPublicKey: Uint8Array;
    readonly deviceType: proto.DeviceProps.PlatformType;
    readonly ref: string;
}): Uint8Array;
/** Seal the pairing request under the derived key, returning the encoded `EncryptedPairingRequest`. */
export declare function encryptPairingRequest(encryptionKey: Uint8Array, plaintext: Uint8Array): Uint8Array;
//# sourceMappingURL=shortcake-crypto.d.ts.map