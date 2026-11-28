import { proto } from '../../WAProto/index.js';
import type { WAMessageContent, WAMessageKey } from '../Types/index.js';
import { type BinaryNode } from '../WABinary/index.js';
export type ReportingField = {
    f: number;
    m?: boolean;
    s?: ReportingField[];
};
/**
 * Whether a message we send gets a reporting token, by WA Web WAWebReportingTokenUtils.genReportingToken's rules
 * (a messageSecret and a non-empty content aside), the type read from the content as the receiver's parser reads it:
 * - WAWebMessagePluginGenerateReportingTokenContent.isMsgTypeReportingTokenCompatible: none for a reaction, an
 *   encrypted reaction, an event response, a poll vote, a pin or a keep-in-chat, nor for a protocol message that
 *   revokes or unschedules one (subtypes sender_revoke, admin_revoke, scheduled_message_unschedule)
 * - none for a comment, nor for an encrypted edit of a message, event or poll or an encrypted added poll option: WA
 *   Web gives those one only from the content it works out before encrypting them (WAWebAddonEncryption), which is
 *   not done here
 * - none for an edit either: WA Web keys an edit's token with the edited message's secret (WAWebSendMessageEditAction
 *   createEditMsgData) and receivers check it with that one (validateReportingTokenInfo, getIsEdited); an edit made
 *   here carries a secret of its own, so its token would fail
 */
export declare const shouldIncludeReportingToken: (message: proto.IMessage) => boolean;
/**
 * The <reporting> node of a message we send, as WA Web WAWebReportingTokenUtils.genReportingTokenBody builds it:
 * HMAC-SHA256 of the message's reporting content (WAWebReportingTokenContent.calculateReportingTokenContent, version
 * 2), keyed with HKDF(messageSecret, stanza id + sender + chat + "Report Token"), cut to 16 bytes. Whether the message
 * should get one is shouldIncludeReportingToken's call.
 * @param msgProtobuf the message's protobuf as encrypted, without its random padding: WA Web encodes the message
 * (MessageSpec) for it, and receivers check it against the unpadded plaintext
 * @param key `id` the stanza id; `remoteJid` the chat it goes to (WA Web T(e)); `participant` its sender (v(e)): for
 * a message we send, our own user as the chat addresses us
 */
export declare const getMessageReportingToken: (msgProtobuf: Uint8Array, message: WAMessageContent, key: WAMessageKey) => Promise<BinaryNode | null>;
//# sourceMappingURL=reporting-utils.d.ts.map