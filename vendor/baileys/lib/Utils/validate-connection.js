import { Boom } from '@hapi/boom';
import { createHash } from 'crypto';
import { proto } from '../../WAProto/index.js';
import { KEY_BUNDLE_TYPE, WA_ADV_ACCOUNT_SIG_PREFIX, WA_ADV_DEVICE_SIG_PREFIX, WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX } from '../Defaults/index.js';
import { getBinaryNodeChild, jidDecode, S_WHATSAPP_NET } from '../WABinary/index.js';
import { constantTimeEqual, Curve, hmacSign } from './crypto.js';
import { encodeBigEndian } from './generics.js';
import { createSignalIdentity } from './signal.js';
const getUserAgent = (config) => {
    let platform = proto.ClientPayload.UserAgent.Platform.WEB;
    if (config.browser[0] === 'Android') {
        platform = proto.ClientPayload.UserAgent.Platform.ANDROID;
    }
    else if (config.browser[0] === 'SMB_Android') {
        platform = proto.ClientPayload.UserAgent.Platform.SMB_ANDROID;
    }
    return {
        appVersion: {
            primary: config.version[0],
            secondary: config.version[1],
            tertiary: config.version[2]
        },
        platform,
        releaseChannel: proto.ClientPayload.UserAgent.ReleaseChannel.RELEASE,
        osVersion: '0.1',
        device: 'Desktop',
        osBuildNumber: '0.1',
        localeLanguageIso6391: 'en',
        mnc: '000',
        mcc: '000',
        localeCountryIso31661Alpha2: config.countryCode
    };
};
const PLATFORM_MAP = {
    'Mac OS': proto.ClientPayload.WebInfo.WebSubPlatform.DARWIN,
    Windows: proto.ClientPayload.WebInfo.WebSubPlatform.WIN_HYBRID
};
const getWebInfo = (config) => {
    let webSubPlatform = proto.ClientPayload.WebInfo.WebSubPlatform.WEB_BROWSER;
    if (config.syncFullHistory &&
        PLATFORM_MAP[config.browser[0]] &&
        config.browser[1] === 'Desktop') {
        webSubPlatform = PLATFORM_MAP[config.browser[0]];
    }
    return { webSubPlatform };
};
const getClientPayload = (config) => {
    const payload = {
        connectType: proto.ClientPayload.ConnectType.WIFI_UNKNOWN,
        connectReason: proto.ClientPayload.ConnectReason.USER_ACTIVATED,
        userAgent: getUserAgent(config)
    };
    if (config.browser[0] !== 'Android' && config.browser[0] !== 'SMB_Android') {
        payload.webInfo = getWebInfo(config);
    }
    if (config.pushName) {
        payload.pushName = config.pushName;
    }
    return payload;
};
export const generateLoginNode = (userJid, config, { loginCounter = 0, connectAttemptCount = 0 } = {}) => {
    const { user, device } = jidDecode(userJid);
    const payload = {
        ...getClientPayload(config),
        passive: true,
        pull: true,
        username: +user,
        device: device,
        lidDbMigrated: true,
        // WA Web WAWebClientPayload.getClientPayloadForLogin
        lc: loginCounter,
        connectAttemptCount
    };
    return proto.ClientPayload.fromObject(payload);
};
export const getPlatformType = (platform) => {
    const platformType = platform.toUpperCase();
    if (platformType === 'ANDROID') {
        return proto.DeviceProps.PlatformType.ANDROID_PHONE;
    }
    return (proto.DeviceProps.PlatformType[platformType] ||
        proto.DeviceProps.PlatformType.CHROME);
};
export const generateRegistrationNode = ({ registrationId, signedPreKey, signedIdentityKey }, config) => {
    // the app version needs to be md5 hashed
    // and passed in
    const appVersionBuf = createHash('md5')
        .update(config.version.join('.')) // join as string
        .digest();
    const companion = {
        os: config.browser[0],
        platformType: getPlatformType(config.browser[1]),
        requireFullSync: config.syncFullHistory,
        historySyncConfig: {
            fullSyncDaysLimit: undefined,
            fullSyncSizeMbLimit: undefined,
            storageQuotaMb: 10240,
            inlineInitialPayloadInE2EeMsg: true,
            recentSyncDaysLimit: undefined,
            // WA Web WAWebClientPayload sets this unconditionally
            supportCallLogHistory: true,
            supportBotUserAgentChatHistory: true,
            supportCagReactionsAndPolls: true,
            supportBizHostedMsg: true,
            supportRecentSyncChunkMessageCountTuning: true,
            supportHostedGroupMsg: true,
            supportFbidBotChatHistory: true,
            supportAddOnHistorySyncMigration: undefined,
            supportMessageAssociation: true,
            supportGroupHistory: true,
            onDemandReady: undefined,
            supportGuestChat: undefined,
            completeOnDemandReady: undefined,
            thumbnailSyncDaysLimit: 60
        },
        version: {
            primary: 10,
            secondary: 15,
            tertiary: 7
        }
    };
    const companionProto = proto.DeviceProps.encode(companion).finish();
    const registerPayload = {
        ...getClientPayload(config),
        passive: false,
        pull: false,
        devicePairingData: {
            buildHash: appVersionBuf,
            deviceProps: companionProto,
            eRegid: encodeBigEndian(registrationId),
            eKeytype: KEY_BUNDLE_TYPE,
            eIdent: signedIdentityKey.public,
            eSkeyId: encodeBigEndian(signedPreKey.keyId, 3),
            eSkeyVal: signedPreKey.keyPair.public,
            eSkeySig: signedPreKey.signature
        }
    };
    return proto.ClientPayload.fromObject(registerPayload);
};
export const configureSuccessfulPairing = (stanza, { advSecretKey, signedIdentityKey, signalIdentities }) => {
    const msgId = stanza.attrs.id;
    const pairSuccessNode = getBinaryNodeChild(stanza, 'pair-success');
    const deviceIdentityNode = getBinaryNodeChild(pairSuccessNode, 'device-identity');
    const platformNode = getBinaryNodeChild(pairSuccessNode, 'platform');
    const deviceNode = getBinaryNodeChild(pairSuccessNode, 'device');
    const businessNode = getBinaryNodeChild(pairSuccessNode, 'biz');
    const clientPropsNode = getBinaryNodeChild(pairSuccessNode, 'client-props');
    if (!deviceIdentityNode || !deviceNode) {
        throw new Boom('Missing device-identity or device in pair success node', { data: stanza });
    }
    // WA Web WASmaxInMdSetRegRequest: contentBytesRange(device-identity, 1, 500)
    const deviceIdentityBytes = deviceIdentityNode.content;
    if (!(deviceIdentityBytes instanceof Uint8Array) ||
        deviceIdentityBytes.length < 1 ||
        deviceIdentityBytes.length > 500) {
        throw new Boom('invalid device-identity length', { data: stanza });
    }
    const bizName = businessNode?.attrs.name;
    const jid = deviceNode.attrs.jid;
    const lid = deviceNode.attrs.lid;
    const { details, hmac, accountType } = proto.ADVSignedDeviceIdentityHMAC.decode(deviceIdentityBytes);
    let hmacPrefix = Buffer.from([]);
    if (accountType !== undefined && accountType === proto.ADVEncryptionType.HOSTED) {
        hmacPrefix = WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX;
    }
    const advSign = hmacSign(Buffer.concat([hmacPrefix, details]), Buffer.from(advSecretKey, 'base64'));
    if (!constantTimeEqual(hmac, advSign)) {
        throw new Boom('Invalid account signature');
    }
    const account = proto.ADVSignedDeviceIdentity.decode(details);
    const { accountSignatureKey, accountSignature, details: deviceDetails } = account;
    const deviceIdentity = proto.ADVDeviceIdentity.decode(deviceDetails);
    const accountSignaturePrefix = deviceIdentity.deviceType === proto.ADVEncryptionType.HOSTED
        ? WA_ADV_HOSTED_ACCOUNT_SIG_PREFIX
        : WA_ADV_ACCOUNT_SIG_PREFIX;
    const accountMsg = Buffer.concat([accountSignaturePrefix, deviceDetails, signedIdentityKey.public]);
    if (!Curve.verify(accountSignatureKey, accountMsg, accountSignature)) {
        throw new Boom('Failed to verify account signature');
    }
    const deviceMsg = Buffer.concat([
        WA_ADV_DEVICE_SIG_PREFIX,
        deviceDetails,
        signedIdentityKey.public,
        accountSignatureKey
    ]);
    account.deviceSignature = Curve.sign(signedIdentityKey.private, deviceMsg);
    const identity = createSignalIdentity(lid, accountSignatureKey);
    const accountEnc = encodeSignedDeviceIdentity(account, false);
    // whatsmeow 37a86e67: parsed and surfaced; an unparseable blob must not fail pairing
    let clientPairingProps;
    if (clientPropsNode?.content instanceof Uint8Array) {
        try {
            clientPairingProps = proto.ClientPairingProps.decode(clientPropsNode.content);
        }
        catch {
            clientPairingProps = undefined;
        }
    }
    const reply = {
        tag: 'iq',
        attrs: {
            to: S_WHATSAPP_NET,
            type: 'result',
            id: msgId
        },
        content: [
            {
                tag: 'pair-device-sign',
                attrs: {},
                content: [
                    {
                        tag: 'device-identity',
                        attrs: { 'key-index': deviceIdentity.keyIndex.toString() },
                        content: accountEnc
                    }
                ]
            }
        ]
    };
    const authUpdate = {
        account,
        me: { id: jid, name: bizName, lid },
        signalIdentities: [...(signalIdentities || []), identity],
        platform: platformNode?.attrs.name,
        // WA Web resetLoginCounter on companion registration
        loginCounter: 0
    };
    return {
        creds: authUpdate,
        reply,
        clientPairingProps
    };
};
export const encodeSignedDeviceIdentity = (account, includeSignatureKey) => {
    account = { ...account };
    // set to null if we are not to include the signature key
    // or if we are including the signature key but it is empty
    if (!includeSignatureKey || !account.accountSignatureKey?.length) {
        account.accountSignatureKey = null;
    }
    return proto.ADVSignedDeviceIdentity.encode(account).finish();
};
//# sourceMappingURL=validate-connection.js.map