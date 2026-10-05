const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const pino = require('pino');
const config = require('./settings');
const axios = require('axios');
const mongoose = require('mongoose');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    getContentType,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    jidNormalizedUser,
    downloadContentFromMessage,
    proto,
    prepareWAMessageMedia,
    generateWAMessageFromContent,
    generateForwardMessageContent,
    S_WHATSAPP_NET
} = require('@whiskeysockets/baileys');

const { getBuffer, getGroupAdmins, getRandom, h2k, isUrl, Json, runtime, sleep, fetchJson } = require('./lib/functions');
const { sms } = require('./lib/msg');
const NodeCache = require('node-cache');
const util = require('util');

const app = express();
const PORT = process.env.PORT || 3000;
const SESSION_BASE_PATH = './sessions';
const msgRetryCounterCache = new NodeCache();

require('events').EventEmitter.defaultMaxListeners = 500;

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://mongo:mMIurYvWLTegtpZaYNznIJpisuNKQpex@kodama.proxy.rlwy.net:23611';

mongoose.connect(MONGODB_URI)
    .then(() => console.log('SHALA MINI | Mongodb Connected ✅ '))
    .catch(err => console.log('SHALA MINI | Mongodb Error ❎', err));

const SessionSchema = new mongoose.Schema({
    sessionId: String,
    data: Object
});
const Session = mongoose.model('Session', SessionSchema);

fs.readdirSync("./plugins/").forEach((plugin) => {
    if (path.extname(plugin).toLowerCase() == ".js") {
        require("./plugins/" + plugin);
    }
});

console.log('SHALA MINI | Plugin Installed ✅');

const events = require('./lib/command');

const commandMap = new Map();
for (const cmd of events.commands) {
    if (cmd.pattern) commandMap.set(cmd.pattern, cmd);
    if (cmd.alias) {
        for (const alias of cmd.alias) {
            if (!commandMap.has(alias)) commandMap.set(alias, cmd);
        }
    }
}

app.use(express.static(path.join(__dirname, 'public')));

const activeSockets = {};
const keepAliveTimers = {};
const reconnectTimers = {};

const fileCache = {};

const saveDebounceTimers = {};

function cleanupSession(sessionId) {
    if (keepAliveTimers[sessionId]) {
        clearInterval(keepAliveTimers[sessionId]);
        delete keepAliveTimers[sessionId];
    }

    if (reconnectTimers[sessionId]) {
        clearTimeout(reconnectTimers[sessionId]);
        delete reconnectTimers[sessionId];
    }

    if (saveDebounceTimers[sessionId]) {
        clearTimeout(saveDebounceTimers[sessionId]);
        delete saveDebounceTimers[sessionId];
    }

    const sock = activeSockets[sessionId];

    if (sock) {
        try {
            sock.ev.removeAllListeners();
            sock.ws?.terminate?.();
        } catch (e) {}

        delete activeSockets[sessionId];
    }
}

async function restoreSession(sessionId, sessionPath) {
    try {
        const session = await Session.findOne({ sessionId });

        if (!session) return false;

        await fs.ensureDir(sessionPath);

        for (const file in session.data) {
            await fs.writeFile(
                path.join(sessionPath, file),
                session.data[file]
            );
        }

        console.log('SHALA MINI | Restored ✅', sessionId);

        return true;

    } catch (err) {
        console.error('SHALA MINI | Restore Error ❎', err);
        return false;
    }
}

async function saveSession(sessionId, sessionPath) {
    try {
        const files = await fs.readdir(sessionPath);

        let data = {};
        let hasChanges = false;

        for (const file of files) {
            try {
                const content = await fs.readFile(
                    path.join(sessionPath, file),
                    'utf-8'
                );

                const cacheKey = `${sessionId}:${file}`;

                if (fileCache[cacheKey] !== content) {
                    fileCache[cacheKey] = content;
                    hasChanges = true;
                }

                data[file] = content;

            } catch (e) {}
        }

        if (!hasChanges) {
            console.log(
                'SHALA MINI | No changes, skipping DB write:',
                sessionId
            );
            return;
        }

        await Session.findOneAndUpdate(
            { sessionId },
            { data },
            { upsert: true }
        );

        console.log('SHALA MINI | SavedSession ✅', sessionId);

    } catch (err) {
        console.error('SHALA MINI | SaveSession error ❎', err);
    }
}

function debouncedSaveSession(sessionId, sessionPath) {

    if (saveDebounceTimers[sessionId]) {
        clearTimeout(saveDebounceTimers[sessionId]);
    }

    saveDebounceTimers[sessionId] = setTimeout(async () => {

        delete saveDebounceTimers[sessionId];

        await saveSession(
            sessionId,
            sessionPath
        );

    }, 5000);
}

async function Pair(number, res = null) {

    const xnumber = number.replace(/[^0-9]/g, '');

    const sessionId = `shala_mini_${xnumber}`;

    const sessionPath = path.join(
        SESSION_BASE_PATH,
        sessionId
    );

    if (activeSockets[sessionId]) {

        console.log(
            'SHALA MINI | Socket already active for:',
            sessionId
        );

        if (res && !res.headersSent) {
            res.json({
                error: 'Session already active. Please wait.'
            });
        }

        return;
    }

    try {

        await restoreSession(
            sessionId,
            sessionPath
        );

        await fs.ensureDir(sessionPath);

        const { state, saveCreds } =
            await useMultiFileAuthState(sessionPath);

        const { version } =
            await fetchLatestBaileysVersion();

        const logger =
            pino({ level: 'silent' });

        const sock = makeWASocket({
            version,
            logger,

            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(
                    state.keys,
                    logger
                ),
            },

            printQRInTerminal: false,

            browser: [
                'Shala Mini',
                'Chrome',
                '1.0.0'
            ],

            generateHighQualityLinkPreview: true,
            syncFullHistory: false,

            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 30000,
            keepAliveIntervalMs: 30000,

            msgRetryCounterCache
        });

        activeSockets[sessionId] = sock;

        sock.sendFileUrl = async (
            jid,
            url,
            caption,
            quoted,
            options = {}
        ) => {

            const r = await axios.head(url);

            const mime =
                r.headers['content-type'];

            if (mime.split("/")[1] === "gif")

                return sock.sendMessage(
                    jid,
                    {
                        video: await getBuffer(url),
                        caption,
                        gifPlayback: true,
                        ...options
                    },
                    { quoted }
                );

            if (mime === "application/pdf")

                return sock.sendMessage(
                    jid,
                    {
                        document: await getBuffer(url),
                        mimetype: 'application/pdf',
                        caption,
                        ...options
                    },
                    { quoted }
                );

            if (mime.split("/")[0] === "image")

                return sock.sendMessage(
                    jid,
                    {
                        image: await getBuffer(url),
                        caption,
                        ...options
                    },
                    { quoted }
                );

            if (mime.split("/")[0] === "video")

                return sock.sendMessage(
                    jid,
                    {
                        video: await getBuffer(url),
                        caption,
                        mimetype: 'video/mp4',
                        ...options
                    },
                    { quoted }
                );

            if (mime.split("/")[0] === "audio")

                return sock.sendMessage(
                    jid,
                    {
                        audio: await getBuffer(url),
                        caption,
                        mimetype: 'audio/mpeg',
                        ...options
                    },
                    { quoted }
                );
        };

        sock.edite = async (
            gg,
            newmg,
            from
        ) => {

            await sock.relayMessage(
                from,
                {
                    protocolMessage: {
                        key: gg.key,
                        type: 14,
                        editedMessage: {
                            conversation: newmg
                        }
                    }
                },
                {}
            );
        };

        sock.forwardMessage = async (
            jid,
            message,
            forceForward = false,
            options = {}
        ) => {

            let mtype =
                Object.keys(message.message)[0];

            let content =
                await generateForwardMessageContent(
                    message,
                    forceForward
                );

            let ctype =
                Object.keys(content)[0];

            let context =
                mtype !== "conversation"
                    ? message.message[mtype].contextInfo
                    : {};

            content[ctype].contextInfo = {
                ...context,
                ...content[ctype].contextInfo
            };

            const waMessage =
                await generateWAMessageFromContent(
                    jid,
                    content,
                    options
                        ? {
                            ...content[ctype],
                            ...options,

                            ...(options.contextInfo
                                ? {
                                    contextInfo: {
                                        ...content[ctype].contextInfo,
                                        ...options.contextInfo
                                    }
                                }
                                : {})
                        }
                        : {}
                );

            await sock.relayMessage(
                jid,
                waMessage.message,
                {
                    messageId: waMessage.key.id
                }
            );

            return waMessage;
        };

        let pairingCode = null;
        let responded = false;

        if (!sock.authState.creds.registered) {

            try {

                await new Promise(
                    r => setTimeout(r, 3000)
                );

                pairingCode =
                    await sock.requestPairingCode(
                        xnumber
                    );

                console.log(
                    'SHALA MINI | Pairing Code:',
                    pairingCode
                );

                if (res && !res.headersSent) {

                    res.json({
                        code: pairingCode
                    });

                    responded = true;
                }

            } catch (pairErr) {

                console.error(
                    'SHALA MINI | Pairing code request failed:',
                    pairErr
                );

                if (res && !res.headersSent) {

                    res.json({
                        error:
                            'Failed to generate pairing code. Try again.'
                    });

                    responded = true;
                }

                cleanupSession(sessionId);

                return;
            }

        } else {

            console.log(
                'SHALA MINI | Already registered:',
                sessionId
            );

            if (res && !res.headersSent) {

                res.json({
                    error:
                        'This number is already paired.'
                });

                responded = true;
            }
        }

        if (res && !responded) {

            setTimeout(() => {

                if (!res.headersSent)

                    res.json({
                        error:
                            'Pairing timed out. Try again.'
                    });

            }, 15000);
        }

        sock.ev.on(
            'creds.update',
            async () => {

                await saveCreds();

                debouncedSaveSession(
                    sessionId,
                    sessionPath
                );
            }
        );

        sock.ev.on(
            'connection.update',
            async (update) => {

                const {
                    connection,
                    lastDisconnect
                } = update;

                if (connection === 'close') {

                    const statusCode =
                        lastDisconnect?.error?.output?.statusCode;

                    const isLoggedOut =
                        statusCode ===
                        DisconnectReason.loggedOut;

                    console.log(
                        `SHALA MINI | Disconnected: ${sessionId} | Code: ${statusCode}`
                    );

                    cleanupSession(sessionId);

                    if (!isLoggedOut) {

                        console.log(
                            'SHALA MINI | Reconnecting:',
                            sessionId
                        );

                        reconnectTimers[sessionId] =
                            setTimeout(
                                () => Pair(number),
                                5000
                            );

                    } else {

                        console.log(
                            'SHALA MINI | Logged out:',
                            sessionId
                        );

                        await Session.findOneAndDelete({
                            sessionId
                        });

                        await fs.remove(
                            sessionPath
                        );
                    }

                } else if (connection === 'open') {

                    console.log(
                        'SHALA MINI | Connected ✅',
                        sessionId
                    );

                    keepAliveTimers[sessionId] =
                        setInterval(
                            async () => {

                                if (!activeSockets[sessionId]) {

                                    clearInterval(
                                        keepAliveTimers[sessionId]
                                    );

                                    delete keepAliveTimers[
                                        sessionId
                                    ];

                                    return;
                                }

                                sock.sendPresenceUpdate(
                                    'available',
                                    sock.user.id
                                ).catch(() => {

                                    console.log(
                                        'SHALA MINI | Keep-alive failed:',
                                        sessionId
                                    );

                                    cleanupSession(
                                        sessionId
                                    );

                                    reconnectTimers[
                                        sessionId
                                    ] = setTimeout(
                                        () => Pair(number),
                                        3000
                                    );
                                });

                            },
                            30000
                        );

                    try {

                        const jid =
                            xnumber +
                            '@s.whatsapp.net';

                        await sock.sendMessage(
                            jid,
                            {
                                text:
                                    `*SHALA MINI ACTIVE! 🟢*\n\n` +
                                    `Your *Shala Mini* bot is now connected successfully.\n` +
                                    `Pairing code used: *${pairingCode ?? 'Already registered'}*`
                            }
                        );

                    } catch (e) {

                        console.error(
                            'SHALA MINI | Welcome message failed:',
                            e
                        );
                    }
                }
            }
        );

        sock.ev.on(
            'messages.upsert',
            async (mek) => {

                try {

                    mek = mek.messages[0];

                    if (!mek.message) return;

                    mek.message =
                        (getContentType(mek.message) ===
                            'ephemeralMessage')

                            ? mek.message
                                .ephemeralMessage
                                .message

                            : mek.message;

                    if (
                        mek.key &&
                        mek.key.remoteJid ===
                        'status@broadcast'
                    ) {

                        if (config.AUTO_READ_STATUS) {

                            await sock.readMessages([
                                mek.key
                            ]);
                        }

                        if (config.AUTO_REACT) {

                            await sock.sendMessage(
                                mek.key.remoteJid,
                                {
                                    react: {
                                        text: '❤️',
                                        key: mek.key
                                    }
                                }
                            );
                        }

                        return;
                    }

                    const m = sms(sock, mek);

                    const type =
                        getContentType(mek.message);

                    const from =
                        mek.key.remoteJid;

                    const body =
                        type === 'conversation'
                            ? mek.message.conversation

                            : type === 'extendedTextMessage'
                                ? mek.message
                                    .extendedTextMessage.text

                                : type === 'imageMessage' &&
                                    mek.message.imageMessage?.caption
                                    ? mek.message.imageMessage.caption

                                    : type === 'videoMessage' &&
                                        mek.message.videoMessage?.caption
                                        ? mek.message.videoMessage.caption

                                        : type === 'interactiveResponseMessage'
                                            ? (() => {

                                                try {

                                                    return JSON.parse(
                                                        mek.message
                                                            .interactiveResponseMessage
                                                            ?.nativeFlowResponseMessage
                                                            ?.paramsJson
                                                    )?.id || '';

                                                } catch {

                                                    return '';
                                                }

                                            })()

                                            : type ===
                                                'templateButtonReplyMessage'
                                                ? mek.message
                                                    .templateButtonReplyMessage
                                                    ?.selectedId

                                                : m.msg?.text ||
                                                    m.msg?.conversation ||
                                                    m.msg?.caption ||
                                                    '';

                    const prefix =
                        config.PREFIX;

                    const isCmd =
                        body.startsWith(prefix);

                    const command =
                        isCmd
                            ? body
                                .slice(prefix.length)
                                .trim()
                                .split(' ')
                                .shift()
                                .toLowerCase()
                            : '';

                    const args =
                        body
                            .trim()
                            .split(/ +/)
                            .slice(1);

                    const q =
                        args.join(' ');

                    const isGroup =
                        from.endsWith('@g.us');

                    const sender =
                        mek.key.fromMe

                            ? (
                                sock.user.id
                                    .split(':')[0] +
                                '@s.whatsapp.net'
                            )

                            : (
                                mek.key.participant ||
                                mek.key.remoteJid
                            );

                    const senderNumber =
                        sender.split('@')[0];

                    const botNumber =
                        sock.user.id.split(':')[0];

                    const botNumber2 =
                        await jidNormalizedUser(
                            sock.user.id
                        );

                    const pushname =
                        mek.pushName ||
                        'User';

                    const isMe =
                        botNumber.includes(
                            senderNumber
                        );

                    const isOwner =
                        isMe ||
                        (xnumber === senderNumber);

                    const isReact =
                        m.message?.reactionMessage
                            ? true
                            : false;

                    const quoted =
                        type ===
                            'extendedTextMessage' &&
                            mek.message
                                .extendedTextMessage
                                .contextInfo != null

                            ? mek.message
                                .extendedTextMessage
                                .contextInfo
                                .quotedMessage || []

                            : [];

                    const groupMetadata =
                        isGroup
                            ? await sock
                                .groupMetadata(from)
                                .catch(() => null)
                            : null;

                    const groupName =
                        isGroup &&
                            groupMetadata
                            ? groupMetadata.subject
                            : '';

                    const participants =
                        isGroup &&
                            groupMetadata
                            ? groupMetadata.participants
                            : [];

                    const groupAdmins =
                        isGroup
                            ? getGroupAdmins(
                                participants
                            )
                            : [];

                    const isBotAdmins =
                        isGroup
                            ? groupAdmins.includes(
                                botNumber2
                            )
                            : false;

                    const isAdmins =
                        isGroup
                            ? groupAdmins.includes(
                                sender
                            )
                            : false;

                    const isSudo = false;
                    const isPre = false;

                    const reply =
                        async (teks) =>
                            await sock.sendMessage(
                                from,
                                {
                                    text: teks
                                },
                                {
                                    quoted: mek
                                }
                            );

                    if (isCmd)
                        await sock.readMessages([
                            mek.key
                        ]);

                    if (
                        config.AUTO_REACT &&
                        !isMe &&
                        !isReact &&
                        Math.random() < 0.3
                    ) {

                        const emojis =
                            config.REACT_EMOJIS;

                        sock.sendMessage(
                            from,
                            {
                                react: {
                                    text:
                                        emojis[
                                            Math.floor(
                                                Math.random() *
                                                emojis.length
                                            )
                                        ],
                                    key: mek.key
                                }
                            }
                        ).catch(() => {});
                    }

                    if (config.AUTO_TYPING) {

                        sock.sendPresenceUpdate(
                            'composing',
                            from
                        ).catch(() => {});

                        setTimeout(
                            () =>
                                sock.sendPresenceUpdate(
                                    'paused',
                                    from
                                ).catch(() => {}),
                            3000
                        );
                    }

                    const cmdName =
                        isCmd
                            ? body
                                .slice(prefix.length)
                                .trim()
                                .split(' ')[0]
                                .toLowerCase()
                            : false;

                    if (isCmd) {

                        const cmd =
                            commandMap.get(
                                cmdName
                            );

                        if (cmd) {

                            if (cmd.react)

                                sock.sendMessage(
                                    from,
                                    {
                                        react: {
                                            text: cmd.react,
                                            key: mek.key
                                        }
                                    }
                                );

                            try {

                                cmd.function(
                                    sock,
                                    mek,
                                    m,
                                    {
                                        from,
                                        prefix,
                                        isSudo,
                                        quoted,
                                        body,
                                        isCmd,
                                        isPre,
                                        command,
                                        args,
                                        q,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply
                                    }
                                );

                            } catch (e) {

                                console.error(
                                    '[SHALA MINI PLUGIN ERROR]',
                                    e
                                );
                            }
                        }
                    }

                    for (
                        const cmd of events.commands
                    ) {

                        try {

                            if (
                                body &&
                                cmd.on === 'body'
                            ) {

                                cmd.function(
                                    sock,
                                    mek,
                                    m,
                                    {
                                        from,
                                        prefix,
                                        quoted,
                                        body,
                                        isSudo,
                                        isCmd,
                                        command,
                                        args,
                                        q,
                                        isPre,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply
                                    }
                                );

                            } else if (
                                mek.q &&
                                cmd.on === 'text'
                            ) {

                                cmd.function(
                                    sock,
                                    mek,
                                    m,
                                    {
                                        from,
                                        quoted,
                                        body,
                                        isSudo,
                                        isCmd,
                                        isPre,
                                        command,
                                        args,
                                        q,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply
                                    }
                                );

                            } else if (
                                (
                                    cmd.on === 'image' ||
                                    cmd.on === 'photo'
                                ) &&
                                mek.type ===
                                'imageMessage'
                            ) {

                                cmd.function(
                                    sock,
                                    mek,
                                    m,
                                    {
                                        from,
                                        prefix,
                                        quoted,
                                        isSudo,
                                        body,
                                        isCmd,
                                        command,
                                        isPre,
                                        args,
                                        q,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply
                                    }
                                );

                            } else if (
                                cmd.on === 'sticker' &&
                                mek.type ===
                                'stickerMessage'
                            ) {

                                cmd.function(
                                    sock,
                                    mek,
                                    m,
                                    {
                                        from,
                                        prefix,
                                        quoted,
                                        isSudo,
                                        body,
                                        isCmd,
                                        command,
                                        args,
                                        isPre,
                                        q,
                                        isGroup,
                                        sender,
                                        senderNumber,
                                        botNumber2,
                                        botNumber,
                                        pushname,
                                        isMe,
                                        isOwner,
                                        groupMetadata,
                                        groupName,
                                        participants,
                                        groupAdmins,
                                        isBotAdmins,
                                        isAdmins,
                                        reply
                                    }
                                );
                            }

                        } catch (e) {

                            console.error(
                                '[SHALA MINI CMD MAP ERROR]',
                                e
                            );
                        }
                    }

                    switch (command) {

                        case 'jid':

                            reply(from);

                            break;

                        case 'ev': {

                            if (isOwner) {

                                try {

                                    let result =
                                        await eval(q);

                                    reply(
                                        util.format(result)
                                    );

                                } catch (err) {

                                    reply(
                                        util.format(err)
                                    );
                                }
                            }

                            break;
                        }

                        default:
                            break;
                    }

                } catch (e) {

                    console.error(
                        '[SHALA MINI MESSAGE ERROR]',
                        String(e)
                    );
                }
            }
        );

    } catch (err) {

        console.error(
            'SHALA MINI | Pair Error:',
            err
        );

        cleanupSession(sessionId);

        if (res && !res.headersSent)

            res.json({
                error:
                    'Pair failed: ' +
                    err.message
            });
    }
}

async function restoreAllSessions() {

    try {

        const sessions =
            await Session.find();

        console.log(
            `SHALA MINI | Restoring ${sessions.length} session(s)...`
        );

        await Promise.all(

            sessions

                .filter(s => {

                    if (!s.sessionId) {

                        console.warn(
                            'Skipping session without sessionId:',
                            s
                        );

                        return false;
                    }

                    return true;
                })

                .map(
                    async (s, index) => {

                        const number =
                            s.sessionId.replace(
                                'shala_mini_',
                                ''
                            );

                        try {

                            await new Promise(
                                r =>
                                    setTimeout(
                                        r,
                                        index * 500
                                    )
                            );

                            await Pair(number);

                        } catch (err) {

                            console.error(
                                'SHALA MINI | Failed to restore session',
                                s.sessionId,
                                err
                            );
                        }
                    }
                )
        );

    } catch (err) {

        console.error(
            'SHALA MINI | restoreAllSessions error:',
            err
        );
    }
}

app.get(
    '/pair',
    async (req, res) => {

        const number =
            req.query.number;

        if (!number)

            return res.json({
                error:
                    'Number required'
            });

        res.setTimeout(
            30000,
            () => {

                if (!res.headersSent)

                    res.json({
                        error:
                            'Request timed out. Try again.'
                    });
            }
        );

        await Pair(
            number,
            res
        );
    }
);

app.get(
    '/',
    (req, res) =>
        res.send(
            'SHALA MINI BOT SERVER RUNNING! 🚀'
        )
);

app.listen(
    PORT,
    async () => {

        console.log(
            `🚀 SHALA MINI | Server running on port ${PORT}`
        );

        await fs.ensureDir(
            SESSION_BASE_PATH
        );

        await restoreAllSessions();
    }
);

process.on(
    'uncaughtException',
    (err) => {

        const e = String(err);

        if (
            e.includes(
                'Socket connection timeout'
            )
        ) return;

        if (
            e.includes(
                'rate-overlimit'
            )
        ) return;

        if (
            e.includes(
                'Connection Closed'
            )
        ) return;

        if (
            e.includes(
                'Value not found'
            )
        ) return;

        console.log(
            'SHALA MINI | Caught exception:',
            err
        );
    }
);
