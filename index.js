const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const pino = require('pino');
const config = require('./settings');
const axios = require('axios');
const mongoose = require('mongoose');
const util = require('util');
const NodeCache = require('node-cache');

const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    getContentType,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    jidNormalizedUser,
    generateWAMessageFromContent,
    generateForwardMessageContent
} = require('@whiskeysockets/baileys');

const {
    getBuffer,
    getGroupAdmins,
    getRandom
} = require('./lib/functions');

const { sms } = require('./lib/msg');

const {
    updateCMDStore,
    getCMDStore
} = require('./lib/database');


/*
 * ============================================================
 * SHALA MINI
 * ============================================================
 */

const BOT_NAME = 'SHALA MINI';
const SESSION_PREFIX = 'shala_mini_';
const SESSION_BASE_PATH = path.join(
    __dirname,
    'sessions'
);

const PORT =
    process.env.PORT || 3000;

const msgRetryCounterCache =
    new NodeCache();

require('events')
    .EventEmitter
    .defaultMaxListeners = 500;


/*
 * ============================================================
 * MONGODB
 * ============================================================
 */

const MONGODB_URI =
    process.env.MONGODB_URI ||
    'mongodb://mongo:mMIurYvWLTegtpZaYNznIJpisuNKQpex@kodama.proxy.rlwy.net:23611';

mongoose.connect(MONGODB_URI)
    .then(() => {

        console.log(
            '╔══════════════════════════════════════╗'
        );

        console.log(
            `║       ${BOT_NAME} - DATABASE          ║`
        );

        console.log(
            '╚══════════════════════════════════════╝'
        );

        console.log(
            'MongoDB Connected ✅'
        );

    })
    .catch((err) => {

        console.error(
            '❌ MongoDB Error:',
            err.message
        );

    });


/*
 * ============================================================
 * SESSION SCHEMA
 * ============================================================
 */

const SessionSchema =
    new mongoose.Schema({

        sessionId: {
            type: String,
            required: true,
            unique: true,
            index: true
        },

        data: {
            type: Object,
            default: {}
        }

    });

const Session =
    mongoose.model(
        'Session',
        SessionSchema
    );


/*
 * ============================================================
 * LOAD PLUGINS
 * ============================================================
 */

const PLUGIN_PATH =
    path.join(
        __dirname,
        'plugins'
    );

if (fs.existsSync(PLUGIN_PATH)) {

    fs.readdirSync(PLUGIN_PATH)
        .forEach((plugin) => {

            if (
                path.extname(plugin)
                    .toLowerCase() === '.js'
            ) {

                try {

                    require(
                        path.join(
                            PLUGIN_PATH,
                            plugin
                        )
                    );

                } catch (err) {

                    console.error(
                        `Plugin load failed: ${plugin}`,
                        err
                    );

                }
            }

        });

}

console.log(
    `All ${BOT_NAME} Plugins Loaded ⚡`
);


/*
 * ============================================================
 * COMMAND SYSTEM
 * ============================================================
 */

const events =
    require('./lib/command');

const commandMap =
    new Map();

for (
    const cmd of events.commands
) {

    if (cmd.pattern) {

        commandMap.set(
            String(cmd.pattern)
                .toLowerCase(),
            cmd
        );

    }

    if (cmd.alias) {

        const aliases =
            Array.isArray(cmd.alias)
                ? cmd.alias
                : [cmd.alias];

        for (
            const alias of aliases
        ) {

            const name =
                String(alias)
                    .toLowerCase();

            if (
                !commandMap.has(name)
            ) {

                commandMap.set(
                    name,
                    cmd
                );

            }

        }

    }

}


/*
 * ============================================================
 * EXPRESS
 * ============================================================
 */

const app =
    express();

app.use(
    express.json({
        limit: '10mb'
    })
);

app.use(
    express.urlencoded({
        extended: true,
        limit: '10mb'
    })
);

app.use(
    express.static(
        path.join(
            __dirname,
            'public'
        )
    )
);


/*
 * ============================================================
 * SESSION MANAGERS
 * ============================================================
 */

const activeSockets = {};
const keepAliveTimers = {};
const reconnectTimers = {};
const fileCache = {};
const saveDebounceTimers = {};


/*
 * ============================================================
 * CLEANUP SESSION
 * ============================================================
 */

function cleanupSession(
    sessionId
) {

    if (
        keepAliveTimers[sessionId]
    ) {

        clearInterval(
            keepAliveTimers[sessionId]
        );

        delete keepAliveTimers[
            sessionId
        ];

    }


    if (
        reconnectTimers[sessionId]
    ) {

        clearTimeout(
            reconnectTimers[sessionId]
        );

        delete reconnectTimers[
            sessionId
        ];

    }


    if (
        saveDebounceTimers[sessionId]
    ) {

        clearTimeout(
            saveDebounceTimers[sessionId]
        );

        delete saveDebounceTimers[
            sessionId
        ];

    }


    const sock =
        activeSockets[sessionId];

    if (sock) {

        try {

            sock.ev.removeAllListeners();

            sock.ws
                ?.terminate
                ?.();

        } catch (_) {}

        delete activeSockets[
            sessionId
        ];

    }

}


/*
 * ============================================================
 * RESTORE SESSION
 * ============================================================
 */

async function restoreSession(
    sessionId,
    sessionPath
) {

    try {

        const session =
            await Session.findOne({
                sessionId
            });

        if (!session) {

            return false;

        }

        await fs.ensureDir(
            sessionPath
        );


        for (
            const file in session.data
        ) {

            await fs.writeFile(
                path.join(
                    sessionPath,
                    file
                ),
                session.data[file]
            );

        }


        console.log(
            `✅ Restored: ${sessionId}`
        );

        return true;

    } catch (err) {

        console.error(
            'Restore Error:',
            err
        );

        return false;

    }

}


/*
 * ============================================================
 * SAVE SESSION
 * ============================================================
 */

async function saveSession(
    sessionId,
    sessionPath
) {

    try {

        if (
            !await fs.pathExists(
                sessionPath
            )
        ) {

            return;

        }


        const files =
            await fs.readdir(
                sessionPath
            );

        const data = {};
        let hasChanges = false;


        for (
            const file of files
        ) {

            try {

                const filePath =
                    path.join(
                        sessionPath,
                        file
                    );

                const content =
                    await fs.readFile(
                        filePath,
                        'utf8'
                    );

                const cacheKey =
                    `${sessionId}:${file}`;


                if (
                    fileCache[cacheKey] !==
                    content
                ) {

                    fileCache[
                        cacheKey
                    ] = content;

                    hasChanges = true;

                }


                data[file] =
                    content;

            } catch (_) {}

        }


        if (!hasChanges) {

            return;

        }


        await Session.findOneAndUpdate(

            {
                sessionId
            },

            {
                $set: {
                    data
                }
            },

            {
                upsert: true,
                new: true,
                setDefaultsOnInsert: true
            }

        );


        console.log(
            `💾 Saved: ${sessionId}`
        );

    } catch (err) {

        console.error(
            'Save Session Error:',
            err
        );

    }

}


/*
 * ============================================================
 * DEBOUNCED SAVE
 * ============================================================
 */

function debouncedSaveSession(
    sessionId,
    sessionPath
) {

    if (
        saveDebounceTimers[sessionId]
    ) {

        clearTimeout(
            saveDebounceTimers[sessionId]
        );

    }


    saveDebounceTimers[
        sessionId
    ] = setTimeout(
        async () => {

            delete saveDebounceTimers[
                sessionId
            ];

            await saveSession(
                sessionId,
                sessionPath
            );

        },
        5000
    );

}


/*
 * ============================================================
 * PAIR
 * ============================================================
 */

async function Pair(
    number,
    res = null
) {

    const xnumber =
        String(number)
            .replace(
                /[^0-9]/g,
                ''
            );


    if (!xnumber) {

        if (
            res &&
            !res.headersSent
        ) {

            return res.json({
                error:
                    'Invalid number'
            });

        }

        return;

    }


    const sessionId =
        `${SESSION_PREFIX}${xnumber}`;


    const sessionPath =
        path.join(
            SESSION_BASE_PATH,
            sessionId
        );


    /*
     * Prevent duplicate socket
     */

    if (
        activeSockets[sessionId]
    ) {

        if (
            res &&
            !res.headersSent
        ) {

            return res.json({

                error:
                    'Session already active. Please wait.'

            });

        }

        return;

    }


    try {

        /*
         * Restore
         */

        await restoreSession(
            sessionId,
            sessionPath
        );

        await fs.ensureDir(
            sessionPath
        );


        /*
         * Baileys Auth
         */

        const {
            state,
            saveCreds
        } =
            await useMultiFileAuthState(
                sessionPath
            );


        const {
            version
        } =
            await fetchLatestBaileysVersion();


        const logger =
            pino({
                level: 'silent'
            });


        /*
         * WhatsApp Socket
         */

        const sock =
            makeWASocket({

                version,

                logger,

                auth: {

                    creds:
                        state.creds,

                    keys:
                        makeCacheableSignalKeyStore(
                            state.keys,
                            logger
                        )

                },

                printQRInTerminal:
                    false,

                browser: [
                    BOT_NAME,
                    'Chrome',
                    '22.04.4'
                ],

                generateHighQualityLinkPreview:
                    true,

                syncFullHistory:
                    false,

                connectTimeoutMs:
                    60000,

                defaultQueryTimeoutMs:
                    30000,

                keepAliveIntervalMs:
                    30000,

                msgRetryCounterCache

            });


        activeSockets[
            sessionId
        ] = sock;


        /*
         * ====================================================
         * SEND FILE FROM URL
         * ====================================================
         */

        sock.sendFileUrl =
            async (
                jid,
                url,
                caption = '',
                quoted = null,
                options = {}
            ) => {

                try {

                    const response =
                        await axios.head(
                            url,
                            {
                                timeout:
                                    15000,

                                maxRedirects:
                                    5
                            }
                        );


                    const mime =
                        String(
                            response
                                .headers[
                                    'content-type'
                                ] || ''
                        )
                        .split(';')[0]
                        .toLowerCase();


                    if (!mime) {

                        throw new Error(
                            'Unable to detect MIME type'
                        );

                    }


                    const buffer =
                        await getBuffer(
                            url
                        );


                    if (
                        mime ===
                        'application/pdf'
                    ) {

                        return sock.sendMessage(
                            jid,
                            {

                                document:
                                    buffer,

                                mimetype:
                                    'application/pdf',

                                fileName:
                                    options.fileName ||
                                    'file.pdf',

                                caption,

                                ...options

                            },
                            {
                                quoted
                            }
                        );

                    }


                    if (
                        mime.startsWith(
                            'image/'
                        )
                    ) {

                        return sock.sendMessage(
                            jid,
                            {

                                image:
                                    buffer,

                                caption,

                                ...options

                            },
                            {
                                quoted
                            }
                        );

                    }


                    if (
                        mime.startsWith(
                            'video/'
                        )
                    ) {

                        return sock.sendMessage(
                            jid,
                            {

                                video:
                                    buffer,

                                caption,

                                mimetype:
                                    mime,

                                ...options

                            },
                            {
                                quoted
                            }
                        );

                    }


                    if (
                        mime.startsWith(
                            'audio/'
                        )
                    ) {

                        return sock.sendMessage(
                            jid,
                            {

                                audio:
                                    buffer,

                                mimetype:
                                    mime,

                                ...options

                            },
                            {
                                quoted
                            }
                        );

                    }


                    return sock.sendMessage(
                        jid,
                        {

                            document:
                                buffer,

                            mimetype:
                                mime,

                            fileName:
                                options.fileName ||
                                'file',

                            caption,

                            ...options

                        },
                        {
                            quoted
                        }
                    );


                } catch (err) {

                    console.error(
                        '[SEND FILE URL]',
                        err
                    );

                    throw err;

                }

            };


        /*
         * ====================================================
         * CONNECTION UPDATE
         * ====================================================
         */

        sock.ev.on(
            'connection.update',
            async (
                update
            ) => {

                const {
                    connection,
                    lastDisconnect
                } = update;


                if (
                    connection ===
                    'close'
                ) {

                    const statusCode =
                        lastDisconnect
                            ?.error
                            ?.output
                            ?.statusCode;


                    const isLoggedOut =
                        statusCode ===
                        DisconnectReason.loggedOut;


                    console.log(
                        `Disconnected: ${sessionId} | Code: ${statusCode}`
                    );


                    cleanupSession(
                        sessionId
                    );


                    if (
                        !isLoggedOut
                    ) {

                        if (
                            !reconnectTimers[
                                sessionId
                            ]
                        ) {

                            reconnectTimers[
                                sessionId
                            ] = setTimeout(
                                () => {

                                    delete reconnectTimers[
                                        sessionId
                                    ];

                                    Pair(
                                        xnumber
                                    );

                                },
                                5000
                            );

                        }

                    } else {

                        console.log(
                            `🚪 Logged out: ${sessionId}`
                        );


                        await Session
                            .findOneAndDelete({
                                sessionId
                            })
                            .catch(() => {});


                        await fs.remove(
                            sessionPath
                        )
                        .catch(() => {});

                    }

                }


                else if (
                    connection ===
                    'open'
                ) {

                    console.log(
                        `✅ ${BOT_NAME} Connected: ${sessionId}`
                    );


                    /*
                     * Keep Alive
                     */

                    if (
                        keepAliveTimers[
                            sessionId
                        ]
                    ) {

                        clearInterval(
                            keepAliveTimers[
                                sessionId
                            ]
                        );

                    }


                    keepAliveTimers[
                        sessionId
                    ] = setInterval(
                        async () => {

                            if (
                                !activeSockets[
                                    sessionId
                                ]
                            ) {

                                clearInterval(
                                    keepAliveTimers[
                                        sessionId
                                    ]
                                );

                                delete keepAliveTimers[
                                    sessionId
                                ];

                                return;

                            }


                            try {

                                await sock
                                    .sendPresenceUpdate(
                                        'available',
                                        jidNormalizedUser(
                                            sock.user.id
                                        )
                                    );

                            } catch (_) {}

                        },
                        30000
                    );


                    /*
                     * Welcome
                     */

                    try {

                        const jid =
                            `${xnumber}@s.whatsapp.net`;


                        await sock.sendMessage(
                            jid,
                            {

                                text:
                                    `*${BOT_NAME} Active!*\n\n` +
                                    `Your bot is now connected successfully.\n\n` +
                                    `Status: *ONLINE* ✅`

                            }
                        );

                    } catch (err) {

                        console.error(
                            'Welcome message failed:',
                            err
                        );

                    }

                }

            }
        );


        /*
         * ====================================================
         * CREDENTIAL UPDATE
         * ====================================================
         */

        sock.ev.on(
            'creds.update',
            async () => {

                try {

                    await saveCreds();

                    debouncedSaveSession(
                        sessionId,
                        sessionPath
                    );

                } catch (err) {

                    console.error(
                        'Creds update error:',
                        err
                    );

                }

            }
        );


        /*
         * ====================================================
         * MESSAGE HANDLER
         * ====================================================
         */

        sock.ev.on(
            'messages.upsert',
            async (
                update
            ) => {

                try {

                    let mek =
                        update.messages?.[0];


                    if (
                        !mek ||
                        !mek.message
                    ) {

                        return;

                    }


                    /*
                     * Ephemeral
                     */

                    if (
                        mek.message
                            ?.ephemeralMessage
                            ?.message
                    ) {

                        mek.message =
                            mek.message
                                .ephemeralMessage
                                .message;

                    }


                    /*
                     * STATUS
                     */

                    if (
                        mek.key
                            ?.remoteJid ===
                        'status@broadcast'
                    ) {

                        if (
                            config.AUTO_READ_STATUS
                        ) {

                            await sock
                                .readMessages([
                                    mek.key
                                ])
                                .catch(() => {});

                        }


                        if (
                            config.AUTO_REACT
                        ) {

                            await sock
                                .sendMessage(
                                    mek.key.remoteJid,
                                    {

                                        react: {

                                            text:
                                                '❤️',

                                            key:
                                                mek.key

                                        }

                                    }
                                )
                                .catch(() => {});

                        }

                        return;

                    }


                    /*
                     * Message wrapper
                     */

                    const m =
                        sms(
                            sock,
                            mek
                        );


                    const type =
                        getContentType(
                            mek.message
                        );


                    const from =
                        mek.key.remoteJid;


                    /*
                     * =================================================
                     * RAW BODY
                     * =================================================
                     */

                    let body = '';


                    if (
                        type ===
                        'conversation'
                    ) {

                        body =
                            mek.message
                                .conversation ||
                            '';

                    }

                    else if (
                        type ===
                        'extendedTextMessage'
                    ) {

                        body =
                            mek.message
                                .extendedTextMessage
                                ?.text ||
                            '';

                    }

                    else if (
                        type ===
                        'imageMessage'
                    ) {

                        body =
                            mek.message
                                .imageMessage
                                ?.caption ||
                            '';

                    }

                    else if (
                        type ===
                        'videoMessage'
                    ) {

                        body =
                            mek.message
                                .videoMessage
                                ?.caption ||
                            '';

                    }

                    else if (
                        type ===
                        'buttonsResponseMessage'
                    ) {

                        body =
                            mek.message
                                .buttonsResponseMessage
                                ?.selectedButtonId ||
                            '';

                    }

                    else if (
                        type ===
                        'listResponseMessage'
                    ) {

                        body =
                            mek.message
                                .listResponseMessage
                                ?.singleSelectReply
                                ?.selectedRowId ||
                            '';

                    }

                    else if (
                        type ===
                        'templateButtonReplyMessage'
                    ) {

                        body =
                            mek.message
                                .templateButtonReplyMessage
                                ?.selectedId ||
                            '';

                    }

                    else if (
                        type ===
                        'interactiveResponseMessage'
                    ) {

                        try {

                            const params =
                                mek.message
                                    .interactiveResponseMessage
                                    ?.nativeFlowResponseMessage
                                    ?.paramsJson;


                            const parsed =
                                JSON.parse(
                                    params || '{}'
                                );


                            body =
                                parsed.id ||
                                parsed
                                    .selectedId ||
                                '';

                        } catch (_) {

                            body = '';

                        }

                    }

                    else {

                        body =
                            m.msg?.text ||
                            m.msg?.conversation ||
                            m.msg?.caption ||
                            '';

                    }


                    /*
                     * =================================================
                     * BUTTON / LIST COMMAND RESOLVER
                     * =================================================
                     */

                    let resolvedBody =
                        String(
                            body || ''
                        ).trim();


                    try {

                        const contextInfo =
                            mek.message
                                ?.extendedTextMessage
                                ?.contextInfo ||

                            mek.message
                                ?.buttonsResponseMessage
                                ?.contextInfo ||

                            mek.message
                                ?.listResponseMessage
                                ?.contextInfo ||

                            mek.message
                                ?.templateButtonReplyMessage
                                ?.contextInfo ||

                            mek.message
                                ?.interactiveResponseMessage
                                ?.contextInfo;


                        const quotedStanzaId =
                            contextInfo
                                ?.stanzaId;


                        if (
                            quotedStanzaId &&
                            resolvedBody
                        ) {

                            const stored =
                                await getCMDStore(
                                    quotedStanzaId
                                );


                            if (
                                Array.isArray(
                                    stored
                                )
                            {

                                const selected =
                                    stored.find(
                                        item =>
                                            String(
                                                item.cmdId
                                            ) ===
                                            String(
                                                resolvedBody
                                            )
                                    );


                                if (
                                    selected?.cmd
                                ) {

                                    resolvedBody =
                                        String(
                                            selected.cmd
                                        ).trim();


                                    console.log(
                                        `[BUTTON/LIST] ${body} → ${resolvedBody}`
                                    );

                                }

                            }

                        }

                    } catch (err) {

                        console.error(
                            '[BUTTON/LIST RESOLVER]',
                            err
                        );

                    }


                    /*
                     * =================================================
                     * COMMAND DATA
                     * =================================================
                     */

                    const prefix =
                        String(
                            config.PREFIX ||
                            '.'
                        );


                    const commandBody =
                        resolvedBody;


                    const isCmd =
                        commandBody
                            .startsWith(
                                prefix
                            );


                    const command =
                        isCmd
                            ? commandBody
                                .slice(
                                    prefix.length
                                )
                                .trim()
                                .split(
                                    /\s+/
                                )
                                .shift()
                                .toLowerCase()
                            : '';


                    const args =
                        commandBody
                            .trim()
                            .split(
                                /\s+/
                            )
                            .slice(1);


                    const q =
                        args.join(' ');


                    const isGroup =
                        from.endsWith(
                            '@g.us'
                        );


                    /*
                     * =================================================
                     * SENDER
                     * =================================================
                     */

                    const sender =
                        mek.key.fromMe

                            ? jidNormalizedUser(
                                sock.user.id
                            )

                            : (
                                mek.key.participant ||
                                mek.key.remoteJid
                            );


                    const senderNumber =
                        String(
                            sender
                        )
                        .split('@')[0]
                        .split(':')[0];


                    const botNumber =
                        String(
                            sock.user.id
                        )
                        .split(':')[0];


                    const botNumber2 =
                        await jidNormalizedUser(
                            sock.user.id
                        );


                    const pushname =
                        mek.pushName ||
                        'User';


                    const isMe =
                        botNumber ===
                        senderNumber;


                    const isOwner =
                        isMe ||
                        xnumber ===
                        senderNumber;


                    const isReact =
                        Boolean(
                            m.message
                                ?.reactionMessage
                        );


                    /*
                     * =================================================
                     * QUOTED
                     * =================================================
                     */

                    const context =
                        mek.message
                            ?.extendedTextMessage
                            ?.contextInfo;


                    const quoted =
                        context
                            ?.quotedMessage ||
                        null;


                    /*
                     * =================================================
                     * GROUP
                     * =================================================
                     */

                    let groupMetadata =
                        null;

                    let participants = [];

                    let groupAdmins = [];

                    let groupName = '';

                    let isBotAdmins =
                        false;

                    let isAdmins =
                        false;


                    if (isGroup) {

                        groupMetadata =
                            await sock
                                .groupMetadata(
                                    from
                                )
                                .catch(
                                    () => null
                                );


                        if (
                            groupMetadata
                        ) {

                            groupName =
                                groupMetadata
                                    .subject ||
                                '';

                            participants =
                                groupMetadata
                                    .participants ||
                                [];


                            groupAdmins =
                                getGroupAdmins(
                                    participants
                                );


                            isBotAdmins =
                                groupAdmins
                                    .includes(
                                        botNumber2
                                    );


                            isAdmins =
                                groupAdmins
                                    .includes(
                                        sender
                                    );

                        }

                    }


                    const isSudo =
                        false;

                    const isPre =
                        false;


                    /*
                     * =================================================
                     * REPLY
                     * =================================================
                     */

                    const reply =
                        async (
                            teks
                        ) => {

                            return sock
                                .sendMessage(
                                    from,
                                    {
                                        text:
                                            String(
                                                teks
                                            )
                                    },
                                    {
                                        quoted:
                                            mek
                                    }
                                );

                        };


                    /*
                     * =================================================
                     * REPLYAD
                     * =================================================
                     */

                    sock.replyad =
                        async (
                            teks
                        ) => {

                            return sock
                                .sendMessage(
                                    from,
                                    {
                                        text:
                                            String(
                                                teks
                                            )
                                    },
                                    {
                                        quoted:
                                            mek
                                    }
                                );

                        };


                    /*
                     * =================================================
                     * BUTTON SYSTEM
                     * =================================================
                     */

                    const NON_BUTTON =
                        true;


                    /*
                     * -------------------------------------------------
                     * BUTTON MESSAGE 2
                     * -------------------------------------------------
                     */

                    sock.buttonMessage2 =
                        async (
                            jid,
                            text,
                            footer,
                            buttons,
                            quotedMessage = null,
                            options = {}
                        ) => {

                            try {

                                const btns =
                                    Array.isArray(
                                        buttons
                                    )
                                        ? buttons
                                        : [];


                                const formatted =
                                    btns.map(
                                        (
                                            button,
                                            index
                                        ) => {

                                            if (
                                                typeof button ===
                                                'string'
                                            ) {

                                                return {

                                                    buttonId:
                                                        button,

                                                    buttonText:
                                                    {
                                                        displayText:
                                                            button
                                                    },

                                                    type: 1

                                                };

                                            }


                                            return {

                                                buttonId:
                                                    String(
                                                        button.buttonId ||
                                                        button.id ||
                                                        button.cmd ||
                                                        `btn_${index + 1}`
                                                    ),

                                                buttonText:
                                                {
                                                    displayText:
                                                        String(
                                                            button
                                                                .buttonText
                                                                ?.displayText ||
                                                            button.displayText ||
                                                            button.text ||
                                                            button.title ||
                                                            `Button ${index + 1}`
                                                        )
                                                },

                                                type: 1

                                            };

                                        }
                                    );


                                const message =
                                {

                                    text:
                                        text || '',

                                    footer:
                                        footer || '',

                                    buttons:
                                        formatted,

                                    headerType:
                                        1,

                                    ...options

                                };


                                const sent =
                                    await sock
                                        .sendMessage(
                                            jid,
                                            message,
                                            {
                                                quoted:
                                                    quotedMessage
                                            }
                                        );


                                /*
                                 * Save command mappings
                                 */

                                if (
                                    sent?.key?.id &&
                                    btns.length
                                ) {

                                    const mappings =
                                        btns.map(
                                            (
                                                button,
                                                index
                                            ) => {

                                                if (
                                                    typeof button ===
                                                    'string'
                                                ) {

                                                    return {

                                                        cmdId:
                                                            String(
                                                                button
                                                            ),

                                                        cmd:
                                                            String(
                                                                button
                                                            )

                                                    };

                                                }


                                                const cmdId =
                                                    String(
                                                        button.buttonId ||
                                                        button.id ||
                                                        button.cmd ||
                                                        `btn_${index + 1}`
                                                    );


                                                const cmd =
                                                    String(
                                                        button.cmd ||
                                                        button.command ||
                                                        button.buttonId ||
                                                        button.id ||
                                                        cmdId
                                                    );


                                                return {

                                                    cmdId,

                                                    cmd

                                                };

                                            }
                                        );


                                    await updateCMDStore(
                                        sent.key.id,
                                        mappings
                                    );

                                }


                                return sent;


                            } catch (err) {

                                console.error(
                                    '[BUTTON MESSAGE 2 ERROR]',
                                    err
                                );

                                return null;

                            }

                        };


                    /*
                     * -------------------------------------------------
                     * BUTTON MESSAGE
                     * -------------------------------------------------
                     */

                    sock.buttonMessage =
                        async (
                            jid,
                            text,
                            footer,
                            buttons,
                            quotedMessage = null,
                            options = {}
                        ) => {

                            return sock
                                .buttonMessage2(
                                    jid,
                                    text,
                                    footer,
                                    buttons,
                                    quotedMessage,
                                    options
                                );

                        };


                    /*
                     * -------------------------------------------------
                     * LIST MESSAGE
                     * -------------------------------------------------
                     */

                    sock.listMessage =
                        async (
                            jid,
                            text,
                            footer,
                            title,
                            sections,
                            quotedMessage = null,
                            options = {}
                        ) => {

                            try {

                                const rows = [];

                                const mappings = [];


                                const sectionList =
                                    Array.isArray(
                                        sections
                                    )
                                        ? sections
                                        : [];


                                for (
                                    const section
                                    of sectionList
                                ) {

                                    const sectionRows =
                                        section?.rows ||
                                        section?.options ||
                                        [];


                                    for (
                                        const row
                                        of sectionRows
                                    ) {

                                        const rowId =
                                            String(
                                                row.rowId ||
                                                row.id ||
                                                row.cmd ||
                                                row.optionId ||
                                                getRandom(
                                                    'row_'
                                                )
                                            );


                                        rows.push({

                                            title:
                                                String(
                                                    row.title ||
                                                    row.displayText ||
                                                    row.description ||
                                                    rowId
                                                ),

                                            description:
                                                String(
                                                    row.description ||
                                                    ''
                                                ),

                                            rowId

                                        });


                                        mappings.push({

                                            cmdId:
                                                rowId,

                                            cmd:
                                                String(
                                                    row.cmd ||
                                                    row.command ||
                                                    rowId
                                                )

                                        });

                                    }

                                }


                                const message =
                                {

                                    text:
                                        text || '',

                                    footer:
                                        footer || '',

                                    title:
                                        title || '',

                                    buttonText:
                                        options.buttonText ||
                                        'Select',

                                    sections: [
                                        {
                                            title:
                                                options.sectionTitle ||
                                                '',

                                            rows
                                        }
                                    ],

                                    ...options

                                };


                                /*
                                 * Internal options
                                 */

                                delete message
                                    .sectionTitle;


                                const sent =
                                    await sock
                                        .sendMessage(
                                            jid,
                                            message,
                                            {
                                                quoted:
                                                    quotedMessage
                                            }
                                        );


                                if (
                                    sent?.key?.id &&
                                    mappings.length
                                ) {

                                    await updateCMDStore(
                                        sent.key.id,
                                        mappings
                                    );

                                }


                                return sent;


                            } catch (err) {

                                console.error(
                                    '[LIST MESSAGE ERROR]',
                                    err
                                );

                                return null;

                            }

                        };


                    /*
                     * -------------------------------------------------
                     * EDIT
                     * -------------------------------------------------
                     */

                    sock.edite =
                        async (
                            gg,
                            newmg,
                            jid = null
                        ) => {

                            try {

                                const targetJid =
                                    jid ||
                                    gg?.key
                                        ?.remoteJid ||
                                    from;


                                if (
                                    !targetJid ||
                                    !gg?.key
                                ) {

                                    throw new Error(
                                        'Invalid message key'
                                    );

                                }


                                return await sock
                                    .relayMessage(
                                        targetJid,
                                        {

                                            protocolMessage:
                                            {

                                                key:
                                                    gg.key,

                                                type:
                                                    14,

                                                editedMessage:
                                                {

                                                    conversation:
                                                        String(
                                                            newmg
                                                        )

                                                }

                                            }

                                        },
                                        {}
                                    );


                            } catch (err) {

                                console.error(
                                    '[EDIT ERROR]',
                                    err
                                );

                                return null;

                            }

                        };


                    /*
                     * =================================================
                     * READ COMMAND
                     * =================================================
                     */

                    if (isCmd) {

                        await sock
                            .readMessages([
                                mek.key
                            ])
                            .catch(() => {});

                    }


                    /*
                     * =================================================
                     * AUTO REACT
                     * =================================================
                     */

                    if (
                        config.AUTO_REACT &&
                        !isMe &&
                        !isReact &&
                        Math.random() < 0.3
                    ) {

                        const emojis =
                            Array.isArray(
                                config.REACT_EMOJIS
                            )
                                ? config.REACT_EMOJIS
                                : ['❤️'];


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

                                    key:
                                        mek.key

                                }

                            }
                        )
                        .catch(() => {});

                    }


                    /*
                     * =================================================
                     * AUTO TYPING
                     * =================================================
                     */

                    if (
                        config.AUTO_TYPING
                    ) {

                        sock
                            .sendPresenceUpdate(
                                'composing',
                                from
                            )
                            .catch(() => {});


                        setTimeout(
                            () => {

                                sock
                                    .sendPresenceUpdate(
                                        'paused',
                                        from
                                    )
                                    .catch(() => {});

                            },
                            3000
                        );

                    }


                    /*
                     * =================================================
                     * COMMAND NAME
                     * =================================================
                     */

                    const cmdName =
                        isCmd
                            ? commandBody
                                .slice(
                                    prefix.length
                                )
                                .trim()
                                .split(
                                    /\s+/
                                )[0]
                                .toLowerCase()
                            : false;


                    /*
                     * =================================================
                     * COMMAND MAP
                     * =================================================
                     */

                    if (isCmd) {

                        const cmd =
                            commandMap.get(
                                cmdName
                            );


                        if (cmd) {

                            if (
                                cmd.react
                            ) {

                                sock.sendMessage(
                                    from,
                                    {

                                        react: {

                                            text:
                                                cmd.react,

                                            key:
                                                mek.key

                                        }

                                    }
                                )
                                .catch(() => {});

                            }


                            try {

                                await Promise.resolve(
                                    cmd.function(
                                        sock,
                                        mek,
                                        m,
                                        {

                                            from,
                                            prefix,

                                            isSudo,
                                            quoted,

                                            body:
                                                commandBody,

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
                                    )
                                );

                            } catch (err) {

                                console.error(
                                    '[PLUGIN ERROR]',
                                    err
                                );

                            }

                        }

                    }


                    /*
                     * =================================================
                     * EVENT COMMANDS
                     * =================================================
                     */

                    for (
                        const cmd of
                        events.commands
                    ) {

                        try {

                            if (
                                commandBody &&
                                cmd.on ===
                                'body'
                            ) {

                                await Promise.resolve(
                                    cmd.function(
                                        sock,
                                        mek,
                                        m,
                                        {

                                            from,
                                            prefix,

                                            quoted,

                                            body:
                                                commandBody,

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
                                    )
                                );

                            }


                            else if (
                                mek.q &&
                                cmd.on ===
                                'text'
                            ) {

                                await Promise.resolve(
                                    cmd.function(
                                        sock,
                                        mek,
                                        m,
                                        {

                                            from,
                                            quoted,

                                            body:
                                                commandBody,

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
                                    )
                                );

                            }


                            else if (
                                (
                                    cmd.on ===
                                    'image' ||
                                    cmd.on ===
                                    'photo'
                                ) &&
                                type ===
                                'imageMessage'
                            ) {

                                await Promise.resolve(
                                    cmd.function(
                                        sock,
                                        mek,
                                        m,
                                        {

                                            from,
                                            prefix,
                                            quoted,

                                            isSudo,
                                            body:
                                                commandBody,

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
                                    )
                                );

                            }


                            else if (
                                cmd.on ===
                                'sticker' &&
                                type ===
                                'stickerMessage'
                            ) {

                                await Promise.resolve(
                                    cmd.function(
                                        sock,
                                        mek,
                                        m,
                                        {

                                            from,
                                            prefix,
                                            quoted,

                                            body:
                                                commandBody,

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
                                    )
                                );

                            }

                        } catch (err) {

                            console.error(
                                '[CMD MAP ERROR]',
                                err
                            );

                        }

                    }


                    /*
                     * =================================================
                     * INTERNAL COMMANDS
                     * =================================================
                     */

                    switch (
                        command
                    ) {

                        case 'jid':

                            await reply(
                                from
                            );

                            break;


                        case 'ev':

                            if (
                                isOwner
                            ) {

                                try {

                                    /*
                                     * Keep owner-only.
                                     */

                                    const result =
                                        await eval(
                                            q
                                        );


                                    await reply(
                                        util.format(
                                            result
                                        )
                                    );

                                } catch (err) {

                                    await reply(
                                        util.format(
                                            err
                                        )
                                    );

                                }

                            }

                            break;


                        default:
                            break;

                    }


                } catch (err) {

                    console.error(
                        '[MESSAGE ERROR]',
                        err
                    );

                }

            }
        );


        /*
         * ====================================================
         * PAIRING CODE
         * ====================================================
         */

        let pairingCode =
            null;

        let responded =
            false;


        if (
            !sock.authState
                ?.creds
                ?.registered
        ) {

            try {

                await new Promise(
                    resolve =>
                        setTimeout(
                            resolve,
                            3000
                        )
                );


                pairingCode =
                    await sock
                        .requestPairingCode(
                            xnumber
                        );


                console.log(
                    `🔐 ${BOT_NAME} Pairing Code: ${pairingCode}`
                );


                if (
                    res &&
                    !res.headersSent
                ) {

                    res.json({
                        code:
                            pairingCode
                    });

                    responded =
                        true;

                }

            } catch (err) {

                console.error(
                    'Pairing code request failed:',
                    err
                );


                if (
                    res &&
                    !res.headersSent
                ) {

                    res.json({

                        error:
                            'Failed to generate pairing code. Try again.'

                    });

                    responded =
                        true;

                }


                cleanupSession(
                    sessionId
                );

                return;

            }

        } else {

            console.log(
                `Already registered: ${sessionId}`
            );


            if (
                res &&
                !res.headersSent
            ) {

                res.json({

                    error:
                        'This number is already paired.'

                });

                responded =
                    true;

            }

        }


        if (
            res &&
            !responded
        ) {

            setTimeout(
                () => {

                    if (
                        !res.headersSent
                    ) {

                        res.json({

                            error:
                                'Pairing timed out. Try again.'

                        });

                    }

                },
                15000
            );

        }


    } catch (err) {

        console.error(
            'Pair Error:',
            err
        );


        cleanupSession(
            sessionId
        );


        if (
            res &&
            !res.headersSent
        ) {

            res.json({

                error:
                    'Pair failed: ' +
                    err.message

            });

        }

    }

}


/*
 * ============================================================
 * RESTORE ALL SHALA MINI SESSIONS
 * ============================================================
 */

async function restoreAllSessions() {

    try {

        const sessions =
            await Session.find({
                sessionId: {
                    $regex:
                        `^${SESSION_PREFIX}`
                }
            });


        console.log(
            `🔄 Restoring ${sessions.length} ${BOT_NAME} session(s)...`
        );


        await Promise.all(

            sessions
                .filter(
                    session =>
                        Boolean(
                            session.sessionId
                        )
                )
                .map(
                    async (
                        session,
                        index
                    ) => {

                        const number =
                            session.sessionId
                                .replace(
                                    SESSION_PREFIX,
                                    ''
                                );


                        await new Promise(
                            resolve =>
                                setTimeout(
                                    resolve,
                                    index * 500
                                )
                        );


                        try {

                            await Pair(
                                number
                            );

                        } catch (err) {

                            console.error(
                                `Failed to restore ${session.sessionId}`,
                                err
                            );

                        }

                    }
                )

        );

    } catch (err) {

        console.error(
            'Restore all sessions error:',
            err
        );

    }

}


/*
 * ============================================================
 * PAIR API
 * ============================================================
 */

app.get(
    '/pair',
    async (
        req,
        res
    ) => {

        const number =
            req.query.number;


        if (!number) {

            return res.json({

                error:
                    'Number required'

            });

        }


        res.setTimeout(
            30000,
            () => {

                if (
                    !res.headersSent
                ) {

                    res.json({

                        error:
                            'Request timed out. Try again.'

                    });

                }

            }
        );


        await Pair(
            number,
            res
        );

    }
);


/*
 * ============================================================
 * HEALTH CHECK
 * ============================================================
 */

app.get(
    '/health',
    (req, res) => {

        res.json({

            status:
                'online',

            bot:
                BOT_NAME,

            sessions:
                Object.keys(
                    activeSockets
                ).length,

            uptime:
                process.uptime(),

            timestamp:
                new Date()
                    .toISOString()

        });

    }
);


/*
 * ============================================================
 * ROOT
 * ============================================================
 */

app.get(
    '/',
    (req, res) => {

        res.send(
            `${BOT_NAME} Bot Server Running! 🚀`
        );

    }
);


/*
 * ============================================================
 * SERVER START
 * ============================================================
 */

app.listen(
    PORT,
    async () => {

        console.log('');

        console.log(
            '╔══════════════════════════════════════════╗'
        );

        console.log(
            `║          ${BOT_NAME} SERVER 🚀            ║`
        );

        console.log(
            '╠══════════════════════════════════════════╣'
        );

        console.log(
            `║ Port    : ${PORT}`
        );

        console.log(
            `║ Prefix  : ${SESSION_PREFIX}`
        );

        console.log(
            '║ Status  : ONLINE ✅'
        );

        console.log(
            '║ Buttons : ENABLED ✅'
        );

        console.log(
            '║ Lists   : ENABLED ✅'
        );

        console.log(
            '║ CMD Map : ENABLED ✅'
        );

        console.log(
            '╚══════════════════════════════════════════╝'
        );

        console.log('');


        await fs.ensureDir(
            SESSION_BASE_PATH
        );


        /*
         * Give MongoDB a moment before restoring.
         */

        if (
            mongoose.connection.readyState !==
            1
        ) {

            await new Promise(
                resolve => {

                    const timeout =
                        setTimeout(
                            resolve,
                            10000
                        );


                    const check =
                        setInterval(
                            () => {

                                if (
                                    mongoose
                                        .connection
                                        .readyState ===
                                    1
                                ) {

                                    clearInterval(
                                        check
                                    );

                                    clearTimeout(
                                        timeout
                                    );

                                    resolve();

                                }

                            },
                            250
                        );

                }
            );

        }


        await restoreAllSessions();

    }
);


/*
 * ============================================================
 * GLOBAL ERROR HANDLER
 * ============================================================
 */

process.on(
    'uncaughtException',
    (err) => {

        const error =
            String(err);
        if (
            error.includes(
                'Socket connection timeout'
            )
        ) return;
        if (
            error.includes(
                'rate-overlimit'
            )
        ) return;
        if (
            error.includes(
                'Connection Closed'
            )
        ) return;
        if (
            error.includes(
                'Value not found'
            )
        ) return;
        
        console.error(
            'Caught exception:',
            err
        );

    }
);


process.on(
    'unhandledRejection',
    (reason) => {

        console.error(
            'Unhandled Rejection:',
            reason
        );

    }
);
