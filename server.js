require('dotenv').config();

const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const { TikTokConnectionWrapper, getGlobalConnectionCount } = require('./connectionWrapper');
const { clientBlocked } = require('./limiter');

const app = express();
const httpServer = createServer(app);

// Enable cross origin resource sharing
const io = new Server(httpServer, {
    cors: {
        origin: '*'
    }
});

// Store active streams managed by FastAPI
const activeStreams = new Map(); // stream_id -> { wrapper, username, metadata, startedAt }

// ============================================
// NAMESPACE: /backend (FastAPI Communication)
// ============================================
const backendNamespace = io.of('/backend');

backendNamespace.on('connection', (socket) => {
    console.info('[BACKEND] FastAPI connected');

    // Command: Start a new stream
    socket.on('start-stream', (data) => {
        const { stream_id, username, metadata } = data;

        console.info(`[STREAM] Starting: ${stream_id} for @${username}`);

        if (!stream_id || !username) {
            socket.emit('stream-error', {
                stream_id,
                error: 'stream_id and username are required'
            });
            return;
        }

        if (activeStreams.has(stream_id)) {
            socket.emit('stream-error', {
                stream_id,
                error: 'Stream already active'
            });
            return;
        }

        try {
            const options = {};

            // Session ID in .env file is optional
            if (process.env.SESSIONID) {
                options.sessionId = process.env.SESSIONID;
            }

            const wrapper = new TikTokConnectionWrapper(username, options, true);

            // Handle connection success
            wrapper.once('connected', (state) => {
                console.info(`[OK] Stream ${stream_id} connected to room ${state.roomId}`);

                const streamInfo = activeStreams.get(stream_id);
                if (streamInfo) {
                    streamInfo.status = 'connected';
                    streamInfo.roomId = state.roomId;
                }

                socket.emit('stream-connected', {
                    stream_id,
                    room_id: state.roomId,
                    username,
                    upgraded_to_websocket: state.upgradedToWebsocket
                });
            });

            // Handle disconnection
            wrapper.once('disconnected', (reason) => {
                console.info(`[DISCONNECT] Stream ${stream_id}: ${reason}`);
                activeStreams.delete(stream_id);

                socket.emit('stream-disconnected', {
                    stream_id,
                    reason: reason
                });
            });

            // Forward TikTok events to FastAPI
            wrapper.connection.on('chat', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'chat',
                    data: msg
                });
            });

            wrapper.connection.on('questionNew', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'question',
                    data: msg
                });
            });

            wrapper.connection.on('gift', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'gift',
                    data: msg
                });
            });

            wrapper.connection.on('member', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'member',
                    data: msg
                });
            });

            wrapper.connection.on('like', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'like',
                    data: msg
                });
            });

            wrapper.connection.on('social', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'social',
                    data: msg
                });
            });

            wrapper.connection.on('roomUser', (msg) => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'room_stats',
                    data: msg
                });
            });

            wrapper.connection.on('streamEnd', () => {
                socket.emit('tiktok-event', {
                    stream_id,
                    event_type: 'stream_ended',
                    data: {}
                });
            });

            // Connect to TikTok
            wrapper.connect();

            // Store stream info
            activeStreams.set(stream_id, {
                wrapper,
                username,
                metadata,
                status: 'connecting',
                startedAt: new Date()
            });

            socket.emit('stream-started', {
                success: true,
                stream_id,
                status: 'connecting'
            });

        } catch (error) {
            console.error(`[ERROR] Starting stream ${stream_id}:`, error);
            socket.emit('stream-error', {
                stream_id,
                error: error.message
            });
        }
    });

    // Command: Stop a stream
    socket.on('stop-stream', (data) => {
        const { stream_id } = data;

        console.info(`[STOP] Stopping stream: ${stream_id}`);

        const stream = activeStreams.get(stream_id);

        if (!stream) {
            socket.emit('stream-error', {
                stream_id,
                error: 'Stream not found'
            });
            return;
        }

        try {
            stream.wrapper.disconnect();
            activeStreams.delete(stream_id);

            socket.emit('stream-stopped', {
                stream_id,
                success: true
            });

        } catch (error) {
            socket.emit('stream-error', {
                stream_id,
                error: error.message
            });
        }
    });

    // Command: Get active streams
    socket.on('get-streams', () => {
        const streams = Array.from(activeStreams.entries()).map(([id, info]) => ({
            stream_id: id,
            username: info.username,
            status: info.status,
            room_id: info.roomId,
            started_at: info.startedAt,
            metadata: info.metadata
        }));

        socket.emit('streams-list', { streams });
    });

    socket.on('disconnect', () => {
        console.info('[BACKEND] FastAPI disconnected');
    });
});

// ============================================
// NAMESPACE: / (Original Web Interface)
// ============================================
io.on('connection', (socket) => {
    let tiktokConnectionWrapper;

    console.info('New web client connection from origin', socket.handshake.headers['origin'] || socket.handshake.headers['referer']);

    socket.on('setUniqueId', (uniqueId, options) => {

        // Prohibit the client from specifying these options (for security reasons)
        if (typeof options === 'object' && options) {
            delete options.requestOptions;
            delete options.websocketOptions;
        } else {
            options = {};
        }

        // Session ID in .env file is optional
        if (process.env.SESSIONID) {
            options.sessionId = process.env.SESSIONID;
            console.info('Using SessionId');
        }

        // Check if rate limit exceeded
        if (process.env.ENABLE_RATE_LIMIT && clientBlocked(io, socket)) {
            socket.emit('tiktokDisconnected', 'You have opened too many connections or made too many connection requests. Please reduce the number of connections/requests or host your own server instance. The connections are limited to avoid that the server IP gets blocked by TokTok.');
            return;
        }

        // Connect to the given username (uniqueId)
        try {
            tiktokConnectionWrapper = new TikTokConnectionWrapper(uniqueId, options, true);
            tiktokConnectionWrapper.connect();
        } catch (err) {
            socket.emit('tiktokDisconnected', err.toString());
            return;
        }

        // Redirect wrapper control events once
        tiktokConnectionWrapper.once('connected', state => socket.emit('tiktokConnected', state));
        tiktokConnectionWrapper.once('disconnected', reason => socket.emit('tiktokDisconnected', reason));

        // Notify client when stream ends
        tiktokConnectionWrapper.connection.on('streamEnd', () => socket.emit('streamEnd'));

        // Redirect message events
        tiktokConnectionWrapper.connection.on('roomUser', msg => socket.emit('roomUser', msg));
        tiktokConnectionWrapper.connection.on('member', msg => socket.emit('member', msg));
        tiktokConnectionWrapper.connection.on('chat', msg => socket.emit('chat', msg));
        tiktokConnectionWrapper.connection.on('gift', msg => socket.emit('gift', msg));
        tiktokConnectionWrapper.connection.on('social', msg => socket.emit('social', msg));
        tiktokConnectionWrapper.connection.on('like', msg => socket.emit('like', msg));
        tiktokConnectionWrapper.connection.on('questionNew', msg => socket.emit('questionNew', msg));
        tiktokConnectionWrapper.connection.on('linkMicBattle', msg => socket.emit('linkMicBattle', msg));
        tiktokConnectionWrapper.connection.on('linkMicArmies', msg => socket.emit('linkMicArmies', msg));
        tiktokConnectionWrapper.connection.on('liveIntro', msg => socket.emit('liveIntro', msg));
        tiktokConnectionWrapper.connection.on('emote', msg => socket.emit('emote', msg));
        tiktokConnectionWrapper.connection.on('envelope', msg => socket.emit('envelope', msg));
        tiktokConnectionWrapper.connection.on('subscribe', msg => socket.emit('subscribe', msg));
    });

    socket.on('disconnect', () => {
        if (tiktokConnectionWrapper) {
            tiktokConnectionWrapper.disconnect();
        }
    });
});

// Emit global connection statistics
setInterval(() => {
    io.emit('statistic', { globalConnectionCount: getGlobalConnectionCount() });
}, 5000)

// Serve frontend files
app.use(express.static('public'));

// Start http listener
const port = process.env.PORT || 8081;
httpServer.listen(port);
console.info(`Server running! Please visit http://localhost:${port}`);
console.info(`Backend namespace ready at /backend for FastAPI communication`);