require('dotenv').config();

const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const { TikTokConnectionWrapper, getGlobalConnectionCount } = require('./connectionWrapper');
const { clientBlocked } = require('./limiter');
const htmlPdf = require('html-pdf-node');

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

// Store all comments for export
const storedComments = [];

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
                // Store comment for PDF export
                storedComments.push({
                    username: msg.uniqueId,
                    comment: msg.comment,
                    timestamp: new Date()
                });
                
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
        tiktokConnectionWrapper.connection.on('chat', msg => {
            // Store comment for PDF export
            storedComments.push({
                username: msg.uniqueId,
                comment: msg.comment,
                timestamp: new Date()
            });
            socket.emit('chat', msg);
        });
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

// Endpoint to export comments as PDF
app.get('/export-comments', async (req, res) => {
    try {
        // Generate HTML content
        const htmlContent = `
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {
                    font-family: Arial, sans-serif;
                    padding: 20px;
                    background: #fff;
                    font-size: 11px;
                }
                h1 {
                    color: #2c3e50;
                    text-align: center;
                    font-size: 18px;
                    margin: 10px 0;
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 8px;
                }
                .info {
                    text-align: center;
                    color: #7f8c8d;
                    margin-bottom: 15px;
                    font-size: 10px;
                }
                .comment {
                    background: #fafafa;
                    margin: 5px 0;
                    padding: 6px 10px;
                    border-left: 3px solid #3498db;
                }
                .comment:nth-child(even) {
                    background: #fff;
                    border-left-color: #e74c3c;
                }
                .number {
                    color: #3498db;
                    font-weight: bold;
                    font-size: 9px;
                    display: inline;
                }
                .username {
                    color: #2c3e50;
                    font-weight: bold;
                    font-size: 12px;
                    display: inline;
                    margin-left: 5px;
                }
                .text {
                    color: #000;
                    font-size: 11px;
                    margin: 3px 0 2px 0;
                    line-height: 1.3;
                    word-wrap: break-word;
                }
                .timestamp {
                    color: #95a5a6;
                    font-size: 8px;
                }
            </style>
        </head>
        <body>
            <h1>Comentarios del Live</h1>
            <div class="info">
                <p><strong>Total:</strong> ${storedComments.length} | <strong>Generado:</strong> ${new Date().toLocaleString('es-ES')}</p>
            </div>
            ${storedComments.length === 0 
                ? '<p style="text-align: center; color: #7f8c8d;">No hay comentarios almacenados.</p>'
                : storedComments.map((comment, index) => `
                <div class="comment">
                    <span class="number">#${index + 1}</span>
                    <span class="username">${comment.username}</span>
                    <div class="text">${comment.comment || '[sin texto]'}</div>
                    <div class="timestamp">${comment.timestamp.toLocaleString('es-ES')}</div>
                </div>
            `).join('')}
        </body>
        </html>
        `;

        const options = { 
            format: 'A4',
            margin: { top: '10mm', right: '10mm', bottom: '10mm', left: '10mm' }
        };
        
        const file = { content: htmlContent };
        
        // Generate PDF from HTML
        const pdfBuffer = await htmlPdf.generatePdf(file, options);
        
        // Set response headers
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename=comentarios-tiktok-${Date.now()}.pdf`);
        
        // Send PDF
        res.send(pdfBuffer);
    } catch (error) {
        console.error('Error generating HTML PDF:', error);
        res.status(500).send('Error generating PDF');
    }
});

// Endpoint to clear stored comments
app.post('/clear-comments', (req, res) => {
    storedComments.length = 0;
    res.json({ success: true, message: 'Comments cleared' });
});

// Endpoint to get comment count
app.get('/comments-count', (req, res) => {
    res.json({ count: storedComments.length });
});

// Serve frontend files
app.use(express.static('public'));

// Start http listener
const port = process.env.PORT || 8081;
httpServer.listen(port);
console.info(`Server running! Please visit http://localhost:${port}`);
console.info(`Backend namespace ready at /backend for FastAPI communication`);