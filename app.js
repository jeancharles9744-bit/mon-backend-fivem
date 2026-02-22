/**
 * ═══════════════════════════════════════════════════════════════
 *  ARK AC — Streaming Backend pour Render.com
 *  Recoit les frames du serveur FiveM et les diffuse via WebSocket
 *  au Web Panel pour l'affichage live
 * ═══════════════════════════════════════════════════════════════
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

// ═══ Configuration ═══
const PORT = process.env.PORT || 10000;
const MAX_FRAME_SIZE = 2 * 1024 * 1024; // 2MB max par frame

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
    maxHttpBufferSize: MAX_FRAME_SIZE,
    transports: ['websocket', 'polling'],
});

app.use(express.json({ limit: '5mb' }));

// ═══ Health check (Render l'utilise pour verifier que le service tourne) ═══
app.get('/', (req, res) => {
    res.json({ status: 'ok', service: 'ark-stream-backend', uptime: process.uptime() });
});

// ═══ Etat ═══
const activeStreams = new Map();
const streamLogs = [];

// ═══════════════════════════════════════════
//  API REST (recoit du serveur FiveM)
// ═══════════════════════════════════════════

// Recevoir un frame du serveur FiveM
app.post('/api/frame', (req, res) => {
    const { playerId, playerName, frame, quality, timestamp } = req.body;
    if (!playerId || !frame) {
        return res.status(400).json({ error: 'Missing playerId or frame' });
    }

    const stream = activeStreams.get(String(playerId));
    if (!stream) {
        activeStreams.set(String(playerId), {
            playerName: playerName || 'Unknown',
            quality: quality || 'medium',
            startTime: Date.now(),
            lastFrame: Date.now(),
        });
    } else {
        stream.lastFrame = Date.now();
    }

    // Diffuser le frame a tous les clients connectes qui regardent ce joueur
    io.to(`stream_${playerId}`).emit('frame', {
        playerId,
        playerName,
        frame,
        quality,
        timestamp: timestamp || Date.now(),
    });

    res.json({ ok: true });
});

// Recevoir des events du serveur FiveM
app.post('/api/event', (req, res) => {
    const { event, data } = req.body;

    if (event === 'stream_start') {
        activeStreams.set(String(data.playerId), {
            playerName: data.playerName,
            quality: data.quality || 'medium',
            staffId: data.staffId,
            startTime: Date.now(),
            lastFrame: Date.now(),
        });

        streamLogs.push({
            type: 'start',
            playerId: data.playerId,
            playerName: data.playerName,
            staffId: data.staffId,
            time: new Date().toISOString(),
        });

        io.emit('stream_update', { activeStreams: getActiveStreamsList() });
        console.log(`[STREAM] Started for player ${data.playerName} (${data.playerId})`);
    }

    if (event === 'stream_stop') {
        const stream = activeStreams.get(String(data.playerId));
        if (stream) {
            streamLogs.push({
                type: 'stop',
                playerId: data.playerId,
                playerName: stream.playerName,
                staffId: stream.staffId,
                time: new Date().toISOString(),
                duration: Math.floor((Date.now() - stream.startTime) / 1000),
            });
            activeStreams.delete(String(data.playerId));
        }
        io.emit('stream_update', { activeStreams: getActiveStreamsList() });
        console.log(`[STREAM] Stopped for player ${data.playerId}`);
    }

    res.json({ ok: true });
});

// Status endpoint
app.get('/api/status', (req, res) => {
    res.json({
        ok: true,
        activeStreams: getActiveStreamsList(),
        connectedClients: io.engine.clientsCount,
        uptime: process.uptime(),
    });
});

// Logs endpoint
app.get('/api/logs', (req, res) => {
    res.json({
        ok: true,
        logs: streamLogs.slice(-100),
    });
});

// ═══════════════════════════════════════════
//  WEBSOCKET (Panel se connecte ici)
// ═══════════════════════════════════════════

io.on('connection', (socket) => {
    console.log(`[WS] Client connected: ${socket.id}`);

    socket.emit('stream_update', { activeStreams: getActiveStreamsList() });

    socket.on('watch', (playerId) => {
        socket.join(`stream_${playerId}`);
        console.log(`[WS] ${socket.id} watching player ${playerId}`);

        const stream = activeStreams.get(String(playerId));
        socket.emit('watch_status', {
            playerId,
            active: !!stream,
            playerName: stream ? stream.playerName : null,
            quality: stream ? stream.quality : null,
        });
    });

    socket.on('unwatch', (playerId) => {
        socket.leave(`stream_${playerId}`);
    });

    socket.on('get_streams', () => {
        socket.emit('stream_update', { activeStreams: getActiveStreamsList() });
    });

    socket.on('disconnect', () => {
        console.log(`[WS] Client disconnected: ${socket.id}`);
    });
});

// ═══════════════════════════════════════════
//  CLEANUP: streams inactifs (pas de frame depuis 30s)
// ═══════════════════════════════════════════

setInterval(() => {
    const now = Date.now();
    for (const [playerId, stream] of activeStreams) {
        if (now - stream.lastFrame > 30000) {
            console.log(`[STREAM] Auto-cleanup: player ${playerId} (no frames for 30s)`);
            activeStreams.delete(playerId);
            io.emit('stream_update', { activeStreams: getActiveStreamsList() });
        }
    }
}, 10000);

// ═══ Helpers ═══
function getActiveStreamsList() {
    const list = [];
    for (const [playerId, stream] of activeStreams) {
        list.push({
            playerId,
            playerName: stream.playerName,
            quality: stream.quality,
            staffId: stream.staffId,
            startTime: stream.startTime,
            uptime: Math.floor((Date.now() - stream.startTime) / 1000),
        });
    }
    return list;
}

// ═══ Start ═══
server.listen(PORT, '0.0.0.0', () => {
    console.log(`[ARK Stream Backend] Running on port ${PORT}`);
});
