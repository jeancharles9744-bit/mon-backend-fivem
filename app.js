/**
 * ═══════════════════════════════════════════════════════════════
 *  ARK AC — Streaming Backend v2 (Binary WebSocket + Token Auth)
 *
 *  Architecture:
 *  - Players (NUI) → Raw WebSocket binaire sur /stream
 *  - Staff (Panel) → Socket.IO sur /
 *  - Frames binaires pipe direct: Player WS → Staff Socket.IO room
 *  - Latence < 100ms, zero traitement des frames
 *
 *  Securite:
 *  - Handshake par token unique genere par le serveur FiveM
 *  - Tokens enregistres/revoques via API REST
 * ═══════════════════════════════════════════════════════════════
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebSocketServer } = require('ws');
const url = require('url');

// ═══ Configuration ═══
const PORT = process.env.PORT || 10000;

const app = express();
const server = http.createServer(app);

// ═══ Socket.IO pour le Panel (staff) ═══
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    maxHttpBufferSize: 5 * 1024 * 1024,
    transports: ['polling', 'websocket'],
    path: '/socket.io/',
});

// ═══ Raw WebSocket pour les joueurs (frames binaires) ═══
// Mode noServer: on gere manuellement le upgrade HTTP pour eviter
// tout conflit avec Socket.IO sur le meme port
const wss = new WebSocketServer({ noServer: true });

// Separation propre des upgrades WebSocket
server.on('upgrade', (request, socket, head) => {
    const pathname = new URL(request.url, 'http://localhost').pathname;

    if (pathname === '/stream') {
        // → Raw WS pour les joueurs (frames binaires)
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    }
    // Socket.IO gere ses propres upgrades sur /socket.io/ automatiquement
});

app.use(express.json({ limit: '1mb' }));

// ═══ Health check ═══
app.get('/', (req, res) => {
    res.json({
        status: 'ok',
        service: 'ark-stream-backend-v2',
        architecture: 'binary-ws + token-auth',
        uptime: process.uptime(),
        activeStreams: activeStreams.size,
        connectedPlayers: playerConnections.size,
        connectedStaff: io.engine.clientsCount,
    });
});

// ═══ Etat ═══
const validTokens = new Map();       // token -> { playerId, playerName, quality, staffId, created }
const activeStreams = new Map();      // playerId -> { playerName, quality, ws, startTime, lastFrame, fps, bytesSent }
const playerConnections = new Map();  // playerId -> ws
const streamLogs = [];
const streamStats = new Map();       // playerId -> { fpsCounter, currentFps, bytesPerSec, bytesCounter }

// ═══ FPS tracker par stream ═══
setInterval(() => {
    for (const [pid, stats] of streamStats) {
        stats.currentFps = stats.fpsCounter;
        stats.bytesPerSec = stats.bytesCounter;
        stats.fpsCounter = 0;
        stats.bytesCounter = 0;
    }
}, 1000);

// ═══════════════════════════════════════════
//  API REST — Token Management (FiveM server)
// ═══════════════════════════════════════════

// Enregistrer un token (appele par le serveur FiveM au demarrage d'un stream)
app.post('/api/auth/register', (req, res) => {
    const { token, playerId, playerName, quality, staffId } = req.body;
    if (!token || !playerId) {
        return res.status(400).json({ error: 'Missing token or playerId' });
    }

    validTokens.set(token, {
        playerId: String(playerId),
        playerName: playerName || 'Unknown',
        quality: quality || 'medium',
        staffId: staffId || '',
        created: Date.now(),
    });

    console.log(`[AUTH] Token registered for player ${playerName} (${playerId})`);
    res.json({ ok: true });
});

// Revoquer un token (appele par le serveur FiveM a l'arret d'un stream)
app.post('/api/auth/revoke', (req, res) => {
    const { playerId } = req.body;
    if (!playerId) return res.status(400).json({ error: 'Missing playerId' });

    const pid = String(playerId);

    // Supprimer tous les tokens pour ce joueur
    for (const [tok, data] of validTokens) {
        if (data.playerId === pid) validTokens.delete(tok);
    }

    // Fermer la connexion WS si existante
    const existingWs = playerConnections.get(pid);
    if (existingWs) {
        try { existingWs.close(1000, 'token revoked'); } catch (e) {}
        playerConnections.delete(pid);
    }

    // Nettoyer le stream
    activeStreams.delete(pid);
    streamStats.delete(pid);

    // Notifier le panel
    io.emit('stream_stop', { playerId: parseInt(playerId) });
    io.emit('stream_update', { activeStreams: getActiveStreamsList() });

    console.log(`[AUTH] Token revoked for player ${playerId}`);
    res.json({ ok: true });
});

// Status
app.get('/api/status', (req, res) => {
    res.json({
        ok: true,
        activeStreams: getActiveStreamsList(),
        connectedPlayers: playerConnections.size,
        connectedStaff: io.engine.clientsCount,
        validTokens: validTokens.size,
        uptime: process.uptime(),
    });
});

// Logs
app.get('/api/logs', (req, res) => {
    res.json({ ok: true, logs: streamLogs.slice(-100) });
});

// ═══════════════════════════════════════════
//  LEGACY: Support ancien POST /api/frame (compat)
// ═══════════════════════════════════════════
app.post('/api/frame', (req, res) => {
    const { playerId, playerName, frame, quality } = req.body;
    if (!playerId || !frame) return res.status(400).json({ error: 'Missing data' });

    const pid = String(playerId);
    if (!activeStreams.has(pid)) {
        activeStreams.set(pid, {
            playerName: playerName || 'Unknown',
            quality: quality || 'medium',
            startTime: Date.now(),
            lastFrame: Date.now(),
        });
    } else {
        activeStreams.get(pid).lastFrame = Date.now();
    }

    // Pipe vers les watchers (base64 legacy)
    io.to(`stream_${playerId}`).emit('frame', {
        playerId: parseInt(playerId),
        frame,
        quality,
        timestamp: Date.now(),
        binary: false,
    });

    res.json({ ok: true });
});

// Legacy event endpoint
app.post('/api/event', (req, res) => {
    const { event, data } = req.body;
    if (event === 'stream_start' && data) {
        activeStreams.set(String(data.playerId), {
            playerName: data.playerName,
            quality: data.quality || 'medium',
            staffId: data.staffId,
            startTime: Date.now(),
            lastFrame: Date.now(),
        });
        io.emit('stream_update', { activeStreams: getActiveStreamsList() });
    }
    if (event === 'stream_stop' && data) {
        activeStreams.delete(String(data.playerId));
        io.emit('stream_update', { activeStreams: getActiveStreamsList() });
    }
    res.json({ ok: true });
});

// ═══════════════════════════════════════════
//  RAW WEBSOCKET — Players (frames binaires)
// ═══════════════════════════════════════════

wss.on('connection', (ws, req) => {
    const params = new URL(req.url, 'http://localhost').searchParams;
    const token = params.get('token') || '';
    const reqPlayerId = params.get('playerId') || '';

    let authenticated = false;
    let playerId = '';
    let playerName = 'Unknown';

    // Timer d'auth: 10s pour s'authentifier sinon ferme
    const authTimeout = setTimeout(() => {
        if (!authenticated) {
            ws.close(4001, 'Auth timeout');
        }
    }, 10000);

    ws.on('message', (data, isBinary) => {
        // Premier message = handshake JSON d'auth
        if (!authenticated) {
            try {
                const msg = JSON.parse(data.toString());
                if (msg.type === 'auth') {
                    const tokenData = validTokens.get(msg.token || token);
                    if (!tokenData || tokenData.playerId !== String(msg.playerId || reqPlayerId)) {
                        ws.close(4003, 'Invalid token');
                        return;
                    }

                    authenticated = true;
                    playerId = tokenData.playerId;
                    playerName = tokenData.playerName;
                    clearTimeout(authTimeout);

                    // Fermer l'ancienne connexion si existante
                    const old = playerConnections.get(playerId);
                    if (old && old !== ws) {
                        try { old.close(1000, 'replaced'); } catch (e) {}
                    }

                    playerConnections.set(playerId, ws);

                    // Enregistrer le stream actif
                    activeStreams.set(playerId, {
                        playerName: playerName,
                        quality: tokenData.quality,
                        staffId: tokenData.staffId,
                        ws: ws,
                        startTime: Date.now(),
                        lastFrame: Date.now(),
                    });

                    // Stats tracker
                    streamStats.set(playerId, {
                        fpsCounter: 0,
                        currentFps: 0,
                        bytesCounter: 0,
                        bytesPerSec: 0,
                    });

                    // Log
                    streamLogs.push({
                        type: 'connect',
                        playerId: parseInt(playerId),
                        playerName,
                        time: new Date().toISOString(),
                    });

                    // Notifier le panel
                    io.emit('stream_update', { activeStreams: getActiveStreamsList() });

                    // Confirmer l'auth
                    ws.send(JSON.stringify({ type: 'auth_ok', playerId }));

                    console.log(`[WS] Player ${playerName} (${playerId}) authenticated — binary stream ready`);
                }
            } catch (e) {
                // Pas du JSON et pas encore auth → ignorer
            }
            return;
        }

        // Messages suivants: frames binaires
        if (isBinary && data.length > 100) {
            const stream = activeStreams.get(playerId);
            if (stream) stream.lastFrame = Date.now();

            const stats = streamStats.get(playerId);
            if (stats) {
                stats.fpsCounter++;
                stats.bytesCounter += data.length;
            }

            // ═══ PIPE BINAIRE DIRECT → Panel watchers ═══
            // Zero copie, zero traitement — vitesse maximale
            const room = `stream_${playerId}`;
            const sockets = io.sockets.adapter.rooms.get(room);
            if (sockets && sockets.size > 0) {
                io.to(room).emit('binaryFrame', {
                    playerId: parseInt(playerId),
                    frame: data,  // Buffer binaire — Socket.IO le gere nativement
                    timestamp: Date.now(),
                    fps: stats ? stats.currentFps : 0,
                    kbps: stats ? Math.round(stats.bytesPerSec / 1024) : 0,
                });
            }
        }
    });

    ws.on('close', () => {
        clearTimeout(authTimeout);
        if (authenticated && playerId) {
            playerConnections.delete(playerId);

            const stream = activeStreams.get(playerId);
            if (stream) {
                streamLogs.push({
                    type: 'disconnect',
                    playerId: parseInt(playerId),
                    playerName,
                    time: new Date().toISOString(),
                    duration: Math.floor((Date.now() - stream.startTime) / 1000),
                });
            }

            activeStreams.delete(playerId);
            streamStats.delete(playerId);

            io.emit('stream_stop', { playerId: parseInt(playerId) });
            io.emit('stream_update', { activeStreams: getActiveStreamsList() });

            console.log(`[WS] Player ${playerName} (${playerId}) disconnected`);
        }
    });

    ws.on('error', () => {});
});

// ═══ Ping players toutes les 5s pour mesurer latence ═══
setInterval(() => {
    for (const [pid, ws] of playerConnections) {
        if (ws.readyState === 1) {
            try {
                ws.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
            } catch (e) {}
        }
    }
}, 5000);

// ═══════════════════════════════════════════
//  SOCKET.IO — Panel Staff (viewers)
// ═══════════════════════════════════════════

io.on('connection', (socket) => {
    console.log(`[Panel] Staff connected: ${socket.id}`);

    socket.emit('stream_update', { activeStreams: getActiveStreamsList() });

    socket.on('watch', (playerId) => {
        const pid = String(playerId);
        socket.join(`stream_${pid}`);
        console.log(`[Panel] ${socket.id} watching player ${pid}`);

        const stream = activeStreams.get(pid);
        const stats = streamStats.get(pid);
        socket.emit('watch_status', {
            playerId: parseInt(pid),
            active: !!stream,
            playerName: stream ? stream.playerName : null,
            quality: stream ? stream.quality : null,
            fps: stats ? stats.currentFps : 0,
            kbps: stats ? Math.round(stats.bytesPerSec / 1024) : 0,
        });
    });

    socket.on('unwatch', (playerId) => {
        socket.leave(`stream_${playerId}`);
    });

    socket.on('get_streams', () => {
        socket.emit('stream_update', { activeStreams: getActiveStreamsList() });
    });

    socket.on('disconnect', () => {
        console.log(`[Panel] Staff disconnected: ${socket.id}`);
    });
});

// ═══════════════════════════════════════════
//  CLEANUP: streams inactifs + tokens expires
// ═══════════════════════════════════════════

setInterval(() => {
    const now = Date.now();

    // Streams sans frame depuis 30s
    for (const [pid, stream] of activeStreams) {
        if (now - stream.lastFrame > 30000) {
            console.log(`[CLEANUP] Player ${pid} — no frames for 30s`);
            activeStreams.delete(pid);
            streamStats.delete(pid);
            const ws = playerConnections.get(pid);
            if (ws) { try { ws.close(1000, 'timeout'); } catch (e) {} }
            playerConnections.delete(pid);
            io.emit('stream_stop', { playerId: parseInt(pid) });
            io.emit('stream_update', { activeStreams: getActiveStreamsList() });
        }
    }

    // Tokens > 1h non utilises
    for (const [tok, data] of validTokens) {
        if (now - data.created > 3600000 && !playerConnections.has(data.playerId)) {
            validTokens.delete(tok);
        }
    }
}, 10000);

// ═══ Helpers ═══
function getActiveStreamsList() {
    const list = [];
    for (const [pid, stream] of activeStreams) {
        const stats = streamStats.get(pid);
        list.push({
            playerId: parseInt(pid),
            playerName: stream.playerName,
            quality: stream.quality,
            staffId: stream.staffId,
            startTime: stream.startTime,
            uptime: Math.floor((Date.now() - stream.startTime) / 1000),
            fps: stats ? stats.currentFps : 0,
            kbps: stats ? Math.round(stats.bytesPerSec / 1024) : 0,
            connected: playerConnections.has(pid),
        });
    }
    return list;
}

// ═══ Start ═══
server.listen(PORT, '0.0.0.0', () => {
    console.log(`[ARK Stream Backend v2] Running on port ${PORT}`);
    console.log(`[ARK Stream Backend v2] Binary WebSocket: /stream`);
    console.log(`[ARK Stream Backend v2] Socket.IO Panel: /`);
});
