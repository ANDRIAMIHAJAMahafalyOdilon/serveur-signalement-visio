const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// CORS restreint en production
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map((origin) => origin.trim()).filter(Boolean);
const io = new Server(server, {
    cors: {
        origin: ALLOWED_ORIGINS.length > 0 ? ALLOWED_ORIGINS : "*",
        methods: ["GET", "POST"]
    }
});

// Sanitise le texte: supprime les balises HTML et limite la longueur
function sanitizeText(text, maxLength = 2000) {
    if (typeof text !== 'string') return '';
    return text.replace(/<[^>]*>/g, '').slice(0, maxLength).trim();
}

// Valide qu'un socket est bien dans la même salle que la cible
function areInSameRoom(sender, targetId, rooms) {
    const senderRoom = sender.data.roomId;
    if (!senderRoom) return false;
    const room = rooms.get(senderRoom);
    return room && room.has(sender.id) && room.has(targetId);
}

// Rate limiting par socket (tokens par seconde)
function createRateLimiter(maxPerSecond) {
    let tokens = maxPerSecond;
    let lastRefill = Date.now();
    return () => {
        const now = Date.now();
        const elapsed = (now - lastRefill) / 1000;
        tokens = Math.min(maxPerSecond, tokens + elapsed * maxPerSecond);
        lastRefill = now;
        if (tokens < 1) return false;
        tokens--;
        return true;
    };
}

app.get('/', (req, res) => {
    res.send('Serveur de signalement OK — en ligne et prêt.');
});

app.get('/health', (req, res) => {
    res.json({ status: 'ok', rooms: rooms.size });
});

const rooms = new Map();
const whiteboards = new Map();
const ROOM_TTL_MS = 30 * 60 * 1000; // 30 minutes
const roomTimers = new Map();

function scheduleRoomCleanup(roomId) {
    if (roomTimers.has(roomId)) return;
    const timer = setTimeout(() => {
        const room = rooms.get(roomId);
        if (room && room.size === 0) {
            rooms.delete(roomId);
            whiteboards.delete(roomId);
            roomTimers.delete(roomId);
            console.log(`[CLEAN] Salon ${roomId} supprimé (TTL expiré)`);
        }
    }, ROOM_TTL_MS);
    roomTimers.set(roomId, timer);
}

function getAuthorizedRoom(socket) {
    const roomId = socket.data.roomId;
    if (!roomId) return null;
    const room = rooms.get(roomId);
    return room?.has(socket.id) ? { roomId, room } : null;
}

function isPoint(point) {
    return point && Number.isFinite(point.x) && Number.isFinite(point.y);
}

function isStroke(stroke) {
    return stroke && typeof stroke.id === 'string' && stroke.id.length <= 80 &&
        Array.isArray(stroke.points) && stroke.points.length > 0 && stroke.points.length <= 2_000 &&
        stroke.points.every(isPoint) && typeof stroke.color === 'string' && stroke.color.length <= 32 &&
        Number.isFinite(stroke.width) && stroke.width > 0 && stroke.width <= 100 &&
        typeof stroke.authorId === 'string' && stroke.authorId.length <= 50;
}

function publicStroke(stroke) {
    const { ownerId, ...safeStroke } = stroke;
    return safeStroke;
}

function cancelRoomCleanup(roomId) {
    if (roomTimers.has(roomId)) {
        clearTimeout(roomTimers.get(roomId));
        roomTimers.delete(roomId);
    }
}

io.on('connection', (socket) => {
    console.log(`[CONN] Nouveau client : ${socket.id}`);

    const rateLimiter = createRateLimiter(50);

    // --- Gestion des Salons (Rooms) ---
    socket.on('room:join', async (payload) => {
        try {
            let { roomId, username, isHost } = payload || {};
            if (typeof roomId !== 'string' || typeof username !== 'string' || typeof isHost !== 'boolean') {
                socket.emit('room:error', { message: 'Paramètres invalides.' });
                return;
            }
            const cleanRoomId = roomId.trim().slice(0, 10);
            const cleanUsername = sanitizeText(username, 50);
            if (!cleanRoomId || !cleanUsername) {
                socket.emit('room:error', { message: 'Nom de salle ou pseudo manquant.' });
                return;
            }
            roomId = cleanRoomId;
            username = cleanUsername;

            console.log(`[JOIN] ${username} tente de rejoindre ${roomId} (host: ${isHost})`);

            const roomExists = rooms.has(roomId);
            if (!isHost && !roomExists) {
                socket.emit('room:error', { message: "Cette réunion n'existe pas ou n'a pas encore démarré." });
                return;
            }

            const previousRoomId = socket.data.roomId;
            if (previousRoomId && previousRoomId !== roomId && rooms.has(previousRoomId)) {
                const previousRoom = rooms.get(previousRoomId);
                const previousUser = previousRoom.get(socket.id);
                previousRoom.delete(socket.id);
                await socket.leave(previousRoomId);
                socket.to(previousRoomId).emit('room:user-left', socket.id);
                if (previousUser?.isHost && previousRoom.size > 0) {
                    const [, nextParticipant] = Array.from(previousRoom.entries())[0];
                    nextParticipant.isHost = true;
                    io.to(Array.from(previousRoom.keys())[0]).emit('room:you-are-host');
                }
                if (previousRoom.size === 0) scheduleRoomCleanup(previousRoomId);
            }

            await socket.join(roomId);

            if (!rooms.has(roomId)) rooms.set(roomId, new Map());
            cancelRoomCleanup(roomId);
            const room = rooms.get(roomId);

            const alreadyHasHost = Array.from(room.values()).some((p) => p.isHost);
            const finalIsHost = isHost && !alreadyHasHost;

            const others = Array.from(room.entries()).map(([id, p]) => ({ socketId: id, username: p.username }));
            socket.data.roomId = roomId;
            room.set(socket.id, { username, isHost: finalIsHost });
            socket.emit('room:participants', others);
            socket.to(roomId).emit('room:user-joined', { socketId: socket.id, username });
            socket.emit('whiteboard:state', Array.from(whiteboards.get(roomId)?.values() ?? []).map(publicStroke));

            console.log(`[ROOM] ${username} a rejoint ${roomId}. Total: ${room.size}`);
        } catch (err) {
            console.error('[JOIN] Erreur:', err);
            socket.emit('room:error', { message: 'Erreur interne lors de la connexion.' });
        }
    });

    // --- Signalement WebRTC ---
    socket.on('webrtc:offer', (payload) => {
        if (!rateLimiter()) return;
        const { to, offer } = payload || {};
        if (typeof to !== 'string' || !offer || typeof offer.sdp !== 'string' || !areInSameRoom(socket, to, rooms)) return;
        io.to(to).emit('webrtc:offer', { from: socket.id, offer });
    });
    socket.on('webrtc:answer', (payload) => {
        if (!rateLimiter()) return;
        const { to, answer } = payload || {};
        if (typeof to !== 'string' || !answer || typeof answer.sdp !== 'string' || !areInSameRoom(socket, to, rooms)) return;
        io.to(to).emit('webrtc:answer', { from: socket.id, answer });
    });
    socket.on('webrtc:ice-candidate', (payload) => {
        if (!rateLimiter()) return;
        const { to, candidate } = payload || {};
        if (typeof to !== 'string' || !candidate || typeof candidate.candidate !== 'string' || !areInSameRoom(socket, to, rooms)) return;
        io.to(to).emit('webrtc:ice-candidate', { from: socket.id, candidate });
    });

    // --- Tableau Blanc Collaboratif ---
    socket.on('whiteboard:stroke-start', (payload) => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized || !isStroke(payload)) return;
        const { roomId } = authorized;
        if (!whiteboards.has(roomId)) whiteboards.set(roomId, new Map());
        const board = whiteboards.get(roomId);
        if (board.has(payload.id)) return;
        const stroke = { ...payload, authorId: authorized.room.get(socket.id).username, ownerId: socket.id };
        whiteboards.get(roomId).set(stroke.id, stroke);
        socket.to(roomId).emit('whiteboard:stroke-start', publicStroke(stroke));
    });
    socket.on('whiteboard:stroke-update', (payload) => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized || !payload || typeof payload.id !== 'string' ||
            !Array.isArray(payload.points) || payload.points.length > 500 || !payload.points.every(isPoint)) return;
        const stroke = whiteboards.get(authorized.roomId)?.get(payload.id);
        if (!stroke || stroke.ownerId !== socket.id ||
            stroke.points.length + payload.points.length > 10_000) return;
        stroke.points.push(...payload.points);
        socket.to(authorized.roomId).emit('whiteboard:stroke-update', { id: payload.id, points: payload.points });
    });
    socket.on('whiteboard:stroke-end', (payload) => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized || !payload || typeof payload.id !== 'string') return;
        socket.to(authorized.roomId).emit('whiteboard:stroke-end', { id: payload.id });
    });
    socket.on('whiteboard:clear', () => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized) return;
        if (!authorized.room.get(socket.id)?.isHost) {
            socket.emit('room:error', { message: "Seul l'hôte peut effacer le tableau." });
            return;
        }
        whiteboards.set(authorized.roomId, new Map());
        io.to(authorized.roomId).emit('whiteboard:clear');
    });

    // --- Chat ---
    socket.on('chat:send', (payload) => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized || !payload || typeof payload.text !== 'string') {
            console.warn(`[CHAT] Tentative d'envoi sans roomId (socket: ${socket.id})`);
            return;
        }

        const { roomId, room } = authorized;
        const sender = room.get(socket.id).username;
        const text = sanitizeText(payload.text, 2000);
        if (!text) return;

        const message = {
            id: crypto.randomUUID(),
            senderId: socket.id,
            sender,
            text,
            timestamp: new Date().toISOString(),
        };

        console.log(`[CHAT] [${roomId}] ${sender}: ${text.slice(0, 50)}`);
        io.in(roomId).emit('chat:message', message);
    });

    // --- Lever la main ---
    socket.on('hand:toggle', (payload) => {
        if (!rateLimiter()) return;
        const authorized = getAuthorizedRoom(socket);
        if (!authorized || !payload || typeof payload.raised !== 'boolean') return;

        const username = authorized.room.get(socket.id).username;
        io.to(authorized.roomId).emit('hand:update', { socketId: socket.id, username, raised: payload.raised });
    });

    // --- Déconnexion ---
    socket.on('disconnect', () => {
        const roomId = socket.data.roomId;
        if (roomId && rooms.has(roomId)) {
            const room = rooms.get(roomId);
            const user = room.get(socket.id);
            const wasHost = user?.isHost;

            room.delete(socket.id);
            console.log(`[QUIT] ${user?.username || socket.id} a quitté ${roomId}. Reste: ${room.size}`);

            if (wasHost && room.size > 0) {
                const [nextId, nextParticipant] = Array.from(room.entries())[0];
                nextParticipant.isHost = true;
                io.to(nextId).emit('room:you-are-host');
                console.log(`[HOST] Nouveau host pour ${roomId}: ${nextParticipant.username}`);
            }

            socket.to(roomId).emit('room:user-left', socket.id);

            if (room.size === 0) {
                scheduleRoomCleanup(roomId);
            }
        }
    });
});

// --- Graceful shutdown ---
function shutdown(signal) {
    console.log(`\n[SHUTDOWN] ${signal} reçu — fermeture gracieuse...`);
    io.emit('room:error', { message: 'Le serveur redémarre, veuillez vous reconnecter.' });
    io.close(() => {
        server.close(() => {
            // Nettoie les timers de room
            roomTimers.forEach((timer) => clearTimeout(timer));
            process.exit(0);
        });
    });
    // Force exit après 5 secondes
    setTimeout(() => process.exit(1), 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Serveur de signalement en ligne sur le port ${PORT}`);
});
