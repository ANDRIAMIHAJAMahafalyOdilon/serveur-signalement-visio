const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// Configuration de Socket.io avec CORS pour autoriser les connexions de l'APK mobile
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// ✅ Route de test — utile pour vérifier que le serveur est bien en ligne
// (Render affichera "Cannot GET /" sinon, ce qui prête à confusion)
app.get('/', (req, res) => {
    res.send('Serveur de signalement OK — en ligne et prêt.');
});

// ✅ Route de santé simple (utile pour un futur monitoring / ping)
app.get('/health', (req, res) => {
    res.json({ status: 'ok', rooms: rooms.size });
});

// Stockage global des salons en mémoire
const rooms = new Map();

io.on('connection', (socket) => {

    // --- Gestion des Salons (Rooms) ---
    socket.on('room:join', ({ roomId, username, isHost }) => {
        const roomExists = rooms.has(roomId);
        if (!isHost && !roomExists) {
            socket.emit('room:error', { message: "Cette réunion n'existe pas ou n'a pas encore démarré." });
            return;
        }

        socket.join(roomId);
        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        const room = rooms.get(roomId);

        const alreadyHasHost = Array.from(room.values()).some((p) => p.isHost);
        const finalIsHost = isHost && !alreadyHasHost;

        const others = Array.from(room.entries()).map(([id, p]) => ({ socketId: id, username: p.username }));
        socket.emit('room:participants', others);

        room.set(socket.id, { username, isHost: finalIsHost });
        socket.to(roomId).emit('room:user-joined', { socketId: socket.id, username });
        socket.data.roomId = roomId;
    });

    // --- Signalement WebRTC (Visioconférence) ---
    socket.on('webrtc:offer', ({ to, offer }) => io.to(to).emit('webrtc:offer', { from: socket.id, offer }));
    socket.on('webrtc:answer', ({ to, answer }) => io.to(to).emit('webrtc:answer', { from: socket.id, answer }));
    socket.on('webrtc:ice-candidate', ({ to, candidate }) => io.to(to).emit('webrtc:ice-candidate', { from: socket.id, candidate }));

    // --- Tableau Blanc Collaboratif ---
    socket.on('whiteboard:stroke-start', (stroke) => socket.to(socket.data.roomId).emit('whiteboard:stroke-start', stroke));
    socket.on('whiteboard:stroke-update', (payload) => socket.to(socket.data.roomId).emit('whiteboard:stroke-update', payload));
    socket.on('whiteboard:stroke-end', (payload) => socket.to(socket.data.roomId).emit('whiteboard:stroke-end', payload));
    socket.on('whiteboard:clear', () => socket.to(socket.data.roomId).emit('whiteboard:clear'));

    // --- Permissions de dessin ---
    // Un participant demande la permission de dessiner : on envoie la demande à l'hôte
    socket.on('whiteboard:request-draw', ({ roomId }) => {
        const room = rooms.get(roomId);
        if (!room) return;
        const requester = room.get(socket.id);
        if (!requester) return;

        // Trouver le socket de l'hôte dans la salle
        const hostEntry = Array.from(room.entries()).find(([, p]) => p.isHost);
        if (!hostEntry) return;
        const [hostSocketId] = hostEntry;

        io.to(hostSocketId).emit('whiteboard:draw-request', {
            socketId: socket.id,
            username: requester.username,
        });
    });

    // L'hôte autorise un participant à dessiner
    socket.on('whiteboard:allow-draw', ({ targetSocketId }) => {
        io.to(targetSocketId).emit('whiteboard:draw-granted');
    });

    // L'hôte refuse la demande de dessin
    socket.on('whiteboard:deny-draw', ({ targetSocketId }) => {
        io.to(targetSocketId).emit('whiteboard:draw-denied');
    });

    // --- Chat ---
    socket.on('chat:send', ({ roomId, sender, text }) => {
        const message = {
            id: Date.now().toString(36) + Math.random().toString(36).slice(2, 8),
            sender,
            text,
            timestamp: new Date().toISOString(),
        };
        io.to(roomId).emit('chat:message', message);
    });

    // --- Lever la main ---
    socket.on('hand:toggle', ({ roomId, raised }) => {
        const room = rooms.get(roomId);
        const username = room?.get(socket.id)?.username ?? 'Participant';
        io.to(roomId).emit('hand:update', { socketId: socket.id, username, raised });
    });

    // --- Déconnexion d'un participant ---
    socket.on('disconnect', () => {
        const roomId = socket.data.roomId;
        if (roomId && rooms.has(roomId)) {
            const room = rooms.get(roomId);
            const wasHost = room.get(socket.id)?.isHost;
            room.delete(socket.id);

            // ✅ Si l'hôte part, transfère le rôle au participant suivant (si présent)
            if (wasHost && room.size > 0) {
                const [nextId, nextParticipant] = Array.from(room.entries())[0];
                nextParticipant.isHost = true;
                room.set(nextId, nextParticipant);
                io.to(nextId).emit('room:you-are-host');
            }

            // Notification aux autres participants
            socket.to(roomId).emit('room:user-left', socket.id);

            // Nettoyage de la mémoire si le salon est vide
            if (room.size === 0) {
                rooms.delete(roomId);
            }
        }
    });
});

// Écoute du serveur sur le port attribué dynamiquement par la plateforme d'hébergement (Render/Railway) ou 5000 en local
const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Serveur de signalement en ligne sur le port ${PORT}`);
});