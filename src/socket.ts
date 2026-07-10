import { createServer } from "node:http";
import { Server } from "socket.io";

const wsServer = createServer()

// todo: Move origin value to config
const io = new Server(wsServer, {cors: {origin: "http://localhost:5173"}})

io.on("connection", (socket) => {
    console.log("Client connected", socket.id);

    socket.on("join", (data) => {
        socket.join(String(data.tenantId))

        console.log(io.of("/").adapter.rooms);
        
        socket.emit("join", { roomId: String(data.tenantId) });
    })
});

export default {
  wsServer,
  io,
};