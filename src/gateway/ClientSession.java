package gateway;

import common.Message;
import common.Wire;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;

/** One connected client. Replies and pushed events share the socket, so sending is synchronized. */
final class ClientSession {
    final Socket socket;
    final BufferedReader in;
    private final BufferedWriter out;
    volatile String role;
    volatile String id;

    ClientSession(Socket socket) throws IOException {
        this.socket = socket;
        this.in = Wire.reader(socket);
        this.out = Wire.writer(socket);
    }

    synchronized void send(Message m) throws IOException {
        Wire.send(out, m);
    }

    String name() {
        return id == null ? socket.getRemoteSocketAddress().toString() : role + " " + id;
    }
}
