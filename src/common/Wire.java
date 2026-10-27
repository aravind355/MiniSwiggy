package common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** Small helpers for sending and receiving one-line messages over TCP sockets. */
public final class Wire {
    private Wire() {}

    /** Upper limit for opening a connection. A dead node should be noticed quickly. */
    public static final int CONNECT_TIMEOUT_MS = 500;

    public static BufferedReader reader(Socket s) throws IOException {
        return new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
    }

    public static BufferedWriter writer(Socket s) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
    }

    /** Writes one message as one line. Threads sharing a writer must synchronize around this call. */
    public static void send(BufferedWriter out, Message m) throws IOException {
        out.write(m.encode());
        out.write('\n');
        out.flush();
    }

    /** Reads the next message, skipping blank lines. Returns null when the other side closed the connection. */
    public static Message receive(BufferedReader in) throws IOException {
        String line;
        do {
            line = in.readLine();
            if (line == null) return null;
        } while (line.isBlank());
        return Message.decode(line);
    }

    /**
     * Opens a fresh connection, sends one request and waits for one reply.
     * Throws IOException if the other side is down, refuses, or is slower than timeoutMs.
     */
    public static Message call(String host, int port, Message request, int timeoutMs) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), Math.min(timeoutMs, CONNECT_TIMEOUT_MS));
            s.setSoTimeout(timeoutMs);
            send(writer(s), request);
            Message reply = receive(reader(s));
            if (reply == null) throw new EOFException("connection closed by " + host + ":" + port);
            return reply;
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed reply from " + host + ":" + port, e);
        }
    }

    /** Splits "host:port" into its two parts. */
    public static String[] hostPort(String text) {
        int colon = text.lastIndexOf(':');
        if (colon <= 0) throw new IllegalArgumentException("expected host:port but got '" + text + "'");
        return new String[] {text.substring(0, colon), text.substring(colon + 1)};
    }
}
