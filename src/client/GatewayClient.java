package client;

import common.Message;
import common.Protocol;
import common.Wire;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A client's single TCP connection to the gateway. The socket carries two kinds of traffic:
 * <ul>
 *   <li>request/reply pairs, matched by reqId, so several threads may call request() at once;</li>
 *   <li>EVENT messages pushed by the gateway, which go through an {@link OrderedEventBuffer}
 *       so each order's updates are handed to the listener in sequence order.</li>
 * </ul>
 * The listener runs on its own thread, so it may call request() itself.
 */
public final class GatewayClient implements Closeable {
    private static final long REQUEST_TIMEOUT_MS = 20000; // longer than the gateway's failover retry window
    private static final long MAX_HOLD_MS = 1500;

    private final String id;
    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;
    private final String reqPrefix;
    private final AtomicLong counter = new AtomicLong();
    private final Map<String, CompletableFuture<Message>> waiting = new ConcurrentHashMap<>();
    private final OrderedEventBuffer ordering = new OrderedEventBuffer(MAX_HOLD_MS);
    private final ExecutorService eventThread = Executors.newSingleThreadExecutor(daemon("events"));
    private final ScheduledExecutorService gapTimer = Executors.newSingleThreadScheduledExecutor(daemon("gaps"));
    private volatile Consumer<Message> listener = e -> { };
    private volatile boolean closed;

    private GatewayClient(String id, Socket socket) throws IOException {
        this.id = id;
        this.socket = socket;
        this.in = Wire.reader(socket);
        this.out = Wire.writer(socket);
        // reqIds must be unique across restarts too: the node uses them to spot retried orders
        this.reqPrefix = id + "-" + Long.toString(System.currentTimeMillis(), 36) + "-";
        Thread reader = new Thread(this::readLoop, id + "-reader");
        reader.setDaemon(true);
        reader.start();
        gapTimer.scheduleWithFixedDelay(() -> deliver(ordering.expire(System.currentTimeMillis())),
            250, 250, TimeUnit.MILLISECONDS);
    }

    /** Connects to "host:port" and introduces this client with HELLO. */
    public static GatewayClient connect(String hostPort, String role, String id) throws IOException {
        String[] hp = Wire.hostPort(hostPort);
        Socket s = new Socket();
        s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 3000);
        GatewayClient c = new GatewayClient(id, s);
        Message hello = c.request(Message.of(Protocol.HELLO).put("role", role).put("id", id));
        if (!hello.isOk()) {
            c.close();
            throw new IOException("gateway refused HELLO: " + hello.get("reason"));
        }
        return c;
    }

    public String id() {
        return id;
    }

    public void onEvent(Consumer<Message> listener) {
        this.listener = listener;
    }

    /** Sends a request and waits for its reply (OK or ERROR). Throws IOException if the gateway is unreachable. */
    public Message request(Message req) throws IOException {
        String reqId = reqPrefix + counter.incrementAndGet();
        req.put("reqId", reqId);
        CompletableFuture<Message> reply = new CompletableFuture<>();
        waiting.put(reqId, reply);
        try {
            synchronized (out) {
                Wire.send(out, req);
            }
            return reply.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IOException("no reply from gateway within " + REQUEST_TIMEOUT_MS + " ms");
        } catch (ExecutionException e) {
            throw new IOException(e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        } finally {
            waiting.remove(reqId);
        }
    }

    /** Tells the ordering buffer the current version of an order we just started following. */
    public void follow(String orderId, int version) {
        deliver(ordering.startAt(orderId, version, System.currentTimeMillis()));
    }

    public OrderedEventBuffer ordering() {
        return ordering;
    }

    private void readLoop() {
        try {
            Message m;
            while ((m = Wire.receive(in)) != null) {
                if (Protocol.EVENT.equals(m.type())) {
                    deliver(ordering.offer(m, System.currentTimeMillis()));
                } else {
                    CompletableFuture<Message> f = waiting.get(m.get("reqId", ""));
                    if (f != null) f.complete(m);
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            // fall through: connection is gone or broken
        }
        if (!closed) System.out.println("[" + id + "] connection to the gateway was lost");
        IOException lost = new IOException("connection to gateway lost");
        for (CompletableFuture<Message> f : waiting.values()) f.completeExceptionally(lost);
    }

    private void deliver(List<Message> events) {
        for (Message e : events) {
            eventThread.execute(() -> {
                try {
                    listener.accept(e);
                } catch (RuntimeException ex) {
                    System.out.println("[" + id + "] event handler failed: " + ex);
                }
            });
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closed
        }
        gapTimer.shutdownNow();
        eventThread.shutdown();
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
