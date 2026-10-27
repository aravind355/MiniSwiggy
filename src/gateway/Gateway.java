package gateway;

import common.Args;
import common.Log;
import common.Message;
import common.Protocol;
import common.Wire;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static common.Protocol.error;

/**
 * The single entry point for all clients (like an API gateway / load balancer).
 *
 * <ul>
 *   <li><b>Load balancing:</b> reads go to the live order nodes in round-robin order;
 *       writes go to the current primary.</li>
 *   <li><b>Failure detection:</b> every second it sends PING to each node. A node that
 *       misses 3 heartbeats in a row is marked DOWN.</li>
 *   <li><b>Failover:</b> if the primary is DOWN, the first live node in the configured
 *       list is promoted, with a new epoch number. Writes that arrive during the
 *       switch are held and retried, so clients only notice a short delay.</li>
 *   <li><b>Pub/sub:</b> after each successful write it pushes an EVENT to the
 *       interested clients (see {@link PubSub}).</li>
 * </ul>
 *
 * Usage: java -cp out gateway.Gateway --port 9000 --nodes N1=127.0.0.1:9101,N2=127.0.0.1:9102
 *        [--bind 127.0.0.1] [--jitter-ms 0]
 */
public final class Gateway {
    static final String ME = "GATEWAY";
    static final int HEARTBEAT_EVERY_MS = 1000;
    static final int HEARTBEAT_TIMEOUT_MS = 500;
    static final int MISSES_BEFORE_DOWN = 3;
    static final int NODE_CALL_TIMEOUT_MS = 3000;
    static final int WRITE_RETRY_WINDOW_MS = 10000;

    /** What the gateway knows about one order node. */
    public static final class NodeInfo {
        final String id;
        final String host;
        final int port;
        volatile boolean up;
        volatile boolean everUp;
        volatile int missed;
        volatile String role = "?";
        volatile int epoch = -1;
        volatile int orders;

        NodeInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }
    }

    private final String bindHost;
    private final int port;
    private final List<NodeInfo> nodes;
    private final PubSub pubsub;
    private final AtomicInteger nextRead = new AtomicInteger();

    private volatile NodeInfo primary; // changed only inside reconcileRoles()
    private volatile int epoch = 0;

    private final ExecutorService clientPool = Executors.newCachedThreadPool(); // one thread per client
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private ServerSocket server;
    private volatile boolean running;

    public Gateway(String bindHost, int port, List<NodeInfo> nodes, int jitterMs) {
        this.bindHost = bindHost;
        this.port = port;
        this.nodes = nodes;
        this.pubsub = new PubSub(jitterMs);
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        Gateway g = new Gateway(a.get("bind", "127.0.0.1"), a.getInt("port", Protocol.DEFAULT_GATEWAY_PORT),
            parseNodes(a.get("nodes", "N1=127.0.0.1:9101,N2=127.0.0.1:9102")), a.getInt("jitter-ms", 0));
        g.start();
    }

    /** Parses "N1=host:port,N2=host:port". The order matters: earlier nodes are preferred as primary. */
    public static List<NodeInfo> parseNodes(String spec) {
        List<NodeInfo> list = new ArrayList<>();
        for (String part : spec.split(",")) {
            String[] idAndAddr = part.trim().split("=");
            String[] hp = Wire.hostPort(idAndAddr[1]);
            list.add(new NodeInfo(idAndAddr[0], hp[0], Integer.parseInt(hp[1])));
        }
        return list;
    }

    public void start() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(bindHost, port));
        running = true;
        StringBuilder names = new StringBuilder();
        for (NodeInfo n : nodes) names.append(n.id).append('@').append(n.host).append(':').append(n.port).append(' ');
        Log.info(ME, "listening on %s:%d, order nodes: %s", bindHost, port, names.toString().trim());
        heartbeat.scheduleWithFixedDelay(this::heartbeatRound, 0, HEARTBEAT_EVERY_MS, TimeUnit.MILLISECONDS);
        clientPool.submit(this::acceptLoop);
    }

    public void stop() {
        running = false;
        heartbeat.shutdownNow();
        try {
            server.close();
        } catch (IOException ignored) {
            // already closed
        }
        for (Socket s : connections) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
        clientPool.shutdownNow();
        pubsub.shutdown();
    }

    // ------------------------------------------------------------------ failure detection and failover

    private void heartbeatRound() {
        try {
            for (NodeInfo n : nodes) ping(n);
            reconcileRoles();
        } catch (RuntimeException e) {
            Log.warn(ME, "heartbeat round failed: %s", e); // never let the scheduler thread die
        }
    }

    private void ping(NodeInfo n) {
        try {
            Message pong = Wire.call(n.host, n.port, Message.of(Protocol.PING), HEARTBEAT_TIMEOUT_MS);
            n.role = pong.get("role", "?");
            n.epoch = pong.getInt("epoch", -1);
            n.orders = pong.getInt("orders", 0);
            if (n.up && n.missed > 0) Log.info(ME, "heartbeat from %s is back after %d miss(es)", n.id, n.missed);
            n.missed = 0;
            // A node that is still catching up (ready=false) does not get traffic yet.
            if (!n.up && "true".equals(pong.get("ready"))) {
                n.up = true;
                Log.info(ME, "NODE UP: %s %s (%d orders)", n.id, n.everUp ? "has RECOVERED" : "joined", n.orders);
                n.everUp = true;
            }
        } catch (IOException e) {
            n.missed++;
            if (n.up) {
                Log.warn(ME, "heartbeat to %s missed (%d/%d): %s", n.id, n.missed, MISSES_BEFORE_DOWN, e.getMessage());
                if (n.missed >= MISSES_BEFORE_DOWN) {
                    n.up = false;
                    Log.warn(ME, "FAILURE DETECTED: %s missed %d heartbeats in a row - marked DOWN",
                        n.id, MISSES_BEFORE_DOWN);
                }
            }
        }
    }

    /**
     * Makes sure exactly one live node is PRIMARY and every other live node is BACKUP.
     * The primary is kept as long as it is alive (no fail-back), which avoids needless switches.
     */
    private synchronized void reconcileRoles() {
        NodeInfo current = primary;
        if (current == null || !current.up) {
            NodeInfo next = null;
            for (NodeInfo n : nodes) {
                if (n.up) {
                    next = n;
                    break;
                }
            }
            if (next != null) {
                epoch++;
                if (current == null) {
                    Log.info(ME, "ELECTION: %s is the PRIMARY (epoch %d)", next.id, epoch);
                } else {
                    Log.warn(ME, "FAILOVER: primary %s is down -> promoting %s to PRIMARY (epoch %d)",
                        current.id, next.id, epoch);
                }
                primary = next;
            } else if (current != null) {
                Log.warn(ME, "no order node is alive - writes are unavailable");
                primary = null;
            }
        }
        for (NodeInfo n : nodes) {
            if (!n.up) continue;
            String want = n == primary ? Protocol.PRIMARY : Protocol.BACKUP;
            if (want.equals(n.role) && n.epoch == epoch) continue;
            try {
                Message reply = Wire.call(n.host, n.port,
                    Message.of(Protocol.SET_ROLE).put("role", want).put("epoch", epoch), NODE_CALL_TIMEOUT_MS);
                if (reply.isOk()) {
                    n.role = want;
                    n.epoch = epoch;
                    Log.info(ME, "told %s: you are %s (epoch %d)", n.id, want, epoch);
                }
            } catch (IOException ignored) {
                // the next heartbeat will notice if the node is gone
            }
        }
    }

    // ------------------------------------------------------------------ clients

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                connections.add(s);
                clientPool.submit(() -> serveClient(s));
            } catch (IOException e) {
                if (running) Log.warn(ME, "accept failed: %s", e.getMessage());
            }
        }
    }

    private void serveClient(Socket s) {
        ClientSession session;
        try {
            session = new ClientSession(s);
        } catch (IOException e) {
            return;
        }
        try (s) {
            String line;
            while ((line = session.in.readLine()) != null) {
                if (line.isBlank()) continue;
                Message reply;
                String reqId = null;
                try {
                    Message req = Message.decode(line);
                    reqId = req.get("reqId");
                    reply = handleClient(session, req);
                } catch (IllegalArgumentException e) {
                    reply = error(Protocol.BAD_REQUEST, e.getMessage());
                }
                reply.put("reqId", reqId);
                session.send(reply);
            }
        } catch (IOException e) {
            // client disconnected
        } finally {
            pubsub.removeSession(session);
            connections.remove(s);
            if (session.id != null) Log.info(ME, "client %s disconnected", session.name());
        }
    }

    private Message handleClient(ClientSession session, Message req) {
        String type = req.type();
        switch (type) {
            case Protocol.HELLO:
                return hello(session, req);
            case Protocol.SUBSCRIBE:
                pubsub.subscribe(req.get("topic", ""), session);
                return Protocol.ok();
            case Protocol.UNSUBSCRIBE:
                pubsub.unsubscribe(req.get("topic", ""), session);
                return Protocol.ok();
            case Protocol.CLUSTER:
                return clusterStatus();
            default:
                break;
        }
        if (session.id == null) return error(Protocol.BAD_REQUEST, "send HELLO first");
        if (Protocol.isRead(type)) return forwardRead(session, req);
        if (!Protocol.isWrite(type)) return error(Protocol.BAD_REQUEST, "unknown message type " + type);

        // The gateway, not the client, fills in who is acting, so a client cannot pretend to be someone else.
        if (type.equals(Protocol.PLACE_ORDER)) {
            if (!Protocol.CUSTOMER.equals(session.role)) return error(Protocol.NOT_ALLOWED, "only customers order");
            req.put("customer", session.id);
        } else {
            if (type.equals(Protocol.CLAIM_ORDER) && !Protocol.RIDER.equals(session.role)) {
                return error(Protocol.NOT_ALLOWED, "only riders claim orders");
            }
            req.put("actor", session.id);
        }
        Message reply = forwardWrite(session, req);
        if (reply.isOk()) publishChange(session, type, reply);
        return reply;
    }

    private Message hello(ClientSession session, Message req) {
        session.role = req.get("role", Protocol.CUSTOMER);
        session.id = req.get("id", "anonymous");
        if (Protocol.RESTAURANT.equals(session.role)) pubsub.subscribe("restaurant:" + session.id, session);
        if (Protocol.RIDER.equals(session.role)) pubsub.subscribe("riders", session);
        Log.info(ME, "client connected: %s", session.name());
        return Protocol.ok();
    }

    private List<NodeInfo> liveNodes() {
        List<NodeInfo> live = new ArrayList<>();
        for (NodeInfo n : nodes) {
            if (n.up) live.add(n);
        }
        return live;
    }

    /** Round-robin over live nodes; if the chosen node fails, try the next one. */
    private Message forwardRead(ClientSession session, Message req) {
        List<NodeInfo> live = liveNodes();
        if (live.isEmpty()) return error(Protocol.NO_NODE_AVAILABLE, "no order node is up");
        int start = Math.floorMod(nextRead.getAndIncrement(), live.size());
        for (int i = 0; i < live.size(); i++) {
            NodeInfo n = live.get((start + i) % live.size());
            try {
                Message reply = Wire.call(n.host, n.port, req, NODE_CALL_TIMEOUT_MS);
                Log.info(ME, "READ  %-16s from %-5s -> %s (round-robin)", req.type(), session.id, n.id);
                return reply;
            } catch (IOException e) {
                Log.warn(ME, "read on %s failed (%s) - trying the next node", n.id, e.getMessage());
            }
        }
        return error(Protocol.NO_NODE_AVAILABLE, "all order nodes failed");
    }

    /**
     * Sends a write to the primary. If the primary cannot be reached (for example it
     * just crashed), the write is held and retried until failover has picked a new
     * primary. Retrying is safe because nodes recognise repeated requests (same reqId
     * or same target status) and do not apply them twice.
     */
    private Message forwardWrite(ClientSession session, Message req) {
        long deadline = System.currentTimeMillis() + WRITE_RETRY_WINDOW_MS;
        boolean held = false;
        while (true) {
            NodeInfo p = primary;
            if (p != null && p.up) {
                try {
                    Message reply = Wire.call(p.host, p.port, req, NODE_CALL_TIMEOUT_MS);
                    if (!reply.isError(Protocol.NOT_PRIMARY)) {
                        String result = reply.isOk() ? "" : " = " + reply.get("code");
                        Log.info(ME, "WRITE %-16s from %-5s -> %s (primary)%s%s", req.type(), session.id, p.id,
                            result, held ? " - delivered after failover" : "");
                        return reply;
                    }
                    // The node has not received its new role yet; try again in a moment.
                } catch (IOException e) {
                    if (!held) {
                        Log.warn(ME, "write %s from %s to primary %s failed (%s) - holding it until failover",
                            req.type(), session.id, p.id, e.getMessage());
                    }
                    held = true;
                }
            } else if (!held) {
                Log.warn(ME, "no primary right now - holding %s from %s", req.type(), session.id);
                held = true;
            }
            if (System.currentTimeMillis() > deadline) {
                return error(Protocol.NO_NODE_AVAILABLE, "no primary available, please retry later");
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return error(Protocol.NO_NODE_AVAILABLE, "gateway is shutting down");
            }
        }
    }

    /** Pushes the new state of the order to everyone who follows it. */
    private void publishChange(ClientSession session, String type, Message reply) {
        String orderId = reply.get("orderId");
        // Subscribe the customer / rider before publishing, so they cannot miss this event or later ones.
        if (type.equals(Protocol.PLACE_ORDER) || type.equals(Protocol.CLAIM_ORDER)) {
            pubsub.subscribe("order:" + orderId, session);
        }
        Message event = Message.of(Protocol.EVENT)
            .put("event", reply.get("event"))
            .put("orderId", orderId)
            .put("seq", reply.get("version"))
            .put("status", reply.get("status"))
            .put("customer", reply.get("customer"))
            .put("restaurant", reply.get("restaurant"))
            .put("rider", reply.get("rider"))
            .put("total", reply.get("total"))
            .put("items", reply.get("items"));
        int n = pubsub.publish(event, "order:" + orderId, "restaurant:" + reply.get("restaurant"));
        Log.info(ME, "PUBLISH %s %s seq=%s -> %d subscriber(s)", orderId, reply.get("event"), reply.get("version"), n);

        if ("ACCEPTED".equals(reply.get("event"))) {
            // Job announcement for riders. It is not part of the order's numbered event stream.
            Message job = Message.of(Protocol.EVENT).put("kind", "JOB").put("orderId", orderId)
                .put("restaurant", reply.get("restaurant"));
            pubsub.publish(job, "riders");
        }
    }

    private Message clusterStatus() {
        NodeInfo p = primary;
        Message m = Protocol.ok().put("primary", p == null ? "none" : p.id).put("epoch", epoch)
            .put("count", nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            NodeInfo n = nodes.get(i);
            m.put("n" + i + ".id", n.id).put("n" + i + ".up", n.up).put("n" + i + ".role", n.role)
                .put("n" + i + ".orders", n.orders);
        }
        return m;
    }
}
