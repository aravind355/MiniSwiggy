package node;

import common.Args;
import common.Catalog;
import common.Log;
import common.Message;
import common.Order;
import common.OrderStatus;
import common.Protocol;
import common.Wire;
import node.OrderStore.Change;
import node.OrderStore.StoreException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static common.Protocol.error;

/**
 * One order-service node (a small stand-in for one server of Swiggy's order service).
 *
 * <ul>
 *   <li>Serves reads (restaurants, menus, orders) whatever its role is.</li>
 *   <li>Accepts writes only while it is PRIMARY. Each write is first copied to
 *       the peer node (synchronous primary-backup replication) and only then
 *       committed locally and acknowledged.</li>
 *   <li>On start-up it pulls a snapshot from its peer, so a restarted node
 *       catches up on everything it missed while it was down.</li>
 *   <li>The gateway decides who is PRIMARY and says so with SET_ROLE. Each
 *       decision has an epoch number; messages from an older epoch are refused
 *       (fencing), so an out-of-date primary cannot overwrite newer data.</li>
 * </ul>
 *
 * Usage: java -cp out node.OrderNode --id N1 --port 9101 --peer 127.0.0.1:9102 [--bind 127.0.0.1]
 */
public final class OrderNode {
    private static final int PEER_TIMEOUT_MS = 1500;

    private final String id;
    private final String bindHost;
    private final int port;
    private final String peerHost;
    private final int peerPort;

    private final OrderStore store = new OrderStore();
    private final Object writeLock = new Object(); // one write at a time: prepare -> replicate -> commit

    private volatile String role = Protocol.BACKUP; // every node starts as backup until the gateway decides
    private volatile int epoch = 0;
    private volatile boolean ready = false;         // true once the start-up sync is done
    private volatile boolean peerReachable = true;
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();

    private final ExecutorService pool = Executors.newCachedThreadPool(); // one thread per connection
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private ServerSocket server;
    private volatile boolean running;

    public OrderNode(String id, String bindHost, int port, String peerHost, int peerPort) {
        this.id = id;
        this.bindHost = bindHost;
        this.port = port;
        this.peerHost = peerHost;
        this.peerPort = peerPort;
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String[] peer = Wire.hostPort(a.require("peer"));
        OrderNode node = new OrderNode(a.require("id"), a.get("bind", "127.0.0.1"),
            a.getInt("port", 9101), peer[0], Integer.parseInt(peer[1]));
        node.start();
        // The accept loop runs on a non-daemon thread, so the JVM keeps running after main returns.
    }

    /** Opens the listening socket, catches up from the peer, then reports ready to heartbeats. */
    public void start() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(bindHost, port));
        running = true;
        pool.submit(this::acceptLoop);
        Log.info(id, "listening on %s:%d, peer is %s:%d", bindHost, port, peerHost, peerPort);
        syncFromPeer();
        ready = true;
    }

    /** Stops the node as if it crashed: listening socket and all connections are closed at once. */
    public void stop() {
        running = false;
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
        pool.shutdownNow();
        Log.info(id, "stopped");
    }

    public String role() {
        return role;
    }

    public int epoch() {
        return epoch;
    }

    public OrderStore store() {
        return store;
    }

    // ------------------------------------------------------------------ networking

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                connections.add(s);
                pool.submit(() -> serve(s));
            } catch (IOException e) {
                if (running) Log.warn(id, "accept failed: %s", e.getMessage());
            }
        }
    }

    /** Handles every request on one connection until the other side closes it. */
    private void serve(Socket s) {
        try (s) {
            BufferedReader in = Wire.reader(s);
            BufferedWriter out = Wire.writer(s);
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) continue;
                Message reply;
                String reqId = null;
                try {
                    Message req = Message.decode(line);
                    reqId = req.get("reqId");
                    reply = handle(req);
                } catch (IllegalArgumentException e) {
                    reply = error(Protocol.BAD_REQUEST, e.getMessage());
                }
                reply.put("reqId", reqId);
                Wire.send(out, reply);
            }
        } catch (IOException e) {
            // the other side went away; nothing to do
        } finally {
            connections.remove(s);
        }
    }

    private Message handle(Message req) {
        switch (req.type()) {
            case Protocol.PING:
                return Message.of(Protocol.PONG).put("node", id).put("role", role).put("epoch", epoch)
                    .put("ready", ready).put("orders", store.size());
            case Protocol.SET_ROLE:
                return setRole(req);
            case Protocol.STATS:
                return ok().put("epoch", epoch).put("orders", store.size()).put("reads", reads.get())
                    .put("writes", writes.get()).put("checksum", store.checksum());
            case Protocol.REPLICATE:
                return applyReplica(req);
            case Protocol.SYNC_REQUEST:
                return snapshot(req);
            default:
                if (Protocol.isRead(req.type())) return handleRead(req);
                if (Protocol.isWrite(req.type())) return handleWrite(req);
                return error(Protocol.BAD_REQUEST, "unknown message type " + req.type());
        }
    }

    private Message ok() {
        return Protocol.ok().put("servedBy", id).put("role", role);
    }

    // ------------------------------------------------------------------ roles and epochs

    private synchronized Message setRole(Message req) {
        int newEpoch = req.getInt("epoch", 0);
        String newRole = req.get("role", Protocol.BACKUP);
        if (newEpoch < epoch) return error(Protocol.STALE_EPOCH, id + " is already in epoch " + epoch);
        if (!newRole.equals(role)) {
            Log.info(id, "ROLE %s -> %s (epoch %d)", role, newRole, newEpoch);
        } else if (newEpoch != epoch) {
            Log.info(id, "ROLE stays %s, epoch %d -> %d", role, epoch, newEpoch);
        }
        epoch = newEpoch;
        role = newRole;
        return ok();
    }

    /** Accepts messages from the current or a newer epoch; seeing a newer one makes this node step down. */
    private synchronized boolean acceptEpoch(int senderEpoch) {
        if (senderEpoch < epoch) return false;
        if (senderEpoch > epoch) {
            if (Protocol.PRIMARY.equals(role)) {
                Log.warn(id, "saw epoch %d > my epoch %d: another node is primary, stepping down", senderEpoch, epoch);
                role = Protocol.BACKUP;
            }
            epoch = senderEpoch;
        }
        return true;
    }

    // ------------------------------------------------------------------ reads

    private Message handleRead(Message req) {
        reads.incrementAndGet();
        Message reply = ok();
        switch (req.type()) {
            case Protocol.LIST_RESTAURANTS: {
                int i = 0;
                for (Catalog.Restaurant r : Catalog.RESTAURANTS) {
                    reply.put("r" + i + ".id", r.id()).put("r" + i + ".name", r.name())
                        .put("r" + i + ".cuisine", r.cuisine());
                    i++;
                }
                return reply.put("count", i);
            }
            case Protocol.GET_MENU: {
                Catalog.Restaurant r = Catalog.find(req.get("restaurant", ""));
                if (r == null) return error(Protocol.NOT_FOUND, "no restaurant " + req.get("restaurant"));
                reply.put("restaurant", r.id()).put("name", r.name()).put("count", r.menu().size());
                for (int i = 0; i < r.menu().size(); i++) {
                    Catalog.MenuItem m = r.menu().get(i);
                    reply.put("m" + i + ".id", m.id()).put("m" + i + ".name", m.name())
                        .put("m" + i + ".price", m.price());
                }
                return reply;
            }
            case Protocol.GET_ORDER: {
                Order o = store.get(req.get("orderId", ""));
                if (o == null) return error(Protocol.NOT_FOUND, "no order " + req.get("orderId"));
                o.writeTo(reply);
                return reply;
            }
            default: { // LIST_ORDERS with optional filters
                String customer = req.get("customer");
                String restaurant = req.get("restaurant");
                String rider = req.get("rider");
                String status = req.get("status");
                List<Order> found = store.list(o ->
                    (customer == null || customer.equals(o.customerId))
                        && (restaurant == null || restaurant.equals(o.restaurantId))
                        && (rider == null || rider.equals(o.riderId))
                        && (status == null || status.equals(o.status.name())));
                for (int i = 0; i < found.size(); i++) found.get(i).writeTo(reply, "o" + i + ".");
                return reply.put("count", found.size());
            }
        }
    }

    // ------------------------------------------------------------------ writes and replication

    private Message handleWrite(Message req) {
        synchronized (writeLock) {
            if (!Protocol.PRIMARY.equals(role)) return error(Protocol.NOT_PRIMARY, id + " is " + role);
            Change change;
            try {
                switch (req.type()) {
                    case Protocol.PLACE_ORDER:
                        change = store.preparePlace(req.get("reqId", ""), req.get("customer", ""),
                            req.get("restaurant", ""), req.get("items", ""));
                        break;
                    case Protocol.UPDATE_STATUS:
                        change = store.prepareStatus(req.get("orderId", ""),
                            OrderStatus.parse(req.get("status")), req.get("actor", ""));
                        break;
                    default:
                        change = store.prepareClaim(req.get("actor", ""));
                }
            } catch (StoreException e) {
                return error(e.code, e.getMessage());
            }

            Order o = change.order();
            if (change.changed()) {
                // Replicate first: only a write the backup holds too is acknowledged to the client.
                String outcome = replicate(o);
                if (outcome == null) {
                    return error(Protocol.NOT_PRIMARY, id + " stepped down, a newer primary exists");
                }
                store.apply(o);
                writes.incrementAndGet();
                Log.info(id, "WRITE %-14s %s v%d %s (%s)", change.event(), o.id, o.version, o.status, outcome);
            }
            Message reply = ok().put("event", change.event()).put("retry", !change.changed());
            o.writeTo(reply);
            return reply;
        }
    }

    /**
     * Sends one order version to the peer and waits for its acknowledgement.
     * Returns a short note for the log, or null if the peer is in a newer epoch
     * (then this node is no longer the real primary and must not commit).
     * If the peer is down we keep going without a replica; it will catch up
     * with SYNC_REQUEST when it restarts.
     */
    private String replicate(Order o) {
        Message msg = Message.of(Protocol.REPLICATE).put("from", id).put("epoch", epoch);
        o.writeTo(msg);
        try {
            Message ack = Wire.call(peerHost, peerPort, msg, PEER_TIMEOUT_MS);
            if (ack.isError(Protocol.STALE_EPOCH)) {
                Log.warn(id, "peer refused my epoch %d: %s - stepping down", epoch, ack.get("reason"));
                role = Protocol.BACKUP;
                return null;
            }
            if (!peerReachable) Log.info(id, "peer is reachable again - replication resumed");
            peerReachable = true;
            return "replicated to peer";
        } catch (IOException e) {
            if (peerReachable) Log.warn(id, "peer unreachable (%s) - continuing WITHOUT a replica", e.getMessage());
            peerReachable = false;
            return "NOT replicated, peer down";
        }
    }

    /** Backup side of replication. */
    private Message applyReplica(Message req) {
        if (!acceptEpoch(req.getInt("epoch", 0))) {
            return error(Protocol.STALE_EPOCH, "sender epoch " + req.get("epoch") + " < my epoch " + epoch);
        }
        Order o = Order.readFrom(req);
        if (store.apply(o)) {
            Log.info(id, "REPLICA %s v%d %s (from %s)", o.id, o.version, o.status, req.get("from"));
        }
        return ok();
    }

    // ------------------------------------------------------------------ recovery

    /** Sends every order we hold, so a restarted peer can catch up. */
    private Message snapshot(Message req) {
        List<Order> all = store.list(o -> true);
        Message reply = ok().put("count", all.size()).put("epoch", epoch);
        for (int i = 0; i < all.size(); i++) all.get(i).writeTo(reply, "o" + i + ".");
        Log.info(id, "SYNC sent %d orders to %s", all.size(), req.get("from"));
        return reply;
    }

    /** Pulls the peer's orders. On a first start both stores are empty, so "peer not reachable" is normal. */
    private void syncFromPeer() {
        try {
            Message snap = Wire.call(peerHost, peerPort,
                Message.of(Protocol.SYNC_REQUEST).put("from", id), 3000);
            int count = snap.getInt("count", 0);
            int fresh = 0;
            for (int i = 0; i < count; i++) {
                if (store.apply(Order.readFrom(snap, "o" + i + "."))) fresh++;
            }
            Log.info(id, "RECOVERY: pulled %d orders from peer (%d new), store now has %d",
                count, fresh, store.size());
        } catch (IOException e) {
            Log.info(id, "peer not reachable at start-up (%s) - starting with an empty store", e.getMessage());
        }
    }
}
