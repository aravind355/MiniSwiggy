package tests;

import client.GatewayClient;
import client.OrderedEventBuffer;
import common.Log;
import common.Message;
import common.Order;
import common.OrderStatus;
import common.Protocol;
import common.Wire;
import gateway.Gateway;
import node.OrderNode;
import node.OrderStore;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Automated tests, written without JUnit so the project has no dependencies.
 * Cluster tests start two OrderNodes and a Gateway inside this JVM on free ports.
 *
 * Run: java -cp out tests.TestRunner [--verbose]
 * The cluster tests run in order and build on each other (the failover test
 * leaves N2 as primary, the recovery test brings N1 back).
 */
public final class TestRunner {
    private static int passed;
    private static final List<String> failed = new ArrayList<>();

    interface TestBody {
        void run() throws Exception;
    }

    public static void main(String[] args) throws Exception {
        Log.setEnabled(args.length > 0 && args[0].equals("--verbose"));
        System.out.println("MiniSwiggy automated tests");
        System.out.println("--------------------------");

        test("protocol: encode/decode keeps special characters", TestRunner::messageRoundTrip);
        test("store: status moves forward only, and only by the right actor", TestRunner::statusRules);
        test("store: applying an older replica version is ignored", TestRunner::replicaVersioning);
        test("ordering: hold-back buffer re-orders, drops duplicates, skips lost gaps", TestRunner::orderingBuffer);

        try (Cluster c = Cluster.start(0)) {
            test("replication: an order placed via the gateway is on BOTH nodes", () -> replication(c));
            test("load balancing: reads are spread round-robin over N1 and N2", () -> roundRobin(c));
            test("concurrency: 20 customers at once get 20 unique orders; replicas identical", () -> concurrent(c));
            test("idempotency: a retried PLACE_ORDER (same reqId) is applied once", () -> idempotent(c));
            test("roles: the backup refuses writes with NOT_PRIMARY", () -> backupRefusesWrites(c));
            test("fencing: REPLICATE from an old epoch is refused", () -> staleEpoch(c));
            test("pub/sub: customer receives all 6 live updates, seq 1..6 in order", () -> lifecycle(c));
            test("FAILOVER: kill primary N1 -> detected by heartbeats, N2 promoted, no data lost", () -> failover(c));
            test("RECOVERY: restarted N1 re-syncs from N2 and rejoins as BACKUP", () -> recovery(c));
        }
        try (Cluster c = Cluster.start(300)) {
            test("ordering: with 0-300 ms network jitter, updates are still shown in order", () -> jitterOrdering(c));
        }

        System.out.println("--------------------------");
        System.out.printf("%d passed, %d failed%n", passed, failed.size());
        for (String f : failed) System.out.println("  FAILED: " + f);
        System.exit(failed.isEmpty() ? 0 : 1);
    }

    private static void test(String name, TestBody body) {
        long t0 = System.currentTimeMillis();
        try {
            body.run();
            passed++;
            System.out.printf("[PASS] %s (%d ms)%n", name, System.currentTimeMillis() - t0);
        } catch (Throwable e) {
            failed.add(name + " -> " + e);
            System.out.printf("[FAIL] %s%n       %s%n", name, e);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    interface Condition {
        boolean holds() throws Exception;
    }

    private static void waitFor(Condition c, long timeoutMs, String what) throws Exception {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (c.holds()) return;
            Thread.sleep(100);
        }
        throw new AssertionError("timed out waiting for: " + what);
    }

    // ------------------------------------------------------------------ unit tests

    private static void messageRoundTrip() {
        Message m = Message.of("TEST").put("text", "a|b=c d%e\nnew line").put("empty", "").put("n", 42);
        Message back = Message.decode(m.encode());
        check(back.type().equals("TEST"), "type lost");
        check(back.get("text").equals("a|b=c d%e\nnew line"), "text changed: " + back.get("text"));
        check(back.get("empty").isEmpty(), "empty value lost");
        check(back.getInt("n", 0) == 42, "number lost");
        check(!m.encode().contains("\n"), "encoded message must be one line");
    }

    private static void statusRules() throws Exception {
        OrderStore s = new OrderStore();
        Order o = s.preparePlace("req-1", "C1", "R1", "R1-M1x2").order();
        s.apply(o);
        check(o.totalRupees == 640, "price should be 2 x 320");
        expectError(() -> s.prepareStatus(o.id, OrderStatus.READY, "R1"), Protocol.INVALID_TRANSITION);
        expectError(() -> s.prepareStatus(o.id, OrderStatus.ACCEPTED, "R2"), Protocol.NOT_ALLOWED);
        s.apply(s.prepareStatus(o.id, OrderStatus.ACCEPTED, "R1").order());
        s.apply(s.prepareStatus(o.id, OrderStatus.READY, "R1").order());
        expectError(() -> s.prepareStatus(o.id, OrderStatus.PICKED_UP, "RD1"), Protocol.NOT_ALLOWED); // no rider yet
        s.apply(s.prepareClaim("RD1").order());
        expectError(() -> s.prepareStatus(o.id, OrderStatus.PICKED_UP, "RD2"), Protocol.NOT_ALLOWED);
        s.apply(s.prepareStatus(o.id, OrderStatus.PICKED_UP, "RD1").order());
        s.apply(s.prepareStatus(o.id, OrderStatus.DELIVERED, "RD1").order());
        check(s.get(o.id).status == OrderStatus.DELIVERED && s.get(o.id).version == 6, "should be DELIVERED v6");
        expectError(() -> s.preparePlace("req-2", "C1", "R1", "R2-M1x1"), Protocol.BAD_REQUEST); // wrong menu
    }

    interface StoreCall {
        void run() throws OrderStore.StoreException;
    }

    private static void expectError(StoreCall call, String code) {
        try {
            call.run();
        } catch (OrderStore.StoreException e) {
            check(e.code.equals(code), "expected " + code + " but got " + e.code);
            return;
        }
        throw new AssertionError("expected error " + code + " but the call succeeded");
    }

    private static void replicaVersioning() throws Exception {
        OrderStore s = new OrderStore();
        Order v1 = s.preparePlace("r", "C1", "R2", "R2-M1x1").order();
        Order v2 = v1.copy();
        v2.status = OrderStatus.ACCEPTED;
        v2.version = 2;
        check(s.apply(v2), "v2 should be stored");
        check(!s.apply(v1), "v1 must be ignored after v2");
        check(!s.apply(v2), "applying v2 twice must be a no-op");
        check(s.get(v1.id).version == 2, "store should keep v2");
    }

    private static void orderingBuffer() {
        OrderedEventBuffer b = new OrderedEventBuffer(1500);
        check(seqs(b.offer(ev("O-1", 1), 0)).equals(List.of(1)), "seq 1 should pass");
        check(seqs(b.offer(ev("O-1", 3), 10)).isEmpty(), "seq 3 must wait for seq 2");
        List<Message> out = b.offer(ev("O-1", 2), 20);
        check(seqs(out).equals(List.of(2, 3)), "seq 2 should release 2 and 3, got " + seqs(out));
        check(out.get(1).has("heldBack"), "seq 3 should be marked as held back");
        check(b.offer(ev("O-1", 2), 30).isEmpty(), "duplicate must be dropped");
        check(b.offer(ev("O-1", 5), 1000).isEmpty(), "seq 5 must wait for seq 4");
        check(b.expire(2000).isEmpty(), "gap should not be skipped before 1500 ms");
        out = b.expire(2600);
        check(seqs(out).equals(List.of(5)), "after the timeout seq 5 should be delivered");
        check("4".equals(out.get(0).get("skipped")), "the skipped seq should be reported");
        check(b.duplicates() == 1 && b.reordered() == 1 && b.skipped() == 1, "counters wrong");
        // a rider that starts following an order at version 4
        check(b.offer(ev("O-2", 4), 0).isEmpty(), "unknown order starting at 4 is held");
        check(seqs(b.startAt("O-2", 4, 0)).equals(List.of(4)), "startAt(4) should release seq 4");
    }

    private static Message ev(String orderId, int seq) {
        return Message.of(Protocol.EVENT).put("orderId", orderId).put("seq", seq);
    }

    private static List<Integer> seqs(List<Message> events) {
        List<Integer> out = new ArrayList<>();
        for (Message m : events) out.add(m.getInt("seq", -1));
        return out;
    }

    // ------------------------------------------------------------------ cluster tests

    private static void replication(Cluster c) throws Exception {
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-C1")) {
            Message r = cust.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", "R1").put("items", "R1-M1x1"));
            check(r.isOk(), "order failed: " + r);
            String id = r.get("orderId");
            Message onN1 = c.node(c.p1, Message.of(Protocol.GET_ORDER).put("orderId", id));
            Message onN2 = c.node(c.p2, Message.of(Protocol.GET_ORDER).put("orderId", id));
            check(onN1.isOk() && onN2.isOk(), "order must exist on both nodes");
            check(onN1.get("version").equals(onN2.get("version")), "versions differ");
        }
    }

    private static void roundRobin(Cluster c) throws Exception {
        Map<String, Integer> served = new HashMap<>();
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-RR")) {
            for (int i = 0; i < 10; i++) {
                Message r = cust.request(Message.of(Protocol.GET_MENU).put("restaurant", "R2"));
                check(r.isOk(), "read failed");
                served.merge(r.get("servedBy"), 1, Integer::sum);
            }
        }
        check(served.getOrDefault("N1", 0) >= 4 && served.getOrDefault("N2", 0) >= 4,
            "reads should be split between N1 and N2, got " + served);
    }

    private static void concurrent(Cluster c) throws Exception {
        int n = 20;
        Set<String> ids = ConcurrentHashMap.newKeySet();
        List<Throwable> errors = new ArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String name = "T-CC" + i;
            Thread t = new Thread(() -> {
                try (GatewayClient cust = c.client(Protocol.CUSTOMER, name)) {
                    go.await(); // all customers order at the same moment
                    Message r = cust.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", "R3").put("items", "R3-M2x1"));
                    if (!r.isOk()) throw new AssertionError(r.toString());
                    ids.add(r.get("orderId"));
                } catch (Throwable e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                }
            });
            threads.add(t);
            t.start();
        }
        go.countDown();
        for (Thread t : threads) t.join();
        check(errors.isEmpty(), "client errors: " + errors);
        check(ids.size() == n, "expected " + n + " unique ids, got " + ids.size());
        Message s1 = c.node(c.p1, Message.of(Protocol.STATS));
        Message s2 = c.node(c.p2, Message.of(Protocol.STATS));
        check(s1.get("checksum").equals(s2.get("checksum")) && s1.get("orders").equals(s2.get("orders")),
            "replicas differ: " + s1 + " / " + s2);
    }

    private static void idempotent(Cluster c) throws Exception {
        Message req = Message.of(Protocol.PLACE_ORDER).put("reqId", "retry-test-1").put("customer", "T-ID")
            .put("restaurant", "R2").put("items", "R2-M3x1");
        int before = c.node(c.p1, Message.of(Protocol.STATS)).getInt("orders", -1);
        Message first = c.node(c.p1, req);
        Message second = c.node(c.p1, req);
        check(first.isOk() && second.isOk(), "both calls should succeed");
        check(first.get("orderId").equals(second.get("orderId")), "retry created a second order");
        check("true".equals(second.get("retry")), "second call should be recognised as a retry");
        int after = c.node(c.p1, Message.of(Protocol.STATS)).getInt("orders", -1);
        check(after == before + 1, "exactly one order should be added, before=" + before + " after=" + after);
        // another customer who happens to use the same reqId must get their own order
        Message other = c.node(c.p1, Message.of(Protocol.PLACE_ORDER).put("reqId", "retry-test-1")
            .put("customer", "T-ID2").put("restaurant", "R2").put("items", "R2-M3x1"));
        check(other.isOk() && !other.get("orderId").equals(first.get("orderId")),
            "same reqId from a different customer must create a new order");
    }

    private static void backupRefusesWrites(Cluster c) throws Exception {
        Message r = c.node(c.p2, Message.of(Protocol.PLACE_ORDER).put("reqId", "x").put("customer", "T")
            .put("restaurant", "R1").put("items", "R1-M1x1"));
        check(r.isError(Protocol.NOT_PRIMARY), "backup should refuse: " + r);
    }

    private static void staleEpoch(Cluster c) throws Exception {
        Order fake = new Order();
        fake.id = "O-999";
        fake.customerId = "X";
        fake.restaurantId = "R1";
        fake.items = "R1-M1x1";
        fake.status = OrderStatus.PLACED;
        fake.version = 1;
        Message rep = Message.of(Protocol.REPLICATE).put("from", "OLD-PRIMARY").put("epoch", 0);
        fake.writeTo(rep);
        Message r = c.node(c.p2, rep);
        check(r.isError(Protocol.STALE_EPOCH), "old epoch should be refused: " + r);
        check(c.node(c.p2, Message.of(Protocol.GET_ORDER).put("orderId", "O-999")).isError(Protocol.NOT_FOUND),
            "fenced write must not be stored");
    }

    private static void lifecycle(Cluster c) throws Exception {
        List<Integer> seen = new ArrayList<>();
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-LC");
             GatewayClient rest = c.client(Protocol.RESTAURANT, "R2");
             GatewayClient rider = c.client(Protocol.RIDER, "T-RD")) {
            cust.onEvent(e -> {
                synchronized (seen) {
                    seen.add(e.getInt("seq", -1));
                }
            });
            String id = runLifecycle(cust, rest, rider, "R2", "R2-M1x1");
            waitFor(() -> {
                synchronized (seen) {
                    return seen.size() >= 6;
                }
            }, 5000, "6 events for " + id);
            synchronized (seen) {
                check(seen.equals(List.of(1, 2, 3, 4, 5, 6)), "events out of order or missing: " + seen);
            }
        }
    }

    /** Drives one order through every state as fast as possible. Returns its id. */
    private static String runLifecycle(GatewayClient cust, GatewayClient rest, GatewayClient rider,
                                       String restaurant, String items) throws Exception {
        Message placed = cust.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", restaurant).put("items", items));
        check(placed.isOk(), "place failed: " + placed);
        String id = placed.get("orderId");
        cust.follow(id, placed.getInt("version", 1));
        check(status(rest, id, "ACCEPTED").isOk(), "accept failed");
        Message claim = rider.request(Message.of(Protocol.CLAIM_ORDER));
        check(claim.isOk() && id.equals(claim.get("orderId")), "rider should get " + id + ": " + claim);
        check(status(rest, id, "READY").isOk(), "ready failed");
        check(status(rider, id, "PICKED_UP").isOk(), "pickup failed");
        check(status(rider, id, "DELIVERED").isOk(), "deliver failed");
        return id;
    }

    private static Message status(GatewayClient who, String orderId, String status) throws IOException {
        return who.request(Message.of(Protocol.UPDATE_STATUS).put("orderId", orderId).put("status", status));
    }

    private static void failover(Cluster c) throws Exception {
        int ordersBefore = c.node(c.p2, Message.of(Protocol.STATS)).getInt("orders", -1);
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-FO")) {
            c.n1.stop(); // crash the primary
            long killedAt = System.currentTimeMillis();
            // This write arrives while the gateway still thinks N1 is alive; it must be held, then succeed on N2.
            Message r = cust.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", "R1").put("items", "R1-M3x2"));
            long tookMs = System.currentTimeMillis() - killedAt;
            check(r.isOk(), "write during failover failed: " + r);
            check("N2".equals(r.get("servedBy")), "write should be served by the new primary N2");
            Message cl = c.cluster();
            check("N2".equals(cl.get("primary")) && cl.getInt("epoch", 0) == 2, "N2 should be primary in epoch 2: " + cl);
            check("false".equals(cl.get("n0.up")), "N1 should be marked down");
            int ordersAfter = c.node(c.p2, Message.of(Protocol.STATS)).getInt("orders", -1);
            check(ordersAfter == ordersBefore + 1, "N2 lost data: before=" + ordersBefore + " after=" + ordersAfter);
            Message old = cust.request(Message.of(Protocol.GET_ORDER).put("orderId", "O-1"));
            check(old.isOk(), "an order written before the crash must still be readable");
            System.out.printf("       (write was held %d ms until failover completed)%n", tookMs);
        }
    }

    private static void recovery(Cluster c) throws Exception {
        c.n1 = new OrderNode("N1", "127.0.0.1", c.p1, "127.0.0.1", c.p2);
        c.n1.start(); // pulls a snapshot from N2 before answering heartbeats as ready
        waitFor(() -> {
            Message cl = c.cluster();
            return "true".equals(cl.get("n0.up")) && Protocol.BACKUP.equals(cl.get("n0.role"));
        }, 8000, "N1 back up as BACKUP");
        Message s1 = c.node(c.p1, Message.of(Protocol.STATS));
        Message s2 = c.node(c.p2, Message.of(Protocol.STATS));
        check(s1.get("checksum").equals(s2.get("checksum")), "N1 did not catch up: " + s1 + " / " + s2);
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-RC")) {
            Message r = cust.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", "R2").put("items", "R2-M2x1"));
            check(r.isOk(), "write after recovery failed");
            Message onN1 = c.node(c.p1, Message.of(Protocol.GET_ORDER).put("orderId", r.get("orderId")));
            check(onN1.isOk(), "new write should be replicated from N2 to the recovered N1");
            Set<String> servers = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < 6; i++) {
                servers.add(cust.request(Message.of(Protocol.LIST_RESTAURANTS)).get("servedBy"));
            }
            check(servers.contains("N1") && servers.contains("N2"), "reads should use both nodes again: " + servers);
        }
    }

    private static void jitterOrdering(Cluster c) throws Exception {
        List<Integer> seen = new ArrayList<>();
        AtomicReference<GatewayClient> custRef = new AtomicReference<>();
        try (GatewayClient cust = c.client(Protocol.CUSTOMER, "T-JT");
             GatewayClient rest = c.client(Protocol.RESTAURANT, "R3");
             GatewayClient rider = c.client(Protocol.RIDER, "T-RDJ")) {
            custRef.set(cust);
            cust.onEvent(e -> {
                synchronized (seen) {
                    seen.add(e.getInt("seq", -1));
                }
            });
            runLifecycle(cust, rest, rider, "R3", "R3-M1x1");
            waitFor(() -> {
                synchronized (seen) {
                    return seen.size() >= 6;
                }
            }, 5000, "6 events");
            synchronized (seen) {
                check(seen.equals(List.of(1, 2, 3, 4, 5, 6)), "events not in order: " + seen);
            }
            System.out.printf("       (%d event(s) arrived out of order and were re-sequenced)%n",
                custRef.get().ordering().reordered());
        }
    }

    // ------------------------------------------------------------------ test cluster

    /** Two nodes and a gateway inside this JVM. */
    static final class Cluster implements AutoCloseable {
        final int p1;
        final int p2;
        final int gwPort;
        OrderNode n1;
        OrderNode n2;
        Gateway gateway;

        private Cluster(int p1, int p2, int gwPort) {
            this.p1 = p1;
            this.p2 = p2;
            this.gwPort = gwPort;
        }

        static Cluster start(int jitterMs) throws Exception {
            Cluster c = new Cluster(freePort(), freePort(), freePort());
            c.n1 = new OrderNode("N1", "127.0.0.1", c.p1, "127.0.0.1", c.p2);
            c.n2 = new OrderNode("N2", "127.0.0.1", c.p2, "127.0.0.1", c.p1);
            c.n1.start();
            c.n2.start();
            c.gateway = new Gateway("127.0.0.1", c.gwPort,
                Gateway.parseNodes("N1=127.0.0.1:" + c.p1 + ",N2=127.0.0.1:" + c.p2), jitterMs);
            c.gateway.start();
            waitFor(() -> {
                Message cl = c.cluster();
                return "N1".equals(cl.get("primary")) && Protocol.PRIMARY.equals(cl.get("n0.role"))
                    && Protocol.BACKUP.equals(cl.get("n1.role"));
            }, 8000, "cluster start-up");
            return c;
        }

        GatewayClient client(String role, String id) throws IOException {
            return GatewayClient.connect("127.0.0.1:" + gwPort, role, id);
        }

        Message cluster() throws IOException {
            return Wire.call("127.0.0.1", gwPort, Message.of(Protocol.CLUSTER), 2000);
        }

        Message node(int port, Message req) throws IOException {
            return Wire.call("127.0.0.1", port, req, 3000);
        }

        @Override
        public void close() {
            gateway.stop();
            n1.stop();
            n2.stop();
        }

        private static int freePort() throws IOException {
            try (ServerSocket s = new ServerSocket(0)) {
                return s.getLocalPort();
            }
        }
    }
}
