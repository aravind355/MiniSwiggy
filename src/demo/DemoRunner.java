package demo;

import client.GatewayClient;
import common.Args;
import common.Message;
import common.Protocol;
import common.Wire;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Scripted end-to-end demo. Every node and client is a separate JVM process,
 * talking only over TCP sockets. The primary is stopped with a hard process
 * kill (like "kill -9"), not a clean shutdown.
 *
 * Steps: start cluster -> concurrent orders with live updates -> round-robin reads
 * -> kill the primary mid-order -> failover -> restart it -> re-sync -> consistency check.
 *
 * Usage: java -cp out demo.DemoRunner [--transcript logs/demo-transcript.txt]
 * Exit code 0 means every check passed.
 */
public final class DemoRunner {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String GATEWAY = "127.0.0.1:9000";
    private static final String[] N1_ARGS = {"--id", "N1", "--port", "9101", "--peer", "127.0.0.1:9102"};
    private static final String[] N2_ARGS = {"--id", "N2", "--port", "9102", "--peer", "127.0.0.1:9101"};

    private record Proc(String name, Process process) {}

    private final List<Proc> procs = new ArrayList<>();
    private final List<String> results = new ArrayList<>();
    private final PrintWriter transcript;
    private volatile boolean quiet;
    private boolean allPassed = true;

    private DemoRunner(Path transcriptFile) throws IOException {
        Files.createDirectories(transcriptFile.toAbsolutePath().getParent());
        transcript = new PrintWriter(Files.newBufferedWriter(transcriptFile, StandardCharsets.UTF_8), true);
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        DemoRunner demo = new DemoRunner(Path.of(a.get("transcript", "logs/demo-transcript.txt")));
        Runtime.getRuntime().addShutdownHook(new Thread(demo::killAll)); // also clean up on Ctrl+C
        try {
            demo.run();
        } catch (Exception e) {
            demo.say("DEMO ABORTED: " + e);
            demo.allPassed = false;
        } finally {
            demo.quiet = true;
            demo.killAll();
            demo.transcript.close();
        }
        System.exit(demo.allPassed ? 0 : 1);
    }

    private void run() throws Exception {
        banner("STEP 1", "Start order nodes N1, N2 and the gateway (3 separate processes)");
        start("N1", "node.OrderNode", N1_ARGS);
        start("N2", "node.OrderNode", N2_ARGS);
        Thread.sleep(1500);
        // --jitter-ms simulates network delay on pushed events, so re-ordering can be seen
        start("GATEWAY", "gateway.Gateway", "--port", "9000",
            "--nodes", "N1=127.0.0.1:9101,N2=127.0.0.1:9102", "--jitter-ms", "250");
        boolean up = waitCluster(c -> "N1".equals(c.get("primary")) && "true".equals(c.get("n0.up"))
            && "true".equals(c.get("n1.up")) && Protocol.BACKUP.equals(c.get("n1.role")), 15000);
        record("Cluster up: N1 elected PRIMARY, N2 is BACKUP", up);
        if (!up) return;

        banner("STEP 2", "Start restaurants R1, R2 and riders RD1, RD2 in auto mode (4 processes)");
        start("R1", "client.RestaurantClient", "--id", "R1", "--auto", "--prep-ms", "2000");
        start("R2", "client.RestaurantClient", "--id", "R2", "--auto", "--prep-ms", "2000");
        start("RD1", "client.RiderClient", "--id", "RD1", "--auto", "--ride-ms", "1500");
        start("RD2", "client.RiderClient", "--id", "RD2", "--auto", "--ride-ms", "1500");
        Thread.sleep(3000);

        banner("STEP 3", "Three customers order at the same time and get live updates (pub/sub, in order)");
        Proc c1 = start("C1", "client.CustomerClient", "--id", "C1", "--script",
            "menu R1; order R1 R1-M1x2,R1-M3x1; waitdelivered 45");
        Proc c2 = start("C2", "client.CustomerClient", "--id", "C2", "--script",
            "order R2 R2-M1x2,R2-M3x2; waitdelivered 45");
        Proc c3 = start("C3", "client.CustomerClient", "--id", "C3", "--script",
            "restaurants; order R1 R1-M2x1; waitdelivered 45");
        record("3 concurrent customers: every order DELIVERED", waitExit(60000, c1, c2, c3));

        banner("STEP 4", "Round-robin load balancing: 6 reads through the gateway");
        Map<String, Integer> served = readBurst("READER1");
        record("Round-robin: reads were served by both nodes " + served, served.size() == 2);

        banner("STEP 5", "FAILURE: hard-kill the primary N1 while orders are in flight");
        Proc c4 = start("C4", "client.CustomerClient", "--id", "C4", "--script",
            "order R2 R2-M2x2; waitdelivered 60");
        Proc c5 = start("C5", "client.CustomerClient", "--id", "C5", "--script",
            "order R1 R1-M1x1; waitdelivered 60");
        Thread.sleep(2500); // orders are placed and being cooked
        kill("N1");
        say("C6 orders right after the crash; the gateway must hold the write until failover");
        Proc c6 = start("C6", "client.CustomerClient", "--id", "C6", "--script",
            "order R2 R2-M1x1,R2-M3x1; waitdelivered 60");
        boolean failedOver = waitCluster(c -> "N2".equals(c.get("primary")) && "false".equals(c.get("n0.up")), 15000);
        Message cl = cluster();
        record("Heartbeats detected the crash; N2 promoted to PRIMARY (epoch " + cl.get("epoch") + ")",
            failedOver && cl.getInt("epoch", 0) == 2);
        record("Orders in flight and the order placed during the outage were all DELIVERED",
            waitExit(75000, c4, c5, c6));

        banner("STEP 6", "RECOVERY: restart N1; it pulls the orders it missed from N2 and rejoins as BACKUP");
        start("N1", "node.OrderNode", N1_ARGS);
        boolean rejoined = waitCluster(c -> "true".equals(c.get("n0.up"))
            && Protocol.BACKUP.equals(c.get("n0.role")), 15000);
        record("N1 restarted, re-synced and rejoined as BACKUP", rejoined);
        say("C7 orders after recovery: primary N2 now replicates to N1 again");
        Proc c7 = start("C7", "client.CustomerClient", "--id", "C7", "--script",
            "order R1 R1-M3x3; waitdelivered 45");
        record("Order after recovery DELIVERED", waitExit(60000, c7));
        Map<String, Integer> served2 = readBurst("READER2");
        record("Round-robin includes the recovered N1 again " + served2, served2.containsKey("N1")
            && served2.containsKey("N2"));

        banner("STEP 7", "Consistency check: ask both replicas directly");
        Message s1 = Wire.call("127.0.0.1", 9101, Message.of(Protocol.STATS), 3000);
        Message s2 = Wire.call("127.0.0.1", 9102, Message.of(Protocol.STATS), 3000);
        say(String.format("N1: role=%s orders=%s checksum=%s", s1.get("role"), s1.get("orders"), s1.get("checksum")));
        say(String.format("N2: role=%s orders=%s checksum=%s", s2.get("role"), s2.get("orders"), s2.get("checksum")));
        Message all = Wire.call("127.0.0.1", 9101, Message.of(Protocol.LIST_ORDERS), 3000);
        int delivered = 0;
        for (int i = 0; i < all.getInt("count", 0); i++) {
            String p = "o" + i + ".";
            say(String.format("  %-4s %-9s %s -> %s, rider %s, v%s", all.get(p + "orderId"), all.get(p + "status"),
                all.get(p + "restaurant"), all.get(p + "customer"), all.get(p + "rider"), all.get(p + "version")));
            if ("DELIVERED".equals(all.get(p + "status"))) delivered++;
        }
        record("Replicas identical: " + s1.get("orders") + " orders on each, checksum " + s1.get("checksum"),
            s1.get("checksum").equals(s2.get("checksum")) && s1.getInt("orders", 0) == 7);
        record("All 7 orders DELIVERED (none lost in the crash)", delivered == 7);

        banner("RESULT", "Summary of automatic checks");
        for (String r : results) say(r);
        say(allPassed ? "ALL CHECKS PASSED" : "SOME CHECKS FAILED");
    }

    // ------------------------------------------------------------------ helpers

    /** Starts one component as a separate JVM and streams its output into ours. */
    private Proc start(String name, String mainClass, String... args) throws IOException {
        List<String> cmd = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx64m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1",
            "-cp", System.getProperty("java.class.path"), mainClass));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        Proc proc = new Proc(name, p);
        synchronized (procs) {
            procs.add(proc);
        }
        Thread pump = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (!quiet) print(line);
                }
            } catch (IOException ignored) {
                // process ended
            }
        });
        pump.setDaemon(true);
        pump.start();
        say("started " + name + " (pid " + p.pid() + ")");
        return proc;
    }

    private void kill(String name) throws InterruptedException {
        synchronized (procs) {
            for (Proc p : procs) {
                if (p.name().equals(name) && p.process().isAlive()) {
                    say("!!! KILLING " + name + " (pid " + p.process().pid() + ") with a hard process kill !!!");
                    p.process().destroyForcibly();
                    p.process().waitFor();
                }
            }
        }
    }

    private void killAll() {
        synchronized (procs) {
            for (Proc p : procs) p.process().destroyForcibly();
        }
    }

    private boolean waitExit(long timeoutMs, Proc... list) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        boolean ok = true;
        for (Proc p : list) {
            long left = Math.max(1, end - System.currentTimeMillis());
            boolean exited = p.process().waitFor(left, TimeUnit.MILLISECONDS);
            if (!exited || p.process().exitValue() != 0) {
                say(p.name() + (exited ? " exited with code " + p.process().exitValue() : " did not finish in time"));
                ok = false;
            }
        }
        return ok;
    }

    private Message cluster() throws IOException {
        String[] hp = Wire.hostPort(GATEWAY);
        return Wire.call(hp[0], Integer.parseInt(hp[1]), Message.of(Protocol.CLUSTER), 2000);
    }

    private boolean waitCluster(Predicate<Message> condition, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            try {
                if (condition.test(cluster())) return true;
            } catch (IOException e) {
                // gateway not up yet
            }
            Thread.sleep(200);
        }
        return false;
    }

    /** Sends 6 reads through the gateway and counts which node answered each. */
    private Map<String, Integer> readBurst(String clientId) throws IOException {
        Map<String, Integer> served = new TreeMap<>();
        try (GatewayClient c = GatewayClient.connect(GATEWAY, Protocol.CUSTOMER, clientId)) {
            for (int i = 1; i <= 6; i++) {
                Message r = c.request(Message.of(Protocol.GET_MENU).put("restaurant", i % 2 == 0 ? "R2" : "R1"));
                String by = r.isOk() ? r.get("servedBy") : "ERROR";
                say(String.format("read %d (GET_MENU) served by %s", i, by));
                served.merge(by, 1, Integer::sum);
            }
        }
        return served;
    }

    private void record(String what, boolean ok) {
        String line = (ok ? "[PASS] " : "[FAIL] ") + what;
        results.add(line);
        if (!ok) allPassed = false;
        say("CHECK " + line);
    }

    private void banner(String step, String title) {
        print("");
        print("======================================================================");
        print(" " + step + ": " + title);
        print("======================================================================");
    }

    private void say(String text) {
        print(LocalTime.now().format(TIME) + " [DEMO] " + text);
    }

    private synchronized void print(String line) {
        System.out.println(line);
        System.out.flush();
        transcript.println(line);
    }
}
