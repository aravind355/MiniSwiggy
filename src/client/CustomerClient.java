package client;

import common.Args;
import common.Message;
import common.Protocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The customer app: browse restaurants, place orders and watch them live.
 *
 * Usage: java -cp out client.CustomerClient --id C1 [--gateway 127.0.0.1:9000] [--script "cmd; cmd"]
 * With --script the commands run one after another and the program exits
 * with code 0 only if all of them succeeded (used by the demo).
 */
public final class CustomerClient extends ClientBase {
    private final Map<String, String> myOrders = new ConcurrentHashMap<>(); // orderId -> latest status

    CustomerClient(GatewayClient gw) {
        super(gw);
        gw.onEvent(this::onEvent);
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        GatewayClient gw = GatewayClient.connect(a.get("gateway", "127.0.0.1:9000"), Protocol.CUSTOMER,
            a.get("id", "C1"));
        CustomerClient c = new CustomerClient(gw);
        String script = a.get("script", null);
        if (script == null) {
            c.repl();
            return;
        }
        c.runScript(script);
        gw.close();
        System.exit(c.allOk ? 0 : 1);
    }

    @Override
    protected String helpText() {
        return String.join("\n",
            "Customer commands:",
            "  restaurants                      list restaurants",
            "  menu R1                          show a menu",
            "  order R1 R1-M1x2,R1-M3x1         place an order (item x quantity)",
            "  track O-1                        show one order",
            "  myorders                         list my orders",
            "  waitdelivered [seconds]          wait until all my orders are delivered",
            "  wait 5                           pause (live updates keep printing)",
            "  help | quit");
    }

    @Override
    protected void command(String[] w) throws IOException, InterruptedException {
        switch (w[0].toLowerCase()) {
            case "restaurants": {
                Message r = gw.request(Message.of(Protocol.LIST_RESTAURANTS));
                if (!check(r)) return;
                say("%d restaurants (read served by %s):", r.getInt("count", 0), r.get("servedBy"));
                for (int i = 0; i < r.getInt("count", 0); i++) {
                    say("   %-3s %-14s %s", r.get("r" + i + ".id"), r.get("r" + i + ".name"), r.get("r" + i + ".cuisine"));
                }
                break;
            }
            case "menu": {
                Message r = gw.request(Message.of(Protocol.GET_MENU).put("restaurant", arg(w, 1)));
                if (!check(r)) return;
                say("menu of %s (read served by %s):", r.get("name"), r.get("servedBy"));
                for (int i = 0; i < r.getInt("count", 0); i++) {
                    say("   %-6s %-18s Rs %s", r.get("m" + i + ".id"), r.get("m" + i + ".name"), r.get("m" + i + ".price"));
                }
                break;
            }
            case "order": {
                Message r = gw.request(Message.of(Protocol.PLACE_ORDER).put("restaurant", arg(w, 1)).put("items", arg(w, 2)));
                if (!check(r)) return;
                String orderId = r.get("orderId");
                myOrders.putIfAbsent(orderId, r.get("status")); // an event may already have moved it on
                gw.follow(orderId, r.getInt("version", 1));
                say("ORDER PLACED %s: %s = Rs %s (written by primary %s)", orderId, r.get("items"), r.get("total"),
                    r.get("servedBy"));
                break;
            }
            case "track": {
                Message r = gw.request(Message.of(Protocol.GET_ORDER).put("orderId", arg(w, 1)));
                if (!check(r)) return;
                say("%s is %s, rider=%s, version %s (read served by %s)", r.get("orderId"), r.get("status"),
                    r.get("rider", "").isEmpty() ? "-" : r.get("rider"), r.get("version"), r.get("servedBy"));
                break;
            }
            case "myorders": {
                Message r = gw.request(Message.of(Protocol.LIST_ORDERS).put("customer", id));
                if (!check(r)) return;
                say("%d order(s) (read served by %s):", r.getInt("count", 0), r.get("servedBy"));
                for (int i = 0; i < r.getInt("count", 0); i++) {
                    String p = "o" + i + ".";
                    say("   %-5s %-10s %s Rs %s", r.get(p + "orderId"), r.get(p + "status"), r.get(p + "restaurant"),
                        r.get(p + "total"));
                }
                break;
            }
            case "waitdelivered":
                waitDelivered(w.length > 1 ? Integer.parseInt(w[1]) : 60);
                break;
            default:
                throw new IllegalArgumentException("unknown command '" + w[0] + "'");
        }
    }

    private void onEvent(Message e) {
        if ("JOB".equals(e.get("kind"))) return;
        myOrders.put(e.get("orderId"), e.get("status"));
        say("LIVE %s #%s %-9s %s%s", e.get("orderId"), e.get("seq"), e.get("status"), describe(e), orderingNote(e));
    }

    /** Waits for DELIVERED on every order. Events do the work; a slow poll is a safety net. */
    private void waitDelivered(int timeoutSec) throws IOException, InterruptedException {
        long end = System.currentTimeMillis() + timeoutSec * 1000L;
        long nextPoll = System.currentTimeMillis() + 5000;
        while (true) {
            List<String> pending = myOrders.entrySet().stream()
                .filter(e -> !"DELIVERED".equals(e.getValue())).map(Map.Entry::getKey).sorted().toList();
            if (pending.isEmpty()) {
                say("all %d of my order(s) delivered", myOrders.size());
                return;
            }
            long now = System.currentTimeMillis();
            if (now > end) {
                say("TIMEOUT: still waiting for %s", pending);
                allOk = false;
                return;
            }
            if (now > nextPoll) {
                for (String orderId : pending) {
                    Message r = gw.request(Message.of(Protocol.GET_ORDER).put("orderId", orderId));
                    if (r.isOk() && "DELIVERED".equals(r.get("status"))) myOrders.put(orderId, "DELIVERED");
                }
                nextPoll = now + 5000;
            }
            Thread.sleep(200);
        }
    }
}
