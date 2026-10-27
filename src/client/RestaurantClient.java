package client;

import common.Args;
import common.Message;
import common.Protocol;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The restaurant app. The gateway subscribes it to "restaurant:&lt;id&gt;", so new
 * orders arrive as pushed events.
 *
 * Usage: java -cp out client.RestaurantClient --id R1 [--gateway 127.0.0.1:9000] [--auto] [--prep-ms 2000]
 * In --auto mode it accepts every new order and marks it READY after prep-ms.
 */
public final class RestaurantClient extends ClientBase {
    private final boolean auto;
    private final int prepMs;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private final Set<String> inHand = ConcurrentHashMap.newKeySet(); // orders with an action already planned

    RestaurantClient(GatewayClient gw, boolean auto, int prepMs) {
        super(gw);
        this.auto = auto;
        this.prepMs = prepMs;
        gw.onEvent(this::onEvent);
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        GatewayClient gw = GatewayClient.connect(a.get("gateway", "127.0.0.1:9000"), Protocol.RESTAURANT,
            a.get("id", "R1"));
        RestaurantClient r = new RestaurantClient(gw, a.flag("auto"), a.getInt("prep-ms", 2000));
        if (!r.auto) {
            r.repl();
            System.exit(0);
        }
        r.say("auto mode: accepting every order, cooking takes %d ms", r.prepMs);
        // Safety net: every few seconds look for orders that an event did not cover.
        r.worker.scheduleWithFixedDelay(r::sweep, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    protected String helpText() {
        return String.join("\n",
            "Restaurant commands:",
            "  orders           list my orders",
            "  accept O-1       accept an order",
            "  ready O-1        mark an order ready for pickup",
            "  help | quit");
    }

    @Override
    protected void command(String[] w) throws IOException {
        switch (w[0].toLowerCase()) {
            case "orders": {
                Message r = gw.request(Message.of(Protocol.LIST_ORDERS).put("restaurant", id));
                if (!check(r)) return;
                for (int i = 0; i < r.getInt("count", 0); i++) {
                    String p = "o" + i + ".";
                    say("   %-5s %-10s %s for %s", r.get(p + "orderId"), r.get(p + "status"), r.get(p + "items"),
                        r.get(p + "customer"));
                }
                break;
            }
            case "accept":
                setStatus(arg(w, 1), "ACCEPTED");
                break;
            case "ready":
                setStatus(arg(w, 1), "READY");
                break;
            default:
                throw new IllegalArgumentException("unknown command '" + w[0] + "'");
        }
    }

    private boolean setStatus(String orderId, String status) throws IOException {
        Message r = gw.request(Message.of(Protocol.UPDATE_STATUS).put("orderId", orderId).put("status", status));
        if (!check(r)) return false;
        say("%s %s (written by primary %s)", orderId, status, r.get("servedBy"));
        return true;
    }

    private void onEvent(Message e) {
        if ("JOB".equals(e.get("kind"))) return;
        if ("PLACED".equals(e.get("event"))) {
            say("NEW ORDER %s from %s: %s (Rs %s)%s", e.get("orderId"), e.get("customer"), e.get("items"),
                e.get("total"), orderingNote(e));
            if (auto) planAccept(e.get("orderId"), 500);
        } else if (!"ACCEPTED".equals(e.get("event")) && !"READY".equals(e.get("event"))) {
            // our own ACCEPTED/READY changes are already printed when we make them
            say("update %s #%s: %s%s", e.get("orderId"), e.get("seq"), describe(e), orderingNote(e));
        }
    }

    private void planAccept(String orderId, long delayMs) {
        if (inHand.add(orderId)) worker.schedule(() -> accept(orderId), delayMs, TimeUnit.MILLISECONDS);
    }

    private void accept(String orderId) {
        try {
            if (setStatus(orderId, "ACCEPTED")) {
                worker.schedule(() -> markReady(orderId), prepMs, TimeUnit.MILLISECONDS);
                return;
            }
        } catch (IOException e) {
            say("accept %s failed: %s", orderId, e.getMessage());
        }
        inHand.remove(orderId); // the sweep will try again
    }

    private void markReady(String orderId) {
        try {
            setStatus(orderId, "READY");
        } catch (IOException e) {
            say("ready %s failed: %s", orderId, e.getMessage());
        }
        inHand.remove(orderId);
    }

    /** Auto mode safety net: pick up PLACED or ACCEPTED orders that have no planned action. */
    private void sweep() {
        try {
            Message r = gw.request(Message.of(Protocol.LIST_ORDERS).put("restaurant", id));
            if (!r.isOk()) return;
            for (int i = 0; i < r.getInt("count", 0); i++) {
                String orderId = r.get("o" + i + ".orderId");
                String status = r.get("o" + i + ".status");
                if ("PLACED".equals(status)) {
                    planAccept(orderId, 0);
                } else if ("ACCEPTED".equals(status) && inHand.add(orderId)) {
                    worker.schedule(() -> markReady(orderId), 0, TimeUnit.MILLISECONDS);
                }
            }
        } catch (IOException | RuntimeException e) {
            say("sweep failed: %s", e.getMessage());
        }
    }
}
