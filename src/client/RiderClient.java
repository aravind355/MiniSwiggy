package client;

import common.Args;
import common.Message;
import common.Protocol;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The delivery-partner (rider) app. The gateway subscribes riders to the "riders"
 * topic, where it announces orders that need a rider. After a rider claims an
 * order it also receives that order's live updates.
 *
 * Usage: java -cp out client.RiderClient --id RD1 [--gateway 127.0.0.1:9000] [--auto] [--ride-ms 1500]
 * In --auto mode it claims an order, waits for READY, picks it up and delivers it.
 */
public final class RiderClient extends ClientBase {
    private final boolean auto;
    private final int rideMs;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();

    // Auto-mode state. Only touched on the worker thread, so no locking is needed.
    private String current;       // order being carried, or null when free
    private boolean pickedUp;

    RiderClient(GatewayClient gw, boolean auto, int rideMs) {
        super(gw);
        this.auto = auto;
        this.rideMs = rideMs;
        gw.onEvent(e -> worker.execute(() -> onEvent(e)));
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        GatewayClient gw = GatewayClient.connect(a.get("gateway", "127.0.0.1:9000"), Protocol.RIDER,
            a.get("id", "RD1"));
        RiderClient r = new RiderClient(gw, a.flag("auto"), a.getInt("ride-ms", 1500));
        if (!r.auto) {
            r.repl();
            System.exit(0);
        }
        r.say("auto mode: online and waiting for orders");
        r.worker.scheduleWithFixedDelay(r::tick, 1, 2, TimeUnit.SECONDS);
    }

    @Override
    protected String helpText() {
        return String.join("\n",
            "Rider commands:",
            "  claim            take the next order waiting for a rider",
            "  pickup O-1       mark an order picked up",
            "  deliver O-1      mark an order delivered",
            "  help | quit");
    }

    @Override
    protected void command(String[] w) throws IOException {
        switch (w[0].toLowerCase()) {
            case "claim": {
                Message r = gw.request(Message.of(Protocol.CLAIM_ORDER));
                if (!check(r)) return;
                gw.follow(r.get("orderId"), r.getInt("version", 1));
                say("CLAIMED %s at %s (status %s)", r.get("orderId"), r.get("restaurant"), r.get("status"));
                break;
            }
            case "pickup":
                setStatus(arg(w, 1), "PICKED_UP");
                break;
            case "deliver":
                setStatus(arg(w, 1), "DELIVERED");
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
        if ("JOB".equals(e.get("kind"))) {
            if (auto && current == null) tryClaim();
            else if (!auto) say("new order waiting for a rider: %s at %s", e.get("orderId"), e.get("restaurant"));
            return;
        }
        if (!"RIDER_ASSIGNED".equals(e.get("event")) && !"PICKED_UP".equals(e.get("event"))
                && !"DELIVERED".equals(e.get("event"))) {
            say("update %s #%s: %s%s", e.get("orderId"), e.get("seq"), describe(e), orderingNote(e));
        }
        if (auto && e.get("orderId").equals(current)) onStatus(e.get("status"));
    }

    /** Auto mode: runs every 2 s in case an event was missed. */
    private void tick() {
        try {
            if (current == null) {
                tryClaim();
            } else {
                Message r = gw.request(Message.of(Protocol.GET_ORDER).put("orderId", current));
                if (r.isOk()) onStatus(r.get("status"));
            }
        } catch (IOException | RuntimeException e) {
            say("tick failed: %s", e.getMessage());
        }
    }

    private void tryClaim() {
        try {
            Message r = gw.request(Message.of(Protocol.CLAIM_ORDER));
            if (r.isError(Protocol.NO_ORDER_AVAILABLE)) return; // nothing to do yet
            if (!check(r)) return;
            current = r.get("orderId");
            pickedUp = false;
            gw.follow(current, r.getInt("version", 1));
            say("CLAIMED %s at %s (status %s)", current, r.get("restaurant"), r.get("status"));
            onStatus(r.get("status"));
        } catch (IOException e) {
            say("claim failed: %s", e.getMessage());
        }
    }

    private void onStatus(String status) {
        if ("READY".equals(status) && !pickedUp) {
            try {
                if (setStatus(current, "PICKED_UP")) startRide();
            } catch (IOException e) {
                say("pickup failed: %s", e.getMessage()); // tick() will try again
            }
        } else if ("PICKED_UP".equals(status) && !pickedUp) {
            startRide(); // e.g. we restarted while carrying this order
        }
    }

    private void startRide() {
        pickedUp = true;
        worker.schedule(this::deliver, rideMs, TimeUnit.MILLISECONDS);
    }

    private void deliver() {
        try {
            if (setStatus(current, "DELIVERED")) {
                current = null;
                pickedUp = false;
                tryClaim();
                return;
            }
        } catch (IOException e) {
            say("deliver failed: %s", e.getMessage());
        }
        worker.schedule(this::deliver, 2, TimeUnit.SECONDS);
    }
}
