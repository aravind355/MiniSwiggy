package node;

import common.Catalog;
import common.Order;
import common.OrderStatus;
import common.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.zip.CRC32;

/**
 * The in-memory order table of one node.
 *
 * Writes happen in two steps so the node can replicate before it commits:
 * <ol>
 *   <li>prepareXxx() checks the request and builds the next version of the
 *       order, without changing the table;</li>
 *   <li>apply() stores that version (after the backup has acknowledged it).</li>
 * </ol>
 * apply() is also used for versions received from the primary and for the
 * snapshot pulled during recovery. It keeps only the highest version of each
 * order, so applying the same data twice, or in a different order, is harmless.
 */
public final class OrderStore {

    /** The outcome of preparing a write. changed=false means "this was a retry, nothing new to do". */
    public record Change(Order order, boolean changed, String event) {}

    /** A write that must be refused, with a protocol error code. */
    public static final class StoreException extends Exception {
        public final String code;

        StoreException(String code, String message) {
            super(message);
            this.code = code;
        }
    }

    private final Map<String, Order> orders = new ConcurrentHashMap<>();
    private final Map<String, String> orderByRequest = new ConcurrentHashMap<>();
    private int nextNumber = 1;

    public synchronized Change preparePlace(String requestId, String customerId, String restaurantId, String items)
            throws StoreException {
        // Same request seen before (the gateway retried after a crash): return the existing order.
        // The key includes the customer, so two customers that reuse the same reqId cannot clash.
        String existing = requestId.isEmpty() ? null : orderByRequest.get(requestKey(customerId, requestId));
        if (existing != null) return new Change(orders.get(existing).copy(), false, "PLACED");

        if (customerId.isEmpty()) throw new StoreException(Protocol.BAD_REQUEST, "customer is missing");
        int total;
        try {
            total = Catalog.priceOf(restaurantId, items);
        } catch (IllegalArgumentException e) {
            throw new StoreException(Protocol.BAD_REQUEST, e.getMessage());
        }
        Order o = new Order();
        o.id = "O-" + nextNumber;
        o.customerId = customerId;
        o.restaurantId = restaurantId;
        o.items = items;
        o.totalRupees = total;
        o.status = OrderStatus.PLACED;
        o.version = 1;
        o.requestId = requestId;
        o.createdAt = System.currentTimeMillis();
        return new Change(o, true, "PLACED");
    }

    public synchronized Change prepareStatus(String orderId, OrderStatus next, String actor) throws StoreException {
        Order cur = orders.get(orderId);
        if (cur == null) throw new StoreException(Protocol.NOT_FOUND, "no order " + orderId);
        if (cur.status == next) return new Change(cur.copy(), false, next.name()); // a retry of a finished write
        if (!cur.status.canMoveTo(next)) {
            throw new StoreException(Protocol.INVALID_TRANSITION,
                orderId + " is " + cur.status + ", it cannot become " + next);
        }
        // The restaurant moves the order to ACCEPTED/READY; the assigned rider does the rest.
        boolean restaurantStep = next == OrderStatus.ACCEPTED || next == OrderStatus.READY;
        if (!restaurantStep && cur.riderId.isEmpty()) {
            throw new StoreException(Protocol.NOT_ALLOWED, orderId + " has no rider yet");
        }
        String owner = restaurantStep ? cur.restaurantId : cur.riderId;
        if (!owner.equals(actor)) {
            throw new StoreException(Protocol.NOT_ALLOWED, actor + " may not mark " + orderId + " as " + next);
        }
        Order o = cur.copy();
        o.status = next;
        o.version++;
        return new Change(o, true, next.name());
    }

    /** Gives the rider the oldest accepted order that has no rider yet. A rider carries one order at a time. */
    public synchronized Change prepareClaim(String riderId) throws StoreException {
        if (riderId.isEmpty()) throw new StoreException(Protocol.BAD_REQUEST, "rider is missing");
        for (Order o : orders.values()) {
            if (riderId.equals(o.riderId) && o.status != OrderStatus.DELIVERED) {
                return new Change(o.copy(), false, "RIDER_ASSIGNED"); // already busy with this one
            }
        }
        Order best = null;
        for (Order o : orders.values()) {
            boolean waiting = o.riderId.isEmpty()
                && (o.status == OrderStatus.ACCEPTED || o.status == OrderStatus.READY);
            if (waiting && (best == null || o.number() < best.number())) best = o;
        }
        if (best == null) throw new StoreException(Protocol.NO_ORDER_AVAILABLE, "no order is waiting for a rider");
        Order o = best.copy();
        o.riderId = riderId;
        o.version++;
        return new Change(o, true, "RIDER_ASSIGNED");
    }

    /** Stores an order version unless we already hold the same or a newer one. Returns true if stored. */
    public synchronized boolean apply(Order incoming) {
        Order cur = orders.get(incoming.id);
        if (cur != null && cur.version >= incoming.version) return false;
        orders.put(incoming.id, incoming.copy());
        if (!incoming.requestId.isEmpty()) {
            orderByRequest.put(requestKey(incoming.customerId, incoming.requestId), incoming.id);
        }
        nextNumber = Math.max(nextNumber, incoming.number() + 1);
        return true;
    }

    private static String requestKey(String customerId, String requestId) {
        return customerId + "/" + requestId;
    }

    public Order get(String orderId) {
        Order o = orders.get(orderId);
        return o == null ? null : o.copy();
    }

    /** Orders matching the filter, oldest first. */
    public List<Order> list(Predicate<Order> filter) {
        List<Order> out = new ArrayList<>();
        for (Order o : orders.values()) {
            if (filter.test(o)) out.add(o.copy());
        }
        out.sort(Comparator.comparingInt(Order::number));
        return out;
    }

    public int size() {
        return orders.size();
    }

    /** A fingerprint of the whole table. Two replicas are in sync when their checksums match. */
    public synchronized String checksum() {
        CRC32 crc = new CRC32();
        for (Order o : list(x -> true)) {
            String row = o.id + ":" + o.version + ":" + o.status + ":" + o.riderId + ";";
            crc.update(row.getBytes(StandardCharsets.UTF_8));
        }
        return Long.toHexString(crc.getValue());
    }
}
