package common;

/**
 * One food order. Plain data object; the order store hands out copies so
 * callers can never change the stored version by accident.
 *
 * "version" is the order's sequence number: it starts at 1 and goes up by
 * one on every change. It is used for replication (a node keeps the highest
 * version it has seen) and for ordering the live updates a client receives.
 */
public final class Order {
    public String id;
    public String customerId;
    public String restaurantId;
    public String items;          // e.g. "R1-M1x2,R1-M3x1"
    public int totalRupees;
    public OrderStatus status;
    public String riderId = "";   // empty until a rider claims the order
    public int version;
    public String requestId = ""; // client request that created it (makes retries safe)
    public long createdAt;

    public Order copy() {
        Order o = new Order();
        o.id = id;
        o.customerId = customerId;
        o.restaurantId = restaurantId;
        o.items = items;
        o.totalRupees = totalRupees;
        o.status = status;
        o.riderId = riderId;
        o.version = version;
        o.requestId = requestId;
        o.createdAt = createdAt;
        return o;
    }

    /** The number part of the id, e.g. 12 for "O-12". Used for sorting. */
    public int number() {
        return Integer.parseInt(id.substring(2));
    }

    public void writeTo(Message m) {
        writeTo(m, "");
    }

    /** Copies all fields into a message. The prefix lets one message carry many orders ("o0.", "o1.", ...). */
    public void writeTo(Message m, String prefix) {
        m.put(prefix + "orderId", id)
         .put(prefix + "customer", customerId)
         .put(prefix + "restaurant", restaurantId)
         .put(prefix + "items", items)
         .put(prefix + "total", totalRupees)
         .put(prefix + "status", status.name())
         .put(prefix + "rider", riderId)
         .put(prefix + "version", version)
         .put(prefix + "requestId", requestId)
         .put(prefix + "createdAt", createdAt);
    }

    public static Order readFrom(Message m) {
        return readFrom(m, "");
    }

    public static Order readFrom(Message m, String prefix) {
        Order o = new Order();
        o.id = m.get(prefix + "orderId");
        if (o.id == null) throw new IllegalArgumentException("message has no " + prefix + "orderId");
        o.customerId = m.get(prefix + "customer", "");
        o.restaurantId = m.get(prefix + "restaurant", "");
        o.items = m.get(prefix + "items", "");
        o.totalRupees = m.getInt(prefix + "total", 0);
        o.status = OrderStatus.parse(m.get(prefix + "status"));
        o.riderId = m.get(prefix + "rider", "");
        o.version = m.getInt(prefix + "version", 0);
        o.requestId = m.get(prefix + "requestId", "");
        o.createdAt = m.getLong(prefix + "createdAt", 0);
        return o;
    }

    @Override
    public String toString() {
        return id + " [" + status + "] " + restaurantId + " -> " + customerId
            + (riderId.isEmpty() ? "" : " rider=" + riderId) + " v" + version;
    }
}
