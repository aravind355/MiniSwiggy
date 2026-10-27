package common;

/**
 * All message types, roles and error codes used by MiniSwiggy.
 *
 * Who talks to whom:
 * <pre>
 *   client  --(HELLO, reads, writes, SUBSCRIBE)-->  gateway
 *   gateway --(EVENT pushes)-->                     client
 *   gateway --(reads, writes, PING, SET_ROLE)-->    order node
 *   primary --(REPLICATE)-->                        backup node
 *   restarted node --(SYNC_REQUEST)-->              peer node
 * </pre>
 * Every request may carry a "reqId" field; the reply echoes it back.
 */
public final class Protocol {
    private Protocol() {}

    public static final int DEFAULT_GATEWAY_PORT = 9000;

    // ---- handled by the gateway itself ----
    public static final String HELLO = "HELLO";             // role=CUSTOMER|RESTAURANT|RIDER, id=...
    public static final String SUBSCRIBE = "SUBSCRIBE";     // topic=...
    public static final String UNSUBSCRIBE = "UNSUBSCRIBE"; // topic=...
    public static final String CLUSTER = "CLUSTER";         // which nodes are up, who is primary

    // ---- reads: the gateway spreads them round-robin over live nodes ----
    public static final String LIST_RESTAURANTS = "LIST_RESTAURANTS";
    public static final String GET_MENU = "GET_MENU";       // restaurant=R1
    public static final String GET_ORDER = "GET_ORDER";     // orderId=O-1
    public static final String LIST_ORDERS = "LIST_ORDERS"; // optional filters: customer, restaurant, rider, status

    // ---- writes: the gateway sends them to the primary only ----
    public static final String PLACE_ORDER = "PLACE_ORDER";     // restaurant=R1, items=R1-M1x2,R1-M3x1
    public static final String UPDATE_STATUS = "UPDATE_STATUS"; // orderId=O-1, status=ACCEPTED
    public static final String CLAIM_ORDER = "CLAIM_ORDER";     // a rider asks for the next waiting order

    // ---- gateway <-> node control ----
    public static final String PING = "PING";
    public static final String PONG = "PONG";
    public static final String SET_ROLE = "SET_ROLE";       // role=PRIMARY|BACKUP, epoch=n
    public static final String STATS = "STATS";

    // ---- node <-> node ----
    public static final String REPLICATE = "REPLICATE";     // one order version + sender's epoch
    public static final String SYNC_REQUEST = "SYNC_REQUEST";

    // ---- replies and pushes ----
    public static final String OK = "OK";
    public static final String ERROR = "ERROR";             // code=..., reason=...
    public static final String EVENT = "EVENT";             // pushed by the gateway to subscribers

    // ---- client roles ----
    public static final String CUSTOMER = "CUSTOMER";
    public static final String RESTAURANT = "RESTAURANT";
    public static final String RIDER = "RIDER";

    // ---- node roles ----
    public static final String PRIMARY = "PRIMARY";
    public static final String BACKUP = "BACKUP";

    // ---- error codes ----
    public static final String BAD_REQUEST = "BAD_REQUEST";
    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String NOT_ALLOWED = "NOT_ALLOWED";
    public static final String INVALID_TRANSITION = "INVALID_TRANSITION";
    public static final String NO_ORDER_AVAILABLE = "NO_ORDER_AVAILABLE";
    public static final String NOT_PRIMARY = "NOT_PRIMARY";
    public static final String STALE_EPOCH = "STALE_EPOCH";
    public static final String NO_NODE_AVAILABLE = "NO_NODE_AVAILABLE";

    public static boolean isRead(String type) {
        return type.equals(LIST_RESTAURANTS) || type.equals(GET_MENU)
            || type.equals(GET_ORDER) || type.equals(LIST_ORDERS);
    }

    public static boolean isWrite(String type) {
        return type.equals(PLACE_ORDER) || type.equals(UPDATE_STATUS) || type.equals(CLAIM_ORDER);
    }

    public static Message ok() {
        return Message.of(OK);
    }

    public static Message error(String code, String reason) {
        return Message.of(ERROR).put("code", code).put("reason", reason);
    }
}
