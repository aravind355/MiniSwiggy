package common;

/** The life of an order. It may only move forward one step at a time. */
public enum OrderStatus {
    PLACED,     // customer placed it
    ACCEPTED,   // restaurant accepted it and started cooking
    READY,      // food is packed and waiting for the rider
    PICKED_UP,  // rider collected it
    DELIVERED;  // rider handed it to the customer

    public boolean canMoveTo(OrderStatus next) {
        return next.ordinal() == ordinal() + 1;
    }

    /** Parses a status name; throws IllegalArgumentException for unknown names. */
    public static OrderStatus parse(String name) {
        if (name == null) throw new IllegalArgumentException("status is missing");
        return valueOf(name.trim().toUpperCase());
    }
}
