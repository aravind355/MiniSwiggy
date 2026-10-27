package common;

import java.util.List;

/**
 * Restaurants and menus. This is fixed reference data, so every node loads the
 * same copy at start-up instead of replicating it. (Names are made up.)
 */
public final class Catalog {
    private Catalog() {}

    public record MenuItem(String id, String name, int price) {}

    public record Restaurant(String id, String name, String cuisine, List<MenuItem> menu) {}

    public static final List<Restaurant> RESTAURANTS = List.of(
        new Restaurant("R1", "Biryani House", "Biryani", List.of(
            new MenuItem("R1-M1", "Chicken Biryani", 320),
            new MenuItem("R1-M2", "Paneer Biryani", 280),
            new MenuItem("R1-M3", "Gulab Jamun", 90))),
        new Restaurant("R2", "Dosa Corner", "South Indian", List.of(
            new MenuItem("R2-M1", "Masala Dosa", 120),
            new MenuItem("R2-M2", "Rava Idli", 90),
            new MenuItem("R2-M3", "Filter Coffee", 50))),
        new Restaurant("R3", "Burger Point", "Burgers", List.of(
            new MenuItem("R3-M1", "Veg Burger", 180),
            new MenuItem("R3-M2", "Peri Peri Fries", 150))));

    public static Restaurant find(String restaurantId) {
        for (Restaurant r : RESTAURANTS) {
            if (r.id().equals(restaurantId)) return r;
        }
        return null;
    }

    /**
     * Checks an item list like "R1-M1x2,R1-M3x1" and returns its total price.
     * Throws IllegalArgumentException for unknown items, items from another
     * restaurant, or bad quantities.
     */
    public static int priceOf(String restaurantId, String items) {
        Restaurant r = find(restaurantId);
        if (r == null) throw new IllegalArgumentException("unknown restaurant " + restaurantId);
        if (items == null || items.isBlank()) throw new IllegalArgumentException("no items given");
        int total = 0;
        for (String part : items.split(",")) {
            String[] idAndQty = part.trim().split("x");
            if (idAndQty.length != 2) throw new IllegalArgumentException("bad item '" + part + "', use ITEMxQTY");
            int qty;
            try {
                qty = Integer.parseInt(idAndQty[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("bad quantity in '" + part + "'");
            }
            if (qty < 1 || qty > 20) throw new IllegalArgumentException("quantity must be 1-20 in '" + part + "'");
            MenuItem item = r.menu().stream().filter(m -> m.id().equals(idAndQty[0])).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(idAndQty[0] + " is not on the menu of " + restaurantId));
            total += item.price() * qty;
        }
        return total;
    }
}
