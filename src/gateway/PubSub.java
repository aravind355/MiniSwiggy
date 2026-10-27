package gateway;

import common.Message;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Topic-based publish/subscribe inside the gateway.
 *
 * Topics used by MiniSwiggy:
 * <ul>
 *   <li>order:O-7       - the customer who placed O-7 and the rider carrying it</li>
 *   <li>restaurant:R1   - restaurant R1 hears about all of its orders</li>
 *   <li>riders          - every rider hears "a new order needs a rider"</li>
 * </ul>
 *
 * Optional network jitter: with jitterMs > 0 every delivery is delayed by a
 * random 0..jitterMs, so two events published close together can overtake each
 * other, as they can on a real network. Clients must then use the sequence
 * numbers to put them back in order.
 */
final class PubSub {
    private final Map<String, Set<ClientSession>> topics = new ConcurrentHashMap<>();
    private final int jitterMs;
    private final ScheduledExecutorService delayer;

    PubSub(int jitterMs) {
        this.jitterMs = jitterMs;
        this.delayer = jitterMs > 0 ? Executors.newScheduledThreadPool(2) : null;
    }

    void subscribe(String topic, ClientSession s) {
        topics.computeIfAbsent(topic, t -> ConcurrentHashMap.newKeySet()).add(s);
    }

    void unsubscribe(String topic, ClientSession s) {
        Set<ClientSession> subs = topics.get(topic);
        if (subs != null) subs.remove(s);
    }

    void removeSession(ClientSession s) {
        for (Set<ClientSession> subs : topics.values()) subs.remove(s);
    }

    /** Sends the event once to every session subscribed to any of the topics. Returns how many got it. */
    int publish(Message event, String... topicNames) {
        Set<ClientSession> targets = new LinkedHashSet<>();
        for (String t : topicNames) {
            Set<ClientSession> subs = topics.get(t);
            if (subs != null) targets.addAll(subs);
        }
        for (ClientSession s : targets) {
            if (delayer == null) {
                deliver(s, event);
            } else {
                long delay = ThreadLocalRandom.current().nextInt(jitterMs + 1);
                delayer.schedule(() -> deliver(s, event), delay, TimeUnit.MILLISECONDS);
            }
        }
        return targets.size();
    }

    private void deliver(ClientSession s, Message event) {
        try {
            s.send(event);
        } catch (IOException e) {
            removeSession(s); // client is gone
        }
    }

    void shutdown() {
        if (delayer != null) delayer.shutdownNow();
    }
}
