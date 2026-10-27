package client;

import common.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Puts each order's live events back into sequence-number order (a hold-back queue).
 *
 * Every event carries seq = the order's version (1, 2, 3, ...). For each order we
 * remember the next seq we expect:
 * <ul>
 *   <li>seq == expected: deliver it, then deliver any held events that now follow on;</li>
 *   <li>seq &lt; expected: an old or duplicate event, drop it;</li>
 *   <li>seq &gt; expected: it overtook an earlier one, hold it back.</li>
 * </ul>
 * If a gap is not filled within maxHoldMs the missing events are given up on,
 * so one lost message cannot block an order forever.
 *
 * Events without an orderId/seq (e.g. job announcements) are passed straight through.
 * Not tied to sockets, so it can be unit-tested with a fake clock.
 */
public final class OrderedEventBuffer {

    private static final class Stream {
        int expected = 1;
        final TreeMap<Integer, Message> held = new TreeMap<>();
        long heldSince;
    }

    private final Map<String, Stream> streams = new HashMap<>();
    private final long maxHoldMs;
    private int duplicates;
    private int reordered;
    private int skipped;

    public OrderedEventBuffer(long maxHoldMs) {
        this.maxHoldMs = maxHoldMs;
    }

    /** Takes one incoming event and returns the events that can be delivered now, in order. */
    public synchronized List<Message> offer(Message event, long now) {
        List<Message> out = new ArrayList<>();
        String orderId = event.get("orderId");
        int seq = event.getInt("seq", -1);
        if (orderId == null || seq < 0) {
            out.add(event);
            return out;
        }
        Stream s = streams.computeIfAbsent(orderId, k -> new Stream());
        if (seq < s.expected || s.held.containsKey(seq)) {
            duplicates++;
            return out;
        }
        if (seq > s.expected) {
            if (s.held.isEmpty()) s.heldSince = now;
            s.held.put(seq, event);
            return out;
        }
        out.add(event);
        s.expected++;
        release(s, out, now, true);
        return out;
    }

    /**
     * Tells the buffer that the order is known to be at 'version' (learned from a reply,
     * e.g. after claiming it). Events before that version are old news.
     */
    public synchronized List<Message> startAt(String orderId, int version, long now) {
        Stream s = streams.computeIfAbsent(orderId, k -> new Stream());
        List<Message> out = new ArrayList<>();
        if (version > s.expected) {
            s.expected = version;
            s.held.headMap(version).clear();
        }
        release(s, out, now, false);
        return out;
    }

    /** Called regularly. Gives up on gaps that have been open longer than maxHoldMs. */
    public synchronized List<Message> expire(long now) {
        List<Message> out = new ArrayList<>();
        for (Stream s : streams.values()) {
            if (!s.held.isEmpty() && now - s.heldSince >= maxHoldMs) {
                int from = s.expected;
                int to = s.held.firstKey() - 1;
                skipped += to - from + 1;
                s.expected = to + 1;
                s.held.get(to + 1).put("skipped", from == to ? String.valueOf(from) : from + "-" + to);
                release(s, out, now, false);
            }
        }
        return out;
    }

    /** Moves held events that are now next in line to the output. */
    private void release(Stream s, List<Message> out, long now, boolean countAsReordered) {
        while (!s.held.isEmpty() && s.held.firstKey() == s.expected) {
            Message m = s.held.pollFirstEntry().getValue();
            if (countAsReordered) {
                m.put("heldBack", "true");
                reordered++;
            }
            out.add(m);
            s.expected++;
        }
        if (!s.held.isEmpty()) s.heldSince = now;
    }

    public synchronized int duplicates() {
        return duplicates;
    }

    public synchronized int reordered() {
        return reordered;
    }

    public synchronized int skipped() {
        return skipped;
    }
}
