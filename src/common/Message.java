package common;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One protocol message. On the wire every message is exactly one line of text:
 *
 * <pre>
 *     TYPE|key1=value1|key2=value2
 * </pre>
 *
 * Keys are plain words chosen by the program. Values are URL-encoded, so they
 * may safely contain '|', '=', spaces or non-ASCII text. A newline ends the
 * message, which makes it easy to read with BufferedReader.readLine().
 */
public final class Message {
    private final String type;
    private final Map<String, String> fields = new LinkedHashMap<>();

    public Message(String type) {
        if (type == null || type.isEmpty() || type.contains("|") || type.contains("\n")) {
            throw new IllegalArgumentException("bad message type: " + type);
        }
        this.type = type;
    }

    public static Message of(String type) {
        return new Message(type);
    }

    /** Adds a field. A null value is ignored, so callers can pass optional values directly. */
    public Message put(String key, Object value) {
        if (value != null) fields.put(key, String.valueOf(value));
        return this;
    }

    public String type() {
        return type;
    }

    public boolean has(String key) {
        return fields.containsKey(key);
    }

    public String get(String key) {
        return fields.get(key);
    }

    public String get(String key, String defaultValue) {
        return fields.getOrDefault(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String v = fields.get(key);
        if (v == null) return defaultValue;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String v = fields.get(key);
        if (v == null) return defaultValue;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean isOk() {
        return Protocol.OK.equals(type);
    }

    /** True if this is an ERROR message with the given error code. */
    public boolean isError(String code) {
        return Protocol.ERROR.equals(type) && code.equals(fields.get("code"));
    }

    public Map<String, String> fields() {
        return Collections.unmodifiableMap(fields);
    }

    /** Turns the message into its one-line wire form (without the newline). */
    public String encode() {
        StringBuilder sb = new StringBuilder(type);
        for (Map.Entry<String, String> e : fields.entrySet()) {
            sb.append('|').append(e.getKey()).append('=')
              .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    /** Parses one line from the wire. Throws IllegalArgumentException if it is malformed. */
    public static Message decode(String line) {
        if (line == null || line.isBlank()) throw new IllegalArgumentException("empty message");
        String[] parts = line.strip().split("\\|");
        Message m = new Message(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            int eq = parts[i].indexOf('=');
            if (eq <= 0) throw new IllegalArgumentException("bad field '" + parts[i] + "'");
            String key = parts[i].substring(0, eq);
            String value = URLDecoder.decode(parts[i].substring(eq + 1), StandardCharsets.UTF_8);
            m.fields.put(key, value);
        }
        return m;
    }

    /** Human-readable form for logs (values are not encoded). */
    @Override
    public String toString() {
        return type + " " + fields;
    }
}
