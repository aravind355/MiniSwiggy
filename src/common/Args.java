package common;

import java.util.HashMap;
import java.util.Map;

/** A tiny "--name value" command-line parser, so the project needs no libraries. */
public final class Args {
    private final Map<String, String> values = new HashMap<>();

    public Args(String[] argv) {
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            if (!a.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + a);
            String name = a.substring(2);
            // "--auto" followed by another option (or nothing) is an on/off switch
            if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                values.put(name, argv[++i]);
            } else {
                values.put(name, "true");
            }
        }
    }

    public String get(String name, String defaultValue) {
        return values.getOrDefault(name, defaultValue);
    }

    public String require(String name) {
        String v = values.get(name);
        if (v == null) throw new IllegalArgumentException("missing required option --" + name);
        return v;
    }

    public int getInt(String name, int defaultValue) {
        String v = values.get(name);
        return v == null ? defaultValue : Integer.parseInt(v);
    }

    public boolean flag(String name) {
        return "true".equals(values.get(name));
    }
}
