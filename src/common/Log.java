package common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Timestamped console logging: "12:00:01.234 [N1] message". Plain ASCII so it looks the same in every terminal. */
public final class Log {
    private Log() {}

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile boolean enabled = true;

    /** The automated tests switch logging off to keep their output short. */
    public static void setEnabled(boolean on) {
        enabled = on;
    }

    public static void info(String who, String format, Object... args) {
        print(who, "", format, args);
    }

    public static void warn(String who, String format, Object... args) {
        print(who, "WARN ", format, args);
    }

    private static synchronized void print(String who, String level, String format, Object... args) {
        if (!enabled) return;
        String text = args.length == 0 ? format : String.format(format, args);
        System.out.println(LocalTime.now().format(TIME) + " [" + who + "] " + level + text);
        System.out.flush();
    }
}
