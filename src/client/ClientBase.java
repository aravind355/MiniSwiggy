package client;

import common.Log;
import common.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** Shared plumbing for the customer, restaurant and rider programs: printing, error checks, the command loop. */
abstract class ClientBase {
    protected final GatewayClient gw;
    protected final String id;
    protected volatile boolean allOk = true;

    ClientBase(GatewayClient gw) {
        this.gw = gw;
        this.id = gw.id();
    }

    /** Runs one typed command. */
    protected abstract void command(String[] words) throws IOException, InterruptedException;

    protected abstract String helpText();

    protected void say(String format, Object... args) {
        Log.info(id, format, args);
    }

    /** Prints an ERROR reply and returns false; returns true for OK. */
    protected boolean check(Message reply) {
        if (reply.isOk()) return true;
        say("ERROR %s: %s", reply.get("code"), reply.get("reason"));
        allOk = false;
        return false;
    }

    protected static String arg(String[] words, int i) {
        if (i >= words.length) throw new IllegalArgumentException("'" + words[0] + "' needs more arguments");
        return words[i];
    }

    /** Runs one command line and reports problems instead of crashing. */
    protected void run(String line) {
        // Windows tools may put an invisible byte-order mark (U+FEFF) in front of piped input; drop it.
        String[] words = line.replace("﻿", "").trim().split("\\s+");
        if (words[0].isEmpty()) return;
        try {
            if (words[0].equalsIgnoreCase("help")) {
                System.out.println(helpText());
            } else if (words[0].equalsIgnoreCase("wait")) {
                Thread.sleep(Long.parseLong(arg(words, 1)) * 1000); // lets live updates print in scripts
            } else {
                command(words);
            }
        } catch (IOException e) {
            say("ERROR: %s", e.getMessage());
            allOk = false;
        } catch (IllegalArgumentException e) {
            say("usage error: %s (type help)", e.getMessage());
            allOk = false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Runs "cmd1; cmd2; ..." without user input (used by the demo), echoing each command. */
    protected void runScript(String script) {
        for (String cmd : script.split(";")) {
            if (cmd.isBlank()) continue;
            say("> %s", cmd.trim());
            run(cmd);
        }
    }

    /** Interactive mode: read commands from the keyboard until "quit". */
    protected void repl() throws IOException {
        System.out.println(helpText());
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(id + "> ");
            System.out.flush();
            String line = stdin.readLine();
            if (line == null || line.trim().equalsIgnoreCase("quit")) break;
            run(line);
        }
        gw.close();
    }

    /** Short text for one live event, shared by all client types. */
    protected static String describe(Message e) {
        String rider = e.get("rider", "");
        switch (e.get("event", "")) {
            case "PLACED": return "order placed at " + e.get("restaurant") + " (Rs " + e.get("total") + ")";
            case "ACCEPTED": return "restaurant " + e.get("restaurant") + " accepted it and is cooking";
            case "READY": return "food is packed and ready for pickup";
            case "RIDER_ASSIGNED": return "rider " + rider + " is assigned";
            case "PICKED_UP": return "rider " + rider + " picked it up";
            case "DELIVERED": return "handed over by rider " + rider + " - enjoy!";
            default: return e.get("event", "?");
        }
    }

    /** Extra note when the ordering buffer had to fix something. */
    protected static String orderingNote(Message e) {
        String note = "";
        if (e.has("heldBack")) note += "  <- arrived out of order, held back and re-sequenced";
        if (e.has("skipped")) note += "  <- update #" + e.get("skipped") + " never arrived, skipped";
        return note;
    }
}
