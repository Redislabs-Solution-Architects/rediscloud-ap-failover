package com.redis.failoverdemo;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Thread-safe fixed-capacity ring buffer of error log entries.
 * Newest entries are prepended; oldest are evicted once capacity is reached.
 */
public class ErrorLog {

    private static final int MAX_ENTRIES = 200;
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final LinkedList<Map<String, String>> entries = new LinkedList<>();

    public synchronized void add(String source, String message) {
        Map<String, String> entry = new LinkedHashMap<>();
        entry.put("ts", FMT.format(Instant.now()));
        entry.put("source", source);
        entry.put("message", message != null ? message : "unknown error");
        entries.addFirst(entry);
        if (entries.size() > MAX_ENTRIES) entries.removeLast();
    }

    public synchronized List<Map<String, String>> getAll() {
        return new ArrayList<>(entries);
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized void clear() {
        entries.clear();
    }
}
