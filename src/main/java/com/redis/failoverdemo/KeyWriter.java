package com.redis.failoverdemo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background thread that continuously SETs keys into the active Redis instance.
 * Uses pipelining in batches to sustain high throughput.
 */
public class KeyWriter implements Runnable {

    private final RedisConnectionManager connMgr;
    private final String keyPrefix = "key:";
    private static final int PIPELINE_BATCH = 100; // commands per pipeline flush
    private volatile int opsPerSecond = 2;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(true);
    private final AtomicLong keyCount = new AtomicLong(0);
    private volatile boolean initialized = false;
    private int consecutiveErrors = 0;
    private final LinkedList<Map<String, String>> recentKeys = new LinkedList<>();

    public KeyWriter(RedisConnectionManager connMgr) {
        this.connMgr = connMgr;
    }

    /** Read keycount from the active DB's pool and seed the counter. */
    private void initFromRedis() {
        if (initialized) return;
        String activeDb = connMgr.getActiveDb();
        redis.clients.jedis.JedisPool pool = "A".equals(activeDb) ? connMgr.getAPool() : connMgr.getBPool();
        if (pool == null) return;
        try (Jedis j = pool.getResource()) {
            String val = j.get("keycount");
            if (val != null) {
                long n = Long.parseLong(val);
                keyCount.set(n);
                System.out.println("[WRITER] Resuming from keycount = " + n);
            } else {
                j.set("keycount", "0");
                System.out.println("[WRITER] No keycount found, starting at 0");
            }
            initialized = true;
        } catch (Exception e) {
            System.err.println("[WRITER] Failed to read keycount: " + e.getMessage());
        }
    }

    public void startThread() {
        Thread t = new Thread(this, "key-writer");
        t.setDaemon(true);
        t.start();
    }

    public void setOpsPerSecond(int ops) { this.opsPerSecond = Math.max(1, ops); }
    public int getOpsPerSecond() { return opsPerSecond; }

    @Override
    public void run() {
        while (running.get()) {
            if (!paused.get() && connMgr.isConnected()) {
                initFromRedis();
                int ops = opsPerSecond;
                long cycleStartNs = System.nanoTime();
                int sent = 0;

                try {
                    Jedis active = connMgr.getActive();
                    while (sent < ops && !paused.get()) {
                        int batchSize = Math.min(PIPELINE_BATCH, ops - sent);
                        Pipeline pipe = active.pipelined();
                        List<Map<String, String>> batchEntries = new ArrayList<>(batchSize);

                        long lastSeq = 0;
                        for (int i = 0; i < batchSize; i++) {
                            long seq = keyCount.incrementAndGet();
                            lastSeq = seq;
                            String key = keyPrefix + seq;
                            String value = "val-" + seq + "-" + Instant.now();
                            pipe.set(key, value);
                            batchEntries.add(Map.of("seq", String.valueOf(seq), "key", key, "value", value));
                        }
                        // Update the keycount tracker so readers know the latest key
                        pipe.set("keycount", String.valueOf(lastSeq));
                        pipe.sync();
                        sent += batchSize;
                        consecutiveErrors = 0;

                        // Track most recent keys (only last few from batch)
                        synchronized (recentKeys) {
                            for (int i = batchEntries.size() - 1; i >= 0; i--) {
                                recentKeys.addFirst(batchEntries.get(i));
                            }
                            while (recentKeys.size() > 10) recentKeys.removeLast();
                        }
                    }
                } catch (Exception e) {
                    consecutiveErrors++;
                    System.err.println("[WRITER] Error (" + consecutiveErrors + "/3): " + e.getMessage());
                    if (consecutiveErrors >= 3) {
                        System.out.println("[WRITER] Reconnecting...");
                        connMgr.reconnectActive();
                        consecutiveErrors = 0;
                    }
                }

                // Sleep remainder of the 1-second window
                long elapsedNs = System.nanoTime() - cycleStartNs;
                long sleepMs = (1_000_000_000L - elapsedNs) / 1_000_000;
                if (sleepMs > 0) {
                    try { Thread.sleep(sleepMs); } catch (InterruptedException e) { return; }
                }
            } else {
                try { Thread.sleep(100); } catch (InterruptedException e) { break; }
            }
        }
    }

    public List<Map<String, String>> getRecentKeys() {
        synchronized (recentKeys) {
            return new ArrayList<>(recentKeys);
        }
    }

    public void pause()  { paused.set(true); }
    public void resume() { initialized = false; paused.set(false); }
    public void stop()   { running.set(false); }
    public long getKeyCount() { return keyCount.get(); }
    public boolean isPaused() { return paused.get(); }
}
