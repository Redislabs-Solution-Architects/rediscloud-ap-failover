package com.redis.failoverdemo;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

public class RedisConnectionManager {

    private Jedis dbAConn, dbBConn;           // dedicated write connections (single-threaded writer)
    private JedisPool dbAPool, dbBPool;       // pools for stat + read (thread-safe)
    private volatile Jedis activeConn;
    private volatile String activeDb = "A";
    private volatile boolean connected = false;

    private String dbAHost, dbBHost;
    private int dbAPort, dbBPort;
    private String dbAPassword, dbBPassword;

    private ErrorLog errorLog;

    public RedisConnectionManager() {}

    public void setErrorLog(ErrorLog errorLog) { this.errorLog = errorLog; }

    public void connectA(String host, int port, String password) {
        closeOne("A");
        this.dbAHost = host; this.dbAPort = port; this.dbAPassword = password;
        dbAConn = buildConnection(host, port, password);
        dbAPool = buildPool(host, port, password);
        if (activeConn == null) { activeConn = dbAConn; activeDb = "A"; }
        connected = (dbAConn != null && dbBConn != null);
    }

    public void connectB(String host, int port, String password) {
        closeOne("B");
        this.dbBHost = host; this.dbBPort = port; this.dbBPassword = password;
        dbBConn = buildConnection(host, port, password);
        dbBPool = buildPool(host, port, password);
        connected = (dbAConn != null && dbBConn != null);
    }

    private Jedis buildConnection(String host, int port, String password) {
        JedisClientConfig cfg = DefaultJedisClientConfig.builder()
                .password(password)
                .ssl(false)
                .build();
        Jedis jedis = new Jedis(new HostAndPort(host, port), cfg);
        jedis.ping();
        return jedis;
    }

    private JedisPool buildPool(String host, int port, String password) {
        JedisPoolConfig poolCfg = new JedisPoolConfig();
        poolCfg.setMaxTotal(4);
        poolCfg.setMaxIdle(4);
        return new JedisPool(poolCfg, host, port, 5000, password);
    }

    public void switchTo(String which) {
        if ("A".equalsIgnoreCase(which)) { activeConn = dbAConn; activeDb = "A"; }
        else { activeConn = dbBConn; activeDb = "B"; }
    }

    public synchronized void reconnectActive() {
        try {
            if ("A".equals(activeDb)) {
                try { if (dbAConn != null) dbAConn.close(); } catch (Exception ignored) {}
                dbAConn = buildConnection(dbAHost, dbAPort, dbAPassword);
                activeConn = dbAConn;
            } else {
                try { if (dbBConn != null) dbBConn.close(); } catch (Exception ignored) {}
                dbBConn = buildConnection(dbBHost, dbBPort, dbBPassword);
                activeConn = dbBConn;
            }
            System.out.println("[CONN] Reconnected to Database-" + activeDb);
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (errorLog != null && e instanceof JedisException)
                errorLog.add("WRITE-" + activeDb, "Reconnect failed: " + msg);
            System.err.println("[CONN] Reconnect failed: " + msg);
        }
    }

    public boolean isConnected() { return connected; }
    public Jedis getActive() { return activeConn; }
    public String getActiveDb() { return activeDb; }

    public JedisPool getAPool() { return dbAPool; }
    public JedisPool getBPool() { return dbBPool; }

    public String getAHost() { return dbAHost; }
    public int getAPort() { return dbAPort; }
    public String getAPassword() { return dbAPassword; }
    public String getBHost() { return dbBHost; }
    public int getBPort() { return dbBPort; }
    public String getBPassword() { return dbBPassword; }

    public String getReplicaUri(String which) {
        if ("A".equalsIgnoreCase(which)) return "redis://:" + dbAPassword + "@" + dbAHost + ":" + dbAPort;
        return "redis://:" + dbBPassword + "@" + dbBHost + ":" + dbBPort;
    }

    private void closeOne(String which) {
        if ("A".equalsIgnoreCase(which)) {
            try { if (dbAConn != null) dbAConn.close(); } catch (Exception ignored) {}
            try { if (dbAPool != null) dbAPool.close(); } catch (Exception ignored) {}
        } else {
            try { if (dbBConn != null) dbBConn.close(); } catch (Exception ignored) {}
            try { if (dbBPool != null) dbBPool.close(); } catch (Exception ignored) {}
        }
    }

    public void close() { closeOne("A"); closeOne("B"); connected = false; }
}
