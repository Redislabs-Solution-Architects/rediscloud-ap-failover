package com.redis.failoverdemo;

import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final RedisConnectionManager connMgr;
    private final KeyWriter writer;
    private final RedisCloudApiClient apiClient;
    private final AppConfig appConfig;
    private FailoverOrchestrator orchestrator;
    private final AtomicBoolean failoverInProgress = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<Map<String, Object>> failoverSteps = new CopyOnWriteArrayList<>();
    private volatile Long failoverElapsedMs = null;
    private volatile boolean writerStarted = false;

    public ApiController(RedisConnectionManager connMgr,
                         KeyWriter writer,
                         RedisCloudApiClient apiClient,
                         AppConfig appConfig) {
        this.connMgr = connMgr;
        this.writer = writer;
        this.apiClient = apiClient;
        this.appConfig = appConfig;
    }

    @GetMapping("/saved-config")
    public Map<String, String> savedConfig() {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("apiAccountKey", appConfig.get("API_ACCOUNT_KEY", ""));
        c.put("apiSecretKey", appConfig.get("API_SECRET_KEY", ""));
        c.put("dbAEndpoint", appConfig.get("DBA_ENDPOINT", ""));
        c.put("dbAPassword", appConfig.get("DBA_PASSWORD", ""));
        c.put("dbASubId", appConfig.get("DBA_SUB_ID", ""));
        c.put("dbADbId", appConfig.get("DBA_DB_ID", ""));
        c.put("dbBEndpoint", appConfig.get("DBB_ENDPOINT", ""));
        c.put("dbBPassword", appConfig.get("DBB_PASSWORD", ""));
        c.put("dbBSubId", appConfig.get("DBB_SUB_ID", ""));
        c.put("dbBDbId", appConfig.get("DBB_DB_ID", ""));
        return c;
    }

    private void ensureOrchestrator() {
        if (orchestrator == null) {
            orchestrator = new FailoverOrchestrator(connMgr, apiClient, failoverSteps,
                    () -> { writer.pause(); failoverInProgress.set(true); },
                    () -> { failoverInProgress.set(false); });
            orchestrator.initSteps();
        }
    }

    @PostMapping("/credentials")
    public Map<String, String> saveCredentials(@RequestBody Map<String, String> body) {
        String ak = body.get("accountKey"), sk = body.get("secretKey");
        if (ak == null || ak.isBlank() || sk == null || sk.isBlank())
            return Map.of("status", "error", "message", "Account Key and User API Key are required");
        apiClient.setCredentials(ak, sk);
        try { apiClient.validateCredentials(); }
        catch (Exception e) { apiClient.setCredentials(null, null); return Map.of("status", "error", "message", "Invalid credentials: " + e.getMessage()); }
        appConfig.set("API_ACCOUNT_KEY", ak);
        appConfig.set("API_SECRET_KEY", sk);
        appConfig.save();
        return Map.of("status", "saved");
    }

    @PostMapping("/connect")
    public Map<String, Object> connect(@RequestBody Map<String, String> body) {
        String type = body.get("type"); // "A" or "B"
        String endpoint = body.get("endpoint"), password = body.get("password");
        String subId = body.get("subscriptionId"), dbId = body.get("dbId");
        if (endpoint == null || endpoint.isBlank() || password == null || password.isBlank())
            return Map.of("status", "error", "message", "Endpoint and password are required");
        if (subId == null || subId.isBlank() || dbId == null || dbId.isBlank())
            return Map.of("status", "error", "message", "Subscription ID and Database ID are required");
        try { apiClient.validateDatabase(subId, dbId); }
        catch (Exception e) { return Map.of("status", "error", "message", "Cloud API error: " + e.getMessage()); }
        try {
            String[] parts = endpoint.split(":"); String host = parts[0]; int port = Integer.parseInt(parts[1]);
            if ("A".equalsIgnoreCase(type)) { connMgr.connectA(host, port, password); apiClient.setDbA(subId, dbId); }
            else { connMgr.connectB(host, port, password); apiClient.setDbB(subId, dbId); }
            String prefix = "A".equalsIgnoreCase(type) ? "DBA" : "DBB";
            appConfig.set(prefix + "_ENDPOINT", endpoint); appConfig.set(prefix + "_PASSWORD", password);
            appConfig.set(prefix + "_SUB_ID", subId); appConfig.set(prefix + "_DB_ID", dbId);
            appConfig.save();
            if (!writerStarted) { writer.startThread(); writerStarted = true; }
            ensureOrchestrator();
            return Map.of("status", "connected", "type", type);
        } catch (Exception e) { return Map.of("status", "error", "message", "Redis connection failed: " + e.getMessage()); }
    }

    @GetMapping("/replication-status")
    public Map<String, String> replicationStatus() {
        if (!apiClient.isFullyResolved()) return Map.of("direction", "unknown");
        try { return Map.of("direction", apiClient.getReplicationDirection()); }
        catch (Exception e) { return Map.of("direction", "error", "message", e.getMessage()); }
    }

    @PostMapping("/start-replication")
    public Map<String, String> startReplication(@RequestBody Map<String, String> body) {
        String direction = body.get("direction"); // "A_TO_B" or "B_TO_A"
        if (!apiClient.isFullyResolved()) return Map.of("status", "error", "message", "API not configured");
        if (!connMgr.isConnected()) return Map.of("status", "error", "message", "Databases not connected");
        try {
            String sourceDb = "A_TO_B".equals(direction) ? "A" : "B";
            String uri = connMgr.getReplicaUri(sourceDb);
            // Build descriptive info
            String replicaSub, replicaDbId, replicaLabel;
            if ("A_TO_B".equals(direction)) {
                replicaSub = apiClient.getSubIdB(); replicaDbId = apiClient.getDbIdB(); replicaLabel = "Database-B";
            } else {
                replicaSub = apiClient.getSubIdA(); replicaDbId = apiClient.getDbIdA(); replicaLabel = "Database-A";
            }
            String apiPath = "PUT /subscriptions/" + replicaSub + "/databases/" + replicaDbId;
            String payload = "{\"replicaOf\": [\"" + uri + "\"]}";

            String taskId = apiClient.enableReplicationByDirection(direction, uri);
            new Thread(() -> {
                try { apiClient.waitForTask(taskId, 120_000); } catch (Exception ignored) {}
            }, "replication-enable").start();
            return Map.of("status", "started", "taskId", taskId,
                    "apiPath", apiPath + " (" + replicaLabel + ")",
                    "payload", payload);
        } catch (Exception e) { return Map.of("status", "error", "message", e.getMessage()); }
    }

    @PostMapping("/switch-write")
    public Map<String, String> switchWrite(@RequestBody Map<String, String> body) {
        connMgr.switchTo(body.get("to"));
        return Map.of("status", "ok", "writingTo", connMgr.getActiveDb());
    }

    @PostMapping("/flush/{which}")
    public Map<String, String> flush(@PathVariable String which) {
        try (var jedis = "A".equalsIgnoreCase(which) ? connMgr.getAPool().getResource() : connMgr.getBPool().getResource()) {
            jedis.flushAll();
            return Map.of("status", "flushed");
        } catch (Exception e) { return Map.of("status", "error", "message", e.getMessage()); }
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("connected", connMgr.isConnected());
        data.put("dbASize", safeDbSize(connMgr.getAPool()));
        data.put("dbBSize", safeDbSize(connMgr.getBPool()));
        data.put("keyCount", writer.getKeyCount());
        data.put("writingTo", connMgr.getActiveDb());
        data.put("failoverInProgress", failoverInProgress.get());
        data.put("failoverElapsedMs", failoverElapsedMs);
        data.put("recentKeys", writer.getRecentKeys());
        data.put("trafficRunning", !writer.isPaused());
        data.put("opsPerSecond", writer.getOpsPerSecond());
        data.put("hasApiCredentials", apiClient.hasCredentials());
        data.put("failoverSteps", new ArrayList<>(failoverSteps));
        return data;
    }

    @PostMapping("/traffic/toggle")
    public Map<String, Object> toggleTraffic() {
        if (writer.isPaused()) writer.resume(); else writer.pause();
        return Map.of("trafficRunning", !writer.isPaused());
    }

    @PostMapping("/traffic/rate")
    public Map<String, Object> setRate(@RequestBody Map<String, Integer> body) {
        int ops = body.getOrDefault("opsPerSecond", 2);
        writer.setOpsPerSecond(ops);
        return Map.of("opsPerSecond", writer.getOpsPerSecond());
    }

    @PostMapping("/reset-failover-steps")
    public Map<String, String> resetFailoverSteps() {
        ensureOrchestrator();
        failoverSteps.clear();
        orchestrator.initSteps();
        return Map.of("status", "reset");
    }

    @PostMapping("/failover")
    public Map<String, String> startFailover() {
        ensureOrchestrator();
        if (failoverInProgress.get()) return Map.of("status", "already_in_progress");
        failoverElapsedMs = null;
        failoverSteps.clear();
        orchestrator.initSteps();
        orchestrator.executeAsync(elapsed -> failoverElapsedMs = elapsed);
        return Map.of("status", "started");
    }

    @GetMapping("/read-keys")
    public Map<String, Object> readKeys() {
        return Map.of(
            "dbAKeys", readRecentFrom(connMgr.getAPool()),
            "dbBKeys", readRecentFrom(connMgr.getBPool())
        );
    }

    private List<Map<String, String>> readRecentFrom(JedisPool pool) {
        List<Map<String, String>> result = new ArrayList<>();
        if (pool == null) return padTo10(result);
        try (var jedis = pool.getResource()) {
            String countStr = jedis.get("keycount");
            if (countStr == null) return padTo10(result);
            long highest = Long.parseLong(countStr);
            long start = Math.max(1, highest - 9);
            List<String> keys = new ArrayList<>();
            for (long i = highest; i >= start; i--) keys.add("key:" + i);
            List<String> vals = jedis.mget(keys.toArray(new String[0]));
            for (int i = 0; i < keys.size(); i++) {
                String k = keys.get(i);
                String seq = k.substring(k.lastIndexOf(':') + 1);
                String v = vals.get(i);
                result.add(Map.of("seq", seq, "key", k, "value", v != null ? v : ""));
            }
        } catch (Exception ignored) {}
        return padTo10(result);
    }

    private List<Map<String, String>> padTo10(List<Map<String, String>> list) {
        while (list.size() < 10) list.add(Map.of("seq", "", "key", "", "value", ""));
        return list;
    }

    private long safeDbSize(JedisPool pool) {
        if (pool == null) return -1;
        try (var jedis = pool.getResource()) { return jedis.dbSize(); }
        catch (Exception e) { return -1; }
    }
}
