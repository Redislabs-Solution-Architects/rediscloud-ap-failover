package com.redis.failoverdemo;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Orchestrates Active-Passive failover with structured step reporting.
 */
public class FailoverOrchestrator {

    private final RedisConnectionManager connMgr;
    private final RedisCloudApiClient apiClient;
    private final Runnable onMaintenanceStart;
    private final Runnable onMaintenanceEnd;

    // Structured steps list shared with controller
    private final CopyOnWriteArrayList<Map<String, Object>> steps;

    public FailoverOrchestrator(RedisConnectionManager connMgr,
                                RedisCloudApiClient apiClient,
                                CopyOnWriteArrayList<Map<String, Object>> steps,
                                Runnable onMaintenanceStart,
                                Runnable onMaintenanceEnd) {
        this.connMgr = connMgr;
        this.apiClient = apiClient;
        this.steps = steps;
        this.onMaintenanceStart = onMaintenanceStart;
        this.onMaintenanceEnd = onMaintenanceEnd;
    }

    /** Initialize the 4 steps in "pending" state with preview text. */
    public void initSteps() {
        String from = connMgr.getActiveDb();
        String to = "A".equals(from) ? "B" : "A";
        steps.clear();
        steps.add(stepMap(1, "Pause Traffic", "pending", "", List.of()));
        steps.add(stepMap(2, "Disable Replication", "pending", "", List.of()));
        steps.add(stepMap(3, "Switch Writes to Database-" + to, "pending", "", List.of()));
        steps.add(stepMap(4, "Resume Traffic", "pending", "", List.of()));
    }

    public void executeAsync(Consumer<Long> onComplete) {
        new Thread(() -> {
            try {
                execute(onComplete);
            } catch (Exception e) {
                updateStep(currentStep(), "error", "Error: " + e.getMessage(), null);
                onMaintenanceEnd.run();
            }
        }, "failover-thread").start();
    }

    private int currentStep() {
        for (int i = 0; i < steps.size(); i++) {
            if ("active".equals(steps.get(i).get("state"))) return i;
        }
        return 0;
    }

    private void execute(Consumer<Long> onComplete) throws Exception {
        long start = System.currentTimeMillis();
        String from = connMgr.getActiveDb();
        String to = "A".equals(from) ? "B" : "A";

        // Step 1: Pause traffic
        updateStep(0, "active", "Pausing Traffic...", null);
        onMaintenanceStart.run();
        updateStep(0, "done", "Traffic Disabled", null);

        // Step 2: Disable replication
        String dir = apiClient.getReplicationDirection();
        String replicaLabel, replicaSub, replicaDbId;
        if ("A_TO_B".equals(dir)) {
            replicaLabel = "Database-B"; replicaSub = apiClient.getSubIdB(); replicaDbId = apiClient.getDbIdB();
        } else {
            replicaLabel = "Database-A"; replicaSub = apiClient.getSubIdA(); replicaDbId = apiClient.getDbIdA();
        }
        String apiPath = "PUT /subscriptions/" + replicaSub + "/databases/" + replicaDbId + " (" + replicaLabel + ")";

        updateStep(1, "active", "Disabling Replication...",
            List.of(apiPath, "Body: {\"replicaOf\": []}",  "Waiting for confirmation..."));
        String taskId = apiClient.disableActiveReplication();
        boolean success = apiClient.waitForTask(taskId, 120_000);
        if (!success) {
            updateStep(1, "error", "Replication disable failed",
                List.of(apiPath, "Body: {\"replicaOf\": []}", "Failed or timed out"));
            onMaintenanceEnd.run();
            return;
        }
        updateStep(1, "done", "Replication Disabled",
            List.of(apiPath, "Body: {\"replicaOf\": []}", "Completed"));

        // Step 3: Switch writes
        updateStep(2, "active", "Switching Writes to Database-" + to + "...",
            List.of("connMgr.switchTo(\"" + to + "\")", "writer.initFromRedis()"));
        connMgr.switchTo(to);
        updateStep(2, "done", "Writes Switched to Database-" + to,
            List.of("connMgr.switchTo(\"" + to + "\") — done"));

        // Step 4: Resume traffic
        long elapsed = System.currentTimeMillis() - start;
        updateStep(3, "active", "Resuming Traffic...", null);
        onMaintenanceEnd.run();
        updateStep(3, "done", "Failover Completed in " + elapsed + "ms", null);

        if (onComplete != null) onComplete.accept(elapsed);
    }

    private void updateStep(int index, String state, String text, List<String> details) {
        Map<String, Object> step = new LinkedHashMap<>(steps.get(index));
        step.put("state", state);
        step.put("text", text);
        if (details != null) step.put("details", details);
        steps.set(index, step);
    }

    private Map<String, Object> stepMap(int num, String text, String state, String label, List<String> details) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("num", num);
        m.put("text", text);
        m.put("state", state);
        m.put("details", details);
        return m;
    }
}
