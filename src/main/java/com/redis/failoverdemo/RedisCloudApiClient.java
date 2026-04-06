package com.redis.failoverdemo;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * REST client for Redis Cloud API.
 * Supports dynamic credential setting and replication management.
 */
public class RedisCloudApiClient {

    private static final String DEFAULT_BASE_URL = "https://api.redislabs.com/v1";

    private volatile String baseUrl = DEFAULT_BASE_URL;
    private volatile String accountKey;
    private volatile String secretKey;
    private volatile String subIdA, dbIdA;
    private volatile String subIdB, dbIdB;
    private final HttpClient http;
    private final Gson gson = new Gson();

    public RedisCloudApiClient() {
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    }

    public void setCredentials(String accountKey, String secretKey) {
        this.accountKey = accountKey;
        this.secretKey = secretKey;
    }

    public boolean hasCredentials() { return accountKey != null && secretKey != null; }

    public boolean isFullyResolved() {
        return hasCredentials() && subIdA != null && dbIdA != null && subIdB != null && dbIdB != null;
    }

    public void setDbA(String subId, String dbId) { this.subIdA = subId; this.dbIdA = dbId; }
    public void setDbB(String subId, String dbId) { this.subIdB = subId; this.dbIdB = dbId; }
    public String getSubIdA() { return subIdA; }
    public String getDbIdA() { return dbIdA; }
    public String getSubIdB() { return subIdB; }
    public String getDbIdB() { return dbIdB; }

    /** Validate API credentials by hitting GET /v1/ (account root). Throws on failure. */
    public void validateCredentials() throws IOException, InterruptedException {
        apiGet("/");
    }

    /** Validate a specific database is accessible. Returns the database JSON. */
    public JsonObject validateDatabase(String subId, String dbId) throws IOException, InterruptedException {
        return apiGet("/subscriptions/" + subId + "/databases/" + dbId);
    }

    private JsonObject apiGet(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("x-api-key", accountKey)
                .header("x-api-secret-key", secretKey)
                .GET()
                .build();
        HttpResponse<String> resp = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IOException("API error " + resp.statusCode() + ": " + resp.body());
        }
        return gson.fromJson(resp.body(), JsonObject.class);
    }

    /**
     * Check replication direction by inspecting replicaOf on both databases.
     * Returns: "A_TO_B", "B_TO_A", or "none".
     */
    public String getReplicationDirection() throws IOException, InterruptedException {
        if (!isFullyResolved()) return "no_credentials";

        boolean bReplicatesA = hasReplicaOf(subIdB, dbIdB);
        if (bReplicatesA) return "A_TO_B";

        boolean aReplicatesB = hasReplicaOf(subIdA, dbIdA);
        if (aReplicatesB) return "B_TO_A";

        return "none";
    }

    private boolean hasReplicaOf(String subId, String dbId) throws IOException, InterruptedException {
        JsonObject json = apiGet("/subscriptions/" + subId + "/databases/" + dbId);
        JsonElement replicaOf = json.get("replicaOf");
        if (replicaOf == null || replicaOf.isJsonNull()) return false;
        if (replicaOf.isJsonObject()) {
            JsonElement endpoints = replicaOf.getAsJsonObject().get("endpoints");
            return endpoints != null && endpoints.isJsonArray() && endpoints.getAsJsonArray().size() > 0;
        }
        if (replicaOf.isJsonArray()) return replicaOf.getAsJsonArray().size() > 0;
        return false;
    }

    /** Enable replication: replicaDb becomes a replica of sourceDb. */
    public String enableReplication(String replicaSubId, String replicaDbId, String sourceUri)
            throws IOException, InterruptedException {
        return apiPut("/subscriptions/" + replicaSubId + "/databases/" + replicaDbId,
                "{\"replicaOf\": [\"" + sourceUri + "\"]}");
    }

    /** Enable replication by direction: "A_TO_B" means B replicates A, "B_TO_A" means A replicates B. */
    public String enableReplicationByDirection(String direction, String sourceUri)
            throws IOException, InterruptedException {
        if ("A_TO_B".equals(direction)) return enableReplication(subIdB, dbIdB, sourceUri);
        if ("B_TO_A".equals(direction)) return enableReplication(subIdA, dbIdA, sourceUri);
        throw new IOException("Invalid direction: " + direction);
    }

    /** Disable replication on a specific database. */
    public String disableReplication(String subId, String dbId) throws IOException, InterruptedException {
        return apiPut("/subscriptions/" + subId + "/databases/" + dbId, "{\"replicaOf\": []}");
    }

    // Convenience: disable on whichever DB is currently the replica
    public String disableActiveReplication() throws IOException, InterruptedException {
        String dir = getReplicationDirection();
        if ("A_TO_B".equals(dir)) return disableReplication(subIdB, dbIdB);
        if ("B_TO_A".equals(dir)) return disableReplication(subIdA, dbIdA);
        throw new IOException("No active replication to disable");
    }

    private String apiPut(String path, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("x-api-key", accountKey)
                .header("x-api-secret-key", secretKey)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) throw new IOException("API error " + resp.statusCode() + ": " + resp.body());
        JsonObject json = gson.fromJson(resp.body(), JsonObject.class);
        return json.get("taskId").getAsString();
    }

    /** Poll task until completed or error. */
    public boolean waitForTask(String taskId, long timeoutMs) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            String url = baseUrl + "/tasks/" + taskId;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("x-api-key", accountKey)
                    .header("x-api-secret-key", secretKey)
                    .GET()
                    .build();

            HttpResponse<String> resp = http.send(request, HttpResponse.BodyHandlers.ofString());
            JsonObject json = gson.fromJson(resp.body(), JsonObject.class);
            String status = json.get("status").getAsString();

            if ("processing-completed".equals(status)) return true;
            if ("processing-error".equals(status)) return false;

            Thread.sleep(2000);
        }
        return false;
    }
}
