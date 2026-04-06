package com.redis.failoverdemo;

import java.io.*;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads and writes configuration to a .env file in the project root.
 */
public class AppConfig {

    private static final Path ENV_PATH = Path.of(".env");
    private final Map<String, String> env = new LinkedHashMap<>();

    public AppConfig() {
        load();
    }

    private void load() {
        env.clear();
        File f = ENV_PATH.toFile();
        if (!f.exists()) return;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                int eq = line.indexOf('=');
                if (eq > 0) {
                    env.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
                }
            }
        } catch (Exception ignored) {}
    }

    public String get(String key) { return env.get(key); }
    public String get(String key, String defaultVal) { return env.getOrDefault(key, defaultVal); }

    public void set(String key, String value) {
        if (value != null && !value.isBlank()) {
            env.put(key, value);
        }
    }

    public void save() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(ENV_PATH.toFile()))) {
            for (var entry : env.entrySet()) {
                pw.println(entry.getKey() + "=" + entry.getValue());
            }
        } catch (Exception e) {
            System.err.println("Failed to save .env: " + e.getMessage());
        }
    }

    public Map<String, String> getAll() { return new LinkedHashMap<>(env); }
}
