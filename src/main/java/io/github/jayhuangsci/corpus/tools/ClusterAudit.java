package io.github.jayhuangsci.corpus.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Verify real YARN job history and execution on multiple registered workers. */
public final class ClusterAudit {
    private static final ObjectMapper JSON = new ObjectMapper();
    private ClusterAudit() { }

    public static Map<String, Object> audit(Path manifestPath, String resourceManager, String historyServer) throws Exception {
        JsonNode manifest = JSON.readTree(manifestPath.toFile());
        if (!manifest.path("framework").asText().equals("yarn") || manifest.path("jobs").size() != 2) {
            throw new IOException("Expected two completed YARN jobs in the dataset manifest");
        }
        Set<String> registered = new TreeSet<>();
        for (JsonNode node : array(get(resourceManager + "/ws/v1/cluster/nodes").path("nodes").path("node"))) {
            if (node.path("state").asText().equals("RUNNING")) { registered.add(node.path("nodeHostName").asText()); }
        }
        if (registered.size() < 2) { throw new IOException("Fewer than two YARN workers are RUNNING"); }
        Set<String> hosts = new TreeSet<>();
        List<Map<String, Object>> attempts = new ArrayList<>();
        List<String> completed = new ArrayList<>();
        for (JsonNode job : manifest.path("jobs")) {
            String id = job.path("job_id").asText();
            if (!id.matches("job_[0-9]+_[0-9]+")) { throw new IOException("Job did not execute through a YARN cluster: " + id); }
            String base = historyServer + "/ws/v1/history/mapreduce/jobs/" + id;
            JsonNode details = awaitHistory(base).path("job");
            if (!details.path("state").asText().equals("SUCCEEDED") || details.path("mapsCompleted").asInt() < 1) {
                throw new IOException("History server did not confirm successful map execution: " + id);
            }
            completed.add(id);
            for (JsonNode task : array(get(base + "/tasks").path("tasks").path("task"))) {
                String taskId = task.path("id").asText();
                for (JsonNode attempt : array(get(base + "/tasks/" + taskId + "/attempts").path("taskAttempts").path("taskAttempt"))) {
                    if (!attempt.path("state").asText().equals("SUCCEEDED")) { continue; }
                    String address = attempt.path("nodeHttpAddress").asText();
                    String host = address.split(":", 2)[0];
                    if (!registered.contains(host)) { throw new IOException("Task execution host is not a registered worker: " + host); }
                    hosts.add(host);
                    Map<String, Object> evidence = new LinkedHashMap<>();
                    evidence.put("job_id", id); evidence.put("task_id", taskId);
                    evidence.put("attempt_id", attempt.path("id").asText());
                    evidence.put("type", task.path("type").asText()); evidence.put("host", host);
                    attempts.add(evidence);
                }
            }
        }
        if (hosts.size() < 2) { throw new IOException("The workload did not execute successful tasks on both workers"); }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("status", "PASS"); report.put("framework", "yarn");
        report.put("registered_workers", registered); report.put("execution_hosts", hosts);
        report.put("successful_jobs", completed); report.put("successful_task_attempts", attempts);
        report.put("corpus_counters", manifest.path("counters"));
        return report;
    }

    private static List<JsonNode> array(JsonNode value) {
        List<JsonNode> result = new ArrayList<>();
        if (value.isArray()) { value.forEach(result::add); }
        else if (value.isObject()) { result.add(value); }
        return result;
    }

    private static JsonNode awaitHistory(String url) throws Exception {
        IOException last = null;
        for (int attempt = 0; attempt < 60; attempt++) {
            try { return get(url); }
            catch (IOException e) { last = e; Thread.sleep(2000); }
        }
        throw new IOException("Completed job was not available from the history server", last);
    }

    private static JsonNode get(String url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        connection.setConnectTimeout(5000); connection.setReadTimeout(5000);
        connection.setRequestProperty("Accept", "application/json");
        try {
            if (connection.getResponseCode() != 200) { throw new IOException("Cluster API returned HTTP " + connection.getResponseCode() + " for " + url); }
            try (InputStream stream = connection.getInputStream()) { return JSON.readTree(stream); }
        } finally { connection.disconnect(); }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) { throw new IllegalArgumentException("ClusterAudit <manifest.json> <resource-manager-url> <history-server-url> <report.json>"); }
        Map<String, Object> report = audit(Path.of(args[0]), args[1], args[2]);
        JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[3]).toFile(), report);
        System.out.println("Verified YARN execution on workers " + report.get("execution_hosts"));
    }
}
