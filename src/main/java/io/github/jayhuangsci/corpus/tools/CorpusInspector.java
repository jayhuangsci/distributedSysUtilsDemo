package io.github.jayhuangsci.corpus.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/** Local/demo audit. Stores IDs, not corpus text; use Hadoop counters for very large datasets. */
public final class CorpusInspector {
    private static final ObjectMapper JSON = new ObjectMapper();
    private CorpusInspector() { }

    public static Map<String, Object> inspect(Path directory) throws IOException {
        if (!Files.exists(directory.resolve("_SUCCESS")) || !Files.exists(directory.resolve("_MANIFEST.json"))) {
            throw new IOException("Dataset has no complete publication markers");
        }
        Set<String> ids = new HashSet<>();
        Map<String, String> documentSplits = new HashMap<>();
        Map<String, Object> report = new LinkedHashMap<>();
        long tokenCount = 0;
        for (String split : List.of("train", "validation")) {
            long records = 0;
            List<Path> shards;
            try (Stream<Path> paths = Files.walk(directory.resolve(split))) {
                shards = paths.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".jsonl")
                        || p.getFileName().toString().endsWith(".jsonl.gz")).collect(Collectors.toList());
            }
            for (Path shard : shards) {
                try (InputStream source = Files.newInputStream(shard);
                     InputStream decoded = shard.toString().endsWith(".gz") ? new GZIPInputStream(source) : source;
                     BufferedReader reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        JsonNode record = JSON.readTree(line);
                        if (record == null || !record.path("schema_version").asText().equals("1.0")
                                || !record.path("split").asText().equals(split)
                                || !record.path("id").asText().matches("[0-9a-f]{64}")
                                || !record.path("document_id").asText().matches("[0-9a-f]{64}")
                                || !record.path("source").path("id").asText().matches("[0-9a-f]{64}")
                                || record.path("text").asText().isBlank() || !record.path("tokens").isArray()
                                || record.path("token_count").asInt() < 1
                                || record.path("token_count").asInt() != record.path("tokens").size()
                                || record.path("token_end").asInt() - record.path("token_start").asInt() != record.path("token_count").asInt()
                                || record.path("source_occurrences").asLong() < 1) {
                            throw new IOException("Invalid training record in " + shard);
                        }
                        if (!ids.add(record.path("id").asText())) { throw new IOException("Duplicate chunk ID in published dataset"); }
                        String previous = documentSplits.putIfAbsent(record.path("document_id").asText(), split);
                        if (previous != null && !previous.equals(split)) { throw new IOException("Document leakage across train/validation splits"); }
                        records++; tokenCount += record.path("token_count").asLong();
                    }
                }
            }
            report.put(split + "_chunks", records);
        }
        report.put("unique_documents", documentSplits.size()); report.put("tokens_including_overlap", tokenCount);
        report.put("status", "PASS");
        JsonNode counters = JSON.readTree(directory.resolve("_MANIFEST.json").toFile()).path("counters");
        if (counters.path("unique_documents").asLong(-1) != documentSplits.size()
                || counters.path("tokens_written").asLong(-1) != tokenCount
                || counters.path("train_chunks").asLong(-1) != (Long) report.get("train_chunks")
                || counters.path("validation_chunks").asLong(-1) != (Long) report.get("validation_chunks")) {
            throw new IOException("Published records do not match the job counters");
        }
        return report;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) { throw new IllegalArgumentException("inspect <local-dataset-directory>"); }
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(inspect(Path.of(args[0]))));
    }
}
