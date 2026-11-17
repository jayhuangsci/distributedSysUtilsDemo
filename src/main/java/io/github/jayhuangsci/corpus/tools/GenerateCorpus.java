package io.github.jayhuangsci.corpus.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/** Seeded synthetic text, distributed over shards with deliberate cross-file duplicates. */
public final class GenerateCorpus {
    private GenerateCorpus() { }
    public static void main(String[] args) throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        Set<String> allowed = Set.of("--output", "--documents", "--shards", "--duplicate-every", "--seed");
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length || !allowed.contains(args[i]) || values.putIfAbsent(args[i], args[i + 1]) != null) {
                throw new IllegalArgumentException("generate --output <new-directory> [--documents 1000] [--shards 8] [--duplicate-every 10] [--seed 42]");
            }
        }
        if (!values.containsKey("--output")) { throw new IllegalArgumentException("Missing --output"); }
        generate(Path.of(values.get("--output")), Integer.parseInt(values.getOrDefault("--documents", "1000")),
                Integer.parseInt(values.getOrDefault("--shards", "8")), Integer.parseInt(values.getOrDefault("--duplicate-every", "10")),
                Long.parseLong(values.getOrDefault("--seed", "42")));
    }

    public static void generate(Path directory, int documents, int shards, int duplicateEvery, long seed) throws Exception {
        if (documents < 1 || shards < 1 || shards > documents || shards > 1000 || duplicateEvery < 0) {
            throw new IllegalArgumentException("Positive documents/shards, at most 1000 shards, and a nonnegative duplicate interval are required");
        }
        Path output = directory.toAbsolutePath();
        Files.createDirectories(output.getParent()); Files.createDirectory(output);
        List<BufferedWriter> writers = new ArrayList<>();
        Random random = new Random(seed);
        int duplicates = 0;
        String previous = "";
        String[] topics = {"distributed storage", "language model training", "reliable data processing", "parallel computation", "dataset quality"};
        try {
            for (int shard = 0; shard < shards; shard++) {
                writers.add(Files.newBufferedWriter(output.resolve(String.format(Locale.ROOT, "raw-%04d.txt", shard)), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW));
            }
            for (int i = 0; i < documents; i++) {
                String text;
                if (i > 0 && duplicateEvery > 0 && i % duplicateEvery == 0) {
                    text = previous; duplicates++;
                } else {
                    text = String.format(Locale.ROOT,
                            "Document %08d discusses %s. Durable systems process independent text records across workers, validate their quality, and publish repeatable training datasets. "
                                    + "Examples describe fault recovery, data locality, and consistent output schemas. Contact sample%d@example.invalid or call 202-555-%04d. "
                                    + "The reference endpoint is https://example.invalid/docs?token=synthetic%d and the sample host address is 192.0.2.42.",
                            i, topics[random.nextInt(topics.length)], i, i % 10000, i);
                    if (i % 7 == 0) { text = "<html><nav>Menu</nav><article><p>" + text + "</p></article><script>tracking()</script></html>"; }
                    previous = text;
                }
                BufferedWriter writer = writers.get(i % shards);
                writer.write(text); writer.write('\n');
            }
        } finally {
            for (BufferedWriter writer : writers) { writer.close(); }
        }
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("documents", documents); metadata.put("shards", shards); metadata.put("seed", seed);
        metadata.put("exact_duplicate_inputs", duplicates); metadata.put("expected_unique_documents", documents - duplicates);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.resolve("_GENERATOR.json").toFile(), metadata);
        System.out.println("Generated " + documents + " raw documents across " + shards + " files; " + duplicates + " deliberate duplicates");
    }
}
