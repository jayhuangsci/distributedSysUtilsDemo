package io.github.jayhuangsci.corpus.hadoop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jayhuangsci.corpus.tools.CorpusInspector;
import io.github.jayhuangsci.corpus.tools.GenerateCorpus;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import static org.junit.jupiter.api.Assertions.*;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Native Hadoop filesystem utilities are not bundled; Linux CI executes these MapReduce jobs")
class PipelineIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path temporary;

    private static Configuration local() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///"); conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 16);
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 64);
        return conf;
    }

    private static String[] args(Path input, Path output, String... extra) {
        List<String> args = new ArrayList<>(List.of("--input", input.toString(), "--output", output.toString(),
                "--chunk-tokens", "12", "--chunk-overlap", "3", "--validation-percent", "50"));
        args.addAll(List.of(extra)); return args.toArray(new String[0]);
    }

    private static List<String> records(Path output) throws Exception {
        List<String> records = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(output)) {
            for (Path path : paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".jsonl") || p.toString().endsWith(".jsonl.gz")).collect(Collectors.toList())) {
                try (InputStream file = Files.newInputStream(path);
                     InputStream decoded = path.toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
                    String text = new String(decoded.readAllBytes(), StandardCharsets.UTF_8);
                    for (String line : text.split("\n")) { if (!line.isBlank()) { records.add(line); } }
                }
            }
        }
        Collections.sort(records); return records;
    }

    @Test void splitBoundariesDedupAndChunkIdentityAreIndependentOfReducerCount() throws Exception {
        Path raw = temporary.resolve("raw"); GenerateCorpus.generate(raw, 24, 4, 5, 42);
        Set<String> expectedIds = new HashSet<>();
        List<String> reference = null;
        Path evidence = Path.of("target/pipeline-evidence"); Files.createDirectories(evidence);
        for (int reducers : new int[]{1, 3}) {
            Path output = temporary.resolve("result-" + reducers);
            Map<String, Object> manifest = CorpusPipeline.run(local(), args(raw, output, "--reducers", String.valueOf(reducers)));
            JsonNode counters = JSON.valueToTree(manifest.get("counters"));
            assertEquals(24, counters.path("documents_read").asInt());
            assertEquals(20, counters.path("unique_documents").asInt());
            assertEquals(4, counters.path("duplicates_removed").asInt());
            List<String> actual = records(output);
            assertFalse(actual.isEmpty());
            if (reference != null) { assertEquals(reference, actual); } else { reference = actual; }
            for (String line : actual) {
                JsonNode record = JSON.readTree(line);
                expectedIds.add(record.path("document_id").asText());
                assertFalse(record.path("text").asText().contains("@example.invalid"));
                assertFalse(record.path("text").asText().contains("tracking()"));
                assertFalse(record.path("text").asText().contains("202-555"));
                assertTrue(record.path("token_count").asInt() <= 12);
            }
            assertEquals(20, expectedIds.size());
            assertEquals("PASS", CorpusInspector.inspect(output).get("status"));
            Files.copy(output.resolve("_MANIFEST.json"), evidence.resolve("reducers-" + reducers + "-manifest.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.write(evidence.resolve("reducers-" + reducers + "-records.jsonl"), actual, StandardCharsets.UTF_8);
        }
    }

    @Test void countsInvalidUtf8OversizedShortEmptyAndNoisyDocumentsWithoutPartialRows() throws Exception {
        Path raw = temporary.resolve("quality.txt");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write("A useful text document contains enough explanatory words for preparing a training example.\n".getBytes(StandardCharsets.UTF_8));
        bytes.write("x".repeat(300).getBytes(StandardCharsets.UTF_8)); bytes.write('\n');
        bytes.write(new byte[]{(byte) 0xC3, 0x28, '\n'});
        bytes.write("menu\n\n1234567890 !!!!! 1234567890 !!!!! 1234567890 !!!!!\n".getBytes(StandardCharsets.UTF_8));
        Files.write(raw, bytes.toByteArray());
        Path output = temporary.resolve("quality-output");
        Map<String, Object> manifest = CorpusPipeline.run(local(), args(raw, output, "--max-document-bytes", "128"));
        JsonNode counters = JSON.valueToTree(manifest.get("counters"));
        assertEquals(6, counters.path("documents_read").asInt());
        for (String key : List.of("accepted_documents", "oversized_documents", "invalid_utf8_documents", "too_short_documents", "empty_documents", "low_letter_ratio_documents")) {
            assertEquals(1, counters.path(key).asInt(), key);
        }
        assertEquals("PASS", CorpusInspector.inspect(output).get("status"));
    }

    @Test void wholeFileHtmlAndPlainTextAreSupported() throws Exception {
        Path output = temporary.resolve("files");
        Map<String, Object> manifest = CorpusPipeline.run(local(), args(Path.of("sample_input/documents"), output, "--input-mode", "files"));
        JsonNode counters = JSON.valueToTree(manifest.get("counters"));
        assertEquals(2, counters.path("documents_read").asInt()); assertEquals(2, counters.path("unique_documents").asInt());
        String data = String.join("\n", records(output));
        assertFalse(data.contains("tracking_payload")); assertFalse(data.contains("Menu Subscribe"));
        assertEquals("PASS", CorpusInspector.inspect(output).get("status"));
    }

    @Test void readsGzipAndWritesCompressedJsonLines() throws Exception {
        Path raw = temporary.resolve("input.txt.gz");
        try (GZIPOutputStream stream = new GZIPOutputStream(Files.newOutputStream(raw))) {
            stream.write(Files.readAllBytes(Path.of("sample_input/raw-documents.txt")));
        }
        Path output = temporary.resolve("compressed");
        CorpusPipeline.run(local(), args(raw, output, "--compression", "gzip"));
        assertFalse(records(output).isEmpty());
        try (Stream<Path> paths = Files.walk(output)) {
            assertTrue(paths.anyMatch(p -> p.toString().endsWith(".jsonl.gz")));
        }
        assertEquals("PASS", CorpusInspector.inspect(output).get("status"));
    }

    @Test void protectsExistingOutputAndCleansFailureWorkspace() throws Exception {
        Path output = temporary.resolve("protected"); Files.createDirectory(output);
        Files.writeString(output.resolve("keep.txt"), "preserved");
        assertThrows(Exception.class, () -> CorpusPipeline.run(local(), args(Path.of("sample_input/raw-documents.txt"), output)));
        assertEquals("preserved", Files.readString(output.resolve("keep.txt")));
        Path absent = temporary.resolve("not-published");
        assertThrows(Exception.class, () -> CorpusPipeline.run(local(), args(temporary.resolve("missing-input"), absent)));
        assertFalse(Files.exists(absent));
        try (Stream<Path> paths = Files.list(temporary)) {
            assertFalse(paths.anyMatch(p -> p.getFileName().toString().contains("-work-")));
        }
    }

    @Test void emptyInputPublishesAnAuditableEmptyDataset() throws Exception {
        Path raw = temporary.resolve("empty.txt"); Files.writeString(raw, "");
        Path output = temporary.resolve("empty-output");
        CorpusPipeline.run(local(), args(raw, output));
        assertEquals(0, records(output).size());
        assertEquals("PASS", CorpusInspector.inspect(output).get("status"));
    }
}
