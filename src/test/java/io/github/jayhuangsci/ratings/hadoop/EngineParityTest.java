package io.github.jayhuangsci.ratings.hadoop;

import io.github.jayhuangsci.ratings.core.Csv;
import io.github.jayhuangsci.ratings.core.MovieRow;
import io.github.jayhuangsci.ratings.spark.SparkAnalytics;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Distributed local runners are verified on Linux CI, without third-party winutils downloads")
class EngineParityTest {
    @TempDir Path temporary;
    private static SparkContext spark;

    @BeforeAll static void startSpark() {
        spark = new SparkContext(new SparkConf().setAppName("ratings-parity-tests").setMaster("local[2]")
                .set("spark.ui.enabled", "false").set("spark.driver.host", "127.0.0.1")
                .set("spark.driver.bindAddress", "127.0.0.1").set("spark.default.parallelism", "2"));
        spark.setLogLevel("ERROR");
    }
    @AfterAll static void stopSpark() { if (spark != null) { spark.stop(); } }

    @Test void everyAnalyticsTaskMatchesAnIndependentOracleAcrossPartitionCounts() throws Exception {
        List<String> inputLines = Files.readAllLines(Path.of("src/test/resources/ratings-edge-cases.csv"));
        Path input = temporary.resolve("input");
        Files.createDirectories(input);
        for (int i = 0; i < inputLines.size(); i++) { Files.writeString(input.resolve("movie-" + i + ".csv"), inputLines.get(i) + "\n"); }
        List<MovieRow> rows = inputLines.stream().map(MovieRow::parse).collect(Collectors.toList());
        Path evidence = Path.of("target/parity-evidence");
        Files.createDirectories(evidence);
        for (int partitions : new int[]{1, 3}) {
            for (String task : Arrays.asList("top-raters", "total", "user-counts", "agreement", "movie-stats", "recommend")) {
                Path hadoop = temporary.resolve("hadoop-" + task + "-" + partitions);
                Path scala = temporary.resolve("spark-" + task + "-" + partitions);
                HadoopAnalytics.run(HadoopTest.local(), args(task, input, hadoop, partitions));
                SparkAnalytics.run(spark, args(task, input, scala, partitions));
                List<String> expected = oracle(rows, task);
                assertEquals(expected, HadoopTest.lines(hadoop), "Hadoop " + task + "/" + partitions);
                assertEquals(expected, HadoopTest.lines(scala), "Spark " + task + "/" + partitions);
                Files.write(evidence.resolve(task + "-p" + partitions + ".csv"), expected);
                Files.copy(hadoop.resolve("_RUN.json"), evidence.resolve(task + "-p" + partitions + "-hadoop.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                Files.copy(scala.resolve("_RUN.json"), evidence.resolve(task + "-p" + partitions + "-spark.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
    @Test void wordCountHandlesWhitespaceCaseAndUnicodeIdentically() throws Exception {
        Path input = temporary.resolve("words.txt");
        Files.writeString(input, "Scala, scala!\tHADOOP\nHadoop café café 42\n");
        Path hadoop = temporary.resolve("wc-hadoop"), scala = temporary.resolve("wc-spark");
        HadoopAnalytics.run(HadoopTest.local(), args("word-count", input, hadoop, 3));
        SparkAnalytics.run(spark, args("word-count", input, scala, 3));
        List<String> expected = Arrays.asList("42,1", "café,2", "hadoop,2", "scala,2");
        assertEquals(expected, HadoopTest.lines(hadoop));
        assertEquals(expected, HadoopTest.lines(scala));
    }
    @Test void skipPolicyCountsRejectedRowsAndEmptyTotalIsZero() throws Exception {
        Path input = temporary.resolve("mixed.csv");
        Files.writeString(input, "Valid,5,,3\nInvalid,4,7\n");
        for (String engine : Arrays.asList("hadoop", "spark")) {
            Path output = temporary.resolve("skip-" + engine);
            List<String> options = new ArrayList<>(Arrays.asList(args("total", input, output, 2)));
            options.addAll(Arrays.asList("--bad-records", "skip"));
            run(engine, options.toArray(new String[0]));
            assertEquals(Collections.singletonList("2"), HadoopTest.lines(output));
            String metrics = Files.readString(output.resolve("_RUN.json"));
            assertTrue(metrics.contains("invalid_rows\": 1"), metrics);
            assertTrue(metrics.contains("rating_events\": 2"), metrics);
        }
        Path empty = temporary.resolve("empty.csv");
        Files.writeString(empty, "");
        for (String engine : Arrays.asList("hadoop", "spark")) {
            Path output = temporary.resolve("empty-" + engine);
            run(engine, args("total", empty, output, 2));
            assertEquals(Collections.singletonList("0"), HadoopTest.lines(output));
        }
    }
    @Test void pairExpansionLimitFailsWithoutPublishingInEitherEngine() throws Exception {
        Path input = temporary.resolve("limit.csv");
        Files.writeString(input, "A,5\nB,5\nC,5\n");
        for (String engine : Arrays.asList("hadoop", "spark")) {
            Path output = temporary.resolve("limit-" + engine);
            List<String> options = new ArrayList<>(Arrays.asList(args("agreement", input, output, 2)));
            options.addAll(Arrays.asList("--max-movies-per-user", "2"));
            assertThrows(Exception.class, () -> run(engine, options.toArray(new String[0])));
            assertFalse(Files.exists(output));
        }
    }
    private static void run(String engine, String[] args) throws Exception {
        if (engine.equals("hadoop")) { HadoopAnalytics.run(HadoopTest.local(), args); }
        else { SparkAnalytics.run(spark, args); }
    }
    private static String[] args(String task, Path input, Path output, int partitions) {
        return new String[]{"--task", task, "--input", input.toString(), "--output", output.toString(),
                "--partitions", String.valueOf(partitions), "--top-k", "2", "--min-support", "1"};
    }

    // Deliberately small, driver-only specification: compare movie rows directly,
    // independently of either engine's user-side join and bounded shuffle state.
    private static List<String> oracle(List<MovieRow> rows, String task) {
        List<String> output = new ArrayList<>();
        if (task.equals("total")) { output.add(String.valueOf(rows.stream().mapToLong(r -> r.ratings().size()).sum())); }
        if (task.equals("top-raters") || task.equals("movie-stats")) {
            for (MovieRow row : rows) {
                int max = row.ratings().stream().mapToInt(MovieRow.Rating::score).max().orElse(0);
                if (task.equals("top-raters")) {
                    String users = row.ratings().stream().filter(r -> r.score() == max).map(r -> String.valueOf(r.user())).collect(Collectors.joining(","));
                    output.add(Csv.escape(row.title()) + (users.isEmpty() ? "" : "," + users));
                } else {
                    long sum = row.ratings().stream().mapToLong(MovieRow.Rating::score).sum();
                    int count = row.ratings().size();
                    int min = row.ratings().stream().mapToInt(MovieRow.Rating::score).min().orElse(0);
                    output.add(Csv.row(row.title(), count, sum, String.format(Locale.ROOT, "%.6f", count == 0 ? 0.0 : (double) sum / count), min, max));
                }
            }
        }
        if (task.equals("user-counts")) {
            Map<Integer, Long> counts = new TreeMap<>();
            for (MovieRow row : rows) { for (MovieRow.Rating rating : row.ratings()) { counts.merge(rating.user(), 1L, Long::sum); } }
            counts.forEach((user, count) -> output.add(Csv.row(user, count)));
        }
        if (task.equals("agreement") || task.equals("recommend")) {
            Map<String, Map<String, Long>> neighbors = new HashMap<>();
            for (int i = 0; i < rows.size(); i++) {
                for (int j = i + 1; j < rows.size(); j++) {
                    MovieRow a = rows.get(i), b = rows.get(j);
                    long support = 0;
                    for (MovieRow.Rating ar : a.ratings()) {
                        for (MovieRow.Rating br : b.ratings()) {
                            if (ar.user() == br.user() && ar.score() == br.score()) { support++; }
                        }
                    }
                    if (support == 0) { continue; }
                    if (task.equals("agreement")) {
                        output.add(a.title().compareTo(b.title()) < 0 ? Csv.row(a.title(), b.title(), support) : Csv.row(b.title(), a.title(), support));
                    }
                    neighbors.computeIfAbsent(a.title(), ignored -> new HashMap<>()).put(b.title(), support);
                    neighbors.computeIfAbsent(b.title(), ignored -> new HashMap<>()).put(a.title(), support);
                }
            }
            if (task.equals("recommend")) {
                neighbors.forEach((title, candidates) -> {
                    List<Map.Entry<String, Long>> sorted = new ArrayList<>(candidates.entrySet());
                    sorted.sort(Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue).reversed().thenComparing(Map.Entry::getKey));
                    for (int i = 0; i < Math.min(2, sorted.size()); i++) { output.add(Csv.row(title, sorted.get(i).getKey(), sorted.get(i).getValue(), i + 1)); }
                });
            }
        }
        Collections.sort(output);
        return output;
    }
}
