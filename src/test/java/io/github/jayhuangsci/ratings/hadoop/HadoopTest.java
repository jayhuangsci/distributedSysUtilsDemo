package io.github.jayhuangsci.ratings.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Hadoop local filesystem requires native Windows utilities; Linux CI runs these jobs")
class HadoopTest {
    @TempDir Path temporary;
    static Configuration local() {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("mapreduce.task.io.sort.mb", 16);
        return conf;
    }
    static List<String> lines(Path output) throws IOException {
        List<String> result = new ArrayList<>();
        try (Stream<Path> files = Files.list(output)) {
            for (Path part : files.filter(p -> p.getFileName().toString().startsWith("part-")).collect(Collectors.toList())) {
                result.addAll(Files.readAllLines(part));
            }
        }
        Collections.sort(result);
        return result;
    }
    @Test void originalFourTasksMatchSuppliedGoldenFiles() throws Exception {
        String[] tasks = {"top-raters", "total", "user-counts", "agreement"};
        for (int i = 0; i < tasks.length; i++) {
            Path output = temporary.resolve(tasks[i]);
            HadoopAnalytics.run(local(), new String[]{"--task", tasks[i], "--input", "sample_input/smalldata.txt", "--output", output.toString(), "--partitions", "2"});
            List<String> expected = Files.readAllLines(Path.of("sample_output/Task" + (i + 1) + "_sample.txt"));
            Collections.sort(expected);
            assertEquals(expected, lines(output), tasks[i]);
            assertTrue(Files.exists(output.resolve("_RUN.json")));
        }
    }
    @Test void failedValidationAndExistingOutputNeverPublishOrReplaceResults() throws Exception {
        Path input = temporary.resolve("bad.csv");
        Files.writeString(input, "Movie,5,invalid\n");
        Path output = temporary.resolve("result");
        assertThrows(Exception.class, () -> HadoopAnalytics.run(local(), new String[]{"--task", "movie-stats", "--input", input.toString(), "--output", output.toString()}));
        assertFalse(Files.exists(output));
        Files.createDirectory(output);
        Files.writeString(output.resolve("keep.txt"), "preserve me");
        assertThrows(IOException.class, () -> HadoopAnalytics.run(local(), new String[]{"--task", "total", "--input", input.toString(), "--output", output.toString()}));
        assertEquals("preserve me", Files.readString(output.resolve("keep.txt")));
        try (Stream<Path> siblings = Files.list(temporary)) { assertFalse(siblings.anyMatch(p -> p.getFileName().toString().contains("-work-"))); }
    }
}
