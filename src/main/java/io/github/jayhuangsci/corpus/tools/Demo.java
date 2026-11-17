package io.github.jayhuangsci.corpus.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jayhuangsci.corpus.hadoop.CorpusPipeline;
import org.apache.hadoop.conf.Configuration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

public final class Demo {
    private Demo() { }
    public static void main(String[] args) throws Exception {
        if (args.length > 2) { throw new IllegalArgumentException("demo [raw-input] [new-output-directory]"); }
        String input = args.length > 0 ? args[0] : "sample_input/raw-documents.txt";
        String output = args.length > 1 ? args[1] : "demo-output/run-" + Instant.now().toEpochMilli();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///"); conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 16);
        CorpusPipeline.run(conf, new String[]{"--input", input, "--output", output,
                "--reducers", "2", "--chunk-tokens", "32", "--chunk-overlap", "8", "--validation-percent", "20"});
        Map<String, Object> report = CorpusInspector.inspect(Path.of(output));
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(output, "_INSPECTION.json").toFile(), report);
        System.out.println("Demo inspection: " + report);
    }
}
