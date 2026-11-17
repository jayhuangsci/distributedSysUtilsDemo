package io.github.jayhuangsci.corpus.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jayhuangsci.corpus.core.Hashes;
import io.github.jayhuangsci.corpus.core.PipelineOptions;
import io.github.jayhuangsci.corpus.core.TextProcessor;
import io.github.jayhuangsci.corpus.core.TextTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.CharacterCodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Two distributed jobs: prepare/deduplicate, then tokenize/chunk/export JSONL. */
public final class CorpusPipeline {
    public static final String VERSION = "2.0.0";
    private static final ObjectMapper JSON = new ObjectMapper();
    private CorpusPipeline() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) { System.out.println(PipelineOptions.usage()); return; }
        Configuration configuration = new Configuration();
        run(configuration, new GenericOptionsParser(configuration, args).getRemainingArgs());
    }

    public static Map<String, Object> run(Configuration original, String[] args) throws Exception {
        PipelineOptions options = PipelineOptions.parse(args);
        Configuration configuration = new Configuration(original);
        configuration.set("corpus.options.json", JSON.writeValueAsString(args));
        configuration.set("corpus.input.mode", options.inputMode());
        configuration.setInt("corpus.max.document.bytes", options.integer("max-document-bytes"));
        configuration.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true);
        long start = System.nanoTime();
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("schema_version", "1.0"); manifest.put("pipeline_version", VERSION);
        manifest.put("tokenizer", TextTokenizer.NAME); manifest.put("created_at", Instant.now().toString());
        manifest.put("framework", configuration.get("mapreduce.framework.name", "local"));
        manifest.put("input_id", Hashes.sha256(options.input())); manifest.put("parameters", options.metadata());
        List<Map<String, Object>> jobs = new ArrayList<>();
        try (OutputTransaction output = new OutputTransaction(configuration, options.output())) {
            Job prepare = Job.getInstance(configuration, "corpus: extract, clean, redact and deduplicate");
            prepare.setJarByClass(CorpusPipeline.class);
            prepare.setInputFormatClass(DocumentInputFormat.class);
            prepare.setMapperClass(PrepareMapper.class);
            prepare.setCombinerClass(DedupCombiner.class);
            prepare.setReducerClass(DedupReducer.class);
            prepare.setNumReduceTasks(options.integer("reducers"));
            prepare.setMapOutputKeyClass(Text.class); prepare.setMapOutputValueClass(PreparedDocumentWritable.class);
            prepare.setOutputKeyClass(Text.class); prepare.setOutputValueClass(PreparedDocumentWritable.class);
            prepare.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(prepare, new Path(options.input()));
            FileOutputFormat.setOutputPath(prepare, output.path("deduplicated"));
            jobs.add(execute(prepare, "prepare"));

            Job export = Job.getInstance(configuration, "corpus: tokenize and export training JSONL");
            export.setJarByClass(CorpusPipeline.class);
            export.setInputFormatClass(SequenceFileInputFormat.class);
            export.setMapperClass(ExportMapper.class); export.setNumReduceTasks(0);
            export.setOutputKeyClass(NullWritable.class); export.setOutputValueClass(Text.class);
            LazyOutputFormat.setOutputFormatClass(export, JsonLineOutputFormat.class);
            MultipleOutputs.addNamedOutput(export, "corpus", JsonLineOutputFormat.class, NullWritable.class, Text.class);
            if (options.compression().equals("gzip")) {
                FileOutputFormat.setCompressOutput(export, true);
                FileOutputFormat.setOutputCompressorClass(export, GzipCodec.class);
            }
            FileInputFormat.addInputPath(export, output.path("deduplicated"));
            FileOutputFormat.setOutputPath(export, output.path("result"));
            jobs.add(execute(export, "export"));
            manifest.put("jobs", jobs);
            Map<String, Long> counters = new LinkedHashMap<>();
            for (CorpusCounters counter : CorpusCounters.values()) {
                counters.put(counter.name().toLowerCase(Locale.ROOT), Math.addExact(
                        prepare.getCounters().findCounter(counter).getValue(), export.getCounters().findCounter(counter).getValue()));
            }
            manifest.put("counters", counters);
            manifest.put("elapsed_ms", (System.nanoTime() - start) / 1_000_000);
            output.publish(manifest);
        }
        System.out.println("Published JSONL corpus: " + options.output());
        System.out.println(JSON.writeValueAsString(manifest.get("counters")));
        return manifest;
    }

    private static Map<String, Object> execute(Job job, String stage) throws Exception {
        long start = System.nanoTime();
        if (!job.waitForCompletion(true)) { throw new IOException("MapReduce stage failed: " + stage); }
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("stage", stage); metrics.put("job_id", job.getJobID().toString());
        metrics.put("elapsed_ms", (System.nanoTime() - start) / 1_000_000);
        for (TaskCounter counter : new TaskCounter[]{TaskCounter.MAP_INPUT_RECORDS, TaskCounter.MAP_OUTPUT_RECORDS,
                TaskCounter.COMBINE_INPUT_RECORDS, TaskCounter.COMBINE_OUTPUT_RECORDS, TaskCounter.REDUCE_SHUFFLE_BYTES,
                TaskCounter.REDUCE_INPUT_RECORDS, TaskCounter.REDUCE_OUTPUT_RECORDS}) {
            metrics.put(counter.name().toLowerCase(Locale.ROOT), job.getCounters().findCounter(counter).getValue());
        }
        return metrics;
    }

    private static PipelineOptions options(Configuration configuration) throws IOException {
        return PipelineOptions.parse(JSON.readValue(configuration.get("corpus.options.json"), String[].class));
    }

    public static final class PrepareMapper extends Mapper<LongWritable, RawDocumentWritable, Text, PreparedDocumentWritable> {
        private PipelineOptions options;
        private String sourceId;
        @Override protected void setup(Context context) throws IOException {
            options = options(context.getConfiguration());
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            sourceId = Hashes.sha256(path.getFileSystem(context.getConfiguration()).makeQualified(path).toString());
        }

        @Override protected void map(LongWritable offset, RawDocumentWritable raw, Context context) throws IOException, InterruptedException {
            context.getCounter(CorpusCounters.DOCUMENTS_READ).increment(1);
            if (raw.oversized()) { context.getCounter(CorpusCounters.OVERSIZED_DOCUMENTS).increment(1); return; }
            String text;
            try { text = raw.decode(); }
            catch (CharacterCodingException e) { context.getCounter(CorpusCounters.INVALID_UTF8_DOCUMENTS).increment(1); return; }
            TextProcessor.Result result = TextProcessor.process(text, options);
            if (!result.accepted()) { context.getCounter(CorpusCounters.valueOf(result.rejection)).increment(1); return; }
            context.getCounter(CorpusCounters.ACCEPTED_DOCUMENTS).increment(1);
            for (Map.Entry<String, Integer> entry : result.redactions.entrySet()) {
                context.getCounter(CorpusCounters.valueOf("REDACTED_" + entry.getKey().toUpperCase(Locale.ROOT))).increment(entry.getValue());
            }
            context.write(new Text(Hashes.sha256(result.text)), new PreparedDocumentWritable(result, sourceId, offset.get()));
        }
    }

    private static PreparedDocumentWritable aggregate(Iterable<PreparedDocumentWritable> values) throws IOException {
        PreparedDocumentWritable result = null;
        for (PreparedDocumentWritable value : values) { result = result == null ? value.copy() : result.merge(value); }
        if (result == null) { throw new IOException("Empty deduplication group"); }
        return result;
    }

    public static final class DedupCombiner extends Reducer<Text, PreparedDocumentWritable, Text, PreparedDocumentWritable> {
        @Override protected void reduce(Text id, Iterable<PreparedDocumentWritable> values, Context context) throws IOException, InterruptedException {
            context.write(id, aggregate(values));
        }
    }

    public static final class DedupReducer extends Reducer<Text, PreparedDocumentWritable, Text, PreparedDocumentWritable> {
        @Override protected void reduce(Text id, Iterable<PreparedDocumentWritable> values, Context context) throws IOException, InterruptedException {
            PreparedDocumentWritable result = aggregate(values);
            context.getCounter(CorpusCounters.UNIQUE_DOCUMENTS).increment(1);
            context.getCounter(CorpusCounters.DUPLICATES_REMOVED).increment(result.occurrences() - 1);
            context.write(id, result);
        }
    }

    public static final class ExportMapper extends Mapper<Text, PreparedDocumentWritable, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> outputs;
        private PipelineOptions options;
        @Override protected void setup(Context context) throws IOException {
            outputs = new MultipleOutputs<>(context); options = options(context.getConfiguration());
        }

        @Override protected void map(Text key, PreparedDocumentWritable document, Context context) throws IOException, InterruptedException {
            String documentId = key.toString();
            String split = Hashes.split(documentId, options.integer("validation-percent"));
            context.getCounter(split.equals("train") ? CorpusCounters.TRAIN_DOCUMENTS : CorpusCounters.VALIDATION_DOCUMENTS).increment(1);
            try {
                TextTokenizer.chunks(document.text(), options.integer("chunk-tokens"), options.integer("chunk-overlap"), chunk -> {
                    try {
                        Map<String, Object> record = new LinkedHashMap<>();
                        record.put("schema_version", "1.0");
                        record.put("id", Hashes.sha256(documentId + ":" + chunk.index + ":" + options.fingerprint()));
                        record.put("document_id", documentId); record.put("text", chunk.text);
                        record.put("tokens", chunk.tokens); record.put("token_count", chunk.tokens.size());
                        record.put("tokenizer", TextTokenizer.NAME); record.put("split", split);
                        record.put("chunk_index", chunk.index); record.put("token_start", chunk.tokenStart); record.put("token_end", chunk.tokenEnd);
                        record.put("source_occurrences", document.occurrences());
                        record.put("source", Map.of("id", document.sourceId(), "offset", document.sourceOffset()));
                        record.put("document_redactions", document.redactions());
                        outputs.write("corpus", NullWritable.get(), new Text(JSON.writeValueAsString(record)), split + "/part");
                        context.getCounter(split.equals("train") ? CorpusCounters.TRAIN_CHUNKS : CorpusCounters.VALIDATION_CHUNKS).increment(1);
                        context.getCounter(CorpusCounters.TOKENS_WRITTEN).increment(chunk.tokens.size());
                    } catch (IOException e) { throw new UncheckedIOException(e); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new ExportInterrupted(e); }
                });
            } catch (UncheckedIOException e) { throw e.getCause(); }
            catch (ExportInterrupted e) { throw (InterruptedException) e.getCause(); }
        }

        @Override protected void cleanup(Context context) throws IOException, InterruptedException { outputs.close(); }
    }

    private static final class ExportInterrupted extends RuntimeException {
        ExportInterrupted(InterruptedException cause) { super(cause); }
    }
}
