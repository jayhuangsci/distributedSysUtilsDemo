package io.github.jayhuangsci.ratings.hadoop;

import io.github.jayhuangsci.ratings.core.Csv;
import io.github.jayhuangsci.ratings.core.MovieRow;
import io.github.jayhuangsci.ratings.core.MovieStats;
import io.github.jayhuangsci.ratings.core.Options;
import io.github.jayhuangsci.ratings.core.Tokenizer;
import io.github.jayhuangsci.ratings.core.TopK;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public final class HadoopAnalytics {
    private static final String PREFIX = "ratings.";
    public enum DataCounter { INPUT_ROWS, VALID_ROWS, INVALID_ROWS, RATING_EVENTS, PAIR_CONTRIBUTIONS }
    private HadoopAnalytics() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) { System.out.println(Options.usage()); return; }
        Configuration conf = new Configuration();
        run(conf, new GenericOptionsParser(conf, args).getRemainingArgs());
    }

    public static void run(Configuration original, String[] args) throws Exception {
        Options options = Options.parse(args);
        Configuration conf = new Configuration(original);
        conf.set(PREFIX + "task", options.task());
        conf.setBoolean(PREFIX + "skip", options.skipBadRecords());
        conf.setInt(PREFIX + "max.movies", options.maxMoviesPerUser());
        conf.setInt(PREFIX + "top.k", options.topK());
        conf.setInt(PREFIX + "min.support", options.minSupport());
        long start = System.nanoTime();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("engine", "hadoop"); metadata.put("task", options.task());
        metadata.put("input", options.input()); metadata.put("partitions", options.partitions());
        metadata.put("bad_records", options.skipBadRecords() ? "skip" : "fail");
        metadata.put("top_k", options.topK()); metadata.put("min_support", options.minSupport());
        metadata.put("max_movies_per_user", options.maxMoviesPerUser());
        try (OutputTransaction output = new OutputTransaction(conf, options.output())) {
            if (options.task().equals("agreement") || options.task().equals("recommend")) {
                Job pairs = job(conf, "user-side movie join", options.partitions());
                pairs.setMapperClass(UserRatingsMapper.class);
                pairs.setReducerClass(UserPairsReducer.class);
                pairs.setMapOutputKeyClass(Text.class); pairs.setMapOutputValueClass(Text.class);
                pairs.setOutputKeyClass(Text.class); pairs.setOutputValueClass(LongWritable.class);
                pairs.setOutputFormatClass(SequenceFileOutputFormat.class);
                execute(pairs, new Path(options.input()), output.path("pairs"), metadata, "join");

                Job agreement = job(conf, "aggregate exact-rating agreement", options.partitions());
                agreement.setInputFormatClass(SequenceFileInputFormat.class);
                agreement.setMapperClass(Mapper.class);
                agreement.setMapOutputKeyClass(Text.class); agreement.setMapOutputValueClass(LongWritable.class);
                agreement.setCombinerClass(LongSumCombiner.class); agreement.setReducerClass(AgreementReducer.class);
                Path agreementPath = output.path(options.task().equals("recommend") ? "agreement" : "result");
                execute(agreement, output.path("pairs"), agreementPath, metadata, "agreement");
                if (options.task().equals("recommend")) {
                    Job recommendations = job(conf, "bounded item recommendations", options.partitions());
                    recommendations.setMapperClass(RecommendationMapper.class);
                    recommendations.setMapOutputKeyClass(Text.class); recommendations.setMapOutputValueClass(Text.class);
                    recommendations.setReducerClass(RecommendationReducer.class);
                    execute(recommendations, agreementPath, output.path("result"), metadata, "recommend");
                }
            } else {
                Job job = job(conf, options.task(), options.task().equals("total") ? 1 : options.partitions());
                job.setMapOutputKeyClass(Text.class);
                switch (options.task()) {
                    case "movie-stats":
                        job.setMapperClass(StatsMapper.class); job.setMapOutputValueClass(StatsWritable.class);
                        job.setCombinerClass(StatsCombiner.class); job.setReducerClass(StatsReducer.class); break;
                    case "top-raters":
                        job.setMapperClass(TopRatersMapper.class); job.setMapOutputValueClass(Text.class);
                        job.setReducerClass(TopRatersReducer.class); break;
                    default:
                        job.setMapperClass(options.task().equals("word-count") ? WordMapper.class : CountsMapper.class);
                        job.setMapOutputValueClass(LongWritable.class);
                        job.setCombinerClass(LongSumCombiner.class); job.setReducerClass(CountsReducer.class);
                }
                execute(job, new Path(options.input()), output.path("result"), metadata, "analytics");
                // A directory with no input splits must still have an additive identity for total.
                if (options.task().equals("total") && job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue() == 0) {
                    try (org.apache.hadoop.fs.FSDataOutputStream stream = output.path("result/part-r-empty").getFileSystem(conf).create(output.path("result/part-r-empty"), false)) {
                        stream.write("0\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
            metadata.put("elapsed_ms", (System.nanoTime() - start) / 1_000_000);
            output.publish(metadata);
        }
    }

    private static Job job(Configuration conf, String name, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "ratings-lab: " + name);
        job.setJarByClass(HadoopAnalytics.class);
        job.setNumReduceTasks(reducers);
        job.setOutputKeyClass(NullWritable.class); job.setOutputValueClass(Text.class);
        return job;
    }
    private static void execute(Job job, Path input, Path output, Map<String, Object> metrics, String stage) throws Exception {
        FileInputFormat.addInputPath(job, input); FileOutputFormat.setOutputPath(job, output);
        long started = System.nanoTime();
        if (!job.waitForCompletion(true)) { throw new IOException("Hadoop stage failed: " + job.getJobName()); }
        metrics.put(stage + ".elapsed_ms", (System.nanoTime() - started) / 1_000_000);
        metrics.put(stage + ".job_id", job.getJobID().toString());
        for (DataCounter counter : DataCounter.values()) { metrics.put(stage + "." + counter.name().toLowerCase(java.util.Locale.ROOT), job.getCounters().findCounter(counter).getValue()); }
        for (TaskCounter counter : new TaskCounter[]{TaskCounter.MAP_OUTPUT_RECORDS, TaskCounter.COMBINE_INPUT_RECORDS,
                TaskCounter.COMBINE_OUTPUT_RECORDS, TaskCounter.REDUCE_SHUFFLE_BYTES, TaskCounter.REDUCE_OUTPUT_RECORDS}) {
            Counter value = job.getCounters().findCounter(counter);
            metrics.put(stage + "." + counter.name().toLowerCase(java.util.Locale.ROOT), value.getValue());
        }
    }

    public abstract static class RowsMapper<V> extends Mapper<LongWritable, Text, Text, V> {
        @Override protected final void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            context.getCounter(DataCounter.INPUT_ROWS).increment(1);
            MovieRow row;
            try { row = MovieRow.parse(line.toString()); }
            catch (IllegalArgumentException e) {
                context.getCounter(DataCounter.INVALID_ROWS).increment(1);
                if (!context.getConfiguration().getBoolean(PREFIX + "skip", false)) {
                    throw new IOException("Invalid movie record at byte " + offset.get() + ": " + e.getMessage(), e);
                }
                return;
            }
            context.getCounter(DataCounter.VALID_ROWS).increment(1);
            context.getCounter(DataCounter.RATING_EVENTS).increment(row.ratings().size());
            emit(row, context);
        }
        protected abstract void emit(MovieRow row, Context context) throws IOException, InterruptedException;
    }
    public static final class StatsMapper extends RowsMapper<StatsWritable> {
        @Override protected void emit(MovieRow row, Context context) throws IOException, InterruptedException {
            context.write(new Text(row.title()), new StatsWritable(MovieStats.from(row)));
        }
    }
    public static final class StatsCombiner extends Reducer<Text, StatsWritable, Text, StatsWritable> {
        @Override protected void reduce(Text title, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
            MovieStats stats = MovieStats.empty();
            for (StatsWritable value : values) { stats = stats.merge(value.stats()); }
            context.write(title, new StatsWritable(stats));
        }
    }
    public static final class StatsReducer extends Reducer<Text, StatsWritable, NullWritable, Text> {
        @Override protected void reduce(Text title, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
            MovieStats stats = MovieStats.empty();
            for (StatsWritable value : values) { stats = stats.merge(value.stats()); }
            context.write(NullWritable.get(), new Text(stats.csv(title.toString())));
        }
    }
    public static final class TopRatersMapper extends RowsMapper<Text> {
        @Override protected void emit(MovieRow row, Context context) throws IOException, InterruptedException {
            if (row.ratings().isEmpty()) { context.write(new Text(row.title()), new Text("0:")); }
            int maximum = row.ratings().stream().mapToInt(MovieRow.Rating::score).max().orElse(0);
            for (MovieRow.Rating rating : row.ratings()) {
                if (rating.score() == maximum) { context.write(new Text(row.title()), new Text(rating.score() + ":" + rating.user())); }
            }
        }
    }
    public static final class TopRatersReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override protected void reduce(Text title, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            TreeSet<Integer> users = new TreeSet<>();
            for (Text value : values) {
                String[] parts = value.toString().split(":", -1);
                int score = Integer.parseInt(parts[0]);
                if (score > max) { users.clear(); max = score; }
                if (score == max && !parts[1].isEmpty()) { users.add(Integer.parseInt(parts[1])); }
            }
            StringBuilder line = new StringBuilder(Csv.escape(title));
            for (Integer user : users) { line.append(',').append(user); }
            context.write(NullWritable.get(), new Text(line.toString()));
        }
    }
    public static final class CountsMapper extends RowsMapper<LongWritable> {
        @Override protected void emit(MovieRow row, Context context) throws IOException, InterruptedException {
            if (context.getConfiguration().get(PREFIX + "task").equals("total")) {
                context.write(new Text("total"), new LongWritable(row.ratings().size()));
            } else {
                for (MovieRow.Rating rating : row.ratings()) { context.write(new Text(String.valueOf(rating.user())), new LongWritable(1)); }
            }
        }
    }
    public static final class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            context.getCounter(DataCounter.INPUT_ROWS).increment(1); context.getCounter(DataCounter.VALID_ROWS).increment(1);
            for (String word : Tokenizer.tokens(line.toString())) { context.write(new Text(word), new LongWritable(1)); }
        }
    }
    public static final class LongSumCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) { sum = Math.addExact(sum, value.get()); }
            context.write(key, new LongWritable(sum));
        }
    }
    public static final class CountsReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
        @Override protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) { sum = Math.addExact(sum, value.get()); }
            String result = context.getConfiguration().get(PREFIX + "task").equals("total") ? String.valueOf(sum) : Csv.row(key, sum);
            context.write(NullWritable.get(), new Text(result));
        }
    }
    public static final class UserRatingsMapper extends RowsMapper<Text> {
        @Override protected void emit(MovieRow row, Context context) throws IOException, InterruptedException {
            for (MovieRow.Rating rating : row.ratings()) {
                context.write(new Text(String.valueOf(rating.user())), new Text(Csv.row(row.title(), rating.score())));
            }
        }
    }
    public static final class UserPairsReducer extends Reducer<Text, Text, Text, LongWritable> {
        @Override protected void reduce(Text user, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<String, Integer> movies = new TreeMap<>();
            int limit = context.getConfiguration().getInt(PREFIX + "max.movies", 10000);
            for (Text value : values) {
                List<String> parts = Csv.parse(value.toString());
                Integer previous = movies.put(parts.get(0), Integer.parseInt(parts.get(1)));
                if (previous != null) { throw new IOException("Duplicate movie title for user " + user + ": " + parts.get(0)); }
                if (movies.size() > limit) { throw new IOException("User " + user + " exceeds --max-movies-per-user=" + limit); }
            }
            Map<Integer, List<String>> groups = new TreeMap<>();
            movies.forEach((title, score) -> groups.computeIfAbsent(score, ignored -> new ArrayList<>()).add(title));
            for (List<String> titles : groups.values()) {
                for (int i = 0; i < titles.size(); i++) {
                    for (int j = i + 1; j < titles.size(); j++) {
                        context.write(new Text(Csv.row(titles.get(i), titles.get(j))), new LongWritable(1));
                        context.getCounter(DataCounter.PAIR_CONTRIBUTIONS).increment(1);
                    }
                }
            }
        }
    }
    public static final class AgreementReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
        @Override protected void reduce(Text pair, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) { count = Math.addExact(count, value.get()); }
            if (count >= context.getConfiguration().getInt(PREFIX + "min.support", 1)) {
                context.write(NullWritable.get(), new Text(pair.toString() + "," + count));
            }
        }
    }
    public static final class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            List<String> fields = Csv.parse(value.toString());
            context.write(new Text(fields.get(0)), new Text(Csv.row(fields.get(1), fields.get(2))));
            context.write(new Text(fields.get(1)), new Text(Csv.row(fields.get(0), fields.get(2))));
        }
    }
    public static final class RecommendationReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override protected void reduce(Text title, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TopK top = new TopK(context.getConfiguration().getInt(PREFIX + "top.k", 3));
            for (Text value : values) {
                List<String> fields = Csv.parse(value.toString());
                top.add(fields.get(0), Long.parseLong(fields.get(1)));
            }
            int rank = 1;
            for (TopK.Neighbor neighbor : top.sorted()) {
                context.write(NullWritable.get(), new Text(Csv.row(title, neighbor.title(), neighbor.support(), rank++)));
            }
        }
    }
}
