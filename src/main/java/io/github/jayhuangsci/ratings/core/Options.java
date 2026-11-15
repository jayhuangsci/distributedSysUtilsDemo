package io.github.jayhuangsci.ratings.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class Options implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public static final Set<String> TASKS = new HashSet<>(Arrays.asList(
            "top-raters", "total", "user-counts", "agreement", "movie-stats", "recommend", "word-count"));
    private static final Set<String> KEYS = new HashSet<>(Arrays.asList(
            "task", "input", "output", "partitions", "top-k", "min-support", "max-movies-per-user", "bad-records"));
    private final Map<String, String> values;

    private Options(Map<String, String> values) { this.values = values; }
    public static Options parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 == args.length) { throw new IllegalArgumentException(usage()); }
            String key = args[i].substring(2);
            if (!KEYS.contains(key) || values.putIfAbsent(key, args[i + 1]) != null) {
                throw new IllegalArgumentException("Unknown or duplicate option: " + args[i]);
            }
        }
        Options options = new Options(values);
        for (String key : Arrays.asList("task", "input", "output")) {
            if (!values.containsKey(key) || values.get(key).trim().isEmpty()) { throw new IllegalArgumentException("Missing --" + key + "\n" + usage()); }
        }
        if (!TASKS.contains(options.task())) { throw new IllegalArgumentException("Unknown task: " + options.task()); }
        options.partitions(); options.topK(); options.minSupport(); options.maxMoviesPerUser();
        if (!Arrays.asList("fail", "skip").contains(options.get("bad-records", "fail"))) {
            throw new IllegalArgumentException("--bad-records must be fail or skip");
        }
        return options;
    }
    private String get(String key, String fallback) { return values.getOrDefault(key, fallback); }
    private int positive(String key, String fallback) {
        int value;
        try { value = Integer.parseInt(get(key, fallback)); }
        catch (NumberFormatException e) { throw new IllegalArgumentException("--" + key + " must be a positive integer", e); }
        if (value < 1) { throw new IllegalArgumentException("--" + key + " must be positive"); }
        return value;
    }
    public String task() { return get("task", ""); }
    public String input() { return get("input", ""); }
    public String output() { return get("output", ""); }
    public int partitions() { return positive("partitions", "2"); }
    public int topK() { return positive("top-k", "3"); }
    public int minSupport() { return positive("min-support", "1"); }
    public int maxMoviesPerUser() { return positive("max-movies-per-user", "10000"); }
    public boolean skipBadRecords() { return get("bad-records", "fail").equals("skip"); }
    public static String usage() {
        return "--task <top-raters|total|user-counts|agreement|movie-stats|recommend|word-count> "
                + "--input <path> --output <new-path> [--partitions 2] [--top-k 3] "
                + "[--min-support 1] [--max-movies-per-user 10000] [--bad-records fail|skip]";
    }
}
