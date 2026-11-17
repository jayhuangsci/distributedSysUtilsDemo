package io.github.jayhuangsci.corpus.core;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class PipelineOptions {
    private static final Set<String> KEYS = Set.of("input", "output", "input-mode", "content-type", "reducers",
            "min-chars", "min-tokens", "min-letter-ratio", "max-document-bytes", "max-document-tokens",
            "chunk-tokens", "chunk-overlap", "validation-percent", "compression");
    private final Map<String, String> values;

    private PipelineOptions(Map<String, String> values) { this.values = values; }

    public static PipelineOptions parse(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException(usage());
            }
            String key = args[i].substring(2);
            if (!KEYS.contains(key) || values.putIfAbsent(key, args[i + 1]) != null) {
                throw new IllegalArgumentException("Unknown or duplicate option: " + args[i]);
            }
        }
        PipelineOptions options = new PipelineOptions(values);
        for (String key : Arrays.asList("input", "output")) {
            if (options.get(key, "").trim().isEmpty()) { throw new IllegalArgumentException("Missing --" + key); }
        }
        options.choice("input-mode", "lines", "lines", "files");
        options.choice("content-type", "auto", "auto", "text", "html");
        options.choice("compression", "none", "none", "gzip");
        for (String key : Arrays.asList("reducers", "min-chars", "min-tokens", "max-document-bytes", "max-document-tokens", "chunk-tokens")) {
            options.positive(key);
        }
        int maxBytes = options.integer("max-document-bytes");
        if (maxBytes > 64 * 1024 * 1024) { throw new IllegalArgumentException("--max-document-bytes cannot exceed 64 MiB"); }
        int overlap = options.integer("chunk-overlap");
        if (overlap < 0 || overlap >= options.integer("chunk-tokens")) {
            throw new IllegalArgumentException("--chunk-overlap must be nonnegative and smaller than --chunk-tokens");
        }
        int percent = options.integer("validation-percent");
        if (percent < 0 || percent > 100) { throw new IllegalArgumentException("--validation-percent must be between 0 and 100"); }
        double ratio = options.letterRatio();
        if (!Double.isFinite(ratio) || ratio < 0 || ratio > 1) {
            throw new IllegalArgumentException("--min-letter-ratio must be a finite number between 0 and 1");
        }
        return options;
    }

    private String get(String key, String fallback) { return values.getOrDefault(key, fallback); }
    public String input() { return values.get("input"); }
    public String output() { return values.get("output"); }
    public String inputMode() { return get("input-mode", "lines"); }
    public String contentType() { return get("content-type", "auto"); }
    public String compression() { return get("compression", "none"); }
    public double letterRatio() { return Double.parseDouble(get("min-letter-ratio", "0.5")); }

    public int integer(String key) {
        Map<String, String> defaults = Map.of("reducers", "2", "min-chars", "40", "min-tokens", "8",
                "max-document-bytes", "1048576", "max-document-tokens", "100000", "chunk-tokens", "256",
                "chunk-overlap", "32", "validation-percent", "10");
        try { return Integer.parseInt(get(key, defaults.get(key))); }
        catch (NumberFormatException e) { throw new IllegalArgumentException("--" + key + " must be an integer", e); }
    }

    private void positive(String key) {
        if (integer(key) < 1) { throw new IllegalArgumentException("--" + key + " must be positive"); }
    }

    private void choice(String key, String fallback, String... choices) {
        if (!Arrays.asList(choices).contains(get(key, fallback))) {
            throw new IllegalArgumentException("Invalid --" + key + "; choose " + Arrays.toString(choices));
        }
    }

    public Map<String, Object> metadata() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("input_mode", inputMode()); result.put("content_type", contentType());
        result.put("compression", compression()); result.put("min_letter_ratio", letterRatio());
        for (String key : Arrays.asList("reducers", "min-chars", "min-tokens", "max-document-bytes", "max-document-tokens", "chunk-tokens", "chunk-overlap", "validation-percent")) {
            result.put(key.replace('-', '_'), integer(key));
        }
        return result;
    }

    public String fingerprint() {
        return Hashes.sha256("schema-1|" + TextTokenizer.NAME + "|" + integer("chunk-tokens") + "|" + integer("chunk-overlap"));
    }

    public static String usage() {
        return "--input <file-or-directory> --output <new-directory> [--input-mode lines|files] "
                + "[--content-type auto|text|html] [--reducers 2] [--min-chars 40] [--min-tokens 8] "
                + "[--min-letter-ratio 0.5] [--max-document-bytes 1048576] [--max-document-tokens 100000] "
                + "[--chunk-tokens 256] [--chunk-overlap 32] [--validation-percent 10] [--compression none|gzip]";
    }
}
