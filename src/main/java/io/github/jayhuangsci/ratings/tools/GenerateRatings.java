package io.github.jayhuangsci.ratings.tools;

import io.github.jayhuangsci.ratings.core.Csv;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/** Streaming synthetic data generator. Identical parameters produce identical bytes. */
public final class GenerateRatings {
    private GenerateRatings() { }
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("--output <new-file> [--movies 1000] [--users 100] [--density 0.1] [--seed 42]");
            return;
        }
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 == args.length || !java.util.Arrays.asList("--output", "--movies", "--users", "--density", "--seed").contains(args[i])
                    || values.putIfAbsent(args[i], args[i + 1]) != null) { throw new IllegalArgumentException("Unknown, duplicate, or incomplete generator option"); }
        }
        if (!values.containsKey("--output")) { throw new IllegalArgumentException("Missing --output"); }
        generate(Path.of(values.get("--output")), Integer.parseInt(values.getOrDefault("--movies", "1000")),
                Integer.parseInt(values.getOrDefault("--users", "100")), Double.parseDouble(values.getOrDefault("--density", "0.1")),
                Long.parseLong(values.getOrDefault("--seed", "42")));
    }
    public static void generate(Path output, int movies, int users, double density, long seed) throws Exception {
        if (movies < 1 || users < 1 || !Double.isFinite(density) || density < 0 || density > 1) {
            throw new IllegalArgumentException("Positive movie/user counts and a finite density between 0 and 1 are required");
        }
        Path absolute = output.toAbsolutePath();
        Files.createDirectories(absolute.getParent());
        Random random = new Random(seed);
        long events = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(absolute, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            for (int movie = 1; movie <= movies; movie++) {
                StringBuilder row = new StringBuilder(Csv.escape(String.format(Locale.ROOT, "Movie %06d", movie)));
                for (int user = 1; user <= users; user++) {
                    row.append(',');
                    if (random.nextDouble() < density) { row.append(1 + random.nextInt(5)); events++; }
                }
                writer.write(row.toString()); writer.write('\n');
            }
        }
        System.out.println("Generated " + movies + " movies, " + users + " user columns, " + events + " ratings at " + absolute);
    }
}
