package io.github.jayhuangsci.ratings.tools;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Order-independent multiset comparison, preserving duplicate result records. */
public final class CompareOutputs {
    private CompareOutputs() { }
    public static List<String> read(Path path) throws IOException {
        if (!Files.isDirectory(path)) { return sorted(Files.readAllLines(path, StandardCharsets.UTF_8)); }
        List<String> records = new ArrayList<>();
        List<Path> parts;
        try (Stream<Path> files = Files.list(path)) {
            parts = files.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().startsWith("part-")).collect(Collectors.toList());
        }
        if (parts.isEmpty()) { throw new IOException("No part-* result files in " + path); }
        for (Path part : parts) { records.addAll(Files.readAllLines(part, StandardCharsets.UTF_8)); }
        return sorted(records);
    }
    private static List<String> sorted(List<String> values) { Collections.sort(values); return values; }
    public static void verify(Path left, Path right) throws IOException {
        List<String> a = read(left), b = read(right);
        if (!a.equals(b)) {
            int first = 0;
            while (first < Math.min(a.size(), b.size()) && a.get(first).equals(b.get(first))) { first++; }
            throw new IOException("Outputs differ at sorted record " + (first + 1) + ": "
                    + (first < a.size() ? a.get(first) : "<end>") + " versus " + (first < b.size() ? b.get(first) : "<end>"));
        }
        System.out.println("MATCH: " + a.size() + " records (partition and row order ignored)");
    }
    public static void main(String[] args) throws IOException {
        if (args.length != 2) { throw new IllegalArgumentException("compare <left-file-or-directory> <right-file-or-directory>"); }
        verify(Path.of(args[0]), Path.of(args[1]));
    }
}
