package io.github.jayhuangsci.ratings.core;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Strict, single-record CSV. Embedded newlines are deliberately unsupported. */
public final class Csv {
    private Csv() { }

    public static List<String> parse(String line) {
        if (line.indexOf('\n') >= 0 || line.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("CSV records must fit on one line");
        }
        List<String> fields = new ArrayList<>();
        int i = 0;
        while (true) {
            StringBuilder field = new StringBuilder();
            if (i < line.length() && line.charAt(i) == '"') {
                i++;
                boolean closed = false;
                while (i < line.length()) {
                    char c = line.charAt(i++);
                    if (c == '"') {
                        if (i < line.length() && line.charAt(i) == '"') { field.append('"'); i++; }
                        else { closed = true; break; }
                    } else { field.append(c); }
                }
                if (!closed || (i < line.length() && line.charAt(i) != ',')) {
                    throw new IllegalArgumentException("Malformed quoted CSV field");
                }
            } else {
                while (i < line.length() && line.charAt(i) != ',') {
                    char c = line.charAt(i++);
                    if (c == '"') { throw new IllegalArgumentException("Quote inside unquoted CSV field"); }
                    field.append(c);
                }
            }
            fields.add(field.toString());
            if (i == line.length()) { return fields; }
            i++;
        }
    }

    public static String escape(Object value) {
        String s = String.valueOf(value);
        return s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0
                ? '"' + s.replace("\"", "\"\"") + '"' : s;
    }

    public static String row(Object... values) {
        List<String> fields = new ArrayList<>();
        for (Object value : values) { fields.add(escape(value)); }
        return fields.stream().collect(Collectors.joining(","));
    }
}
