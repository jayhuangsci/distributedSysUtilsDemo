package io.github.jayhuangsci.ratings.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/** Publish complete output with rename-without-overwrite on the same filesystem. */
public final class OutputTransaction implements AutoCloseable {
    private final Configuration configuration;
    private final FileSystem fs;
    private final Path destination;
    private final Path workspace;

    public OutputTransaction(Configuration configuration, String destination) throws IOException {
        this.configuration = configuration;
        Path requested = new Path(destination);
        fs = requested.getFileSystem(configuration);
        this.destination = fs.makeQualified(requested);
        if (fs.exists(this.destination)) { throw new FileAlreadyExistsException("Output already exists: " + this.destination); }
        workspace = new Path(this.destination.getParent(), "." + this.destination.getName() + "-work-" + UUID.randomUUID());
        if (!fs.mkdirs(workspace)) { throw new IOException("Cannot create workspace " + workspace); }
    }
    public Path path(String name) { return new Path(workspace, name); }
    public void publish(Map<String, Object> metadata) throws IOException {
        Path result = path("result");
        if (!fs.exists(new Path(result, "_SUCCESS"))) { throw new IOException("Missing output success marker"); }
        try (Writer writer = new OutputStreamWriter(fs.create(new Path(result, "_RUN.json"), false), StandardCharsets.UTF_8)) {
            writer.write("{\n");
            boolean first = true;
            for (Map.Entry<String, Object> entry : new TreeMap<>(metadata).entrySet()) {
                if (!first) { writer.write(",\n"); }
                first = false;
                writer.write("  " + quote(entry.getKey()) + ": ");
                Object value = entry.getValue();
                writer.write(value instanceof Number || value instanceof Boolean ? String.valueOf(value) : quote(String.valueOf(value)));
            }
            writer.write("\n}\n");
        }
        FileContext.getFileContext(destination.toUri(), configuration).rename(result, destination, Rename.NONE);
    }
    private static String quote(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') { out.append('\\').append(c); }
            else if (c < 32) { out.append(String.format("\\u%04x", (int) c)); }
            else { out.append(c); }
        }
        return out.append('"').toString();
    }
    @Override public void close() throws IOException { fs.delete(workspace, true); }
}
