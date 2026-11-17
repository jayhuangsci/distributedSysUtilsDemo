package io.github.jayhuangsci.corpus.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

/** Complete jobs and manifest before publishing, without overwriting existing datasets. */
public final class OutputTransaction implements AutoCloseable {
    private final Configuration configuration;
    private final FileSystem filesystem;
    private final Path destination, workspace;

    public OutputTransaction(Configuration configuration, String output) throws IOException {
        this.configuration = configuration;
        Path requested = new Path(output);
        filesystem = requested.getFileSystem(configuration);
        destination = filesystem.makeQualified(requested);
        String scheme = destination.toUri().getScheme();
        if (!scheme.equals("file") && !scheme.equals("hdfs")) {
            throw new IOException("Publication requires a local filesystem or HDFS, not " + scheme);
        }
        if (filesystem.exists(destination)) { throw new FileAlreadyExistsException("Output already exists: " + destination); }
        workspace = new Path(destination.getParent(), "." + destination.getName() + "-work-" + UUID.randomUUID());
        if (!filesystem.mkdirs(workspace)) { throw new IOException("Cannot create owned staging directory"); }
    }

    public Path path(String name) { return new Path(workspace, name); }

    public void publish(Map<String, Object> manifest) throws IOException {
        Path result = path("result");
        if (!filesystem.exists(new Path(result, "_SUCCESS"))) { throw new IOException("Export job has no success marker"); }
        filesystem.mkdirs(new Path(result, "train")); filesystem.mkdirs(new Path(result, "validation"));
        try (OutputStream stream = filesystem.create(new Path(result, "_MANIFEST.json"), false)) {
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(stream, manifest);
        }
        FileContext.getFileContext(destination.toUri(), configuration).rename(result, destination, Rename.NONE);
    }

    @Override public void close() throws IOException { filesystem.delete(workspace, true); }
}
