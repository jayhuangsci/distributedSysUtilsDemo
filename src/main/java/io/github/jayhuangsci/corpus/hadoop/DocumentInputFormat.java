package io.github.jayhuangsci.corpus.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/** Split-aware line records and bounded whole-file documents, including compressed inputs. */
public final class DocumentInputFormat extends FileInputFormat<LongWritable, RawDocumentWritable> {
    @Override protected boolean isSplitable(JobContext context, Path path) {
        return context.getConfiguration().get("corpus.input.mode", "lines").equals("lines")
                && new CompressionCodecFactory(context.getConfiguration()).getCodec(path) == null;
    }

    @Override public RecordReader<LongWritable, RawDocumentWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return context.getConfiguration().get("corpus.input.mode", "lines").equals("files")
                ? new WholeFileReader() : new BoundedLineReader();
    }

    private abstract static class DocumentReader extends RecordReader<LongWritable, RawDocumentWritable> {
        final LongWritable key = new LongWritable();
        final RawDocumentWritable value = new RawDocumentWritable();
        int maximum;
        InputStream stream;
        @Override public LongWritable getCurrentKey() { return key; }
        @Override public RawDocumentWritable getCurrentValue() { return value; }
        @Override public void close() throws IOException { if (stream != null) { stream.close(); } }
        InputStream open(FileSplit split, Configuration conf) throws IOException {
            FSDataInputStream file = split.getPath().getFileSystem(conf).open(split.getPath());
            CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(split.getPath());
            try { return codec == null ? file : codec.createInputStream(file); }
            catch (IOException | RuntimeException e) { file.close(); throw e; }
        }
    }

    public static final class BoundedLineReader extends DocumentReader {
        private LineReader lines;
        private long position, start, end;
        private boolean eof;
        @Override public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) inputSplit;
            Configuration conf = context.getConfiguration();
            maximum = conf.getInt("corpus.max.document.bytes", 1048576);
            boolean compressed = new CompressionCodecFactory(conf).getCodec(split.getPath()) != null;
            start = split.getStart(); end = compressed ? Long.MAX_VALUE : start + split.getLength();
            stream = open(split, conf);
            if (!compressed && start > 0) { ((FSDataInputStream) stream).seek(start - 1); }
            lines = new LineReader(stream, conf);
            position = start;
            if (!compressed && start > 0) {
                position = start - 1 + lines.readLine(new Text(), 0, Integer.MAX_VALUE);
            }
        }

        @Override public boolean nextKeyValue() throws IOException {
            if (eof || position >= end) { return false; }
            Text text = new Text();
            int consumed = lines.readLine(text, maximum + 1, Integer.MAX_VALUE);
            if (consumed == 0) { eof = true; return false; }
            if (consumed < 0) { throw new IOException("Document line exceeds the supported byte-offset range"); }
            key.set(position); position += consumed;
            value.set(text, text.getLength() > maximum);
            return true;
        }

        @Override public float getProgress() {
            if (eof) { return 1; }
            return end == start ? 1 : Math.min(1, (float) (position - start) / (end - start));
        }
    }

    public static final class WholeFileReader extends DocumentReader {
        private boolean emitted;
        @Override public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
            maximum = context.getConfiguration().getInt("corpus.max.document.bytes", 1048576);
            stream = open((FileSplit) inputSplit, context.getConfiguration());
        }

        @Override public boolean nextKeyValue() throws IOException {
            if (emitted) { return false; }
            emitted = true;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(Math.min(maximum + 1, 8192));
            byte[] bytes = new byte[Math.min(maximum + 1, 8192)];
            while (buffer.size() <= maximum) {
                int count = stream.read(bytes, 0, Math.min(bytes.length, maximum + 1 - buffer.size()));
                if (count < 0) { break; }
                if (count > 0) { buffer.write(bytes, 0, count); }
            }
            Text text = new Text(); text.set(buffer.toByteArray());
            value.set(text, buffer.size() > maximum); key.set(0);
            return true;
        }

        @Override public float getProgress() { return emitted ? 1 : 0; }
    }
}
