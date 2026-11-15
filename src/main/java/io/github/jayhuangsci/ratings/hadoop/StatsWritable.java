package io.github.jayhuangsci.ratings.hadoop;

import io.github.jayhuangsci.ratings.core.MovieStats;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Compact shuffle state for associative, exact movie statistics. */
public final class StatsWritable implements Writable {
    private MovieStats stats = MovieStats.empty();
    public StatsWritable() { }
    public StatsWritable(MovieStats stats) { this.stats = stats; }
    public MovieStats stats() { return stats; }
    @Override public void write(DataOutput out) throws IOException {
        out.writeLong(stats.count()); out.writeLong(stats.sum()); out.writeInt(stats.min()); out.writeInt(stats.max());
    }
    @Override public void readFields(DataInput in) throws IOException {
        stats = new MovieStats(in.readLong(), in.readLong(), in.readInt(), in.readInt());
    }
}
