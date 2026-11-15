package io.github.jayhuangsci.ratings.core;

import java.io.Serializable;
import java.util.Locale;

/** Mergeable integer state: no averages of averages or floating point shuffle drift. */
public final class MovieStats implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long count;
    private final long sum;
    private final int min;
    private final int max;

    public MovieStats(long count, long sum, int min, int max) {
        this.count = count; this.sum = sum; this.min = min; this.max = max;
    }
    public static MovieStats empty() { return new MovieStats(0, 0, 0, 0); }
    public static MovieStats from(MovieRow row) {
        MovieStats stats = empty();
        for (MovieRow.Rating r : row.ratings()) { stats = stats.merge(new MovieStats(1, r.score(), r.score(), r.score())); }
        return stats;
    }
    public MovieStats merge(MovieStats other) {
        if (count == 0) { return other; }
        if (other.count == 0) { return this; }
        return new MovieStats(Math.addExact(count, other.count), Math.addExact(sum, other.sum),
                Math.min(min, other.min), Math.max(max, other.max));
    }
    public long count() { return count; }
    public long sum() { return sum; }
    public int min() { return min; }
    public int max() { return max; }
    public String csv(String title) {
        String mean = String.format(Locale.ROOT, "%.6f", count == 0 ? 0.0 : (double) sum / count);
        return Csv.row(title, count, sum, mean, min, max);
    }
}
