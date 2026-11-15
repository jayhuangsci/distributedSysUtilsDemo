package io.github.jayhuangsci.ratings.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/** Bounded per-source state; equal support is resolved by title, ascending. */
public final class TopK implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int capacity;
    private final PriorityQueue<Neighbor> heap = new PriorityQueue<>(Collections.reverseOrder());
    public TopK(int capacity) {
        if (capacity < 1) { throw new IllegalArgumentException("top-k must be positive"); }
        this.capacity = capacity;
    }
    public TopK add(String title, long support) {
        heap.add(new Neighbor(title, support));
        if (heap.size() > capacity) { heap.poll(); }
        return this;
    }
    public TopK merge(TopK other) {
        for (Neighbor neighbor : other.heap) { add(neighbor.title, neighbor.support); }
        return this;
    }
    public List<Neighbor> sorted() {
        List<Neighbor> result = new ArrayList<>(heap);
        Collections.sort(result);
        return result;
    }
    public static final class Neighbor implements Serializable, Comparable<Neighbor> {
        private static final long serialVersionUID = 1L;
        private final String title;
        private final long support;
        Neighbor(String title, long support) { this.title = title; this.support = support; }
        public String title() { return title; }
        public long support() { return support; }
        @Override public int compareTo(Neighbor other) {
            int bySupport = Long.compare(other.support, support);
            return bySupport == 0 ? title.compareTo(other.title) : bySupport;
        }
    }
}
