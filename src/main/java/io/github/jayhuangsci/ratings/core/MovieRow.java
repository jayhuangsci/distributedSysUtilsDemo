package io.github.jayhuangsci.ratings.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One movie per row; user IDs are one-based rating column positions. */
public final class MovieRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String title;
    private final List<Rating> ratings;

    private MovieRow(String title, List<Rating> ratings) {
        this.title = title;
        this.ratings = Collections.unmodifiableList(ratings);
    }

    public static MovieRow parse(String line) {
        List<String> fields = Csv.parse(line);
        String title = fields.get(0).trim();
        if (title.isEmpty() || fields.size() < 2) {
            throw new IllegalArgumentException("Expected a movie title and at least one user column");
        }
        List<Rating> ratings = new ArrayList<>();
        for (int user = 1; user < fields.size(); user++) {
            String value = fields.get(user).trim();
            if (value.isEmpty()) { continue; }
            if (!value.matches("[1-5]")) {
                throw new IllegalArgumentException("User " + user + ": expected an integer rating from 1 to 5");
            }
            ratings.add(new Rating(user, Integer.parseInt(value)));
        }
        return new MovieRow(title, ratings);
    }

    public String title() { return title; }
    public List<Rating> ratings() { return ratings; }

    public static final class Rating implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int user;
        private final int score;
        Rating(int user, int score) { this.user = user; this.score = score; }
        public int user() { return user; }
        public int score() { return score; }
    }
}
