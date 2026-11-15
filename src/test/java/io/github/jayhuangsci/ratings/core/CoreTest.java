package io.github.jayhuangsci.ratings.core;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class CoreTest {
    @Test void csvRoundTripsQuotedTitlesAndTrailingEmptyColumns() {
        assertEquals(Arrays.asList("A, \"film\"", "5", "", ""), Csv.parse("\"A, \"\"film\"\"\",5,,"));
        assertEquals("\"A, \"\"film\"\"\",5,,", Csv.row("A, \"film\"", 5, "", ""));
    }
    @Test void parserKeepsOneBasedUserPositions() {
        MovieRow row = MovieRow.parse("Apollo 13,,,4,5,,5");
        assertEquals(3, row.ratings().size());
        assertEquals(3, row.ratings().get(0).user());
        assertEquals(6, row.ratings().get(2).user());
    }
    @Test void invalidRecordsCannotPartiallyEmitRatings() {
        for (String line : Arrays.asList("", ",5", "movie", "movie,0", "movie,6", "movie,3.5", "movie,NaN", "movie,5,invalid", "\"movie,5", "mo\"vie,5")) {
            assertThrows(IllegalArgumentException.class, () -> MovieRow.parse(line), line);
        }
    }
    @Test void statsMergeIsAssociativeAndIgnoresEmptyChunks() {
        MovieStats a = MovieStats.from(MovieRow.parse("A,5,,3"));
        MovieStats b = MovieStats.from(MovieRow.parse("A,1,4"));
        MovieStats c = MovieStats.from(MovieRow.parse("A,2"));
        assertEquals(a.merge(b).merge(c).csv("A"), a.merge(b.merge(c)).csv("A"));
        assertEquals("A,5,15,3.000000,1,5", a.merge(b).merge(c).csv("A"));
        assertEquals(a.csv("A"), a.merge(MovieStats.empty()).csv("A"));
        assertEquals("A,0,0,0.000000,0,0", MovieStats.empty().csv("A"));
    }
    @Test void commandLineRejectsTyposAndUnsafeNumbers() {
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[]{"--task", "unknown", "--input", "in", "--output", "out"}));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[]{"--task", "total", "--input", "in", "--output", "out", "--partitions", "0"}));
        assertThrows(IllegalArgumentException.class, () -> Options.parse(new String[]{"--task", "total", "--task", "total"}));
    }
}
