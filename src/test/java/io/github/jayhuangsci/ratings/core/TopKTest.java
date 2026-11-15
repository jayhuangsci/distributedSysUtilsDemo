package io.github.jayhuangsci.ratings.core;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TopKTest {
    @Test void boundedMergesKeepHighestSupportAndStableTies() {
        TopK left = new TopK(2).add("Zulu", 4).add("Alpha", 4).add("Low", 1);
        TopK right = new TopK(2).add("Best", 8).add("Beta", 4);
        List<TopK.Neighbor> result = left.merge(right).sorted();
        assertEquals(2, result.size());
        assertEquals("Best", result.get(0).title());
        assertEquals("Alpha", result.get(1).title());
    }
}
