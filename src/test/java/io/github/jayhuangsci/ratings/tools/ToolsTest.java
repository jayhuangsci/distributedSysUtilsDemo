package io.github.jayhuangsci.ratings.tools;

import io.github.jayhuangsci.ratings.core.MovieRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class ToolsTest {
    @TempDir Path temporary;
    @Test void generatorIsReproducibleAndRefusesOverwrite() throws Exception {
        Path a = temporary.resolve("a.csv"), b = temporary.resolve("b.csv");
        GenerateRatings.generate(a, 20, 12, 0.25, 42);
        GenerateRatings.generate(b, 20, 12, 0.25, 42);
        assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(b));
        assertEquals(20, Files.readAllLines(a).size());
        assertTrue(Files.readAllLines(a).stream().map(MovieRow::parse).mapToLong(r -> r.ratings().size()).sum() > 0);
        assertThrows(IOException.class, () -> GenerateRatings.generate(a, 20, 12, 0.25, 42));
        assertThrows(IllegalArgumentException.class, () -> GenerateRatings.generate(temporary.resolve("invalid.csv"), 1, 1, Double.NaN, 1));
    }
    @Test void comparatorIgnoresMetadataButDetectsDuplicateCountDifferences() throws Exception {
        Path a = temporary.resolve("a"), b = temporary.resolve("b");
        Files.createDirectories(a); Files.createDirectories(b);
        Files.writeString(a.resolve("part-00000"), "b,2\na,1\na,1\n");
        Files.writeString(b.resolve("part-00000"), "a,1\nb,2\n");
        Files.writeString(b.resolve("part-00001"), "a,1\n");
        Files.writeString(a.resolve("_RUN.json"), "ignored");
        CompareOutputs.verify(a, b);
        Files.writeString(b.resolve("part-00001"), "");
        assertThrows(IOException.class, () -> CompareOutputs.verify(a, b));
        assertEquals("b,2\na,1\na,1\n", Files.readString(a.resolve("part-00000")));
    }
}
