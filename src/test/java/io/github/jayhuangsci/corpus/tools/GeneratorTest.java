package io.github.jayhuangsci.corpus.tools;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class GeneratorTest {
    @TempDir Path temporary;
    @Test void seededShardsAreReproducibleAndNeverOverwritten() throws Exception {
        Path a = temporary.resolve("a"), b = temporary.resolve("b");
        GenerateCorpus.generate(a, 20, 4, 5, 42); GenerateCorpus.generate(b, 20, 4, 5, 42);
        for (int i = 0; i < 4; i++) {
            String name = String.format(java.util.Locale.ROOT, "raw-%04d.txt", i);
            assertArrayEquals(Files.readAllBytes(a.resolve(name)), Files.readAllBytes(b.resolve(name)));
        }
        assertThrows(java.io.IOException.class, () -> GenerateCorpus.generate(a, 20, 4, 5, 42));
    }
}
