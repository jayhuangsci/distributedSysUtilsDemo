package io.github.jayhuangsci.corpus.core;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TextPreparationTest {
    private static PipelineOptions options(String... extra) {
        List<String> args = new ArrayList<>(List.of("--input", "input", "--output", "output", "--min-chars", "1", "--min-tokens", "1"));
        args.addAll(List.of(extra));
        return PipelineOptions.parse(args.toArray(new String[0]));
    }

    @Test void extractsHtmlAndNormalizesEntitiesUnicodeAndWhitespace() {
        TextProcessor.Result result = TextProcessor.process("<nav>Menu</nav><article><p>Ｆｕｌｌ　ｗｉｄｔｈ &amp; readable text.</p></article><script>tracking()</script>", options("--content-type", "html"));
        assertEquals("Full width & readable text.", result.text);
        assertTrue(result.accepted());
    }

    @Test void redactsFiveCommonPatternClassesAndPreservesAtomicTags() {
        TextProcessor.Result result = TextProcessor.process("Contact alice@example.invalid or 202-555-0142. Host 192.0.2.42, card 4111 1111 1111 1111, link https://example.invalid/?secret=sample. Useful explanatory words remain available.", options());
        for (String label : List.of("EMAIL", "PHONE", "IP", "CARD", "URL")) {
            assertTrue(result.text.contains("[" + label + "]"), result.text);
            assertEquals(1, result.redactions.get(label.toLowerCase(java.util.Locale.ROOT)));
        }
        assertFalse(result.text.contains("alice")); assertFalse(result.text.contains("secret"));
        List<TextTokenizer.Chunk> chunks = new ArrayList<>();
        TextTokenizer.chunks(result.text, 100, 0, chunks::add);
        assertTrue(chunks.get(0).tokens.contains("[EMAIL]"));
    }

    @Test void redactionPrecedesDeduplicationIdentity() {
        String left = TextProcessor.process("A readable document contact alice@example.invalid", options()).text;
        String right = TextProcessor.process("A readable document contact bob@example.invalid", options()).text;
        assertEquals(Hashes.sha256(left), Hashes.sha256(right));
    }

    @Test void redactsIpv4BeforeSentencePunctuationWithoutMatchingInvalidAddresses() {
        for (String punctuation : List.of(".", ",", ":", "!")) {
            TextProcessor.Result result = TextProcessor.process("A synthetic host uses address 192.0.2.42" + punctuation + " Other explanatory words remain readable.", options());
            assertEquals(1, result.redactions.get("ip"));
            assertFalse(result.text.contains("192.0.2.42"));
        }
        assertEquals(0, TextProcessor.process("Invalid address 999.0.2.42 remains unclassified.", options()).redactions.get("ip"));
    }

    @Test void redactionLabelsDoNotTurnSensitiveOnlyNoiseIntoQualityText() {
        assertEquals("LOW_LETTER_RATIO_DOCUMENTS", TextProcessor.process("person@example.invalid ".repeat(10), options()).rejection);
    }

    @Test void qualityRulesHaveExplicitRejectionReasons() {
        assertEquals("EMPTY_DOCUMENTS", TextProcessor.process("<script>noise</script>", options()).rejection);
        PipelineOptions defaults = PipelineOptions.parse(new String[]{"--input", "i", "--output", "o"});
        assertEquals("TOO_SHORT_DOCUMENTS", TextProcessor.process("menu", defaults).rejection);
        assertEquals("LOW_LETTER_RATIO_DOCUMENTS", TextProcessor.process("1234 !!! 5678 $$$ 9012 ### 3456 !!! 7890 $$$ 1234", defaults).rejection);
        assertEquals("TOO_MANY_TOKENS_DOCUMENTS", TextProcessor.process("one two three four five", options("--max-document-tokens", "3")).rejection);
    }

    @Test void lexicalTokensHandleHanCharactersAccentsAndPunctuation() {
        List<TextTokenizer.Chunk> chunks = new ArrayList<>();
        TextTokenizer.chunks("Café 训练 [EMAIL] isn't empty.", 100, 0, chunks::add);
        assertEquals(List.of("Café", "训", "练", "[EMAIL]", "isn't", "empty", "."), chunks.get(0).tokens);
    }

    @Test void chunksCoverAllTokensWithConfiguredOverlap() {
        List<TextTokenizer.Chunk> chunks = new ArrayList<>();
        TextTokenizer.chunks("one two three four five six seven", 4, 1, chunks::add);
        assertEquals(2, chunks.size());
        assertEquals("one two three four", chunks.get(0).text);
        assertEquals("four five six seven", chunks.get(1).text);
        assertEquals(3, chunks.get(1).tokenStart); assertEquals(7, chunks.get(1).tokenEnd);
    }

    @Test void neverEmitsAnOverlapOnlyTrailingChunk() {
        List<TextTokenizer.Chunk> chunks = new ArrayList<>();
        TextTokenizer.chunks("one two three four", 4, 2, chunks::add);
        assertEquals(1, chunks.size());
        assertThrows(IllegalArgumentException.class, () -> TextTokenizer.chunks("text", 4, 4, chunks::add));
    }

    @Test void identityAndDocumentSplitDoNotDependOnReducerCount() {
        PipelineOptions one = options("--reducers", "1"), three = options("--reducers", "3");
        assertEquals(one.fingerprint(), three.fingerprint());
        String id = Hashes.sha256("stable text");
        assertEquals("train", Hashes.split(id, 0)); assertEquals("validation", Hashes.split(id, 100));
    }

    @Test void rejectsMisspelledOptionsAndInvalidResourceLimits() {
        assertThrows(IllegalArgumentException.class, () -> options("--reducer", "2"));
        assertThrows(IllegalArgumentException.class, () -> options("--reducers", "0"));
        assertThrows(IllegalArgumentException.class, () -> options("--chunk-overlap", "256"));
        assertThrows(IllegalArgumentException.class, () -> options("--validation-percent", "101"));
        assertThrows(IllegalArgumentException.class, () -> options("--min-letter-ratio", "NaN"));
        assertThrows(IllegalArgumentException.class, () -> options("--max-document-bytes", "100000000"));
    }

    @Test void longNonEmailTokensDoNotTriggerUnboundedEmailBacktracking() {
        assertTimeout(java.time.Duration.ofSeconds(4), () -> {
            TextProcessor.Result result = TextProcessor.process("x".repeat(100000) + " explanatory words stay readable in this synthetic document", options());
            assertEquals(0, result.redactions.get("email"));
        });
    }
}
