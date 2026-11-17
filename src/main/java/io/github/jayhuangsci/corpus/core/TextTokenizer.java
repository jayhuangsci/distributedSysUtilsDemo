package io.github.jayhuangsci.corpus.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Model-independent lexical tokens; Han characters and redaction tags remain atomic. */
public final class TextTokenizer {
    public static final String NAME = "unicode-lexical-v1";
    private static final String WORD = "[\\p{L}\\p{M}\\p{N}&&[^\\p{IsHan}]]+";
    private static final Pattern TOKEN = Pattern.compile("\\[(?:EMAIL|PHONE|IP|CARD|URL)\\]|\\p{IsHan}|" + WORD + "(?:['’]" + WORD + ")*|[^\\s]");
    private TextTokenizer() { }

    public static int count(String text, int stopAfter) {
        Matcher matcher = TOKEN.matcher(text);
        int count = 0;
        while (matcher.find()) { if (++count > stopAfter) { return count; } }
        return count;
    }

    public static void chunks(String text, int size, int overlap, Consumer<Chunk> consumer) {
        if (size < 1 || overlap < 0 || overlap >= size) { throw new IllegalArgumentException("Invalid chunk window"); }
        Matcher matcher = TOKEN.matcher(text);
        Deque<Span> window = new ArrayDeque<>();
        int tokenIndex = 0, chunkIndex = 0, lastEnd = 0;
        while (matcher.find()) {
            window.addLast(new Span(matcher.start(), matcher.end(), tokenIndex++));
            if (window.size() == size) {
                consumer.accept(chunk(text, window, chunkIndex++));
                lastEnd = tokenIndex;
                for (int i = 0; i < size - overlap; i++) { window.removeFirst(); }
            }
        }
        if (!window.isEmpty() && tokenIndex > lastEnd) { consumer.accept(chunk(text, window, chunkIndex)); }
    }

    private static Chunk chunk(String text, Deque<Span> spans, int index) {
        List<String> tokens = new ArrayList<>(spans.size());
        for (Span span : spans) { tokens.add(text.substring(span.start, span.end)); }
        return new Chunk(index, spans.getFirst().index, spans.getLast().index + 1,
                text.substring(spans.getFirst().start, spans.getLast().end), tokens);
    }

    private static final class Span {
        final int start, end, index;
        Span(int start, int end, int index) { this.start = start; this.end = end; this.index = index; }
    }

    public static final class Chunk {
        public final int index, tokenStart, tokenEnd;
        public final String text;
        public final List<String> tokens;
        Chunk(int index, int tokenStart, int tokenEnd, String text, List<String> tokens) {
            this.index = index; this.tokenStart = tokenStart; this.tokenEnd = tokenEnd;
            this.text = text; this.tokens = List.copyOf(tokens);
        }
    }
}
