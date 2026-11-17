package io.github.jayhuangsci.corpus.core;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import java.text.Normalizer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Extraction, normalization, heuristic redaction and deterministic quality rules. */
public final class TextProcessor {
    private static final Pattern HTML = Pattern.compile("(?is)<(?:!doctype|html|body|article|div|p|h[1-6]|script|style|nav)(?:[\\s>])");
    private static final Pattern URL = Pattern.compile("(?i)\\b(?:https?://|www\\.)[^\\s<>\"']+");
    private static final Pattern EMAIL = Pattern.compile("(?i)(?<![a-z0-9._%+-])[a-z0-9._%+-]{1,64}@[a-z0-9.-]{1,253}\\.[a-z]{2,63}(?![a-z])");
    private static final Pattern PHONE = Pattern.compile("(?<!\\d)(?:\\+?1[ .-]?)?\\(?[2-9]\\d{2}\\)?[ .-]?\\d{3}[ .-]?\\d{4}(?!\\d)");
    private static final String OCTET = "(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)";
    private static final Pattern IP = Pattern.compile("(?<![\\d.])" + OCTET + "(?:\\." + OCTET + "){3}(?!\\d|\\.\\d)");
    private static final Pattern CARD = Pattern.compile("(?<!\\d)(?:\\d[ -]?){12,18}\\d(?!\\d)");
    private TextProcessor() { }

    public static Result process(String raw, PipelineOptions options) {
        String text = raw;
        boolean html = options.contentType().equals("html")
                || (options.contentType().equals("auto") && HTML.matcher(text).find());
        if (html) {
            Document document = Jsoup.parse(text);
            document.select("script,style,nav,header,footer,noscript,svg,form").remove();
            text = document.body().text();
        } else {
            text = Parser.unescapeEntities(text, false);
        }
        text = Normalizer.normalize(text, Normalizer.Form.NFKC)
                .replaceAll("[\\p{Cc}&&[^\\r\\n\\t]]", "")
                .replaceAll("[\\u200B\\uFEFF\\u00AD]", "")
                .replaceAll("[\\s\\p{Z}]+", " ").trim();
        Map<String, Integer> redactions = new LinkedHashMap<>();
        text = redact(text, URL, "URL", redactions, false);
        text = redact(text, EMAIL, "EMAIL", redactions, false);
        text = redact(text, CARD, "CARD", redactions, true);
        text = redact(text, PHONE, "PHONE", redactions, false);
        text = redact(text, IP, "IP", redactions, false);
        if (text.isEmpty()) { return new Result(text, redactions, "EMPTY_DOCUMENTS", 0); }
        if (text.length() < options.integer("min-chars")) { return new Result(text, redactions, "TOO_SHORT_DOCUMENTS", 0); }
        String withoutTags = text.replaceAll("\\[(?:EMAIL|PHONE|IP|CARD|URL)\\]", "");
        long letters = withoutTags.codePoints().filter(Character::isLetter).count();
        long visible = text.codePoints().filter(c -> !Character.isWhitespace(c)).count();
        if ((double) letters / visible < options.letterRatio()) {
            return new Result(text, redactions, "LOW_LETTER_RATIO_DOCUMENTS", 0);
        }
        int count = TextTokenizer.count(text, options.integer("max-document-tokens"));
        if (count > options.integer("max-document-tokens")) { return new Result(text, redactions, "TOO_MANY_TOKENS_DOCUMENTS", count); }
        if (count < options.integer("min-tokens")) { return new Result(text, redactions, "TOO_SHORT_DOCUMENTS", count); }
        return new Result(text, redactions, null, count);
    }

    private static String redact(String text, Pattern pattern, String label, Map<String, Integer> counts, boolean requireLuhn) {
        Matcher matcher = pattern.matcher(text);
        StringBuffer output = new StringBuffer();
        int count = 0;
        while (matcher.find()) {
            if (requireLuhn && !luhn(matcher.group())) { continue; }
            matcher.appendReplacement(output, Matcher.quoteReplacement("[" + label + "]"));
            count++;
        }
        matcher.appendTail(output);
        counts.put(label.toLowerCase(java.util.Locale.ROOT), count);
        return output.toString();
    }

    private static boolean luhn(String candidate) {
        String digits = candidate.replaceAll("[^0-9]", "");
        if (digits.length() < 13 || digits.length() > 19) { return false; }
        int sum = 0;
        boolean doubleDigit = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int digit = digits.charAt(i) - '0';
            if (doubleDigit) { digit *= 2; if (digit > 9) { digit -= 9; } }
            sum += digit; doubleDigit = !doubleDigit;
        }
        return sum % 10 == 0;
    }

    public static final class Result {
        public final String text, rejection;
        public final Map<String, Integer> redactions;
        public final int tokenCount;
        Result(String text, Map<String, Integer> redactions, String rejection, int tokenCount) {
            this.text = text; this.redactions = Map.copyOf(redactions);
            this.rejection = rejection; this.tokenCount = tokenCount;
        }
        public boolean accepted() { return rejection == null; }
    }
}
