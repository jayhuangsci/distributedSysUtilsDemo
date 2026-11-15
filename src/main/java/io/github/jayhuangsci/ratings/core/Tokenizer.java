package io.github.jayhuangsci.ratings.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Tokenizer {
    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{N}]+(?:'[\\p{L}\\p{N}]+)*");
    private Tokenizer() { }
    public static List<String> tokens(String line) {
        List<String> words = new ArrayList<>();
        Matcher matcher = WORD.matcher(line.toLowerCase(Locale.ROOT));
        while (matcher.find()) { words.add(matcher.group()); }
        return words;
    }
}
