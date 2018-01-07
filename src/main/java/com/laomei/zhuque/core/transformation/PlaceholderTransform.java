package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.util.PlaceholderParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * replace placeholder with context;
 * SqlTransform and PlaceholderTransform can't be exist together;
 * @author luobo
 */
public class PlaceholderTransform implements Transform {
    private static final String COMMA = ",";

    private List<String> placeholders;

    public PlaceholderTransform(String placeholders) {
        Preconditions.checkNotNull(placeholders);
        this.placeholders = Stream.of(placeholders.split(COMMA)).parallel().map(String::trim).collect(toList());
    }

    @Override
    public Map<String, Object> transform(final Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        return replacePlaceholderWithContext(context);
    }

    private Map<String, Object> replacePlaceholderWithContext(Map<String, Object> context) {
        PlaceholderParser placeholderParser = PlaceholderParser.getParser(context);
        Map<String, Object> result = new HashMap<>(placeholders.size());
        for (String placeholder : placeholders) {
            String placeholderName = placeholder.substring(2, placeholder.length() - 1);
            String placeholderValue = placeholderParser.replacePlaceholder(placeholder);
            result.put(placeholderName, placeholderValue);
        }
        return result;
    }
}
