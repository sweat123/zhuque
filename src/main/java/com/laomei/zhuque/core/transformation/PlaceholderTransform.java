package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.Context;
import com.laomei.zhuque.util.PlaceholderParser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * replace placeholder with context;
 * SqlTransform and PlaceholderTransform can't be exist together;
 * @author luobo
 */
public class PlaceholderTransform implements Transform {
    private static final String COMMA = ",";
    private static final String AS = "AS";

    private final Map<String, String> keysWithAlias;

    public PlaceholderTransform(String detail) {
        Preconditions.checkNotNull(detail);
        List<String> placeholders = Arrays.stream(detail.split(COMMA))
                .map(String::trim).collect(Collectors.toList());
        keysWithAlias = new HashMap<>(placeholders.size());
        Consumer<String> selectKeysAndAliasAction = getKeysAndAlias(keysWithAlias);
        placeholders.forEach(selectKeysAndAliasAction);
    }

    @Override
    public Context transform(final Context context) {
        Preconditions.checkNotNull(context);
        final Context newContext = Context.emptyCtx(context);
        final PlaceholderParser parser = PlaceholderParser.getParser(context.getUnmodifiableCtx());
        keysWithAlias.forEach((key, alias) -> {
            String value = parser.replacePlaceholder(key);
            newContext.put(alias, value);
        });
        return newContext;
    }

    private Consumer<String> getKeysAndAlias(final Map<String, String> map) {
        return str -> {
            if (str.contains(AS) || str.contains(AS.toLowerCase())) {
                String as = str.contains(AS) ? AS : AS.toLowerCase();
                //${abc} as ABC => key: ${abc}, alia: ABC
                String[] arr = Arrays.stream(str.split(as))
                        .map(String::trim)
                        .collect(Collectors.toList())
                        .toArray(new String[0]);
                map.put(arr[0], arr[1]);
            } else {
                //${xxx} => key: ${xxx}, alias: xxx
                String trimStr = str.trim();
                String alias = trimStr.substring(2, trimStr.length() - 1);
                map.put(trimStr, alias);
            }
        };
    }
}
