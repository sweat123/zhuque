package com.laomei.zhuque.util;

import com.laomei.zhuque.contants.ZhuQueMagicNumContants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.PropertyPlaceholderHelper;

import java.util.Map;

/**
 * @author luobo
 */
public class PlaceholderParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlaceholderParser.class);

    private static PropertyPlaceholderHelper placeholderHelper = new PropertyPlaceholderHelper("${", "}");

    private static String PLACEHOLDER_REGEX = "\\$\\{(\\w+\\.*)+}";

    public static PlaceholderParser getParser(Map<String, Object> context) {
        return new PlaceholderParser(context);
    }

    private PropertyPlaceholderHelper.PlaceholderResolver resolver;

    private PlaceholderParser(Map<String, Object> context) {
        resolver = new ZhuQuePlaceholderResolver(context);
    }

    /**
     * replace placeholder in string with values from context
     * @param str string with placeholder
     * @return the result of replacing placeholder with values in context
     */
    public String replacePlaceholder(String str) {
        return placeholderHelper.replacePlaceholders(str, resolver);
    }

    /**
     * resolve replacement values for placeholders contained in Strings.
     */
    static class ZhuQuePlaceholderResolver implements PropertyPlaceholderHelper.PlaceholderResolver {

        private Map<String, Object> context;

        ZhuQuePlaceholderResolver(Map<String, Object> context) {
            this.context = context;
        }

        @Override
        public String resolvePlaceholder(final String placeholderName) {
            try {
                String[] prefixAndPlaceholder = placeholderName.split("\\.");
                int prefixLength = prefixAndPlaceholder.length - 1;
                Map<String, Object> values = context;
                for (int i = 0; i < prefixLength; i++) {
                    String prefix = prefixAndPlaceholder[i];
                    values = (Map<String, Object>) values.get(prefix);
                }
                Object value = values.get(prefixAndPlaceholder[prefixLength]);
                return String.valueOf(value);
            } catch (Exception e) {
                LOGGER.debug("replace placeholder with values in context failed. " +
                                "placeholder name: {}, context: {}", placeholderName, context);
                return ZhuQueMagicNumContants.NULL;
            }
        }
    }
}
