package com.laomei.zhuque.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author luobo
 */
public class ObjTypeUtil {

    public static int compare(Object value1, String value2) {
        if (value1 instanceof Number) {
            return compareNumber((Number) value1, value2);
        }
        if (value1 instanceof String) {
            return compareString((String) value1, value2);
        }
        if (value1 instanceof Timestamp) {
            return compareTimestamp((Timestamp) value1, Timestamp.valueOf(LocalDateTime.parse(value2, DateTimeFormatter.ISO_DATE_TIME)));
        }
        if (value1 instanceof LocalDateTime) {
            return compareLocalDateTime((LocalDateTime) value1, LocalDateTime.parse(value2, DateTimeFormatter.ISO_DATE_TIME));
        }
        if (value1 instanceof ZonedDateTime) {
            return compareZonedDateTime((ZonedDateTime) value1, ZonedDateTime.parse(value2, DateTimeFormatter.ISO_LOCAL_DATE));
        }
        return compareString(String.valueOf(value1), value2);
    }

    public static boolean equals(Object value, String expectValue) {
        if (value == null && StrUtil.isNullOrNullStr(expectValue)) {
            return true;
        }
        if (value instanceof Boolean && verifyBoolean((boolean) value, expectValue)) {
            return true;
        }
        if (value instanceof Number && verifyNumber((Number) value, expectValue)) {
            return true;
        }
        if (value instanceof String && verifyString((String) value, expectValue)) {
            return true;
        }
        if (value instanceof Timestamp && verifyTimestamp((Timestamp) value, expectValue)) {
            return true;
        }
        if (value instanceof LocalDateTime && verifyLocalDateTime((LocalDateTime) value, expectValue)) {
            return true;
        }
        if (value instanceof ZonedDateTime && verifyZonedDateTime((ZonedDateTime) value, expectValue)) {
            return true;
        }
        return compareString(expectValue, String.valueOf(value)) == 0;
    }

    private static boolean verifyZonedDateTime(ZonedDateTime value, String expectValue) {
        return compareZonedDateTime(value, ZonedDateTime.parse(expectValue, DateTimeFormatter.ISO_LOCAL_DATE)) == 0;
    }

    private static boolean verifyLocalDateTime(LocalDateTime value, String expectValue) {
        return compareLocalDateTime(value, LocalDateTime.parse(expectValue, DateTimeFormatter.ISO_DATE_TIME)) == 0;
    }

    private static boolean verifyTimestamp(Timestamp value, String expectValue) {
        return compareTimestamp(value, Timestamp.valueOf(LocalDateTime.parse(expectValue, DateTimeFormatter.ISO_DATE_TIME))) == 0;
    }

    private static boolean verifyString(String value, String expectValue) {
        return compareString(value, expectValue) == 0;
    }

    /**
     * If the Number type value is equal to expectValue, return true;
     * @param value Number type value
     * @param expectValue expect value
     * @return If the Number type value is equal to expectValue, return true;
     */
    private static boolean verifyNumber(Number value, String expectValue) {
        if (value instanceof Byte) {
            return compareByte(value.byteValue(), Byte.valueOf(expectValue)) == 0;
        }
        if (value instanceof Short) {
            return compareShort(value.shortValue(), Short.valueOf(expectValue)) == 0;
        }
        if (value instanceof Integer) {
            return compareInteger(value.intValue(), Integer.valueOf(expectValue)) == 0;
        }
        if (value instanceof Long) {
            return compareLong(value.longValue(), Long.valueOf(expectValue)) == 0;
        }
        if (value instanceof Float) {
            return compareFloat(value.floatValue(), Float.valueOf(expectValue)) == 0;
        }
        if (value instanceof Double) {
            return compareDouble(value.doubleValue(), Double.valueOf(expectValue)) == 0;
        }
        if (value instanceof BigInteger) {
            return compareBigInteger((BigInteger) value, new BigInteger(expectValue)) == 0;
        }
        if (value instanceof BigDecimal) {
            return compareBigDecimal((BigDecimal) value, new BigDecimal(expectValue)) == 0;
        }
        return String.valueOf(value).equals(expectValue);
    }

    private static int compareNumber(Number value1, String value2) {
        if (value1 instanceof Byte) {
            return compareByte(value1.byteValue(), Byte.valueOf(value2));
        }
        if (value1 instanceof Short) {
            return compareShort(value1.shortValue(), Short.valueOf(value2));
        }
        if (value1 instanceof Integer) {
            return compareInteger(value1.intValue(), Integer.valueOf(value2));
        }
        if (value1 instanceof Long) {
            return compareLong(value1.longValue(), Long.valueOf(value2));
        }
        if (value1 instanceof Float) {
            return compareFloat(value1.floatValue(), Float.valueOf(value2));
        }
        if (value1 instanceof Double) {
            return compareDouble(value1.doubleValue(), Double.valueOf(value2));
        }
        if (value1 instanceof BigInteger) {
            return compareBigInteger((BigInteger) value1, new BigInteger(value2));
        }
        if (value1 instanceof BigDecimal) {
            return compareBigDecimal((BigDecimal) value1, new BigDecimal(value2));
        }
        return String.valueOf(value1).compareTo(value2);
    }

    private static int compareZonedDateTime(ZonedDateTime value1, ZonedDateTime value2) {
        return value1.compareTo(value2);
    }

    private static int compareLocalDateTime(LocalDateTime value1, LocalDateTime value2) {
        return value1.compareTo(value2);
    }

    private static int compareTimestamp(Timestamp value1, Timestamp value2) {
        return value1.compareTo(value2);
    }

    private static int compareString(String value1, String value2) {
        return value1.compareTo(value2);
    }

    private static int compareBigDecimal(BigDecimal value1, BigDecimal value2) {
        return value1.compareTo(value2);
    }

    private static int compareBigInteger(BigInteger value1, BigInteger value2) {
        return value1.compareTo(value2);
    }

    private static int compareDouble(Double value1, Double value2) {
        return Double.compare(value1, value2);
    }

    private static int compareFloat(Float value1, Float value2) {
        return Float.compare(value1, value2);
    }

    private static int compareLong(Long value1, Long value2) {
        return Long.compare(value1, value2);
    }

    private static int compareInteger(Integer value1, Integer value2) {
        return Integer.compare(value1, value2);
    }

    private static int compareShort(Short value1, Short value2) {
        return Short.compare(value1, value2);
    }

    private static int compareByte(Byte value1, Byte value2) {
        return Byte.compare(value1, value2);
    }

    /**
     * If the Boolean type value is equal to expectValue, return true;
     * @param value Boolean type value
     * @param expectValue expect value
     * @return If the Boolean type value is equal to expectValue, return true;
     */
    private static boolean verifyBoolean(boolean value, String expectValue) {
        return Boolean.valueOf(expectValue).equals(value);
    }
}
