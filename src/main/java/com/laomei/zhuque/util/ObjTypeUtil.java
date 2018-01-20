package com.laomei.zhuque.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * @author luobo
 */
public class ObjTypeUtil {

    public static Object convert(Object obj, Class transToClazz) {
        if (obj instanceof Number) {
            return convertNumber((Number) obj, transToClazz);
        }
        if (obj instanceof Timestamp) {
            return convertTimestamp((Timestamp) obj, transToClazz);
        }
        if (obj instanceof LocalDateTime) {
            return convertLocalDateTime((LocalDateTime) obj, transToClazz);
        }
        if (obj instanceof ZonedDateTime) {
            return convertZonedDateTime((ZonedDateTime) obj, transToClazz);
        }
        if (obj instanceof Boolean) {
            return convertBoolean((Boolean) obj, transToClazz);
        }
        return convertString(String.valueOf(obj), transToClazz);
    }

    public static int compare(Object value1, String value2) {
        if (value1 instanceof Number) {
            return compareNumber((Number) value1, value2);
        }
        if (value1 instanceof String) {
            return compareString((String) value1, value2);
        }
        if (value1 instanceof Timestamp) {
            return compareTimestamp((Timestamp) value1, Timestamp.valueOf(value2));
        }
        if (value1 instanceof LocalDateTime) {
            return compareLocalDateTime((LocalDateTime) value1, LocalDateTime.parse(value2, DateTimeFormatter.ISO_DATE_TIME));
        }
        if (value1 instanceof ZonedDateTime) {
            return compareZonedDateTime((ZonedDateTime) value1, ZonedDateTime.parse(value2, DateTimeFormatter.ISO_DATE_TIME));
        }
        return compareString(String.valueOf(value1), value2);
    }

    public static boolean equals(Object value, String expectValue) {
        if (value == null && expectValue == null) {
            return true;
        } else if (value == null) {
            return false;
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

    /**
     * convert Number to Clazz type
     * @param obj value in Number class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertNumber(Number obj, Class clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return obj;
        }
        if (Boolean.class.isAssignableFrom(clazz)) {
            if (obj instanceof Byte) {
                return obj.byteValue() > 0;
            }
            if (obj instanceof Short) {
                return obj.shortValue() > 0;
            }
            if (obj instanceof Integer) {
                return obj.intValue() > 0;
            }
            if (obj instanceof Long) {
                return obj.longValue() > 0;
            }
            if (obj instanceof Float) {
                return obj.floatValue() > 0;
            }
            if (obj instanceof Double) {
                return obj.doubleValue() > 0;
            }
            if (obj instanceof BigInteger) {
                return ((BigInteger) obj).compareTo(new BigInteger("0")) > 0;
            }
            if (obj instanceof BigDecimal) {
                return ((BigDecimal) obj).compareTo(new BigDecimal("0")) > 0;
            }
        }
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return new Timestamp(obj.longValue());
        }
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(obj.longValue()), TimeZone.getDefault().toZoneId());
        }
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(obj.longValue()), TimeZone.getDefault().toZoneId());
        }
        return String.valueOf(obj);
    }

    /**
     * convert Timestamp to Clazz type
     * @param obj value in Timestamp class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertTimestamp(Timestamp obj, Class clazz) {
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return obj;
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return obj.getTime();
        }
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return obj.toLocalDateTime();
        }
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return obj.toInstant().atZone(ZoneId.systemDefault());
        }
        return String.valueOf(obj);
    }

    /**
     * convert LocalDateTime to Clazz type
     * @param obj value in LocalDateTime class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertLocalDateTime(LocalDateTime obj, Class clazz) {
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return obj;
        }
        if (Number.class.isAssignableFrom(clazz)) {
            return Timestamp.valueOf(obj).getTime();
        }
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return Timestamp.valueOf(obj);
        }
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return obj.atZone(ZoneId.systemDefault());
        }
        return String.valueOf(obj);
    }

    /**
     * convert ZonedDateTime to Clazz type
     * @param obj value in ZonedDateTime class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertZonedDateTime(ZonedDateTime obj, Class clazz) {
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return obj;
        }
        if (Number.class.isAssignableFrom(clazz)) {
            return Timestamp.valueOf(obj.toLocalDateTime()).getTime();
        }
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return Timestamp.valueOf(obj.toLocalDateTime());
        }
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return obj.toLocalDateTime();
        }
        return String.valueOf(obj);
    }

    /**
     * convert Boolean to Clazz type
     * @param obj value in Boolean class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertBoolean(Boolean obj, Class clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return obj ? 1 : 0;
        }
        return String.valueOf(obj);
    }

    /**
     * convert String to Clazz type
     * @param obj value in String class
     * @param clazz target class
     * @return converted value
     */
    private static Object convertString(String obj, Class clazz) {
        if (Byte.class.isAssignableFrom(clazz)) {
            return Byte.parseByte(obj);
        }
        if (Short.class.isAssignableFrom(clazz)) {
            return Short.parseShort(obj);
        }
        if (Integer.class.isAssignableFrom(clazz)) {
            return Integer.parseInt(obj);
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return Long.parseLong(obj);
        }
        if (Float.class.isAssignableFrom(clazz)) {
            return Float.parseFloat(obj);
        }
        if (Double.class.isAssignableFrom(clazz)) {
            return Double.parseDouble(obj);
        }
        if (BigDecimal.class.isAssignableFrom(clazz)) {
            return new BigDecimal(obj);
        }
        if (BigInteger.class.isAssignableFrom(clazz)) {
            return new BigInteger(obj);
        }
        if (Boolean.class.isAssignableFrom(clazz)) {
            return Boolean.valueOf(obj);
        }
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return Timestamp.valueOf(obj);
        }
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return LocalDateTime.parse(obj, DateTimeFormatter.ISO_DATE_TIME);
        }
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return ZonedDateTime.parse(obj, DateTimeFormatter.ISO_DATE_TIME);
        }
        return obj;
    }

    private static boolean verifyZonedDateTime(ZonedDateTime value, String expectValue) {
        return compareZonedDateTime(value, ZonedDateTime.parse(expectValue, DateTimeFormatter.ISO_DATE_TIME)) == 0;
    }

    private static boolean verifyLocalDateTime(LocalDateTime value, String expectValue) {
        return compareLocalDateTime(value, LocalDateTime.parse(expectValue, DateTimeFormatter.ISO_DATE_TIME)) == 0;
    }

    private static boolean verifyTimestamp(Timestamp value, String expectValue) {
        return compareTimestamp(value, Timestamp.valueOf(expectValue)) == 0;
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
