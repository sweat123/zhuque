package com.laomei.zhuque.constants;

import com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

/**
 * @author luobo
 */
@VisibleForTesting
public enum JavaClazzEnum {

    BYTE(Byte.class, "byte"),
    SHORT(Short.class, "short"),
    INTEGER(Integer.class, "integer"),
    LONG(Long.class, "long"),
    FLOAT(Float.class, "float"),
    DOUBLE(Double.class, "double"),
    BIG_DECIMAL(BigDecimal.class, "big_decimal"),
    BIG_INTEGER(BigInteger.class, "big_integer"),
    TIMESTAMP(Timestamp.class, "timestamp"),
    STRING(String.class, "String"),
    LOCAL_DATETIME(LocalDateTime.class, "local_datetime"),
    ZONED_DATETIME(ZonedDateTime.class, "zoned_datetime"),
    JODA_DATETIME(DateTime.class, "joda_datetime"),
    BOOLEAN(Boolean.class, "boolean");

    private Class clazz;

    private String name;

    JavaClazzEnum(Class clazz, String name) {
        this.name = name;
        this.clazz = clazz;
    }

    public Class getClazz() {
        return clazz;
    }

    public String getName() {
        return name;
    }

    public static Class getClazz(String name) {
        return javaClazzEnum(name).clazz;
    }

    public static String getName(Class clazz) {
        return javaClazzEnum(clazz).name;
    }

    public static JavaClazzEnum javaClazzEnum(Class clazz) {
        if (Boolean.class.isAssignableFrom(clazz)) {
            return BOOLEAN;
        }
        if (Byte.class.isAssignableFrom(clazz)) {
            return BYTE;
        }
        if (Short.class.isAssignableFrom(clazz)) {
            return SHORT;
        }
        if (Integer.class.isAssignableFrom(clazz)) {
            return INTEGER;
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return LONG;
        }
        if (Float.class.isAssignableFrom(clazz)) {
            return FLOAT;
        }
        if (Double.class.isAssignableFrom(clazz)) {
            return DOUBLE;
        }
        if (BigDecimal.class.isAssignableFrom(clazz)) {
            return BIG_DECIMAL;
        }
        if (BigInteger.class.isAssignableFrom(clazz)) {
            return BIG_INTEGER;
        }
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return TIMESTAMP;
        }
        if (LocalDateTime.class.isAssignableFrom(clazz)) {
            return LOCAL_DATETIME;
        }
        if (ZonedDateTime.class.isAssignableFrom(clazz)) {
            return ZONED_DATETIME;
        }
        if (DateTime.class.isAssignableFrom(clazz)) {
            return JODA_DATETIME;
        }
        return STRING;
    }

    public static JavaClazzEnum javaClazzEnum(String name) {
        switch (name) {
        case "byte": return BYTE;
        case "short": return SHORT;
        case "integer": return INTEGER;
        case "long": return LONG;
        case "float": return FLOAT;
        case "double": return DOUBLE;
        case "big_decimal": return BIG_DECIMAL;
        case "big_integer": return BIG_INTEGER;
        case "timestamp": return TIMESTAMP;
        case "local_datetime": return LOCAL_DATETIME;
        case "zoned_datetime": return ZONED_DATETIME;
        case "joda_datetime": return JODA_DATETIME;
        case "boolean": return BOOLEAN;
        default: return STRING;
        }
    }
}
