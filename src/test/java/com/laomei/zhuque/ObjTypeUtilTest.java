package com.laomei.zhuque;

import com.laomei.zhuque.util.ObjTypeUtil;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * test class for ObjTypeUtil.class
 * @author luobo
 */
public class ObjTypeUtilTest {

    @Test
    public void numberEqualsTest() {
        Object v1 = 12;
        Object v2 = "12";
        Object v3 = 12L;
        Object v4 = (short) 12;
        Object v5 = (byte) 12;
        Object v6 = 12f;
        Object v7 = 12d;
        Object v8 = new BigInteger("12");
        Object v9 = new BigDecimal("12");
        String v10 = "12";
        Assert.assertTrue(ObjTypeUtil.equals(v1, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v2, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v3, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v4, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v5, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v6, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v7, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v8, v10));
        Assert.assertTrue(ObjTypeUtil.equals(v9, v10));
    }

    @Test
    public void timestampEqualsTest() {
        String t1 = "2017-01-20 15:35:36";
        Timestamp t2 = Timestamp.valueOf(t1);
        Assert.assertTrue(ObjTypeUtil.equals(t2, t1));
    }

    @Test
    public void localDateTimeEqualsTest() {
        String t1 = "2017-01-20T15:35:36";
        LocalDateTime t2 = LocalDateTime.parse(t1, DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertTrue(ObjTypeUtil.equals(t2, t1));
    }

    @Test
    public void zonedDateTimeEqualsTest() {
        String t1 = "2017-01-20T15:35:36+08:00[Asia/Shanghai]";
        ZonedDateTime t2 = ZonedDateTime.parse(t1, DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertTrue(ObjTypeUtil.equals(t2, t1));
    }

    @Test
    public void booleanEqualsTest() {
        Object v1 = true;
        Object v2 = "a";
        Assert.assertTrue(ObjTypeUtil.equals(v1, "true"));
        Assert.assertFalse(ObjTypeUtil.equals(v2, "true"));
    }

    @Test
    public void numberCompareTest() {
        Object v1 = 12;
        Object v2 = "12";
        Object v3 = 12L;
        Object v4 = (short) 12;
        Object v5 = (byte) 12;
        Object v6 = 12f;
        Object v7 = 12d;
        Object v8 = new BigInteger("12");
        Object v9 = new BigDecimal("12");
        String v10 = "11";
        String v11 = "12";
        String v12 = "13";
        Assert.assertTrue(ObjTypeUtil.compare(v1, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v4, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v5, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v6, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v7, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v8, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v9, v10) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v1, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v4, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v5, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v6, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v7, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v8, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v9, v11) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v1, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v4, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v5, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v6, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v7, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v8, v12) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v9, v12) < 0);
    }

    @Test
    public void timestampCompareTest() {
        String t0 = "2017-01-20 15:36:37";
        String t1 = "2017-01-20 15:35:36";
        String t2 = "2017-01-20 15:39:37";
        String t3 = "2017-01-21 15:35:37";
        Timestamp v0 = Timestamp.valueOf(t0);
        Timestamp v1 = Timestamp.valueOf(t1);
        Timestamp v2 = Timestamp.valueOf(t2);
        Timestamp v3 = Timestamp.valueOf(t3);
        Assert.assertTrue(ObjTypeUtil.compare(v0, t0) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v1, t0) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, t0) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, t0) > 0);
    }

    @Test
    public void localDateTimeCompareTest() {
        String t0 = "2017-01-20T15:36:37";
        String t1 = "2017-01-20T15:35:36";
        String t2 = "2017-01-20T15:39:37";
        String t3 = "2017-01-21T15:35:37";
        LocalDateTime v0 = LocalDateTime.parse(t0, DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime v1 = LocalDateTime.parse(t1, DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime v2 = LocalDateTime.parse(t2, DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime v3 = LocalDateTime.parse(t3, DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertTrue(ObjTypeUtil.compare(v0, t0) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v1, t0) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, t0) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, t0) > 0);
    }

    @Test
    public void zonedDateTimeCompareTest() {
        String t0 = "2017-01-20T15:36:37+08:00[Asia/Shanghai]";
        String t1 = "2017-01-20T15:35:36+08:00[Asia/Shanghai]";
        String t2 = "2017-01-20T15:39:37+08:00[Asia/Shanghai]";
        String t3 = "2017-01-21T15:35:37+08:00[Asia/Shanghai]";
        ZonedDateTime v0 = ZonedDateTime.parse(t0, DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime v1 = ZonedDateTime.parse(t1, DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime v2 = ZonedDateTime.parse(t2, DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime v3 = ZonedDateTime.parse(t3, DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertTrue(ObjTypeUtil.compare(v0, t0) == 0);
        Assert.assertTrue(ObjTypeUtil.compare(v1, t0) < 0);
        Assert.assertTrue(ObjTypeUtil.compare(v2, t0) > 0);
        Assert.assertTrue(ObjTypeUtil.compare(v3, t0) > 0);
    }

    @Test
    public void numberConvertTest() {
        Number v1 = 1484897797000L;
        String s1 = "2017-01-20 15:36:37";
        Timestamp v2 = new Timestamp(v1.longValue());
        Long v3 = 1484897797000L;
        Boolean v4 = true;
        LocalDateTime v5 = v2.toLocalDateTime();
        ZonedDateTime v6 = ZonedDateTime.parse("2017-01-20T15:36:37+08:00[Asia/Shanghai]", DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Timestamp.class), v2);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Number.class), v3);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Boolean.class), v4);
        Assert.assertEquals(ObjTypeUtil.convert(v1, LocalDateTime.class), v5);
        Assert.assertEquals(ObjTypeUtil.convert(v1, ZonedDateTime.class), v6);
    }

    @Test
    public void timestampConvertTest() {
        String str = "2017-01-20 15:36:37";
        Timestamp v1 = Timestamp.valueOf(str);
        Timestamp v2 = Timestamp.valueOf(str);
        Long v3 = 1484897797000L;
        LocalDateTime v4 = LocalDateTime.parse("2017-01-20T15:36:37", DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime v5 = ZonedDateTime.parse("2017-01-20T15:36:37+08:00[Asia/Shanghai]", DateTimeFormatter.ISO_DATE_TIME);
        String v6 = "2017-01-20 15:36:37.0";
        Assert.assertEquals(ObjTypeUtil.convert(v1, Timestamp.class), v2);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Long.class), v3);
        Assert.assertEquals(ObjTypeUtil.convert(v1, LocalDateTime.class), v4);
        Assert.assertEquals(ObjTypeUtil.convert(v1, ZonedDateTime.class), v5);
        Assert.assertEquals(ObjTypeUtil.convert(v1, String.class), v6);
    }

    @Test
    public void localDateTimeConvertTest() {
        String str = "2017-01-20T15:36:37";
        LocalDateTime v1 = LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
        LocalDateTime v2 = LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
        Long v3 = 1484897797000L;
        Timestamp v4 = new Timestamp(v3);
        ZonedDateTime v5 = ZonedDateTime.parse("2017-01-20T15:36:37+08:00[Asia/Shanghai]", DateTimeFormatter.ISO_DATE_TIME);
        String v6 = "2017-01-20T15:36:37";
        Assert.assertEquals(ObjTypeUtil.convert(v1, LocalDateTime.class), v2);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Long.class), v3);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Timestamp.class), v4);
        Assert.assertEquals(ObjTypeUtil.convert(v1, ZonedDateTime.class), v5);
        Assert.assertEquals(ObjTypeUtil.convert(v1, String.class), v6);
    }

    @Test
    public void zonedDateTimeConvertTest() {
        String str = "2017-01-20T15:36:37+08:00[Asia/Shanghai]";
        ZonedDateTime v1 = ZonedDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime v2 = ZonedDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
        Long v3 = 1484897797000L;
        Timestamp v4 = new Timestamp(v3);
        String v5 = "2017-01-20T15:36:37+08:00[Asia/Shanghai]";
        LocalDateTime v6 = LocalDateTime.parse("2017-01-20T15:36:37", DateTimeFormatter.ISO_DATE_TIME);
        Assert.assertEquals(ObjTypeUtil.convert(v1, ZonedDateTime.class), v2);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Long.class), v3);
        Assert.assertEquals(ObjTypeUtil.convert(v1, Timestamp.class), v4);
        Assert.assertEquals(ObjTypeUtil.convert(v1, String.class), v5);
        Assert.assertEquals(ObjTypeUtil.convert(v1, LocalDateTime.class), v6);
    }

    @Test
    public void stringConvertTest() {
        String s1 = "1";
        String s2 = "2017-01-20 15:36:37";
        String s3 = "2017-01-20T15:36:37";
        String s4 = "2017-01-20T15:36:37+08:00[Asia/Shanghai]";
        String s5 = "true";
        String s6 = "abc";
        String s7 = "";
        Assert.assertEquals(ObjTypeUtil.convert(s1, Byte.class), (byte) 1);
        Assert.assertEquals(ObjTypeUtil.convert(s1, Short.class), (short) 1);
        Assert.assertEquals(ObjTypeUtil.convert(s1, Integer.class), 1);
        Assert.assertEquals(ObjTypeUtil.convert(s1, Long.class), 1L);
        Assert.assertEquals(ObjTypeUtil.convert(s1, Float.class), 1f);
        Assert.assertEquals(ObjTypeUtil.convert(s1, Double.class), 1D);
        Assert.assertEquals(ObjTypeUtil.convert(s1, BigInteger.class), new BigInteger("1"));
        Assert.assertEquals(ObjTypeUtil.convert(s1, BigDecimal.class), new BigDecimal("1"));
        Assert.assertEquals(ObjTypeUtil.convert(s2, Timestamp.class), Timestamp.valueOf(s2));
        Assert.assertEquals(ObjTypeUtil.convert(s3, LocalDateTime.class),
                LocalDateTime.parse("2017-01-20T15:36:37", DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(ObjTypeUtil.convert(s4, ZonedDateTime.class),
                ZonedDateTime.parse("2017-01-20T15:36:37+08:00[Asia/Shanghai]", DateTimeFormatter.ISO_DATE_TIME));
        Assert.assertEquals(ObjTypeUtil.convert(s5, Boolean.class), true);
        Assert.assertEquals(ObjTypeUtil.convert(s6, String.class), "abc");
        Assert.assertEquals(ObjTypeUtil.convert(s7, String.class), "");
    }
}
