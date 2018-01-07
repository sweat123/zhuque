package com.laomei.zhuque.util;

/**
 * @author luobo
 **/
public class StrUtil {
    public static final String EMPTY_STR = "";
    public static final String NULL = "null";

    public static boolean isNullOrNullStr(String str) {
        return str == null || str.toLowerCase().equals(NULL);
    }
}
