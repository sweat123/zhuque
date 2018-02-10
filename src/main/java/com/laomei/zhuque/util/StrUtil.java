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

    /**
     * oldStr => "abc/def/gh"
     * ch => '/'
     * result => "abc/def"
     *
     * @param oldStr
     * @param ch
     * @return
     */
    public static String subStrBeforeLastAssignChar(String oldStr, char ch) {
        int idx = oldStr.lastIndexOf(ch);
        return oldStr.substring(0, idx);
    }

    public static String subStrAfterLastAssignChar(String oldStr, char ch) {
        int idx = oldStr.lastIndexOf(ch);
        return oldStr.substring(idx + 1);
    }

    /**
     * oldStr => "abc/def/gh"
     * ch => '/'
     * result => "abc/def/"
     *
     * @param oldStr
     * @param ch
     * @return
     */
    public static String subStrWithLastAssignChar(String oldStr, char ch) {
        return oldStr.substring(0, oldStr.lastIndexOf(ch) + 1);
    }
}
