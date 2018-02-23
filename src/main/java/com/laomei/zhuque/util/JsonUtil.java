package com.laomei.zhuque.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @author luobo on 2018/2/23 19:31
 */
public class JsonUtil {
    /**
     * 将 json 字符串 byte[] 类型的数据转为指定的 class 类型
     * @param object
     * @param key
     * @param clazz
     * @param <T>
     * @return
     */
    public static<T> T convertJsonByteArrToAssignObj(byte[] object, String key, Class<T> clazz) {
        JSONObject jsonObject = JSONObject.parseObject(new String(object));
        return jsonObject.getObject(key, clazz);
    }

    public static<T> byte[] convertObjToJsonByteArr(T o, String key) {
        JSONObject object = new JSONObject();
        object.put(key, o);
        return object.toJSONString().getBytes();
    }
}
