package com.laomei.zhuque.core.transformation;

import java.util.Map;

/**
 * @author luobo
 */
public interface Transform {

    Map<String, ?> transform(Map<String, ?> context);
}