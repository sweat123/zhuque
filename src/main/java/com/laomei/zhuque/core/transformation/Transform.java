package com.laomei.zhuque.core.transformation;

import java.util.Map;

/**
 * Transform context to another context; SqlTransform or PlaceholderTransform
 * will be the last transform;
 * @author luobo
 */
public interface Transform {

    Map<String, Object> transform(Map<String, Object> context);
}
