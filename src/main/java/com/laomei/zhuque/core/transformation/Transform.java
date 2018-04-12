package com.laomei.zhuque.core.transformation;

import com.laomei.zhuque.core.Context;

/**
 * Transform context to another context; SqlTransform or PlaceholderTransform
 * will be the last transform;
 * @author luobo
 */
public interface Transform {

    Context transform(Context context);
}
