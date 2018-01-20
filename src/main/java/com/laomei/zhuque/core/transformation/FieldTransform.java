package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.Collector;
import com.laomei.zhuque.core.SyncAssignment;
import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.FieldTrans;

import java.util.Map;
import java.util.Objects;

/**
 * FieldTransform is always the first Transform in transform list;
 * If the value of field in before context is same in after context, then discard this context;
 * @author luobo
 **/
public class FieldTransform implements Transform {

    private FieldTrans fieldTrans;

    public FieldTransform(FieldTrans fieldTrans) {
        Preconditions.checkNotNull(fieldTrans);
        this.fieldTrans = fieldTrans;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        return doFieldTransWithContext(context);
    }

    private Map<String, Object> doFieldTransWithContext(Map<String, Object> context) {
        Map<String, Object> beforeContext = (Map<String, Object>) context.get(Collector.FIELD_BEFORE);
        Map<String, Object> afterContext = (Map<String, Object>) context.get(Collector.FIELD_AFTER);
        for (String field : fieldTrans.getFields()) {
            Object beforeValue = beforeContext.get(field);
            Object afterValue = afterContext.get(field);
            if (Objects.equals(beforeValue, afterValue)) {
                return null;
            }
        }
        return context;
    }
}
