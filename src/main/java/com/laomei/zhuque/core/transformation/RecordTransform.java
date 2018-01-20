package com.laomei.zhuque.core.transformation;

import com.google.common.base.Preconditions;
import com.laomei.zhuque.core.Processor;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * If removeBeforeRecord is true RecordTransform will
 * remove before record value from context
 * and return a new context with after context;
 * @author luobo
 **/
public class RecordTransform implements Transform {

    private boolean removeBeforeRecord;

    public RecordTransform(boolean removeBeforeRecord) {
        this.removeBeforeRecord = removeBeforeRecord;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> context) {
        Preconditions.checkNotNull(context);
        if (!removeBeforeRecord) {
            return context;
        }
        return removeBeforeRecordFromContext(context);
    }

    /**
     * retain after record value in context and remove before record value;
     * @param context context with after record values and before record values;
     * @return new context with after record value
     */
    private Map<String, Object> removeBeforeRecordFromContext(Map<String, Object> context) {
        Map<String, Object> newContext = new HashMap<>();
        GenericRecord afterRecord = (GenericRecord) context.get(Processor.PROCESS_KAFKA_RECORD_AFTER_VALUE);
        afterRecord.getSchema().getFields().forEach(field ->  {
            String fieldName = field.name();
            Object fieldValue = afterRecord.get(fieldName);
            newContext.put(fieldName, fieldValue);
        });
        context.clear();
        return newContext;
    }
}
