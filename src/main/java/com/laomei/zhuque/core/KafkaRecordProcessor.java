package com.laomei.zhuque.core;

import com.laomei.zhuque.core.transformation.Transform;
import lombok.Data;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
@Data
public class KafkaRecordProcessor implements Processor {

    private List<Transform> transforms;

    public KafkaRecordProcessor(List<Transform> transforms) {
        this.transforms = transforms;
    }

    @Override
    public Map<String, Object> process(KafkaRecord record) {
        Map<String, Object> context = makeContextWithRecord(record);
        for (Transform transform : transforms) {
            context = transform.transform(context);
            if (context == null) {
                return null;
            }
        }
        return context;
    }

    @Override
    public List<Map<String, Object>> process(final List<KafkaRecord> records) {
        List<Map<String, Object>> results = new ArrayList<>(records.size());
        for (val record : records) {
            Map<String, Object> result = process(record);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    public void close() {
    }

    private Map<String, Object> makeContextWithRecord(KafkaRecord record) {
        Map<String, Object> context = new HashMap<>();
        context.put(PROCESS_KAFKA_RECORD_BEFORE_VALUE, record.getPreProcessRecord().getBeforeValue());
        context.put(PROCESS_KAFKA_RECORD_AFTER_VALUE, record.getPreProcessRecord().getAfterValue());
        return context;
    }
}
