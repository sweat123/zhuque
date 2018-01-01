package com.laomei.zhuque.core;

import lombok.Data;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luobo
 */
@Data
public class PreProcessor implements Processor {

    @Override
    public Object process(KafkaRecord record) {

        return null;
    }

    @Override
    public List<Object> process(final List<KafkaRecord> records) {
        List<Object> results = new ArrayList<>(records.size());
        for (val record : records) {
            Object result = process(record);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    @Override
    public void close() {
    }
}
