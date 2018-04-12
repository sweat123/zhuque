package com.laomei.zhuque.core;

import com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.TopicConfig;
import com.laomei.zhuque.core.transformation.FieldTransform;
import com.laomei.zhuque.core.transformation.FilterTransform;
import com.laomei.zhuque.core.transformation.PlaceholderTransform;
import com.laomei.zhuque.core.transformation.RecordTransform;
import com.laomei.zhuque.core.transformation.SqlTransform;
import com.laomei.zhuque.core.transformation.Transform;
import lombok.Data;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.DataTrans.PLACEHOLDER_MODE;
import static com.laomei.zhuque.core.SyncAssignment.SyncAssignmentProcessor.DataTrans.SQL_MODE;

/**
 * @author luobo
 */
@Data
public class KafkaRecordProcessor implements Processor {

    private AtomicBoolean isClosed;

    private Map<String, List<Transform>> topicTransforms;

    public KafkaRecordProcessor(List<TopicConfig> topicConfigs, JdbcTemplate jdbcTemplate) {
        topicTransforms = new HashMap<>();
        isClosed = new AtomicBoolean(false);
        convertToTransformChannel(topicConfigs, jdbcTemplate);
    }

    @Override
    public Context process(KafkaRecord record) {
        final String topic = record.getTopic();
        final List<Transform> transforms = topicTransforms.get(topic);
        if (isClosed.get()) return null;
        Context context = makeContextWithRecord(record);
        for (Transform transform : transforms) {
            if (isClosed.get()) return null;
            context = transform.transform(context);
            if (context == null) {
                return null;
            }
        }
        return context;
    }

    @Override
    public List<Context> process(final List<KafkaRecord> records) {
        if (isClosed.get()) return null;
        final List<Context> results = new ArrayList<>(records.size());
        records.forEach(kafkaRecord -> {
            if (isClosed.get()) return;
            Context result = process(kafkaRecord);
            if (result != null) {
                results.add(result);
            }
        });
        return results;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            topicTransforms = null;
        }
    }

    /**
     * convert TopicConfigs to a topic transform map;
     * the key of the map is topic, and the value of the map
     * is the transform list for the topic;
     * @param topicConfigs list of TopicConfig
     * @param jdbcTemplate JdbcTemplate
     */
    private void convertToTransformChannel(List<TopicConfig> topicConfigs, JdbcTemplate jdbcTemplate) {
        for (TopicConfig topicConfig : topicConfigs) {
            String topic = topicConfig.getTopic();
            List<Transform> transforms = topicTransforms.computeIfAbsent(topic, k -> new ArrayList<>());
            if (topicConfig.getFieldTrans() != null) {
                transforms.add(new FieldTransform(topicConfig.getFieldTrans()));
            }
            if (topicConfig.getRemoveBeforeRecord() != null && !topicConfig.getRemoveBeforeRecord()) {
                //default remove before record values
                transforms.add(new RecordTransform(false));
            } else {
                transforms.add(new RecordTransform(true));
            }
            if (topicConfig.getFilterTrans() != null) {
                transforms.add(new FilterTransform(topicConfig.getFilterTrans()));
            }
            if (topicConfig.getDataTrans() != null && topicConfig.getDataTrans().getMode().equals(SQL_MODE)) {
                transforms.add(new SqlTransform(jdbcTemplate, topicConfig.getDataTrans().getModeDetail()));
            } else if (topicConfig.getDataTrans() != null && topicConfig.getDataTrans().getMode().equals(PLACEHOLDER_MODE)) {
                transforms.add(new PlaceholderTransform(topicConfig.getDataTrans().getModeDetail()));
            }
        }
    }

    private Context makeContextWithRecord(KafkaRecord record) {
        Context context = new Context(record.getTopic(), record.getPartition(), record.getOffset());
        context.put(PROCESS_KAFKA_RECORD_BEFORE_VALUE, record.getBefore());
        context.put(PROCESS_KAFKA_RECORD_AFTER_VALUE, record.getAfter());
        return context;
    }
}
