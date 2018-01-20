package com.laomei.zhuque.core;

import lombok.Data;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;

/**
 * @author luobo
 */
@Data
public class SyncAssignment {

    private static final Yaml YAML = new Yaml();
    private SyncAssignmentProcessor processor;

    public static SyncAssignment newSyncTaskMetadata(String metadata) {
        return YAML.loadAs(metadata, SyncAssignment.class);
    }

    @Data
    public static class SyncAssignmentProcessor {
        private String kafkaTopic;
        private String autoOffsetReset;
        private List<TopicConfig> topicConfigs;
        private List<EntitySql> entitySqls;
        private String reducerClazz;
        private String solrCollection;

        @Data
        public static class TopicConfig {
            private String topic;
            private Boolean removeBeforeRecord;
            private FilterTrans filterTrans;
            private DataTrans dataTrans;
            private FieldTrans fieldTrans;
        }

        @Data
        public static class FieldTrans {
            private List<String> fields;
        }

        @Data
        public static class DataTrans {
            private String mode;
            private String modeDetail;
        }

        @Data
        public static class FilterTrans {
            private List<String> exist;
            private List<String> notExist;
            private Map<String, String> match;
            private Map<String, String> notMatch;
            private Map<String, Map<String, String>> range;
            private Map<String, List<String>> in;
            private Map<String, List<String>> notIn;
        }

        @Data
        public static class EntitySql {
            private String sql;
            private String name;
            private Boolean required;
        }
    }
}