package com.laomei.zhuque;

import com.laomei.zhuque.core.SyncAssignment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZhuqueApplicationTests {

    @Test
    public void testSyncTaskMetadataYaml() {
        String str = "preProcessor:\n" +
                "  topicConfigs:\n" +
                "    - topic: topicX\n" +
                "      dataTrans:\n" +
                "        mode: sql\n" +
                "        modeDetail: select * from xxx\n" +
                "  autoOffsetReset: latest\n" +
                "  kafkaTopic: TopicXX\n" +
                "processor:\n" +
                "  kafkaTopic: abc\n" +
                "  entitySqls:\n" +
                "    - sql: abc\n" +
                "      name: x\n" +
                "      required: true\n" +
                "  reducerClazz: update\n" +
                "  solrCollection: collection";
        System.out.println(SyncAssignment.newSyncTaskMetadata(str));
    }
}
