package com.tripartite.flinkdemo;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class FailOverDemo {

    public static void main(String[] args) throws TimeoutException, InterruptedException {
        HashMap<String, Object> properties = new HashMap<>() {{
            put("bootstrap.servers", "127.0.0.1:9093");
            put("consumer.client.id", "failover-client-demo");
        }};
        Map<TopicPartition, OffsetAndMetadata> offsets = RemoteClusterUtils
                .translateOffsets(properties, "cluster1", "topic-1-test-consumer", Duration.ofMinutes(5L));
        System.out.println("***************** Failover metadata *********");
        Map<String, Long> topicNewOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            TopicPartition topicPartition = e.getKey();
            OffsetAndMetadata offsetAndMetadata = e.getValue();
            System.out.println("Topic: " + topicPartition.topic() +  " Partition: " + topicPartition.partition() + " Offset: " + offsetAndMetadata.offset());
            Long minOffset = Math.min(topicNewOffset.getOrDefault(topicPartition.topic(), Long.MAX_VALUE), offsetAndMetadata.offset());
            topicNewOffset.put(topicPartition.topic(), minOffset);
        }
        System.out.println("Start consuming from failover cluster at below mentioned offsets: ");
        System.out.println(topicNewOffset);
    }
}
