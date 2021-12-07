//package com.tripartite.flinkdemo;
//
//import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.typeutils.TupleTypeInfo;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.IntegerDeserializer;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nullable;
//import java.util.Arrays;
//import java.util.Base64;
//import java.util.List;
//import java.util.Properties;
//
//public class Main {
//
//    public static final TupleTypeInfo<Tuple2<String, Integer>> TUPLE_2_TUPLE_TYPE_INFO = new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(Integer.class));
//    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
//        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
//
//        Properties consumerProperties = getConsumerProperties();
//        List<String> topics = Arrays.asList("flink.in");
//        FlinkKafkaConsumer<Tuple2<String, Integer>> consumer = getFlinkKafkaConsumer(topics, consumerProperties);
//        Properties producerProperties = getProducerProperties();
//        SinkFunction<Tuple2<String, Integer>> producer = getFlinkKafkaProducer("flink.out", producerProperties);
//
//        env.addSource(consumer)
//                .name("kafka-topic-source").uid("kafka-topic-source")
//                .keyBy(t -> t.f0)
//                .sum(1).name("sum-by-name").uid("sum-by-name")
//                .addSink(producer).name("kafka-topic-sink").uid("kafka-topic-sink");
//
//        env.execute("Flink kafka consumer demo");
//    }
//
//    private static Properties getConsumerProperties() {
//        Properties consumerProperties = new Properties();
//        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
//        consumerProperties.setProperty("group.id", "flink-consumer-demo");
//        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        return consumerProperties;
//    }
//
//    private static Properties getProducerProperties() {
//        Properties producerProperties = new Properties();
//        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        return producerProperties;
//    }
//
//    private static FlinkKafkaProducer<Tuple2<String, Integer>> getFlinkKafkaProducer(String outputTopic, Properties producerProperties) {
//        return new FlinkKafkaProducer<Tuple2<String, Integer>>(outputTopic, new KafkaSerializationSchema<Tuple2<String, Integer>>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, @Nullable Long timestamp) {
//                return new ProducerRecord<>(outputTopic, Serdes.String().serializer().serialize(outputTopic, element.f0),
//                        Serdes.Integer().serializer().serialize(outputTopic, element.f1));
//            }
//        }, producerProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
//    }
//
//    private static FlinkKafkaConsumer<Tuple2<String, Integer>> getFlinkKafkaConsumer(List<String> topics, Properties consumerProperties) {
//        FlinkKafkaConsumer<Tuple2<String, Integer>> consumer = new FlinkKafkaConsumer<>(topics, new KafkaDeserializationSchema<>() {
//            @Override
//            public boolean isEndOfStream(Tuple2<String, Integer> nextElement) {
//                return false;
//            }
//
//            @Override
//            public Tuple2<String, Integer> deserialize(ConsumerRecord<byte[], byte[]> record) {
//                String key = Serdes.String().deserializer().deserialize("flink.in", record.key());
//                Integer value = Serdes.Integer().deserializer().deserialize("flink.in", record.value());
//                return new Tuple2<>(key, value);
//            }
//
//            @Override
//            public TypeInformation<Tuple2<String, Integer>> getProducedType() {
//
//                return TUPLE_2_TUPLE_TYPE_INFO;
//            }
//        }, consumerProperties);
//        consumer.setCommitOffsetsOnCheckpoints(true);
//        consumer.setStartFromGroupOffsets();
//        return consumer;
//    }
//
//}
