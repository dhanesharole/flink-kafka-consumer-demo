package com.tripartite.flinkdemo;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

public class Main2 {

    public static final TupleTypeInfo<Tuple2<String, Integer>> TUPLE_2_TUPLE_TYPE_INFO = new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(Integer.class));
    private static final Logger LOG = LoggerFactory.getLogger(Main2.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);

        Properties consumerProperties = getConsumerProperties();
        FlinkKafkaConsumer<Tuple2<String, Integer>> consumer1 = getFlinkKafkaConsumer(Arrays.asList("flink.in.1"), consumerProperties);
        FlinkKafkaConsumer<Tuple2<String, Integer>> consumer2 = getFlinkKafkaConsumer(Arrays.asList("flink.in.2"), consumerProperties);
        Properties producerProperties = getProducerProperties();
        SinkFunction<Tuple2<String, String>> producer = getFlinkKafkaProducer("flink.out", producerProperties);

        SingleOutputStreamOperator<Tuple2<String, Integer>> source1 = env.addSource(consumer1).name("source-1");
        SingleOutputStreamOperator<Tuple2<String, Integer>> source2 = env.addSource(consumer2).name("source-2");

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStreams = source1
                .connect(source2)
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0.split(":")[1],
                        (KeySelector<Tuple2<String, Integer>, String>) value -> value.f0);

        KeyedStream<Tuple2<String, String>, String> fkJoinStream = connectedStreams
                .process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, String>>() {
                    MapState<String, String> lhs;
                    ValueState<Integer> rhs;

                    @Override
                    public void open(Configuration parameters) {
                        MapStateDescriptor<String, String> lhsMapDescriptor = new MapStateDescriptor<>("lhs",
                                TypeInformation.of(String.class), TypeInformation.of(String.class));
//                        lhsMapDescriptor.setQueryable("fk-lhs-side");
                        this.lhs = getRuntimeContext().getMapState(lhsMapDescriptor);

                        ValueStateDescriptor<Integer> rhsValueDescriptor = new ValueStateDescriptor<>("rhs",
                                TypeInformation.of(Integer.class));
//                        lhsMapDescriptor.setQueryable("fk-rhs-side");
                        this.rhs = getRuntimeContext().getState(rhsValueDescriptor);
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        if (lhs.get(value.f0) == null) {
                            LOG.info("first time seeing key {}, adding it to lhs with empty value", value.f0);
                        }
                        lhs.put(value.f0, value.f1.toString());
                        Optional.ofNullable(rhs.value())
                                .ifPresent(rhsVal -> {
                                    final String result = String.format("lhs: (%s,%s), rhs: (%s,%s)", value.f0, value.f1, value.f0.split(":")[1], rhsVal);
                                    out.collect(Tuple2.of(value.f0, result));
                                });
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        LOG.info("got new key on rhs {}", value);
                        rhs.update(value.f1);
                        final Iterable<Map.Entry<String, String>> lhsEntries = lhs.entries();

                        if (lhsEntries != null) {
                            for (Map.Entry<String, String> e : lhsEntries) {
                                LOG.info("joining with lhs key: {}", e.getKey());
                                final String result = String.format("lhs: (%s,%s), rhs: (%s,%s)", e.getKey(), e.getValue(), value.f0, value.f1);
                                out.collect(Tuple2.of(e.getKey(), result));

                            }
                        }
                    }

                }).keyBy((Tuple2<String, String> t) -> t.f0);

        fkJoinStream.addSink(producer).name("sink");
        QueryableStateStream<String, Tuple2<String, String>> stringTuple2QueryableStateStream = fkJoinStream.asQueryableState("FK-join-state");

        env.execute("Flink kafka consumer demo");
    }

    private static Properties getConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        consumerProperties.setProperty("group.id", "flink-consumer-demo");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return consumerProperties;
    }

    private static Properties getProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return producerProperties;
    }

    private static FlinkKafkaProducer<Tuple2<String, String>> getFlinkKafkaProducer(String outputTopic, Properties producerProperties) {
        return new FlinkKafkaProducer<>(outputTopic, new KafkaSerializationSchema<Tuple2<String, String>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> element, @Nullable Long timestamp) {
                return new ProducerRecord<>(outputTopic,
                        Serdes.String().serializer().serialize(outputTopic, element.f0),
                        Serdes.String().serializer().serialize(outputTopic, element.f1));
            }
        }, producerProperties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    private static FlinkKafkaConsumer<Tuple2<String, Integer>> getFlinkKafkaConsumer(List<String> topics, Properties consumerProperties) {
        FlinkKafkaConsumer<Tuple2<String, Integer>> consumer = new FlinkKafkaConsumer<>(topics, new KafkaDeserializationSchema<>() {
            @Override
            public boolean isEndOfStream(Tuple2<String, Integer> nextElement) {
                return false;
            }

            @Override
            public Tuple2<String, Integer> deserialize(ConsumerRecord<byte[], byte[]> record) {
                String key = Serdes.String().deserializer().deserialize("flink.in", record.key());
                Integer value = Serdes.Integer().deserializer().deserialize("flink.in", record.value());
                return new Tuple2<>(key, value);
            }

            @Override
            public TypeInformation<Tuple2<String, Integer>> getProducedType() {

                return TUPLE_2_TUPLE_TYPE_INFO;
            }
        }, consumerProperties);
        consumer.setCommitOffsetsOnCheckpoints(true);
//        consumer.setStartFromGroupOffsets();
        return consumer;
    }

}
