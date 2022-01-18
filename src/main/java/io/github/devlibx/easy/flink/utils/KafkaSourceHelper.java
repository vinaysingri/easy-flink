package io.github.devlibx.easy.flink.utils;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaSourceHelper {

    public static <T> DataStream<T> flink1_12_2_KafkaSource(KafkaSourceConfig config, StreamExecutionEnvironment env, String name, String id, Class<T> cls) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.brokers);
        properties.setProperty("group.id", config.groupId);
        if (config.topics == null || config.topics.isEmpty()) {
            FlinkKafkaConsumer<T> kafkaSource = new FlinkKafkaConsumer<T>(
                    config.topic,
                    new JsonMessageToEventDeserializationSchema<>(cls),
                    properties
            );
            return env
                    .addSource(kafkaSource)
                    .name(name)
                    .uid(id);
        } else {
            return env
                    .addSource(
                            new FlinkKafkaConsumer<>(
                                    config.topics,
                                    new JsonMessageToEventDeserializationSchema<>(cls),
                                    properties
                            )
                    )
                    .name(name)
                    .uid(id);
        }
    }

    public static <T> DataStream<T> flink1_14_2_KafkaSource(KafkaSourceConfig config, StreamExecutionEnvironment env, String name, String id, Class<T> cls) {
        OffsetsInitializer offsetsInitializer = ConfigReader.getOffsetsInitializer(
                Strings.isNullOrEmpty(config.offsetResetStrategy) ? "committedOffsetsLatest" : config.offsetResetStrategy,
                config.startingOffsetsTimestamp
        );
        int outOfOrderDelay = config.waitForOutOfOrderEventsForSec <= 0 ? 2 : config.waitForOutOfOrderEventsForSec;
        int idleDelay = config.idealWaitTimeout <= 0 ? 10 : config.idealWaitTimeout;
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(config.brokers)
                .setTopics(config.topics == null || config.topics.isEmpty() ? Collections.singletonList(config.topic) : config.topics)
                .setGroupId(config.groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new JsonMessageToEventDeserializationSchema<>(cls))
                .build();
        return env.fromSource(
                source,
                WatermarkStrategy
                        .<T>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderDelay))
                        .withIdleness(Duration.ofSeconds(idleDelay)),
                name
        ).uid(id);
    }

    public static <T> FlinkKafkaProducer<T> flink1_12_2_KafkaSink(KafkaSinkConfig config, ObjectToKeyConvertor<T> keyConvertor, Class<T> cls) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.brokers);
        return new FlinkKafkaProducer<>(
                config.topic,
                new EventToJsonMessageSerializationSchema<T>(config.topic, cls, keyConvertor),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }

    public static <T> KafkaSink<T> flink1_14_2_KafkaSink(KafkaSinkConfig config, ObjectToKeyConvertor<T> keyConvertor, Class<T> cls) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.topic)
                        .setValueSerializationSchema((SerializationSchema<T>) t -> JsonUtils.asJson(t).getBytes())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    @Data
    @Builder
    public static class KafkaSourceConfig {
        private String brokers;
        private String topic;
        private List<String> topics;
        private String groupId;
        private String offsetResetStrategy;
        private long startingOffsetsTimestamp;
        private int waitForOutOfOrderEventsForSec;
        private int idealWaitTimeout;
    }

    @Data
    @Builder
    public static class KafkaSinkConfig {
        private String brokers;
        private String topic;
        private List<String> topics;
    }

    public interface ObjectToKeyConvertor<T> {
        byte[] key(T obj);
    }
}
