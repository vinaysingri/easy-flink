package io.github.devlibx.easy.flink.utils;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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

    @Data
    @Builder
    public static class KafkaSourceConfig {
        private String brokers;
        private String topic;
        private List<String> topics;
        private String groupId;
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
