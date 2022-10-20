package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import lombok.Data;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.Serializable;

@SuppressWarnings("deprecation")
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaSinkConfig implements Serializable {

    @JsonProperty("enabled")
    private boolean enabled = true;

    @JsonProperty("type")
    private String type = "KAFKA";

    @JsonProperty("broker")
    private String broker = "localhost:9092";

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("name")
    private String name;

    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("properties")
    private StringObjectMap properties = new StringObjectMap();

    @JsonIgnore
    public FlinkKafkaProducer<StringObjectMap> getKafkaSinkWithStringObjectMap(StreamExecutionEnvironment env, KafkaSourceHelper.ObjectToKeyConvertor<StringObjectMap> objectMapObjectToKeyConvertor) {
        return KafkaSourceHelper.flink1_12_2_KafkaSink(
                KafkaSourceHelper.KafkaSinkConfig.builder()
                        .brokers(broker)
                        .topic(topic)
                        .build(),
                objectMapObjectToKeyConvertor,
                StringObjectMap.class
        );
    }
}
