package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper.KafkaSourceConfig;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceConfig implements Serializable {
    private String type = "KAFKA";
    private String broker = "localhost:9092";
    private String topic;
    @JsonProperty("group_id")
    private String groupId;
    private StringObjectMap properties = new StringObjectMap();
    private String name;
    @JsonProperty("unique_id")
    private String uniqueId;

    @JsonProperty("offset_reset_strategy")
    private String offsetResetStrategy;

    @JsonProperty("starting_offsets_timestamp")
    private long startingOffsetsTimestamp;

    @JsonProperty("wait_for_out_of_order_events_for_sec")
    private int waitForOutOfOrderEventsForSec;

    @JsonProperty("ideal_wait_timeout")
    private int idealWaitTimeout;

    @JsonIgnore
    public KafkaSourceConfig getKafkaSource() {
        return KafkaSourceConfig.builder()
                .brokers(broker)
                .groupId(groupId)
                .topic(topic)
                .offsetResetStrategy(offsetResetStrategy)
                .startingOffsetsTimestamp(startingOffsetsTimestamp)
                .waitForOutOfOrderEventsForSec(waitForOutOfOrderEventsForSec)
                .idealWaitTimeout(idealWaitTimeout)
                .build();
    }

    @JsonIgnore
    public DataStream<StringObjectMap> getKafkaSourceWithStringObjectMap(StreamExecutionEnvironment env) {
        return KafkaSourceHelper.flink1_14_2_KafkaSource(
                getKafkaSource(),
                env,
                name,
                uniqueId,
                StringObjectMap.class
        );
    }

    public void validate() {
        if (Objects.equal(type, "KAFKA")) {
            if (Strings.isNullOrEmpty(name)) {
                throw new RuntimeException("your configuration file must have 'name' set for each source");
            }
            if (Strings.isNullOrEmpty(uniqueId)) {
                throw new RuntimeException("your configuration file must have 'unique_id' set for each source");
            }
        }
    }
}
