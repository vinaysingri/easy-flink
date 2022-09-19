package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

import java.io.Serializable;

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
}
