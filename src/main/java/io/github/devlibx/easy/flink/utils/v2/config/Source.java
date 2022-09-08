package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Source {
    private String type = "KAFKA";
    private String broker;
    private String topic;
    @JsonProperty("group_id")
    private String groupId;
}
