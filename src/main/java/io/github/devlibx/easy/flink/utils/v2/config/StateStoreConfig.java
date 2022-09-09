package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateStoreConfig {
    private String type = "dynamo";

    @JsonProperty("dynamo")
    private DynamoDbConfig ddbConfig;
}
