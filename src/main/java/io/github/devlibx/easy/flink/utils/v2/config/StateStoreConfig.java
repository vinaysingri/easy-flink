package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateStoreConfig implements Serializable {
    private String type = "dynamo";
    private boolean ddbMustHaveSortKey = false;

    @JsonProperty("dynamo")
    private DynamoDbConfig ddbConfig;
}
