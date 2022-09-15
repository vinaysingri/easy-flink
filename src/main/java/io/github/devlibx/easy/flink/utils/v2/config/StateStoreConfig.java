package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateStoreConfig implements Serializable {

    public static final String MAIN_CLUSTER = "main";
    public static final String DYNAMO = "dynamo";
    public static final String IN_MEMORY_DYNAMO = "dynamo-in-memory";
    public static final String AEROSPIKE = "aerospike";
    public static final String IN_MEMORY_AEROSPIKE = "aerospike-in-memory";

    private String type = DYNAMO;
    private boolean ddbMustHaveSortKey = false;
    private boolean enableMultiDb = false;

    @JsonProperty("dynamo")
    private DynamoDbConfig ddbConfig;

    @JsonProperty("aerospike")
    private AerospikeConfig aerospikeDbConfig;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class StoreGroup {
        private String name = "main";
        private int priority = 0;
    }
}
