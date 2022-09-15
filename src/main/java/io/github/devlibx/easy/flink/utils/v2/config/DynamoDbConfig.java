package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig.StoreGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

import static io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig.MAIN_CLUSTER;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DynamoDbConfig implements Serializable {

    @JsonProperty("enabled")
    private boolean enabled = true;

    @JsonProperty("store_group")
    private StoreGroup storeGroup = new StoreGroup();

    @JsonProperty("access_key")
    public String accessKey;

    @JsonProperty("secret_key")
    public String secretKey;

    @JsonProperty("table")
    public String table;

    public String region = "ap-south-1";
}
