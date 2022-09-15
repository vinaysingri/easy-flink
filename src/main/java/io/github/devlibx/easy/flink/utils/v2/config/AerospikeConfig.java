package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig.StoreGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AerospikeConfig implements Serializable {
    @JsonProperty("enabled")
    private boolean enabled = true;

    @JsonProperty("cluster_name")
    private String clusterName;

    @JsonProperty("store_group")
    private StoreGroup storeGroup = new StoreGroup();

    @JsonProperty("user")
    public String user;

    @JsonProperty("password")
    public String password;

    @JsonProperty("hosts")
    public List<Host> hosts;

    @JsonProperty("namespace")
    public String namespace;

    @JsonProperty("set")
    public String set;

    @JsonProperty("properties")
    public StringObjectMap properties = new StringObjectMap();

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Host implements Serializable {
        private String host;
        private int port;
    }
}
