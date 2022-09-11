package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration implements Serializable {

    @JsonProperty("environment")
    private EnvironmentConfig environment;

    @JsonProperty("sources")
    private Map<String, SourceConfig> sources;

    @JsonProperty("rule_engine")
    private RuleEngineConfig ruleEngine;

    @JsonProperty("ttl")
    private StringObjectMap ttl = new StringObjectMap();

    @JsonProperty("state_store")
    private StateStoreConfig stateStore;

    @JsonProperty("miscellaneous_properties")
    private StringObjectMap miscellaneousProperties = new StringObjectMap();

    /**
     * Do basic validation
     */
    @JsonIgnore
    public void validate() {
        if (sources != null) {
            sources.forEach((s, sourceConfig) -> sourceConfig.validate());
        }
    }

    @JsonIgnore
    public Optional<SourceConfig> getSourceByName(String name) {
        if (sources == null || !sources.containsKey(name)) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(sources.get(name));
        }
    }
}
