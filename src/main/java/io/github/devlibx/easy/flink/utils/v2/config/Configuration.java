package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration implements Serializable {

    @JsonProperty("environment")
    private EnvironmentConfig environment;

    @JsonProperty("sources")
    private Map<String, SourceConfig> sources;

    @JsonProperty("rule_engine")
    private RuleEngineConfig ruleEngine;

    @JsonProperty("miscellaneous_properties")
    private StringObjectMap miscellaneousProperties = new StringObjectMap();

    /**
     * Do basic validation
     */
    public void validate() {
        if (sources != null) {
            sources.forEach((s, sourceConfig) -> sourceConfig.validate());
        }
    }
}
