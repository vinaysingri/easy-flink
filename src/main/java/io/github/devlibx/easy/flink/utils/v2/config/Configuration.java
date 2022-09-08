package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.Data;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration {
    private EnvironmentConfig environment;
    private Map<String, SourceConfig> sources;
    private RuleEngineConfig ruleEngine;

    @JsonProperty("miscellaneous_properties")
    private StringObjectMap miscellaneousProperties = new StringObjectMap();
}
