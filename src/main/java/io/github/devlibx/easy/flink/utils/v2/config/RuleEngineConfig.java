package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleEngineConfig {
    private String ruleUrl;
}
