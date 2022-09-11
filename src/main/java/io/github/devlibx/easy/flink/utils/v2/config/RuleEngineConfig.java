package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RuleEngineConfig implements Serializable {

    @JsonProperty("rules")
    private Map<String, RuleConfig> rules;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class RuleConfig implements Serializable {
        private String url;
    }

    @JsonProperty
    public Optional<RuleConfig> getRuleByName(String name) {
        if (rules == null || !rules.containsKey(name)) {
            return Optional.empty();
        } else {
            return Optional.ofNullable(rules.get(name));
        }
    }
}
