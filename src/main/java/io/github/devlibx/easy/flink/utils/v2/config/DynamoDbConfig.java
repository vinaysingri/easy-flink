package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DynamoDbConfig {
    @JsonProperty("access_key")
    public String accessKey;
    @JsonProperty("secret_key")
    public String secretKey;
    @JsonProperty("table")
    public String table;
    public String region = "ap-south-1";
}
