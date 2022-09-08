package io.github.devlibx.easy.flink.utils.v2.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.streaming.api.CheckpointingMode;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnvironmentConfig {
    private String jobName;
    private String checkpointDir;
    private String stateBackend = "rocksdb";
    private int enableCheckpointing = 30 * 1000;
    private String checkpointingMode = CheckpointingMode.EXACTLY_ONCE.name();
    private int minPauseBetweenCheckpoints = 30 * 1000;
    private int checkpointTimeout = 15 * 60000;
    private int tolerableCheckpointFailureNumber = 5;
    private int maxConcurrentCheckpoints = 1;
    private String enableExternalizedCheckpoints = CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION.name();
    private boolean enableUnalignedCheckpoints = true;
}
