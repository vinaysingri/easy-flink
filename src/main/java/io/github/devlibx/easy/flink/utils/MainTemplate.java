package io.github.devlibx.easy.flink.utils;

import com.google.common.base.Strings;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class MainTemplate {

    public static ParameterTool buildParameterTool(String[] args) throws Exception {
        // Read config from S3
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        ParameterTool parameter = null;
        String url = argsParams.getRequired("config");
        if (url.startsWith("s3")) {
            parameter = ConfigReader.readConfigsFromS3(url, true);
        } else if (url.startsWith("/")) {
            parameter = ConfigReader.readConfigsFromFile(url, true);
        } else {
            throw new Exception("Only s3/file url is supported in config - file must be / or s3://");
        }
        return parameter.mergeWith(argsParams);
    }

    public static void main(String[] args, String jobName, RunJob runJob) throws Exception {

        // Read input parameters
        ParameterTool parameter = buildParameterTool(args);

        // Run the input job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupStore(env, parameter);
        runJob.run(env, parameter);
        env.execute(jobName);
    }

    public static void setupStore(StreamExecutionEnvironment env, ParameterTool parameter) {
        String checkpointDir = null;
        if (!Strings.isNullOrEmpty(parameter.get("checkpoint-dir"))) {
            checkpointDir = parameter.get("checkpoint-dir");
        } else if (!Strings.isNullOrEmpty(parameter.get("state.checkpoints.dir"))) {
            checkpointDir = parameter.get("state.checkpoints.dir");
        }
        if (!Strings.isNullOrEmpty(checkpointDir)) {
            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        }

        String backend = null;
        if (!Strings.isNullOrEmpty(parameter.get("backend"))) {
            backend = parameter.get("backend");
        } else if (!Strings.isNullOrEmpty(parameter.get("state.backend"))) {
            backend = parameter.get("state.backend");
        }

        // check if state backend is explicitly disabled
        boolean enableStateManagement = !Objects.equals(parameter.get("state.backend.disabled"), "false");
        if (enableStateManagement) {
            if (Objects.equals("rocksdb", backend)) {
                try {
                    env.setStateBackend(new EmbeddedRocksDBStateBackend());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to set state backend as rocksdb with path=" + checkpointDir, e);
                }
            }
        }

        // start a checkpoint every 30 sec
        env.enableCheckpointing(parameter.getInt("enableCheckpointing", 30 * 1000));

        // set mode to exactly-once (this is the default)
        String checkpointingMode = parameter.get("checkpointingMode", CheckpointingMode.EXACTLY_ONCE.name());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.valueOf(checkpointingMode));

        // make sure 30 seconds of pause happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameter.getInt("minPauseBetweenCheckpoints", 30 * 1000));

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(parameter.getInt("checkpointTimeout", 15 * 60000));

        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(parameter.getInt("tolerableCheckpointFailureNumber", 5));

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(parameter.getInt("maxConcurrentCheckpoints", 1));

        // enable externalized checkpoints which are retained after job cancellation
        String enableExternalizedCheckpoints = parameter.get("enableExternalizedCheckpoints", RETAIN_ON_CANCELLATION.name());
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.valueOf(enableExternalizedCheckpoints));

        // enables the unaligned checkpoints
        if (parameter.getBoolean("enableUnalignedCheckpoints", true)) {
            env.getCheckpointConfig().enableUnalignedCheckpoints();
        }
    }

    public interface RunJob {
        void run(StreamExecutionEnvironment env, ParameterTool parameter);
    }
}
