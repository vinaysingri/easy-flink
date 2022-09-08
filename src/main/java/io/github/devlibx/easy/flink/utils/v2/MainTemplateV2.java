package io.github.devlibx.easy.flink.utils.v2;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.string.StringHelper;
import io.github.devlibx.easy.flink.utils.ConfigReader;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.EnvironmentConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;
import java.util.Objects;

public class MainTemplateV2<T extends Configuration> {

    public void main(String[] args, String jobName, RunJob<T> runJob, Class<T> cls) throws Exception {
        T config = buildConfig(args, cls);

        // Run the input job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupStore(env, config, cls);
        runJob.run(env, config, cls);

        // Read config from S3 or file if needed
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        if (!argsParams.getBoolean("dryRun", false)) {
            env.execute(jobName);
        }
    }

    private T buildConfig(String[] args, Class<T> cls) throws Exception {

        // Read config from S3 or file if needed
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        String url = argsParams.getRequired("config");

        String configAsString = "";
        if (url.startsWith("s3")) {
            configAsString = ConfigReader.readConfigsFromS3AsString(url, true);
        } else if (url.startsWith("/")) {
            configAsString = ConfigReader.readConfigsFromFileAsString(url, true);
        } else {
            throw new Exception("Only s3/file url is supported in config - file must be / or s3://");
        }

        Yaml yaml = new Yaml();
        Map<String, Object> obj = yaml.load(configAsString);
        return JsonUtils.getCamelCase().readObject(new StringHelper().stringify(obj), cls);
    }

    private void setupStore(StreamExecutionEnvironment env, T mainConfig, Class<T> cls) {
        EnvironmentConfig config = mainConfig.getEnvironmentConfig();

        // Set checkpoint dir if provided
        if (!Strings.isNullOrEmpty(config.getCheckpointDir())) {
            env.getCheckpointConfig().setCheckpointStorage(config.getCheckpointDir());
        }

        // Set state backed as RocksDb if given in config
        if (Objects.equals("rocksdb", config.getStateBackend())) {
            try {
                env.setStateBackend(new EmbeddedRocksDBStateBackend());
            } catch (Exception e) {
                throw new RuntimeException("Failed to set state backend as rocksdb", e);
            }
        }

        // start a checkpoint every 30 sec
        env.enableCheckpointing(config.getEnableCheckpointing());

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.valueOf(config.getCheckpointingMode()));

        // make sure 30 seconds of pause happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getMinPauseBetweenCheckpoints());

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(config.getCheckpointTimeout());

        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(config.getTolerableCheckpointFailureNumber());

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.getMaxConcurrentCheckpoints());

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.valueOf(config.getEnableExternalizedCheckpoints()));

        // enables the unaligned checkpoints
        if (config.isEnableUnalignedCheckpoints()) {
            env.getCheckpointConfig().enableUnalignedCheckpoints();
        }
    }

    /**
     * This is the runner interface which Job has to implement
     */
    public interface RunJob<T> {
        void run(StreamExecutionEnvironment env, T parameter, Class<T> cls);
    }
}
