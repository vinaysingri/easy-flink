package io.github.devlibx.easy.flink.utils.v2;

import io.github.devlibx.easy.flink.utils.v2.MainTemplateV2.RunJob;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

public class MainTemplateV2Test {

    @Test
    public void testConfigFromProjectDirectory() throws Exception {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println(s);
    }

    @Test
    public void testMainTemplateV2() throws Exception {
        Assertions.assertNotNull(getClass().getClassLoader().getResource("config.yaml"));
        String configFilePath = getClass().getClassLoader().getResource("config.yaml").getPath();
        Assertions.assertNotNull(configFilePath);
        System.out.println(configFilePath);

        AtomicReference<Configuration> configuration = new AtomicReference<>();
        RunJob<Configuration> job = (env, parameter, cls) -> configuration.set(parameter);

        MainTemplateV2<Configuration> template = new MainTemplateV2<>();
        template.main(new String[]{"--config", configFilePath, "--dryRun", "true"}, "test", job, Configuration.class);
        Assertions.assertNotNull(configuration.get());

        Configuration cfg = configuration.get();
        Assertions.assertNotNull(cfg.getSources());
        Assertions.assertEquals(1, cfg.getSources().size());
        Assertions.assertEquals("localhost:9092", cfg.getSources().get("mainInput").getBroker());
        Assertions.assertEquals("orders_in", cfg.getSources().get("mainInput").getTopic());
        Assertions.assertEquals("1234", cfg.getSources().get("mainInput").getGroupId());
        Assertions.assertEquals("KAFKA", cfg.getSources().get("mainInput").getType());
    }

    private static class TestConfig extends Configuration {
    }
}