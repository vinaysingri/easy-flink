package io.github.devlibx.easy.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class MainTemplateTest {

    @Test
    public void testBuildParameterTool() throws Exception {

        Path currentRelativePath = Paths.get("");
        String currentPath = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current absolute path is: " + currentPath);
        String configFileFullPath = currentPath + "/src/test/resources/config.properties";
        ParameterTool parameterTool = MainTemplate.buildParameterTool(new String[]{"--config", configFileFullPath});

        Assertions.assertEquals("localhost:9092", parameterTool.get("brokers"));
        Assertions.assertEquals("orders", parameterTool.get("topic"));
        Assertions.assertEquals(202206, parameterTool.getInt("groupId"));
        Assertions.assertEquals("rocksdb", parameterTool.get("state.backend"));
        Assertions.assertEquals("file://tmp/store", parameterTool.get("state.checkpoints.dir"));

    }

}