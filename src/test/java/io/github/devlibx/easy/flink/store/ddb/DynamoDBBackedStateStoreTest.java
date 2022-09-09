package io.github.devlibx.easy.flink.store.ddb;


import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.store.GenericState;
import io.github.devlibx.easy.flink.store.IGenericStateStore;
import io.github.devlibx.easy.flink.store.Key;
import io.github.devlibx.easy.flink.store.ProxyBackedGenericStateStore;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.DynamoDbConfig;
import io.github.devlibx.easy.flink.utils.v2.config.StateStoreConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class DynamoDBBackedStateStoreTest {

    @Test
    public void testDdbStore() {
        String id = UUID.randomUUID().toString();
        StateStoreConfig stateStoreConfig = new StateStoreConfig();
        stateStoreConfig.setType("dynamo");
        stateStoreConfig.setDdbConfig(DynamoDbConfig.builder().table("harish-table").region("ap-south-1").build());
        Configuration configuration = new Configuration();
        configuration.setStateStore(stateStoreConfig);

        IGenericStateStore dynamoDBBackedStateStore = new ProxyBackedGenericStateStore(configuration);

        dynamoDBBackedStateStore.persist(
                Key.builder()
                        .key(id)
                        .subKey("*")
                        .build(),
                GenericState.builder()
                        .data(StringObjectMap.of("name", "harish"))
                        .build()
        );

        GenericState fromDb = dynamoDBBackedStateStore.get(Key.builder().key(id).subKey("*").build());
        Assertions.assertNotNull(fromDb);
        Assertions.assertEquals(StringObjectMap.of("name", "harish"), fromDb.getData());
    }
}