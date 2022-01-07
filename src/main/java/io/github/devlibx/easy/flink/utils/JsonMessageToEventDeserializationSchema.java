package io.github.devlibx.easy.flink.utils;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class JsonMessageToEventDeserializationSchema<T> implements DeserializationSchema<T> {
    private final Class<T> cls;

    @Override
    public T deserialize(byte[] message) throws IOException {
        String line = new String(message, StandardCharsets.UTF_8);
        return JsonUtils.readObject(line, cls);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(cls);
    }
}
