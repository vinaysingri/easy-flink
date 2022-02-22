package io.github.devlibx.easy.flink.utils;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor(onConstructor_ = {@Inject})
@Slf4j
public class JsonMessageToEventDeserializationSchema<T> implements DeserializationSchema<T>, Serializable {
    private final Class<T> cls;

    @Override
    public T deserialize(byte[] message) throws IOException {
        String line = "";
        try {
            line = new String(message, StandardCharsets.UTF_8);
            return JsonUtils.readObject(line, cls);
        } catch (Exception e) {
            log.error("error in deserialization of message={}", line, e);
            return null;
        }
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
