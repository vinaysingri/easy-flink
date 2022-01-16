package io.github.devlibx.easy.flink.utils;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper.ObjectToKeyConvertor;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.Serializable;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class EventToJsonMessageSerializationSchema<T> implements KafkaSerializationSchema<T>, Serializable {
    private final String topic;
    private final Class<T> cls;
    private final ObjectToKeyConvertor<T> keyConvertor;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
        return new ProducerRecord<>(topic, keyConvertor.key(element), JsonUtils.asJson(element).getBytes());
    }
}
