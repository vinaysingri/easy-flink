package io.github.devlibx.easy.flink.functions;

import io.github.devlibx.easy.flink.functions.common.EventCount;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This KeyedProcessFunction is useful if you want emit no of event received in your window.
 */
public class EventCountAggregatorInWindowKeyedProcessFunction extends KeyedProcessFunction<String, Integer, EventCount> {

    @Override
    public void processElement(Integer count, Context ctx, Collector<EventCount> out) throws Exception {
        EventCount eventCount = new EventCount();
        eventCount.setCount(count);
        out.collect(eventCount);
    }
}


