package io.github.devlibx.easy.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Count aggregation function for common usage
 */
public class CountAggregatorFunction<T> implements AggregateFunction<T, EventCounter, Integer> {

    @Override
    public EventCounter createAccumulator() {
        return new EventCounter();
    }

    @Override
    public EventCounter add(T value, EventCounter accumulator) {
        accumulator.count++;
        return accumulator;
    }

    @Override
    public Integer getResult(EventCounter accumulator) {
        return accumulator.count;
    }

    @Override
    public EventCounter merge(EventCounter a, EventCounter b) {
        a.count += b.count;
        return a;
    }
}
