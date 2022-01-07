package io.github.devlibx.easy.flink.functions;

import io.github.devlibx.easy.flink.functions.common.EventCount;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

/**
 * There are cases where you listen to events over window and you want to trigger event when window did not get any
 * events in this window.
 * <p>
 * In such cases where 0 events in time window (n) is needed, you can use this KeyedProcessFunction.
 * <p>
 * What is "timeToWaitBeforeGeneratingMissingEventsEvent"?
 * Suppose you have window "size" = 5 min & "slide" of "1" min.
 * <p>
 * This time will allow you to trigger this no-event in window event after
 * <code>timeToWaitBeforeGeneratingMissingEventsEvent</code> sec i.e. after waiting for
 * <code>timeToWaitBeforeGeneratingMissingEventsEvent</code> many sec with no events, a event will be triggered.
 */
public class EventNotReceivedInWindowKeyedProcessFunction extends KeyedProcessFunction<String, Integer, EventCount> {
    private final int slide;
    private long nextTimeStamp;

    /**
     * @param timeToWaitBeforeGeneratingMissingEventsEvent Suppose you have window "size" = 5 min & "slide" of "1" min.
     *                                                     <p>
     *                                                     This time will allow you to trigger this no-event in window event after
     *                                                     <code>timeToWaitBeforeGeneratingMissingEventsEvent</code> sec i.e. after waiting for
     *                                                     <code>timeToWaitBeforeGeneratingMissingEventsEvent</code> many sec with no events, a event will be triggered.
     */
    public EventNotReceivedInWindowKeyedProcessFunction(int timeToWaitBeforeGeneratingMissingEventsEvent) {
        this.slide = timeToWaitBeforeGeneratingMissingEventsEvent;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EventCount> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        // Send a alert that we did not get events in last window
        EventCount eventCount = new EventCount();
        eventCount.setCount(0);
        out.collect(eventCount);

        ctx.timerService().deleteProcessingTimeTimer(timestamp);
        nextTimeStamp = DateTime.now().plusSeconds(slide + 10).getMillis();
        ctx.timerService().registerProcessingTimeTimer(nextTimeStamp);
    }

    @Override
    public void processElement(Integer count, Context ctx, Collector<EventCount> out) throws Exception {
        ctx.timerService().deleteProcessingTimeTimer(nextTimeStamp);
        nextTimeStamp = DateTime.now().plusSeconds(slide + 10).getMillis();
        ctx.timerService().registerProcessingTimeTimer(nextTimeStamp);
    }
}


