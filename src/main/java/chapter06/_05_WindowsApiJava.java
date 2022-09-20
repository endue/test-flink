package chapter06;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

/**
 * @Author:
 * @Description: 窗口中的trigger和evictor。窗口触发时，先去调用evictor的evictBefore方法，然后计算，计算完成后在调用evictor的evictorAfter方法
 * @Date: 2022/9/20 7:05
 * @Version: 1.0
 */
public class _05_WindowsApiJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean> streamsource = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        });

        SingleOutputStreamOperator<EventBean> watermarksource = streamsource.assignTimestampsAndWatermarks(createWatermarkStrategy());

        OutputTag<EventBean> outputTag = new OutputTag<EventBean>("allowedLateness");

        SingleOutputStreamOperator<EventBean> mainStream = watermarksource.keyBy(EventBean::getEventId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 用户eventId为e0x时立即触发窗口
                .trigger(MyEventTimeTrigger.create())
                // 触发窗口后,忽略eventId为e0x的数据
                .evictor(MyEvictor.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 允许窗口触发后，再次迟到的数据最多2s,比如窗口时间 00:00-00:10,那么在00:10 - 00:12之间到达的属于该窗口的数据将被再次调用主流
                .sideOutputLateData(outputTag) // 迟到超过2s后的数据，输入到测流,比如窗口时间 00:00-00:10,那么在00:12之后到达的属于该窗口的数据将被输入测流,主流不触发
                .apply(new WindowFunction<EventBean, EventBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<EventBean> input, Collector<EventBean> out) throws Exception {
                        while (input.iterator().hasNext()){
                            out.collect(input.iterator().next());
                        }
                    }
                });

        DataStream<EventBean> allowedLatenessStream = mainStream.getSideOutput(outputTag);

    }

    private static WatermarkStrategy<EventBean> createWatermarkStrategy(){
        return WatermarkStrategy
                .<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(0)) // 允许迟到为0
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() { // 获取timestamp
                    @Override
                    public long extractTimestamp(EventBean s, long l) {
                        return s.getTimestamp();
                    }
                });
    }

}

class MyEvictor<W extends Window> implements Evictor<EventBean, W> {
    private final long windowSize;
    private final boolean doEvictAfter;

    public MyEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    /**
     * 触发窗口后,在窗口计算之前
     * @param elements
     * @param size
     * @param window
     * @param ctx
     */
    @Override
    public void evictBefore(
            Iterable<TimestampedValue<EventBean>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**
     * 触发窗口后,在窗口计算之后
     * @param elements
     * @param size
     * @param window
     * @param ctx
     */
    @Override
    public void evictAfter(
            Iterable<TimestampedValue<EventBean>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    private void evict(Iterable<TimestampedValue<EventBean>> elements, int size, EvictorContext ctx) {
        if (!hasTimestamp(elements)) {
            return;
        }

        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<EventBean>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<EventBean> record = iterator.next();
            if (record.getTimestamp() <= evictCutoff || "e0x".equalsIgnoreCase(record.getValue().getEventId())) {
                iterator.remove();
            }
        }
    }

    /**
     * Returns true if the first element in the Iterable of {@link TimestampedValue} has a
     * timestamp.
     */
    private boolean hasTimestamp(Iterable<TimestampedValue<EventBean>> elements) {
        Iterator<TimestampedValue<EventBean>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /**
     * @param elements The elements currently in the pane.
     * @return The maximum value of timestamp among the elements.
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<EventBean>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<EventBean>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<EventBean> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }

    @Override
    public String toString() {
        return "TimeEvictor(" + windowSize + ")";
    }

    @VisibleForTesting
    public long getWindowSize() {
        return windowSize;
    }

    /**
     * Creates a {@code TimeEvictor} that keeps the given number of elements. Eviction is done
     * before the window function.
     *
     * @param windowSize The amount of time for which to keep elements.
     */
    public static <W extends Window> MyEvictor<W> of(Time windowSize) {
        return new MyEvictor<>(windowSize.toMilliseconds());
    }

    /**
     * Creates a {@code TimeEvictor} that keeps the given number of elements. Eviction is done
     * before/after the window function based on the value of doEvictAfter.
     *
     * @param windowSize The amount of time for which to keep elements.
     * @param doEvictAfter Whether eviction is done after window function.
     */
    public static <W extends Window> MyEvictor<W> of(Time windowSize, boolean doEvictAfter) {
        return new MyEvictor<>(windowSize.toMilliseconds(), doEvictAfter);
    }

}

class MyEventTimeTrigger extends Trigger<EventBean, TimeWindow> {

    private MyEventTimeTrigger() {}

    /**
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(
            EventBean element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // 元素包含e0x或者到达时间窗口 会触发
        if("e0x".equalsIgnoreCase(element.getEventId())){
            return TriggerResult.FIRE;
        }

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }

}
