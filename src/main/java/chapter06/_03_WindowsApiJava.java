package chapter06;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/19 21:47
 * @Version: 1.0
 */
public class _03_WindowsApiJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean> stream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
        });

        /**
         * 全局
         */

        // 数量滚动窗口
        stream.countWindowAll(10) // 10条数据一个窗口
                .apply(new AllWindowFunction<EventBean, EventBean, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<EventBean> values, Collector<EventBean> out) throws Exception {

                    }
                });

        // 数量滑动窗口
        stream.countWindowAll(10, 5); // 10条数据一个窗口,滑动步长5

        // 事件时间滚动窗口
        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30))); // 窗口长度为30s

        // 事件时间滚滑动口
        stream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))); // 窗口长度为30s,滑动步长10s

        // 事件时间会话窗口
        stream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))); // 事件间隙超过30s，则划分窗口

        // 处理时间滚动窗口
        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));

        // 处理时间滚滑动口
        stream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

        // 处理时间会话窗口
        stream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

        /**
         * keyed
         */
        KeyedStream<EventBean, String> keyedStream = stream.keyBy(EventBean::getEventId);

        // 数量滑动窗口
        keyedStream.countWindowAll(10, 5); // 10条数据一个窗口,滑动步长5

        // 事件时间滚动窗口
        keyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30))); // 窗口长度为30s

        // 事件时间滚动窗口
        keyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30))); // 窗口长度为30s

        // 事件时间滚滑动口
        keyedStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))); // 窗口长度为30s,滑动步长10s

        // 事件时间会话窗口
        keyedStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))); // 事件间隙超过30s，则划分窗口

        // 处理时间滚动窗口
        keyedStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));

        // 处理时间滚滑动口
        keyedStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

        // 处理时间会话窗口
        keyedStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

        env.execute();
    }
}
