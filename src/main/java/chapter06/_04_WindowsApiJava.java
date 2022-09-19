package chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/19 22:09
 * @Version: 1.0
 */
public class _04_WindowsApiJava {

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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 允许窗口触发后，再次迟到的数据最多2s
                .sideOutputLateData(outputTag) // 迟到超过2s后的数据，输入到测流,比如窗口时间 00:00-00:10,那么再00:12之后到达的属于该窗口的数据将被输入测流
                .apply(new WindowFunction<EventBean, EventBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<EventBean> input, Collector<EventBean> out) throws Exception {
                        while (input.iterator().hasNext()){
                            out.collect(input.iterator().next());
                        }
                    }
                });

        DataStream<EventBean>  allowedLatenessStream = mainStream.getSideOutput(outputTag);

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
