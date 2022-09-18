package chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author:
 * @Description: Watermark生成时机及策略
 * Watermark生成策略
 * WatermarkStrategy.noWatermarks() 禁用事件时间的推进机制
 * WatermarkStrategy.forMonotonousTimestamps() 紧跟最大事件的事件
 * WatermarkStrategy.forBoundedOutOfOrderness() 允许乱序
 *
 * @Date: 2022/9/17 19:30
 * @Version: 1.0
 */
public class _01_WatermarkGenerationStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定时间语意(已过期), 1.12版开始默认为event time，如需调整在widow()后指定即可
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

//        在datasource上设置watermark
//        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
//                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))                // 设置Watermark生成策略
//                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {    // 设置 Watermark生成时机
//                    @Override
//                    public long extractTimestamp(String s, long l) {
//                        return Long.valueOf(s.split(",")[2]);
//                    }
//                });
//        dataStreamSource.assignTimestampsAndWatermarks(watermarkStrategy);


        // 在map后设置watermark, 将并行度由1改为2(并行度下watermark推荐测试用)
        WatermarkStrategy<EventBean> watermarkStrategy = WatermarkStrategy
                .<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))                 // 设置Watermark生成策略
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {     // 设置 Watermark生成时机
                    @Override
                    public long extractTimestamp(EventBean s, long l) {
                        return s.getTimestamp();
                    }
                });

        SingleOutputStreamOperator<EventBean> s2 = dataStreamSource
                .map(s -> {
                    String[] split = s.split(",");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                })
                .returns(TypeInformation.of(EventBean.class))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .setParallelism(2);

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean value, Context ctx, Collector<EventBean> out) throws Exception {
                System.out.println("当前Watermark:" + ctx.timerService().currentWatermark());
                System.out.println("当前ProcessingTime:" + ctx.timerService().currentProcessingTime());

                out.collect(value);
            }
        }).print("process:");

        env.execute();
    }
}

class EventBean{
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;

    public EventBean() {
    }

    public EventBean(long guid, String eventId, long timestamp, String pageId) {
        this.guid = guid;
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.pageId = pageId;
    }

    public long getGuid() {
        return guid;
    }

    public void setGuid(long guid) {
        this.guid = guid;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }
}