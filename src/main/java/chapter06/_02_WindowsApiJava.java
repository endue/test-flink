package chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author:
 * @Description: 基于事件时间处理(1.12版本开始默认就是这个时间)
 * @Date: 2022/9/18 15:43
 * @Version: 1.0
 */
public class _02_WindowsApiJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 分配watermark推荐事件时间
        SingleOutputStreamOperator<EventBean> watermarkBeanStream = source
                .map(s -> {
                    String[] split = s.split(",");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                })
                .returns(TypeInformation.of(EventBean.class))
                .assignTimestampsAndWatermarks(createWatermarkStrategy());

        // 1.每隔10s，统计最近30s中各个用户的事件个数
        SingleOutputStreamOperator<Integer> rs1 = watermarkBeanStream
                .keyBy(EventBean::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventBean, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(EventBean value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        rs1.print("rs1==");

        // 2.每隔10s，统计最近30s中各个用户每次平均行为时长
        SingleOutputStreamOperator<Double> rs2 = watermarkBeanStream
                .keyBy(EventBean::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventBean, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(EventBean value, Tuple2<Integer, Integer> accumulator) {
                        accumulator.setFields(accumulator.f0 + 1, accumulator.f1 + Long.valueOf(value.getTimestamp()).intValue());
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f1 / (double)accumulator.f0;
                    }

                    /**
                     * 在批计算中上游局部聚合，然后局部聚合后结果交给下游再做全局聚合
                     * 在流式计算中，不存在这种情况
                     * @param a
                     * @param b
                     * @return
                     */
                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        a.setFields(a.f0 + b.f0, a.f1 + b.f1);
                        return a;
                    }
                });
        rs2.print("rs2==");

        // 3.每隔10s，统计最近30s的数据中，每个用户行为事件中，时长最长的2条记录
        SingleOutputStreamOperator<EventBean> rs3_apply = watermarkBeanStream
                .keyBy(EventBean::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .apply(new WindowFunction<EventBean, EventBean, Long, TimeWindow>() {
                    /**
                     * q:这里为什么给key呢？
                     * a:假设并行度为2，用户千万怎么处理，所以每个并行度实例上会分配n多用户,当某个用户的统计触发后会调用当前方法，key就是用户id(guid)
                     * @param key 当前keyBy的具体值
                     * @param window 本次触发的窗口元信息
                     * @param input 本次触发窗口中所有数据的迭代器
                     * @param out 输出器
                     * @throws Exception
                     */
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<EventBean> input, Collector<EventBean> out) throws Exception {
                        // a.取出input中的最大和次大值
                        // b.放入out中
                    }
                });
        rs3_apply.print("rs3_apply");

        SingleOutputStreamOperator<EventBean> rs3_process = watermarkBeanStream
                .keyBy(EventBean::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean, EventBean, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<EventBean> elements, Collector<EventBean> out) throws Exception {
                        TimeWindow window = context.window();
                        // a.取出input中的最大和次大值
                        // b.放入out中
                    }
                });
        rs3_process.print("rs3_process");





        env.execute();
    }


    private static WatermarkStrategy<EventBean> createWatermarkStrategy(){
        return WatermarkStrategy
                .<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(0)) // 允许乱序为0
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() { // 获取timestamp
                    @Override
                    public long extractTimestamp(EventBean s, long l) {
                        return s.getTimestamp();
                    }
                });
    }
}
