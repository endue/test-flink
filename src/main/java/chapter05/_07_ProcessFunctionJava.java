package chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:
 * @Description: ProcessFunction大全
 * @Date: 2022/9/15 21:52
 * @Version: 1.0
 */
public class _07_ProcessFunctionJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource1 = env.fromElements("1,aa", "2,bb", "2,bbbb", "3,cc");


        /**
         * ProcessFunction
         * 实现map一样的操作
         */
        SingleOutputStreamOperator<Tuple2<String, String>> processFunctionStream = dataStreamSource1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            @Override
            public void processElement(String s, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] split = s.split(",");
                // 输入到主流
                collector.collect(Tuple2.of(split[0], split[1]));
            }
        });

        /**
         * KeyedProcessFunction
         */
        processFunctionStream.keyBy(t -> t.f0).process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> value, Context context, Collector<Tuple2<Integer, String>> collector) throws Exception {
                // 将ID变为正数，字母变大写
                collector.collect(Tuple2.of(Integer.parseInt(value.f0), value.f1.toUpperCase()));
            }
        });

    }
}
