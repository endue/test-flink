package chapter05;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author:
 * @Description: process分流操作
 * @Date: 2022/9/13 22:55
 * @Version: 1.0
 */
public class _01_ProcessJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource = env.fromElements("1", "2", "3", "4");

        SingleOutputStreamOperator<String> process = dataStreamSource.process(new ProcessFunction<String, String>() {
            /**
             * @param s 输入的数据
             * @param context 上下文，提供"侧流输出"功能
             * @param collector 主流收集器
             * @throws Exception
             */
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                if (s.equals("1") || s.equals("3")) {
                    context.output(new OutputTag<String>("odd", TypeInformation.of(String.class)), s);
                } else if (s.equals("2")) {
                    context.output(new OutputTag<String>("even", TypeInformation.of(String.class)), s);
                } else {
                    collector.collect(s);
                }
            }
        });

        DataStream<String> odd = process.getSideOutput(new OutputTag<String>("odd", TypeInformation.of(String.class)));
        odd.print("odd");

        DataStream<String> even = process.getSideOutput(new OutputTag<String>("even", TypeInformation.of(String.class)));
        even.print("even");

        env.execute();
    }
}
