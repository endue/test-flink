package chapter05;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:
 * @Description: join示例,基于某个条件合并两个流，等同inner join
 * A，B两个流在同一时间窗口内的数据，按照条件匹配。比如代码示例中的数据，将返回1,aa 1,10,bj   1,aa 1,12,sh    2,bb 2,30,xian     2,bbbb 2,30,xian
 * @Date: 2022/9/14 22:40
 * @Version: 1.0
 */
public class _05_JoinJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource1 = env.fromElements("1,aa", "2,bb", "2,bbbb", "3,cc");
        DataStreamSource<String> dataStreamSource2 = env.fromElements("1,10,bj", "1,12,sh", "2,30,xian", "4,60,guangzhou");

        SingleOutputStreamOperator<Tuple2<String, String>> single1 = dataStreamSource1.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], split[1]);
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> single2 = dataStreamSource2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        });

        single1.join(single2)
                .where(new KeySelector<Tuple2<String, String>, String>() { // 左边流数据的某个字段
                    @Override
                    public String getKey(Tuple2<String, String> s) throws Exception {
                        return s.f0;
                    }
                }).equalTo(new KeySelector<Tuple3<String, String, String>, String>() { // 等于右边流数据的某个字段
                    @Override
                    public String getKey(Tuple3<String, String, String> s) throws Exception {
                        return s.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> s1, Tuple3<String, String, String> s2) throws Exception {
                        return null;
                    }
                });

        env.execute();
    }
}
