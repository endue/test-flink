package chapter05;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author:
 * @Description:connect流连接,可以将两个数据类型一样或者不一样的DataStream连接成一个新的ConnectedStreams
 * 需要注意的是connect方法和union方法不同，虽然调用connect方法将两个流合并成一个新流。但是里面的两个流依然是独立
 * 的。这个方法最大的好处是让两个流共享state状态
 * @Date: 2022/9/13 23:22
 * @Version: 1.0
 */
public class _02_ConnectJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.fromElements("1,event01,3", "1,event02,2", "2,event03,4");

        DataStreamSource<Integer> s2 = env.fromElements(6, 1, 9, 0);

        ConnectedStreams<String, Integer> connect = s1.connect(s2);

        // 合并为一个流
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s + "_哈哈哈";
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return integer.toString() + "_嘿嘿嘿";
            }
        });

        map.print("打印：");

        env.execute();

    }
}
