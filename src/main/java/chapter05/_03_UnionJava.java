package chapter05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/14 13:08
 * @Description ：union合并流操作，参与union的流数据类型要一致
 */
public class _03_UnionJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource1 = env.fromElements("1", "2", "3", "4");
        DataStreamSource<String> dataStreamSource2 = env.fromElements("5", "6", "7", "8");

        DataStream<String> union = dataStreamSource1.union(dataStreamSource2);

        union.print();

        env.execute();

    }
}
