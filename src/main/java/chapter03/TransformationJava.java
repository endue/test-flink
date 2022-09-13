package chapter03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/9 13:06
 * @Description ：{@link https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/overview/}
 */
public class TransformationJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("a", "b", "c");

        SingleOutputStreamOperator<String> singleOutputStreamOperator = dataStreamSource.map(s -> s.toUpperCase());

        

    }

}
