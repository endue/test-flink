package chapter03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/9 13:06
 * @Description ：https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/overview/
 */
public class TransformationJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.fromElements("a", "b", "c");

        SingleOutputStreamOperator<String> singleOutputStreamOperator = dataStreamSource.map(s -> s.toUpperCase());

        SingleOutputStreamOperator<String> map = singleOutputStreamOperator.map(new MyMapRichFunction());

        map.print("map==");

        env.execute();

    }


}

class MyMapRichFunction extends RichMapFunction<String, String>{

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open 打开");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close 关闭");
    }

    @Override
    public String map(String value) throws Exception {
        return value + "123";
    }
}