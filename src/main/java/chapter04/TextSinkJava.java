package chapter04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author meng.li1
 * @Date 2022/9/10 10:01
 * @Description ：
 */
public class TextSinkJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);// 并行度会导致多个文件
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

//        dataStreamSource.writeAsText("C:\\flink\\");
//        dataStreamSource.writeAsText("D:\\flink\\", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }
}
