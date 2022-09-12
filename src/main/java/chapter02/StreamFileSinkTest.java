package chapter02;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Arrays;

/**
 * @author meng.li1
 * @Date 2022/9/10 13:18
 * @Description ：
 */
public class StreamFileSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointStorage("d:/flink/");

        DataStreamSource<Integer> dataStreamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        FileSink<Integer> forRowFormatSink = FileSink
                .forRowFormat(new Path("d:/flink/"), new SimpleStringEncoder<Integer>("utr-f"))
                // 文件滚动策略
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(10 * 1000) // 间隔10s，进行文件切换
                        .withMaxPartSize(1024) // 文件到达1024字节，进行切换
                        .build())
                // 文件分桶策略
                .withBucketAssigner(new DateTimeBucketAssigner<Integer>())
                .withBucketCheckInterval(5)
                // 输出文件名相关配置
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("ha")
                        .withPartSuffix(".hh")
                        .build())
                .build();

        dataStreamSource.sinkTo(forRowFormatSink);

    }
}
