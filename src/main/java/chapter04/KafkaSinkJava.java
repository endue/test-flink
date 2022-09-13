package chapter04;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/13 13:08
 * @Description ：读取数据流写入kafka
 */
public class KafkaSinkJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("1", "2", "3", "4");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka 地址")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        dataStreamSource.sinkTo(kafkaSink);

    }
}
