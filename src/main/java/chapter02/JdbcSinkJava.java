package chapter02;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author meng.li1
 * @Date 2022/9/13 19:10
 * @Description ：
 */
public class JdbcSinkJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.fromElements("1", "2", "3", "4");

        SinkFunction<String> jdbcSink = JdbcSink.sink(
                "insert into t_id values(?,?,?) on duplicate key update id=?, service=?, status=?",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement ps, String s) throws SQLException {
                        ps.setLong(1, Long.valueOf(s));
                        ps.setString(2, s);
                        ps.setString(3, s);

                        ps.setLong(4, Long.valueOf(s));
                        ps.setString(5, s);
                        ps.setString(6, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(10 * 1000)
                        .withMaxRetries(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("")
                        .withPassword("")
                        .withUsername("")
                        .build()
        );

        dataStreamSource.addSink(jdbcSink);
    }
}
