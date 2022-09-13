package chapter02;

import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
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

//        SinkFunction<String> jdbcSink = JdbcSink.sink(
//                "insert into t_id values(?,?,?) on duplicate key update id=?, service=?, status=?",
//                new JdbcStatementBuilder<String>() {
//                    @Override
//                    public void accept(PreparedStatement ps, String s) throws SQLException {
//                        ps.setLong(1, Long.valueOf(s));
//                        ps.setString(2, s);
//                        ps.setString(3, s);
//
//                        ps.setLong(4, Long.valueOf(s));
//                        ps.setString(5, s);
//                        ps.setString(6, s);
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(10)
//                        .withBatchIntervalMs(10 * 1000)
//                        .withMaxRetries(1)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("")
//                        .withPassword("")
//                        .withUsername("")
//                        .build()
//        );
//
//        dataStreamSource.addSink(jdbcSink);


        SinkFunction<String> jdbcSink = JdbcSink.exactlyOnceSink(
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
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个事务,必须设置为true
                        .build(),
                new SerializableSupplier<XADataSource>() {// XADataSourcejiushi jdbc连接，只不过它是支持分布式事务的连接
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("");
                        xaDataSource.setUser("");
                        xaDataSource.setPassword("");
                        return xaDataSource;
                    }
                }
        );

        dataStreamSource.addSink(jdbcSink);
    }
}
