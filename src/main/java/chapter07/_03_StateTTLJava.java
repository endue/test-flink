package chapter07;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:
 * @Description: 状态的TTL
 * https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/fault-tolerance/state/
 * @Date: 2022/9/24 9:21
 * @Version: 1.0
 */
public class _03_StateTTLJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 开启状态数据的checkpoint（快照周期）
        env.enableCheckpointing(1000);
        // todo 指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("");
        // todo 开启task级别的故障自动failover(最多重启恢复5次[假设job运行过程中总共遇到5次错误也退出],每5s重试一次)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        // todo 设置statebackend
//        env.setStateBackend(new HashMapStateBackend());
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.map(new RichMapFunction<String, String>() {
            ListState<String> state;
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("value", String.class);

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds(5000)) // 数据存活时长5s
                        .setTtl(Time.milliseconds(5000)) // 数据存活时长5s
                        .updateTtlOnReadAndWrite()
//                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 永远不会返回过期的数据
                        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // 如果过期的数据没被清理则返回
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // ttl的时间语义
//                        .useProcessingTime()
                        .cleanupIncrementally(1000, false) // 增量清理(每访问一条数据,就会检查这数据是否过期,每次检查cleanupSize指定的key数量)
                        .cleanupFullSnapshot() // 全量快照清理策略(当checkpoint时做快照,只存储未过期数据)
                        .cleanupInRocksdbCompactFilter(1000) // 增量清理(只对RockdbStateBackend有效)
                        .build()
                );
                state = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public String map(String key) throws Exception {
                state.add(key);

                StringBuilder sb = new StringBuilder();
                for (String s : state.get()) {
                    sb.append(s);
                }
                return sb.toString();
            }
        })
        .print();

        env.execute();
    }


}