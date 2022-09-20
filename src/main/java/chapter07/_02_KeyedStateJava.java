package chapter07;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/20 21:53
 * @Version: 1.0
 */
public class _02_KeyedStateJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 开启状态数据的checkpoint（快照周期）
        env.enableCheckpointing(1000);
        // todo 指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("");
        // todo 开启task级别的故障自动failover(最多重启恢复5次[假设job运行过程中总共遇到5次错误也退出],每5s重试一次)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.keyBy(s -> s) // 状态是和key绑定的，是每个key都会有一个状态来存储。假设输入a b c会有三个状态
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("lsd", String.class));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        listState.add(value);

                        StringBuilder sb = new StringBuilder();
                        for (String s : listState.get()) {
                            sb.append(s);
                        }

                        return sb.toString();
                    }
                }).setParallelism(2)
                .print().setParallelism(2);
    }
}
