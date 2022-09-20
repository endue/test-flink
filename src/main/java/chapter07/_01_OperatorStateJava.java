package chapter07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/20 13:35
 * @Description ：通过socket端口将每次输入的数据和上次输出的数据拼接在一起保存并返回输给下一个算子,开启状态保存以及task故障自动恢复
 * a
 * ab
 * abc
 */
public class _01_OperatorStateJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 开启状态数据的checkpoint（快照周期）
        env.enableCheckpointing(1000);
        // todo 指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("");
        // todo 开启task级别的故障自动failover(最多重启恢复5次[假设job运行过程中总共遇到5次错误也退出],每5s重试一次)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 每来一条数据，输出该字符串拼接之前到过的所有字符串的结果
        source.map(new MystateFunction())
                .print();

        env.execute();
    }
}

class MystateFunction implements MapFunction<String, String>, CheckpointedFunction{
    // 记录每次拼接的结果
    ListState<String> listState;

    /**
     * 来自MapFunction
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public String map(String value) throws Exception {
        listState.add(value);

        // 模拟task异常
        if(value.equalsIgnoreCase("x")){
            throw new Exception("");
        }

        // todo 拼接

        return value;
    }

    /**
     * 来自CheckpointedFunction
     * 对系统数据快照时调用该方法，在持久化前，对状态数据做一些其他操作
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 来自CheckpointedFunction
     * 在task实例任务失败后，自动重启时，加载最近一次快照的数据
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();

        listState = operatorStateStore.getListState(new ListStateDescriptor<String>("strings", String.class));
    }
}
