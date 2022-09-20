package chapter07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/20 13:35
 * @Description ：算子状态
 */
public class _01_OperatorStateJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 需要开启checkpoint（快照周期）
        env.enableCheckpointing(1000);
        // todo 指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("");

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 每来一条数据，输出该字符串拼接之前到过的所有字符串的结果
        source.map(new MystateFunction())
                .print();

        env.execute();
    }
}

class MystateFunction implements MapFunction<String, String>, CheckpointedFunction{
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
     * 算子任务在启动时调用该方法，初始化状态
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();

        listState = operatorStateStore.getListState(new ListStateDescriptor<String>("strings", String.class));
    }
}
