package chapter07;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author:
 * @Description: https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/config/#checkpointing
 * @Date: 2022/9/25 13:37
 * @Version: 1.0
 */
public class _04_CheckpointJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint的间隔
        env.enableCheckpointing(3000);
        // checkpoint的存储位置
        env.getCheckpointConfig().setCheckpointStorage("");
        // 允许checkpoint的失败最大次数(超过最大数量,job失败)
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // checkpoint的算法模式（EXACTLY_ONCE 是否需要对齐）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint的对齐超时时间(超过该时间,则认为checkpoint失败)
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(3));
        // 两次checkpoint之间最小时间间隔
        env.getCheckpointConfig().setCheckpointInterval(3000);
        // checkpoint最大并行度(barrier1, barrier2, barrier3都在处理中，这就是最大并行度)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        // job取消是否保留checkpoint的数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    }
}
