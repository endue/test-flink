package chapter07;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:
 * @Description: task重启策略
 * @Date: 2022/9/25 16:36
 * @Version: 1.0
 */
public class _05_TaskRestartStrategyJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // task重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 2));
//        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart());
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.hours(1), Time.seconds(10)));

    }
}
