package chapter05;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author meng.li1
 * @Date 2022/9/15 12:21
 * @Description ：
 */
public class _06_BroadcastJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 用户行为流(源源不断)
         * id, 行为
         */
        DataStreamSource<String> s1 = env.fromElements("1,学习", "2,跑步", "2,旅游", "3,开车", "2,跑步", "4,睡觉");
        /**
         * 用户信息流(数据比较少)
         * id, age, address
         */
        DataStreamSource<String> s2 = env.fromElements("1,10,bj", "2,30,xian", "4,60,guangzhou");

        SingleOutputStreamOperator<Tuple2<String, String>> single1 = s1.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], split[1]);
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> single2 = s2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], split[2]);
        });


        MapStateDescriptor<String, Tuple3<String, String, String>> userInfoStateDescriptor = new MapStateDescriptor<>("userInfoState",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                }));

        // 将s2数据流转为广播流
        BroadcastStream<Tuple3<String, String, String>> s2broadcast = single2.broadcast(userInfoStateDescriptor);

        // 哪个流用广播流，就去连接
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connect = single1.connect(s2broadcast);

        SingleOutputStreamOperator<String> process = connect.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
            /**
             * 处理single1流数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Tuple3<String, String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDescriptor);
                Tuple3<String, String, String> userInfo;
                if (broadcastState != null && (userInfo = broadcastState.get(value.f0)) != null) {
                    out.collect(value.f0 + "" + value.f1 + "" + userInfo.f0 + "" + userInfo.f1 + "" + userInfo.f2);
                } else {
                    out.collect(value.f0 + "" + value.f1);
                }
            }

            /**
             * 处理s2broadcast流数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                // 拿到广播状态
                BroadcastState<String, Tuple3<String, String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDescriptor);

                broadcastState.put(value.f0, value);
            }
        });

        process.print();


        env.execute();
    }
}
