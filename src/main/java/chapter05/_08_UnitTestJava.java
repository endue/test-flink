package chapter05;

import chapter05.domain.EventCount;
import chapter05.domain.EventUserInfo;
import chapter05.domain.UserInfo;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author:
 * @Description:
 * @Date: 2022/9/15 22:19
 * @Version: 1.0
 */
public class _08_UnitTestJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> s1 = env.fromElements("1,event01,3", "1,event02,2", "2,event03,4");

        DataStreamSource<String> s2 = env.fromElements("1,male,shanghai", "2,femail,beijing");


        SingleOutputStreamOperator<EventCount> dataSource1 = s1.map(s -> {
            String[] split = s.split(",");
            return new EventCount(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
        });

        SingleOutputStreamOperator<UserInfo> dataSource2 = s2.map(s -> {
            String[] split = s.split(",");
            return new UserInfo(Integer.parseInt(split[0]), split[1], split[2]);
        });


        // 将dataSource1中的数据展开，展开条数基于第三个字段数量
        SingleOutputStreamOperator<EventCount> flattend = dataSource1.process(new ProcessFunction<EventCount, EventCount>() {
            @Override
            public void processElement(EventCount eventCount, Context context, Collector<EventCount> collector) throws Exception {
                for (int i = 0; i < eventCount.getCnt(); i++) {
                    collector.collect(new EventCount(eventCount.getId(), eventCount.getEventId(), RandomUtils.nextInt(1, 1000)));
                }
            }
        });


        // 将dataSource1中展开后的数据流关联dataSource2中的数据(性别，城市信息)，未关联上数据输入到测流
        MapStateDescriptor<Integer, UserInfo> userInfoStateDescriptor = new MapStateDescriptor<>("userInfoState",
                TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<UserInfo>() {}));
        BroadcastStream<UserInfo> dataSource2Broadcast = dataSource2.broadcast(userInfoStateDescriptor);

        BroadcastConnectedStream<EventCount, UserInfo> connect = flattend.connect(dataSource2Broadcast);


        OutputTag coutputTag = new OutputTag("c", TypeInformation.of(EventUserInfo.class));

        SingleOutputStreamOperator<EventUserInfo> joinedStream = connect.process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {
            @Override
            public void processElement(EventCount eventCount, ReadOnlyContext readOnlyContext, Collector<EventUserInfo> collector) throws Exception {
                ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = readOnlyContext.getBroadcastState(userInfoStateDescriptor);

                UserInfo userInfo;
                if (broadcastState == null || (userInfo = broadcastState.get(eventCount.getId())) == null) {
                    readOnlyContext.output(coutputTag, new EventUserInfo(eventCount.getId(), eventCount.getEventId(), eventCount.getCnt(),
                                    null, null));
                } else {
                    collector.collect(new EventUserInfo(eventCount.getId(), eventCount.getEventId(), eventCount.getCnt(),
                            userInfo.getGender(), userInfo.getGender()));
                }
            }

            @Override
            public void processBroadcastElement(UserInfo userInfo, Context context, Collector<EventUserInfo> collector) throws Exception {
                context.getBroadcastState(userInfoStateDescriptor).put(userInfo.getId(), userInfo);
            }
        });

        joinedStream.print("joinedStream");

        // 关联后的数据做测流输出，随机数能被7整除的放入测流，否则放入主流
//        joinedStream.process(new ProcessFunction<EventUserInfo, EventUserInfo>() {
////            @Override
////            public void processElement(EventUserInfo eventUserInfo, Context context, Collector<EventUserInfo> collector) throws Exception {
////                if(eventUserInfo.getCnt() % 7 == 0){
////                    context.output(new OutputTag<EventUserInfo>(""), eventUserInfo);
////                }else {
////                    collector.collect(eventUserInfo);
////                }
////            }
////        })

        // 对主流数据按性别分组，取最大随机数所在的第一条数据作为结果输出
        SingleOutputStreamOperator<EventUserInfo> mainResult = joinedStream.keyBy(new KeySelector<EventUserInfo, String>() {
            @Override
            public String getKey(EventUserInfo eventUserInfo) throws Exception {
                return eventUserInfo.getGender();
            }
        }, TypeInformation.of(String.class))
                .maxBy("cnt");

        mainResult.print("mainResult");

        joinedStream.getSideOutput(coutputTag).print("side");

        // 写入数据库
        SinkFunction<EventUserInfo> sink = JdbcSink.sink("", new JdbcStatementBuilder<EventUserInfo>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventUserInfo eventUserInfo) throws SQLException {

                    }
                }, JdbcExecutionOptions.builder().build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().build());

        mainResult.addSink(sink);

        env.execute();
    }
}
