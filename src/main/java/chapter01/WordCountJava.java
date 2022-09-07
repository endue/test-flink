package chapter01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author meng.li1
 * @Date 2022/9/5 13:25
 * @Description ：
 *
 * public class Test {
 *
 *     public static void main(String[] args) throws IOException {
 *         ServerSocket serverSocket = new ServerSocket();
 *         serverSocket.bind(new InetSocketAddress("127.0.0.1", 9999));
 *
 *         while (true){
 *             Socket accept = serverSocket.accept();
 *             System.out.println("创建连接");
 *             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accept.getOutputStream()));
 *             String message = "a,b,c,e,d,f\n";
 *             while (true){
 *                 try {
 *                     Thread.sleep(1000);
 *                 } catch (InterruptedException e) {
 *                     e.printStackTrace();
 *                 }
 *                 System.out.println(message);
 *                 bw.write(message);
 *                 bw.flush();
 *             }
 *
 *         }
 *     }
 * }
 */
public class WordCountJava {

    public static void main(String[] args) throws Exception {
        // 创建编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 通过source算子,得到一个dataSoruce
        env.socketTextStream("127.0.0.1", 9999)
               .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] split = s.split(",");
                        for (String word : split) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
               })
               .keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple -> tuple.f0)
               .sum(1)
               .print();

        // 触发执行
        env.execute("flink streaming from socket");

    }


//    // 创建编程入口
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//    // 通过source算子,得到一个dataSoruce
//    DataStreamSource<String> source = env.readTextFile("src/main/resources/word.txt");
////        env.setParallelism(2);// 指定默认并行度为2个线程
////        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 调整执行模式是流模式还是批模式
//
//    // 对数据流进行转换
//    SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//        @Override
//        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//            String[] split = s.split(",");
//            for (String word : split) {
//                collector.collect(Tuple2.of(word, 1));
//            }
//        }
//    })
//            // 知道返回类型
////                .returns(new TypeHint<Tuple2<String, Integer>>(){});
////                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
////                .returns(Types.TUPLE(Types.STRING, Types.INT));
//            ;
//
//    KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple -> tuple.f0);
//
//    SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = keyedStream.sum(1);
//
//        outputStreamOperator.print();
//
//        env.execute();
}
