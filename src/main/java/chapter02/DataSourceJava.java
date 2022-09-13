package chapter02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author meng.li1
 * @Date 2022/9/7 13:18
 * @Description ：flink数据源
 */
public class DataSourceJava {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 基于集合的数据源
//        env.fromElements(1, 2, 3, 1);
//        env.fromCollection()

//        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
//        env.fromCollection(list);

        env.addSource(new CustomDataSourceJava());
    }
}
