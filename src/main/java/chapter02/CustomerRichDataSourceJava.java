package chapter02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author meng.li1
 * @Date 2022/9/8 20:26
 * @Description ：单并行度
 */
public class CustomerRichDataSourceJava extends RichSourceFunction {

    /**
     * source组件初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    /**
     * source组件生成数据的过程
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext ctx) throws Exception {

    }

    /**
     * job取消
     */
    @Override
    public void cancel() {

    }

    /**
     * source组件关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
