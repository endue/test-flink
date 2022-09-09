package chapter02;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author meng.li1
 * @Date 2022/9/8 20:03
 * @Description ：单并行度
 */
public class CustomDataSourceJava implements SourceFunction<String> {

    private volatile boolean cancel = false;

    /**
     * 不断调用当前方法,返回数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 这里可以设置数据源
        while (!cancel){
            ctx.collect(System.currentTimeMillis() + "");
        }

    }

    @Override
    public void cancel() {
        if(cancel){

        }
    }
}
