package chapter02;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

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
        TimeUnit.SECONDS.sleep(10);
        // 这里可以设置数据源
        while (!cancel){
            ctx.collect(System.currentTimeMillis() + "");
            TimeUnit.SECONDS.sleep(1);
        }

    }

    @Override
    public void cancel() {
        if(cancel){

        }
    }
}
