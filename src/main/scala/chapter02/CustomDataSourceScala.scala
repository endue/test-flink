package chapter02

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author meng.li1
 * @Date 2022/9/8 19:59
 * @Description ：
 */
class CustomDataSourceScala extends SourceFunction[String]{// 数据流对象

  @volatile private var c = false

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

  }

  override def cancel(): Unit = {

  }
}
