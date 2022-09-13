package chapter04

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.kafka.clients.consumer.OffsetResetStrategy



/**
 * @author meng.li1
 * @Date 2022/9/7 19:39
 * @Description ：
 */
object KafkaDataSourceScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource = KafkaSource.builder[String]()
      .setTopics("kafka_flink_topic")
      .setGroupId("1")
      .setBootstrapServers("10.106.108.179:9092")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      //      .setProperty("auto.offset.commit", "true") // 开启kafka底层消费者(flink)字段位移提交机制,消费者会提交最新消费位移到kafka
      //      .setBounded(OffsetsInitializer.committedOffsets()) // 表示有界流,在读取数据时,读取到指定位置就停止读取,KafkaSource退出
      //      .setUnbounded(OffsetsInitializer.committedOffsets()) // 表示无界流,在读取数据时,读取到知道位置就停止读取,但KafkaSource不会退出
      .build()

    //    env.addSource() // 接收SourceFunction接口的实现类
    val source: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String](), "kafkaSource") // 接收Source接口的实现类
    source.print()

    env.execute("KafkaDataSourceScala")
  }
}
