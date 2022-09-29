package chapter01

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random


object SourceTestScala {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_8", 1547718205, 38.1)
    )
    val dataStream = env.fromCollection(dataList)
    dataStream.print("dataStream")

    // 从kafka读取数据
    val properties = new Properties
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "sensor-group-1")
    properties.put("auto.offset.reset", "latest")
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
    kafkaDataStream.print("kafkaDataStream")

    // 自定义source
    env.addSource(new SensorSource)

    env.execute()
  }
}

/**
  * 自定义source
  */
class SensorSource extends SourceFunction[SensorReading]{

  var close = false


  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    while (close) {
      1.to(10)
        .map(num => SensorReading("sensor_" + num, System.currentTimeMillis() / 1000, Random.nextDouble() * 100))
        .foreach(s => ctx.collect(s))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = close = true
}

/**
  * 温度传感器类
  * @param id
  * @param timestamp
  * @param temperature
  */
case class SensorReading(id: String, timestamp: Long, temperature: Double)