package chapter02

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @author meng.li1
 * @Date 2022/9/7 17:46
 * @Description ：
 */
object DataSourceScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.readTextFile("");

    val value = env.fromElements(1, 2, 3, 4)

    val v1 = env.fromCollection(Array(1, 2, 3))


  }
}
