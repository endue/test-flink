package chapter01

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @author meng.li1
 * @Date 2022/9/5 13:26
 * @Description ：
 */
object WordCountScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.readTextFile("src/main/resources/word.txt")

    val streamOperator = source.flatMap(s => s.split(",")).map(w => (w, 1))

    val keyedStream = streamOperator.keyBy(_._1)

    val outputStreamOperator = keyedStream.sum(1)

    outputStreamOperator.print()

    env.execute()
  }
}
