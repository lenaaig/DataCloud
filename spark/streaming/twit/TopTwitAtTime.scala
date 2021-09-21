package datacloud.spark.streaming.twit

import org.apache.spark._ 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


import org.apache.spark.streaming._

object TopTwitAtTime extends App {
  val conf = new SparkConf().setAppName("TopTwitAtTime").setMaster("local[*]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))
  val lines = ssc.socketTextStream("localhost", 4242)  
  

  val rdd1 = lines.flatMap(_.split(" ")).filter(_.contains("#")).map((_, 1))
  val topAtTime = rdd1.reduceByKey(_+_).transform(_.sortBy(_._2, false))

  val topWindow = rdd1.reduceByKeyAndWindow(_+_, _-_, Seconds(30), Seconds(3)).transform(_.sortBy(_._2, false))

  topAtTime.print(10)
  topWindow.print(10)
  ssc.start()
  ssc.awaitTermination()
}
