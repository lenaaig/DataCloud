package datacloud.spark.streaming.pi

import org.apache.spark._ 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


import org.apache.spark.streaming._

object PiAtTime extends App{
  val conf = new SparkConf().setAppName("PiAtTime").setMaster("local[*]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(5))
  val lines = ssc.socketTextStream("localhost", 4242)
  val rdd1 = lines.map(x => x.split(" "))
  val rdd2 = rdd1.map(x => (x(0).toDouble,x(1).toDouble))
  val rdd3 = rdd2.map(x => if(x._1*x._1 + x._2*x._2 < 1) (1.0,0.0) else (0.0,1.0))
 
  val rdd4 = rdd3.reduce((c1,c2) => (c1._1+c2._1,c1._2+c2._2))
  val rdd5 = rdd4.map(x => 4*x._1/(x._1+x._2))
  
  rdd5.print()
  ssc.start()
  ssc.awaitTermination()
}