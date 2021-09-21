package datacloud.spark.core.lastfm
import org.apache.spark._ 
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object HitParade {
  case class TrackId(id:String)
  case class UserId(id:String)
  
  def loadAndMergeDuplicates(sc : SparkContext , url: String) : RDD[((UserId,TrackId),(Int,Int,Int))] ={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val tf = sc.textFile(url, 1)
    
    val res = tf.map(_.split(" "))
                .map(a=> ((UserId(a(0)), TrackId(a(1))), (a(2).toInt, a(3).toInt, a(4).toInt)))
                .reduceByKey((a,b)=> (a._1+b._1, a._2+b._2, a._3+b._3))
    
    /*
    val res = tf.map(a=> a.split(" ")).map(a => ((UserId(a(0)), TrackId(a(1))), (a(2).toInt, a(3).toInt, a(4).toInt)))
    val ret = res.reduceByKey((a,b)=>(a._1 +b._1 , a._2 +b._2, a._3 +b._3))
    */
    return res
    
  }
  
   def hitparade(rdd:RDD[((UserId,TrackId),(Int,Int,Int))]) : RDD[TrackId] ={
     Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  
     val res = rdd.map(a=> (a._1._2, (if (a._2._1 + a._2._2 >0) 1 else 0, a._2._1 + a._2._2 - a._2._3)))
                .reduceByKey((a,b)=> (a._1+b._1 , a._2+b._2))
                .sortBy(a=> (-a._2._1, -a._2._2, a._1.id))
                .map(_._1)
     
     /*
    val d = rdd.map(x=> (x._1._2 , (if (x._2._1+x._2._2 ==0) 0 else 1, x._2._1+x._2._2-x._2._3 ))).reduceByKey((a,b) => (a._1+b._1 ,a._2+b._2))
     val r = d.sortBy(x=> (-x._2._1, -x._2._2, x._1.id )).map(a=>a._1)
		*/
     return res
   } 
  
}