package datacloud.spark.core.matrix
import org.apache.spark.rdd.RDD
import org.apache.spark._ 
import org.apache.log4j._
import datacloud.scala.tpobject.vector.VectorInt


object MatrixIntAsRDD extends App{
  
  implicit def convert( v : RDD[VectorInt]) : MatrixIntAsRDD ={
    new MatrixIntAsRDD(v)
  }
  
  def makeFromFile(url : String, nbPartitions : Int, sc : SparkContext) : MatrixIntAsRDD ={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val rdd1 = sc.textFile(url, nbPartitions)
    val rdd2 = rdd1.map(_.split(" ").map(_.toInt))
    val rdd3 = rdd2.map(a=> new VectorInt(a))
    val rdd4= rdd3.zipWithIndex()
    val rdd5= rdd4.sortBy(_._2,ascending=true)
    val rdd6 = rdd5.map(_._1)
    
    convert(rdd6)
  }

}

class MatrixIntAsRDD(val lines: RDD[VectorInt] ) { 
	override def toString={
	  val sb = new StringBuilder()
		lines.collect().foreach(line=> sb.append(line+"\n"))
		sb.toString()
  }
	
	def nbLines(): Int={
	  lines.count().toInt
	}
	
	def nbColumns(): Int={
    lines.first().length()
	}
	
	def get(i:Int, j:Int):Int={
	  lines.zipWithIndex().filter(_._2 == i).map(_._1).first().get(j)
	}
	
	override def equals(a:Any):Boolean={
	  a match{
      case m : MatrixIntAsRDD => 
      if (m.nbColumns() != nbColumns() || m.nbLines() != nbColumns()) false
      var x = lines.zip(m.lines).filter(a=>a._1.equals(a._2))
      if(x.count() > 0) true else false
      case _ => false
     }
	  }
	
	def +(other: MatrixIntAsRDD): MatrixIntAsRDD={
	  lines.zip(other.lines).map(a=>(a._1+a._2)) 
	}
	
	def transpose():MatrixIntAsRDD ={
	 val fm= lines.zipWithIndex().flatMap(a =>( a._1.elements.zipWithIndex))
	 val mapp = fm.map(a=> (a._2, a._1))
	 val gb = mapp.groupByKey.sortBy(_._1, ascending=true).map(a=> new VectorInt(a._2.toArray)) 
   return new MatrixIntAsRDD(gb)
	}
	
	def *(other:MatrixIntAsRDD): MatrixIntAsRDD ={
	  val z = transpose().lines.zip(other.lines).map(x=>x._1.prodD(x._2))
	  val fm= z.flatMap(_.zipWithIndex)
	  val mapp= fm.map(a=> (a._2, a._1))
	  val sb= mapp.sortBy(_._1, ascending=true).reduceByKey(_+_).map(_._2)
	  return new MatrixIntAsRDD(sb)
	}
	 
	
}

