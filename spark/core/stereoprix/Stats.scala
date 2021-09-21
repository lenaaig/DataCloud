package datacloud.spark.core.stereoprix
import org.apache.spark._ 
import org.apache.log4j._

object Stats extends App{
  
  
  def chiffreAffaire(url: String, annee: Int): Int ={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Stats").setMaster("local[*]")
	  val spark = new SparkContext(conf)
    val tf = spark.textFile(url, 1)
    
    val res = tf.map(_.split(" "))
                .filter(a=> (a(0).split("_")(2).toInt== annee))
                 .map(a=> (annee, a(2).toInt))
                 .reduceByKey(_+_)
                 .map(_._2).reduce(_+_)
    
	  /*val resmap= tf.map(_.split(" "))
    val resfilter = resmap.filter(_(0).split("_")(2).toInt==annee)
    val resmap2= resfilter.map(a=>(annee,a(2).toInt)).reduceByKey(_+_)
    val res = resmap2.first()._2*/
                 
    spark.stop() 
    return res
  }
  
  def chiffreAffaireParCategorie(urlIn: String, urlOut: String): Unit={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Stats").setMaster("local[*]")
	  val spark = new SparkContext(conf)
    val tf = spark.textFile(urlIn, 1)
    
    val res = tf.map(_.split(" "))
                .map(a=> (a(4), a(2).toInt))
                .reduceByKey(_+_)
                .map(a=> a._1+":"+a._2)
                
    /*
    val resmap = tf.map(_.split(" "))
    val res2 = resmap.map(a =>(a(4), a(2).toInt)).reduceByKey(_+_)
    val res=res2.map(a=>(a._1+":"+a._2))*/
    
    res.saveAsTextFile(urlOut)
    spark.stop()
  }
  
  def produitLePlusVenduParCategorie(urlIn: String, urlOut: String): Unit={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Stats").setMaster("local[*]")
	  val spark = new SparkContext(conf)
    val tf = spark.textFile(urlIn, 1)
    
    val res = tf.map(_.split(" "))
                .map(a=> ((a(4), a(3)), 1)) //categorie, produit, nb
                .reduceByKey(_+_)
                .sortBy(_._2, ascending = false)
                .map(a=> (a._1._1, a._1._2))
                .groupByKey()
                .map(a=> a._1+":"+a._2.head)
                    
    /*
    val resmap = tf.map(_.split(" "))
    val resmap2 = resmap.map(a=> ((a(4), a(3)),1)).groupByKey()
    val r= resmap2.map(a=> (a._1._1, (a._1._2, a._2.size))).sortBy(a=> a._2._2, ascending=false)
    val res = r.groupByKey().map(a=> a._1+":"+a._2.head._1)*/

    res.saveAsTextFile(urlOut)
    spark.stop()
  }
}