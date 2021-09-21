package datacloud.scala.tpfonctionnel

object Statistics {
  def average(n:List[(Double, Int)]) : Double={
    var res= n.reduce((a,b)=> (a._1+b._1*b._2, a._2+b._2))
    res._1/res._2
  }
}