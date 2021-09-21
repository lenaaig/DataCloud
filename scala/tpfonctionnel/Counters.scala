package datacloud.scala.tpfonctionnel

object Counters{
  def nbLetters(l : List[String]) : Int={
    var resf = l.flatMap(a=>a.split(" "))
    var resm = resf.map(b=> b.length())
    var resr = resm.reduce((x,y) => x+y)
    return resr
  }
}