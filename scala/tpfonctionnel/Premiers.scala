package datacloud.scala.tpfonctionnel

object Premiers{
  
  def est_premier(x:Int, n:Int): Boolean={
    for (i <- 2 until n){
      if (x%i == 0 && x!=i)
        return false
    }
    return true
  }
  
  def premiers(n: Int): List[Int]={
    var l = List.range(2, n)
    l.filter(est_premier(_, n))
  }
  
  def premiersWithRec(n:Int): List[Int]={
    var l = List.range(2,n)
	  def f(l:List[Int]): List[Int]={
			  if (l(0)*l(0) > l(l.size-1)){
				  return l
			  }else {
				  var res=l(0)
					res+: f(l.drop(1).filter(_ %res!=0))
			  }
	  }
	  return f(l)
  }

}