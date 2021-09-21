package datacloud.scala.tpfonctionnel

object MySorting {
  def isSorted[A](l: Array[A], mafonction : (A,A)=> Boolean): Boolean={
    for (i <- 0 until l.size-1){
      if (!mafonction(l(i), l(i+1))) return false
    }
    return true
  }
  
  def ascending2[T:Ordering](a: T, b:T): Boolean={
    implicitly[Ordering[T]].compare(a,b)<=0
  }

  def descending2[T:Ordering](a: T, b:T): Boolean={
    implicitly[Ordering[T]].compare(b,a)<=0
  }
  
  //Correction du prof mais au dessus fonctionne quand même
  def ascending[T](a: T, b:T)(implicit o:Ordering[T]):Boolean={
    o.compare(a, b)<=0
  }
  
  def descending[T](a: T, b:T)(implicit o:Ordering[T]):Boolean={
    o.compare(b, a)<=0
  }
}