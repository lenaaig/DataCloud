package datacloud.scala.tpfonctionnel

object FunctionParty {
  def curryfie[A,B,C](f: (A, B) =>C) : A =>B =>C ={
    a:A=>b:B=> f(a,b)
  }
  
  def decurryfie[A,B,C](f: A =>B =>C):(A, B) =>C ={
    (a:A, b:B) => f(a)(b)
  }
  
  def compose[A,B,C](f: B =>C, g: A =>B): A =>C ={
    a:A => f(g(a))
  }
  
  def axplusb(a:Int, b:Int):Int => Int ={
    def plus(a:Int, b:Int): Int={
      a+b
    }
    def mult(a:Int, b:Int): Int={
      a*b
    }
    
    var ax = curryfie(mult)
    var axplusb = curryfie(plus)
    compose(axplusb(b),ax(a))
  }
}