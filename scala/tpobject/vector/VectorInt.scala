package datacloud.scala.tpobject.vector

class VectorInt (var elements : Array[Int]) extends Serializable {
  
  def length(): Int ={
    return this.elements.length
  }
  
  def get(i:Int):Int={
    return this.elements(i)
  }
  
  def tostring():String ={
    var s=""
    for(i <- this.elements){
      s+=i +" "
    }
    return s
  }
  
  override def equals(a:Any):Boolean={
    a match{
      case c : VectorInt => this.elements.sameElements(c.elements)
    }
  }
  
  def +(other:VectorInt):VectorInt={
    var res : Array[Int] = Array[Int]()
    for (i <- 0 to this.elements.length-1) {
      res=res:+ this.elements(i) + other.elements(i)
    }
    return new VectorInt(res)
  }
  
  def *(v:Int):VectorInt={
    var res : Array[Int] = Array[Int]()
    for (i <- 0 to this.elements.length-1){
      res=res :+ this.get(i) * v
    }
    return new VectorInt(res)
  }
  
  def prodD(other:VectorInt):Array[VectorInt]={
    var res : Array[VectorInt] = Array[VectorInt]()
    for (i <- 0 to this.elements.length-1){
      res=res:+( other.*(this.elements(i)))
    }
    return res
  }
}

object VectorInt {
  implicit def convert(i:Array[Int])=new VectorInt(i)
}