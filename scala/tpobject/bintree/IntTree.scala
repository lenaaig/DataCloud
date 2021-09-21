package datacloud.scala.tpobject.bintree

sealed abstract class IntTree
case object EmptyIntTree extends IntTree
case class NodeInt(elem : Int, left : IntTree, right : IntTree) extends IntTree

object BinTrees extends App{
  def contains(it:IntTree, n:Int): Boolean ={
    it match { 
      case EmptyIntTree => false
      case NodeInt(p, l, r) => if (p==n) return true
      else return contains(l, n) || contains(r, n)
    }
    
  }
  def size(it:IntTree): Int={
    it match { 
      case EmptyIntTree => 0
      case NodeInt(x,l,r) => 1+size(l) + size(r)
    }  
  }
  def insert(it:IntTree, n:Int):IntTree={
    it match {
      case EmptyIntTree => return NodeInt(n, EmptyIntTree, EmptyIntTree)
      case NodeInt(x,l,r) => if (size(l)<size(r)) return NodeInt(x,insert(l,n),r)
      else return NodeInt(x,l,insert(r,n))
    }
  }
  
   
	def contains[A](it:Tree[A], n:A): Boolean ={
			it match { 
			case EmptyTree => false
			case Node(p, l, r) => if (p==n) return true
					else return contains(l, n) || contains(r, n)
			}

	}
	def size[A](it:Tree[A]): Int={
			it match { 
			case EmptyTree => 0
			case Node(x,l,r) => 1+ size(l) + size(r)
			}  
	}
	def insert[A](it:Tree[A], n:A):Tree[A]={
			it match {
			case EmptyTree => return Node(n, EmptyTree, EmptyTree)
			case Node(x,l,r) => if (size(l)<size(r)) return Node(x,insert(l,n),r)
      else return Node(x,l,insert(r,n))
			}
	}
  
}