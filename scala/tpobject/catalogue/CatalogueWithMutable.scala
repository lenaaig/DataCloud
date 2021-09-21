package datacloud.scala.tpobject.catalogue;

trait Catalogue{
	def getPrice(produit: String): Double
	def removeProduct(produit: String): Unit
	def selectProducts(min: Double, max: Double): Iterable[String]
	def storeProduct(produit:String, prix:Double): Unit
}

class CatalogueWithMutable extends Catalogue {

	var c : Map[String, Double] = Map[String,Double]()

			def getPrice(produit:String): Double={
					var p = c.get(produit)
							p match {case None => -1.0
							case Some(x) => x}
}

def removeProduct(produit: String){
	c=c - produit
}

def selectProducts(min:Double, max:Double): Iterable[String]={  
		var output= List[String]()
				/*c.foreach ( (prod) =>{
					if (c.getOrSome(prod) > min && c.getOrSome(prod) < max){
						output += prod
					}
				}*/
				for ((prod,prix) <- c){
					if ( prix >= min && prix <= max )
						output =output :+prod
					}
		    return output
}

def storeProduct(produit:String, prix:Double){
	c+=(produit -> prix)
}

}