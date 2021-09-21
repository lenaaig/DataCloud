package datacloud.scala.tpobject.catalogue

class CatalogueWithNonMutable extends Catalogue {

	var c : scala.collection.immutable.Map[String, Double] = Map[String,Double]()

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