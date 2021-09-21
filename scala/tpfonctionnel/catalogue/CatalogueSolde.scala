package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue._

trait CatalogueSolde extends Catalogue {
  def solde(pourcentage : Int) : Unit
}

class CatalogueSoldeWithFor extends CatalogueWithNonMutable with CatalogueSolde{

  def solde(pourcentage : Int) : Unit={
    var output= Map[String,Double]()
    for ((prod,prix) <- this.c){
      output = output + (prod ->prix*pourcentage/100)
    }
    this.c = output
  }
}

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde {
  
  def diminution(a:Double, percent:Int):Double = a* ((100.0 - percent) / 100.0)
  
  def solde(pourcentage: Int): Unit={
    c = c.mapValues(diminution(_, pourcentage));
    //c = c.map(p =>diminution(p._2, pourcentage)); mais rajouter le string
  }
}

class CatalogueSoldeWithAnoFunction extends CatalogueWithNonMutable with CatalogueSolde {
  
    def solde(pourcentage: Int): Unit={
        c = c.mapValues(_* ((100.0 - pourcentage) / 100.0));
    }
}