package datacloud.hbase.synthese
  
import org.apache.spark._ 
import org.apache.log4j._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.spark.rdd._
import datacloud.hbase.SparkHbase.MySparkContext

case class Client(idclient:String, nom:String, prenom:String)
case class Magasin(idmag:String, adresse:String)
case class Categorie(idcat:String, designation:String)
case class Produit(idProduit:String, designation:String, prix:String, categorie:String)
case class Vente(idvente:String, client:String, produit:String, magasin:String, date:String)

object HbaseVersCassandra extends App {

  val conf = new SparkConf().setAppName("CategorieCopie").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
  val sp = new SparkContext(conf)
  val mysparkcontext = new MySparkContext(sp)
  
  // Pour la table client
  //lecture dans hbase
  val scan_client = new Scan()
  scan_client.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idclient"))
  scan_client.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("nom"))
  scan_client.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("prenom"))
  //copie dans Cassandra
  val rddc1 = mysparkcontext.hbaseTableRDD("client", scan_client)
  val rddc2 = rddc1.map(x => Client (
      Bytes.toString(x._1.copyBytes()),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("nom"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("prenom")))))
  rddc2.saveAsCassandraTable("defaultcf", "client")

  // Pour la table magasin
  val scan_magasin = new Scan()
  scan_magasin.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idmag"))
  scan_magasin.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("adresse"))
  val rddm1 = mysparkcontext.hbaseTableRDD("magasin", scan_magasin)
  val rddm2 = rddm1.map(x => Magasin(
      Bytes.toString(x._1.copyBytes()),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("adresse")))))
  rddm2.saveAsCassandraTable("defaultcf", "magasin")

  // Pour la table categorie
  val scan_categorie = new Scan()
  scan_categorie.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idcat"))
  scan_categorie.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation"))
  val rddca1 = mysparkcontext.hbaseTableRDD("categorie", scan_categorie)
  val rddca2 = rddca1.map(x => Categorie(
      Bytes.toString(x._1.copyBytes()),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation")))))
  rddca2.saveAsCassandraTable("defaultcf", "categorie")


  // Pour la table produit
  val scan_produit = new Scan()
  scan_produit.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idprod"))
  scan_produit.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation"))
  scan_produit.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("prix"))
  scan_produit.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie"))
  val rddp1 = mysparkcontext.hbaseTableRDD("produit", scan_produit)
  val rddp2 = rddp1.map(x => Produit(
      Bytes.toString(x._1.copyBytes()),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("prix"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie")))))
  rddp2.saveAsCassandraTable("defaultcf", "produit")

  // Pour la table vente
  val scan_vente = new Scan()
  scan_vente.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idvente"))
  scan_vente.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("client"))
  scan_vente.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"))
  scan_vente.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("magasin"))
  scan_vente.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("date"))
  val rddv1 = mysparkcontext.hbaseTableRDD("vente", scan_vente)
  val rddv2 = rddv1.map(x => Vente(
      Bytes.toString(x._1.copyBytes()),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("client"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("magasin"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("date")))))
  rddv2.saveAsCassandraTable("defaultcf", "vente")

  
  
  
}