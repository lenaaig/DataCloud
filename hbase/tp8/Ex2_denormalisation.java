package datacloud.hbase.tp8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex2_denormalisation {
	//fusion de tables
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" , "localhost");
		Connection c;
		long startTime = System.nanoTime();



		try {
			c = ConnectionFactory.createConnection(conf);

			Admin admin = c.getAdmin();

			TableName tn_vente = TableName.valueOf("vente");
			TableName tn_produit = TableName.valueOf("produit");
			TableName tn_categorie = TableName.valueOf("categorie");

			Table table_vente = c.getTable(tn_vente);
			Table table_produit = c.getTable(tn_produit);
			Table table_categorie = c.getTable(tn_categorie);


			Map<String, Integer> nbVenteParCategorie = new HashMap<>();


			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"));
			ResultScanner results_vente = table_vente.getScanner(scan);

			for(Result r : results_vente) { //chaque ligne de la table vente
				String produit = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"))); //on récupère une ligne de vente

				Get get = new Get(Bytes.toBytes(produit));
				Result res = table_produit.get(get); //jointure entre produit et vente sur "produit" et le rowkey

				String categorie = new String(res.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie"))); //on récupère dans la table produit, la catégorie

				Get get2 = new Get(Bytes.toBytes(categorie));
				Result res2 = table_categorie.get(get2); //jointure sur produit et catégorie sur la colognne "categorie" et le rowkey 

				Put put = new Put(r.getRow());

				if (!res.isEmpty() && !res2.isEmpty()) { 
					//on rajoute sur la table vente, une colonne catégorie contenant la valeur de la colonne "désignation" de la table catégorie
					put.addColumn(Bytes.toBytes("defaultcf"),Bytes.toBytes("categorie"), res2.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation")));
					table_vente.put(put); //envoie de la requete sur la table vente
				}
			}
			
			for(Result r : results_vente) {
				String cle = Bytes.toString(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie")));
				if(!nbVenteParCategorie.containsKey(cle)) {
					nbVenteParCategorie.put(cle, 1);
				} else {
					nbVenteParCategorie.replace(cle, nbVenteParCategorie.get(cle) + 1);
				}
			}
			

			for (Entry<String, Integer> m : nbVenteParCategorie.entrySet()) {
				System.out.println("categorie" + m.getKey() + " : " + m.getValue() + " produits vendus");
			}

			c.close (); 
			long endTime   = System.nanoTime();
			long totalTime = endTime - startTime;
			System.out.println("Temps d'execution : " + totalTime + " millieseconds");

			// QUESTION 5 : pour que le schéma reste cohérent il faut toucher à toutes les tables qui sont utilisées
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}

}