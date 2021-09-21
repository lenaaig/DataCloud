package datacloud.hbase.tp8;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex1_tablehbase {

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" , "localhost");
		Connection c;
		try {
			c = ConnectionFactory.createConnection(conf);

			Admin admin = c.getAdmin();

			TableName TABLE_NAME = TableName.valueOf("default"+":"+"ecoute");

			if (admin.tableExists(TABLE_NAME)) {
				admin.disableTable(TABLE_NAME);
				admin.deleteTable(TABLE_NAME);
			}

			TableDescriptorBuilder tbd = TableDescriptorBuilder.newBuilder(TABLE_NAME);
			ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1")).build();
			tbd.setColumnFamily(cf1);
			admin.createTable(tbd.build());

			/*
			 * The type HTableDescriptor is deprecated
			 * The type HColumnDescriptor is deprecated
			 * 
			HTableDescriptor desc = new  HTableDescriptor(TableName.valueOf(Bytes.toBytes("default"+":"+"ecoute")));
			desc.addFamily(new HColumnDescriptor("cf1"));

			admin.createTable(desc);*/


			Table table =  c.getTable(TABLE_NAME) ; //connexion à une table

			BufferedReader br = new BufferedReader(new FileReader("/Users/lenaig/Documents/M2S3/DataCloud/lastfm_fichier_1")); 	//lecture des lignes du fichier généré
			String line;
			while ((line = br.readLine()) != null) { 

				String[] ligne = line.split(" ");

				byte[] rowkey = Bytes.toBytes(ligne[0] + ligne[1]); // le rowkey est une concaténation du UserId et du TrackId
				Result res = table.get(new Get(rowkey));
				Put put = new Put(rowkey);

				Long LocalListening = Long.valueOf(ligne[2]);
				Long RadioListening = Long.valueOf(ligne[3]);
				Long Skip = Long.valueOf(ligne[4]);

				if (res.isEmpty()) { //on vérifie que la clé n'existe pas

					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("UserID"), Bytes.toBytes(ligne[0])); //ajout da la 1ere valeur de la ligne dans la cellule <rowkey,cf1:UserID>
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("TrackId"), Bytes.toBytes(ligne[1]));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("LocalListening"),LocalListening, Bytes.toBytes(LocalListening));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("RadioListening"),RadioListening, Bytes.toBytes(RadioListening));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Skip"),Skip, Bytes.toBytes(Skip));
					table.put(put); //envoie de la requete sur la table

				} else { //le cas où la clé existe déjà	
					Increment increment = new Increment(rowkey);
					increment.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("LocalListening"), LocalListening);
					increment.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("RadioListening"), RadioListening);
					increment.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Skip"), Skip);
					table.increment(increment);
				}

			}				


			br.close();
			c.close (); 

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//... // code client Hbase
		// fermeture connexion

	}

}

// Question 6 : Que se passerait-il si on lançait ce programme plusieurs fois en parallèle ?
//Si on lance en parallele, il peut y avoir des incohérences parce que put n'assure pas l'atomicité des lectures, on utilise incrémente qui assure l'atomicité et garantie la cohérence
