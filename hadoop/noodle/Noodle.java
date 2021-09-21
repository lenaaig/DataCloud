package datacloud.hadoop.noodle;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Noodle {
	public static class NoodleMapper extends Mapper<LongWritable, Text, Text, Text>{

		private Text tranche = new Text();

		public String extract_something(String line, int n) {
			String[] split = line.split("\\s");
			System.err.println("split"+split.length);
			String[] split_something = split[0].split("_");
			System.err.println("mois"+split_something.length);
			return split_something[n]; // 1 : mois | 3 : heure | 4 : minute
		}

		public Text extract_requete(String line) {
			String[] split = line.split("\\s");
			Text req  = new Text();
			req.set(split[2]);
			return req;
		}

	

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String mois =extract_something(line, 1);
			String heure =extract_something(line, 3);
			String minute =extract_something(line, 4);

			String tranche_mois = mois + "ème mois de " + heure +":";
			if (Integer.valueOf(minute)<30) tranche_mois +="00 à "+heure+":29";
			else tranche_mois +="00 à "+heure+":29";
			Text tr = new Text();
			tr.set(tranche_mois);
			tranche.set(tr);
						
			value.set(extract_requete(line));
			context.write(tranche, value);
		}
	}

	public static class NoodleReducer extends Reducer<Text,Text,Text,IntWritable> {

		private Text mc = new Text();
		private IntWritable value = new IntWritable();

		public void reduce(Text key, Iterable<Text> requests, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> hm = new HashMap<>();
			String res = "";
			int nbReq = 0;
			int cpt = 0;
			for (Text req : requests) {
				nbReq++;
				String[] split = req.toString().split("\\+"); 
				if(res.equals(""))
					res = split[0];
				for(String word : split) {
					if (hm.containsKey(word)) {
						cpt = hm.get(word);
						cpt++;
						hm.put(word,cpt);
						if (hm.get(word) > hm.get(res)) 
							res = word;
					} else 
						hm.put(word, 1);
				}
			}
			mc.set(key+" "+res);
			value.set(nbReq);
			context.write(mc, value);
		}
	}

	public static class NoodlePartitioner extends Partitioner<Text,Text>{

		public int getPartition(Text key, Text mc, int nbReduce) {
			String[] tmp = key.toString().split("-");
			int mois = Integer.parseInt(tmp[0]);
			return mois-1;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", true);
		conf.setBoolean("mapreduce.reduce.speculative", true);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: noodle <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "noodle");
		job.setJarByClass(Noodle.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		job.setMapperClass(NoodleMapper.class); // indique la classe du Mapper
		job.setReducerClass(NoodleReducer.class); // indique la classe du Reducer
		job.setMapOutputKeyClass(Text.class);// indique la classe  de la clé sortie map
		job.setMapOutputValueClass(Text.class);// indique la classe  de la valeur sortie map    
		job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
		job.setOutputValueClass(IntWritable.class);// indique la classe  de la clé de sortie reduce
		job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		job.setPartitionerClass(NoodlePartitioner.class);// indique la classe du partitionneur
		job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)


		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
		final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}
	
	
}

