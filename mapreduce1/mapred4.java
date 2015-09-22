package mapreduce1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;

public class mapred4 {
	public static class Map extends MapReduceBase implements Mapper<Text, MapWritable, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		
		public void map(Text docId, MapWritable doc, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			WritableArrayWritable fields = (WritableArrayWritable) doc.get(new Text("text_content"));
			Writable content = fields.get()[0];
			StringTokenizer tokenizer = new StringTokenizer(content.toString());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				if (countMap.containsKey(word)) {
					countMap.put(word, countMap.get(word) + 1);
				} else {
					countMap.put(word, 1);
				}
				//word.set(tokenizer.nextToken());
				//output.collect(word, one);
			}
			
			/*for (Writable val : doc.values()) {
				word.set(val.toString());
				output.collect(word, one);
			}*/
			
			/*for (Writable key : doc.keySet()) {
				word.set(key.toString());
				output.collect(word, one);
			}*/
			
			/*Writable title = doc.get("article_dc_title");
			if (title != null) {
				log.info(title.toString());
				StringTokenizer tokenizer = new StringTokenizer(title.toString());
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					output.collect(word, one);
				}
			} else {
				log.info("article_dc_title not found, possible values:");
				for (Writable key : doc.keySet()) {
					log.info("KEY: | " + key.toString() + " |\n");
				}
			}*/
		}
				
		protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
			//Text word = new Text();
			//IntWritable value = new IntWritable();
			for (String key : countMap.keySet()) {
				//word.set(key);
				//value.set(countMap.get(key));
				output.collect(new Text(key), new IntWritable(countMap.get(key)));
			}
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}	
	
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: mapred4 outputpath nodeip jsonquery");
			System.exit(0);
		}

		JobConf conf = new JobConf(mapred4.class);
		conf.setJobName("mapred4");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		 	
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setCombinerClass(Reduce.class); //Using a combiner
		
		String ip = args[1]; //TODO check validity
		String query = args[2];
		
		conf.set("es.nodes", ip + ":9200");
		conf.setInputFormat(EsInputFormat.class);       
		conf.set("es.resource", "kb/doc");  
		conf.set("es.query", query);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(conf, new Path(args[0]));
		 	
		JobClient.runJob(conf);
	}

}
