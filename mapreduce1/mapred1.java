package mapreduce1;

import java.io.IOException;
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

public class mapred1 {
	public static class Map extends MapReduceBase implements Mapper<Text, MapWritable, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text docId, MapWritable doc, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			Writable title = doc.get(new Text("text_content"));
			StringTokenizer tokenizer = new StringTokenizer(title.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
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
		if (args.length != 1) {
			System.err.println("Usage: mapred1 outputpath");
			System.exit(0);
		}
		
		JobConf conf = new JobConf(mapred1.class);
		conf.setJobName("mapred1");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		 	
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.set("es.nodes", "10.149.3.3:9200");
		conf.setInputFormat(EsInputFormat.class);       
		conf.set("es.resource", "kb/doc");  
		conf.set("es.query", "{\"query\":{\"query_string\":{\"fields\":[\"article_dc_title\"],\"query\":\"IN HET ZUIDEN\"}}}");
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(conf, new Path(args[0]));
		 	
		JobClient.runJob(conf);
	}

}
