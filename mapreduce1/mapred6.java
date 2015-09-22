package mapreduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.apache.hadoop.conf.Configuration;

public class mapred6 {

    /**
     * The mapper reads one line at the time, splits it into an array of single words and emits every
     * word to the reducers with the value of 1.
     */
	public static class WordCountMapper extends Mapper<Text, MapWritable, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(Text docId, MapWritable doc, Context context) throws IOException, InterruptedException {
			WritableArrayWritable fields = (WritableArrayWritable) doc.get(new Text("text_content"));
			Writable text = fields.get()[0];
			String cleanLine = text.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			StringTokenizer tokenizer = new StringTokenizer(cleanLine);
			int size1 = tokenizer.countTokens();
			fields = (WritableArrayWritable) doc.get(new Text("article_dc_title"));
			text = fields.get()[0];
			cleanLine = text.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			tokenizer = new StringTokenizer(cleanLine);
			int size2 = tokenizer.countTokens();			
			context.write(new Text("nrWords"), new LongWritable(size1 + size2));
			context.write(new Text("nrDocs"), one);
		}
	}

    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            
            context.write(new Text(key), new LongWritable(sum));
        }
    }
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3) {
			System.err.println("Usage: mapred3 outputpath nodeip jsonquery");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("mapred5 - " + args[0]);
		
		job.setJarByClass(mapred6.class);;
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		 	
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		String ip = args[1]; //TODO check validity
		String query = args[2];
		
		Configuration c = job.getConfiguration();
		c.set("es.nodes", ip + ":9200");
		job.setInputFormatClass(EsInputFormat.class);
		c.set("es.resource", "kb/doc");  
		c.set("es.query", query);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//JobClient.runJob(conf);
	}
    
}
