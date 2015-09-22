package mapreduce1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class mapred3 {
	public static class WordCountMapper extends MapReduceBase implements Mapper<Text, MapWritable, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text docId, MapWritable doc, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			WritableArrayWritable fields = (WritableArrayWritable) doc.get(new Text("text_content"));
			Writable title = fields.get()[0];
			String cleanLine = title.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			StringTokenizer tokenizer = new StringTokenizer(cleanLine);
			while (tokenizer.hasMoreTokens()) {
				String w = tokenizer.nextToken();
				if (w.length() < 2) {
					continue;
				}
				word.set(w);
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
	
	public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private Map<Text, IntWritable> countMap = new HashMap<>();
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			countMap.put(new Text(key), new IntWritable(sum));
			//output.collect(key, new IntWritable(sum));
		}
		
        protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 100) {
                    break;
                }
                output.collect(key, sortedMap.get(key));
            }
        }
		
	}	
	
	public static class WordCountCombiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}	
	
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: mapred3 outputpath nodeip jsonquery");
			System.exit(0);
		}

		JobConf conf = new JobConf(mapred3.class);
		conf.setJobName("mapred3");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		 	
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);
		conf.setCombinerClass(WordCountCombiner.class); //Using a combiner
		
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
