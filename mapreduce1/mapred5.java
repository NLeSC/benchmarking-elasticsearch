package mapreduce1;

import java.io.IOException;
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

public class mapred5 {

    /**
     * The mapper reads one line at the time, splits it into an array of single words and emits every
     * word to the reducers with the value of 1.
     */
	public static class WordCountMapper extends Mapper<Text, MapWritable, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text docId, MapWritable doc, Context context) throws IOException, InterruptedException {
			WritableArrayWritable fields = (WritableArrayWritable) doc.get(new Text("text_content"));
			Writable text = fields.get()[0];
			String cleanLine = text.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			StringTokenizer tokenizer = new StringTokenizer(cleanLine);
			while (tokenizer.hasMoreTokens()) {
				String w = tokenizer.nextToken();
				if (w.length() < 2) {
					continue;
				}
				word.set(w);
				context.write(word, one);
			}
			fields = (WritableArrayWritable) doc.get(new Text("article_dc_title"));
			text = fields.get()[0];
			cleanLine = text.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
			tokenizer = new StringTokenizer(cleanLine);
			while (tokenizer.hasMoreTokens()) {
				String w = tokenizer.nextToken();
				if (w.length() < 2) {
					continue;
				}
				word.set(w);
				context.write(word, one);
			}
		}
	}

    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            countMap.put(new Text(key), new IntWritable(sum));
            
            if (countMap.size() > 20000) {
            	// If we reach the memory limit, trim the map

                Map<Text, IntWritable> sortedMap = sortByValues(countMap);

                countMap.clear();
                
                int counter = 0;
                for (Text key2 : sortedMap.keySet()) {
                    if (counter++ == 100) {
                        break;
                    }
                    
                    countMap.put(key2, sortedMap.get(key2));
                }
                
            }
            
            //context.write(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 100) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    /**
     * The combiner retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
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
    
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3) {
			System.err.println("Usage: mapred3 outputpath nodeip jsonquery");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("mapred5 - " + args[0]);
		
		job.setJarByClass(mapred5.class);;
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 	
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountCombiner.class); //Using a combiner
		
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
