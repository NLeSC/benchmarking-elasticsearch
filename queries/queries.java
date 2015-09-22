package queries;

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.termvector.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorRequestBuilder;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders.*;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class queries {
	
	public String convertArticleType(String type) {
		if (type.equals("st_article")) {
			return "artikel";
		} else if (type.equals("st_advert")) {
			return "advertentie";
		} else if (type.equals("st_illust")) {
			return "illustratie met onderschrift";
		} else if (type.equals("st_family")) {
			return "familiebericht";
		} else {
			System.err.printf("Onbekend artikel type: %s\n", type);
			System.exit(1);
			return null;
		}
	}

	public String convertDistribution(String distribution) {
		if (distribution.equals("sd_national")) {
			return "Landelijk";
		} else if (distribution.equals("sd_regional")) {
			return "Regionaal/lokaal";
		} else if (distribution.equals("sd_antilles")) {
			return "Nederlandse Antillen";
		} else if (distribution.equals("sd_surinam")) {
			return "Suriname";
		} else if (distribution.equals("sd_indonesia")) {
			return "Nederlands-Indië / Indonesië";
		} else {
			System.err.printf("Onbekende distribution: %s\n", distribution);
			System.exit(1);
			return null;
		}
	}
	
	private class wordcloudThread extends Thread {
		private TransportClient client;
		private ArrayList<String> ids;
        TreeMap<String,WordData> words = new TreeMap<String,WordData>();
        private boolean terminate = false;
        private String ip;

        public wordcloudThread(String name, String ip) {
	        super(name);
	        this.ip = ip;
	    }
	    
	    public void run() {
			Settings settings = ImmutableSettings.settingsBuilder()
			        .put("client.transport.sniff", false)
			        .put("cluster.name", "texcavator_minicluster").build();
			client = new TransportClient(settings);
			client.addTransportAddress(new InetSocketTransportAddress(ip, 9300));
			System.out.printf("Thread started for %s\n", this.getName());
	    	while (true) {
		    	synchronized (this) {
			    	while (ids == null) {
			    		if (terminate) {
			    			client.close();
			    			return;
			    		}
			    		try {
							wait();
						} catch (InterruptedException e) {}
			    		if (terminate) {
			    			client.close();
			    			return;
			    		}
			    	}
			    	ArrayList<String> ids = this.ids;
		    		try {
		    			long start = System.currentTimeMillis();
						addToWordcloud(ids);
		    			/*for (String id : ids) {
		    				addSingle(id);
		    			}*/
		    			long end = System.currentTimeMillis();
						System.out.printf("%s finished a wordcloud gathering of %d documents in %dms\n", this.getName(), ids.size(), end - start);
					} catch (IOException e) {}
			    	this.ids = null;
		    	}
	    	}
	    }
	    
	    private void addToWordcloud(ArrayList<String> ids2) throws IOException {
			MultiTermVectorsRequestBuilder mtr = client.prepareMultiTermVectors();
			for (String id : ids2) {
				mtr.add(newTermVectorRequest().id(id));
			}
			MultiTermVectorsResponse r = mtr.execute().actionGet();

			TermsEnum e = null;
			DocsEnum docsenum = null;
			
			for (MultiTermVectorsItemResponse a : r) {
				TermVectorResponse t = a.getResponse();
				Fields fields = t.getFields();
				for (String f : fields) {
					Terms terms = fields.terms(f);
					TermsEnum it = terms.iterator(e);
					while (it.next() != null) {
						String term = it.term().utf8ToString();
						if (term.length() < 2) {continue;}
						DocsEnum docsit = it.docs(new Bits.MatchAllBits(ids2.size()), docsenum);
						int freq = docsit.freq();
						
			            WordData data = words.get(term);
			            if (data == null)
			                words.put(term, new WordData(term, freq));
			            else
			                data.count += freq;
					}
				}
			}
	    }
	    private void addSingle(String id) throws IOException {
	    	TermVectorRequestBuilder tr = client.prepareTermVector("kb", "doc", id)
	    							.setSelectedFields("text_content", "article_dc_title")
						            .setPositions(false)
						            .setOffsets(false)
						            .setPayloads(false)
						            .setFieldStatistics(false)
						            .setTermStatistics(false);
	    	TermVectorResponse t = tr.execute().actionGet();

			TermsEnum e = null;
			DocsEnum docsenum = null;
			
	    	Fields fields = t.getFields();
			for (String f : fields) {
				Terms terms = fields.terms(f);
				TermsEnum it = terms.iterator(e);
				while (it.next() != null) {
					String term = it.term().utf8ToString();
					if (term.length() < 2) {continue;}
					DocsEnum docsit = it.docs(new Bits.MatchAllBits(1), docsenum);
					int freq = docsit.freq();
					
		            WordData data = words.get(term);
		            if (data == null)
		                words.put(term, new WordData(term, freq));
		            else
		                data.count += freq;
				}
			}
	    	
	    }
	    
	    
	    public boolean isWorking() {
	    	return (ids != null);
	    }
	    
	    public void setData(ArrayList<String> ids) {
	    	synchronized (this) {
	    		this.ids = ids;
	    		notify();
	    	}
	    }

		public void terminate() {
			synchronized (this) {
				terminate = true;
				notify();
			}
		}
	    
	    
	}
	
	public void startThreaded(String query, String datefrom, String dateto, String[] articletypes, String[] distributions) {
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("client.transport.sniff", false)
		        .put("cluster.name", "texcavator_minicluster").build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress("10.149.3.26", 9300));

		QueryStringQueryBuilder sqsb = QueryBuilders.queryStringQuery(query);//new SimpleQueryStringBuilder(query);
		FilterBuilder rqb = FilterBuilders.rangeFilter("paper_dc_date").gte(datefrom).lte(dateto);
		BoolFilterBuilder mfb = FilterBuilders.boolFilter().must(rqb);

		ArrayList<String> articles = new ArrayList<String>();
		if (articletypes != null) {
			for (String type : articletypes) {
				articles.add(convertArticleType(type));
			}
		}
		ArrayList<String> dists = new ArrayList<String>();
		if (distributions != null) {
			for (String distribution : distributions) {
				dists.add(convertDistribution(distribution));
			}
		}
		OrFilterBuilder excludes = FilterBuilders.orFilter();
		if (articles.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("article_dc_subject", articles);
			excludes.add(tmb);
		}
		if (dists.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("paper_dcterms_spatial", dists);
			excludes.add(tmb);
		}
		mfb.mustNot(excludes);
		
		FilteredQueryBuilder fq = QueryBuilders.filteredQuery(sqsb, mfb);
		
		SearchResponse response = client.prepareSearch("kb")
		        .setTypes("doc").addField("_id")
		        .setScroll(new TimeValue(120000))
		        .setSearchType(SearchType.SCAN)
		        .setSize(60)
		        .setQuery(fq)
		        .execute()
		        .actionGet();

		System.out.printf("Total: %d\n", response.getHits().totalHits());
		
		if (response.getHits().totalHits() == 0 || response.getHits().totalHits() > 50000) {
			System.out.println("Too many or no results");
			client.close();
			return;
		}
		
		wordcloudThread[] threads = new wordcloudThread[3];
		threads[0] = new wordcloudThread("node332", "10.149.3.32");
		threads[1] = new wordcloudThread("node303", "10.149.3.3");
		threads[2] = new wordcloudThread("node326", "10.149.3.26");
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}

		response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();

		do {
			SearchHit[] h = response.getHits().hits();
			ArrayList<String> ids = new ArrayList<String>();
			for (int i = 0; i < h.length; i++) {
				SearchHit hit = h[i];
				ids.add(hit.getId());
			}
			boolean isAssigned = false;
			while (!isAssigned) {
				for (int i = 0; i < threads.length; i++) {
					if (!threads[i].isWorking()) {
						threads[i].setData(ids);
						//System.out.printf("Work assigned to %d\n", i);
						isAssigned = true;
						break;
					}
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
			}
			//System.out.println("Starting new scroll");
			response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();
			//System.out.println("Scroll finished");
		} while (response.getHits().hits().length > 0);
		
		client.close();
		for (int i = 0; i < threads.length; i++) {
			threads[i].terminate();
		}

	}

	public void startThreadedPerShard(String query, String datefrom, String dateto, String[] articletypes, String[] distributions) {
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("client.transport.sniff", false)
		        .put("cluster.name", "texcavator_minicluster").build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress("10.149.3.26", 9300));

		QueryStringQueryBuilder sqsb = QueryBuilders.queryStringQuery(query);//new SimpleQueryStringBuilder(query);
		FilterBuilder rqb = FilterBuilders.rangeFilter("paper_dc_date").gte(datefrom).lte(dateto);
		BoolFilterBuilder mfb = FilterBuilders.boolFilter().must(rqb);

		ArrayList<String> articles = new ArrayList<String>();
		if (articletypes != null) {
			for (String type : articletypes) {
				articles.add(convertArticleType(type));
			}
		}
		ArrayList<String> dists = new ArrayList<String>();
		if (distributions != null) {
			for (String distribution : distributions) {
				dists.add(convertDistribution(distribution));
			}
		}
		OrFilterBuilder excludes = FilterBuilders.orFilter();
		if (articles.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("article_dc_subject", articles);
			excludes.add(tmb);
		}
		if (dists.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("paper_dcterms_spatial", dists);
			excludes.add(tmb);
		}
		mfb.mustNot(excludes);
		
		FilteredQueryBuilder fq = QueryBuilders.filteredQuery(sqsb, mfb);
		
		SearchResponse response = client.prepareSearch("kb")
		        .setTypes("doc").addField("_id")
		        .setScroll(new TimeValue(120000))
		        .setSearchType(SearchType.SCAN)
		        .setSize(100)
		        .setQuery(fq)
		        .execute()
		        .actionGet();

		System.out.printf("Total: %d\n", response.getHits().totalHits());
		
		if (response.getHits().totalHits() == 0 || response.getHits().totalHits() > 10000) {
			System.out.println("Too many or no results");
			client.close();
			return;
		}
		
		wordcloudThread[] threads = new wordcloudThread[3];
		threads[0] = new wordcloudThread("node332", "10.149.3.32");
		threads[1] = new wordcloudThread("node303", "10.149.3.3");
		threads[2] = new wordcloudThread("node326", "10.149.3.26");
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}

		response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();

		//not hard code this:
		int nrShards = 16;
		ArrayList<ArrayList<String>> shardedIds = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < nrShards; i++) {
			shardedIds.add(new ArrayList<String>());
		}
		
		do {
			SearchHit[] h = response.getHits().hits();
			//ArrayList<String> ids = new ArrayList<String>();
			for (int i = 0; i < h.length; i++) {
				SearchHit hit = h[i];
				shardedIds.get(hit.getShard().getShardId()).add(hit.getId());
			}
			int currAssigned = 0;
			while (currAssigned < nrShards) {
				if (shardedIds.get(currAssigned).size() == 0) {
					currAssigned++;
					continue;
				}
				for (int i = 0; i < threads.length; i++) {
					if (!threads[i].isWorking()) {
						threads[i].setData(shardedIds.get(currAssigned));
						currAssigned++;
						break;
					}
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {}
			}
			//System.out.println("Starting new scroll");
			response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();
			//System.out.println("Scroll finished");
		} while (response.getHits().hits().length > 0);
		
		client.close();
		for (int i = 0; i < threads.length; i++) {
			threads[i].terminate();
		}

	}

	
	public void start(String query, String datefrom, String dateto, String[] articletypes, String[] distributions) throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("client.transport.sniff", true)
		        .put("cluster.name", "texcavator_minicluster").build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress("10.149.3.3", 9300));

		QueryStringQueryBuilder sqsb = QueryBuilders.queryStringQuery(query);//new SimpleQueryStringBuilder(query);
		FilterBuilder rqb = FilterBuilders.rangeFilter("paper_dc_date").gte(datefrom).lte(dateto);
		BoolFilterBuilder mfb = FilterBuilders.boolFilter().must(rqb);

		ArrayList<String> articles = new ArrayList<String>();
		if (articletypes != null) {
			for (String type : articletypes) {
				articles.add(convertArticleType(type));
			}
		}
		ArrayList<String> dists = new ArrayList<String>();
		if (distributions != null) {
			for (String distribution : distributions) {
				dists.add(convertDistribution(distribution));
			}
		}
		OrFilterBuilder excludes = FilterBuilders.orFilter();
		if (articles.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("article_dc_subject", articles);
			excludes.add(tmb);
		}
		if (dists.size() > 0) {
			TermsFilterBuilder tmb = FilterBuilders.termsFilter("paper_dcterms_spatial", dists);
			excludes.add(tmb);
		}
		mfb.mustNot(excludes);
		
		FilteredQueryBuilder fq = QueryBuilders.filteredQuery(sqsb, mfb);
		
		SearchResponse response = client.prepareSearch("kb")
		        .setTypes("doc").addField("_id")
		        .setScroll(new TimeValue(120000))
		        .setSearchType(SearchType.SCAN)
		        .setSize(60)
		        .setQuery(fq)
		        .execute()
		        .actionGet();

		System.out.printf("Total: %d\n", response.getHits().totalHits());
		
		if (response.getHits().totalHits() == 0 || response.getHits().totalHits() > 50000) {
			System.out.println("Too many or no results");
			client.close();
			return;
		}
		
		response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();
		long start = System.currentTimeMillis();
		int printed = 0;
        TreeMap<String,WordData> words = new TreeMap<String,WordData>();
        
		do {
			SearchHit[] h = response.getHits().hits();
			MultiTermVectorsRequestBuilder mtr = client.prepareMultiTermVectors();
			long mrqbstart = System.currentTimeMillis();
			for (int i = 0; i < h.length; i++) {
				SearchHit hit = h[i];
				mtr.add(newTermVectorRequest().id(hit.getId()));
				printed++;
				if (printed % 1000 == 0) {
					System.out.printf("Total: %d/%d\n", printed, response.getHits().totalHits());					
				}
			}
			long mrqbend = System.currentTimeMillis();
			System.out.printf("Building multitermvector request took: %dms\n", mrqbend - mrqbstart);	
			long mrqstart = System.currentTimeMillis();
			MultiTermVectorsResponse r = mtr.execute().actionGet();
			long mrqend = System.currentTimeMillis();
			System.out.printf("Multitermvector request took: %dms\n", mrqend - mrqstart);	
			TermsEnum e = null;
			DocsEnum docsenum = null;
			long zstart = System.currentTimeMillis();
			
			for (MultiTermVectorsItemResponse a : r) {
				TermVectorResponse t = a.getResponse();
				Fields fields = t.getFields();
				for (String f : fields) {
					Terms terms = fields.terms(f);
					TermsEnum it = terms.iterator(e);
					while (it.next() != null) {
						String term = it.term().utf8ToString();
						if (term.length() < 2) {continue;}
						DocsEnum docsit = it.docs(new Bits.MatchAllBits(response.getHits().hits().length), docsenum);
						int freq = docsit.freq();
						
			            WordData data = words.get(term);
			            if (data == null)
			                words.put( term, new WordData(term, freq) );
			            else
			                data.count += freq;
					}
				}
			}
			long zend = System.currentTimeMillis();
			System.out.printf("Loop terms took: %dms\n", zend - zstart);	
			System.out.printf("Treemap contains %d words\n", words.size());	
			long rqstart = System.currentTimeMillis();
			response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(120000)).execute().actionGet();
			long rqend = System.currentTimeMillis();
			System.out.printf("Scroll request took: %dms\n", rqend - rqstart);			
		} while (response.getHits().hits().length > 0);
		
		long end = System.currentTimeMillis();

		System.out.printf("Total took: %dms\n", end - start);
		
		long sstart = System.currentTimeMillis();
		ArrayList<WordData> wordsByFrequency = new ArrayList<WordData>( words.values() );
        Collections.sort( wordsByFrequency, new CountCompare() );

        /*System.out.println("Top 20");
        int counter = 0;
        for (WordData data : wordsByFrequency) {
        	System.out.printf("%s %d\n", data.word, data.count);
        	counter++;
        	if (counter == 20) {
        		break;
        	}
        }*/
		long send = System.currentTimeMillis();
		
		System.out.printf("Sorting took: %dms\n", send - sstart);
		
		client.close();
	}
	
    /**
     * Represents the data we need about a word:  the word and
     * the number of times it has been encountered.
     */
    private static class WordData { 
        String word;
        int count;
        WordData(String w, int count) {
                // Constructor for creating a WordData object when
                // we encounter a new word.
            word = w;
            this.count = count;  // The initial value of count is 1.
        }
    } // end class WordData
    
    /**
     * A comparator for comparing objects of type WordData according to 
     * their counts.  This is used for sorting the list of words by frequency.
     */
    private static class CountCompare implements Comparator<WordData> {
        public int compare(WordData data1, WordData data2) {
            return data2.count - data1.count;
                // The return value is positive if data2.count > data1.count.
                // I.E., data1 comes after data2 in the ordering if there
                // were more occurrences of data2.word than of data1.word.
                // The words are sorted according to decreasing counts.
        }
    } // end class CountCompare
    
	
    private TermVectorRequest newTermVectorRequest() {
        return new TermVectorRequest()
                .positions(false)
                .offsets(false)
                .payloads(false)
                .fieldStatistics(false)
                .termStatistics(false)
                .index("kb")
                .type("doc")
                .selectedFields("text_content", "article_dc_title");
    }
	
	public static void main(String[] args) throws IOException {
		if (args.length == 6) {
			String fromdate = args[1];
			String todate = args[2];
			String[] articletypes = (args[3].length() > 0) ? args[3].split("\\+") : null;
			String[] distributions = (args[4].length() > 0) ? args[4].split("\\+") : null;
			String query = args[5];
			new queries().startThreaded(query, fromdate, todate, articletypes, distributions);
		} else {
			System.out.println("Usage: fromdate todate articletypes distributions query");
			System.exit(2);			
		}
		
		/*System.out.printf("Daterange: %s to %s\n", fromdate, todate);
		System.out.printf("Exclude articletypes: ");
		for(String t : articletypes) {System.out.printf("%s, ", t);}
		System.out.printf("\nDistributions: ");
		for(String d : distributions) {System.out.printf("%s, ", d);}
		System.out.printf("\nQuery: %s\n\n", query);*/
		
	}

}
