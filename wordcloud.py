# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from collections import Counter
import time
import sqlite3
from random import shuffle
import sys

_ES_RETURN_FIELDS = ('article_dc_title',
                     'paper_dcterms_temporal',
                     'paper_dcterms_spatial',
                     'paper_dc_title',
                     'paper_dc_date')

_KB_DISTRIBUTION_VALUES = {'sd_national': 'Landelijk',
                           'sd_regional': 'Regionaal/lokaal',
                           'sd_antilles': 'Nederlandse Antillen',
                           'sd_surinam': 'Suriname',
                           'sd_indonesia': 'Nederlands-Indië / Indonesië'}

_KB_ARTICLE_TYPE_VALUES = {'st_article': 'artikel',
                           'st_advert': 'advertentie',
                           'st_illust': 'illustratie met onderschrift',
                           'st_family': 'familiebericht'}

_DOCUMENT_TEXT_FIELD = 'text_content'
_DOCUMENT_TITLE_FIELD = 'article_dc_title'
_AGG_FIELD = _DOCUMENT_TEXT_FIELD

ES_INDEX = 'kb'
ES_DOCTYPE = 'doc'

DBFILE = 'queries.db'
DBRESULTSFILE = 'wordclouds_10k_2.db'

def _es():
    """Returns ElasticSearch instance."""
    return Elasticsearch('10.149.3.3:9200', timeout=120)

def tv_cloud(params, id):
    min_length = 2
    stopwords = []
    return generate_tv_cloud(params, 2, [], id)

def generate_tv_cloud(search_params, min_length, stopwords, id):
    """Generates multiple document word clouds using the termvector approach"""
    burst = False
    chunk_size = 1000
    progress = 0
    wordcloud_counter = Counter()

    result = count_search_results(ES_INDEX,
                                  ES_DOCTYPE,
                                  search_params['query'],
                                  search_params['dates'],
                                  search_params['distributions'],
                                  search_params['article_types'])
    doc_count = result.get('count')

    print 'Document count: ', doc_count

    if doc_count < 10000:
      return False

    if doc_count > 50000:
      return False

    if doc_count <= 0:
      return False

    for subset in document_id_chunks_scroll(chunk_size,
                                    ES_INDEX,
                                    ES_DOCTYPE,
                                    search_params['query'],
                                    search_params['dates'],
                                    search_params['distributions'],
                                    search_params['article_types']):

        if (len(subset) == 0):
            continue
        result = termvector_wordcloud(ES_INDEX,
                                      ES_DOCTYPE,
                                      subset,
                                      min_length)
        wordcloud_counter = wordcloud_counter + result
        progress = progress + len(subset)
        print progress, '/', doc_count, '@', id

    return 1 #counter2wordclouddata(wordcloud_counter, burst, stopwords)

def counter2wordclouddata(wordcloud_counter, burst, stopwords=None):
    """Transform a counter into the data required to draw a word cloud.
    """
    if not stopwords:
        stopwords = []
    for stopw in stopwords:
        del wordcloud_counter[stopw]

    result = []
    for term, count in wordcloud_counter.most_common(100):
        result.append({
            'term': term,
            'count': count
        })

    return {
        'max_count': wordcloud_counter.most_common(1)[0][1],
        'result': result,
        'status': 'ok',
        'burstcloud': burst
    }

def count_search_results(idx, typ, query, date_range, exclude_distributions,
                         exclude_article_types):
    """Count the number of results for a query
    """
    q = create_query(query, date_range, exclude_distributions,
                     exclude_article_types)

    #print q

    return _es().count(index=idx, doc_type=typ, body=q)

def termvector_wordcloud(idx, typ, doc_ids, min_length=0):
    """Return word frequencies in a set of documents.

    Return data required to draw a word cloud for multiple documents by
    'manually' merging termvectors.

    The counter returned by this method can be transformed into the input
    expected by the interface by passing it to the counter2wordclouddata
    method.

    See also
        :func:`single_document_word_cloud` generate data for a single document
        word cloud

        :func:`multiple_document_word_cloud` generate word cloud data using
        terms aggregation approach
    """
    wordcloud = Counter()

    bdy = {
        'ids': doc_ids,
        'parameters': {
            'fields': [_DOCUMENT_TEXT_FIELD, _DOCUMENT_TITLE_FIELD],
            'term_statistics': False,
            'field_statistics': False,
            'offsets': False,
            'payloads': False,
            'positions': False
        }
    }

    t_vectors = _es().mtermvectors(index='kb', doc_type='doc', body=bdy)

    for doc in t_vectors.get('docs'):
        for field, data in doc.get('term_vectors').iteritems():
            temp = {}
            for term, details in data.get('terms').iteritems():
                if len(term) >= min_length:
                    temp[term] = int(details['term_freq'])
            wordcloud.update(temp)

    return wordcloud

def document_id_chunks(chunk_size, idx, typ, query, date_range, dist=[],
                       art_types=[]):
    """Generator for retrieving document ids for all results of a query.

    Used by the generate_tv_cloud task.
    """
    q = create_query(query, date_range, dist, art_types)

    get_more_docs = True
    start = 0
    fields = []

    while get_more_docs:
        results = _es().search(index=idx, doc_type=typ, body=q, from_=start,
                               fields=fields, size=chunk_size)
        yield [result['_id'] for result in results['hits']['hits']]

        start = start + chunk_size

        if len(results['hits']['hits']) < chunk_size:
            get_more_docs = False

def document_id_chunks_scroll(chunk_size, idx, typ, query, date_range, dist=[],
                       art_types=[]):
    """Generator for retrieving document ids for all results of a query.

    Used by the generate_tv_cloud task.
    """
    q = create_query2(query, date_range, dist, art_types)

    get_more_docs = True
    start = 0
    fields = []

    results = _es().search(index=idx, doc_type=typ, body=q, search_type="scan", scroll="2m", size="30", fields=fields)
    while get_more_docs:
        yield [result['_id'] for result in results['hits']['hits']]
        
        results = _es().scroll(scroll_id=results['_scroll_id'], scroll="2m");

        if len(results['hits']['hits']) == 0:
            get_more_docs = False

def create_query(query_str, date_range, exclude_distributions,
                 exclude_article_types):
    """Create elasticsearch query from input string.

    This method accepts boolean queries in the Elasticsearch query string
    syntax (see Elasticsearch reference).

    Returns a dict that represents the query in the elasticsearch query DSL.
    """

    filter_must_not = []
    for ds in exclude_distributions:
        filter_must_not.append(
            {"term": {"paper_dcterms_spatial": _KB_DISTRIBUTION_VALUES[ds]}})

    for typ in exclude_article_types:
        filter_must_not.append(
            {"term": {"article_dc_subject": _KB_ARTICLE_TYPE_VALUES[typ]}})

    query = {
        'query': {
            'filtered': {
                'query': {
                    'query_string': {
                        'query': query_str
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {
                                'range': {
                                    'paper_dc_date': {
                                        'gte': date_range['lower'],
                                        'lte': date_range['upper']
                                    }
                                }
                            }
                        ],
                        'must_not': filter_must_not
                    }
                }
            }
        }
    }

    return query

def create_query2(query_str, date_range, exclude_distributions,
                 exclude_article_types):
    """Create elasticsearch query from input string.

    This method accepts boolean queries in the Elasticsearch query string
    syntax (see Elasticsearch reference).

    Returns a dict that represents the query in the elasticsearch query DSL.
    """

    filter_must_not = []
    for ds in exclude_distributions:
        filter_must_not.append(
            {"term": {"paper_dcterms_spatial": _KB_DISTRIBUTION_VALUES[ds]}})

    for typ in exclude_article_types:
        filter_must_not.append(
            {"term": {"article_dc_subject": _KB_ARTICLE_TYPE_VALUES[typ]}})

    query = {
        'fields': [],
        'query': {
            'filtered': {
                'query': {
                    'query_string': {
                        'query': query_str
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {
                                'range': {
                                    'paper_dc_date': {
                                        'gte': date_range['lower'],
                                        'lte': date_range['upper']
                                    }
                                }
                            }
                        ],
                        'must_not': filter_must_not
                    }
                }
            }
        }
    }

    return query

def createResultTable():
  conn = sqlite3.connect(DBRESULTSFILE)
  c = conn.cursor()
  c.execute('CREATE TABLE IF NOT EXISTS results (identifier text, queryid real, count real, time real)')
  conn.commit()
  conn.close()

def getAll():
  conn = sqlite3.connect(DBFILE)
  c = conn.cursor()
  c.execute('SELECT * FROM queries WHERE 1')
  data = c.fetchall()
  conn.close()
  shuffle(data)
  return data

def saveResult(identifier, queryid, count, time, index):
  identifier = str(identifier) + '@' + str(index)
  conn = sqlite3.connect(DBRESULTSFILE)
  c = conn.cursor()
  c.execute("INSERT INTO results VALUES (?, ?, ?, ?)", (identifier, queryid, count, time))
  conn.commit()
  conn.close()

if len(sys.argv) == 1:
  print "provide benchmark identifier"
  sys.exit(2)
id = sys.argv[1]

print 'Starting run for identifier:', id

createResultTable()
queries = getAll()
total = len(queries)
done = 0

for data in queries:
#  try:
    print 'Starting for query', data[0]
    params = {}
    params['query'] = data[1]
    params['dates'] = {}
    params['dates']['lower'] = data[2]
    params['dates']['upper'] = data[3]
    if len(data[4]) > 0:
      params['article_types'] = data[4].split('+')
    else:
      params['article_types'] = []
    if len(data[5]) > 0:
      params['distributions'] = data[5].split('+')
    else:
      params['distributions'] = []
    #print params
    start = time.time()
    r = tv_cloud(params, id);
    end = time.time()
    done += 1
    if r == False:
      saveResult(id, data[0], -2, -2, 'kb')
      print id, data[0], "failed, too many results"
    else:
      saveResult(id, data[0], -1, end - start, 'kb')
      print id, data[0], end - start, done, '/', total
#  except:
#    saveResult(id, data[0], -1, -1, 'kb')
#    print id, data[0], "failed, unknown"

