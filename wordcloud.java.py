# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from collections import Counter
import time
import sqlite3
from random import shuffle
import sys
import subprocess
import json

DBFILE = 'queries.db'
DBRESULTSFILE = 'wordcloudsjava.db'

_KB_DISTRIBUTION_VALUES = {'sd_national': 'Landelijk',
                           'sd_regional': 'Regionaal/lokaal',
                           'sd_antilles': 'Nederlandse Antillen',
                           'sd_surinam': 'Suriname',
                           'sd_indonesia': 'Nederlands-Indië / Indonesië'}

_KB_ARTICLE_TYPE_VALUES = {'st_article': 'artikel',
                           'st_advert': 'advertentie',
                           'st_illust': 'illustratie met onderschrift',
                           'st_family': 'familiebericht'}

def create_query(query_str, date_range, exclude_distributions,
                 exclude_article_types):
    """Create elasticsearch query from input string.

    This method accepts boolean queries in the Elasticsearch query string
    syntax (see Elasticsearch reference).

    Returns a dict that represents the query in the elasticsearch query DSL.
    """

    filter_must_not = []
    if not exclude_distributions[0] == '':
      for ds in exclude_distributions:
          filter_must_not.append(
              {"term": {"paper_dcterms_spatial": _KB_DISTRIBUTION_VALUES[ds]}})

    if not exclude_article_types[0] == '':
      for typ in exclude_article_types:
          filter_must_not.append(
              {"term": {"article_dc_subject": _KB_ARTICLE_TYPE_VALUES[typ]}})


    query = {
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
  params = {}
  params['query'] = data[1]
  params['dates'] = {}
  params['dates']['lower'] = data[2]
  params['dates']['upper'] = data[3]
  params['article_types'] = data[4]
  params['distributions'] = data[5]
  print 'Starting run for', data[0]
  start = time.time()
  exitCode = subprocess.call(["/home/hkf700/mp/jre1.8.0_45/bin/java", "-Xms12g", "-Xmx12g", "-jar", "/home/hkf700/mp/scripts/queries/mapred/test.jar", "queries.queries",
                               params['dates']['lower'], params['dates']['upper'], params['article_types'], params['distributions'], params['query']])
  end = time.time()
  #start2 = time.time()
  #q = create_query(data[1], {'lower': data[2], 'upper': data[3]}, params['distributions'].split('+'), params['article_types'].split('+'))
  #exitCode = subprocess.call(["/home/hkf700/mp/jre1.8.0_45/bin/java", "-jar", "/home/hkf700/mp/scripts/queries/mapred/test.jar", "queries.queries", json.dumps(q)])
  #end2 = time.time()
  done += 1
  saveResult(id, data[0], -1, end - start, 'kb')
  print id, data[0], end - start,  done, '/', total, exitCode
  #saveResult(id, data[0], -1, -1, '')
  #print id, data[0], "failed"

