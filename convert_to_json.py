#!/usr/bin/env python

# Import KB data into Elasticsearch. Fixed by LB, based on previous version
# (v2). Changes (a.o.):
# * worker count no longer hardwired
# * unescape HTML entities in a proper way
# * take command line arguments
# * fix path joining

from __future__ import print_function

import xml.etree.cElementTree as ElementTree
import zipfile, gzip
from functools import partial
import errno
import os
from os.path import basename, join, splitext
import sys
import elasticsearch
import HTMLParser
import datetime
from multiprocessing import Process, Pool, cpu_count
from HTMLParser import HTMLParser
import json

es = elasticsearch.Elasticsearch()

INDEX_NAME = 'kb'

def index_document(doc_obj, _id):
    es.index(INDEX_NAME,"doc",doc_obj, id=_id)

def write_progress(logfile):
    date_str = str(datetime.datetime.now())

    f = open(logfile, 'w')
    f.write(date_str)
    f.close()

unescape = HTMLParser().unescape

def process_file(zipfilename, progress_dir, json_dir):
    print("Processing:", zipfilename)

    filename = basename(zipfilename)
    logfile = join(progress_dir, splitext(filename)[0] + '.log')
    jsonfilename = join(json_dir, splitext(filename)[0] + '.json.gz')
    print("Checking progress file %s" % logfile)
    if os.path.exists(logfile):
        print("[%s] File already imported." % logfile)
        return 1

    article_count = 0
    try:
        with gzip.GzipFile(zipfilename) as file:
            txt = file.read('utf-8')
            txt = txt.replace('&','&amp;') # quick and dirty solution to encoding entities
            etree = ElementTree.fromstring(txt)
            docs = etree.findall('doc')
            with gzip.GzipFile(jsonfilename, 'wb') as jsonfile:
                for elem in docs:
                    if elem.tag == 'doc':
                        doc_el = elem

                        doc_obj = {}
                        # paper metadata
                        doc_obj['paper_dc_title'] = doc_el.find("field[@name='dc_title']").text
                        doc_obj['paper_dc_identifier'] = doc_el.find("field[@name='dc_identifier']").text
                        doc_obj['paper_dcterms_alternative'] = doc_el.find("field[@name='dcterms_alternative']").text
                        doc_obj['paper_dcterms_isVersionOf'] = doc_el.find("field[@name='dcterms_isVersionOf']").text
                        doc_obj['paper_dc_date'] = doc_el.find("field[@name='dc_date']").text
                        doc_obj['paper_dcterms_temporal'] = doc_el.find("field[@name='dcterms_temporal']").text
                        doc_obj['paper_dcx_recordRights'] = doc_el.find("field[@name='dcx_recordRights']").text 
                        doc_obj['paper_dc_publisher'] = doc_el.find("field[@name='dc_publisher']").text
                        doc_obj['paper_dcterms_spatial'] = doc_el.find("field[@name='dcterms_spatial']").text
                        doc_obj['paper_dc_source'] = doc_el.find("field[@name='dc_source']").text
                        doc_obj['paper_dcx_volume'] = doc_el.find("field[@name='dcx_volume']").text
                        doc_obj['paper_dcx_issuenumber'] = doc_el.find("field[@name='dcx_issuenumber']").text
                        doc_obj['paper_dcx_recordIdentifier'] = doc_el.find("field[@name='dcx_recordIdentifier']").text
                        doc_obj['paper_dc_identifier_resolver'] = doc_el.find("field[@name='dc_identifier_resolver']").text
                        doc_obj['paper_dc_language'] = doc_el.find("field[@name='dc_language']").text
                        doc_obj['paper_dcterms_isPartOf'] = doc_el.find("field[@name='dcterms_isPartOf']").text
                        doc_obj['paper_ddd_yearsDigitized'] = doc_el.find("field[@name='ddd_yearsDigitized']").text
                        doc_obj['paper_dcterms_spatial_creation'] = doc_el.find("field[@name='dcterms_spatial_creation']").text
                        doc_obj['paper_dcterms_issued'] = doc_el.find("field[@name='dcterms_issued']").text

                        # article metadata
                        doc_obj['article_dc_subject'] = doc_el.find("field[@name='dc_subject']").text
                        doc_obj['article_dc_title'] = doc_el.findall("field[@name='dc_title']")[1].text
                        doc_obj['article_dcterms_accessRights'] = doc_el.find("field[@name='dcterms_accessRights']").text
                        doc_obj['article_dcx_recordIdentifier'] = doc_el.findall("field[@name='dcx_recordIdentifier']")[1].text
                        doc_obj['article_dc_identifier_resolver'] = doc_el.findall("field[@name='dc_identifier_resolver']")[1].text
                        doc_obj['paper_dc_language'] = doc_el.find("field[@name='dc_language']").text
                        doc_obj['paper_dcterms_isPartOf'] = doc_el.find("field[@name='dcterms_isPartOf']").text
                        doc_obj['paper_ddd_yearsDigitized'] = doc_el.find("field[@name='ddd_yearsDigitized']").text
                        doc_obj['paper_dcterms_spatial_creation'] = doc_el.find("field[@name='dcterms_spatial_creation']").text
                        doc_obj['paper_dcterms_issued'] = doc_el.find("field[@name='dcterms_issued']").text

                        # article metadata
                        doc_obj['article_dc_subject'] = doc_el.find("field[@name='dc_subject']").text
                        doc_obj['article_dc_title'] = doc_el.findall("field[@name='dc_title']")[1].text
                        doc_obj['article_dcterms_accessRights'] = doc_el.find("field[@name='dcterms_accessRights']").text
                        doc_obj['article_dcx_recordIdentifier'] = doc_el.findall("field[@name='dcx_recordIdentifier']")[1].text
                        doc_obj['article_dc_identifier_resolver'] = doc_el.findall("field[@name='dc_identifier_resolver']")[1].text

                        # text content
                        content_el = doc_el.find("field[@name='content']")
                        text_el = content_el.find("text") #sometimes no text_el found (invalid ocr xml)
                        if text_el is None:
                            doc_obj['text_content'] = ''
                        else:
                            try:
                                text_content = "\n\n".join(el.text for el in text_el.findall("p") if el.text)
                            except:
                                print(ElementTree.tostring(text_el))
                                raise

                            # Unescape HTML entities
                            text_content = unescape(text_content)

                            doc_obj['text_content'] = text_content
                        # upload_document
                        doc_obj['zipfilename'] = zipfilename
                        doc_obj['identifier'] = doc_obj['article_dcx_recordIdentifier']
                        #index_document(doc_obj, doc_obj['identifier'])
                        jsonfile.write('{"index":{"_id":"' + doc_obj['identifier'] + '"}}\n')
                        json.dump(doc_obj, jsonfile)
                        jsonfile.write('\n')

                        article_count += 1

        write_progress(logfile)
        return (article_count)  
    except Exception as error:
        print("%s in %s: %s" % (type(error), zipfilename, error))
        import traceback
        traceback.print_exc()
        return (0)


def create_index(name):
    config = {}
    config['settings'] = {
        'number_of_shards': 3,
        'number_of_replicas': 1, 
        'analysis' : {
            'analyzer':{
              'default':{
            'type':'standard',
            'stopwords': '_dutch_',
              }
            }
          }
    }
    config['mappings'] = { 'doc': {'properties': {
         'article_dc_identifier_resolver': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'article_dc_subject': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'article_dc_title': {'type': 'string', 'term_vector': 'with_positions_offsets_payloads'},
         'article_dcterms_accessRights': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'article_dcx_recordIdentifier': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'identifier': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_date': {'format': 'dateOptionalTime', 'type': 'date'},
         'paper_dc_identifier': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_identifier_resolver': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_language': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_publisher': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_source': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dc_title': {'type': 'string', 'term_vector': 'with_positions_offsets_payloads'},
         'paper_dcterms_alternative': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_isPartOf': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_isVersionOf': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_issued': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_spatial': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_spatial_creation': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcterms_temporal': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcx_issuenumber': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcx_recordIdentifier': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcx_recordRights': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_dcx_volume': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'paper_ddd_yearsDigitized': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
         'text_content': {'type': 'string', 'term_vector': 'with_positions_offsets_payloads'},
         'zipfilename': {'type': 'string', 'include_in_all': 'false', 'index': 'not_analyzed'},
    }}}
    es.indices.create(index=name, body=config)


if __name__  == '__main__':
    try:
        input_dir = sys.argv[1]
        progress_dir = sys.argv[2]
        json_dir = sys.argv[3]
    except IndexError:
        print("usage: %s input_dir log_dir json_dir" % sys.argv[0], file=sys.stderr)
        print("    e.g., %s data/ progress/ json/" % sys.argv[0], file=sys.stderr)
        sys.exit(1)

    try:
        os.makedirs(progress_dir)
        os.makedirs(json_dir)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass

    #create_index(INDEX_NAME)

    pool = Pool(processes=cpu_count())

    pool.map(partial(process_file, progress_dir=progress_dir, json_dir=json_dir),
             (join(input_dir, fname) for fname in os.listdir(input_dir)
                                     if fname.endswith(".gz")))

    #for filename in os.listdir(input_dir):
        #if filename.endswith(".gz"):
            #path = join(input_dir, filename)
            #process_file(path, progress_dir)
            #pool.apply_async(process_file, path)
    pool.close()
    pool.join()