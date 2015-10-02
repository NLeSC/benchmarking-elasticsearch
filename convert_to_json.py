#!/usr/bin/env python

# Convert KB XML data to Elasticsearch bulk import JSON format.
# Written by Tom Kenter (UvA), Ridho Reinanda (UvA), Lars Buitinck (NLeSC),
# Eric de Kruijf (VU).

from __future__ import print_function

import xml.etree.cElementTree as ElementTree
from functools import partial
import errno
import gzip
from HTMLParser import HTMLParser
import json
import os
from os.path import basename, join, splitext
import shutil
import sys
from tempfile import NamedTemporaryFile

from joblib import Parallel, delayed


#es = elasticsearch.Elasticsearch()

INDEX_NAME = 'kb'


unescape = HTMLParser().unescape


def process_file(zipfilename, json_dir):
    print("Processing:", zipfilename)

    filename = basename(zipfilename)
    final_json_path = join(json_dir, splitext(filename)[0] + '.json.gz')

    if os.path.exists(final_json_path):
        print("[%s] File already imported." % filename)
        return 1

    article_count = 0
    try:
        with gzip.GzipFile(zipfilename) as file:
            txt = file.read('utf-8')
        txt = txt.replace('&','&amp;') # quick and dirty solution to encoding entities
        etree = ElementTree.fromstring(txt)
        del txt
        docs = etree.findall('doc')

        with NamedTemporaryFile(dir=json_dir, prefix='_kbimport_tmp_') as temp_json:
            temp_json.close()

            with gzip.GzipFile(temp_json.name, 'wb') as jsonfile:
                for elem in docs:
                    if elem.tag == 'doc':
                        doc_obj = {}
                        # paper metadata
                        doc_obj['paper_dc_title'] = elem.find("field[@name='dc_title']").text
                        doc_obj['paper_dc_identifier'] = elem.find("field[@name='dc_identifier']").text
                        doc_obj['paper_dcterms_alternative'] = elem.find("field[@name='dcterms_alternative']").text
                        doc_obj['paper_dcterms_isVersionOf'] = elem.find("field[@name='dcterms_isVersionOf']").text
                        doc_obj['paper_dc_date'] = elem.find("field[@name='dc_date']").text
                        doc_obj['paper_dcterms_temporal'] = elem.find("field[@name='dcterms_temporal']").text
                        doc_obj['paper_dcx_recordRights'] = elem.find("field[@name='dcx_recordRights']").text
                        doc_obj['paper_dc_publisher'] = elem.find("field[@name='dc_publisher']").text
                        doc_obj['paper_dcterms_spatial'] = elem.find("field[@name='dcterms_spatial']").text
                        doc_obj['paper_dc_source'] = elem.find("field[@name='dc_source']").text
                        doc_obj['paper_dcx_volume'] = elem.find("field[@name='dcx_volume']").text
                        doc_obj['paper_dcx_issuenumber'] = elem.find("field[@name='dcx_issuenumber']").text
                        doc_obj['paper_dcx_recordIdentifier'] = elem.find("field[@name='dcx_recordIdentifier']").text
                        doc_obj['paper_dc_identifier_resolver'] = elem.find("field[@name='dc_identifier_resolver']").text
                        doc_obj['paper_dc_language'] = elem.find("field[@name='dc_language']").text
                        doc_obj['paper_dcterms_isPartOf'] = elem.find("field[@name='dcterms_isPartOf']").text
                        doc_obj['paper_ddd_yearsDigitized'] = elem.find("field[@name='ddd_yearsDigitized']").text
                        doc_obj['paper_dcterms_spatial_creation'] = elem.find("field[@name='dcterms_spatial_creation']").text
                        doc_obj['paper_dcterms_issued'] = elem.find("field[@name='dcterms_issued']").text

                        # article metadata
                        doc_obj['article_dc_subject'] = elem.find("field[@name='dc_subject']").text
                        doc_obj['article_dc_title'] = elem.findall("field[@name='dc_title']")[1].text
                        doc_obj['article_dcterms_accessRights'] = elem.find("field[@name='dcterms_accessRights']").text
                        doc_obj['article_dcx_recordIdentifier'] = elem.findall("field[@name='dcx_recordIdentifier']")[1].text
                        doc_obj['article_dc_identifier_resolver'] = elem.findall("field[@name='dc_identifier_resolver']")[1].text
                        doc_obj['paper_dc_language'] = elem.find("field[@name='dc_language']").text
                        doc_obj['paper_dcterms_isPartOf'] = elem.find("field[@name='dcterms_isPartOf']").text
                        doc_obj['paper_ddd_yearsDigitized'] = elem.find("field[@name='ddd_yearsDigitized']").text
                        doc_obj['paper_dcterms_spatial_creation'] = elem.find("field[@name='dcterms_spatial_creation']").text
                        doc_obj['paper_dcterms_issued'] = elem.find("field[@name='dcterms_issued']").text

                        # article metadata
                        doc_obj['article_dc_subject'] = elem.find("field[@name='dc_subject']").text
                        doc_obj['article_dc_title'] = elem.findall("field[@name='dc_title']")[1].text
                        doc_obj['article_dcterms_accessRights'] = elem.find("field[@name='dcterms_accessRights']").text
                        doc_obj['article_dcx_recordIdentifier'] = elem.findall("field[@name='dcx_recordIdentifier']")[1].text
                        doc_obj['article_dc_identifier_resolver'] = elem.findall("field[@name='dc_identifier_resolver']")[1].text

                        # text content
                        content_el = elem.find("field[@name='content']")
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

            # Move within a directory is atomic, so we don't end up with
            # partially completed files in the final output.
            shutil.move(temp_json.name, final_json_path)

        return article_count
    except Exception as error:
        print("%s in %s: %s" % (type(error), zipfilename, error))
        import traceback
        traceback.print_exc()
        return 0


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
        input_dir, json_dir = sys.argv[1:]
    except IndexError:
        print("usage: %s input_dir json_dir" % sys.argv[0], file=sys.stderr)
        print("    e.g., %s data/ json/" % sys.argv[0], file=sys.stderr)
        sys.exit(1)

    try:
        os.makedirs(json_dir)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass

    #create_index(INDEX_NAME)

    par = Parallel(n_jobs=-1)
    counts = par(delayed(process_file)(join(input_dir, fname), json_dir=json_dir)
        for fname in os.listdir(input_dir))
    print("Processed %d articles" % sum(counts))
