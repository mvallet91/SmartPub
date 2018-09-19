import pandas as pd
import time
import pytz
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}], timeout=30, max_retries=10, retry_on_timeout=True
)

publications = ['arxiv']
abstracts = []
content = []
stats = []
start = time.time()

for publication in publications:
    res = es.search(index="smartpub", body={"query": {"match": {"journal": {"query": publication}}}}, size=500)
    total_docs = res['hits']['total']
    print(total_docs)

    for doc in helpers.scan(es, index="smartpub", query={"query": {"match": {"journal": {"query": publication}}}},
                            size=50):
        values = []
        text = doc["_source"]["content"]
        abstract = doc['_source']['abstract']
        values.append(doc['_id'])
        values.append(len(text))
        values.append(len(abstract))
        values.append(int(doc['_source']['year']))
        values.append(doc['_source']['keywords'][0])
        stats.append(values)

pub_stats = pd.DataFrame(stats, columns=['id', 'content', 'abstract', 'year', 'keywords'])
print((time.time() - start) / 60, 'for first dataframe')
e_stats = []

for publication in publications:
    res = es.search(index="entities_smartpub", body={"query": {"match": {"journal": {"query": publication}}}}, size=1)
    total_docs = res['hits']['total']
    print(total_docs, 'entities to process')

    for doc in helpers.scan(es, index="entities_smartpub",
                            query={"query": {"match": {"journal": {"query": publication}}}}, size=500):
        values = []
        word = doc['_source']['lower']
        label = doc['_source']['label']
        ann = doc['_source']['annotator']
        values.append(word)
        values.append(doc['_source']['paper_id'])
        values.append(label)
        values.append(ann)
        values.append(int(doc['_source']['year']))
        e_stats.append(values)

entity_stats = pd.DataFrame(e_stats, columns=['word', 'paper_id', 'label', 'annotation', 'year'])
clean_stats = entity_stats.loc[(entity_stats['annotation'].isin(['dataset', 'method', 'undefined']))]
print((time.time() - start) / 60, 'for second dataframe')

entity_stats.to_pickle('/data2/SmartPub/app/modules/stats_pickles/entity_stats.pkl')
clean_stats.to_pickle('/data2/SmartPub/app/modules/stats_pickles/clean_stats.pkl')
pub_stats.to_pickle('/data2/SmartPub/app/modules/stats_pickles/pub_stats.pkl')

date = datetime.now(pytz.utc)
f = open('/data2/SmartPub/app/modules/stats_pickles/update_log.txt', 'a')
f.write('\n'+str(date))
f.close()