import operator
import requests
import string
from collections import Counter
from xml.etree import ElementTree

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from nltk.corpus import wordnet, stopwords
from pymongo import MongoClient

client = MongoClient('localhost:27017')
db = client.pub
db_reddit = client.reddit_drug

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

tud_demo = False

publications_index = 'ir_full'
entities_index = 'surfall_entities'

publications_index = 'smartpub'
entities_index = 'entities_smartpub'

if tud_demo:
    publications_index = 'ir_tud'
    entities_index = 'entities_tud'

stopword_list = set(stopwords.words('english'))
expansion = ['although', 'emmanuel', 'gonzalez', 'jeffrey', 'whereas', 'keyword', 'keywords', 'ieee', 'wang']
for word in expansion:
    stopword_list.add(word)
stopword_list = list(stopword_list)


def dosearch(_string):
    """
    :param _string: the query obtained from the website
    :type _string: string
    :return list: search results for the query, id_list, title_list, journal_list, year_list, authors_list
    :rtype: list
    Main search function
    """
    query_dict = {}
    title_list = []
    journal_list = []
    year_list = []
    authors_list = []
    id_list = []

    _query_author = {
        'query': {
            'match_phrase': {
                'authors': _string
            }
        }
    }

    if tud_demo:
        _query_author = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"authors": _string}},
                        {"match": {"supervisors": _string}}
                    ]
                }
            }
        }

    _search_author = es.search(index=publications_index, doc_type="publications", body=_query_author, size=1000)
    for doc in _search_author['hits']['hits']:
        title_list.append(doc['_source']['title'])
        if doc['_source']['journal'] == 'arxiv':
            journal_list.append(doc['_source']['keywords'][0])
        else:
            journal_list.append(doc['_source']['journal'])
        year_list.append(doc['_source']['year'])
        id_list.append(doc['_id'])

        if tud_demo:
            authors = doc['_source']['authors'] + doc['_source']['supervisors']
            authors_list.append(authors)
        else:
            authors_list.append(doc['_source']['authors'])

    _query_author = {"query": {
        "query_string":
            {
                "query": _string

            }
        }
    }
    _search_author = es.search(index=publications_index, doc_type="publications", body=_query_author, size=100)
    for doc in _search_author['hits']['hits']:
        if tud_demo:
            list_authors = doc['_source']['authors'] + doc['_source']['supervisors']
        else:
            list_authors = doc['_source']['authors']

        list_authors = [l.lower() for l in list_authors]
        if any(_string.lower() in s for s in list_authors):
            if (doc['_source']['title']) not in title_list:
                title_list.append(doc['_source']['title'])
                if doc['_source']['journal'] == 'arxiv':
                    journal_list.append(doc['_source']['keywords'][0])
                else:
                    journal_list.append(doc['_source']['journal'])
                year_list.append(doc['_source']['year'])
                id_list.append(doc['_id'])

                if tud_demo:
                    authors = doc['_source']['authors'] + doc['_source']['supervisors']
                    authors_list.append(authors)
                else:
                    authors_list.append(doc['_source']['authors'])

    if len(_string.split()) > 2:
        _query_title = {
            'query': {
                'match_phrase': {
                    'title': _string
                }
            }
        }
        _search_title = es.search(index=publications_index, doc_type="publications", body=_query_title)
        for doc in _search_title['hits']['hits']:
            if (doc['_source']['title']) not in title_list:
                title_list.append(doc['_source']['title'])
                journal_list.append(doc['_source']['journal'])
                if doc['_source']['journal'] == 'arxiv':
                    journal_list.append(doc['_source']['keywords'][0])
                else:
                    journal_list.append(doc['_source']['journal'])
                year_list.append(int(doc['_source']['year'].strip()))
                id_list.append(doc['_id'])

                if tud_demo:
                    authors = doc['_source']['authors'] + doc['_source']['supervisors']
                    authors_list.append(authors)
                else:
                    authors_list.append(doc['_source']['authors'])

    _query_all = {
        'query': {
            "query_string": {
                "default_field": "content",
                "query": _string
            }
        }
    }
    _search = es.search(index=publications_index, doc_type="publications", body=_query_all, size=25)

    context = {}
    list1 = []

    i = 0
    j = 0
    first_flag = True
    flag2 = 0

    if _search is not None:
        for doc in _search['hits']['hits']:  # Secondly, put the matches from the full content in the list
            if first_flag:
                query_dict['title'] = [doc['_source']['title']]
                query_dict['journal'] = [doc['_source']['journal']]
                if doc['_source']['journal'] == 'arxiv':
                    query_dict['journal'] = [doc['_source']['keywords'][0]]
                else:
                    query_dict['journal'] = [doc['_source']['journal']]
                query_dict['year'] = [doc['_source']['year']]
                query_dict['content'] = [doc['_source']['content']]
                if tud_demo:
                    authors = doc['_source']['authors'] + doc['_source']['supervisors']
                    authors_list.append(authors)
                else:
                    authors_list.append(doc['_source']['authors'])
                first_flag = False

            else:
                if (doc['_source']['title']) not in title_list:
                    title_list.append(doc['_source']['title'])
                    if doc['_source']['journal'] == 'arxiv':
                        journal_list.append(doc['_source']['keywords'][0])
                    else:
                        journal_list.append(doc['_source']['journal'])
                    year_list.append(doc['_source']['year'])
                    id_list.append(doc['_id'])
                    if tud_demo:
                        authors = doc['_source']['authors'] + doc['_source']['supervisors']
                        authors_list.append(authors)
                    else:
                        authors_list.append(doc['_source']['authors'])
            # query_dict['content'].append(doc['_source']['content'])
        # list1.append(_search['hits']['hits'][j]['_source']['jou'])
    # print(query_dict)

    return id_list, title_list, journal_list, year_list, authors_list


def popular_upcoming_entities(paper_id_list):
    terms_in_results = []
    terms_labels = {}
    entity_occurrences = {}
    upcoming_occurrences = {}

    for paper in paper_id_list:
        _query_terms = {
            "query": {
                "bool": {
                    "must": [{"match": {'paper_id': paper}}],
                    #        {"match": {'experiment': 'fullcorpusCRF_1cycle_fullcorpusWord2vec'}}],
                    "must_not": [{"match": {'annotator': 'noise'}},
                                 {"match": {'annotator': 'other'}},
                                 {"match": {'in_wordnet': 1}}],
                    "should": [{"match": {"annotator": 'method'}},
                               {"match": {"annotator": 'dataset'}}]
                }
            }
        }

        _query_terms = es.search(index=entities_index, doc_type="entities", body=_query_terms)
        for hit in _query_terms['hits']['hits']:
            entity = hit['_source']['clean']
            entity_words = entity.split()
            if len(entity_words) == 1 and not wordnet.synsets(entity.lower()) and entity.lower() not in stopword_list:
                terms_in_results.append(entity)
                terms_labels[entity] = hit
            else:
                x = 0
                for token in entity_words:
                    if (wordnet.synsets(token.lower())) or token.lower() in stopword_list:
                        x = x + 1
                if x / len(entity_words) < 0.5:
                    terms_in_results.append(entity)
                    terms_labels[entity] = hit

    terms_in_results_t = list(set(terms_in_results))
    terms_in_results = [entity for entity in terms_in_results_t if
                        not wordnet.synsets(entity.lower()) and entity.lower() not in stopword_list]

    for entity in terms_in_results:
        _query_occurrences = {
            'query': {
                'match_phrase': {
                    'content': entity
                }
            }
        }

        _query_occurrences = es.search(index=publications_index, doc_type="publications", body=_query_occurrences)
        years = []
        entity_occurrences[entity] = _query_occurrences['hits']['total']

        for doc in _query_occurrences['hits']['hits']:
            years.append(doc['_source']['year'])
        if years:
            if int(min(years)) > 2010:
                upcoming_occurrences[entity] = _query_occurrences['hits']['total']

    sorted_occurrences = sorted(entity_occurrences.items(), key=operator.itemgetter(1), reverse=True)
    sorted_upcoming = sorted(upcoming_occurrences.items(), key=operator.itemgetter(1), reverse=True)

    triples = []
    for pair in sorted_occurrences:
        amount = pair[1]
        term = pair[0]
        if terms_labels[term]['_source']['annotator'] in ['method', 'dataset']:
            actual_label = terms_labels[term]['_source']['annotator']
        elif terms_labels[term]['_source']['annotator'] in ['other', 'noise', 'software']:
            continue
        elif terms_labels[term]['_source']['mt_similarity'] > terms_labels[term]['_source']['ds_similarity']:
            actual_label = 'method'
        elif terms_labels[term]['_source']['mt_similarity'] < terms_labels[term]['_source']['ds_similarity']:
            actual_label = 'dataset'
        else:
            actual_label = 'method'
        triples.append([term, actual_label, amount])

    dataset_popular_entities = []
    method_popular_entities = []

    for entity in triples:
        if entity[1] == 'dataset':
            dataset_popular_entities.append(entity)
        if entity[1] == 'method':
            method_popular_entities.append(entity)

    return method_popular_entities, dataset_popular_entities, sorted_upcoming


def search_by_id(_string):
    title_list = []
    journal_list = []
    year_list = []
    authors_list = []
    id_list = []
    abstract_list = []
    ee_list = []
    reference_list = []
    method_entities = []
    dataset_entities = []
    method_amb = []
    dataset_amb = []
    all_amb = []

    _query_all = {
        'query': {
            'match_phrase': {
                '_id': _string
            }
        }
    }
    _query_entities = {
        'query': {
            'match_phrase': {
                'paper_id': _string
            }
        }
    }

    _search = es.search(index=publications_index, doc_type="publications", body=_query_all)
    _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities)

    for doc in _search['hits']['hits']:
        title_list.append(doc['_source']['title'])
        if doc['_source']['journal'] == 'arxiv':
            journal_list.append(doc['_source']['keywords'][0])
        else:
            journal_list.append(doc['_source']['journal'])
        year_list.append(doc['_source']['year'])
        authors_list.append(doc['_source']['authors'])
        id_list.append(doc['_id'])
        abstract_list.append(doc['_source']['abstract'])
        #         if tud_demo:
        reference_list.append(doc['_source']['references'])

    # for doc in _search_abs['hits']['hits']:
    #     abstract_list.append(doc['_source']['abstract'])
    #     reference_list.append(doc['_source']['references'])
    #     print(reference_list)

    for doc in _search_entities['hits']['hits']:
        if doc['_source']['annotator'] == 'noise':
            continue

        if doc['_source']['annotator'] == 'dataset':
            dataset_entities.append(doc['_source']['clean'])
            continue

        if doc['_source']['annotator'] == 'method':
            method_entities.append(doc['_source']['clean'])
            continue

        my_word = doc['_source']['clean']
        if wordnet.synsets(my_word.lower()) or my_word.lower() in stopword_list:
            continue

        my_word = ' '.join([t for t in my_word.split() if t not in stopword_list])

        method_score = 0
        dataset_score = 0

        dataset_score = dataset_score + float(doc['_source']['ds_similarity'])
        if int(doc['_source']['PMIdata']) == 1:
            dataset_score = dataset_score + 1
        if doc['_source']['label'] == 'dataset':
            if doc['_source']['clean'] not in method_amb:
                dataset_amb.append(doc['_source']['clean'])
            else:
                all_amb.append(doc['_source']['clean'])

        method_score = method_score + float(doc['_source']['mt_similarity'])
        if int(doc['_source']['PMImethod']) == 1:
            method_score = method_score + 1
        if doc['_source']['label'] == 'method':
            if doc['_source']['clean'] not in dataset_amb:
                method_amb.append(doc['_source']['clean'])
            else:
                all_amb.append(doc['_source']['clean'])

        my_word = doc['_source']['clean']
        if not wordnet.synsets(my_word.lower()) and my_word.lower not in stopword_list:
            my_word = my_word.split()
            final_word = ''
            if len(my_word) > 1:
                url = 'http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=&QueryString=' + str(
                    doc['_source']['clean'])
                try:
                    resp = requests.request('GET', url)
                    root = ElementTree.fromstring(resp.content)
                    check_if_exist = []
                    for child in root.iter('*'):
                        check_if_exist.append(child)
                    if len(check_if_exist) > 1:
                        final_word = doc['_source']['clean']
                except:
                    for ww in my_word:
                        if not wordnet.synsets(ww.lower()):
                            final_word = final_word + ww + ' '

            else:
                final_word = my_word[0]
            if final_word != '':
                if method_score > dataset_score:
                    method_entities.append(final_word)
                elif dataset_score > method_score:
                    dataset_entities.append(final_word)
                elif dataset_score == method_score:
                    all_amb.append(final_word)

            print(final_word, 'm', method_score, 'd', dataset_score)

        if doc['_source']['annotator'] == 'dataset':
            dataset_entities.append(doc['_source']['clean'])
        if doc['_source']['annotator'] == 'method':
            method_entities.append(doc['_source']['clean'])

    ambig_entities = list(set(all_amb))

    for entity in ambig_entities:
        labels = []
        for doc in helpers.scan(es, index=entities_index, query={"query": {"query_string": {"query": _string}}},
                                size=50):
            labels.append(doc['_source']['label'])
        count = Counter(labels)
        if count.most_common(1)[0][0] == 'method':
            method_entities.append(entity)
        else:
            dataset_entities.append(entity)

    m_entities = list(set(method_entities))
    method_entities = []
    for e in m_entities:
        word = ' '.join([t for t in e.split() if t not in stopword_list])
        if len(word) > 1:
            method_entities.append(word)

    d_entities = list(set(dataset_entities))
    dataset_entities = []
    for e in d_entities:
        word = ' '.join([t for t in e.split() if t not in stopword_list])
        if len(word) > 1:
            dataset_entities.append(word)

    a_entities = list(set(all_amb))
    all_amb = []
    for e in a_entities:
        word = ' '.join([t for t in e.split() if t not in stopword_list])
        if len(word) > 1:
            all_amb.append(word)

    print('ds', dataset_entities, 'mt', method_entities, 'all', all_amb)
    return id_list, title_list, journal_list, year_list, authors_list, abstract_list, method_entities, dataset_entities, all_amb


def search_by_author(_string):
    title_list = []
    journal_list = []
    year_list = []
    authors_list = []
    id_list = []
    wordcloud = ''
    abstract_list = []
    ee_list = []
    reference_list = []

    _query_author = {
        'query': {
            'match_phrase': {
                'authors': _string
            }
        }
    }
    if tud_demo:
        _query_author = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"authors": _string}},
                        {"match": {"supervisors": _string}}
                    ]
                }
            }
        }

    _search_author = es.search(index=publications_index, doc_type="publications", body=_query_author)

    for doc in _search_author['hits']['hits']:
        title_list.append(doc['_source']['title'])
        if doc['_source']['journal'] == 'arxiv':
            journal_list.append(doc['_source']['keywords'][0])
        else:
            journal_list.append(doc['_source']['journal'])
        year_list.append(doc['_source']['year'])
        id_list.append(doc['_id'])
        if tud_demo:
            authors = doc['_source']['authors'] + doc['_source']['supervisors']
            authors_list.append(authors)
        else:
            authors_list.append(doc['_source']['authors'])
        reference_list.append(doc['_source']['references'])

    for _id in id_list:
        _query_entities = {
            'query': {
                'match_phrase': {
                    'paper_id': _id
                }
            }
        }

        _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities)

        for doc in _search_entities['hits']['hits']:
            entity = doc['_source']['clean']
            if not wordnet.synsets(entity.lower()) and entity.lower() not in stopword_list:
                ent = '_'.join([w for w in doc['_source']['clean'].strip().split() if
                                len(w) > 1 and w.lower() not in stopword_list])
                wordcloud = wordcloud + ent.strip() + ' '

    return id_list, title_list, journal_list, year_list, authors_list, wordcloud


def word_cloud_for_first_page(id_list, search_text):
    wordcloud = ''
    for paper_id in id_list:
        _query_entities = {
            'query': {
                'match_phrase': {
                    'paper_id': paper_id
                }
            }
        }
        # do the query by matching the string from fulltext

        _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities)
        for doc in _search_entities['hits']['hits']:
            if doc['_source']['inwordNet'] != 1:
                entity = doc['_source']['clean'].lower()
                entity = ''.join(c for c in entity if c not in string.punctuation and c not in stopword_list)
                if entity != 'trec' and entity not in search_text and not wordnet.synsets(entity.lower()):
                    splitted_entity = entity.split()
                    if len(splitted_entity) > 1:
                        # entity = ''.join(c for c in entity if c not in stopwords.words('english'))
                        entity = ' '.join(c for c in splitted_entity if c not in stopword_list)
                        # entity = ps.stem(entity)
                    wordcloud = wordcloud + entity + ';'

    return wordcloud


def search_by_entity(_string):
    data = []
    data2 = []
    top_entities = []
    data_final = []
    entity_list = []
    papers = []
    _string = ' '.join(_string.split('%20'))
    query = {"query": {"match_phrase": {"word": _string}}}
    for doc in helpers.scan(es, index=entities_index, query=query, size=1000):
        papers.append(doc['_source']['paper_id'])

    papers = list(set(papers))

    for paper in papers:
        _query_entities = {
            'query': {
                'match_phrase': {
                    'paper_id': paper
                }
            }
        }
        _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities, size=20)

        for doc in _search_entities['hits']['hits']:
            entity_list = []
            entity = doc['_source']['clean'].lower()
            if not wordnet.synsets(entity.lower()) and entity.lower() not in stopword_list:
                data.append([entity, doc['_source']['year']])

    df = pd.DataFrame(data, columns=['key', 'date'])
    df1 = df.groupby(['key']).size().reset_index(name="value")
    df1 = df1.sort_values(['value'], ascending=False).head(7)
    top_entities.append(_string.lower())
    for dd in df1['key']:
        top_entities.append(dd)

    for i, dd in enumerate(df['key']):
        if dd in top_entities:
            data2.append([dd, df['date'][i]])

    top_entities = list(set(top_entities))
    df2 = pd.DataFrame(data2, columns=['key', 'date'])
    df2 = df2.groupby(['key', 'date']).size().reset_index(name="value")
    df4 = df2.set_index(['key', 'date'])
    df3 = df2.groupby(['date']).size().reset_index(name="value")
    for entity in top_entities:
        for year in df3['date']:
            if (entity, year) not in df4.index:
                data_final.append([entity, year, 0])
    df_final = pd.DataFrame(data_final, columns=['key', 'date', 'value'])
    frames = [df2, df_final]
    result = pd.concat(frames)
    result = result.sort_values(['key', 'date'], ascending=True)
    return result


def filter_by_conf(mystring):
    try:
        mystring = mystring.replace('ESWC', 'esws')
    except:
        pass
    allconf = mystring.split(',')
    print(allconf[0])
    data = []
    data2 = []
    top_entities = []
    data_final = []
    popular = []
    entity_list = []

    papers = []
    _query_entities = {"query": {
        "query_string":
            {
                "query": allconf[0]

            }
    }}
    _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities, size=10000)

    for doc in _search_entities['hits']['hits']:
        for conf in allconf:
            if conf.lower() in doc['_source']['paper_id'].lower():
                papers.append(doc['_source']['paper_id'])
    papers = list(set(papers))
    for paper in papers:
        _query_entities = {
            'query': {
                'match_phrase': {
                    'paper_id': paper
                }
            }
        }
        _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities, size=5000)

        for doc in _search_entities['hits']['hits']:
            data.append([doc['_source']['clean'].lower(), doc['_source']['year']])

    df = pd.DataFrame(data, columns=['key', 'date'])
    df1 = df.groupby(['key']).size().reset_index(name="value")
    df1 = df1.sort_values(['value'], ascending=False).head(7)
    top_entities.append(allconf[0].lower())
    for dd in df1['key']:
        top_entities.append(dd)
    for i, dd in enumerate(df['key']):
        if dd in top_entities:
            data2.append([dd, df['date'][i]])
    top_entities = list(set(top_entities))
    df2 = pd.DataFrame(data2, columns=['key', 'date'])
    df2 = df2.groupby(['key', 'date']).size().reset_index(name="value")
    df4 = df2.set_index(['key', 'date'])
    df3 = df2.groupby(['date']).size().reset_index(name="value")
    for entity in top_entities:
        for year in df3['date']:
            if (entity, year) not in df4.index:
                data_final.append([entity, year, 0])
    df_final = pd.DataFrame(data_final, columns=['key', 'date', 'value'])
    frames = [df2, df_final]
    result = pd.concat(frames)
    result = result.sort_values(['key', 'date'], ascending=True)

    return result


def wordcloud_entity(_string):
    popular = ''
    papers = []
    _string = ' '.join(_string.split('%20'))
    query = {"query": {"match_phrase": {"word": _string}}}
    for doc in helpers.scan(es, index=entities_index, query=query, size=1000):
        papers.append(doc['_source']['paper_id'])
    papers = list(set(papers))
    for paper in papers:
        _query_entities = {
            'query': {
                'match_phrase': {
                    'paper_id': paper
                }
            }
        }

        _search_entities = es.search(index=entities_index, doc_type="entities", body=_query_entities, size=100)
        for doc in _search_entities['hits']['hits']:
            entity = doc['_source']['clean']
            if not wordnet.synsets(entity.lower()) and entity.lower() not in stopword_list:
                entity_list = []
                if _string.lower() not in doc['_source']['clean'].lower():
                    ent = '_'.join([w.strip() for w in doc['_source']['clean'].split() if
                                    len(w) > 1 and w.lower() not in stopword_list])
                    popular = popular + ent.strip() + ' '

    return popular


def filter_by_pie(_string):
    data = []
    _string = ' '.join(_string.split('%20'))
    query = {"query": {"match_phrase": {"word": _string}}}
    for doc in helpers.scan(es, index=entities_index, query=query, size=7000):
        paper_id = doc['_source']['paper_id']
        _search_papers = es.search(index=publications_index, doc_type="publications",
                                   body={"query": {"match_phrase": {"_id": {"query": paper_id}}}}, size=1)
        for paper in _search_papers['hits']['hits']:
            topic = paper['_source']['keywords'][0]
            topic = topic.split(' - ')[-1]
            data.append(topic)

    df = pd.DataFrame(data, columns=['conference'])
    df1 = df.groupby(['conference']).size().reset_index(name="confnum")
    df1 = df1.sort_values(['confnum'], ascending=False).head(6)
    return df1


def dosearch_entity(_string):
    _string = ' '.join(_string.split('%20'))
    query_dict = {}
    title_list = []
    journal_list = []
    year_list = []
    authors_list = []
    id_list = []

    _query_all = {
        'query': {
            'match_phrase': {
                'content': _string
            }
        }
    }
    _search = es.search(index=publications_index, doc_type="publications", body=_query_all, size=30)

    context = {}
    list1 = []

    if _search is not None:
        for doc in _search['hits']['hits']:  # Secondly, put the matches from the fullcontent in the list1
            title_list.append(doc['_source']['title'])
            if doc['_source']['journal'] == 'arxiv':
                journal_list.append(doc['_source']['keywords'][0])
            else:
                journal_list.append(doc['_source']['journal'])
            year_list.append(doc['_source']['year'])
            authors_list.append(doc['_source']['authors'])
            id_list.append(doc['_id'])
            # query_dict['content'].append(doc['_source']['content'])
        # list1.append(_search['hits']['hits'][j]['_source']['jou'])
    return id_list, title_list, journal_list, year_list, authors_list


def select_sentence(entity):
    query = {"query":
        {"match": {
            "content.chapter.sentpositive": {
                "query": entity,
                "operator": "and"
            }
        }
        }
    }

    # query= {"query":
    #     {"bool": {
    #             "must": [
    #                 {
    #                     "match": {
    #                         "content.chapter.sentpositive": {
    #                             "query": datasetname,
    #                             "operator": "and"
    #                         }
    #                     }
    #                 },
    #                 {
    #                     "match": {
    #                         "paper_id": paper
    #                     }
    #                 }
    #             ]
    #         }
    #     }}

    # res = es.search(index="ind", doc_type="allsentnum",
    #                 body=query, size=15)
    # print(len(res['hits']['hits']))
    res = es.search(index="twosent", doc_type="twosentnorules",
                    body=query, size=10)
    # print(len(res['hits']['hits']))
    finalsent = ''

    for doc in res['hits']['hits']:
        # if doc["_source"]["paper_id"] in papernames:

        # sentence = doc["_source"]["text"].replace(',', ' ')

        sentence = doc["_source"]["content.chapter.sentpositive"]
        words = sentence.split()
        if len(words) > 5:
            finalsent = sentence

    return finalsent


def entities_for_crowdsourcing():
    all_entities = db.vague_entities.find_one(no_cursor_timeout=True)
    print(all_entities['entity'])
    return all_entities['entity']


def update_db_crowdsourcing(entity, label):
    db.entities.update_many({'word_lower': entity}, {'$set': {'Annotator': label}})
    db.vague_entities.remove({"entity": entity})

    res = es.search(index=entities_index, doc_type='entities',
                    body={"query": {"match_phrase": {"lower": entity}}}, size=5000)

    print("Got %d Hits" % res['hits']['total'])
    x = 0
    for hit in res['hits']['hits']:
        es.update(index=entities_index, doc_type="entities", id=hit['_id'], body={"doc": {"annotator": label}})
        x += 1
    print('Updated', x, 'in index')


def dashboard():
    publications = []
    entities = []
    return publications, entities


def select_reddit_post():
    reddit = db_reddit.temp_reddit.find_one(no_cursor_timeout=True)
    return reddit['text'], reddit['post_id']


def add_annotation(post_id, label):
    db_reddit.reddit_annotation.insert({'post_id': post_id, 'Annotator': 1, 'ADR': label})


def remove_temp(post_id):
    db_reddit.temp_reddit.remove({"post_id": post_id})
