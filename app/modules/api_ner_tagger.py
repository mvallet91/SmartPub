import gensim
import sys
import os
from nltk.tag.stanford import StanfordNERTagger
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from nltk.corpus import wordnet
from nltk.corpus import stopwords

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.modules import api_ner_processing
from app.modules import api_config as cfg

java_path = "/usr/lib/jvm/java-1.8.0-openjdk-amd64"
os.environ['JAVAHOME'] = java_path

embedding_model = gensim.models.Word2Vec.load(cfg.ROOTPATH+'/embedding_models/modelword2vecbigram.model')

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.cluster.health(wait_for_status='yellow')

client = MongoClient('localhost:' + str(cfg.mongoDB_Port))
path_to_jar = cfg.STANFORD_NER_PATH
entity_names = []


def get_entities(words, current_model):
    results = []
    facet_tag = current_model.upper()
    entities = {}
    for i, (a, b) in enumerate(words):
        if b == facet_tag:
            temp = a
            if i > 1:
                j = i - 1
                if words[j][1] == facet_tag:
                    continue
            j = i + 1
            try:
                if words[j][1] == facet_tag:
                    temp = words[j][0] + ' ' + b
                    z = j + 1
                    if words[j][1] == facet_tag:
                        temp = a + ' ' + words[j][0] + ' ' + words[z][0]
            except IndexError:
                continue
            results.append(temp)

    filtered_words = [word for word in set(results) if word not in stopwords.words('english')]

    if len(filtered_words) < 1:
        pass

    for word in set(filtered_words):
        in_wordnet = 1
        if not wordnet.synsets(word):
            in_wordnet = 0
        print('before filter', word)
        # try:
        filtered_word, pmi_data, pmi_method, ds_similarity, mt_similarity = api_ner_processing.filter_word(word,
                                                                                            embedding_model)
        # except:
        #     continue
        print('after filter', word)
        entities[word] = {'pmi_data': pmi_data}, {'pmi_method': pmi_method}, \
                         {'ds_similarity': ds_similarity}, {'mt_similarity': mt_similarity}

        return entities


def tag_text_block(api_text_block, facet):
    if facet == 'data':
        facet = 'DATA'
    if facet == 'method':
        facet = 'MET'

    path_to_model = cfg.ROOTPATH+'/crf_trained_files/trained_ner_' + facet + '.ser.gz'
    ner_tagger = StanfordNERTagger(path_to_model, path_to_jar)
    text = api_text_block
    labelled_words = ner_tagger.tag(text.split())
    entities = get_entities(labelled_words, facet)

    return entities
