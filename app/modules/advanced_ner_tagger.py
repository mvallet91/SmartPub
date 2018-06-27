from nltk.tag.stanford import StanfordNERTagger
from elasticsearch import Elasticsearch
import itertools
import nltk
from pymongo import MongoClient
from nltk.corpus import wordnet
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
from nltk.corpus import stopwords
import re
import csv
from gensim.models.wrappers import FastText
model = FastText.load_fasttext_format('/Users/sepidehmesbah/Downloads/fastText/modelFT')
from app.modules import filter_entities
client = MongoClient('localhost:4321')
db = client.pub
import random
from nltk.tag.stanford import StanfordPOSTagger
# english_postagger = StanfordPOSTagger('/Users/sepidehmesbah/Downloads/stanford-postagger-full-2016-10-31/models/english-bidirectional-distsim.tagger', '/Users/sepidehmesbah/Downloads/stanford-postagger-full-2016-10-31/stanford-postagger.jar')
# print(english_postagger.tag('Figures 3 (a) and (b) show the precision-recall curves for the three datasets: MovieLens, NewsSmall and NewsBig.3'.split()))

dsnames=[]

# corpuspath = "/Users/sepidehmesbah/SmartPub/DataProfiling/dataset-names.txt"
# with open(corpuspath,"r") as file:
#     for row in file.readlines():
#         dsnames.append(row.strip())
#
# ###############################
#
# lines = []
# dsnames=[x.lower() for x in dsnames]
# dsnames = list(set(dsnames))


def get_continuous_chunks(text):
    chunked = ne_chunk(pos_tag(word_tokenize(text)))
    #print pos_tag(word_tokenize(text))

    prev = None
    continuous_chunk = []
    current_chunk = []

    for i in chunked:
        if type(i) == Tree:
            current_chunk.append(" ".join([token for token, pos in i.leaves()]))
        elif current_chunk:
            named_entity = " ".join(current_chunk)
            if named_entity not in continuous_chunk:
                continuous_chunk.append(named_entity)
                current_chunk = []
        else:
            continue
    return continuous_chunk
#my_sent="We use datasets from American Physical Society and select authors beginning their scientific careers at the year of 1993."





es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}]
)

es.cluster.health(wait_for_status='yellow')
client = MongoClient("localhost:4321")
pub = client.pub.dataset_names.find()
#path_to_model = "/Users/sepidehmesbah/Downloads/stanford-ner-2016-10-31/ner-modelparaLLPunc.ser.gz"
# path_to_model = "/Users/sepidehmesbah/Downloads/PaperSurf-master/ner-model.ser.gz"
# #path_to_model='/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration0_splitted100_0.ser.gz'
# #path_to_model="/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration0_splitted100_0.ser.gz"
path_to_jar = "/Users/sepidehmesbah/Downloads/stanford-ner-2016-10-31/stanford-ner.jar"
# nertagger=StanfordNERTagger(path_to_model, path_to_jar)

def get_NEs(res):

    for i, (a, b) in enumerate(res):
        if b == 'MET':
            temp = a
            if i > 1:
                j = i - 1
                if res[j][1] == 'MET':
                    continue
            j = i + 1
            try:
                if res[j][1] == 'MET':
                    temp = b
                    temp = res[j][0] + ' ' + b
                    z = j + 1
                    if res[j][1] == 'MET':
                        temp = a + ' ' + res[j][0] + ' ' + res[z][0]

            except:
                continue

            # result.append(a)
            result.append(temp)


    print(result)
    filterbywordnet = []
    filtered_words = [word for word in set(result) if word not in stopwords.words('english')]

    # filterbywordnet = [word for word in filtered_words if not wordnet.synsets(word)]
    print(filtered_words)
    for word in set(filtered_words):

        inwordNet = 1

        if not wordnet.synsets(word):
            filterbywordnet.append(word)
            inwordNet = 0
        filteredword, PMIdata, PMImethod, dssimilarity, mtsimilarity, ds_sim_50, ds_sim_60, ds_sim_70, ds_sim_80, ds_sim_90, mt_sim_50, mt_sim_60, mt_sim_70, mt_sim_80, mt_sim_90=filter_entities.filter_it(word,model)


        store_datasetname_in_mongo(db,doc["_id"],doc["_source"]["title"], doc["_source"]["journal"],doc["_source"]["year"],  word, inwordNet,filteredword, PMIdata, PMImethod, dssimilarity, mtsimilarity, ds_sim_50, ds_sim_60, ds_sim_70, ds_sim_80, ds_sim_90, mt_sim_50, mt_sim_60, mt_sim_70, mt_sim_80, mt_sim_90)



def check_if_id_exist_in_db(db, id,word):
    check_string = {'$and':[{'paper_id':id},{'word':word}]}
    if db.datasetNER.find_one(check_string) is not None:
        print("We already checked this paper")
        return True
    else:
        return False
def store_datasetname_in_mongo(db, id, title,journal, year, word,inwordNet,filtered_words, PMIdata, PMImethod, dssimilarity, mtsimilarity, ds_sim_50, ds_sim_60, ds_sim_70, ds_sim_80, ds_sim_90, mt_sim_50, mt_sim_60, mt_sim_70, mt_sim_80, mt_sim_90):
    my_ner = {
        "paper_id": id,
        "title": title,
        "journal": journal,
        "year":year,
        "word":word,
        "label":'method',
        "inwordNet":inwordNet,
        "filtered_words":filtered_words,
        "PMIdata":PMIdata,
        "PMImethod":PMImethod,
        "dssimilarity":dssimilarity,
        "mtsimilarity":mtsimilarity,
        "ds_sim_50":ds_sim_50,
        "ds_sim_60":ds_sim_60,
        "ds_sim_70":ds_sim_70,
        "ds_sim_80":ds_sim_80,
        "ds_sim_90":ds_sim_90,
        "mt_sim_50":mt_sim_50,
        "mt_sim_60":mt_sim_60,
        "mt_sim_70":mt_sim_70,
        "mt_sim_80":mt_sim_80,
        "mt_sim_90":mt_sim_90

    }
    if check_if_id_exist_in_db(db, id,word):
        return False
    else:

        db.entities.insert_one(my_ner)

        return True





#filter_conference = ["WWW", "ICSE", "VLDB", "JCDL", "TREC", "SIGIR", "ICWSM", "ECDL", "ESWC"]
#filter_conference = ["WWW", "ICSE", "VLDB", "PVLDB", "JCDL", "TREC",  "SIGIR", "ICWSM", "ECDL", "ESWC",  "IEEE J. Robotics and Automation", "IEEE Trans. Robotics","ICRA","ICARCV", "HRI", "ICSR", "PVLDB", "TPDL", "ICDM","Journal of Machine Learning Research","Machine Learning"]
#filter_conference = ["WWW", "ICSE", "VLDB","JCDL", "TREC",  "SIGIR", "ICWSM", "ECDL", "ESWC","TPDL", "ICRA","ICARCV", "HRI", "ICSR", "PVLDB",   "IEEE J. Robotics and Automation", "IEEE Trans. Robotics", "ICDM","Journal of Machine Learning Research","Machine Learning"]
filter_conference = ["WWW", "ICSE", "VLDB","JCDL", "TREC",  "SIGIR", "ICWSM", "ECDL", "ESWC","TPDL", "ICRA","ICARCV", "HRI", "ICSR", "PVLDB",   "IEEE J. Robotics and Automation", "IEEE Trans. Robotics", "ICDM","Journal of Machine Learning Research","Machine Learning"]

for conference in filter_conference:

    query = {"query":
        {"match": {
            "journal": {
                "query": conference,
                "operator": "and"
            }
        }
        }
    }

    res = es.search(index="ir", doc_type="publications",
                    body=query,  size=10000)
    print(len(res['hits']['hits']))
    # random_int=random.sample(range(0, 616), 150)
    # for i in random_int:
    #      print(res['hits']['hits'][i]['_id'])

    for doc in res['hits']['hits']:
        # sentence = doc["_source"]["text"].replace(',', ' ')

        query = doc["_source"]["content"]
        print(doc["_source"]["title"])
        # sentence = sentence.replace('\'', "")
        # mynames =''
        #query = "Figures 3 (a) and (b) show the precision-recall curves for the three datasets: MovieLens, NewsSmall and NewsBig. For the NewsBig dataset we were unable to run the memory based algorithm as it would not scale to these numbers; keeping the data in memory for such a large dataset was not feasible, while keeping it on disk and making random disk seeks would have taken a long time"
        #query="The approach selecting the lead sentences, which is taken as the baseline popularly on the DUC01 dataset, is denoted as LEAD. A similar method is to select the lead sentence in each paragraph. Since the information about the paragraphs is not available in DUC01, we do not include this method as a baseline. Two other unsupervised methods we compare include Gong’s algorithm based on LSA and Mihalcea’s algorithm based on graph analysis. Among the several options of Mihalcea’s algorithm, the method based on the authority score of HITS on the directed backward graph is the best. It is taken by us for comparison. These two unsupervised methods are denoted by LSA and HITS respectively."
        #query="The data set is an open benchmark data set which contains 147 documentsummary pairs from Document Understanding Conference (DUC) 2001 http://duc.nist.gov/. We use it because it is for (generic single-document, extraction task) that we are interested in and it is well preprocessed. We denoted it by DUC01."
        query=query.replace('. ', ' ')
        query = query.replace(', ', ' , ')
        query = query.replace('(', '( ')
        #path_to_model = "/Users/sepidehmesbah/Downloads/PaperSurf-master/ner-model.ser.gz"
        #path_to_model='/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration0_splitted100_0.ser.gz'
        #path_to_model='/Users/sepidehmesbah/Downloads/ScholarSurf/GAtraineddataset.ser.gz'
        #path_to_model='/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration3_splitted50_9.ser.gz'
        #path_to_model="/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_filesMet/embeddingClusteringAll1_text_iteration0_splitted100_0.ser.gz"
        #path_to_model='/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration1_splitted10_0.ser.gz'
        # file = open('//Users/sepidehmesbah/Downloads/crash-course-in-causality/03_matching-and-propensity-scores/02_propensity-scores/03_propensity-score-matching-in-r.en.txt',
        #             'r')
        # query = file.read()
        path_to_model='/Users/sepidehmesbah/PycharmProjects/NERDetector/Random_Indexing/backup_files_crfMet/embeddingClusteringAll1MET_text_iteration0_splitted25_0.ser.gz'

        #path_to_model='/Users/sepidehmesbah/Downloads/ScholarSurf/app/files/Methodtraineddataset.ser.gz'
        nertagger = StanfordNERTagger(path_to_model, path_to_jar)

        res = nertagger.tag(query.split())



        result = []
        result2=[]

        get_NEs(res)
        # print('##########')
        # path_to_model = '/Users/sepidehmesbah/PycharmProjects/NERDetector/crf_trained_files/embeddingClusteringAll1_text_iteration1_splitted10_0.ser.gz'
        # nertagger = StanfordNERTagger(path_to_model, path_to_jar)
        # res = nertagger.tag(query.split())
        # get_NEs(res)
