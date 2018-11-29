import codecs
import numpy
import math
import re
import elasticsearch
from numbers import Number

from nltk.corpus import stopwords
from nltk.corpus import wordnet

from app.modules.api_config import ROOTPATH

stopword_path = ROOTPATH + "/data/stopword_en.txt"
stopword_list = []
with open(stopword_path, 'r') as file:
    for sw in file.readlines():
        stopword_list.append(sw.strip())

url_dbpedia = 'http://lookup.dbpedia.org/api/search/KeywordSearch?QueryClass=place&QueryString='
regex = re.compile(".*?\((.*?)\)")

index = 'ir_full'
twosent_index = 'twosent'


class autovivify_list(dict):
    """
    Pickleable class to replicate the functionality of collections.defaultdict
    """

    def __missing__(self, key):
        value = self[key] = []
        return value

    def __add__(self, x):
        """
        Override addition for numeric types when self is empty
        """
        if not self and isinstance(x, Number):
            return x
        raise ValueError

    def __sub__(self, x):
        """
        Also provide subtraction method
        """
        if not self and isinstance(x, Number):
            return -1 * x
        raise ValueError


def build_word_vector_matrix(vector_file, named_entities):
    """
    Read a GloVe array from sys.argv[1] and return its vectors and labels as arrays
    """
    numpy_arrays = []
    labels_array = []
    with codecs.open(vector_file, 'r', 'utf-8') as f:
        for c, r in enumerate(f):
            sr = r.split()
            try:
                if sr[0] in named_entities and not wordnet.synsets(sr[0]) and sr[0].lower() not in stopwords.words(
                        'english'):
                    labels_array.append(sr[0])
                    numpy_arrays.append(numpy.array([float(i) for i in sr[1:]]))
            except:
                continue
    return numpy.array(numpy_arrays), labels_array


def find_word_clusters(labels_array, cluster_labels):
    """
    Read the labels array and clusters label and return the set of words in each cluster
    """
    cluster_to_words = autovivify_list()
    for c, i in enumerate(cluster_labels):
        cluster_to_words[i].append(labels_array[c])
    return cluster_to_words


def normalized_entity_distance(entity, context):
    """

    :param entity:
    :param context:
    :return filtered_entities:
    """
    filtered_entities = []
    cn = context
    es = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])
    entity = entity.lower()

    query = {}
    res = es.search(index=twosent_index, doc_type="twosentnorules", body=query)
    NN = res['hits']['total']

    query = {"query":
        {"match": {
            "content.chapter.sentpositive": {
                "query": entity,
                "operator": "and"
            }
        }
        }
    }
    res = es.search(index=twosent_index, doc_type="twosentnorules", body=query)
    total_a = res['hits']['total']
    query = {"query":
        {"match": {
            "content.chapter.sentpositive": {
                "query": cn,
                "operator": "and"
            }
        }
        }
    }
    res = es.search(index=twosent_index, doc_type="twosentnorules", body=query)
    total_b = res['hits']['total']
    query_text = entity + ' ' + cn
    query = {"query":
        {"match": {
            "content.chapter.sentpositive": {
                "query": query_text,
                "operator": "and"
            }
        }
        }
    }
    res = es.search(index=twosent_index, doc_type="twosentnorules", body=query)
    total_ab = res['hits']['total']
    pmi = 0
    if total_a and total_b and total_ab:
        total_ab = total_ab / NN
        total_a = total_a / NN
        total_b = total_b / NN
        pmi = total_ab / (total_a * total_b)
        pmi = math.log(pmi, 2)
    return pmi
