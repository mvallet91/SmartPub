
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import math
import nltk
sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
###############################

client = MongoClient('localhost:4321')
db=client.pub
pub = client.pub.publications
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200}], timeout=30, max_retries=10, retry_on_timeout=True
)
es.cluster.health(wait_for_status='yellow', request_timeout=1)

###############################
def return_chapters(mongo_string_search, db):
    # mongo_string_search = {"dblpkey": "{}".format(dblkey)}
    results = db.publications.find(mongo_string_search)
    chapters = list()
    chapter_nums = list()
    list_of_docs = list()
    # list_of_abstracts = list()
    merged_chapters = list()
    my_dict = {
        "dblpkey": "",
        "title": "",
        "content": "",
        "journal": "",
        "year": "",
        "abstract": "",
        "references":"",
        "ee":""
    }
    for i, r in enumerate(results):
        # try:
        # list_of_sections = list()
        my_dict['dblpkey'] = r['dblpkey']
        my_dict['title'] = r['title']
        my_dict['journal'] = r['booktitle']
        my_dict['year']=r['year']
        my_dict['ee'] = r['ee']
        # print(r['content']['abstract'])
        try:
            my_dict['content'] = r['content']['fulltext']
        except:
            my_dict['content'] = ""
        try:
            my_dict['abstract'] = r['content']['abstract']
        except:

            my_dict['abstract'] = ""
        try:
            print(r['content']['references']['ref_title'])
            my_dict['references']=r['content']['references']['ref_title']
        except:

            my_dict['references'] =""
            # print(my_dict)
            # sys.exit(1)

        list_of_docs.append(my_dict)

        my_dict = {
            "dblpkey": "",
            "title": "",
            "content": "",
            "journal": "",
            "year": "",
            "abstract": "",
            "references": "",
            "ee": ""
        }

    return list_of_docs
#pipebook = [
#    {'$match': {'content.fulltext' : {"$exists": "true"}}},
#    {'$group': {'_id': '$booktitle', 'count': {'$sum': 1}}}
#]
#
#pipejournal = [
#    {'$match': {'content.fulltext' : {"$exists": "true"}}},
#    {'$group': {'_id': '$journal', 'count': {'$sum': 1}}}
#]
#
#conference = list(pub.aggregate(pipeline=pipebook)) + list(pub.aggregate(pipeline=pipejournal))
#
#with open("conference.csv","w") as file:
#    for conf in conference:
#        file.write("\"" + str(conf.get("_id")) + "\"," + str(conf.get("count"))  + "\n")

###############################

filter_conference = ["WWW", "ICSE", "VLDB", "PVLDB", "JCDL", "TREC",  "SIGIR", "ICWSM", "ECDL", "ESWC",  "IEEE J. Robotics and Automation", "IEEE Trans. Robotics","ICRA","ICARCV", "HRI", "ICSR", "PVLDB", "TPDL", "ICDM","Journal of Machine Learning Research","Machine Learning"]
#filter_conference=["ESWC"]
###############################

# query = {"content.fulltext": {"$exists": "true"}}
list_of_pubs=[]
# total = pub.find(query).count()
# bulksize = 50
# iters = math.ceil(total/bulksize)
# print(total)
for booktitle in filter_conference:
    mongo_string_search = {'$and': [{'booktitle': booktitle}, {'content.fulltext': {'$exists': True}}]}
    list_of_pubs.append(return_chapters(mongo_string_search, db))
for pubs in list_of_pubs:
    actions = []
    for cur in pubs:
        print(cur['dblpkey'])
        print(cur['journal'])

        # # Determine Conference
        # journal = None
        # if "booktitle" in cur:
        #     journal = cur["booktitle"]
        # if "journal" in cur:
        #     journal = cur["journal"]

        # # Filter Conferences
        # if journal not in filter_conference:
        #     continue

        # Extract Lines
        text = cur["content"]

        # lines = text.replace("\n", ".")
        # lines = (sent_detector.tokenize(lines.strip()))
        #
        # cleaned = []
        # for line in lines:
        #     #
        #     # line = line.strip()
        #     # if len(line.split(" ")) < 5:
        #     #     continue
        #     cleaned.append(line)

        # Add to bulk action
        # for num, line in enumerate(cleaned):
        actions.append({
                    "_index": "surfall",
                    "_type": "pubs",
                    "_id" : cur['dblpkey'],
                    "journal":cur['journal'],
                    "year":cur['year'],
                    "ee":cur['ee'],
                    "_source" : {
                        "text" : text,
                        "title": cur["title"],
                        "abstract":cur["abstract"],
                        "references":cur["references"]
                    }
                })
    print(len(actions))


    if len(actions) == 0:
            continue

    res = helpers.bulk(es,actions)

    print(res)

