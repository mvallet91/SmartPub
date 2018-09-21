import string
import re
import time

from pymongo import MongoClient
from nltk.corpus import stopwords, wordnet

start = time.time()
tr = str.maketrans("", "", string.punctuation)
client = MongoClient('localhost:27017')

def token_stopword_filter(word: str):
    filtered_word = ' '.join([t for t in word.split() if t not in stopwords.words('english')]) 
    filtered_word = re.sub(r'\[[^)]*\]', '', filtered_word)
    filtered_word = re.sub(u"[^\w\d'\s\-]+", '', filtered_word)
    return filtered_word

db = client.smartpub
pub_db = client.pub

print('Updating lower and no punctuation')
entities = db.entities.find()
x = 0
for rr in entities:
    if not 'word' in rr:
        continue
    word = rr['word'].lower()
    no_punkt = word.translate(tr)
    clean = ''.join([i for i in no_punkt if not i.isdigit()])
    clean = token_stopword_filter(clean)
    lower = rr['word'].lower()
    ds_sim = rr['ds_similarity']
    mt_sim = rr['mt_similarity']
    if type(ds_sim) is str:
        ds_sim = 0
    if type(mt_sim) is str:
        mt_sim = 0
    db.entities.update_one({'_id': rr['_id']},
                           {"$set": {'clean': clean, 'no_punkt': no_punkt,
                                     'word_lower': lower, 'ds_similarity': ds_sim, 'mt_similarity': mt_sim}}, upsert=False)
    x = x + 1
    if x % 10000 == 0:
        print(x, 'lower and no punctuation updated')


print('Creating dictionary of expert annotations')
annotation = {}
for rr in entities:
    if rr['Annotator'] != 'undefined':
        annotation[rr['clean']] = rr['Annotator']
        
pub_entities = pub_db.entities.find()        
for rr in pub_entities:
    if rr['Annotator'] != 'undefined':
        annotation[rr['clean']] = rr['Annotator']
        
print('Done with', len(annotation), 'entities')

print('Updating Annotations')
entities = db.entities.find()
x = 0
y = 0
for rr in entities:
    try:
        new_annotation = annotation[rr['clean']]
        db.entities.update_one({'_id': rr['_id']}, {"$set": {'Annotator': new_annotation}}, upsert=False)
        x = x + 1
        if x % 5000 == 0:
            print(x, 'entities updated')
    except KeyError:
        y = y + 1
        pass

print(x, 'entities updated in total and', y, 'not found')
print((time.time() - start)/60, 'minutes elapsed')
