from pymongo import MongoClient
import string, time

start = time.time()
tr = str.maketrans("", "", string.punctuation)
client = MongoClient('localhost:4321')

db = client.smartpub
pub_db = client.pub

print('Updating lower and no punctuation')
entities = db.entities.find()
x = 0
for rr in entities:
    word = rr['word'].lower()
    no_punkt = word.translate(tr)
    clean = ''.join([i for i in no_punkt if not i.isdigit()])
    lower = rr['word'].lower()
    db.entities.update_one({'_id': rr['_id']},
                           {"$set": {'clean': clean, 'no_punkt': no_punkt,
                                     'word_lower': lower}}, upsert=False)
    x = x + 1
    if x % 10000 == 0:
        print(x, 'lower and no punctuation updated')

pub_entities = pub_db.entities.find()
print('Creating dictionary of human annotations')
ann = {}
for rr in pub_entities:
    if rr['Annotator'] != 'undefined':
        ann[rr['word_lower']] = rr['Annotator']
print('Done with', len(ann), 'entities')

print('Updating Annotations')
entities = db.entities.find()
x = 0
y = 0
for rr in entities:
    try:
        new = ann[rr['word'].lower()]
        db.entities.update_one({'_id': rr['_id']}, {"$set": {'Annotator': new}}, upsert=False)
        # print(rr['word'].lower(), ann[rr['word'].lower()])
        x = x + 1
        if x % 5000 == 0:
            print(x, 'entities updated')
    except KeyError:
        # print('No annotation for', rr['word'].lower())
        y = y + 1
        pass

print(x, 'entities updated in total and', y, 'not found')
print((time.time() - start)/60, 'minutes elapsed')
