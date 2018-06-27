from pymongo import MongoClient
from nltk.corpus import wordnet
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.corpus import stopwords
from app.modules import normalized_pub_distance
from bson.objectid import ObjectId
from gensim.models.wrappers import FastText
model = FastText.load_fasttext_format('/Users/sepidehmesbah/Downloads/fastText/modelFT')
client = MongoClient('localhost:4321')
db = client.pub

def is_int_or_float(s):
    ''' return 1 for int, 2 for float, -1 for not a number'''
    try:
        float(s)

        return 1 if s.count('.')==0 else 2
    except ValueError:
        return -1
#allentities=db.named_entities.find()
allentities=db.sentences_ner.find({'label':'method','paper_id':'conf_vldb_JinKLT05'})
counting=db.named_entities.find().count()
print(counting)
count=0

dsnames = []
mtnames=[]

datasetspath = '/Users/sepidehmesbah/Downloads/ScholarSurf/app/files/dataset_names'
with open(datasetspath, "r") as file:
        for row in file.readlines():
            dsnames.append(row.strip())

methodpath = '/Users/sepidehmesbah/Downloads/ScholarSurf/app/files/method_names'
with open(methodpath, "r") as file:
        for row in file.readlines():
                    mtnames.append(row.strip())
for rr in allentities:
            print(count)
            ds_sim_90 = 0
            ds_sim_80 = 0
            ds_sim_70 = 0
            ds_sim_60 = 0
            ds_sim_50 = 0

            mt_sim_90 = 0
            mt_sim_80 = 0
            mt_sim_70 = 0
            mt_sim_60 = 0
            mt_sim_50 = 0
            count = count + 1
        #
        # if count>73438:


            ner_word=rr['ner']
            ner_id=rr['_id']
            ner_word=ner_word.split()
            if len(ner_word)>1:

                filterbywordnet = []
                filtered_words = [word for word in set(ner_word) if word not in stopwords.words('english')]
                for word in filtered_words:
                    isint=is_int_or_float(word)
                    if isint != -1:
                        filtered_words.remove(word)



                # filterbywordnet = [word for word in filtered_words if not wordnet.synsets(word)]
                # print(filtered_words)
                for word in set(filtered_words):
                    inwordNet = 1
                    inds = 0
                    if not wordnet.synsets(word):
                        inwordNet = 0
                        filterbywordnet.append(word)
                filtered_words=' '.join(filtered_words)
                filtered_words=filtered_words.replace('(','')
                filtered_words = filtered_words.replace(')', '')
                filtered_words = filtered_words.replace('[', '')
                filtered_words = filtered_words.replace(']', '')
                filtered_words = filtered_words.replace('{', '')
                filtered_words = filtered_words.replace('}', '')
                filtered_words = filtered_words.replace(',', '')
                lower_filteredwords=filtered_words.lower()
                filterbywordnet=' '.join(filterbywordnet)
                PMIdata=normalized_pub_distance.NPD(filtered_words, 'dataset')
                PMImethod=normalized_pub_distance.NPD(filtered_words, 'method')

                dssimilarity = []
                mtsimilarity = []


                for ds in dsnames:
                    try:
                        similarity=model.similarity(ds,lower_filteredwords)
                        dssimilarity.append(similarity)
                        if similarity>0.89:
                            ds_sim_90=1
                        elif similarity >0.79:
                            ds_sim_80=1
                        elif similarity >0.69:
                            ds_sim_70 = 1
                        elif similarity>0.59:
                            ds_sim_60=1
                        elif similarity>0.49:
                            ds_sim_50=1




                    except:

                        pass

                for mt in mtnames:
                    try:
                        similarity=model.similarity(mt, lower_filteredwords)
                        mtsimilarity.append(similarity)
                        # print(similarity)
                        if similarity > 0.89:
                            mt_sim_90 = 1
                        elif similarity > 0.79:
                            mt_sim_80 = 1
                        elif similarity > 0.69:
                            mt_sim_70 = 1
                        elif similarity > 0.59:
                            mt_sim_60 = 1
                        elif similarity > 0.49:
                            mt_sim_50 = 1



                    except:

                        pass

                try:
                    mtsimilarity = float(sum(mtsimilarity)) / len(mtsimilarity)
                except:
                    mtsimilarity = 0

                try:
                    dssimilarity = float(sum(dssimilarity)) / len(dssimilarity)
                except:
                    dssimilarity = 0
                #db.named_entities.update({"_id": ObjectId(ner_id)},
                #                         {'$set': {'filteredWord': filtered_words, 'filteredWordnet':filterbywordnet,'word_lower':lower_filteredwords,'PMIdata':PMIdata,'PMI':PMImethod,'ds_similarity':dssimilarity,'mt_similarity':mtsimilarity,'ds_sim_90':ds_sim_90,'ds_sim_80':ds_sim_80,'ds_sim_70':ds_sim_70,'ds_sim_60':ds_sim_60,'ds_sim_50':ds_sim_50,'mt_sim_90':mt_sim_90,'mt_sim_80':mt_sim_80,'mt_sim_70':mt_sim_70,'mt_sim_60':mt_sim_60,'mt_sim_50':mt_sim_50}})

            else:
                isint = is_int_or_float(rr['ner'])
                if isint == -1:
                    filtered_words = rr['ner'].replace('(', '')
                    filtered_words = filtered_words.replace(')', '')
                    filtered_words = filtered_words.replace('[', '')
                    filtered_words = filtered_words.replace(']', '')
                    filtered_words = filtered_words.replace('{', '')
                    filtered_words = filtered_words.replace('}', '')
                    PMIdata = normalized_pub_distance.NPD(filtered_words, 'dataset')
                    PMImethod = normalized_pub_distance.NPD(filtered_words, 'method')
                    dssimilarity = []
                    mtsimilarity = []


                    for ds in dsnames:
                        try:
                            similarity = model.similarity(ds, filtered_words.lower())
                            dssimilarity.append(similarity)
                            if similarity > 0.89:
                                ds_sim_90 = 1
                            elif similarity > 0.79:
                                ds_sim_80 = 1
                            elif similarity > 0.69:
                                ds_sim_70 = 1
                            elif similarity > 0.59:
                                ds_sim_60 = 1
                            elif similarity > 0.49:
                                ds_sim_50 = 1




                        except:
                            pass
                    for mt in mtnames:
                        try:
                            similarity = model.similarity(mt, filtered_words.lower())
                            mtsimilarity.append(similarity)
                            # print(similarity)
                            if similarity > 0.89:
                                mt_sim_90 = 1
                            elif similarity > 0.79:
                                mt_sim_80 = 1
                            elif similarity > 0.69:
                                mt_sim_70 = 1
                            elif similarity > 0.59:
                                mt_sim_60 = 1
                            elif similarity > 0.49:
                                mt_sim_50 = 1




                        except:
                            pass

                    try:
                     mtsimilarity = float(sum(mtsimilarity)) / len(mtsimilarity)

                    except:
                        mtsimilarity=0

                    try:
                        dssimilarity = float(sum(dssimilarity)) / len(dssimilarity)
                    except:
                        dssimilarity=0
            print(rr['ner'],dssimilarity,mtsimilarity)


                    #db.named_entities.update({"_id": ObjectId(ner_id)},
                    #                         {'$set': {'filteredWord': filtered_words, 'filteredWordnet': filtered_words,
                    #                                   'word_lower': filtered_words.lower(), 'PMIdata': PMIdata,
                    #                                   'PMI': PMImethod, 'ds_similarity':dssimilarity,'mt_similarity':mtsimilarity ,'ds_sim_90':ds_sim_90,'ds_sim_80':ds_sim_80,'ds_sim_70':ds_sim_70,'ds_sim_60':ds_sim_60,'ds_sim_50':ds_sim_50,'mt_sim_90':mt_sim_90,'mt_sim_80':mt_sim_80,'mt_sim_70':mt_sim_70,'mt_sim_60':mt_sim_60,'mt_sim_50':mt_sim_50}})


                #db.named_entities.update({"_id":ObjectId("5a1ea61ca86049349df46d23")}, {'$set': {'filteredWord': "Research Perspectives"}})