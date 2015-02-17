from __future__ import division
import json #or cjson
import re
from stemming.porter2 import stem
from operator import itemgetter
from math import log
from collections import defaultdict
class Hw1(object):

    def __init__(self):
        pass
    @staticmethod
    def read_line(a_json_string_from_document):
        #sample answer:
        return json.loads(a_json_string_from_document)
    @staticmethod
    def tokenize(string):
        unicode_word=re.findall(r'\w+',string.lower())
        return [str(word) for word in unicode_word ]
        #return a list of words

    @staticmethod
    def stopword(a_list_of_words):
        stopword = []
        for line in open('stop_word','r'):
            stopword.append(re.split('\n',line)[0])
        new_list=[word for word in a_list_of_words if word not in stopword]
        return new_list
        #or alternatively use new_list=filter(lambda x: x not in stopword, a_list_of_words)
        #return a list of words 
    @staticmethod
    def stemming(a_list_of_words):
        stems=[stem(word) for word in a_list_of_words]
        return stems
        #return a list of words

    def unigram_count(self, a_document_name):
        #TO NOTE, for this function, it's an default instance method, 
        #in contrast to static method. When calling the function, you need to declare a class instance first.
        freqdict={}
        for line in open(a_document_name,'r'):
            line1=Hw1.read_line(line)
            line2=Hw1.tokenize(line1)
            line3=Hw1.stopword(line2)
            line4=Hw1.stemming(line3)
            for word in line4:
                if word in freqdict.keys():
                    freqdict[word]+=1
                else:
                    freqdict[word]=1
        return sorted(freqdict.iteritems(),key=itemgetter(1),reverse=1)[:20]

        #return top 20 unigrams e.g. {[hot,99],[dog,66],...}

    def bigram_count(self,a_document_name):
        freqdict={}
        for line in open(a_document_name,'r'):
            line1=Hw1.read_line(line)
            line2=Hw1.tokenize(line1)
            line3=Hw1.stopword(line2)
            #line4=Hw1.stemming(line3)
            
            for i in range(0,len(line3)-1):
                bigram=line3[i]+' '+line3[i+1]
                if bigram in freqdict.keys():
                    freqdict[bigram]+=1
                else:
                    freqdict[bigram]=1
        print sorted(freqdict.iteritems(),key=itemgetter(1),reverse=1)[:20]
        
#THE FOLLOWING CODE IS DESIGNED FOR HOMEWORK 2 PART 3
class Hw2(Hw1):
    def tfidf(self, a_document_name):
        tf=defaultdict(dict)
        idf={}# idf dictionary of terms
        rid_mapper={}# map id number to the line number
        num_line=0
        for line in open(a_document_name,'r'):
            line1=Hw2.read_line(line)
            num_line+=1
            r_id=line1["review_id"]
            rid_mapper[r_id]=num_line
            line2=Hw2.tokenize(line1)
            line3=Hw2.stopword(line2)
            line4=Hw2.stemming(line3)
            for word in line4:
                if word in tf[r_id].keys():
                    tf[r_id][word]+=1
                else:
                    tf[r_id][word]=1
                    #if show up first time in a document, count idf++
                    if word in idf.keys():
                        idf[word]+=1
                    else:
                        idf[word]=1
        for key,value in idf.iteritems():
            idf[key]=log(num_line/value) #idf defination:number of document/ number of document has the key
                    
        for key,value in tf.iteritems():
            sum_tfidf=0
            for word,tfreq in value.iteritems():
                tf[key][word]=tfreq*idf[word]
                sum_tfidf+=(tfreq*idf[word])**2
            sum_tfidf=sum_tfidf**0.5 
            #normalize the tfidf vector to unit length
            for word,tfidf in value.iteritems():
                tf[key][word]=tf[key][word]/sum_tfidf
        return tf, rid_mapper
   
    def cosine(self,tfidf,rid_mapper):
        cosine_similarity=defaultdict(dict)
        rank_dict={}
        for key in tfidf.keys():
            for key1 in tfidf.keys():
                if key== key1:
                    cosine_similarity[key][key1]=-1
                else:
                    similarity=0
                    a=tfidf[key].keys()
                    b=tfidf[key1].keys()
                    intersect= [val for val in  a if val in b]
                    for word in intersect:
                        similarity+=tfidf[key][word]*tfidf[key1][word]                 
                    cosine_similarity[key][key1]=similarity
        #Getting the top 10 pairs
                    rank_dict[key+' '+key1]=similarity
        top10=sorted(rank_dict.iteritems(), key=itemgetter(1),reverse=1)[0:20] #they shown up in pairs, so keep 20.
        #print top10
        last='',''
        for a,b in top10:
            key,key1=re.split(' ',a)
            if key!=last[1] and key1!=last[0]:
                print rid_mapper[key],rid_mapper[key1],b
                print key,key1
                last=key,key1

if __name__ == '__main__':

    hw=Hw2()
    #hw.bigram_count('./review_KcSJUq1kwO8awZRMS6Q49g')
    a,b=hw.tfidf('./review_KcSJUq1kwO8awZRMS6Q49g')
    hw.cosine(a,b)