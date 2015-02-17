'''
Created on 16-Jan-2015

@author: akash
'''
from mrjob.job import MRJob
import re
import simplejson as json
from operator import itemgetter
from solution import Hw1
from collections import defaultdict
import math

class StatLangModel(MRJob):
    
    @classmethod
    def setup(self):
        self.collection = {}
        self.review={}
        self.lamb = 0.7
        self.query = 'reputation'
        
        for line in open('collection_count.txt','r'):
            word, count = line.split('\t')
            self.collection[word] = count
                
    def mapper_get_words(self, _, line):
        # yield each word in the line
        text = json.loads(line)['text']
        words = {}
        total = 0
        for word in re.findall(r'\w+', text.lower()):
            if word in words.keys():
                words[word] += 1
            else:
                words[word] = 1
            total +=1
        if self.query in words.keys():
            print text 
            prob = math.log(float(words[self.query])/total)
        else :
            prob = 0
        yield json.loads(line)['review_id'], prob
            

#     def combiner_count_words(self, word, counts):
#         # optimization: sum the words we've seen so far
#         yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (word, sum(counts))

    # discard the key; it is just None
    def reducer_find_max_word(self, _, review_prob_pairs):
        list = sorted(review_prob_pairs, key=itemgetter(1), reverse=True)[:10]
        for a in list:
            print a
        yield "max words:", list
    
    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_words,
#                     combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words),
            self.mr(reducer=self.reducer_find_max_word)
        ]


if __name__ == '__main__':
    StatLangModel.setup()
    StatLangModel.run()
