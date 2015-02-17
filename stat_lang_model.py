'''
Created on 16-Jan-2015

@author: akash
'''
from collections import defaultdict
import math
from operator import itemgetter
import re

from mrjob.job import MRJob

import simplejson as json
from solution import Hw1


class StatLangModel(MRJob):
    
    @classmethod
    def setup(self):
        self.collection = {}
        self.review = {}
        self.lamb = 0.7
        self.query = 'reputation'
        self.total = 0
        
        for line in open('collection_count.txt', 'r'):
            word, count = line.split('\t')
            self.collection[word[1:-1]] = count
            self.total += 1
        print self.total
                
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
            total += 1
        prob = (1 - self.lamb) * float(self.collection[self.query]) / self.total
        if self.query in words.keys():
            print text 
            prob += self.lamb * float(words[self.query]) / total 
        yield 'key', (json.loads(line)['review_id'], prob)
            

#     def combiner_count_words(self, word, counts):
#         # optimization: sum the words we've seen so far
#         yield (word, sum(counts))

    def reducer_count_words(self, _,rev_prob_pair):
        list = sorted(rev_prob_pair, key=itemgetter(1),reverse=True)[:5]
        for a in list:
            print a
        yield "max probs:", list

    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_words,
#                     combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words)
        ]


if __name__ == '__main__':
    StatLangModel.setup()
    StatLangModel.run()
