'''
Created on 16-Jan-2015

@author: akash
'''
from mrjob.job import MRJob
import re
from solution import Hw1
class CountTerms(MRJob):
    '''Count the collective total freq of terms in all the 
    reviews.
    '''

    def mapper_get_terms(self, _, data):
        line = Hw1.read_line(data)
        for word in (Hw1.tokenize(line['text'])):
            yield word, 1
            
    def combiner_count_terms(self,word, counts):
        yield word, sum(counts)
        
    def reducer_count_terms(self, word, counts):
        yield word, sum(counts)
        
    def steps(self):
        return[self.mr(mapper = self.mapper_get_terms,
                       combiner = self.combiner_count_terms,
                       reducer = self.reducer_count_terms)]

if __name__ == '__main__':
    CountTerms.run()
        