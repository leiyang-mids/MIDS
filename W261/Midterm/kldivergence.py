from mrjob.job import MRJob
from math import log
import re
import numpy as np
class kldivergence(MRJob):
    def mapper1(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
        for key in count:
            yield key, [index, count[key]*1.0/len(letter_list)]
            
    def mapper1_smooth(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
        for key in count:
            yield key, [index, (1+count[key]*1.0)/(24+len(letter_list))]
    
    # add this thing here as reducer_init, to just see the sorted mapper output
    def temp(self):
        b=0

    def reducer1(self, key, values):
        #Fill in your code
        # probability holder inline with line index, so that prob[1], prob[2] are P_1, P_2
        prob = [0, 0, 0] 
        # use index to control probablity, as there is no guarantee they arrive as 1, 2 in order
        for v in values:
            index, p = v[0], v[1]
            prob[index] = p
        yield None, prob[1] * log(prob[1]/prob[2])
    
    # added, otherwise weird error it won't run
    def mapper2(self, key, value):
        yield key, value
        
    def reducer2(self, key, values):
        kl_sum = 0
        for value in values:
            kl_sum = kl_sum + value
        yield None, kl_sum
            
    def steps(self):
        return [self.mr(###### switch mapper for (non)smoothing ######
                        #mapper=self.mapper1
                        mapper=self.mapper1_smooth
                        ,reducer_init=self.temp
                        ,reducer=self.reducer1
                       )
               
                , self.mr(mapper=self.mapper2, reducer=self.reducer2)
               ]

if __name__ == '__main__':
    kldivergence.run()