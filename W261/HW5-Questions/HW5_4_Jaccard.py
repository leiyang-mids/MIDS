from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
 
class Jaccard(MRJob):
    
    # job 1 mapper test
    def j1_mapper_test(self, _, line):        
        # time of mapper being called
        self.increment_counter('HW5_4', 'mapper_test', 1)
        # parse line, get doc id and terms
        temp = line.strip().lower().split(' ')
        doc_id, terms = temp[0], temp[1:]
        # Jaccard doesn't care occurrence        
        # emit to build inverted index
        for t in terms:
            yield (t.strip('{')[0], doc_id), 1
    
    # job 1 reducer_init()
    def j1_reducer_init(self):
        self.current_term = None
        self.current_strip = []
        self.count = 0
                
    # job 1 reducer
    def j1_reducer(self, key, count):
        term, doc = key[0], key[1]   
        self.count = sum(count)
        if self.current_term == term:
            self.current_strip.append(doc)
        else:
            if self.current_term:
                yield self.current_term, self.current_strip
            # reset
            self.current_term = term
            self.current_strip = [doc]
        
    # job 1 reducer final - emit last strip
    def j1_reducer_final(self):
        if self.current_term:
            yield self.current_term, self.current_strip
            
    # MapReduce steps
    def steps(self):
        return [MRStep(mapper=self.j1_mapper_test, reducer_init=self.j1_reducer_init,
                       reducer=self.j1_reducer, reducer_final=self.j1_reducer_final)
               ]

if __name__ == '__main__':
    Jaccard.run()
