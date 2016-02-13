from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
 
class Jaccard(MRJob):
    
    # job 1 mapper for SYSTEMS_TEST_DATASET
    def j1_mapper_test(self, _, line):        
        # time of mapper being called
        self.increment_counter('HW5_4', 'mapper_test', 1)
        # parse line, get doc id and terms
        temp = line.strip().lower().split(' ')
        doc_id, terms = temp[0], temp[1:]
        # Jaccard doesn't care occurrence        
        # emit to build inverted index
        for t in terms:
            yield (t.strip('{')[0], doc_id), int(t.strip('{')[2:-1])
            
    # job 1 mapper for 5-gram: build pseudo-document (co-ocurrence matrix)
    def j1_mapper_5gram(self, _, line):
        # parse line, get words and counts
        grams, cnt, p_cnt, b_cnt = line.strip().split('\t')
        grams = grams.lower().split(' ')
        n_gram = len(grams)
        # emit co-ocurrence for each pair
        for w1, w2 in [[grams[i], grams[j]] for i in range(n_gram) for j in range(n_gram)]:
            yield (w2, w1), int(cnt)
            
    # job 1 combiner - local aggregation
    def j1_combiner(self, pair, count):
        yield (pair), sum(count)
    
    # job 1 reducer_init()
    def j1_reducer_init(self):
        self.current_term = None
        self.current_strip = {}
                        
    # job 1 reducer
    def j1_reducer(self, key, count):
        #yield key, sum(count)
        term, doc = key[0], key[1]           
        if self.current_term == term:
            # accumulate postings
            self.current_strip[doc] = sum(count)
        else:
            # yield previous term and stripe
            if self.current_term:
                yield self.current_term, self.current_strip
            # reset new term
            self.current_term = term
            self.current_strip = {doc:sum(count)}
                    
    # job 1 reducer final - emit last strip
    def j1_reducer_final(self):
        if self.current_term:
            yield self.current_term, self.current_strip
            
    # job 2 mapper - emit pair-wise similarity from strips
    def j2_mapper(self, term, postings):
        # get all postings from generator
        posts = [p for p in postings]
        size = len(posts)
        # emit dummy for order inversion, for |A| and |B|
        # emit 1 here since Jaccard is binary and doesn't care count
        for p in posts:
            yield ('*', p), 1
        # emit pairs (here assuming job 1 partitioner did secondary sort) for |A \cap B|
        for p1, p2 in [[posts[i], posts[j]] for i in range(size) for j in range(i+1, size)]:
            yield (p1, p2), 1
    
    # job 2 combiner - local count aggregation
    def j2_combiner(self, pair, count):
        yield (pair), sum(count)
        
    # job 2 reducer_init - create helper data structures
    def j2_reducer_init(self):
        self.current_pair = None
        self.current_count = 0
        self.marginals = {}
        self.current_marginal = None
        
    # job 2 reducer - get pair similarity
    def j2_reducer(self, pair, count):
        p1, p2 = pair[0], pair[1]
        tot = sum(count)
        # accumulate marginal and cache them
        if p1 == '*':
            if p2 not in self.marginals:
                self.marginals[p2] = tot
            else:
                self.marginals[p2] += tot
        else:
            # calculate similarity
            yield (p1,p2), 1.0*tot/(self.marginals[p1]+self.marginals[p2]-tot)           
                   
    # job 3 mapper - for secondary sort
    def j3_mapper(self, pair, sim):
        yield (pair, sim), None
        
    # job 3 reducer_init
    def j3_reducer_init(self):
        self.top = 100
        self.n = 0
    
    # job 3 reducer - show top 100 pairs
    def j3_reducer(self, result, _):
        self.n += 1
        if self.n <= self.top:
            yield result
            
    # MapReduce steps
    def steps(self):
        jobconf1 = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1r -k2,2r', # no need to sort
            #'mapred.text.key.partitioner.options' : '-k1,1 -k2,2r',
            #'mapreduce.partition.keypartitioner.options': '-k1,2',
            'mapreduce.job.maps': '4',
            'mapreduce.job.reduces': '3',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ',',
            'stream.map.output.field.separator': ' ',
        }
        jobconf2 = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1r -k2,2r', # -k2,2r',
            #'mapred.text.key.partitioner.options' : '-k1,1 -k2,2r',
            #'mapreduce.partition.keypartitioner.options': '-k1,2',
            'mapreduce.job.maps': '3',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        jobconf3 = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k3,3nr',
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        # step configure for sorting
        # NOTE: DO NOT use jobconf when running with Python locally
        return [MRStep(mapper=self.j1_mapper_5gram
                       , combiner=self.j1_combiner, reducer_init=self.j1_reducer_init
                 #      , reducer=self.j1_reducer, reducer_final=self.j1_reducer_final
                       , jobconf=jobconf1
                      )
                #,MRStep(mapper=self.j2_mapper, combiner=self.j2_combiner, 
                #       reducer_init=self.j2_reducer_init, reducer=self.j2_reducer
                #       , jobconf=jobconf2
                #       )
                #,MRStep(mapper=self.j3_mapper, reducer_init=self.j3_reducer_init, 
                #       reducer=self.j3_reducer, jobconf=jobconf3)
               ]

if __name__ == '__main__':
    Jaccard.run()
