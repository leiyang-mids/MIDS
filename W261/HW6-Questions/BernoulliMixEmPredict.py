from mrjob.job import MRJob
from mrjob.step import MRStep
from math import log, exp
from scipy.misc import logsumexp
from json import loads
from numpy import argmax


class BernoulliMixEmPred(MRJob):
    DEFAULT_PROTOCOL = 'json'
    PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'
    
    def __init__(self, *args, **kwargs):
        super(BernoulliMixEmPred, self).__init__(*args, **kwargs)
        
    def configure_options(self):
        super(BernoulliMixEmPred, self).configure_options()        
        self.add_passthrough_option(
            '--Test', dest='T', default=0, type='int',
            help='Test: run unit test (default 0)')
                
    def mapper_init(self):
        # load q_mk and alpha_k 
        with open('parameters') as f:
            param = loads(f.read())
        self.alpha_k = param['alpha']
        self.q_mk = param['q']
        self.K = len(self.alpha_k)
        # read words from file: unit test and Twitter data
        path = 'Bernoulli_EM_Unit_Test_header.csv' if self.options.T else 'topUsers_Apr-Jul_2014_1000-words_summaries.txt'
        with open(path) as f:                
            header = f.readline()        
        self.corpus = [w.strip('"') for w in header.strip().split(',')][-len(self.q_mk):]               
        # r_nk
        #self.r_nk = {}
        
    
    def mapper(self, _, line):
        if self.options.T:
            doc_id, fea = line.strip().split(',', 1)
        else:
            doc_id, label, tot, fea = line.strip().split(',', 3)
        # smoothing factor
        eps = 0.0001
               
        # get r_nk for incoming records, with previous parameters
        tf = map(int, fea.split(','))
        prod_tm = [0]*self.K
        for word, freq in zip(self.corpus, tf):                        
            for k in range(self.K):   
                q = self.q_mk[word][k]
                prod_tm[k] += log((q if freq > 0 else (1 - q)) + eps)
        
        log_beta = [log(alpha)+p_tm for alpha, p_tm in zip(self.alpha_k, prod_tm)]    
        lse = logsumexp(log_beta)
        log_rnk = [lb - lse for lb in log_beta]
        
        # emit prediction
        pred = argmax(log_rnk)
        truth = int(label)
        yield truth, pred
    
    def reducer_init(self):
        self.right_cnt = 0
        self.total = 0

    def reducer(self, key, value):
        cnt = {}
        for pred in value:
            if pred not in cnt:
                cnt[pred] = 0
            cnt[pred] += 1
            self.total += 1
            self.right_cnt += pred==key
        for pred in cnt:
            yield 'Truth: %s' %key, 'Prediction: %d; Count: %d' %(pred, cnt[pred])
            
    def reducer_final(self):
        yield 'Total accuracy: ', 1.0*self.right_cnt/self.total
        
    def steps(self):     
        jc = {                
            'mapreduce.job.maps': '1',                      
            'mapreduce.job.reduces': '1'
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper                       
                       , reducer_init=self.reducer_init
                       , reducer=self.reducer
                       , reducer_final=self.reducer_final
                       , jobconf = jc
                      )
               ]
        


if __name__ == '__main__':
    BernoulliMixEmPred.run()