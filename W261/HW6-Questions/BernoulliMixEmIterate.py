from mrjob.job import MRJob
from mrjob.step import MRStep
from math import log, exp
from scipy.misc import logsumexp
import json


class BernoulliMixEmIterate(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(BernoulliMixEmIterate, self).__init__(*args, **kwargs)
        
        self.numMappers = 1     #number of mappers
        self.count = 0
        
    def configure_options(self):
        super(BernoulliMixEmIterate, self).configure_options()        
        self.add_passthrough_option(
            '--Test', dest='T', default=0, type='int',
            help='Test: run unit test (default 0)')
                
    def mapper_init(self):
        # load q_mk and alpha_k 
        with open('parameters') as f:
            param = json.loads(f.read())
        self.alpha_k = param['alpha']
        self.q_mk = param['q']
        self.K = len(self.alpha_k)
        # read words from file: unit test and Twitter data
        path = 'Bernoulli_EM_Unit_Test_header.csv' if self.options.T else 'topUsers_Apr-Jul_2014_1000-words_summaries.txt'
        with open(path) as f:                
            header = f.readline()        
        self.corpus = [w.strip('"') for w in header.strip().split(',')][-len(self.q_mk):]               
        # r_nk
        self.r_nk = {}
        
    
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
        self.r_nk[doc_id] = [exp(x) for x in log_rnk]
                
        # emit for q_mk
        for k in range(self.K):
            yield (k, '*'), (1, self.r_nk[doc_id][k])
        for word, freq in zip(self.corpus, tf):
            for k in range(self.K):                    
                yield (k, word), (freq > 0, self.r_nk[doc_id][k])
    
    # for local debug: save r_nk for the documents processed by the mapper
    def mapper_final(self):
        path = '/Users/leiyang/GitHub/mids/w261/HW6-Questions/rnk'
        with open(path, 'w') as f:
            f.write(json.dumps(self.r_nk))
         
            
    def reducer_init(self):
        self.K = 2 if self.options.T else 4
        self.alpha_k = [0]*self.K
        self.q_mk = {}
        
    def reducer(self, key, value):                
        k, m = key
        # calculate alpha_k
        if m == '*':
            N = sum_r = 0
            for cnt, r in value:
                N += cnt
                sum_r += r
            self.alpha_k[k] = 1.0*sum_r/N            
            return
            
        # calculate q_mk    
        if m not in self.q_mk:
            self.q_mk[m] = [0]*self.K
        
        sum_r = sum_r_I = 0
        for Itm, rnk in value:
            sum_r += rnk
            sum_r_I += rnk*Itm
            
        self.q_mk[m][k] = 1.0*sum_r_I/sum_r
        
    def reducer_final(self):
        # emit parameters for certain cluster(k) and word(m)
        param = {'q':self.q_mk, 'alpha':self.alpha_k}
        yield None, param
        

    def steps(self):     
        jc = {  
            'mapreduce.job.maps': '7',
            'mapreduce.job.reduces': '3',
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , mapper_final=self.mapper_final
                       , reducer_init=self.reducer_init
                       , reducer=self.reducer
                       , reducer_final=self.reducer_final
                       , jobconf = jc
                      )
               ]
        


if __name__ == '__main__':
    BernoulliMixEmIterate.run()