from mrjob.job import MRJob
from mrjob.step import MRStep
from json import dumps


class BernoulliMixEmInit(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(BernoulliMixEmInit, self).__init__(*args, **kwargs)
        
                                                 
    def configure_options(self):
        super(BernoulliMixEmInit, self).configure_options()
        self.add_passthrough_option(
            '--K', dest='K', default=3, type='int',
            help='K: number of clusters (default 3)')
        self.add_passthrough_option(
            '--M', dest='M', default=0, type='int',
            help='M: corpus size (default 0)')
        self.add_passthrough_option(
            '--Test', dest='T', default=0, type='int',
            help='Test: run unit test (default 0)')

    
    def mapper_init(self):
        # read vocabulary         
        path = 'Bernoulli_EM_Unit_Test_header.csv' if self.options.T else 'topUsers_Apr-Jul_2014_1000-words_summaries.txt'
        with open(path) as f:        
            header = f.readline()
        self.corpus = [w.strip('"') for w in header.strip().split(',')][-self.options.M:]        
        # put row ID's that were chosen as seeds here, list index is the category        
        self.seed_id = ['6 sweet chocolate', '7 sweet sugar'] if self.options.T else ['343538409', '608474726', '183322216', '1364735064']
        
        
    def mapper(self, _, line):
        if self.options.T:
            doc_id, fea = line.strip().split(',', 1)
        else:
            doc_id, label, tot, fea = line.strip().split(',', 3)
        
        # if this row is selected as seed
        if doc_id in self.seed_id:
            c_id = self.seed_id.index(doc_id)
            rnk = [0]*(self.options.K)
            rnk[c_id] = 1
            tf = map(int, fea.split(','))
            # emit for alpha_k 
            for k in range(self.options.K):
                yield (k, '*'), (1, rnk[k])
            # emit for q_mk
            for word, freq in zip(self.corpus, tf):
                for k in range(self.options.K):                    
                    yield (k, word), (freq > 0, rnk[k])
            
    def reducer_init(self):
        self.alpha_k = [0]*self.options.K        
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
            self.q_mk[m] = [0]*self.options.K
        
        sum_r = sum_r_I = 0
        for Itm, rnk in value:
            sum_r += rnk
            sum_r_I += rnk*Itm
            
        self.q_mk[m][k] = 1.0*sum_r_I/sum_r
        
    def reducer_final(self):
        # save initial parameters
        param = {'q':self.q_mk, 'alpha':self.alpha_k}
        path = '/Users/leiyang/GitHub/mids/w261/HW6-Questions/parameters'
        with open(path, 'w') as f:
            f.write(dumps(param))
        
    def steps(self):
        # initial scan of the data - no big deal
        jc = {  
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '1',
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
    BernoulliMixEmInit.run()