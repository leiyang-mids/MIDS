from mrjob.job import MRJob
from mrjob.step import MRStep

class ExploreGraph(MRJob):
    
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(ExploreGraph, self).__init__(*args, **kwargs)
        
                                                 
    def configure_options(self):
        super(ExploreGraph, self).configure_options()
        self.add_passthrough_option(
            '--source', dest='source', default='1', type='string',
            help='source: source node (default 1)')        

    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        cmd = 'adj = %s' %dic
        exec cmd        
        # let's emit the node degree as key, this way we can do some aggregation
        # value: number of nodes
        yield len(adj), 1
                        
    def combiner(self, degree, cnt):
        yield degree, sum(cnt)
        
    def reducer_init(self):
        self.degree_cnt = {}
        
    def reducer(self, degree, cnt):
        if degree not in self.degree_cnt:
            self.degree_cnt[degree] = sum(cnt)
        else:
            self.degree_cnt[degree] += sum(cnt)
            
    def reducer_final(self):
        # final aggregation
        tot_node = sum(self.degree_cnt.values())
        tot_degree = sum([d*self.degree_cnt[d] for d in self.degree_cnt])
        yield 'total nodes: ', tot_node
        yield 'total links: ', tot_degree/2
        yield 'average degree: ', 1.0*tot_degree/tot_node
        yield 'degree counts: ', self.degree_cnt        
        
    def steps(self):
        jc = {
            'mapreduce.job.maps': '10',
            'mapreduce.job.reduces': '1',
        }
        return [MRStep(mapper=self.mapper
                       , combiner=self.combiner
                       , reducer_init=self.reducer_init
                       , reducer=self.reducer                       
                       , reducer_final=self.reducer_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    ExploreGraph.run()