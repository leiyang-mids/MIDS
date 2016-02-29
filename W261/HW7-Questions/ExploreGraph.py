from mrjob.job import MRJob
from mrjob.step import MRStep

class ExploreGraph(MRJob):
    
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(ExploreGraph, self).__init__(*args, **kwargs)

    # assuming we are dealing with directed graph
    # this job can handle undirected graph as well, 
    # but it can be done more efficiently
    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        cmd = 'adj = %s' %dic
        exec cmd        
        # we need to emit node ID as key, in order to count dangling nodes
        # the value emitted here is (out, in) degree for the node
        yield nid, (len(adj), 0)
        # emit in degree
        for n in adj:
            yield n, (0, 1)
                        
    def combiner(self, nid, deg):
        din = dout = 0
        for n_out, n_in in deg[0], deg[1]:
            din += n_in
            dout += n_out
        yield nid, sum(dout, din)
        
    def reducer_init(self):
        self.node_cnt = 0
        self.total_deg = 0
        
    def reducer(self, node, degree):
        self.node_cnt += 1
        self.total_deg += sum(degree)
        yield node, tot
            
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
