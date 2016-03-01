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
        # emit in degree from adjacency list
        for n in adj:
            yield n, (0, 1)
                        
    def combiner(self, nid, deg):
        din = dout = 0
        for d in deg:            
            dout += d[0]
            din += d[1]            
        yield nid, (dout, din)
        
    def reducer_init(self):
        self.node_cnt = 0
        self.out_deg = {}
        self.in_deg = {}
        
    def reducer(self, node, deg):
        # add node by 1
        self.node_cnt += 1
        # aggregate in/out degree
        _out = _in = 0
        for d in deg:            
            _out += d[0]
            _in += d[1]    
        # accumulate degree distribution
        if _out not in self.out_deg:
            self.out_deg[_out] = 1
        else:
            self.out_deg[_out] += 1
        if _in not in self.in_deg:
            self.in_deg[_in] = 1
        else:
            self.in_deg[_in] += 1
        
            
    def reducer_final(self):
        # final aggregation        
        tot_degree = sum([d*self.out_deg[d] for d in self.out_deg])
        yield 'total nodes: ', self.node_cnt
        yield 'total links: ', tot_degree
        yield 'average in degree: ', 1.0*tot_degree/sum(self.in_deg.values())
        yield 'average out degree: ', 1.0*tot_degree/sum(self.out_deg.values())
        yield 'dist of in-degree: ', self.in_deg
        yield 'dist of out-degree: ', self.out_deg
        
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
                       #, jobconf = jc
                      )
               ]

if __name__ == '__main__':
    ExploreGraph.run()