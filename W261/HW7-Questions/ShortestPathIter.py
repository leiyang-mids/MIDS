from mrjob.job import MRJob
from mrjob.step import MRStep
from json import dumps


class ShortestPathIter(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(ShortestPathIter, self).__init__(*args, **kwargs)
        
                                                 
    def configure_options(self):
        super(ShortestPathIter, self).configure_options()
        self.add_passthrough_option(
            '--source', dest='source', default='1', type='string',
            help='source: source node (default 1)')        

    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        cmd = 'node = %s' %dic
        exec cmd        
        # if the node structure is incomplete (first pass), add them                
        if 'dist' not in node:            
            node = {'adj':node, 'path':[]}            
            node['dist'] = 0 if self.options.source==nid else -1            
        # emit node
        yield nid, node        
        # emit distances to reachable nodes
        if node['dist'] >= 0:
            for m in node['adj']:                
                yield m, {'dd':node['adj'][m] + node['dist'], 'pp':node['path']+[nid]}
                
    def reducer(self, nid, value):
        dmin = float('inf')
        path = node = None
        # loop through all arrivals
        for v in value:            
            if 'dist' in v:
                node = v
            elif v['dd'] < dmin:
                dmin = v['dd']
                path = v['pp']
        # handle dangling node
        if not node:
            node = {'adj':{}, 'dist':dmin, 'path':path}
        # update distance and path
        if (node['dist'] == -1 and path) or dmin < node['dist']:
            node['dist'] = dmin
            node['path'] = path
        # emit for next iteration
        yield nid, node
        
    def steps(self):
        return [MRStep(mapper=self.mapper
                       , combiner=self.reducer
                       , reducer=self.reducer
                       #, reducer_final=self.reducer_final
                       #, jobconf = jc
                      )
               ]

if __name__ == '__main__':
    ShortestPathIter.run()