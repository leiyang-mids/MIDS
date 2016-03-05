from mrjob.job import MRJob
from mrjob.step import MRStep

class ShortestPathIter(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(ShortestPathIter, self).__init__(*args, **kwargs)


    def configure_options(self):
        super(ShortestPathIter, self).configure_options()
        self.add_passthrough_option(
            '--source', dest='source', default='1', type='string',
            help='source: source node (default 1)')
        self.add_passthrough_option(
            '--destination', dest='destination', default='1', type='string',
            help='destination: destination node (default 1)')
        self.add_passthrough_option(
            '--weighted', dest='weighted', default='1', type='string',
            help='weighted: is weighted graph (default 1)')
    

    def mapper_weighted(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        nid = nid.strip('"')
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
                
    def mapper_unweighted(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %dic
        exec cmd
        # if the node structure is incomplete (first pass), add them
        if 'dist' not in node:
            node = {'adj':node.keys(), 'path':[]}
            node['dist'] = 0 if self.options.source==nid else -1
        # emit node
        yield nid, node
        # emit distances to reachable nodes
        if node['dist'] >= 0:
            for m in node['adj']:
                yield m, {'dd':(1+node['dist']), 'pp':(node['path']+[nid])}
                
    # write a separate combiner ensure the integrity of the graph topology
    # no additional node object will be generated
    def combiner(self, nid, value):
        dmin = float('inf')
        path = None
        # loop through all arrivals
        for v in value:            
            if 'dist' in v:
                yield nid, v           
            elif v['dd'] < dmin:
                dmin, path = v['dd'], v['pp']       
        # emit the smallest distance for nid        
        yield nid, {'dd':dmin, 'pp':path}
    
    def reducer(self, nid, value):              
        dmin = float('inf')
        path = node = None
        # loop through all arrivals
        for v in value:            
            if 'dist' in v:
                node = v            
            elif v['dd'] < dmin:
                dmin, path = v['dd'], v['pp']                 

        # handle dangling node, we only care if it's destination
        if not node:
            if nid == self.options.destination:
                node = {'adj':{}, 'dist':dmin, 'path':path}                
            else:
                return
        elif (dmin < node['dist']) or (node['dist'] == -1 and path):
            node['dist'], node['path'] = dmin, path
            
        # emit for next iteration
        yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '2',
        }
        return [MRStep(mapper=self.mapper_weighted if self.options.weighted == '1' else self.mapper_unweighted
                       , combiner=self.combiner
                       , reducer=self.reducer
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    ShortestPathIter.run()