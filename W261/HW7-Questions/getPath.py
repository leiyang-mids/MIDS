from mrjob.job import MRJob
from mrjob.step import MRStep


class getPath(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(getPath, self).__init__(*args, **kwargs)

    def configure_options(self):
        super(getPath, self).configure_options()        
        self.add_passthrough_option(
            '--destination', dest='destination', default='1', type='string',
            help='destination: destination node (default 1)')

    def mapper_init(self):
        self.path = None

    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)        
        # emit distances to reachable nodes
        if nid.strip('"') == self.options.destination:
            cmd = 'node = %s' %dic
            exec cmd
            self.path = node['path']            

    def mapper_final(self):
        if self.path:
            yield "shortest path - ", self.path+[self.options.destination]

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , mapper_final=self.mapper_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    getPath.run()