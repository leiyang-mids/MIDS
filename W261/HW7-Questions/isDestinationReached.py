from mrjob.job import MRJob
from mrjob.step import MRStep


class isDestinationReached(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(isDestinationReached, self).__init__(*args, **kwargs)

    def configure_options(self):
        super(isDestinationReached, self).configure_options()
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
            if node['dist'] > 0:
                self.path = node['path']+[self.options.destination]

    def mapper_final(self):
        yield 1 if self.path else 0, self.path

    def steps(self):
        jc = {
            'mapreduce.job.maps': '20',
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , mapper_final=self.mapper_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    isDestinationReached.run()
