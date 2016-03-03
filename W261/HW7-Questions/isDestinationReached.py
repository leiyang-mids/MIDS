from mrjob.job import MRJob
from mrjob.step import MRStep


class isDestinationReached(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(isDestinationReached, self).__init__(*args, **kwargs)

    def configure_options(self):
        super(isDestinationReached, self).configure_options()
        self.add_passthrough_option(
            '--source', dest='source', default='1', type='string',
            help='source: source node (default 1)')
        self.add_passthrough_option(
            '--destination', dest='destination', default='1', type='string',
            help='destination: destination node (default 1)')

    def mapper_init(self):
        self.reached = False
        self.path = None

    def mapper(self, _, line):
        if self.reached:
            return
        nid, dic = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %dic
        exec cmd
        # emit distances to reachable nodes
        if nid == self.options.destination and node['dist'] > 0:
            self.reached = True
            self.path = node['path']

    def mapper_final(self):
        yield "destination reached" if self.reached else "keep working", self.path

    def steps(self):
        jc = {
            'mapreduce.job.maps': '16',
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , mapper_final=self.mapper_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    isDestinationReached.run()
