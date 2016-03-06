from mrjob.job import MRJob
from mrjob.step import MRStep


class isTraverseCompleted(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(isTraverseCompleted, self).__init__(*args, **kwargs)

    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)
        # emit node ID and distance
        cmd = 'node = %s' %dic
        exec cmd
        yield nid, node['dist']

    def reducer_init(self):
        self.dist_changed = 0

    def reducer(self, nid, dist):
        pair = [d for d in dist]
        self.dist_changed += pair[0]!=pair[1]

    def reducer_final(self):
        yield self.dist_changed, 'traverse done' if self.dist_changed==0 else 'keep working'

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '2',
        }
        return [MRStep(mapper=self.mapper
                       , reducer_init=self.reducer_init
                       , reducer=self.reducer
                       , reducer_final=self.reducer_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    isTraverseCompleted.run()
