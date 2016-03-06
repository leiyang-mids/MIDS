from mrjob.job import MRJob
from mrjob.step import MRStep


class getLongestDistance(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def __init__(self, *args, **kwargs):
        super(getLongestDistance, self).__init__(*args, **kwargs)


    def mapper_init(self):
        self.longest = []

    def mapper(self, _, line):
        nid, dic = line.strip().split('\t', 1)        
        cmd = 'node = %s' %dic
        exec cmd
        if len(node['path']) > len(self.longest):
            self.longest = node['path']+[nid.strip('"')]  

    def mapper_final(self):        
        yield "longest distance - ", self.longest

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
    getLongestDistance.run()
