from mrjob.job import MRJob
from mrjob.step import MRStep

class Longest5Gram(MRJob):

    # stream through lines, yield char count
    def mapper(self, _, line):
        # get page id
        n_gram, cnt, p_cnt, b_cnt = line.strip().split('\t')
        yield n_gram, len(n_gram)

    def reducer_init(self):
        self.length = 0
        self.longest = None

    def reducer(self, n_gram, n_char):
        cnt = sum(n_char)
        if cnt > self.length:
            self.longest = n_gram
            self.length = cnt

    def reducer_final(self):
        yield self.longest, self.length

    def steps(self):
        jobconf = {
            'mapreduce.job.maps': '30',
            'mapreduce.job.reduces': '1',
        }

        return [MRStep(mapper=self.mapper
                       ,combiner_init=self.reducer_init
                       ,combiner=self.reducer
                       ,combiner_final=self.reducer_final
                       ,reducer_init=self.reducer_init
                       ,reducer=self.reducer
                       ,reducer_final=self.reducer_final
                       ,jobconf=jobconf
                       )                
               ]


if __name__ == '__main__':
    Longest5Gram.run()