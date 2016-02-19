
from mrjob.job import MRJob
from mrjob.step import MRStep

class Get10kWords(MRJob):

    # stream through lines, yield word count
    def mapper(self, _, line):
        # get page id
        n_gram, cnt, p_cnt, b_cnt = line.strip().split('\t')
        cnt = int(cnt)
        for w in n_gram.lower().split(' '):
            yield w, cnt

    # sum word counts, use as combiner too
    def reducer(self, word, count):
        yield word, sum(count)

    # job to sort the results ###########################
    def mapper_sort1(self, word, count):
        yield (word, count), None

    def reducer_sort_init1(self):
        self.cut = 1000
        self.total = 10000
        self.n = 0

    def reducer_sort1(self, results, dummy):
        self.n += 1
        if self.n > self.cut and self.n <= self.total:            
            yield results

    def steps(self):
        jobconf1 = {  #key value pairs
            'mapreduce.job.maps': '20',
            'mapreduce.job.reduces': '20',
        }

        jobconf2 = {  #key value pairs
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '15',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }

        return [MRStep(mapper=self.mapper
                       ,combiner=self.reducer
                       ,reducer=self.reducer
                       ,jobconf=jobconf1
                       )
                ,MRStep(mapper=self.mapper_sort1
                       ,reducer_init=self.reducer_sort_init1
                       ,reducer=self.reducer_sort1
                       ,jobconf=jobconf2
                       )
               ]


if __name__ == '__main__':
    Get10kWords.run()