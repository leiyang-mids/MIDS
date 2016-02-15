from mrjob.job import MRJob
from mrjob.step import MRStep

class MostLeastDenseWords(MRJob):

    # stream through lines, yield word count
    def mapper(self, _, line):
        # get page id
        n_gram, cnt, p_cnt, b_cnt = line.strip().split('\t')
        cnt, p_cnt = int(cnt), int(p_cnt)
        for w in n_gram.lower().split(' '):
            yield w, (cnt, p_cnt)
            
    # combiner
    def combiner(self, word, counts):
        cnt = p_cnt = 0
        for c in counts:
            cnt += c[0]
            p_cnt += c[1]
        yield word, (cnt, p_cnt)
   
    # sum word counts, use as combiner too
    def reducer(self, word, counts):
        cnt = p_cnt = 0
        for c in counts:
            cnt += c[0]
            p_cnt += c[1]
        yield word, 1.0*cnt/p_cnt
        
    # job to sort the results ###########################
    def mapper_sort1(self, word, ratio):
        yield (word, ratio), None
        
    def reducer_sort_init1(self):
        self.top = 200
        self.n = 0
        
    def reducer_sort1(self, results, dummy):        
        if self.n < self.top:
            self.n += 1
            yield results

    def steps(self):
        jobconf1 = {  #key value pairs 
            'mapreduce.job.maps': '3',
            'mapreduce.job.reduces': '3',
        }
        
        jobconf2 = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr -k1,1',
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',            
        }

        return [MRStep(mapper=self.mapper                       
                       ,combiner=self.combiner
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
    MostLeastDenseWords.run()
