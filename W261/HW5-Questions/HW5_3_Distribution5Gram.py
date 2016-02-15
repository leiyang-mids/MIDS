from mrjob.job import MRJob
from mrjob.step import MRStep

class Distribution5Gram(MRJob):

    # stream through lines, yield word count
    def mapper(self, _, line):
        # get counts
        n_gram, cnt, p_cnt, b_cnt = line.strip().split('\t')
        cnt = int(cnt)
        yield '*', cnt
        yield n_gram, cnt
            
    # combiner
    def combiner(self, n_gram, count):        
        yield n_gram, sum(count)
        
    # reducer init
    def reducer_init(self):
        self.total = 0
        
    def reducer(self, n_gram, count):
        if n_gram == '*':
            self.total = sum(count)
        else:
            yield 'n', 1.0*sum(count)/self.total
        
    # job to sort the results ###########################
    def mapper_sort1(self, word, ratio):
        yield (word, ratio), None
        
        
    def reducer_sort1(self, results, dummy):               
        yield results

    def steps(self):
        jobconf1 = {  #key value pairs 
            'mapreduce.job.maps': '3',
            'mapreduce.job.reduces': '1',
        }
        
        jobconf2 = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '4',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',            
        }

        return [MRStep(mapper=self.mapper                       
                       ,combiner=self.combiner
                       ,reducer_init=self.reducer_init
                       ,reducer=self.reducer           
                       ,jobconf=jobconf1
                       )
                ,MRStep(mapper=self.mapper_sort1                
                       ,reducer=self.reducer_sort1
                       ,jobconf=jobconf2
                       )
               ]


if __name__ == '__main__':
    Distribution5Gram.run()
