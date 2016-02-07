from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitPage(MRJob):
    n_freq, i = 5, 0
        
    def mapper_count(self, dummy, line): 
        self.increment_counter('HW4_3','page_map',1)
        # get page id
        pID = line.strip().split(',')[1]
        yield pID.strip(), 1
        
    def reducer_count(self, page, count):
        self.increment_counter('HW4_3','page_count',1)
        yield page, sum(count)
        
    def mapper_sort(self, page, count):        
        yield (page, count), None
        
    def reducer_sort(self, key, _):
        self.increment_counter('HW4_3','page_sort',1)
        if self.i < self.n_freq:
            self.i += 1
            yield key        
        
    def steps(self):             
        sort_jobconf = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        
        count_jobconf = {
            'mapreduce.job.maps': '3',
            'mapreduce.job.reduces': '2',
        }
        
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count, jobconf=count_jobconf)
                ,MRStep(mapper=self.mapper_sort, reducer=self.reducer_sort, jobconf=sort_jobconf)
               ]
    

if __name__ == '__main__':
    FreqVisitPage.run()
    