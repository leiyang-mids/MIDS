from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitPage(MRJob):
    n_freq, i = 5000000, 0
        
    def mapper_count(self, dummy, line): 
        self.increment_counter('HW4_3','page_map',1)
        # get page id
        pID = line.strip().split(',')[1]
        yield pID, 1
        
    def reducer_count(self, page, count):
        self.increment_counter('HW4_3','page_count',1)
        yield (page, sum(count)), None
        
    def mapper_sort(self, key, value):
        yield key, value
        
    def reducer_sort(self, key, _):
        self.increment_counter('HW4_3','page_sort',1)
        #if self.i < self.n_freq:
        #    self.i += 1
        #    yield page, sum(count)
        yield key
        
    def steps(self):
        orig_jobconf = super(FreqVisitPage, self).jobconf()        
        custom_jobconf = {  #key value pairs            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '1',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ',',
            'stream.map.output.field.separator': ','
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        # TODO: doesn work on hadoop with jobconf set, need to check
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count)
                ,MRStep(mapper=self.mapper_sort, reducer=self.reducer_sort, jobconf=combined_jobconf)
               ]
    

if __name__ == '__main__':
    FreqVisitPage.run()
    