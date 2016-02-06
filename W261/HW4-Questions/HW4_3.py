from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitPage(MRJob):
    n_freq, i = 5, 0
        
    def mapper_count(self, dummy, line): 
        # get page id
        pID = line.strip().split(',')[1]
        yield pID, 1
        
    def reducer_count(self, page, count):
        yield page, sum(count)
        
    def reducer_sort(self, page, count):
        self.increment_counter('HW4_3','sort',1)
        if self.i < self.n_freq:
            self.i += 1
            yield page, sum(count)
        
    def steps(self):
        custom_jobconf = {  #key value pairs
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k2,2nr',
            'mapred.reduce.tasks': '1',
            'hadoop-home': '/usr/local/Cellar/hadoop/2.7.1/libexec/etc/hadoop'
        }
        return [MRStep(mapper=self.mapper_count, 
                       reducer=self.reducer_count),
                MRStep(reducer=self.reducer_sort, jobconf=custom_jobconf)
               ]
    

if __name__ == '__main__':
    FreqVisitPage.run()
    