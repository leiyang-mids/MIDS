from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitor(MRJob):
    
    # member variables: visitor ID and url
    url = visitorID = None
    current_page = None
    current_max = 0
    
    # 1. mapper
    def convert_mapper(self, _, line):        
        # time of mapper being called
        self.increment_counter('HW4_2', 'lines', 1)
        # only emit lines start with C and V
        line = line.strip()
        if line[0] not in ['C', 'V', 'A']:
            return
        temp = line.split(',')
        # process A, C, and V lines
        if line[0] == 'C':            
            # get the latest visitor ID
            self.visitorID = temp[2]  
        elif line[0] == 'A':
            # emit V_pageID_*_url as key, dummy 1 as value
            yield 'V_%s_*_%s' %(temp[1], temp[4].strip('"')), 1
        else:
            # emit V_pageID_C_visitorID as key, 1 as value
            yield 'V_%s_C_%s' %(temp[1], self.visitorID), 1
     
    # 2. reducer to get count for each visitor on each page
    def count_reducer(self, key, value):     
        temp = key.strip().split('_')
        # save webpage url for the following visisting records
        if temp[2] == '*':
            self.url = temp[3]
        else:
            yield key+'_'+self.url, sum(value)
            
    # 3. mapper for sorting: partition by page id, secondary sorting/ranking by count
    def rank_mapper(self, key, count):   
        v, pID, c, cID, url = key.strip().split('_')
        # dimension of key and value will differ
        yield 'V_%s\t%s' %(pID, count), 'C_%s\t%s' %(cID, url)
    
    # 4. reducer get max vistor of each page
    def rank_reducer(self, key, value):
        # final result
        for v in value:
            print '%s\t%s' %(key, v)
           
    
    # 0. MapReduce steps
    def steps(self):
        count_jobconf = {  #key value pairs                        
            'mapreduce.job.maps': '1',
            'mapreduce.job.reduces': '1', # must only use 1 reducer to have the proper order
        }
        
        rank_jobconf = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1r -k2,2nr',
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': '\t',
            'stream.map.output.field.separator': ' ',            
        }
        return [MRStep(mapper=self.convert_mapper, reducer=self.count_reducer, jobconf=count_jobconf)
                ,MRStep(mapper=self.rank_mapper, reducer=self.rank_reducer, jobconf=rank_jobconf)
               ]
        
    
if __name__ == '__main__':
    FreqVisitor.run()