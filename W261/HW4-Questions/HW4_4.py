from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitor(MRJob):
    
    # visitor ID and url
    url = visitorID = None
        
    # mapper
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
            
    
    # reducer to get count for each visitor on each page
    def count_reducer(self, key, value):
        
        temp = key.strip().split('_')
        # save webpage url for the following visisting records
        if temp[2] == '*':
            self.url = temp[3]
        else:
            yield key+'_'+self.url, sum(value)
        
    # MapReduce steps
    def steps(self):
        custom_jobconf = {  #key value pairs                        
            'mapreduce.job.maps': '1',
            'mapreduce.job.reduces': '1'            
        }
        return [MRStep(mapper=self.convert_mapper, reducer=self.count_reducer, jobconf=custom_jobconf)]

    
if __name__ == '__main__':
    FreqVisitor.run()