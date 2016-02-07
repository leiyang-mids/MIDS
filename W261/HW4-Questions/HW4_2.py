from mrjob.job import MRJob
from mrjob.step import MRStep
 
class ConvertLog(MRJob):
    # visitor name
    visitor = None
    # mapper
    def convert_mapper(self, _, line):        
        # time of mapper being called
        self.increment_counter('HW4_2', 'lines', 1)
        # only emit lines start with C and V
        line = line.strip()
        if line[0] not in ['C', 'V']:
            return
        # process C and V lines
        if line[0] == 'C':            
            # get the latest visitor suffix
            self.visitor = 'C,%s' %line.split(',')[2]            
        else:
            # emit the desire output, no need for key
            yield None, '%s,%s' %(line, self.visitor)
    
    # MapReduce steps
    def steps(self):
        return [MRStep(mapper=self.convert_mapper)]

if __name__ == '__main__':
    ConvertLog.run()