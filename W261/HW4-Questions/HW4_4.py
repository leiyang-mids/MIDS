from mrjob.job import MRJob
from mrjob.step import MRStep
 
class FreqVisitor(MRJob):
    
    def mapper_vistor(self, _, line):
        temp = line.strip().split(',')
        # emit page ID and visitor ID as key
        yield 'C_%s_V_%s' %(temp[4].strip('"'), temp[1]), 1
        
    def reducer_visitor(self, page, count):
        yield page, sum(count)
        
    def steps(self):
        return [MRStep(mapper=self.mapper_vistor, reducer=self.reducer_visitor)]

    
if __name__ == '__main__':
    FreqVisitor.run()