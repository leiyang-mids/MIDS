
from mrjob.job import MRJob
from numpy import sign

# This MrJob calculates the gradient of the entire training set 
#     Mapper: calculate partial gradient for each example  
#     
class WeightedOLSBatchGD(MRJob):
    # run before the mapper processes any input
    def read_weightsfile(self):
        # Read weights file
        with open('weights.txt', 'r') as f:
            self.weights = [float(v) for v in f.readline().split(',')]            
        # Initialze gradient for this iteration
        self.partial_Gradient = [0]*len(self.weights)
        self.partial_count = 0
    
    # Calculate partial gradient for each example 
    def partial_gradient(self, _, line):
        y,x = (map(float,line.split(',')))
        # y_hat is the predicted value given current weights
        y_hat = self.weights[0]+self.weights[1]*x
        # Update parial gradient vector with gradient form current example        
        self.partial_Gradient[0] += (y_hat-y)/abs(x)
        self.partial_Gradient[1] += (y_hat-y)*sign(x) # simplify from (y_hat-y)*x/abs(x)
        self.partial_count += 1
            
    # Finally emit in-memory partial gradient and partial count
    def partial_gradient_emit(self):
        yield None, (self.partial_Gradient,self.partial_count)
        
    # Accumulate partial gradient from mapper and emit total gradient 
    # Output: key = None, Value = gradient vector
    def gradient_accumulater(self, _, partial_Gradient_Record): 
        total_gradient = [0]*2
        total_count = 0
        for partial_Gradient,partial_count in partial_Gradient_Record:
            total_count += partial_count
            total_gradient[0] += partial_Gradient[0]
            total_gradient[1] += partial_Gradient[1]
        yield None, [v/total_count for v in total_gradient]
        #yield None, total_gradient
    
    def steps(self):
        jobconf = {
            'mapreduce.job.maps': '7',
            'mapreduce.job.reduces': '1',
        }
        return [self.mr(mapper_init=self.read_weightsfile,
                       mapper=self.partial_gradient,
                       mapper_final=self.partial_gradient_emit,
                       reducer=self.gradient_accumulater,
                       jobconf=jobconf)] 
    
if __name__ == '__main__':
    WeightedOLSBatchGD.run()