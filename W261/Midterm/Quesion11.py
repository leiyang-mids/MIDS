from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.step import MRJobStep
from itertools import chain
from math import sqrt

def MinDist(datapoint, centroid_points):
    datapoint = array(datapoint)
    centroid_points = array(centroid_points)
    diff = datapoint - centroid_points 
    diffsq = diff**2
    
    distances = (diffsq.sum(axis = 1))**0.5
    # Get the nearest centroid for each instance
    min_dist = min(distances)
    return min_dist

class Quesion11(MRJob):
        
    def steps(self):
        return [
            MRJobStep(mapper_init = self.mapper_init, mapper=self.mapper,
                      reducer_init=self.reducer_init,                      
                      reducer=self.reducer,
                      reducer_final=self.reducer_final)
               ]
    #load centroids info from file
    def mapper_init(self):
        self.centroid_points = [[-4.5,0.0], [4.5,0.0], [0.0,4.5]]
        
    #load data and output the nearest centroid index and data point 
    def mapper(self, _, line):
        D = (map(float,line.split(',')))
        ######################## let's weight the input ########################
        norm = sqrt(sum([x**2 for x in D]))
        D = [x/norm for x in D]
        
        w_dist = MinDist(D, self.centroid_points)/norm
        yield None, (w_dist, 1.0/norm)

    def reducer_init(self):
        self.sum_d = 0
        self.sum_w = 0
    
    def reducer(self, _, value):
        for v in value:
            d, w = v[0], v[1]
            self.sum_d += d
            self.sum_w += w
            
    def reducer_final(self):
        yield None, self.sum_d/self.sum_w
        

if __name__ == '__main__':
    Quesion11.run()