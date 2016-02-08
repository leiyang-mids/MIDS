
import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import chain
import subprocess

#Calculate find the nearest centroid for data point
def MinDist(datapoint, centroid_points):
    datapoint = np.array(datapoint)
    centroid_points = np.array(centroid_points)
    diff = datapoint - centroid_points
    diffsq = diff*diff
    # Get the nearest centroid for each instance
    minidx = np.argmin(list(diffsq.sum(axis = 1)))
    return minidx

#Check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new,T):
    oldvalue = list(chain(*centroid_points_old))
    newvalue = list(chain(*centroid_points_new))
    Diff = [abs(x-y) for x, y in zip(oldvalue, newvalue)]
    Flag = True
    for i in Diff:
        if(i>T):
            Flag = False
            break
    return Flag

class MRKmeans(MRJob):
    centroid_points=[]
    k, n = 3, 2

    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, mapper=self.mapper, combiner = self.combiner, reducer=self.reducer)
               ]
    #load centroids info from file
    def mapper_init(self):
        hadoopPath = 'Centroids.txt'
        localPath = '/Users/leiyang/GitHub/mids/w261/HW4-Questions/ref/Centroids.txt'
        cat = subprocess.Popen(["cat", localPath], stdout=subprocess.PIPE)
        self.centroid_points = [map(float, s.strip().split(',')) for s in cat.stdout]
        
    #load data and output the nearest centroid index and data point
    def mapper(self, _, line):
        D = (map(float,line.split(',')))
        yield int(MinDist(D,self.centroid_points)), (D,1)

    #Combine sum of data points locally
    def combiner(self, idx, inputdata):
        num = 0
        sum_d = [0]*self.n        
        # iterate through the generator
        for d,c in inputdata:
            num += c                
            sum_d = [a+b for a,b in zip(sum_d, d)]
        yield idx, (sum_d, num)

    #Aggregate sum for each cluster and then calculate the new centroids
    def reducer(self, idx, inputdata):
        num = [0]*self.k
        centroids = [[0]*self.n for i in range(self.k)]
        # iterate through the generator
        for d, c in inputdata:
            num[idx] += c
            centroids[idx] = [a+b for a,b in zip(centroids[idx], d)]          
        # recalculate centroids
        centroids[idx] = [a/num[idx] for a in centroids[idx]]
        yield idx, (centroids[idx])

if __name__ == '__main__':
    MRKmeans.run()
