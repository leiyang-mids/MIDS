
#import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import chain
from math import pow

class MRCanopy(MRJob):
    centroids = {}
    # TODO: figure out why set from mapper_init doesn't preserve value
    k, n = 2, 1000
    T1, T2 = 0.001, 0.0001

    def steps(self):
        return [
            # job 1
            MRStep(mapper=self.mapper1
                   #,combiner = self.combiner1
                   #,reducer=self.reducer1
                  )
            # job 2
               ]

    # Load data and output each point with the canopy it belongs to or defines
    def mapper1(self, _, line):            
        D = [map(float, line.strip().split(','))]        
        name, point = str(D[0]), D[1:]
        yield name, (point, 1)
        if len(self.centroids) == 0:
            # first point, add canopy
            self.centroids[name] = point        
            print 'first'
            yield name, (point, 1)
        else:
            # compare with each centroid to determine status
            isNew = True
            for cen in self.centroids:
                # calculate distance
                dist = sum([pow(a-b, 2) for a,b in zip(self.centroids[cen], point)])
                # inside inner circle of cen
                if self.T2 > dist:                    
                    isNew = False
                    print 'inner'
                    yield cen, (point, 1)
                # in between circles of cen
                elif self.T2 < dist and dist < self.T1:                    
                    print 'close'
                    yield cen, (point, 1)
            # not in anyone's inner circle, add new canopy
            if isNew:                
                self.centroids[name] = point
                print 'new'
                yield name, (point, 1)
                

    # Combine sum of data points locally
    def combiner1(self, name, inputdata):
        num = 0
        sum_d = [0]*self.n
        # iterate through the generator
        for d,c in inputdata:
            num += c
            sum_d = [a+b for a,b in zip(sum_d, d)]
        yield name, (sum_d, num)

    # Aggregate sum for each canopy, and then calculate the new centroids
    def reducer1(self, name, inputdata):
        num = 0 #[0]*self.k
        centroid = [0]*self.n
        current_canopy = None
        if current_canopy != name:
            # aggregation completes for previous canopy, emit
            centroid = [a/num for a in centroid]
            yield current_canopy, centroid
            # reset for new canopy
            current_canopy = name
            num = 0
            centroid = [0]*self.n
        # iterate through the generator
        for d, c in inputdata:
            num += c
            centroid = [a+b for a,b in zip(centroid, d)]


if __name__ == '__main__':
    MRCanopy.run()