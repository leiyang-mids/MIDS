#!/usr/bin/python

import numpy as np
import math
from HW4_5_Kmeans import MinDist

# load the centroids
centroids = np.genfromtxt('Centroids.txt', delimiter=',') 
compo = [[0,0,0,0] for i in range(len(centroids))]

# load the raw data
raw_data = np.genfromtxt('topUsers_Apr-Jul_2014_1000-words.txt', delimiter=',')

# find the cluster for each point, and record code count
for r in raw_data:
    code, point = r[1], r[3:]/r[2]
    compo[int(MinDist(point, centroids))][int(code)] += 1
    
# save results
#with open('Composition.txt', 'w+') as f:
#    f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
for i in range(len(compo)):
    print '\n'
    for j in range(4):
        print 'Centroid %d - code %d: %d (%.2f%%)' %(i, j, compo[i][j], 100.0*compo[i][j]/sum(compo[i]))