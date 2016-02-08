#!/usr/bin/python
#%reload_ext autoreload
#%autoreload 2
import numpy as np
from HW4_5_Kmeans import MRKmeans, stop_criterion
import os

# create MrJob: specify input, available file, hadoop home
mr_job = MRKmeans(args=['HW4_5_train_data.csv', '--file', 'Centroids.txt',
                        '--hadoop-home', '/usr/local/Cellar/hadoop/2.7.1/', '-r', 'hadoop'])

# load initial centroids
centroid_points = np.genfromtxt('Centroids.txt', delimiter=',')

# Update centroids iteratively
i = 1
while(1):
    # save previous centoids to check convergency
    centroid_points_old = centroid_points[:]
    print "Iteration "+str(i)+" ..."
    with mr_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output         
        for line in runner.stream_output():
            key,value =  mr_job.parse_output_line(line)
            print key, value
            centroid_points[key] = value        
            
    print "\n"
    i += 1
    # put the latest centroids in the file for next iteration    
    os.remove('Centroids.txt')  
    with open('Centroids.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)
    
    # check stopping criterion
    if(stop_criterion(centroid_points_old,centroid_points,0.001)):
        break
        
print '\nK-means training completed after %d iterations!' %i