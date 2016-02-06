#!/usr/bin/python
from numpy import random
from Kmeans import MRKmeans, stop_criterion
mr_job = MRKmeans(args=['Kmeandata.csv', '--file', 'Centroids.txt',
                        '--hadoop-home', '/usr/local/Cellar/hadoop/2.7.1/', '-r', 'hadoop'])

#Geneate initial centroids
k = 3
centroid_points = [[random.uniform(-3,3),random.uniform(-3,3)] for i in range(k)]
with open('Centroids.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)

# Update centroids iteratively
i = 1
while(1):
    # save previous centoids to check convergency
    centroid_points_old = centroid_points[:]
    print "iteration "+str(i)+":"
    with mr_job.make_runner() as runner:
        runner.run()
        # stream_output: get access of the output
        for line in runner.stream_output():
            key,value =  mr_job.parse_output_line(line)
            print key, value
            centroid_points[key] = value
    print "\n"
    i = i + 1
    if(stop_criterion(centroid_points_old,centroid_points,0.01)):
        break

    # put the latest centroids in the file for next hadoop job
    # it is important for the clusters to have ALL new centroids
    os.remove('Centroids.txt')
    with open('Centroids.txt', 'w+') as f:
        f.writelines(','.join(str(j) for j in i) + '\n' for i in centroid_points)

print "Centroids\n"
print centroid_points
