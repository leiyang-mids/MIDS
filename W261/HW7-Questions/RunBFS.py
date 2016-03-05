#!/usr/bin/python
from ShortestPathIter import ShortestPathIter
from getPath import getPath
from isTraverseCompleted import isTraverseCompleted
from isDestinationReached import isDestinationReached
from subprocess import call
from time import time
import sys, getopt, datetime, os

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:s:d:m:w:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode> -w <weighted>'
        sys.exit(2)
    if len(opts) != 5:
        print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode> -w <weighted>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode> -w <weighted>'
            sys.exit(2)
        elif opt == '-g':
            graph = arg
        elif opt == '-s':
            source = arg
        elif opt == '-d':
            destination = arg
        elif opt == '-m':
            mode = arg
        elif opt == '-w':
            weighted = arg

start = time()
FNULL = open(os.devnull, 'w')

isWeighted = weighted=='1'
print str(datetime.datetime.now()) + ': BFS started between node %s and node %s on %s graph %s ...' %(
    source, destination, 'weighted' if isWeighted else 'unweighted', graph[graph.rfind('/')+1:])

# creat BFS job
init_job = ShortestPathIter(args=[graph, '--source', source, '--destination', destination, '--weighted', weighted,
                                  '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

iter_job = ShortestPathIter(args=['hdfs:///user/leiyang/in/part*', '--source', source, '--destination', destination,
                                  '--weighted', weighted, '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

path_job = getPath(args=['hdfs:///user/leiyang/out/part*', '--destination', destination, '-r', mode])

if isWeighted:
    stop_job = isTraverseCompleted(args=['hdfs:///user/leiyang/out/part*', 'hdfs:///user/leiyang/in/part*', '-r', mode])
else:
    stop_job = isDestinationReached(args=['hdfs:///user/leiyang/out/part*', '--destination', destination, '-r', mode])

# run initialization job
with init_job.make_runner() as runner:
    print str(datetime.datetime.now()) + ': starting initialization job ...'
    runner.run()
# move the result to input folder
print str(datetime.datetime.now()) + ': moving results for next iteration ...'
call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])

# run BFS iteratively
i = 1
while(1):    
    with iter_job.make_runner() as runner:
        print str(datetime.datetime.now()) + ': running iteration %d ...' %i
        runner.run()
        
    # check if traverse is completed: no node has changing distance
    with stop_job.make_runner() as runner:
        print str(datetime.datetime.now()) + ': checking stopping criterion ...'
        runner.run()
        output = []        
        for line in runner.stream_output():
            n, text = stop_job.parse_output_line(line)
            output.append([n, text])
            
    # if traverse completed, get path and break out
    flag = sum([x[0] for x in output])
    if isWeighted and flag==0:
        print str(datetime.datetime.now()) + ': traverse has completed, retrieving path ...'
        with path_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                text, path = path_job.parse_output_line(line)                
                result = ': shortest path: %s' %(' -> '.join(path))                
        break
    elif (not isWeighted) and flag==1:
        print str(datetime.datetime.now()) + ': destination is reached, retrieving path ...'        
        for x,path in output:
            if x==1:
                result = ': shortest path: %s'%(' -> '.join(path))
                break
        break

    # more iteration needed
    i += 1
    if isWeighted:
        print str(datetime.datetime.now()) + ': %d nodes changed distance' %flag
    else:
        print str(datetime.datetime.now()) + ': destination not reached yet.'
    print str(datetime.datetime.now()) + ': moving results for next iteration ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])

# clear results
print str(datetime.datetime.now()) + ': clearing files ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'], stdout=FNULL)

print str(datetime.datetime.now()) + ": traversing completes in .%1f minutes!\n" %((time()-start)/60.0)
print str(datetime.datetime.now()) + result + '\n'