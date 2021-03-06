#!/usr/bin/python
from ShortestPathIter import ShortestPathIter
from isTraverseCompleted import isTraverseCompleted
from isDestinationReached import isDestinationReached
from LongestPathIter import LongestPathIter
from getLongestDistance import getLongestDistance
from getPath import getPath
from subprocess import call, check_output
from time import time
import sys, getopt, datetime, os

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:s:d:m:w:i:l:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode> -w <weighted>'
        sys.exit(2)
    if len(opts) != 7:
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
        elif opt == '-i':
            index = arg
        elif opt == '-l':
            longest = arg

start = time()
FNULL = open(os.devnull, 'w')

isWeighted = weighted=='y'
isLongest = longest=='y'

if isLongest:
    print str(datetime.datetime.now()) + ': Longest distance BFS started at node %s on %s graph %s ...' %(
        source, 'weighted' if isWeighted else 'unweighted', graph[graph.rfind('/')+1:])
else:
    print str(datetime.datetime.now()) + ': Shortest path BFS started between node %s and node %s on %s graph %s ...' %(
        source, destination, 'weighted' if isWeighted else 'unweighted', graph[graph.rfind('/')+1:])

# creat BFS job
if not isLongest:
    init_job = ShortestPathIter(args=[graph, '--source', source, '--destination', destination, '--weighted', weighted,
                                  '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

    iter_job = ShortestPathIter(args=['hdfs:///user/leiyang/in/part*', '--source', source, '--destination', destination,
                                  '--weighted', weighted, '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])
else:
    init_job = LongestPathIter(args=[graph, '--source', source, '--weighted', weighted,
                                  '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

    iter_job = LongestPathIter(args=['hdfs:///user/leiyang/in/part*', '--source', source, 
                                  '--weighted', weighted, '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])
    
if isLongest:
    path_job = getLongestDistance(args=['hdfs:///user/leiyang/out/part*', '-r', mode])
else:        
    path_job = getPath(args=['hdfs:///user/leiyang/out/part*', '--destination', destination, '-r', mode])

if isWeighted or isLongest:
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
i, path = 1, []
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
    #print 'output value: %s' %str(output)
    if (isWeighted or isLongest) and flag==0:
        print str(datetime.datetime.now()) + ': traverse has completed, retrieving path ...'        
        with path_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                text, ppp = path_job.parse_output_line(line)   
                if len(ppp) > len(path):
                    path = ppp                
        break
    elif (not isWeighted) and flag==1 and (not isLongest):
        print str(datetime.datetime.now()) + ': destination is reached, retrieving path ...'                
        for x,path in output:
            if x==1:                    
                break
        break
    
    # if more iteration needed
    i += 1
    if isWeighted or isLongest:
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

# translate into words if index is valid
if os.path.isfile(index):    
    path = [check_output(['grep', x, index]).split('\t')[0] for x in path]

print str(datetime.datetime.now()) + ": traversing completes in %.1f minutes!\n" %((time()-start)/60.0)
print str(datetime.datetime.now()) + ': %s path: %s\n'%('longest' if isLongest else 'shortest', ' -> '.join(path))