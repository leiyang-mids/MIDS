#!/usr/bin/python
from ShortestPathIter import ShortestPathIter
from getPath import getPath
from subprocess import call
import sys, getopt, datetime

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:s:d:m:")
    except getopt.GetoptError:
        print 'RunUnweightedBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
        sys.exit(2)
    if len(opts) != 4:
        print 'RunUnweightedBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'RunUnweightedBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
            sys.exit(2)
        elif opt == '-g':
            graph = arg
        elif opt == '-s':
            source = arg
        elif opt == '-d':
            destination = arg
        elif opt == '-m':
            mode = arg


# creat BFS job
init_job = ShortestPathIter(args=[graph, '--source', source, '--destination', destination,
                                            '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

iter_job = ShortestPathIter(args=['hdfs:///user/leiyang/in/part*', '--source', source, '--destination',
                                            destination, '-r', mode, '--output-dir', 'hdfs:///user/leiyang/out'])

path_job = getPath(args=['hdfs:///user/leiyang/out/part*', '--destination', destination, '-r', mode])

# run initialization job
with init_job.make_runner() as runner:
    print str(datetime.datetime.now()) + ': starting initialization job ...'
    runner.run()
    print runner.counters() #['weighted']['dist_changed']
# move the result to input folder
#print str(datetime.datetime.now()) + ': node number with distance change: ' + n_change
print str(datetime.datetime.now()) + ': moving results for next initialization ...'
call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])


# run BFS iteratively
i = 1
while(0):
    stop = False
    with iter_job.make_runner() as runner:
        print str(datetime.datetime.now()) + ': running iteration %d ...' %i
        runner.run()
        # check if destination is reached: no counter is called
        stop = 'unweighted' not in runner.counters()

    # if destination reached, get path and break out
    if stop:
        print str(datetime.datetime.now()) + ': destination is reached, retrieving path ...'
        with path_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                text, path = stop_job.parse_output_line(line)
                result = ': shortest path: %s' %(' -> '.join(path+[destination]))
        break

    # more iteration needed
    i += 1
    print str(datetime.datetime.now()) + ': moving results for next initialization ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'])
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])

# clear results
print str(datetime.datetime.now()) + ': clearing files ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'])
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'])

print str(datetime.datetime.now()) + ": traversing completes!"
print str(datetime.datetime.now()) + result