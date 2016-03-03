#!/usr/bin/python
from UnweightedShortestPathIter import UnweightedShortestPathIter
from isDestinationReached import isDestinationReached
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
init_job = UnweightedShortestPathIter(args=[graph, '--source', source, '--destination', destination,
                                            '-r', mode, '--output-dir', 'hdfs:///user/leiyang/wiki_out'])

iter_job = UnweightedShortestPathIter(args=['hdfs:///user/leiyang/wiki_in/part*', '--source', source, '--destination',
                                            destination, '-r', mode, '--output-dir', 'hdfs:///user/leiyang/wiki_out'])

stop_job = isDestinationReached(args=['hdfs:///user/leiyang/wiki_out/part*', '--source', source, '--destination',
                                            destination, '-r', mode])

# run initialization job
with init_job.make_runner() as runner:
    print str(datetime.datetime.now()) + ': starting initialization job ...'
    runner.run()
# move the result to input folder
print str(datetime.datetime.now()) + ': moving results for next initialization ...'
call(['hdfs', 'dfs', '-mv', '/user/leiyang/wiki_out', '/user/leiyang/wiki_in'])


# run BFS iteratively
i = 1
while(1):
    stop = False
    with iter_job.make_runner() as runner:
        print str(datetime.datetime.now()) + ': running iteration %d ...' %i
        runner.run()
    # check if destination is reached
    print str(datetime.datetime.now()) + ': checking if destination is reached ...'
    with stop_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            text, path = stop_job.parse_output_line(line)
            if text == 'destination reached':
                stop = True
                result = '\nshortest path: %s' %(' -> '.join(path+[destination]))
                break

    if stop:
        break
    # more iteration needed
    i += 1
    print str(datetime.datetime.now()) + ': moving results for next initialization ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/wiki_in'])
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/wiki_out', '/user/leiyang/wiki_in'])

# clear results
print str(datetime.datetime.now()) + ': destination reached, clearing files ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/wiki*'])

print str(datetime.datetime.now()) + ": traversing completes!"
print result
