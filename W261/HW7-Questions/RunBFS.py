#!/usr/bin/python
#%load_ext autoreload
#%autoreload 2
from ShortestPathIter import ShortestPathIter
import sys, getopt

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:s:d:m:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
        sys.exit(2)
    if len(opts) != 4:
        print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'RunBFS.py -g <graph> -s <source> -d <destination> -m <mode>'
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
init_job = ShortestPathIter(args=[graph, '--source', source, '-r', mode])
iter_job = ShortestPathIter(args=['graph', '--source', source, '-r', mode])

# run initialization job
with init_job.make_runner() as runner:
    runner.run()

# run BFS iteratively
i = 1
while(1):
    print 'iteration %s' %i
    stop = False
    with iter_job.make_runner() as runner:
        runner.run()
        # check if no distance has changed
        stop = runner.counters() == None # ['weighted']['dist_changed'] == 0

    # retrieve path if stop critierion is satisfied
    if stop:
        break

    # save dist for next iteration comparison
    i += 1

print "Traversing completes!\n"
