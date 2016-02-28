#!/usr/bin/python
#%load_ext autoreload
#%autoreload 2
from UnweightedShortestPathIter import UnweightedShortestPathIter
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
init_job = UnweightedShortestPathIter(args=[graph, '--source', source, '-r', mode])
iter_job = UnweightedShortestPathIter(args=['graph', '--source', source, '-r', mode])

# run initialization job
with init_job.make_runner() as runner:
    runner.run()    
    # save our graph file for iteration
    with open('graph', 'w') as f:
        for line in runner.stream_output():
            # value is nid and node object
            nid, node = init_job.parse_output_line(line)            
            # write graph file 
            f.write('%s\t%s\n' %(nid, node))

# run BFS iteratively

i = 1
while(1):    
    print 'iteration %s' %i    
    stop = False
    with iter_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output    
        with open('graph', 'w') as f:
            for line in runner.stream_output():
                # value is nid and node object
                nid, node = iter_job.parse_output_line(line)                
                f.write('%s\t%s\n' %(nid, str(node)))
                if nid == destination and node['dist'] > 0:
                    stop = True
                    break
    
    if stop:
        break
    # more iteration needed
    i += 1
        
print "Traversing completes!\n"

# show path between source and destination
with open('graph', 'r') as f:
    line = f.readline()
    while (line):
        nid, node = line.split('\t')
        if nid == destination:
            cmd = 'node = %s' %node
            exec cmd
            if node['path']:
                print 'shortest distance between %s and %s: %s' %(source, destination, node['dist'])
                print 'path: %s' %' -> '.join(node['path']+[destination])
            else:
                print '%s is a dangling node, cannot traverse from it!' %source
            break
        line = f.readline()