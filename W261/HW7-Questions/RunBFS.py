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
    dist_old = {}
    # save our graph file for iteration
    with open('graph', 'w') as f:
        for line in runner.stream_output():
            # value is nid and node object
            nid, node = init_job.parse_output_line(line)
            # record distance for each node
            dist_old[nid] = node['dist']
            # write graph file 
            f.write('%s\t%s\n' %(nid, node))

# run BFS iteratively
i = 1
while(1):    
    print 'iteration %s' %i
    dist = {}
    with iter_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output    
        with open('graph', 'w') as f:
            for line in runner.stream_output():
                # value is nid and node object
                nid, node = iter_job.parse_output_line(line)
                dist[nid] = node['dist']
                f.write('%s\t%s\n' %(nid, str(node)))
            
    # check if distance for each node changes
    stop = True
    for n in dist:
        if dist_old[n] != dist[n]:
            stop = False
            break  
    
    if stop:
        break
    
    # save dist for next iteration comparison
    dist_old = dist
    i += 1
        
print "\nTraining completes!\n"

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