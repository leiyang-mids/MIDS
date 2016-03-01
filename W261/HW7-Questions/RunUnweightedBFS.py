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
    fileName = 'shortest_%s_%s' %(source, destination)
    stop = False
    with iter_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output    
        with open('graph', 'w') as f:
            for line in runner.stream_output():
                # value is nid and node object
                nid, node = iter_job.parse_output_line(line)                
                # if the destination is reached, save results and quit
                if nid == destination and node['dist'] > 0:
                    with open(fileName) as s:
                        s.write('%s\t%s\n' %(nid, str(node)))
                    stop = True
                    break
                else:
                    # otherwise write to file for next iteration
                    f.write('%s\t%s\n' %(nid, str(node)))
    
    if stop:
        break
    # more iteration needed
    i += 1
        
print "Traversing completes!\n"