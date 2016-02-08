#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_5,Reducer_s_cnt,1\n")

n_out = 0
n_top = 50

print 'top %d pairs: ' %n_top

for line in sys.stdin:   
    # parse mapper output  
    n_out += 1
    if n_out <= n_top:        
        print line.strip().replace(',', '\t')