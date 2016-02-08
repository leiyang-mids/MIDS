#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_7,Reducer_1_cnt,1\n")

current_prod = None
current_count = 0
min_support = 100

for line in sys.stdin:   
    # get k-v pair
    prod, count = line.strip().split('\t', 1)
    
    # skip bad count
    try:
        count = int(count)
    except ValueError:
        continue
        
    # get count
    if current_prod == prod:
        current_count += count
    else:
        if current_prod and current_count > min_support:
            # emit prod above min support
            print '%s\t%d' %(current_prod, current_count)
        # reset
        current_prod = prod
        current_count = count
    