#!/usr/bin/python
import sys

# increase counter for reducer being called
sys.stderr.write("reporter:counter:HW3_4,Combiner_cnt,1\n")

current_pair = None
current_count = 0

for line in sys.stdin:       
    # get all products from the session
    pair, count = line.strip().split(',', 1)
    
    # skip bad count
    try:
        count = int(count)
    except ValueError:
        continue
        
    # accumulate counts for whatever keys it receives
    if current_pair == pair:
        current_count += count
    else:
        # previous pair finishes streaming, emit results
        if current_pair:            
            print '%s,%s' %(current_pair, current_count)
        # reset new pair
        current_pair = pair
        current_count = count