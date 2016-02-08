#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_7,Reducer_3_cnt,1\n")

current_prod = None
current_dummy = None
current_count = 0
min_support = 100
marginal = 0

for line in sys.stdin:   
        
    # get k-v freqPair
    prod, count = line.strip().split('\t', 1)
    
    # skip bad count
    try:
        count = int(count)
    except ValueError:
        continue
    
    # handle marginal with dummy key
    if '*' == prod[-1]:        
        if current_dummy == prod:
            # accumulate marginal
            marginal += count
        else:
            # reset marginal for new dummy key
            current_dummy = prod
            marginal = count
        continue

    # processing triple and emit rules
    if current_prod == prod:
        current_count += count
    else:
        if current_prod and current_count > min_support:
            # for debug, check if current dummy matches current triple
            if current_prod[:-8] != current_dummy[:-1]:
                print 'WARNING: mismatch between %s and %s(%d)' %(current_prod, current_dummy, marginal)
            else:
                # emit triples for the rule
                w1,w2,w3 = current_prod.split('_')
                conf = 100.0*current_count/marginal
                print '(%s, %s) => %s, %d, %d, %.2f%%' %(w1, w2, w3, current_count, marginal, conf)
            
        # reset for new triple
        current_prod = prod
        current_count = count
        