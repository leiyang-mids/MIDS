#!/usr/bin/python
from operator import itemgetter
import sys, operator, math
import numpy as np

current_msg = None
current_prob = [0, 0]
msgID = None
n_error = 0
n_msg = 0
n_zero = [0, 0]
prio = [0, 0] # [math.log(.56), math.log(.44)]

print '%s\t%s\t%s' %('TRUTH', 'PREDICTION', 'EMAIL ID')
# input comes from STDIN
for line in sys.stdin:
    print line
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    msgID, p0, p1, isSpam = line.split('\t', 3)
    prob = [float(p0), float(p1)]
    
    # convert count and spam flag (currently a string) to int
    try:        
        isSpam = int(isSpam)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_msg == msgID:        
        current_prob = np.sum([[math.log(x) if x>0 else float('-inf') for x in prob], current_prob], 0)
    else:
        if current_msg:
            # count finish for one word, predict and print result
            pred = np.argmax(current_prob)
            n_error += pred != isSpam
            n_msg += 1
            n_zero[isSpam] += float('-inf') in current_prob
            print '%s\t%s\t%s\t%s' %(isSpam, pred, str(current_prob), msgID)
            
        # initialize new count for new word
        current_prob = np.sum([[math.log(x) if x>0 else float('-inf') for x in prob], prio], 0)
        current_msg = msgID

# do not forget to print the last msg result if needed!
if current_msg == msgID:
    pred = np.argmax(current_prob)
    n_error += pred != isSpam
    n_msg += 1
    n_zero[isSpam] += float('-inf') in current_prob
    print '%s\t%s\t%s\t%s' %(isSpam, pred, str(current_prob), msgID)
    
# calculate the overall error rate
print 'Error rate: %.4f' %(1.0*n_error/n_msg)
print 'Number of zero probability: spam(%d), ham(%d)' %(n_zero[1], n_zero[0])