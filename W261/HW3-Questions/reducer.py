#!/usr/bin/python

# function to combine associative array
def elementSum(H1, H2):    
    # make sure H1 is the long one
    if len(H1)<len(H2):
        H0 = H2
        H2 = H1
        H1 = H0
    # merge shorter one H2 into longer one H1
    for h in H2:
        if h not in H1:
            H1[h] = H2[h]
        else:
            H1[h] += H2[h]        
    # return
    return H1

import sys
import numpy as np

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_5,Reducer_cnt,1\n")

min_support = 100
current_word = None
current_aArray = None
n_total = 0

for line in sys.stdin:
    # parse out keyword and the associative array
    word, aArray = line.strip().split('\t', 1)
    
    # get total basket
    if word == '*':
        n_total += int(aArray)
        continue
    
    # get array into variable
    cmdStr = 'aArray = ' + aArray
    exec cmdStr
        
    # merge the associative array
    if current_word == word:
        current_aArray = elementSum(current_aArray, aArray)           
    else:
        # finish one word merge
        if current_word:
            # get the top pairs with heap
            for p in current_aArray:
                if current_aArray[p] > min_support:                    
                    # get relative freq
                    rf = 100.0*current_aArray[p]/n_total
                    print '%s,%s,%s,%.4f%%' %(current_word, p, current_aArray[p], rf)
        # reset for a new word
        current_word = word
        current_aArray = aArray

#print '\ntotal basket: %d' %n_total