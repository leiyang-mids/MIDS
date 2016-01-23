#!/usr/bin/python
from operator import itemgetter
import sys, operator
import numpy as np

current_word = None
smooth_factor = 0 # no smoothing
current_count = [smooth_factor, smooth_factor]
word = None
wordcount = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count, isSpam = line.split('\t', 2)

    # convert count and spam flag (currently a string) to int
    try:
        count = int(count)
        isSpam = int(isSpam)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:        
        current_count[isSpam] += count
    else:
        if current_word:
            # count finish for one word, save it
            wordcount[current_word] = current_count
        # initialize new count for new word
        current_count = [smooth_factor, smooth_factor]
        current_count[isSpam] = count
        current_word = word

# do not forget to save the last word count if needed!
if current_word == word:    
    wordcount[current_word] = current_count
    
# calculate NB parameters, and write the dictionary to a file for the classification job
n_total = np.sum(wordcount.values(), 0)
#print 'total count %s' %(str(n_total))
#probability = {key:value for (key,value) in zip(wordcount.keys(), wordcount.values()/(1.0*n_total))}
#print probability
for (key,value) in zip(wordcount.keys(), wordcount.values()/(1.0*n_total)):
    print '%s\t%s\t%s' %(key, value[0], value[1])