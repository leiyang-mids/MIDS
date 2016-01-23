#!/usr/bin/python
from operator import itemgetter
import sys, operator

current_word = None
current_count = 0
word = None
wordcount = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # save count            
            wordcount[current_word] = current_count
        current_count = count
        current_word = word

# do not forget to save the last word count if needed!
if current_word == word:    
    wordcount[current_word] = current_count
    
# sort count top get top n counts:
n = 10
wordcount = sorted(wordcount.items(), key=operator.itemgetter(1))
print 'Top %d counts out of %d words:' %(n, len(wordcount))
for i in range(n):
    print '%s\t%d' %(wordcount[-i-1])