#!/usr/bin/python
import sys, re, string, subprocess
# read the probability from HDFS
prob = {}
cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/leiyang/prob/part-00000"], stdout=subprocess.PIPE)
for line in cat.stdout:
    word, p0, p1 = line.split()
    prob[word] = [float(p0), float(p1)]

# define regex for punctuation removal
regex = re.compile('[%s]' % re.escape(string.punctuation))
# input comes from STDIN (standard input)
for line in sys.stdin:
    # use subject and body
    msg = line.split('\t', 2)
    # skip bad message 
    if len(msg) < 3:
        continue
    msgID, isSpam = msg[0], msg[1]    
    # remove punctuations, only have white-space as delimiter
    words = regex.sub('', msg[-1].lower())
    # split the line into words
    words = words.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s\t%s\t%s' % (msgID, prob[word][0], prob[word][1], isSpam)