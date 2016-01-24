#!/usr/bin/python
import sys, Queue

n_max, n_min = 10, 10
q_max = Queue.Queue(n_max)
a_min = []

for line in sys.stdin:
    rec = line.split('\t')[1].strip()
    # put the biggest
    if len(a_min) < n_min:
        a_min.append(rec)
    
    # whatever left is the smallest
    if q_max.full():
        q_max.get()
    q_max.put(rec)

print '\n%d smallest records:' %n_min
for record in a_min:
    print record

print '\n%d smallest records:' %n_max
for i in range(n_max):
    print q_max.get()