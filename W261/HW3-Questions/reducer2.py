#!/usr/bin/python
from operator import itemgetter
import sys

# buffer for top and bottom
n_bottom, n_top = 10, 50
bottom, top = [], []

current_issue = None
current_count = 0
issue = None
n_total = 0


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    
    issue, count = line.strip().split(',')
    
    # skip bad count
    try:
        count = int(count)
    except ValueError:
        continue
    
    # get total count
    if '*' == issue:
        n_total += count
        continue
        
    # calculate joint count, and relative frequency
    if current_issue == issue:
        current_count += count
    else:
        if current_issue:
            # calculate relative frequency
            rf = 1.0*current_count/n_total
            # buffer top and bottom
            if len(bottom) < n_bottom:
                bottom.append([current_issue, current_count, rf])
                
            if len(top) < n_top:
                top.append([current_issue, current_count, rf])
            else:
                top = top[1:] + [[current_issue, current_count, rf]]
        
        # reset new count
        current_count = count
        current_issue = issue

top.reverse()
print '\ntop %d issues:' %n_top
for rec in top:
    print '%.2f\t%d\t%s' %(100*rec[2], rec[1], rec[0])

print '\nbottom %d issues:' %n_bottom
for rec in bottom:
    print '%.2f\t%d\t%s' %(100*rec[2], rec[1], rec[0])