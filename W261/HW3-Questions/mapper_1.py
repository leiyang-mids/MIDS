#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_7,Mapper_1_cnt,1\n")

for line in sys.stdin:   
    # get words and emit
    for prod in line.strip().split(' '):
        print '%s\t%d' %(prod, 1)