#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_5,Mapper_s_cnt,1\n")

for line in sys.stdin:   
    # just emit
    print line.strip()