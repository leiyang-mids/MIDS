#!/usr/bin/python
import sys
for line in sys.stdin:   
    parts = line.strip('<').split(',')
    if len(parts) == 0:
        continue
    try:
        temp = int(parts[0])
    except ValueError:
        continue
        
    print "%s\t%s" %(parts[0], line.strip('\n'))