#!/usr/bin/python
import sys
for line in sys.stdin:   
    parts = line.strip('<').split(',')
    if len(parts) == 0:
        continue
    try:
        key = int(parts[0])
    except ValueError:
        continue
    
    # use integer as the key, and the entire record is value emitted by the mapper
    print "%d\t%s" %(key, line.strip())