#!/usr/bin/python
import sys
for line in sys.stdin:    
    print "%s" %(line.split('\t')[1])