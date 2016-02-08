#!/usr/bin/python
import sys, subprocess 

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_7,Mapper_2_cnt,1\n")

singleton = []
cat = subprocess.Popen(['cat', 'part-00000'], stdout=subprocess.PIPE)
for line in cat.stdout:
    singleton.append(line.strip().split('\t')[0])

# read the input data
for line in sys.stdin:   
    
    line = line.strip()
        
    # get words for each session
    prod = line.strip().split(' ')
        
    # keep product from singleton set only
    products = [val for val in prod if val in singleton]
    products.sort()
    
    # get pairs to emit
    size = len(products)
    pairs = [products[i] + '_' + products[j] for i in range(size) for j in range(i+1, size)]
    for p in pairs:
        print '%s\t%d' %(p, 1)
        