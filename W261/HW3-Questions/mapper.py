#!/usr/bin/python
import sys

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_5,Mapper_cnt,1\n")

# composite associative array
H = {}

for line in sys.stdin:   
    # get all products from the session
    products = line.strip().split(' ')
    size = len(products)
    if size==0:
        continue
    
    # sort products so the pair is lexicographically sound
    products.sort()
    
    # get pairs of products
    pairs = [[products[i], products[j]] for i in range(size) for j in range(i+1, size)]
    
    # emit dummy record
    print '%s\t%s' %('*', 1)
    
    # build associative arrays
    for w1, w2 in pairs:
        # each pair is lexicographically in order        
        if w1 not in H:
            # if w1 is new, add an associative array for it
            H[w1] = {}
            H[w1][w2] = 1            
        elif w2 not in H[w1]:
            # w1 is not new, but it doesn't have key for w2
            H[w1][w2] = 1
        else:
            # both are there, increase it
            H[w1][w2] += 1
        
# emit associative arrays
for h in H:
    print '%s\t%s' %(h, str(H[h]))