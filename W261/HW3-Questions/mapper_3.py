#!/usr/bin/python
import sys, subprocess 

# increase counter for mapper being called
sys.stderr.write("reporter:counter:HW3_7,Mapper_3_cnt,1\n")

# load the frequent freqPairs given by Job 2
freqPair = []
cat = subprocess.Popen(['cat', 'part-00001'], stdout=subprocess.PIPE)
for line in cat.stdout:
    freqPair.append(line.strip().split('\t')[0])
    
# still read frequent freqPairs first, then session data to generate triples
for line in sys.stdin:   
    
    line = line.strip()
            
    # get products from each session
    prod = line.split(' ')
    prod.sort()
    n = len(prod)
    
    # generate freqPairs and triples from the session, in the format of a_b and a_b_c, alphabetically sorted
    triples = [[prod[i],prod[j],prod[k]] for i in range(n) for j in range(i+1,n) for k in range(i+2,n)]
    pairs = [prod[i]+'_'+prod[j] for i in range(n) for j in range(i+1,n)]

    # processing pairs
    for pair in pairs:
        # if the pair is in freqPair, emit a dummy key a_b_*
        if pair in freqPair:
            print '%s_*\t%d' %(pair, 1)

    # processing triples
    for tri in triples:
        # from each triple a_b_c: check if the 3 child-pairs (a_b, b_c, a_c) are in the freqPair set
        if tri[0]+'_'+tri[1] in freqPair and tri[1]+'_'+tri[2] in freqPair and tri[0]+'_'+tri[2] in freqPair:
            # if so, emit the triple a_b_c            
            print '%s_%s_%s\t%d' %(tri[0], tri[1], tri[2], 1)
