#!/usr/bin/python
import sys
import math
from sets import Set

n_count, s_count = {}, {}
nSpam, nNormal = 0, 0
counts = []

# scan through each output file from the chunks
for filename in sys.argv[1:]:
    # we first read out the 2 count dictionaries
    with open (filename, "r") as myfile:         
        for line in myfile.readlines():
            cmd = 'counts.append(' + line + ')'
            exec cmd
            
    # we then combine word counts, for non-spam and spam messages, respectively
    for word in counts[0]:
        if word not in n_count:
            n_count[word] = counts[0][word]
        else:
            n_count[word] += counts[0][word]
    
    for word in counts[1]:
        if word not in s_count:
            s_count[word] = counts[1][word]
        else:
            s_count[word] += counts[1][word]
            
    # combine spam and non-spam count
    nNormal += int(counts[2])
    nSpam += int(counts[3])
    
    # clear counts for next chunk
    counts = []

testfile = 'enronemail_1h.txt'
print 'Classify messages with all words'
   
# we now estimate NB parameters for all present words
allwords = Set(s_count.keys() + n_count.keys())
B = len(allwords)
tot_n = sum(n_count.values())
tot_s = sum(s_count.values())

#### prior probability ####
p_s = 1.0*nSpam/(nSpam+nNormal)
p_n = 1.0*nNormal/(nSpam+nNormal)

#### conditional probabilities for words ####
p_word_s, p_word_n = {}, {}
for word in allwords:
    p_word_s[word] = 1.0*((s_count[word] if word in s_count else 0) + 1) / (tot_s + B)
    p_word_n[word] = 1.0*((n_count[word] if word in n_count else 0) + 1) / (tot_n + B)

# finally we classify the messages which contains the specified word
#### we won't print model parameters, to save some space ####
#print '\n============= Model Parameters ============='
#print 'P(spam) = %f' %(p_s)
#print 'P(non-spam) = %f' %(p_n)
#for word in keywords:
#    print 'P(%s|spam) = %f' %(word, p_word_s[word])
#    print 'P(%s|non-spam) = %f' %(word, p_word_n[word])

#### likelihood: dependend on the frequency of current word ####
print '\n============= Classification Results ============='
print 'TRUTH \t CLASS \t ID'
n_correct = 0
with open (testfile, "r") as myfile:  
    for line in myfile.readlines():
        msg = line.lower().split()
        words = msg[2:] # only include words in subject and content
        #### initialize posterior probability ####
        p_s_word = math.log(p_s)
        p_n_word = math.log(p_n)
        
        #### add likelihood for each keyword ####        
        for key in Set(words):
            n_key = sum([1 if key in word else 0 for word in words])
            p_s_word += n_key * math.log(p_word_s[key])
            p_n_word += n_key * math.log(p_word_n[key])
            
        isSpam = True if p_s_word > p_n_word else False
        n_correct += isSpam == int(msg[1])
        # print results
        print ('spam' if int(msg[1]) else 'ham') + '\t' + ('spam' if isSpam else 'ham') + '\t' + msg[0]

print '\nClassification rate: %f' %(1.0*n_correct/(nSpam+nNormal))