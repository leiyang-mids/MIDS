#!/usr/bin/python
import sys
import re
# let's use two dictionaries to hold the word counts for spam and non-spam
n_count, s_count = {}, {}
nSpam, nNormal = 0, 0
WORD_RE = re.compile(r"[\w']+")
filename = sys.argv[1]
#keywords = sys.argv[2].lower()
with open (filename, "r") as myfile:
    for email in myfile.readlines():
        isSpam = email.split('\t')[1] == '1'
        if isSpam:
            nSpam += 1
            for word in email.lower().split()[2:]: # only use subject & content for modeling
                if word not in s_count:
                    s_count[word] = 1
                else:
                    s_count[word] += 1
        else:
            nNormal += 1
            for word in email.lower().split()[2:]: # only use subject & content for modeling
                if word not in n_count:
                    n_count[word] = 1
                else:
                    n_count[word] += 1
print n_count
print s_count
print nNormal
print nSpam