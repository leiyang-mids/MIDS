from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from subprocess import Popen, PIPE

class Jaccard(MRJob):

    PARTITIONER='org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    #################################  job 0 - create co-occurrence matrix ##############################

    # job 0 mapper for SYSTEMS_TEST_DATASET
    def j0_mapper_read_test(self, _, line):
        # time of mapper being called
        self.increment_counter('HW5_4', 'mapper_test', 1)
        # parse line, get doc id and terms
        word, strip = line.strip().split(' ', 1)
        cmd = 'strip = ' + strip
        exec cmd
        # emit co-occurrence matrix
        yield word, strip

    # job 0 mapper_init: load our 9k favorite words, and evaluate those only
    def j0_mapper_read_5gram_init(self):
        localPath = '/Users/leiyang/GitHub/mids/w261/HW5-Questions/Top10kWords'
        cat = Popen(["cat", 'Top10kWords'], stdout=PIPE)
        self.corpus = [s.split()[0].strip('"') for s in cat.stdout][8000:]

    # job 0 mapper for 5-gram: build pseudo-document (co-ocurrence matrix) & emit inverted index
    def j0_mapper_read_5gram(self, _, line):
        # parse line, get words and counts
        grams, cnt, p_cnt, b_cnt = line.strip().split('\t')
        # only keep words from the 9k corpus
        grams = grams.lower().split(' ')
        grams = [x for x in grams if x in self.corpus]
        n_gram = len(grams)
        # emit co-ocurrence for each pair (MUST include all pairs to have correct inverted index)
        for w1, w2 in [[grams[i], grams[j]] for i in range(n_gram) for j in range(n_gram)]:
            yield (w1, w2), int(cnt)

    # job 0 combiner - local aggregation of co-occurrence
    def j0_combiner(self, pair, count):
        yield (pair), sum(count)

    # job 0 reducer_init()
    def j0_reducer_init(self):
        self.current_term = None
        self.current_strip = {}

    # job 0 reducer
    def j0_reducer(self, key, count):
        w1, w2 = key[0], key[1]
        if w1 == w2:
            return
        if self.current_term == w1:
            # accumulate co-occurent words
            self.current_strip[w2] = sum(count)
        else:
            # yield previous word and stripe
            if self.current_term:
                yield self.current_term, self.current_strip
            # reset new term
            self.current_term = w1
            self.current_strip = {w2:sum(count)}

    # job 0 reducer final - emit last word strip
    def j0_reducer_final(self):
        if self.current_term:
            yield self.current_term, self.current_strip

    #################################  job 1 - create inverted indexing ##############################

    # job 1 mapper - build inverted index
    def j1_mapper_jaccard(self, w1, stripe):
        # here stripe is a dictionary
        norm = sqrt(len(stripe))
        for w2 in stripe:
            yield (w2, w1), 1/norm

    def j1_mapper_cosine(self, w1, stripe):
        # here stripe is a dictionary
        norm = sqrt(sum(pow(x,2) for x in stripe.values()))
        for w2 in stripe:
            yield (w2, w1), stripe[w2]/norm

    # job 1 reducer_init()
    def j1_reducer_init(self):
        self.current_term = None
        self.current_strip = {}

    # job 1 reducer
    def j1_reducer(self, pair, count):
        w2, w1 = pair[0], pair[1]
        if self.current_term == w2:
            # accumulate postings
            self.current_strip[w1] = sum(count)
        else:
            # yield previous term and stripe
            if self.current_term:
                yield self.current_term, self.current_strip
            # reset new term
            self.current_term = w2
            self.current_strip = {w1:sum(count)}

    # job 1 reducer final - emit last index strip
    def j1_reducer_final(self):
        if self.current_term:
            yield self.current_term, self.current_strip

    #################################  job 2 - evaluate similarity between words ##############################

    # job 2 mapper - emit pair-wise similarity from strips
    def j2_mapper(self, term, postings):
        # get all postings from generator
        posts = postings.keys() # [p for p in postings]
        posts.sort()
        size = len(posts)
        # emit pairs on sorted stripe, so we only evaluate half of the symmetric relation
        for w1, w2 in [[posts[i], posts[j]] for i in range(size) for j in range(i+1, size)]:
            yield (w1, w2), postings[w1]*postings[w2]

    # job 2 reducer - get pair similarity
    def j2_reducer(self, pair, prod):
        # calculate similarity
        yield pair, sum(prod)

    #################################  job 3 - rank pairwise similarities ##############################

    # job 3 mapper - for secondary sort
    def j3_mapper1(self, page, count):
        yield ('%s__%s' %(page[0], page[1]), count), None

    def j3_reducer_init1(self):
        self.i = 0
        self.n_freq = 1000

    def j3_reducer1(self, key, _):
        #if True: self.i < self.n_freq:
            #self.i += 1
        yield key

    #################################  mrjob definition ##############################
    # MapReduce steps
    def steps(self):
        jobconf0 = {  #key value pairs
            #'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #'mapreduce.partition.keycomparator.options':  '-k2,2', # '-k1,1r -k2,2r', # no need to sort
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'mapreduce.job.maps': '30',
            'mapreduce.job.reduces': '30',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        jobconf1 = {  #key value pairs
            #'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #'mapreduce.partition.keycomparator.options': '-k1,1r -k2,2r', # no need to sort
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'mapreduce.job.maps': '30',
            'mapreduce.job.reduces': '30',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        jobconf2 = {  #key value pairs
            #'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #'mapreduce.partition.keycomparator.options': '-k1,1r -k2,2r', # -k2,2r',
            #'mapreduce.partition.keypartitioner.options': '-k1,2',
            'mapreduce.job.maps': '30',
            'mapreduce.job.reduces': '50',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }
        jobconf3 = {  #key value pairs
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k2,2nr',
            'mapreduce.job.maps': '15',
            'mapreduce.job.reduces': '1',
            'stream.num.map.output.key.fields': '2',
            'mapreduce.map.output.key.field.separator': ' ',
            'stream.map.output.field.separator': ' ',
        }

        # NOTE: DO NOT use jobconf when running with Python locally
        return [
                ######## job 0: get co-ocurrence matrix ########
                ### for SYSTEMS_TEST_DATASET.txt ###
                #MRStep(mapper=self.j0_mapper_read_test)
                ### for n-gram file ###
                MRStep(mapper_init=self.j0_mapper_read_5gram_init, mapper=self.j0_mapper_read_5gram
                        , combiner=self.j0_combiner, reducer_init=self.j0_reducer_init
                        , reducer=self.j0_reducer, reducer_final=self.j0_reducer_final
                        , jobconf=jobconf0
                      )
                ######## job 1: get inverted indexing ########
                ,MRStep(mapper=self.j1_mapper_cosine
                        , reducer_init=self.j1_reducer_init
                        , reducer=self.j1_reducer, reducer_final=self.j1_reducer_final
                        , jobconf=jobconf1
                      )
                ######## job 2: calculate pair similarity between words ########
                ,MRStep(mapper=self.j2_mapper, combiner=self.j2_reducer
                        , reducer=self.j2_reducer
                        , jobconf=jobconf2
                      )
                ######## job 3: sort similarities ########
                ,MRStep(mapper=self.j3_mapper1, reducer_init=self.j3_reducer_init1
                        , reducer=self.j3_reducer1
                        , jobconf=jobconf3
                       )
               ]

if __name__ == '__main__':
    Jaccard.run()
