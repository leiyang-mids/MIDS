
from sklearn.feature_extraction.text import *
from sklearn.preprocessing import normalize
import numpy as np

class letor_online:
    ''' runtime component for letor, give plan ranking for query
    '''
    characterizer = None
    learnt_query = None
    plan_ranks = None
    encode_query = None
    normalize_query = None
    similarity_limit = 0.8
    n_query = 0

    def __init__(self, learnt_query, plan_ranks, similarity = 0.8):
        '''
        learnt_query [nx1]: a list of n strings, each one is a query
        plan_ranks [m:1xn]: a dictionary with m key-value pairs;
                            key is plan_id, value is a [1xn] list of plan weight for each query
        '''
        # TODO: data uniformity check


        self.learnt_query = learnt_query
        self.n_query = len(learnt_query)
        self.plan_ranks = plan_ranks
        # initialize encoder
        self.characterizer = CountVectorizer()
        self.encode_query = self.characterizer.fit_transform(learnt_query)
        self.normalize_query = normalize(self.encode_query.astype(np.float))
        self.similarity_limit = similarity

    def get_rank(self, query):
        '''
        get plan rank for a query, if no similar query if found, None is returned
        query [str]: a string for query
        '''
        if type(query) is not str:
            print 'Input parameter query should be a string.'
            return None

        # encode the query, if it's orthogonal to all learnt queries, no ranking
        vQuery = self.characterizer.transform([query])
        if vQuery.data.size == 0:
            print 'No similar query is found, no ranking from LETOR'
            return None

        # set all values to 1 of encoded query (don't care duplicate terms in query)
        for i in range(vQuery.data.size):
            vQuery.data[i] = 1

        # find the most similar query, by cosine distance
        similarity = normalize(vQuery.astype(np.float)).dot(self.normalize_query.T)
        candidates = similarity > self.similarity_limit
        if sum(candidates.data) == 0:
            # if no one above limit, return the synthesized ranking from the closest queries
            candidates = similarity == max(similarity.data)

        # synthesize rank to return
        index = [i for i in range(self.n_query) if candidates.getcol(i)]
        print 'return %d closest match query with index: %s' %(len(index), str(index))
        sum_sim = sum(similarity.getcol(i).data[0] for i in index)
        weights = [similarity.getcol(i).data[0]/sum_sim for i in index]
        return {pid:sum(rank[i]*w for i,w in zip(index,weights)) for pid,rank in self.plan_ranks.items()}

    def features(self):
        return self.characterizer.vocabulary_
