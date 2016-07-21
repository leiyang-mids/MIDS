from sklearn.feature_extraction.text import *
from get_query_clusters import *

def query_characterizer(queries, similarity_limit = 0.9):
    '''
    queries - list of string for queries
    return  - list of integers to indicate cluster for each query
    '''
    # vectorize queries
    characterizer = CountVectorizer()
    encoded_query = characterizer.fit_transform(queries)
    # set all values to 1 of encoded query (don't care duplicate terms in query)
    for i in range(encoded_query.data.size):
        encoded_query.data[i] = 1
    # find the optimal clusters based on minimum within cluster distance
    avg_sim, k = 0, 0
    while avg_sim < similarity_limit:
        k += 1
        clusters, avg_sim, centroids = get_query_clusters(encoded_query, k)

    return clusters, characterizer, centroids #, avg_sim, k
