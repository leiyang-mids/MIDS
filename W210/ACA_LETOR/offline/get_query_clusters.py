from sklearn.preprocessing import normalize
import numpy as np


def get_query_clusters(points, k):
    '''
    points [n,m] - array for n points with dimention m - encoded query
    '''
    # normalize input
    points = normalize(points.astype(np.float))
    # get similarity matrix (cosine distance)
    dist = points.dot(points.T)
    # initialize variables
    n_pt = len(points)
    cluster_old, cluster_new = np.ones(n_pt), np.zeros(n_pt)
    # special case, no clustering
    if k==1 or n_pt==1:
        return np.zeros(n_pt), 1 if n_pt==1 else np.mean(dist[np.triu_indices(n_pt,k=1)])
    # randomly choose k starting centroids
    centroids = points[np.random.permutation(n_pt)[:k]]
    while not np.array_equal(cluster_old, cluster_new):
        cluster_old = cluster_new
        # get cluster index for each point
        cluster_new = np.argmax(points.dot(centroids.T), axis=1)
        # get new centroids, and within class mean distance/similarity
        centroids, in_dist = [], []
        for c in np.unique(cluster_new):
            pid = cluster_new==c
            # set new centroid as the one who has minimum total distance to rest of the points in the cluster
            cid = np.argmax(np.sum(dist[np.ix_(pid, pid)], axis=1))
            centroids.append(points[pid][cid])
            in_dist.append(1 if sum(pid)==1 else np.mean(dist[np.ix_(pid,pid)][np.triu_indices(sum(pid),k=1)]))
        centroids = np.array(centroids)
        # traditional way to get new centroid, not working well for cosine distance
#         centroids = normalize([np.mean(points[cluster_new==c], axis=0) for c in np.unique(cluster_new)])

    return cluster_new, np.mean(in_dist), centroids
