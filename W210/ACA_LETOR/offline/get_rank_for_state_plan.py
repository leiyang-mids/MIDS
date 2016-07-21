from sets import Set
from scipy.sparse import *
from sklearn import svm
import pickle, glob
import numpy as np
from s3_helpers import *


def get_rank_for_state_plan(query_cluster, click_data):
    '''
    function     : train LETOR models for queries against one state
    query_cluster: list of [nx1] integers to indicate query cluster of one state
    click_data   : list of [nx2] lists of plan_id - each query has two lists:
                   1st - total plan ranking from ES results
                   2nd - list of clicked plan_id
    '''
    s3loader = s3_helper()
    # get state info from click data
    state_ids = list(Set(s[5:7] for j in click_data for s in j[0]))
    state = state_ids[0]
    if len(state_ids) > 1:
        print 'warning: click data has plans from multiple states, training for ' + state

    # load feature data from S3 if no local copy is found
    state_pickle = glob.glob('feature/%s*.pickle' %state)
    if not state_pickle:
        state_pickle.append(s3loader.download_feature_pickle(state))
        print 'downloaded feature pickle %s from s3' %state_pickle[0]
    if len(state_pickle) > 1:
        print 'warning: find multiple state feature pickles, using %s' %state_pickle[0]

    # testData = 'feature/UT_74_19243.pickle'
    # with open(testData) as f:
    with open(state_pickle[0]) as f:
        feature, plans = pickle.load(f)
    n_plan, n_fea = feature.shape
    print 'load %d plans from feature data with dimension %d' %(n_plan, n_fea)

    # for each query cluster
    print 'training started for %d query clusters' %(np.max(query_cluster)+1)
    p_index = {p:plans.index(p) for p in plans}
    letor_rank = []
    for c in np.unique(query_cluster):
        print '\ngetting training data from cluster %d with %d queries' %(c, sum(query_cluster==c))
        fea_mat, tgt_vec = [], []
        # assemble training points from its queries
        for q in click_data[query_cluster==c]:
            # loop through each click to get training pairs
            click_indice = [np.where(q[0]==p)[0][0] for p in q[1]]
            print 'query has %d clicks' %(len(q[1]))
            for c_index in click_indice:
                print 'extracting feature for clicked plan %s' %q[0][c_index]
                # loop through all items before current clicked item
                for i in range(c_index):
                    if i in click_indice:
                        continue
                    fea_mat.append( (feature.getrow(p_index[q[0][c_index]]) - feature.getrow(p_index[q[0][i]]))
                                     if i%2==0 else (feature.getrow(p_index[q[0][i]]) - feature.getrow(p_index[q[0][c_index]])) )
                    tgt_vec.append((-1)**i)
        print 'start training with %d pair features with %d +1' %(len(tgt_vec), sum(np.array(tgt_vec)==1))
        clf = svm.SVC(kernel='linear', C=.2, max_iter=-1)
        clf.fit(vstack(fea_mat, format='csr'), tgt_vec)
        print 'training completed, obtain plan ranking'
        r_weight = clf.coef_.dot(feature.T).toarray()[0]
        r_min = np.min(r_weight)
        r_range = np.max(r_weight) - r_min
        letor_rank.append((r_weight-r_min)/r_range)

    # save pickle in training data for ES indexing
    return np.array(letor_rank)
