from sets import Set
import pickle, glob
from scipy.sparse import *
from sklearn import svm


def get_rank_for_state_plan(query_cluster, click_data):
    '''
    function     : train LETOR models for queries against one state
    query_cluster: list of [nx1] integers to indicate query cluster of one state
    click_data   : list of [nx2] lists of plan_id - each query has two lists:
                   1st - total plan ranking from ES results
                   2nd - list of clicked plan_id
    '''

    # get state info from click data
    state_ids = Set(s[5:7] for j in click_data for s in j[0])
    state = state_ids[0]
    if len(state_ids) > 1:
        print 'warning: click data has plans from multiple states, training for ' + state

    # load feature data from S3 if no local copy is found
    state_pickle = glob.glob('%s*.pickle' %state)
    if not state_pickle:
        # TODO: download from S3
        print 'download from s3'
    if len(state_pickle) > 1:
        print 'warning: find multiple state feature pickles, using %s' %state_pickle[0]

    testData = 'UT_74_19243.pickle'
    # with open(state_pickle[0]) as f:
    with open(testData) as f:
        feature, plans = pickle.load(f)
    n_plan, n_fea = feature.shape

    # for each query cluster
    p_index = {p:plans.index(p) for p in plans}
    letor_rank = []
    for c in np.unique(query_cluster):
        fea_mat, tgt_vec = lil_matrix((1, n_fea)), []
        # assemble training points from its queries
        for q in click_data[query_cluster==c]:
            # loop through each click to get training pairs
            click_indice = [q[0].index(p) for p in q[1]]
            for c_index in click_indice:
                # loop through all items before current clicked item
                for i in range(c_index):
                    if i in click_indice:
                        continue
                    fea_mat = vstack([fea_mat,
                                      ( (feature.getrow(p_index[q[0][c_index]]) - feature.getrow(p_index[q[0][i]]))
                                      if i%2==0 else
                                      (feature.getrow(p_index[q[0][i]]) - feature.getrow(p_index[q[0][c_index]])) )
                                     ], format='lil')
                    tgt_vec.append((-1)**i)
        # get rid of the first row, then  train SVM
        fea_mat = csr_matrix(fea_mat[range(1, fea_mat.shape[0]), :])
        clf = svm.SVC(kernel='linear', C=.1)
        clf.fit(fea_mat, tgt_vec)
        # coef = clf.coef_.toarray()[0]
        letor_rank.append(clf.coef_.dot(feature.T))

    return letor_rank
