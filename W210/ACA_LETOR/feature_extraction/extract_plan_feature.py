from query_plan_feature import *
from scipy.sparse import *

def extract_plan_feature(plan_col, plan_ids):
    '''
    '''
    fea_mat, n_plan = [], len(plan_ids)
    print 'get formulary state space for all plans'
    all_plan_states = getFormularyAllStates1(plan_col, plan_ids) + \
                      getFormularyAllStates2(plan_col, plan_ids) + \
                      getFormularyAllStates3(plan_col, plan_ids)
    print 'total formulary states: %d' %(len(all_plan_states))

    print 'extract formulary states for each plan'
    plan_feature = lil_matrix((n_plan, len(all_plan_states)))
    valid_plan1 = []
    for f in [getFormularyStatesForPlan1,getFormularyStatesForPlan2,getFormularyStatesForPlan3]:
        for p in f(plan_col, plan_ids):
            r_id = plan_ids.index(p['_id'])
            valid_plan1.append(p['_id'])
            for s in p['plan_states']:
                plan_feature[r_id, all_plan_states.index(s)] = 1
    fea_mat.append(plan_feature)
    print 'complete for %d plans' %(len(valid_plan1))

    print 'get formulary summary feature for each plan'
    plan_sumstat = [[0]*3]*len(valid_plan1)
    for p in getFormularyAggregate(plan_col, valid_plan1):
        r_id = plan_ids.index(p['plan'])
        plan_sumstat[r_id] = [p['avg_copay'],p['avg_ci_rate'],p['count']]
    fea_mat.append(plan_sumstat)
    print 'complete for %d plans' %(len(valid_plan1))

    return fea_mat
