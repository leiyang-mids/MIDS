from query_plan_feature import *
from scipy.sparse import *

def extract_plan_feature(plan_col, plan_ids, log):
    '''
    '''
    fea_mat = []
    log.trace('get formulary state space for all plans')
    all_plan_states = getFormularyAllStates1(plan_col, plan_ids) + \
                      getFormularyAllStates2(plan_col, plan_ids) + \
                      getFormularyAllStates3(plan_col, plan_ids)
    n_state = len(all_plan_states)
    log.trace('total formulary states: %d' %n_state)

    log.trace('extract formulary states for each plan')
    plan_feature = {}
    for f in [getFormularyStatesForPlan1,getFormularyStatesForPlan2,getFormularyStatesForPlan3]:
        for p in f(plan_col, plan_ids):
            p_row = lil_matrix((1, n_state))
            for s in p['plan_states']:
                p_row[0, all_plan_states.index(s)] = 1
            plan_feature[p['_id']] = p_row
    if plan_feature:
        fea_mat.append(plan_feature)
        log.trace('complete for %d plans' %(len(plan_feature)))

    log.trace('get formulary summary feature for each plan')
    plan_sumstat = {p['plan']:[p['avg_copay'],p['avg_ci_rate'],p['count']] for p in getFormularyAggregate(plan_col, plan_ids)}
    if plan_sumstat:
        fea_mat.append(plan_sumstat)
        log.trace('complete for %d plans' %(len(plan_sumstat)))

    return fea_mat
