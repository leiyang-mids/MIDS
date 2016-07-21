from query_drug_feature import *
from scipy.sparse import *

def extract_drug_feature(drug_col, plan_ids):
    '''
    '''

    print 'get all drugs covered by all plans'
    all_rxnorm = drug_col.find({'plans.plan_id':{'$in':valid_plan1}}).distinct('rxnorm_id')
    print 'total rx: %d' %(len(all_rxnorm))

    print 'check drug coverage for each plan'
    drug_coverage = lil_matrix((n_plan, len(all_rxnorm)))
    valid_plan2 = []
    for p in getDrugListForPlans(drug_col, valid_plan1):
        valid_plan2.append(p['plan'])
        r_id = state_plan.index(p['plan'])
        for r in p['drug']:
            drug_coverage[r_id, all_rxnorm.index(r)] = 1
    print 'complete for %d plans' %(len(valid_plan2))

    print 'get summary feature for drug'
    all_drug_states = getDrugAggregateAllStates(drug_col, valid_plan2)
    print 'total drug states: %d' %(len(all_drug_states))

    print 'extract drug sumstat for each plan'
    drug_sumstat = lil_matrix((n_plan, len(all_drug_states)))
    valid_plan3 = []
    for p in getDrugAggregateCountForPlans(drug_col, valid_plan2):
        valid_plan3.append(p['plan'])
        r_id = state_plan.index(p['plan'])
        for d in p['drug_state']:
            drug_sumstat[r_id, all_drug_states.index(d['key'])] = d['cnt']
    print 'complete for %d plans' %(len(valid_plan3))
