from query_drug_feature import *
from scipy.sparse import *

def extract_drug_feature(drug_col, plan_ids, log):
    '''
    '''
    fea_mat = []
    log.trace('get all drugs covered by all plans')
    all_rxnorm = drug_col.find({'plans.plan_id':{'$in':plan_ids}}).distinct('rxnorm_id')
    n_rxnorm = len(all_rxnorm)
    log.trace('total rx: %d' %(n_rxnorm))

    log.trace('check drug coverage for each plan')
    drug_coverage = {}
    for p in getDrugListForPlans(drug_col, plan_ids):
        d_row = lil_matrix((1, n_rxnorm))
        for r in p['drug']:
            d_row[0, all_rxnorm.index(r)] = 1
        drug_coverage[p['plan']] = d_row
    if drug_coverage:
        fea_mat.append(drug_coverage)
        log.trace('complete for %d plans' %(len(drug_coverage)))

    log.trace('get summary feature for drug')
    all_drug_states = getDrugAggregateAllStates(drug_col, plan_ids)
    n_state = len(all_drug_states)
    log.trace('total drug states: %d' %n_state)

    log.trace('extract drug sumstat for each plan')
    drug_sumstat = {}
    for p in getDrugAggregateCountForPlans(drug_col, plan_ids):
        d_row = lil_matrix((1, n_state))
        for d in p['drug_state']:
            d_row[0, all_drug_states.index(d['key'])] = d['cnt']
        drug_sumstat[p['plan']] = d_row
    if drug_sumstat:
        fea_mat.append(drug_sumstat)
        log.trace('complete for %d plans' %(len(drug_sumstat)))

    return fea_mat
