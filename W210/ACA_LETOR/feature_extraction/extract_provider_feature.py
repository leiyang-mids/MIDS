from query_provider_feature import *
from scipy.sparse import *

def extract_provider_feature(prov_col, plan_ids, log):
    '''
    '''
    fea_mat = []
    log.trace('get provider under the plans')
    all_npi = prov_col.find({'plans.plan_id':{'$in':plan_ids}}).distinct('npi')
    n_npi = len(all_npi)
    log.trace('total providers: %d' %n_npi)

    log.trace('check provider coverage for each plan') ##### slow #####
    provider_coverage = {}
    for p in getProviderListForPlans(prov_col, plan_ids):
        p_row = lil_matrix((1, n_npi))
        for npi in p['npi']:
            p_row[0, all_npi.index(npi)] = 1
        provider_coverage[p['plan']] = p_row
    if provider_coverage:
        fea_mat.append(provider_coverage)
        log.trace('complete for %d plans' %(len(provider_coverage)))

    log.trace('get summary feature for provider')
    all_provider_states = getProviderAllStates(prov_col, plan_ids)
    n_prov = len(all_provider_states)
    log.trace('total provider summary: %d' %(n_prov))

    log.trace('extract provider sumstat for each plan')
    provider_sumstat = {}
    for p in getProviderStateForPlans(prov_col, plan_ids):
        p_row = lil_matrix((1, n_prov))
        for d in p['plan_states']:
            p_row[0, all_provider_states.index(d['key'])] = d['count'] #[d['count'], d['location']]
        provider_sumstat[p['_id']] = p_row
    if provider_sumstat:
        fea_mat.append(provider_sumstat)
        log.trace('complete for %d plans' %(len(provider_sumstat)))

    return fea_mat
