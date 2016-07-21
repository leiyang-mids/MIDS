from query_provider_feature import *
from scipy.sparse import *

def extract_provider_feature(prov_col, plan_ids):
    '''
    '''
    fea_mat = []
    print 'get provider under the plans'
    all_npi = prov_col.find({'plans.plan_id':{'$in':plan_ids}}).distinct('npi')
    n_npi = len(all_npi)
    print 'total providers: %d' %n_npi

    print 'check provider coverage for each plan' ##### slow #####
    provider_coverage = {}
    for p in getProviderListForPlans(prov_col, plan_ids):
        p_row = lil_matrix((1, n_npi))
        for npi in p['npi']:
            p_row[0, all_npi.index(npi)] = 1
        provider_coverage[p['plan']] = p_row
    fea_mat.append(provider_coverage)
    print 'complete for %d plans' %(len(provider_coverage))

    print 'get summary feature for provider'
    all_provider_states = getProviderAllStates(prov_col, plan_ids)
    n_prov = len(all_provider_states)
    print 'total provider summary: %d' %(n_prov)

    print 'extract provider sumstat for each plan'
    provider_sumstat = {}
    for p in getProviderStateForPlans(prov_col, valid_plan4):
        p_row = lil_matrix((1, n_prov))
        for d in p['plan_states']:
            p_row[0, all_provider_states.index(d['key'])] = d['count'] #[d['count'], d['location']]
        provider_sumstat[p['_id']] = p_row
    fea_mat.append(provider_sumstat)

    return fea_mat
