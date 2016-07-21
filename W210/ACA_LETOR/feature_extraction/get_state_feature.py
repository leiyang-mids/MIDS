from aca_drug_feature import *
from aca_plan_feature import *
from extract_plan_feature import *

def get_state_feature(state_plan, plan, drug, provider):
    '''
    state_plan - plan IDs for the state
    plan       - MongoDB plan collection
    drug       - MongoDB drug collection
    provider   - MongoDB provider collection
    '''

    fea_mat = extract_plan_feature(plan, state_plan)

    fea_mat += extract_drug_feature(drug, )

    print '8/11 get provider under the plans'
    all_npi = prov_col.find({'plans.plan_id':{'$in':valid_plan3}}).distinct('npi')
    print 'total providers: %d' %(len(all_npi))

    print '9/11 check provider coverage for each plan' ##### slow #####
    provider_coverage = lil_matrix((n_plan, len(all_npi)))
    valid_plan4 = []
    for p in getProviderListForPlans(prov_col, valid_plan3):
        valid_plan4.append(p['plan'])
        r_id = state_plan.index(p['plan'])
        for npi in p['npi']:
            provider_coverage[r_id, all_npi.index(npi)] = 1
    print 'complete for %d plans' %(len(valid_plan4))

    print '10/11 get summary feature for provider'
    all_provider_states = getProviderAllStates(prov_col, valid_plan4)
    print 'total provider summary: %d' %(len(all_provider_states))

    print '11/11 extract provider sumstat for each plan'
    provider_sumstat = lil_matrix((n_plan, len(all_provider_states)))
    valid_plan5 = []
    for p in getProviderStateForPlans(prov_col, valid_plan4):
        r_id = state_plan.index(p['_id'])
        valid_plan5.append(p['_id'])
        for d in p['plan_states']:
            provider_sumstat[r_id, all_provider_states.index(d['key'])] = d['count'] #[d['count'], d['location']]
    print 'completed feature extraction for %d plans' %(len(valid_plan5))
