from extract_provider_feature import *
from extract_drug_feature import *
from extract_plan_feature import *
from sets import Set
from scipy.sparse import *

def get_state_feature(state_plan, plan, drug, provider):
    '''
    state_plan - plan IDs for the state
    plan       - MongoDB plan collection
    drug       - MongoDB drug collection
    provider   - MongoDB provider collection
    '''
    # extract features from plan, drug, and provider
    fea_mat = extract_plan_feature(plan, state_plan)
    fea_mat += extract_drug_feature(drug, state_plan)
    fea_mat += extract_provider_feature(provider, state_plan)

    # get common keys (plan has all elements)
    valid_plan = Set(fea_mat[0].keys())
    for i in range(1, len(fea_mat)):
        valid_plan = valid_plan.intersection(fea_mat[i].keys())
    valid_plan = list(valid_plan)
    
    # combine all elements for each plan and return
    return valid_plan, csr_matrix([hstack([f[p] for f in fea_mat]) for p in valid_plan])
