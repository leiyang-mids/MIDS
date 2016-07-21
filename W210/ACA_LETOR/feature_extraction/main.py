from pymongo import MongoClient
from s3_helpers import *
import numpy as np

def main():
    '''
    main procedure to extract features for all states
    '''

    # connect to MongoDB and get collections
    m_url = 'ec2-54-153-83-172.us-west-1.compute.amazonaws.com'
    client = MongoClient(m_url, 27017)
    plan_col = client.plans.plans
    drug_col = client.formularies.drugs
    prov_col = client.providers.providers
    faci_col = client.providers.facilities
    print 'connected to MongoDB at %s' %m_url
    # parse out plan ID for states
    all_plan = drug_col.distinct('plans.plan_id')
    state_ids = np.unique([i[5:7] for i in all_plan])
    print 'find plan from %d states: %s' %(len(state_ids), ', '.join(state_ids))
    # run procedure for each state
    for state in state_ids:
        try:
            state_plan = [i for i in all_plan if state in i]
            print 'processing %d plans for %s' %(len(state_plan), state)
            plan, feature = get_state_feature(state_plan, plan_col, drug_col, prov_col)
            print 'completed feature extraction for %d plans, with dimension %s' %(len(plan), str(feature.shape))
            # savee pickle to s3
            save_name = 'feature/%s_%d_%d.pickle' %(state, feature.shape[0], feature.shape[1])
            with open(save_name, 'w') as f:
                pickle.dump([feature, plan], f)
            s3_helper().upload(save_name)
        except Exception as ex:
            traceback.print_exc()
            print 'feature extraction has encountered error for state %s' %state


if __name__ == "__main__":
	main()
