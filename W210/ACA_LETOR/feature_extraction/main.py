from pymongo import MongoClient
from get_state_feature import *
from s3_helpers import *
from logger import *
import numpy as np
import traceback, pickle

def main():
    '''
    main procedure to extract features for all states
    '''
    log, s3clnt = logger('feature'), s3_helper()
    # connect to MongoDB and get collections
    m_url = 'ec2-52-53-230-141.us-west-1.compute.amazonaws.com'
    client = MongoClient(m_url, 27017)
    plan_col = client.plans.plans
    drug_col = client.formularies.drugs
    prov_col = client.providers.providers
    faci_col = client.providers.facilities
    log.trace('connected to MongoDB at %s' %m_url)
    # parse out plan ID for states
    all_plan = drug_col.distinct('plans.plan_id')
    state_ids = np.unique([i[5:7] for i in all_plan])
    log.trace('find plan from %d states: %s' %(len(state_ids), ', '.join(state_ids)))
    # run procedure for each state
    failure = []
    for state in state_ids:
        try:
            state_plan = [i for i in all_plan if state in i]
            log.trace('processing %d plans for %s' %(len(state_plan), state))
            plan, feature = get_state_feature(state_plan, plan_col, drug_col, prov_col, log)
            log.trace('completed feature extraction for %d plans, with dimension %s' %(len(plan), str(feature.shape)))
            # savee pickle to s3
            save_name = 'feature/%s_%d_%d.pickle' %(state, feature.shape[0], feature.shape[1])
            with open(save_name, 'w') as f:
                pickle.dump([feature, plan], f)
            s3clnt.delete_by_state('feature/%s' %state)
            s3clnt.upload(save_name)
            # s3clnt.set_public(save_name)
            log.trace('feature pickle saved to s3, complete for %s' %state)
        except Exception as ex:
            traceback.print_exc(file=log.log_handler())
            failure.append(state)
            log.error('feature extraction has encountered error for state %s' %state)

    log.trace('feature extraction completed, faied for %d states: %s' %(len(failure), ', '.join(failure)))
    log.close()
    # put log on S3
    s3clnt.upload2(log.log_file(), 'log/'+log.log_file())

if __name__ == "__main__":
	main()
