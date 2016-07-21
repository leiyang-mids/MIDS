from sklearn.preprocessing import OneHotEncoder
from pymongo import MongoClient, DESCENDING
from zipfile import ZipFile, ZIP_DEFLATED
from scipy.sparse import *
from scipy import stats
from sklearn import svm
from sets import Set
import numpy as np
import json, sys, os, time, re, datetime, itertools, pickle

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
            extract_state_feature(state_plan, plan_col, drug_col, prov_col)
        except Exception as ex:

            print 'feature extraction has encountered error for state %s' %state


if __name__ == "__main__":
	main()
