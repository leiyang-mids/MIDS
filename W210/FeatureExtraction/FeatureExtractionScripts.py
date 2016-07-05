from sklearn.preprocessing import OneHotEncoder
from pymongo import MongoClient
from zipfile import ZipFile, ZIP_DEFLATED
from sets import Set
import numpy as np
import json, sys, os, time, re, datetime

def logTime():
    return str(datetime.datetime.now())

def getEncodeFields(encode_def, rtn, path=''):
    ''' extract the selected fields from the encode json definition '''
    if 'encode' in encode_def and encode_def['encode'] == 1:
        rtn[path[1:]] = encode_def['type']
    elif 'encode' not in encode_def:
        for f in encode_def:
            getEncodeFields(encode_def[f], rtn, path + ('' if f=='properties' else '.'+f))
    return rtn

if len(sys.argv) not in [2,3]:
    sys.exit('only take 1 (mongo address) or 2 (mongo address & state) parameters')

mongo = sys.argv[1]
state = sys.argv[2].upper() if len(sys.argv)==3 else None

# connecting 'ec2-54-153-83-172.us-west-1.compute.amazonaws.com'
print '%s: connecting to Mongo at: %s' %(logTime(), mongo)
client = MongoClient(mongo, 27017)
plan_col = client.plans.plans
drug_col = client.formularies.drugs
prov_col = client.providers.providers
faci_col = client.providers.facilities
print '%s: MongoDB connected, total drug doc: %d, total plan doc: %d, total provider doc: %d' %(logTime(), drug_col.count(), plan_col.count(), prov_col.count())

# load encode config
print '%s: load encode configuration.' %logTime()
encode_list = getEncodeFields(json.load(open('encode2.json')), {})

# data uniformity check
print '%s: checking data uniformity ...' %logTime()
all_plan = drug_col.distinct('plans.plan_id')
all_drug = drug_col.distinct('rxnorm_id')
print '%s: plan document: %d' %(logTime(), plan_col.count())
print '%s: drug document: %d' %(logTime(), drug_col.count())
print '%s: unique plan_id: %d' %(logTime(), len(all_plan))
print '%s: unique rxnorm_id: %d' %(logTime(), len(all_drug))

multi_plan = [1 for p in plan_col.aggregate([{"$group": {"_id":"$plan_id", "count":{"$sum":1}}}]) if p['count']>1]
print '%s: plans with multiple documents: %d' %(logTime(), sum(multi_plan))

multi_drug = [1 for p in drug_col.aggregate([{"$group": {"_id":"$rxnorm_id", "count":{"$sum":1}}}]) if p['count']>1]
print '%s: drugs with multiple documents: %d' %(logTime(), sum(multi_drug))

state_id = np.unique([i[5:7] for i in all_plan])
print '%s: states in the plan: %s' %(logTime(), ', '.join(state_id))

if state not in state_id:
    sys.exit('your state %s does not have any matching plans' %state)

# get feature space
print '%s: retrieving feature space ...' %logTime()
ex_id = all_plan if not state else [i for i in all_plan if state in i]
print '%s: processing %d plans for %s ...' %(logTime(), len(ex_id), 'all states' if not state else state)

feature_space = {
    k : (plan_col if k.startswith('plan') else drug_col).find(
        { ('plan_id' if k.startswith('plan') else 'plans.plan_id') : {'$in':ex_id} }
    ).distinct(k[k.index('.') + 1:])
    for k,v in encode_list.items() if v=='string'
}

# get drugs from all selected plans
print '%s: retrieving drugs for selected plans ...'
common_drug = drug_col.find({'plans.plan_id':{'$in':ex_id}}).distinct('rxnorm_id')
n_drug = len(common_drug)
drug_attr = [[k.split('.')[-1],v,k] for k,v in encode_list.items() if k.startswith ('drug')]
# add one feature to indicate if the drug is included
drug_cat_index = ([False]+[k[1]=='string' for k in drug_attr])*n_drug
print '%s: there are %d drugs for %d plans!' %(logTime(), n_drug, len(ex_id))

# Get unique pharmacy_type for each drug_tier
print '%s: retrieving unique pharmacy_type for each drug_tier ...' %logTime()
tier_pharm = {}
for p in plan_col.find({'plan_id':{'$in':ex_id}}):
    if 'formulary' not in p:
        continue
    if type(p['formulary']) is dict:
        p['formulary'] = [p['formulary']]
    for f in p['formulary']:
        if f['drug_tier'] not in tier_pharm:
            tier_pharm[f['drug_tier']]=[]
        if 'cost_sharing' not in f:
            continue
        for cs in f['cost_sharing']:
            tier_pharm[f['drug_tier']].append(cs['pharmacy_type'])
tier_pharm = {k:list(Set(v)) for k,v in tier_pharm.items()}

print '%s: evaluating feature dimension and variable index ...' %logTime()
# put tier names into list so the order is fixed for feature extraction
tiers = tier_pharm.keys()
# we build pharmacy type into the order of feature vector, so no need to include
cost_attr = [[k.split('.')[-1],v,k] for k,v in encode_list.items()
             if 'cost_sharing' in k and 'pharmacy_type' not in k]
# flatten the vector to combine all tiers
cat2d = [[False] + [k[1]=='string' for k in cost_attr]*len(tier_pharm[t]) for t in tiers]
cost_cat_index = [y for x in cat2d for y in x]
# # plan level attributes
# plan_attr = [[k.split('.')[-1],v,k] for k,v in encode_list.items() if k.startswith('plan') and 'formulary' not in k]
# plan_cat_index = [a[1]=='string' for a in plan_attr]
# total feature catagrical index - must match with the order of feature canconnation in plan
cat_index = cost_cat_index + drug_cat_index
catagorical_var = [i for i,v in zip(range(len(cat_index)),cat_index) if v]
print '%s: feature dimension before encode: %d' %(logTime(), len(cat_index))

# extracting integer features for each plan
# for each plan, get int features (plan level & combined fomulary level)
plan_int_feature, i, skip = {}, 0, 0

for pid in ex_id:
    i += 1
    if i%10==0:
        print '%s: processing plans %d/%d ...' %(logTime(), i, len(ex_id))
    # initialize feature vector for formulary drug_tier
    tier_feature = [None]*len(tiers)

    # for each plan document, assemble normalized tier info
    for tier, pharm in tier_pharm.items():
        t_ind = tiers.index(tier)
        doc_cur = plan_col.find(
            {'plan_id':pid, 'formulary.drug_tier':tier},
            {'_id':0, 'formulary':{'$elemMatch':{'drug_tier':tier}}}
        )

        # no data for this tier, fill in the space with None (1 is for mail_order of the tier)
        n_doc = doc_cur.count()
        if n_doc == 0:
            tier_feature[t_ind] = [None] * (1 + len(pharm) * len(cost_attr))
            continue

        if n_doc > 1:
            print '\tWARNING: plan with multiple document:', pid, tier

        # parse tier info
        pharm_feature = [[None]*len(cost_attr)]*len(pharm)
        for doc in doc_cur:
            ##### query issue doc is empty when formulary is not array in plan document
            if not doc:
                print '\tWARNING, formulary is not array',pid,tier,n_doc
                fml = plan_col.find_one({'plan_id':pid, 'formulary.drug_tier':tier})['formulary']
            else:
                fml = doc['formulary'][0]
            # get mail_order for this tier
            tier_feature[t_ind] = [fml['mail_order']]

            # no pharmarcy type, only put mail_order (from one doc)
            if len(pharm) == 0 or 'cost_sharing' not in fml:
                continue

            # put pharmarcy types
            try:
                for cs in fml['cost_sharing']:
                    cost_feature = [cs[a[0]] if a[1]!='string'
                                    else feature_space[a[2]].index(cs[a[0]])
                                    for a in cost_attr]
                    pharm_feature[pharm.index(cs['pharmacy_type'])] = cost_feature
            except Exception as ex:
                print '\tERROR parsing cost_sharing value',ex,pid,tier
        # attach pharmacy info to tier feature: merge with mail_order info
        tier_feature[t_ind] += [y for x in pharm_feature for y in x]

    # flaten the vector for the plan from hierarchy: tier-cost-pharmacy
    formulary_feature = [y for x in tier_feature for y in x]
    if len(formulary_feature) != len(cost_cat_index):
        skip += 1
        print 'Error: plan feature dimension mismatch for %s' %pid
        continue

    # get the list of drug attributes for a plan
    drug_cur = drug_col.find(
        {'plans.plan_id':pid, 'rxnorm_id':{'$in':common_drug}},# 'plans.0':{'$exists':True}},
        {'_id':0, 'rxnorm_id':1, 'plans':{ '$elemMatch':{'plan_id':pid} }}
    )

    drug_dict = {d['rxnorm_id']:d['plans'][0] for d in drug_cur}

    # flat the drug attributes for all common drugs
    # add a flag here to indicate if the drug is covered
    drug_feature = []
    try:
        for rx in common_drug:
            if rx not in drug_dict:
                drug_feature += ([0]+[None]*len(drug_attr))
            else:
                drug_feature += ([1]+[drug_dict[rx][attr[0]] if attr[1]!='string'
                                      else feature_space[attr[2]].index(drug_dict[rx][attr[0]])
                                      for attr in drug_attr])
    except Exception as ex:
        skip += 1
        print '\tERROR parsing drug info, skip plan',ex,pid
        continue

    # combine for plan feature - must match with catagroical index concannation order
    plan_int_feature[pid] = formulary_feature + drug_feature

print '%s: completed processing %s plan, %d skipped due to parsing issue.' %(logTime(), state, skip)

# one-hot-encode
print '%s: one-hot encoding ...'
feature, IDs = [], []
for pid, fea in plan_int_feature.items():
    feature.append(fea)
    IDs.append(pid)

enc = OneHotEncoder(categorical_features=catagorical_var)
encode_feature = enc.fit_transform(feature)
print '%s: one-hot encode completed'








# the end - close client
client.close()
