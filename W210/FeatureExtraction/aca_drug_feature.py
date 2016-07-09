# get state space of drugs - count for all combinations of drug_tier/step_therapy/quantity_limit/prior_authorization
def getDrugAggregateAllStates(drug_collection, plans):
    for d in drug_collection.aggregate(
        [
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$group':{
                    '_id':{
                           'ti':'$plans.drug_tier',
                           'st':'$plans.step_therapy',
                           'ql':'$plans.quantity_limit',
                           'pa':'$plans.prior_authorization',
                           },
                }
            },
            {'$project':{
                    'drug_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA',
                                      '$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.st',None]},{'$eq':['$_id.st','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.st',True]},'Y','N']}]},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ql',None]},{'$eq':['$_id.ql','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.ql',True]},'Y','N']}]},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.pa',None]},{'$eq':['$_id.pa','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.pa',True]},'Y','N']}]},
                        ]
                    },
                }
            },
            {'$group':{'_id':None, 'state':{'$addToSet':'$drug_state'}}},
        ]
    ):
        states = d['state']
    return states


def getDrugListForPlans(drug_collection, plans):
    '''get rxnorm_id list for a group of plans'''
    return drug_col.aggregate(
        [
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$group':{'_id':'$plans.plan_id', 'drugs': {'$addToSet':'$rxnorm_id'}}},
            {'$project':{'plan':'$_id', 'drug':'$drugs', '_id':0 }},
        ]
    )


# get state space of drugs - count for all combinations of drug_tier/step_therapy/quantity_limit/prior_authorization
def getDrugAggregateCountForPlans(drug_collection, plans):
    return drug_collection.aggregate(
        [
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$group':{
                    '_id':{'pl':'$plans.plan_id',
                           'ti':'$plans.drug_tier',
                           'st':'$plans.step_therapy',
                           'ql':'$plans.quantity_limit',
                           'pa':'$plans.prior_authorization',
                           },
                    'cnt':{'$sum':1}
                }
            },
            {'$project':{
                    'pid':'$_id.pl',
                    'drug_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA',
                                      '$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.st',None]},{'$eq':['$_id.st','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.st',True]},'Y','N']}]},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ql',None]},{'$eq':['$_id.ql','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.ql',True]},'Y','N']}]},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.pa',None]},{'$eq':['$_id.pa','']}]},'NA',
                                      {'$cond':[{'$eq':['$_id.pa',True]},'Y','N']}]},
                        ]
                    },
                    'count':'$cnt', '_id':0
                }
            },
            {'$group':{'_id':'$pid', 'state':{'$push':{'key':'$drug_state','cnt':'$count'}}}},
            {'$project':{'plan':'$_id', 'drug_state':'$state', '_id':0}}
        ]
    )
