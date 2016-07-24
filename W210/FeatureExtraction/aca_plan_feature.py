# get plan state - combinations of drug_tier/pharmacy_type/copay_opt/coinsurance_opt for one plan
def getFormularyStatesForPlan1(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    return plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}, 'formulary.cost_sharing':{'$exists':True}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$formulary.cost_sharing'},
            {'$unwind':'$network'},
            # still use a group stage so the context can collapse into key
            {'$group':{
                    '_id':{
                        'pl':'$plan_id',
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'ph':'$formulary.cost_sharing.pharmacy_type',
                        'cp':'$formulary.cost_sharing.copay_opt',
                        'ci':'$formulary.cost_sharing.coinsurance_opt',
                        # 'nt':'$network.network_tier',
                    },
                    'cnt':{'$sum':1},
                }
            },
            {'$project':{
                    '_id':0,
                    'plan':'$_id.pl',
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ph',None]},{'$eq':['$_id.ph','']}]},'NA','$_id.ph']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.cp',None]},{'$eq':['$_id.cp','']}]},'NA','$_id.cp']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ci',None]},{'$eq':['$_id.ci','']}]},'NA','$_id.ci']},'|',
                            # {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                    {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                    'count':'$cnt',
                }
            },
            {'$group':{'_id':'$plan', 'count':{'$addToSet':'$count'}, 'plan_states':{'$addToSet':'$plan_state'}}},
            {'$sort':{'plan':1}}
        ], allowDiskUse=True
    )

def getFormularyStatesForPlan2(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    return plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}, 'formulary.costSharing':{'$exists':True}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$formulary.costSharing'},
            {'$unwind':'$network'},
            # still use a group stage so the context can collapse into key
            {'$group':{
                    '_id':{
                        'pl':'$plan_id',
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'ph':'$formulary.costSharing.pharmacyType',
                        'nt':'$network.network_tier',
                    },
                    'cnt':{'$sum':1},
                }
            },
            {'$project':{
                    '_id':0,
                    'plan':'$_id.pl',
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ph',None]},{'$eq':['$_id.ph','']}]},'NA','$_id.ph']},'|',
                            'NA|NA|', # no copay_opt and coinsurance_opt for this type of plan, so put NA instead
                            {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                    {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                    'count':'$cnt',
                }
            },
            {'$group':{'_id':'$plan', 'count':{'$addToSet':'$count'}, 'plan_states':{'$addToSet':'$plan_state'}}},
            {'$sort':{'plan':1}}
        ], allowDiskUse=True
    )

def getFormularyStatesForPlan3(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    return plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans},'formulary.costSharing':{'$exists':False},'formulary.cost_sharing':{'$exists':False}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$network'},
            # still use a group stage so the context can collapse into key
            {'$group':{
                    '_id':{
                        'pl':'$plan_id',
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'nt':'$network.network_tier',
                    },
                    'cnt':{'$sum':1},
                }
            },
            {'$project':{
                    '_id':0,
                    'plan':'$_id.pl',
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            'NA|NA|NA|', # no copay_opt and coinsurance_opt for this type of plan, so put NA instead
                            {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                    {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                    'count':'$cnt',
                }
            },
            {'$group':{'_id':'$plan', 'count':{'$addToSet':'$count'}, 'plan_states':{'$addToSet':'$plan_state'}}},
            {'$sort':{'plan':1}}
        ], allowDiskUse=True
    )

# get plan state - unique combinations of drug_tier/pharmacy_type/copay_opt/coinsurance_opt from all plans
def getFormularyAllStates1(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    states = []
    for p in plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}, 'formulary.cost_sharing':{'$exists':True}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$formulary.cost_sharing'},
            {'$unwind':'$network'},
            # {'$unwind':'$network.network_tier'},
            # group context from all plans
            {'$group':{
                    '_id':{
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'ph':'$formulary.cost_sharing.pharmacy_type',
                        'cp':'$formulary.cost_sharing.copay_opt',
                        'ci':'$formulary.cost_sharing.coinsurance_opt',
                        # 'nt':'$network.network_tier',
                    },
                }
            },
            {'$project':{
                    '_id':0,
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ph',None]},{'$eq':['$_id.ph','']}]},'NA','$_id.ph']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.cp',None]},{'$eq':['$_id.cp','']}]},'NA','$_id.cp']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ci',None]},{'$eq':['$_id.ci','']}]},'NA','$_id.ci']},'|',
                            # {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                }
            },
            {'$group':{'_id':None, 'count':{'$addToSet':'$count'}, 'all_states':{'$addToSet':'$plan_state'}}},
        ], allowDiskUse=True
    ):
        states = p['all_states']
    return states

def getFormularyAllStates2(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    states = []
    for p in plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}, 'formulary.costSharing':{'$exists':True}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$formulary.costSharing'},
            {'$unwind':'$network'},
            # group context from all plans
            {'$group':{
                    '_id':{
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'ph':'$formulary.costSharing.pharmacyType',
                        'nt':'$network.network_tier',
                    },
                }
            },
            {'$project':{
                    '_id':0,
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.ph',None]},{'$eq':['$_id.ph','']}]},'NA','$_id.ph']},'|',
                            'NA|NA|',
                            {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                    {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                }
            },
            {'$group':{'_id':None, 'count':{'$addToSet':'$count'}, 'all_states':{'$addToSet':'$plan_state'}}},
        ], allowDiskUse=True
    ):
        states = p['all_states']
    return states

def getFormularyAllStates3(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    states = []
    for p in plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}, 'formulary.cost_sharing':{'$exists':False}, 'formulary.costSharing':{'$exists':False}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$network'},
            # group context from all plans
            {'$group':{
                    '_id':{
                        'ti':'$formulary.drug_tier',
                        'mo':'$formulary.mail_order',
                        'nt':'$network.network_tier',
                    },
                }
            },
            {'$project':{
                    '_id':0,
                    'plan_state':{
                        '$concat':[
                            {'$cond':[{'$or':[{'$eq':['$_id.ti',None]},{'$eq':['$_id.ti','']}]},'NA','$_id.ti']},'|',
                            'NA|NA|NA|',
                            {'$cond':[{'$or':[{'$eq':['$_id.nt',None]},{'$eq':['$_id.nt','']}]},'NA','$_id.nt']},'|',
                            {'$cond':[{'$or':[{'$eq':['$_id.mo',None]},{'$eq':['$_id.mo','']}]},'NA',
                                        {'$cond':[{'$eq':['$_id.mo',True]},'Y','N']}]},
                        ]
                    },
                }
            },
            {'$group':{'_id':None, 'count':{'$addToSet':'$count'}, 'all_states':{'$addToSet':'$plan_state'}}},
        ], allowDiskUse=True
    ):
        states = p['all_states']
    return states


# get the mean value of copay_amout and coinsurance_rate (over all tier/pharmacy/copay/coinsurance options) for a plan
def getFormularyAggregate(plan_collection, plans):
    if type(plans) is str:
        plans = [plans]
    return plan_collection.aggregate(
        [
            {'$match':{'plan_id':{'$in':plans}}},
            {'$unwind':'$formulary'},
            {'$unwind':'$formulary.cost_sharing'},
            {'$group':{
                '_id':{'plan':'$plan_id'},
                'a_cp':{'$avg':'$formulary.cost_sharing.copay_amount'},
                'a_in':{'$avg':'$formulary.cost_sharing.coinsurance_rate'},
                'cnt':{'$sum':1},
                }
            },
            {'$project':{
                '_id':0,
                'plan':'$_id.plan',
                'avg_copay':'$a_cp',
                'avg_ci_rate':'$a_in',
                'count':'$cnt',
                }
            },
            {'$sort':{'plan':1}}
        ], allowDiskUse=True
    )
