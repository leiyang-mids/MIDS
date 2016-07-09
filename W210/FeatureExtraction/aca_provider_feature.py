# get provider features
def getProviderAllStates(provider_collection, plans):
    for p in provider_collection.aggregate(
        [
            {'$match':{'facility_name':{'$exists':False}}},
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$unwind':'$speciality'},
            {'$unwind':'$languages'},
            {'$group':{
                    '_id':{
                        'sp':'$speciality',
                        'ac':'$accepting',
                        'lg':'$languages',
                        'pn':'$plans.network_tier',
                        'ty':'$type',
                    },
                }
            },
            {'$project':{
                    '_id':0,
                    'prov_state':{'$concat':[
                            '$_id.sp','|','$_id.ty','|','$_id.ac','|','$_id.lg','|','$_id.pn'
                    ]},
                }
            },
            {'$group':{'_id':None, 'states':{'$addToSet':'$prov_state'}}},
        ]
    ):
        states = p['states']
    return states


def getProviderStateForPlans(provider_collection, plans):
    return provider_collection.aggregate(
        [
            {'$match':{'facility_name':{'$exists':False}}},
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$unwind':'$speciality'},
            {'$unwind':'$languages'},
            {'$group':{
                    '_id':{
                        'pl':'$plans.plan_id',
                        'sp':'$speciality',
                        'ac':'$accepting',
                        'lg':'$languages',
                        'pn':'$plans.network_tier',
                        'ty':'$type',
                    },
                    'cnt':{'$sum':1},
                    'loc':{'$sum':{'$size':'$addresses'}}
                }
            },
            {'$project':{
                    '_id':0,
                    'plan':'$_id.pl',
                    'prov_state':{'$concat':[
                            '$_id.sp','|','$_id.ty','|','$_id.ac','|','$_id.lg','|','$_id.pn'
                    ]},
                    'count':'$cnt',
                    'location':'$loc',
                }
            },
            {'$group':{
                    '_id':'$plan',
                    'plan_states':{'$push':{'key':'$prov_state','count':'$count','location':'$location'}}
                }
            },
        ]
    )


# get npi list for a group of plans
def getProviderListForPlans(provider_collection, plans):
    return provider_collection.aggregate(
        [
            {'$unwind':'$plans'},
            {'$match':{'plans.plan_id':{'$in':plans}}},
            {'$group':{'_id':'$plans.plan_id', 'providers': {'$addToSet':'$npi'}}},
            {'$project':{'plan':'$_id', 'npi':'$providers', '_id':0 }},
        ]
    )
