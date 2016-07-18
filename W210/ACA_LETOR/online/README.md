# ACA LETOR Runtime Executer

###class
    letor_online(queries, plan_ranks)
- **queries**: list of strings for queries, for example
    _['Glaucoma',
      'diabetes Obesophobia',
      'Obesophobia diabetes Alzheimer',
      'Glaucoma diabetes Alzheimer Hypertension',
      'Glaucoma Alzheimer diabetes Obesophobia Hypertension']_
- **plan_ranks**: ranks of plans for each query
- method: get_rank(query), obtain rank for current query

###runtime data:
- located at s3: https://s3.amazonaws.com/w210.data/online/runtime_data_SS.pickle
- replace _SS_ with state abbreviation, e.g. OR, NJ etc.
- each file contain two data for **letor_online** class initialization:
 - queries: list of queries learned for the plans
 - plan_ranks: plan ranking information

###Example:
    savedData = 'runtime_data_OR.pickle'
    with open(savedData) as f:
    plan_ranks, queries = pickle.load(f)

    letor = letor_online(queries, plan_ranks)
    rank = letor.get_rank('glaucoma')
