import numpy as np
import pickle

def simulate_clicks(n_query = 8):
    ''''''

    savedData = 'feature/UT_74_19243.pickle'
    with open(savedData) as f:
        feature, plan = pickle.load(f)
    plan=np.array(plan)
    n_plan, n_fea = feature.shape
    sim_click = np.array([[plan[np.random.permutation(n_plan)],
                       plan[np.random.permutation(n_plan)[0:np.random.randint(low=1, high=10)]]]
                      for i in range(n_query)])
    q_cluster = np.random.random_integers(0,2,n_query)
    print 'simulated clicks with %d queries' %n_query
    return plan, q_cluster, sim_click
