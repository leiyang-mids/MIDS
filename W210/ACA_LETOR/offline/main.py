from datetime import datetime, timedelta
from s3_helpers import *
from get_rank_for_state_plan import *
from get_click_data import *
import traceback, pickle
import numpy as np

from simulate_clicks import *

def main():
	# execute cyclically
	next_run = datetime.now()
	interval = 5
	while True:
		try:
			if datetime.now() < next_run:
				continue
			next_run += timedelta(hours=interval)
			# main procedure starts here
			print 'get query and click data '
			click_data = get_click_data()
			print 'group sessions by state'
			states = np.unique(click_data['state'])
			print 'characterize queries for each state'

			print 'run letor training for each state'
			plans, q_cluster, clicks = simulate_clicks(8)
			letor_rank = get_rank_for_state_plan(q_cluster, clicks)

			print 'save result on s3, for ES indexing'
			save_name = 'training/%s_%d.pickle' %('UT', len(letor_rank))

			with open(save_name, 'w') as f:
				pickle.dump([plans, letor_rank], f)

			# print '%s: feature matrix saved as %s' %(logTime(), saveName)
			s3clnt = s3_helper()
            s3clnt.upload(save_name)
            s3clnt.set_public(save_name)
			print 'next run time is %s' %str(next_run)

		except KeyboardInterrupt:
			break
			# sys.exit('User termination')
		except Exception as ex:
			traceback.print_exc()
			print 'Execution has encountered an error, restart in 10 minutes ...'
			next_run = datetime.now() + timedelta(minutes=10)

		# upload log file to S3


if __name__ == "__main__":
	main()
