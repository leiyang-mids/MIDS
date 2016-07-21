from datetime import datetime, timedelta
from s3_helpers import *
from get_rank_for_state_plan import *
import sys, traceback, pickle

from simulate_clicks import *

def main():
	# execute cyclically
	next_run = datetime.now()
	interval = 5
	while True:
		try:
			if datetime.now() > next_run:
				next_run += timedelta(hours=interval)
				# main procedure starts here
				print 'get query and click data '

				print 'group sessions by state'

				print 'characterize queries for each state'

				print 'run letor training for each state'
				plans, q_cluster, clicks = simulate_clicks(8)
				letor_rank = get_rank_for_state_plan(q_cluster, clicks)

				print 'save result on s3, for ES indexing'
				saveName = 'training/%s_%d.pickle' %('UT', len(letor_rank))

				with open(saveName, 'w') as f:
					pickle.dump([plans, letor_rank], f)

				# print '%s: feature matrix saved as %s' %(logTime(), saveName)
				s3_helper().upload_training_pickle(saveName)
				print 'next run time is %s' %str(next_run)

		except KeyboardInterrupt:
			sys.exit('User termination')
		except Exception as ex:
			traceback.print_exc()
			print 'Execution has encountered an error, restart in 10 minutes ...'
			next_run = datetime.now() + timedelta(minutes=10)

		# upload log file to S3


if __name__ == "__main__":
	main()
