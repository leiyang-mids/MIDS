from datetime import datetime, timedelta
from s3_helpers import *
from get_rank_for_state_plan import *
from get_click_data import *
import traceback, pickle, time
import numpy as np

from simulate_clicks import *

def main():
	'''
	'''
	next_run, hour, minute = datetime.now(), 5, 10
	global s3clnt, log = s3_helper(), logger('training')

	while True:
		try:
			if datetime.now() < next_run:
				time.sleep((next_run-datetime.now()).seconds)
				continue
			# main procedure starts here
			print 'get query and click data '
			click_data = get_click_data()
			for state in np.unique(click_data['state']):
				s_rows = click_data[click_data['state']==state]
				print 'characterize queries for the state'
				q_cluster, q_characterizer, centroids = query_characterizer(s_rows)
				print 'run letor training for the state'
				letor_rank = get_rank_for_state_plan(q_cluster, np.array([[r['ranks'],r['clicks']] for r in s_rows]))
				print 'save result on s3, for ES indexing'
				save_name = 'training/%s_%d.pickle' %(state, len(letor_rank))
				with open(save_name, 'w') as f:
					pickle.dump([plans, letor_rank], f)
				s3clnt.delete_by_state('training/%s' %state)
	            s3clnt.upload(save_name)
	            s3clnt.set_public(save_name)
			# training completed, get next run time
			next_run = datetime.now() + timedelta(hours=hour)
			print 'training has completed, next run time is %s' %str(next_run)
		except KeyboardInterrupt:
			break
			# sys.exit('User termination')
		except Exception as ex:
			traceback.print_exc(file=log.log_handler())
			print 'training has encountered an error, restart in %s minutes ...' %minute
			next_run = datetime.now() + timedelta(minutes=minute)

		# upload log file to S3
		s3clnt.upload2(log.log_name(), 'log/'+log.log_name())

if __name__ == "__main__":
	main()
