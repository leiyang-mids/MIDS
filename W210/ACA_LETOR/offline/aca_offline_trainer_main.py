from datetime import datetime, timedelta
import sys, traceback

def main():
	# execute cyclically
	next_run = datetime.now()
	interval = 5
	while True:
		try:
			if datetime.now() > next_run:
				next_run += timedelta(hours0=interval)
				# main procedure starts here
				print 'get query and click data '

				print 'group sessions by state'

				print 'characterize queries for each state'

				print 'run letor training for each state'

				print 'save result on s3, for ES indexing'
				
		except KeyboardInterrupt:
			sys.exit('User termination')
		except Exception as ex:
			traceback.print_exc()
			print 'Execution has encountered an error, restart in 10 minutes ...'
			next_run = datetime.now() + timedelta(seconds=10)


if __name__ == "__main__":
	main()
