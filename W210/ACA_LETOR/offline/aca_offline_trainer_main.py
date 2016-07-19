from datetime import datetime, timedelta

def main():	
	# execute cyclically
	next_run = datetime.now()
	interval = 5
	while True:		
		try:			
			if datetime.now() > next_run:
				next_run += timedelta(seconds=interval)
				# main procedure starts here
				print 'test', str(datetime.now())
		except KeyboardInterrupt:
			print 'user termination'
			sys.exit(1)
		except:
			print 'execution error, restart now'
			next_run = datetime.now()
		
if __name__ == "__main__":
	main()
		