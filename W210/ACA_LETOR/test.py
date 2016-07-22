import traceback

f = open('traceTest.txt', 'w')

def test():
    try:
        a = int0('23')
    except:
        traceback.print_exc(file=f)
        print 'done saving trace'

if __name__ == "__main__":
	test()
