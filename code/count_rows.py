import pandas

def main():
	apples = pandas.read_csv("/tmp/apples/2019-04-18.txt",sep=" ",names=['type','date','time'])
	numApples = apples.agg('count').type
	return numApples

if __name__ == '__main___':
	main()
