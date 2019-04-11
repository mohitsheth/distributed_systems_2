import multiprocessing as mp 
from collections import defaultdict
from os import listdir
from sys import argv
from xsorted import xsorted

def map_func(filename=None):
	
	res=defaultdict(set)
	with open(filename,'r') as file:
	
		for line in file:
			line=line.strip()
			person,friends=line.split('\t')
			friends=sorted(friends.split(','))
			for i in range(len(friends)):
				for j in range(i+1,len(friends)):
					res[(friends[i],friends[j])].add(person)
	with open('int-'+filename.split('-')[1],'w') as file:

		for key in res:

			file.write(key[0]+","+key[1]+"\t"+",".join([x for x in res[key]])+"\n")

	


def reduce_func(inputs):

	
	with open('out-'+mp.current_process().name.split('-')[1]+'.txt','a+') as file:
		
		mapper={}
		pair=inputs.keys()[0]
		
		for mutual_friend_list in inputs.values():
			for mutual_friends in mutual_friend_list:
				for friend in mutual_friends:
					mapper[friend]=1

		file.write(pair[0]+","+pair[1]+"\t"+",".join([x for x in mapper.keys()])+"\n")
		


def merge(files):
	for file in files:
		with open(file,'r') as f:
			for line in f:

				line=line.strip()
				pair,friends=line.split('\t')
				pair=tuple(pair.split(','))
				friends=set(friends.split(','))
				yield (pair,friends)


def shuffle():
	prev=None
	cur=defaultdict(list)
	for el in xsorted(merge([filename for filename in listdir('.') if filename.startswith("int-")]),key=lambda x:x[0]):
		current=el[0]
		
		if not prev or current==prev:
			cur[current].append(el[1])
			prev=current
		else:
			
			yield cur
			prev=None
			cur=defaultdict(list)

if __name__ == '__main__':
	
	if len(argv)>1:
		pool_size=int(argv[1])
	else:
		pool_size=2
	Pool=mp.Pool(pool_size)
  
	mappers=Pool.map_async(map_func,[filename for filename in listdir('.') if filename.startswith("inp-")],chunksize=1)
	
	mappers.wait()

	
	reducers=Pool.map_async(reduce_func,shuffle(),chunksize=10)
	
	reducers.wait()
