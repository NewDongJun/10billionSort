import array
import pickle
import time
import heapq

sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 10000000

chunkcnt = 101 #100개 인데 원래 파일 에서는 +1 하고 끝남
chunckReadList = []
start = time.time()

#성능 좋은 합치는 제너레이터 만들기
def 


#chunk들을 읽어올 객체 리스트 만들기
for i in range(1,chunkcnt):
    file = 'chunk'+str(i)+'.pickle'
    chunckReadList.append(chunckread(file))

#chunk들 읽어 오면서 정렬하기
f = open(sortedfilename, 'wb')
merge_data = heapq.merge(*chunckReadList)
for i in merge_data:
    pickle.dump(i, f)
f.close()
print('endtime :',time.time()-start)