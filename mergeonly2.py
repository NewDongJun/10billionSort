from os import terminal_size
import pickle
import time
import heapq
import math

sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 20000000
arraysize = 10000

chunkcnt = 51 #100개 인데 원래 파일 에서는 +1 하고 끝남
chunkReadList = []
start = time.time()

#성능 좋은 합치는 제너레이터 만들기
def chunkread(filename):
    f = open(filename, 'rb')
    try:
        for i in range(math.ceil(chunksize/arraysize)):
            temp = pickle.load(f)
            for num in temp:
                yield num
    except Exception as e:
        pass


#chunk들을 읽어올 객체 리스트 만들기
for i in range(1,chunkcnt):
    file = 'chunk'+str(i)+'.pickle'
    chunkReadList.append(chunkread(file))

#chunk들 읽어 오면서 정렬하기
f = open(sortedfilename, 'wb')
merge_data = heapq.merge(*chunkReadList)
cnt = 1
try:
    while True:
        temp = []
        for i in range(arraysize):
            temp.append(next(merge_data))
        pickle.dump(temp, f)
        if cnt%1000000 == 0:
            print(cnt)
        cnt += 1
except:
    pass
finally:
    f.close()

print('endtime :',time.time()-start)