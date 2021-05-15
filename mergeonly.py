import array
import time
import heapq

sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 10000000
arraysize = 10000

chunkcnt = 101 #100개 인데 원래 파일 에서는 +1 하고 끝남
chunkReadList = []
start = time.time()

#성능 좋은 합치는 제너레이터 만들기
def chunkread(filename):
    f = open(filename, 'rb')
    data = array.array(datatype)
    try:
        while True:
            data  = array.array(datatype)
            #이거는 원래 청크에서도 실험해 볼 필요가 있음
            data.fromfile(f,arraysize)
            for i in data:
                yield i
    except :
        if len(data) >0:
            for i in data:
                yield i
        else:
            pass


#chunk들을 읽어올 객체 리스트 만들기
for i in range(1,chunkcnt):
    file = 'chunk'+str(i)+'.pickle'
    chunkReadList.append(chunkread(file))

#chunk들 읽어 오면서 정렬하기
f = open(sortedfilename, 'wb')
merge_data = heapq.merge(*chunkReadList)
try:
    block = None
    while True:
        block = array.array(datatype)
        for i in range(arraysize):
            block.append(next(merge_data))
        block.tofile(f)
except:
    if len(block)>0:
        block.tofile(f)
    else:
        pass
finally:
    f.close()

print('endtime :',time.time()-start)