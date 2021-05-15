import array
import pickle
import time
import heapq
from typing_extensions import ParamSpec

filename = '10billion.int'
sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 10000000
arraysize = 10000

chunkcnt = 1
chunckReadList = []
start = time.time()

#chunksize 만큼 읽어서 소팅하고 저장하기
f = open(filename,'rb')
while True:
    data = array.array(datatype)

    #data read 부분
    try:
        for i in range(chunksize):
            data.append(pickle.load(f))
        print('read chunk'+str(chunkcnt)+':', time.time()-start)
    except EOFError:
        if len(data)>0: #남아 있는 데이터가 chunk사이즈 보다 작을 때
            pass
        else:           #남아 있는 데이터가 없을 경우
            break

    #sort
    sortedData = sorted(data)
    print('sort chunk'+str(chunkcnt)+':', time.time()-start)

    #store 이 부분에서 데이터를 어레이로 묶어서 묶은 단위 만큼 저장
    chunk = open('chunk'+str(chunkcnt)+'.pickle','wb')
    block = array.array(datatype)
    

f.close()