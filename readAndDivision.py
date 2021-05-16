import array
import pickle
import time
import math

filename = '10billion.int'
sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 10000000 #약160메가 좀 넘는 용량
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
    except EOFError:
        if len(data)>0: #남아 있는 데이터가 chunk사이즈 보다 작을 때
            pass
        else:           #남아 있는 데이터가 없을 경우
            break
    print('read chunk'+str(chunkcnt)+':', time.time()-start)
    

    #sort
    sortedData = sorted(data)
    print('sort chunk'+str(chunkcnt)+':', time.time()-start)


    #store 이 부분에서 데이터를 어레이로 묶어서 묶은 단위 만큼 저장
    chunk = open('chunk'+str(chunkcnt)+'.pickle','wb')
    try:
        slicestart = 0
        for i in range(math.ceil(chunksize/arraysize)):
            temp = sortedData[slicestart:slicestart + arraysize]
            pickle.dump(temp, chunk)
            slicestart += arraysize
            #block.tofile(chunk) #어레이 사이즈 만큼 읽어서 파일에 한번에 저장
    except:
        temp = sortedData[slicestart:]
        pickle.dump(temp, chunk)
    finally:
        chunk.close()
        print('store chunk'+str(chunkcnt)+':', time.time()-start)
        chunkcnt += 1
f.close()