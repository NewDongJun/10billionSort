import array
import _pickle as pickle
import time
import heapq

filename = '10billion.int'
sortedfilename = '10billion.sorted'
datatype = 'i'
chunksize = 10000000

chunkcnt = 1
chunckReadList = []
start = time.time()


#chunksize 만큼 읽어서 소팅하고 저장하기
f = open(filename,'rb')

while True:
    data = array.array('i')

    #데이터 read 이 부분도 예외 처리가 필요 하네요 중요!!!!
    #for 문이 아니라 while 을 활용할 필요가 있어 보임
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
    
    #store
    chunk = open('chunk'+str(chunkcnt)+'.pickle','wb')
    for i in sortedData:
        pickle.dump(i,chunk)
    chunk.close()
    print('store chunk'+str(chunkcnt)+':', time.time()-start)
    
    chunkcnt += 1

f.close()


#제너레이터 객체 이게 왜 함수로 선언해도 객체로 할당 됨? 원래 그런가?
def chunckread(filename):
    f = open(filename, 'rb')
    try:
        while True:
            yield pickle.load(f)
    except:
        pass
    finally:
        f.close()


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