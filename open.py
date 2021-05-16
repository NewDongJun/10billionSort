import pickle
import math

filename = 'chunk55.pickle'
chunksize = 10000000 #약160메가 좀 넘는 용량
arraysize = 10000

f = open(filename,'rb')
cnt = 1
try:
    for i in range(math.ceil(chunksize/arraysize)):
        temp = pickle.load(f)
        for j in temp:
            if cnt%1000000 == 0:
                print(cnt, j)
            cnt += 1
except Exception as e:
    print(e)
print(temp[-1])