import random as rd
import pickle
import time
import math

totalsize = 1000000000
arraysize = 10000
start = time.time()

#10억 수 바이너리로 생성?
cnt = 1
f = open('10billion2.int','wb')
for i in range(math.ceil(totalsize/arraysize)):
    temp = []
    for j in range(arraysize):
        num = rd.randint(1,100000000)
        temp.append(num)
    pickle.dump(temp,f)
    

print('time :',time.time()-start)