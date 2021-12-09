import sys
import numpy as np
from collections import OrderedDict
import itertools as it
data=[]
with open('task2_case1.txt','r') as f:
    for index,line in enumerate(f):
        if index == 0:
            row = line.strip('\n').split(' ')
            data.append([int(row[0]),int(row[1]),float(row[2]),int(row[3])])
        else:
            data.append(list(map(int,line.strip('\n').split(' '))))

n = data[0][0]
q = data[0][1]
alpha = data[0][2]
s_t = data[0][3]
C = data[1]
S = data[3]
B = data[4:4+n]
D = data[4+n:4+2*n]

def g(e,alpha):
    return 1.0-alpha*(e-1)

tp_list = []
tp_max_list = []
for i, b in enumerate(B):
    tp_max_list.append(max([x / S[i] for x in b]))

    #########
    B_i={}
    for k, d in enumerate(D[i]):
        if d in B_i.keys():
            B_i[d].append(b[k])
        else:
            B_i[d]= [b[k]]
    e = len(B_i.keys())
    s_i = g(e,alpha)*S[i]
    tp_i_list = []
    for host in B_i.keys():
        tp_i_list.append(sum(B_i[host])/s_i)
    tp_i = max(tp_i_list)
    tp_list.append(tp_i)

tp_max = sum(tp_list)
tp_min = max(tp_max_list)
print(tp_max,tp_min)
sorted_id = sorted(range(len(tp_max_list)), key=lambda k: tp_max_list[k])
B_sorted = [B[x] + [x] for x in sorted_id]
print("start")

def check(limit):
    # 剪枝：排序后，大的先拿出来试，如果方案不行，失败得更快
    arr = B_sorted

    #groups = [0] * 10
    C_dict = OrderedDict()
    for i in range(q):
        for j in range(C[i]):
            C_dict[str(i)+'_'+str(j)] = 0
    # 分成K 组，看看在这个limit 下 能不能安排完工作
    if backtrace(arr, C_dict, limit):
        return True
    else:
        return False

def backtrace(arr, groups, limit):
    # 尝试每种可能性
    print(len(arr))
    if not arr: return True  # 分完，则方案可行
    job = arr.pop()
    groups_now = OrderedDict(sorted(groups.items(), key=lambda item: item[1]))
    start_list = list(set(groups_now.values()))
    cores = []
    for start in start_list:
        for i in groups_now.keys():
            if start >= groups_now[i]:
                cores.append(i)
        tp_use = 10000
        for core_now in it.combinations_with_replacement(cores,len(job)-1):
            core_num = len(list(set(core_now)))
            dict_use = {}
            for i in list(set(core_now)):
                dict_use[i] = 0
            for i,j in zip(core_now,job):
                if i.split('_')[0]==str(job[-1]):
                    dict_use[i] += j / S[job[-1]]*g(core_num,alpha)
                else:
                    dict_use[i] += j / S[job[-1]]*g(core_num,alpha) + j/q
            tp  = max(dict_use.values())
            if tp < tp_use:
                tp_use = tp
                core_use = list(set(core_now))

        if start + tp_use <= limit:
            dict_last = {}
            for i in core_use:
                dict_last[i] = groups_now[i]
            for i in core_use:
                groups_now[i] = start + tp_use
            if backtrace(arr, groups_now, limit):
                return True
            for i in core_use:
                groups_now[i] = dict_last[i]
            #print('a')


    arr.append(job)
    return False

# 每个人承担的工作的上限，最小为，job 里面的最大值，最大为 jobs 之和
l, r = tp_min, tp_max

while l < r:
    mid = (l + r) // 2

    if check(mid):
        r = mid
    else:
        l = mid + 1

print(l)