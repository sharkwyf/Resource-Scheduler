import sys
import numpy as np
from collections import OrderedDict
import itertools as it
import random
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
B_sorted = [sorted(B[x],reverse=True) + [x] for x in sorted_id]
D_sorted = []
for x in range(len(B)):
    sorted_id2 = sorted(range(len(B[x])), key=lambda k: B[x][k],reverse=True)
    D_sorted.append([D[x][y] for y in sorted_id2])
D_sorted_dict = []
for d in D_sorted:
    d_list = []
    for i in range(q):
        d_list.append(d.count(i))
    D_sorted_dict.append(d_list)



def check(limit):
    # 剪枝：排序后，大的先拿出来试，如果方案不行，失败得更快
    arr = B_sorted.copy()

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
    if not arr: return True  # 分完，则方案可行
    job = arr.pop()

    groups_now = OrderedDict(sorted(groups.items(), key=lambda item: item[1]))
    start_list = sorted(list(set(groups_now.values())))
    for start in start_list:
        cores = []
        #print(len(arr)+1,start)
        for i in groups_now.keys():
            if start >= groups_now[i]:
                cores.append(i)
        core_max_num = min([len(job) - 1, len(cores)])

        tp_use = float("inf")
        core_dict = {}
        for i in cores:
            index = int(i.split('_')[0])
            if index in core_dict.keys():
                core_dict[index].append(i)
            else:
                core_dict[index]= [i]

        for core_max in range(core_max_num,0,-1):
            cores_copy = cores.copy()
            core_set = []
            for index, i in enumerate(D_sorted_dict[job[-1]]):
                if index in core_dict.keys():
                    core_add = list(core_dict[index][0:min([i,len(core_dict[index]),core_max-len(core_set)])])
                    core_set += core_add
                    for j in core_add:
                        cores_copy.remove(j)

            core_need_add = core_max-len(core_set)
            # if core_need_add>0:
            #     core_add = list(it.combinations(cores_copy, core_need_add))#cores_copy[0:core_need_add]
            #     core_set = core_set + core_add
            if core_need_add>0:
                for index, core_add in enumerate(it.combinations(cores_copy, core_need_add)):
                    core_now = core_set + list(core_add)
                    core_num = len(core_now)
                    dict_use = OrderedDict()
                    for i in core_now:
                        dict_use[i] = 0
                    speed_now = S[job[-1]]*g(core_num,alpha)
                    for index2,j in enumerate(job[0:-1]):
                        dict_use = OrderedDict(sorted(dict_use.items(), key=lambda item: item[1]))
                        key_use = list(dict_use.keys())[0]
                        if key_use.split('_')[0]==str(D_sorted[job[-1]][index2]):
                            dict_use[key_use] += j / speed_now
                        else:
                            dict_use[key_use] += j / speed_now + j/s_t
                    tp  = max(dict_use.values())

                    if tp < tp_use:
                        tp_use = tp
                        core_use = core_now
            else:
                core_now = core_set
                core_num = len(core_now)
                dict_use = OrderedDict()
                for i in core_now:
                    dict_use[i] = 0
                speed_now = S[job[-1]] * g(core_num, alpha)
                for index2, j in enumerate(job[0:-1]):
                    dict_use = OrderedDict(sorted(dict_use.items(), key=lambda item: item[1]))
                    key_use = list(dict_use.keys())[0]
                    if key_use.split('_')[0] == str(D_sorted[job[-1]][index2]):
                        dict_use[key_use] += j / speed_now
                    else:
                        dict_use[key_use] += j / speed_now + j / s_t
                tp = max(dict_use.values())

                if tp < tp_use:
                    tp_use = tp
                    core_use = core_now

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
            # else:
            #     break

    arr.append(job)
    return False

# 每个人承担的工作的上限，最小为，job 里面的最大值，最大为 jobs 之和
l, r = tp_min, tp_max

while l < r:
    mid = (l + r) // 2
    print("start:"+str(mid))
    if check(mid):
        r = mid
    else:
        l = mid + 1

print(l)