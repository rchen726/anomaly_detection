import codecs
import json
import os
import numpy as np
import time
from collections import defaultdict
from collections import deque
from operator import itemgetter
import sys

def AddFriend(id1,id2,friend_list):
    friend_list[id1].add(id2)
    friend_list[id2].add(id1)

def UnFriend(id1,id2,friend_list):
    friend_list[id1].remove(id2)
    friend_list[id2].remove(id1)

def Purchase_update(person_id,  amount, purchase_tot, purchase_count, T):
    purchase_count += 1
    if person_id not in purchase_tot:
        purchase_tot[person_id] =  deque([], T)
    purchase_tot[person_id].append((purchase_count, amount))
    return purchase_count
    
def Find_upto_DFriend(person_id, friend_list, D):
    visited_list = set()
    work_list = [(person_id, 0)]
    idx = 0
    while idx < len(work_list):
        current = work_list[idx]
        idx += 1
        if not current[0] in visited_list and current[1] < D:
            for friend in friend_list[current[0]]:
                work_list.append((friend, current[1] + 1))
        visited_list.add(current[0])
    visited_list.remove(person_id)
    return visited_list

def DFriend_purchases(DFriends, T, purchase_tot):
    purchase=[]
    for friend_id in DFriends:
        if friend_id in purchase_tot:
            purchase.extend(purchase_tot[friend_id])
            
    purchase_tot_num = len(purchase)
    if purchase_tot_num >= T:
        tmp = heapq.nlargest(T, purchase, key=lambda x:x[0])
        purchases_num = T
    else:
        tmp = purchase
        purchases_num = purchase_tot_num
    recent_purchases = [x[1] for x in tmp]
            
    network_mean = np.mean(recent_purchases)
    network_std = np.std(recent_purchases)
    return purchases_num, network_mean, network_std

def test_anomaly(amount, purchases_num, network_mean, network_std):
    if purchases_num >= 2 and amount >= network_mean + 3 * network_std:
        return True
    return False

def jsonWrite_anomaly(file_path, line, mean, std):
    line.update({'mean':  '{0:.2f}'.format(mean), 'sd':  '{0:.2f}'.format(std)})
    with open(file_path, 'a') as f:
        json.dump(line, f)
        f.write('\n')

def run_batch(path_batch):
    purchase_count = 0
    friend_list = defaultdict(set)
    purchase_tot = {}    
    
    with codecs.open(path_batch,'rU','utf-8') as f:
        for num, line in enumerate(f):
            each = json.loads(line)
            if num == 0:
                if 'D' not in each and 'T' not in each:
                    print('I can not find D and T in the batch log!!!')
                    print('returning default values D=2 and T=50')
                    D=2
                    T=50
                else:
                    D = int(each['D'])
                    T = int(each['T'])
            else:
                if each['event_type']=='befriend':
                    AddFriend(each['id1'], each['id2'], friend_list)
                if each['event_type']=='unfriend':
                    UnFriend(each['id1'], each['id2'], friend_list)
                if each['event_type']=='purchase':
                    purchase_count = Purchase_update(each['id'], float(each['amount']), purchase_tot , purchase_count, T)
    return D, T, purchase_count, friend_list, purchase_tot

    
def run_stream(path_stream, path_flagged,  D, T, purchase_count, friend_list, purchase_tot):
    open(path_flagged, 'w').close()
        
    with codecs.open(path_stream,'rU','utf-8') as f:
        for line in f:
            if line == '\n':
                break
            else:
                each = json.loads(line)           
                if each['event_type']=='purchase':
                    # Calculate the network of this specific ID up to D degree
                    DFriend_list_union = Find_upto_DFriend(each['id'], friend_list, D)
                    # find the recent purchase between [2, T] in this ID's network
                    purchases_num, network_mean, network_std = DFriend_purchases(DFriend_list_union, T, purchase_tot)
                    # find the mean and std of the recent purchase, determine this specific purchase is anomalous
                    if test_anomaly(float(each['amount']),purchases_num, network_mean, network_std) == True:
                        jsonWrite_anomaly(path_flagged, each, network_mean, network_std)               
                    purchase_count = Purchase_update(each['id'],  float(each['amount']), purchase_tot , purchase_count, T)        
                if each['event_type']=='befriend':
                    AddFriend(each['id1'], each['id2'], friend_list)
                if each['event_type']=='unfriend':
                    UnFriend(each['id1'], each['id2'], friend_list)
    


def main(path_batch, path_stream, path_flagged):

    D, T, purchase_count, friend_list, purchase_tot = run_batch(path_batch)
    run_stream(path_stream, path_flagged, D, T, purchase_count, friend_list, purchase_tot)
    
if __name__ == "__main__":
    t = time.time()
    main(sys.argv[1], sys.argv[2], sys.argv[3])            
    elapsed = time.time() - t
    print('time', elapsed)            
