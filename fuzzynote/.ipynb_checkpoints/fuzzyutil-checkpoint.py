from fuzzywuzzy import fuzz
import numpy as np
import itertools
import multiprocess as mp
import qgrid
import pandas as pd

def getcorrections(toadjust, to_correct='to_correct', corrected='corrected'):
    cc = pd.DataFrame(toadjust)
    cc.columns = ['to_correct']
    cc['corrected'] = ''
    qgrid_widget = qgrid.QgridWidget(df=cc) 
    return qgrid_widget

def token_sort(inp):
    return fuzz.token_sort_ratio(inp[0],inp[1])

def token_set(inp):
    return fuzz.token_set_ratio(inp[0],inp[1])

def partial(inp):
    return fuzz.partial_ratio(inp[0],inp[1])

def ratio(inp):
    return fuzz.ratio(inp[0],inp[1])

def similorder(input, scorelimit=90, method='token_sort', verbose=False):
    res=[]
    options = {'ratio': fuzz.ratio, 
               'partial': fuzz.partial_ratio, 
               'token_sort': fuzz.token_sort_ratio, 
               'token_set': fuzz.token_set_ratio}
    fuzzymethod = options[method]
    alist = list(input)
    while len(alist) >=2:
        distance=[]
        if verbose:
            print(alist[0])
        res.append(alist[0])
        dist=False
        for i,v in enumerate(alist):
            if alist[0] != alist[i]:
                dist = fuzzymethod(alist[0],alist[i])
            if dist:
                distance.append(dist)
            if dist >= scorelimit:
                if alist[0] != alist[i]:
                    if verbose:
                        print(alist[0],alist[i])
                    if alist[i] not in res:
                        res.append(alist[i])
                alist.remove(alist[i])
        if verbose:
            print("\n")
        alist.remove(alist[0])
        alist=list(reversed([x for _,x in sorted(zip(distance,alist))]))
    if verbose:
        print(alist)
    res.append(alist[0])
    return res

def matchscan(items, fun):
    p = mp.Pool()
    count=len(items)
    input = itertools.combinations_with_replacement(items, 2)
    inp = (x for x in input if x[0]!=x[1] is not None)
    results = p.map(fun, inp)
    p.close()
    p.join()
    tri = np.zeros((count, count))
    iu1 = np.triu_indices(count,1,count)
    il1 = np.tril_indices(count,-1,count)
    tri[iu1]=results
    tri[il1]=np.flipud(np.rot90(tri, 1))[il1]
    np.fill_diagonal(tri, 100)
    return tri

def matchscan_tri(items, fun):
    p = mp.Pool()
    count=len(items)
    input = itertools.combinations_with_replacement(items, 2)
    inp = (x for x in input if x[0]!=x[1] is not None)
    results = p.map(fun, inp)
    p.close()
    p.join()
    tri = np.zeros((count, count))
    tri[np.triu_indices(count, 1)] = results
    return tri

def scored_threshold(results, score, specieslist, verbose=True):
    scored = []
    for i in range(results.shape[0]):
            if verbose:
                print(i, specieslist[i], len(np.where((results[i,:] >= score) & (results[i,:] < 100))[0]))
            scored.append([i,len(np.where((results[i,:] >= score) & (results[i,:] < 100))[0])])
    return scored


def matchinglist(list1, list2, scorelimit=90, method='token_sort', perfectmatch=False, verbose=False):
    res = []
    options = {'ratio': fuzz.ratio, 
               'partial': fuzz.partial_ratio, 
               'token_sort': fuzz.token_sort_ratio, 
               'token_set': fuzz.token_set_ratio}
    if method not in list(options.keys()):
        print('method not implemented: %s ' % method)
        return
    fuzzymethod = options[method]
    for i in list1:
        for j in list2:
            score = fuzzymethod(i,j)
            if not perfectmatch:
                if score >= scorelimit and score < 100:
                    if verbose:
                        print(i,'--' ,j, '--', score)
                    #, '--- method: %s' % method
                    res.append([i,j,score])
            if perfectmatch:
                if score >= scorelimit:
                    if verbose:
                        print(i,'--' ,j, '--', score)
                    #, '--- method: %s' % method
                    res.append([i,j,score])
    return res

