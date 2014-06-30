#!~/anaconda/bin/python
"""
    Clean SNAP Amazon Review Data on Books and Movies 
    
    1. only keep reviews with non-"unknown" or empty contributes
    
    2. only keep products with at least 20 reviews, and users with at least 20 reviews
    
    3. Remove TVs from Movies&TVs
    
    4. Output a clean tables for each of books and movies:
    columns: productId, userId, score, helpfulness, time, title
    
    Sumin Tang, June 11, 2014
"""

import numpy as np
import matplotlib.pyplot as plt
import pylab
import pickle
import time
from collections import Counter

def parse(filename):
    '''
        parse review data to extract productid and userid for pre-selection.
    '''
    datadir = './data/' 
    f = open(datadir + filename, 'r')
    dict = {'productId':[], 'userId':[]}
    dictkeys = dict.keys()
    for l in f:
        l = l.strip()
        colonPos = l.find(': ')
        if colonPos == -1:
            continue
        eName = l[:colonPos].split('/')[-1]
        if eName in dictkeys: # only grab the info defined in dict
            rest = l[colonPos+2:]
            dict[eName].append(rest)

    with open(datadir + filename[:-4] + '.pickle', 'wb') as handle:
        pickle.dump(dict, handle)


def countlist(ncut, outfile, *args):
    '''
        count and save unique ids to a file, requiring n_review>ncut
    '''
    datadir = './data/' 
    c = Counter(args)
    uid = c.keys()
    nid = c.values()
    
    nabn = 1e4
    ix = np.where(nid>1e5)[0]
    if len(ix)>0:
        print "abnormal ids with >%i reviews: " % nabn
        print uid[ix]
    
    # save to file
    h = open(datadir + outfile, 'w')
    for x, y in zip(uid, nid):
        if y>=ncut:
            h.write('%s\n' % x)
    h.close()
    print "===== %s saved:" % outfile
                
    # print out the numbers  
    print "Total number of reviews: %i" % len(args)        
    print "Total number of unique ids: %i" % len(nid)
    ix = [x for x in nid if x >= ncut]
    print "Number of ids with >=%i reviews: %i" % (ncut, len(ix))
    print "Number of reviews for ids with >=%i reviews: %i" % (ncut, np.sum(ix))
    print "Number of reviews per id: Max = %i, Min = %i, mean = %i, median = %i" % (np.max(nid), np.min(nid), np.mean(nid), np.median(nid))
    print "---------- end of countlist-------------"            

    # plot the histgram of number of reviews
    figname = './plots/' + outfile[:-8] + '_hist.png'
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.hist(np.log10(nid), bins = 100)
    plt.xlabel("log(Number of reviews)", fontsize = 18)
    plt.ylabel("Number of items", fontsize = 18)
    pylab.savefig(figname)
    plt.close()
    

def countreview(pkfile):
    '''
        A wrapper of countlist to list both productid and userid
    '''
    datadir = './data/' 
    with open(datadir + pkfile, 'rb') as handle:
        dict = pickle.load(handle)
    
    # output productId with at least 10 reviews
    pid = dict['productId']
    ncut = 20
    outfile = pkfile[:-7] + '_productID_n' + str(ncut) + '.txt'
    countlist(ncut, outfile, *pid)

    # output productId with at least 10 reviews
    pid = dict['userId']
    ncut = 20
    outfile = pkfile[:-7] + '_userID_n' + str(ncut) + '.txt'
    countlist(ncut, outfile, *pid)

def clean(filename, pidfile, uidfile):
    '''
        parse and clean review data to extract productid, userid, score, helpfulness, timestamp, and title
        only keep itemids with n_review>=10, and all the attributes not 'unknown'
    '''
    datadir = './data/' 
    
    outfile = datadir + filename[:-4] + '_clean.txt'
    h = open(outfile, 'w')
    h.write("# product_id::user_id::score::helpfulness::timestamp::title\n")
    
    pid = np.loadtxt(datadir + pidfile, dtype = 'str')
    uid = np.loadtxt(datadir + uidfile, dtype = 'str')
    
    f = open(datadir + filename, 'r')
    review = {}
    for l in f:
        l = l.strip()
        if len(l)<=1: # save when encounter empty lines between different reviews 
            if ('unknown' not in review.values()) & ('' not in review.values()):
                if (review['product_id'] in pid) & (review['user_id'] in uid):
                    h.write("%s::%s::%s::%s::%s::%s\n" % (review['product_id'], review['user_id'], review['score'], review['helpfulness'], review['time'], review['title']))
            
            # empty review and move on to the next one
            review = {}
            continue 
        
        key, value = l.split(':', 1)
        value = value.strip()
        if 'product/productId' in key:
            review['product_id'] = value
        if 'review/userId' in key:
            review['user_id'] = value
        if 'review/score' in key:
            review['score'] = value
        if 'review/helpfulnes' in key:
            review['helpfulness'] = value
        if 'review/time' in key:
            review['time'] = value
        if 'product/title' in key:
            review['title'] = value
    
    h.close()


def getmovieid(ctgrfile):
    '''
        get ids for movies from the description file (to remove TVs from the cleaned movie&tv data)
    '''
    datadir = './data/'   
    h = open(datadir + 'movieid.txt', 'w')
    ctgr = ''    
    # load the file in reverse order (starting with the last line)
    for l in reversed(open(datadir + ctgrfile).readlines()):
        if l[0] != ' ': # productid
            pid = l.strip()
            myctgr = ctgr.split(', ')
            if 'Movies' in myctgr:
                h.write('%s\n' % pid)
            
            # clear description and move on to the next item
            ctgr = ''
            continue
        
        ctgr = ctgr + l.strip() + ', '
    h.close()


def getmoviedata(datafile, idfile, outfile):
    '''
        remove TVs from the cleaned movie&tv data
    '''
    datadir = './data/'
    h = open(datadir + outfile, 'w')
    h.write("# product_id::user_id::score::helpfulness::timestamp::title\n")
    
    movieid = np.loadtxt(datadir + idfile, dtype = 'str')
    #pid, uid, score, helpfulness, timestamp, title = np.loadtxt(datadir + datafile, unpack = True, delimiter = '::', dtype = 'str', skiprows = 1)
    f = open(datadir + datafile, 'r')
    l = f.readline() # skip the first row - header
    for l in f:
        l = l.strip()
        values = l.split('::', 1)
        myid = values[0]
        if myid in movieid:
            h.write("%s\n" % (l))
    
    h.close()



# Process the data

if __name__ == "__main__":
    
    # === Movies & TVs
    
    tic = time.time()
    filename = 'Movies_&_TV.txt'
    parse(filename)
    print "Time elapsed for parsing %s for pre-processing: %.2f s" % (filename, time.time()-tic)
    
    
    tic = time.time()
    pkfile = 'Movies_&_TV.pickle'
    countreview(pkfile)
    print "Time elapsed for loading, counting and outputing list of ids %s: %.2f s" % (pkfile, time.time()-tic)
    
    
    tic = time.time()
    filename = 'Movies_&_TV.txt'
    pidfile = 'Movies_&_TV_productID_n20.txt'
    uidfile = 'Movies_&_TV_userID_n20.txt'    
    clean(filename, pidfile, uidfile)
    print "Time elapsed for cleaning %s: %.2f s" % (filename, time.time()-tic)
    
    tic = time.time()
    ctgrfile = 'categories.txt'
    getmovieid(ctgrfile)
    print "Time elapsed to get movie ids from %s: %.2f s" % (ctgrfile, time.time()-tic)
    
    tic = time.time()
    datafile = 'Movies_&_TV_clean.txt'
    idfile = 'movieid.txt'
    outfile = 'Movies_clean.txt'
    getmoviedata(datafile, idfile, outfile)
    print "Time elapsed to get cleaned movie data: %.2f s" % (time.time()-tic)
    
    
    # === Books
    
    tic = time.time()
    filename = 'Books.txt'
    parse(filename)
    print "Time elapsed for pasing %s for pre-processing: %.2f s" % (filename, time.time()-tic)


    tic = time.time()
    pkfile = 'Books.pickle'
    countreview(pkfile)
    print "Time elapsed for loading and counting %s: %.2f s" % (pkfile, time.time()-tic)
        
    tic = time.time()
    filename = 'Books.txt'
    pidfile = 'Books_productID_n20.txt'
    uidfile = 'Books_userID_n20.txt'    
    clean(filename, pidfile, uidfile)
    print "Time elapsed for cleaning %s: %.2f s" % (filename, time.time()-tic)
    

    