#!~/anaconda/bin/python
"""
    Get descriptions and pick reviews for the cleaned books and movies.
    
    Inputs:
    871392 Books_clean2.csv  ~100MB
    1126279 Movies_clean2.csv ~100MB
    Columns: product_id, user_id, rating, nhelp, nview, timestamp, title
    
    8.2G  Movies_&_TV.txt
    14G   Books.txt
    1.9G  descriptions.txt
    
    Processing Steps:
    
    1. Remove duplicated entries (same user_id, same title, but different product_id), and keep the books/movies with more than 20 reviews
    
    2. For each item, only keep the top 20 reviews (ranked by nhelp)
    
    3. Output a review tables for each of books and movies:
    columns: productId, nrating, avgrating, title, description, reviewcontent
    
    Sumin Tang, June 17, 2014

"""

import numpy as np
import matplotlib.pyplot as plt
import pylab
import pickle
import time
from collections import Counter
from snap_utils_simple import *

def countreview(filename, ncut, outfile):
    '''
        Count the number of reviews, and output if nreview>=ncut
        output 3 columns: product_id, title, n_review
    '''
    
    tic = time.time()
    datadir = './data/'
    f = open(datadir + filename, 'r')
    dict = {'product_id':[], 'title':[]}
    for l in f:
        l = l.strip()
        results = l.split(",", 6)
        dict['product_id'].append(results[0])
        dict['title'].append(results[6])
    f.close()
    allpid = np.array(dict['product_id'])
            
    # count the number of reviews
    c = Counter(dict['product_id'])
    uid = np.array(c.keys())
    nid = np.array(c.values())
    utitle = []
    for myid in uid:
        ix = np.where(allpid==myid)[0]
        utitle.append(dict['title'][ix[0]])
    utitle = np.array(utitle)

    # select the ones with at least ncut reviews
    ix = np.where(nid>=ncut)[0]
    print "Number of unique product_id with >= %i reviews: %i (out of %i)" % (ncut, len(ix), len(uid))
    uid2 = uid[ix]
    nid2 = nid[ix]
    utitle2 = utitle[ix]

    # sort by nid2, descending order
    ix = np.argsort(uid2)[::-1]
    uid3 = uid2[ix]
    nid3 = nid2[ix]
    utitle3 = utitle2[ix]

    # Remove duplicates and save to a txt file
    h = open(datadir + outfile, 'w')
    titlelist = []
    kgood = 0
    for i in range(len(uid3)):
        titleA = utitle3[i]
        if not isDuplicateTitle(titleA, titlelist):
            h.write("%s::%i::%s\n" % (uid3[i], nid3[i], titleA))
            titlelist.append(titleA)
            kgood = kgood + 1
    h.close()
    print "Number of unique product with >= %i reviews: %i" % (ncut, kgood)
    print "Time elapsed on countreview for %s: %.2f s" % (filename, time.time()-tic)



def reviewstats(listfile, dataname, outfile):
    '''
        Calculate simple stats for each item.
        Output: product_id, nrating, avgrating, title
    '''
    tic = time.time()
    datadir = './data/'
    # load the product_id from listfile
    dtype = [('product_id', '|S20'), ('n_review', 'i'), ('title', '|S200')]
    pid, n_review, titles = np.loadtxt(datadir + listfile, dtype = dtype, delimiter='::', unpack = True)
    
    # load the clean2file 
    f = open(datadir + dataname, 'r')
    dict = {'product_id':[], 'rating':[]}
    for l in f:
        l = l.strip()
        results = l.split(",", 6)
        dict['product_id'].append(results[0])
        dict['rating'].append(results[2])
    f.close()
    
    allpid = np.array(dict['product_id'])
    allrate = np.array(dict['rating'], float)
            
    # write to outfile
    h = open(datadir + outfile, 'w')
    
    for (myid, mytitle) in zip(pid, titles):
        ix = np.where(allpid==myid)[0]
        ratings = allrate[ix]
        nreview = len(ix)
        avgrating = np.mean(ratings)
        h.write("%s::%i::%.3f::%s\n" % (myid, nreview, avgrating, mytitle))
    
    h.close()
    print "Time elapsed on pickreview for %s: %.2f s" % (dataname, time.time()-tic)
    



def fetchdescription(filename, outfile):
    '''
        Fetch descriptions for items in the filename.
        Output: product_id, description
    '''
    tic = time.time()
    datadir = './data/'
    
    # load the product_id
    dtype = [('product_id', '|S20'), ('n_review', 'i'), ('title', '|S200')]
    pid, n_review, titles = np.loadtxt(datadir + filename, dtype = dtype, delimiter='::', unpack = True)
    
    # write to outfile
    h = open(datadir + outfile, 'w')
    
    
    # load the description file
    f = open(datadir + "descriptions.txt", 'r')
    review = {}
    pidlist = []
    for l in f:
        l = l.strip()
        if len(l)<=1: # save when encounter empty lines between different reviews 
            if ('unknown' not in review.values()) & ('' not in review.values()):
                if (review['product_id'] in pid) & (review['product_id'] not in pidlist):
                    h.write("%s::%s\n" % (review['product_id'], review['desc']))
                    pidlist.append(review['product_id'])
            
            # empty review and move on to the next one
            review = {}
            continue 
        
        key, value = l.split(':', 1)
        value = value.strip()
        if 'product/productId' in key:
            review['product_id'] = value
        if 'product/description' in key:
            review['desc'] = value
    
    h.close()
    print "Time elapsed on fetchdescription for %s: %.2f s" % (filename, time.time()-tic)


def pickreview(listfile, dataname, ncut, outfile):
    '''
        Pick up the top ncut reviews based on nhelp
        output: product_id, user_id, nhelp
    '''
    
    tic = time.time()
    datadir = './data/'
    
    # load the product_id from listfile
    dtype = [('product_id', '|S20'), ('n_review', 'i'), ('title', '|S200')]
    pid, n_review, titles = np.loadtxt(datadir + listfile, dtype = dtype, delimiter='::', unpack = True)
    
    # load the clean2file to get nhelp
    f = open(datadir + dataname, 'r')
    dict = {'product_id':[], 'user_id':[], 'nhelp':[]}
    for l in f:
        l = l.strip()
        results = l.split(",", 6)
        dict['product_id'].append(results[0])
        dict['user_id'].append(results[1])
        dict['nhelp'].append(results[3])
    f.close()
            
    allpid = np.array(dict['product_id'])
    alluserid = np.array(dict['user_id'])
    allnhelps = np.array(dict['nhelp'], int)

    # write to outfile
    h = open(datadir + outfile, 'w')
            
    for myid in pid:
        ix = np.where(allpid==myid)[0]
        nreview = len(ix)
        myuserid = alluserid[ix]
        nhelps = allnhelps[ix]
        iy = np.argsort(nhelps)[::-1]
        if nreview>=ncut:
            for i in range(ncut):
                h.write("%s::%s::%s\n" % (myid, myuserid[iy[i]], nhelps[iy[i]]))

    h.close()
    print "Time elapsed on pickreview for %s: %.2f s" % (dataname, time.time()-tic)




def fetchreviews(listfile, datafile, outfile):
    '''
        Fetch reviews for pairs of (product_id, user_id) in listfile from the reviewdata.
        Output: product_id, user_id, score, helpfulness, title, review_content
    '''
    tic = time.time()
    datadir = './data/'
    
    # load the product_id and user_id from list file
    dtype = [('product_id', '|S20'), ('user_id', '|S20'), ('n_help', 'i')]
    pid, uid, nhelps = np.loadtxt(datadir + listfile, dtype = dtype, delimiter='::', unpack = True)
    
    # write to outfile
    h = open(datadir + outfile, 'w')
    
    # load the review datafile
    f = open(datadir + datafile, 'r')
    review = {}
    for l in f:
        l = l.strip()
        if len(l)<=1: # save when encounter empty lines between different reviews 
            if ('unknown' not in review.values()) & ('' not in review.values()):
                # only fetch once for a given user on a given product
                ix = np.where((pid == review['product_id']) & (uid == review['user_id']))[0]
                if len(ix)>0:
                    print review['product_id'], review['user_id']
                    # if (review['product_id'] in pid) & (review['user_id'] in uid): 
                    h.write("%s::%s::%s::%s::%s::%s\n" % (review['product_id'], review['user_id'], review['score'], review['helpfulness'], review['title'], review['review']))
                    
            
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
        if 'product/title' in key:
            review['title'] = value
        if 'review/text' in key:
            review['review'] = value

    
    h.close()
    print "Time elapsed on fetchreviews for %s: %.2f s" % (listfile, time.time()-tic)

                
def combineresults(listfile, desfile, reviewfile, outfile):
    '''
        combine the files
        input: *_clean3b, *_descriptions, *_top20reviews
        output: productId, nrating, avgrating, title, description, reviewcontent
    '''
    tic = time.time()
    datadir = './data/'
                
    # load the product_id and user_id from list file (clean3b)
    dtype = [('product_id', '|S20'), ('n_review', 'i'), ('avgrating', 'f'), ('title', '|S200')]
    pid, n_review, avgrating, titles = np.loadtxt(datadir + listfile, dtype = dtype, delimiter='::', unpack = True)
    
    # load the description file
    dtype = [('product_id', '|S20'), ('des', '|S10000')]
    pid2, des = np.loadtxt(datadir + desfile, dtype = dtype, delimiter='::', unpack = True)
    
    # load the reviewfile; cut at 5000 charaters for reviews
    # there are some missing values, so read it line by line
    f = open(datadir + reviewfile, 'r')
    pid3 = []
    userid = []
    rcontent = []
    for l in f:
        l = l.strip()
        values = l.split("::", 5) 
        if len(values)==6:
            pid3.append(values[0])
            userid.append(values[1])
            rcontent.append(values[5])
    
    pid3 = np.array(pid3)
    userid = np.array(userid)
    rcontent = np.array(rcontent)
    
    # output
    h = open(datadir + outfile, 'w')
    for i in range(len(pid)):
        myid = pid[i]
        
        # match with des file
        ix = np.where(pid2==myid)[0]
        mydes = "  "
        if len(ix)>0:
            mydes = des[ix[0]] + "  "
        
        # match with reviews
        ix = np.where(pid3==myid)[0]
        myreview = "  "
        myusrlist = []
        for iy in ix:
            myusrid = userid[iy]
            # remove duplicate entries with the same pid and user_id
            if len(list(set(myusrlist).intersection(myusrid)))==0:
                myusrlist.append(myusrid)
                myreview = myreview + " ||| " + rcontent[iy]
    
        # write to file:
        h.write("%s::%i::%.3f::%s::%s::%s\n" % (myid, n_review[i], avgrating[i], titles[i], mydes, myreview))

    h.close()
    print "Time elapsed on combineresults for %s: %.2f s" % (reviewfile, time.time()-tic)
    



# Process the data

if __name__ == "__main__":
    
    # Step 1. clean duplicates and only keep n_review>=20. It takes 2-3h each
    countreview("Books_clean2.csv", 20, "Books_clean3.txt")
    countreview("Movies_clean2.csv", 20, "Movies_clean3.txt")
    
    # product_id, nrating, avgrating, title. It takes ~10min
    reviewstats("Books_clean3.txt", "Books_clean2.csv", "Books_clean3b.txt")
    reviewstats("Movies_clean3.txt", "Movies_clean2.csv", "Movies_clean3b.txt")
    
    # Step 2. Fetch descriptions. It takes ~10min
    fetchdescription("Movies_clean3.txt", "Movies_clean3_descriptions.txt")
    fetchdescription("Books_clean3.txt", "Books_clean3_descriptions.txt")
    
    # Step 3. Product product_id, user_id list, which contains top 20 reviews (by nhelp) for each item. It takes ~10min
    pickreview("Movies_clean3.txt", "Movies_clean2.csv", 20, "Movies_clean3_pid-uid-top20.txt")
    pickreview("Books_clean3.txt", "Books_clean2.csv", 20, "Books_clean3_pid-uid-top20.txt")
    
    # Step 4. Fetch the top 20 reviews for each product. It takes 9-10h each. Started on 10:30pm Tues.
    fetchreviews("Movies_clean3_pid-uid-top20.txt", "Movies_&_TV.txt", "Movies_clean3_top20reviews.txt")
    fetchreviews("Books_clean3_pid-uid-top20.txt", "Books.txt", "Books_clean3_top20reviews.txt")
    
    # Step 5. Merge descriptions with reviews; It takes less than a minute
    #         input: *_clean3b, *_descriptions, *_top20reviews
    #         output: productId, nrating, avgrating, title, description, reviewcontent
    combineresults("Movies_clean3b.txt", "Movies_clean3_descriptions.txt", "Movies_clean3_top20reviews.txt", "Movies_clean3_all.txt")
    combineresults("Books_clean3b.txt", "Books_clean3_descriptions.txt", "Books_clean3_top20reviews.txt", "Books_clean3_all.txt")
    








    