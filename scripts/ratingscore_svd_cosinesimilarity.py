"""
    Apply SVD in recsys to SNAP Amazon user rating score data, and calculate cosine similarity for movie-book pairs.
    
    Input:  
    201103 MERGED6.csv
    columns: item_id, user_id, rating, title
    156776 ratings on 2884 movies from 8366 users (54 reviews per movie, 19 reviews per user)
    44327 ratings on 1085 books from 6863 users (41 reviews per book, 6 reviews per user)
    All items have >=20 reviews; 
    Users: 8501 users in total, 2320 of them has>=20 reviews (152212 reviews), and 4339 with >=10 reviews (179527 reviews in total)
    
    Output:
    similarityscores6
    3 columns: item_id1, item_id2, sim_cosine
    
    Sumin Tang, June 13, 2014
    
"""

import numpy as np
import MySQLdb
from recsys.algorithm.factorize import SVD
from snap_utils import *
import time

tic = time.time()

# load the book/movie id data
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# movie id
cur.execute('SELECT key_id, title from Movies_clean3_id')
results = np.array(cur.fetchall())
moviekeys = np.array(results[:, 0], int)
movietitles = np.array(results[:, 1], str)

# book id
cur.execute('SELECT key_id, title from Books6')
results = np.array(cur.fetchall())
bookkeys = np.array(results[:, 0], int)
booktitles = np.array(results[:, 1], str)

# create a new table called simscores with 3 column: item_id1, item_id2, similarityscore
simtable = 'svdsimilarityscores6'
cur.execute('drop table if exists %s' % simtable)
cur.execute('CREATE TABLE %s (item_id1 INT NOT NULL, item_id2 INT NOT NULL, sim_cosine FLOAT NOT NULL)' % simtable)



# load rating data
svd = SVD()
svd.load_data(filename='./data/MERGED6.csv',
              sep=',',
              format={'row':0, 'col':1, 'value':2, 'ids': int})
# About format parameter:
#   'row': 0 -> Rows in matrix come from first column; itemkey_id
#   'col': 1 -> Cols in matrix come from second column; usrkey_id 
#   'value': 2 -> Values (Mij) in matrix come from third column
#   'ids': int -> Ids (row and col ids) are integers (not strings)
# if row is item (not user), then it's item based, and the similarity scores will be between items.


k = 100
svd.compute(k=k,
            min_values=10,
            pre_normalize=None,
            mean_center=True,
            post_normalize=True,
            savefile='./data/MERGED6_svd')

# to load a saved svd
# svd = SVD(filename='./data/MERGED_svd') # Loading already computed SVD model


# get the item_id with available results (n<10 rows & columns were cut out)
m = svd.get_matrix()
rowlabl = m._matrix.row_labels
ids = np.array(rowlabl)



# ==== can further reduce the tables using this list of id.

# calculate cosine similarity score between 2 items:
# svd.similarity(ids[0], ids[100])  # cosine similarity

# For each movie:
# 1. get the top 50 books
# 2. eliminating duplicates by comparing titles
# 3. save id and scores for the final 10 books & movies

print len(ids)
ids2 = ids[np.where(ids<100000)[0]]
print len(ids2)

nitemsall = len(ids)

for id in ids2:  # for movies
    nsearch = 400
    ncut1 = 250
    toplist = np.array(svd.similar(id, n=nsearch))
    topid = np.array(toplist[:,0], int).flatten()
    topscore = np.array(toplist[:,1], float).flatten()
        
    ixbook = np.where((topid != id) & (topid > 1e5))[0]
    n2 = min(len(ixbook), ncut1)
    bookids = topid[ixbook][:n2]
    bookscores = topscore[ixbook][:n2]
    
    while (n2<ncut1):
        nsearch = min(nsearch*2, nitemsall)
        toplist = np.array(svd.similar(id, n=nsearch))
        topid = np.array(toplist[:,0], int).flatten()
        topscore = np.array(toplist[:,1], float).flatten()
        
        ixbook = np.where((topid != id) & (topid > 1e5))[0]
        n2 = min(len(ixbook), ncut1)
        bookids = topid[ixbook][:n2]
        bookscores = topscore[ixbook][:n2]
    
    # get title for the given movie
    titleA = movietitles[np.where(id==moviekeys)][0].lower()
    
    print titleA
    TitleList = []
    #TitleList.append(titleA)

    # get title for the similar items, and remove duplicated items.
    # save the top 200 items
    ncut = 200
    j = 0
    kb = 0 # books

    while (kb<ncut) & (j<ncut1):
        bookid = bookids[j]
        bookscore = bookscores[j]  # cosine similarity from SVD
        
        # check for duplicate books by titles
        titleB = booktitles[np.where(bookkeys==bookid)][0].lower()
        if not isDuplicateTitle(titleB, TitleList): # inject to the simtable if not duplicate
            cur.execute("INSERT INTO %s (item_id1, item_id2, sim_cosine) VALUES (%i, %i, %.3f)" % (simtable, id, bookid, bookscore)) 
            TitleList.append(titleB)
            kb = kb + 1
        j = j + 1
    print TitleList
            
            
db.commit()
db.close()

print "Time elapsed to build the similarity table: %.2f" % (time.time() - tic)

Save2File(simtable) # save to csv file

print "Time elapsed to build the similarity table and save to csv: %.2f" % (time.time() - tic)
