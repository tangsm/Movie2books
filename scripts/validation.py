#!~/anaconda/bin/python
"""
    Check the jaccard4 scores for recommended pairs vs random baseline
    
    Input:  
    textsimilarityscores3
    columns: item_id1, item_id2, sim_cosine, gen_score, simgen_score
    
    svdsimilarityscores6b
    8 columns: item_id1, item_id2, nuser, sim_cosine, jaccard, jaccard4, jaccard5, pearson
    
    
    Sumin Tang, June 22, 2014
    
"""

import numpy as np
import MySQLdb
from snap_utils import *
import time
from scipy import stats

tic = time.time()

# connect to the database
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

jactable = "svdsimilarityscores6b"  # input jaccard4 table
simtable = "textsimilarityscores3" # input simtable

# fetch jaccard4 scores
cur.execute('SELECT item_id1, item_id2, jaccard4 from %s' % jactable)
results = np.array(cur.fetchall())
j4item_id1 = np.array(results[:, 0], int)
j4item_id2 = np.array(results[:, 1], int)
jaccard4 = np.array(results[:, 2], float)

# for each movie, calculate the random baseline
# number of books 
cur.execute("select distinct item_id2 from %s" % simtable)
results = cur.fetchall()
nbook = len(results)

# sum of jaccard4 for each movie
cur.execute("select item_id1, sum(jaccard4) from %s group by item_id1" % jactable)
results = np.array(cur.fetchall())
j4movieid = np.array(results[:, 0], int)
j4base = np.array(results[:, 1], float)/nbook

print j4base

# get the jaccard4 score for top 5 books
# unique id of movies
cur.execute("select distinct item_id1 from %s" % simtable)
results = np.array(cur.fetchall())
movieid = np.array(results[:, 0], int)
nmovie = len(movieid)

print nmovie
h = open('./data/validationscores.txt', 'w')
for i in range(nmovie):
    print i
    mymovieid = movieid[i]
    
    # select top 5 books
    cur.execute("select item_id2 from %s where item_id1=%i order by simgen_score limit 5" % (simtable, mymovieid))
    results = np.array(cur.fetchall())
    bookids = np.array(results, int)
    
    # fetch the jaccard4 scores for these books
    myj4s = []
    for mybookid in bookids:
        ix = np.where((j4item_id1==mymovieid) & (j4item_id2==mybookid))[0]
        if len(ix)>0:
            myj4s.append(jaccard4[ix[0]])
    if len(myj4s)>0:
        myj4 = np.mean(np.array(myj4s))
    else:
        myj4 = 0

    # fetch the base line for this movie
    ix = np.where(j4movieid==mymovieid)[0]
    if len(ix)>0:
        mj4base = j4base[ix[0]]
    else:
        mj4base = 0
    
    if (myj4>0) & (mj4base>0):
        topj4 = myj4/mj4base
        h.write("%i  %.5f %.4f\n" % (mymovieid, mj4base, topj4))

h.close()


# read and plot the validation
import numpy as np
import pylab 
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams.update({'font.size': 18})
matplotlib.rc("axes", linewidth=1.0)

mid, mj4base, topj4 = np.loadtxt('./data/validationscores.txt', unpack = True)

figname = "./plots/validation_hist.png" 
fig = plt.figure(1, figsize=(10,6))
ax = pylab.axes([0.12, 0.15, 0.8, 0.8])

myy, edges = np.histogram(topj4, bins=np.linspace(0, 80, 21))
myx = (edges[:-1] + edges[1:])/2.
pylab.bar(myx, myy, color="b", align="center", width=4)
#ix = np.where(topj4>80)[0]
#pylab.bar(82, len(ix), color="b", align="center", width=4, alpha=0.5)
mymean = np.mean(topj4)
mymedian = np.median(topj4)
ax.plot([mymedian, mymedian], [0, 45], 'r--', linewidth=3)
#ax.plot([mymean, mymean], [0, 45], 'r--', linewidth=2)

ax.plot([1, 1], [0, 45], 'g--', linewidth=3)
ax.text(mymedian+2., 41, "Median(IF)=%.0f" % mymedian, color = 'r', fontsize=24)
ax.text(2, 41, "Random", color = 'g', fontsize=24)
ax.text(2, 37, "Baseline", color = 'g', fontsize=24)
#ax.text(70, 35, "IF>80", color = 'b', fontsize=18)

pylab.xlabel("Improvement Factor (IF)", fontsize = 24)
pylab.ylabel("Number of Movies", fontsize = 24)
#ax.set_xlim([0, 85])
pylab.savefig(figname)
pylab.close()




print "Time elapsed: %.2f" % (time.time() - tic)
