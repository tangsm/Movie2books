#!~/anaconda/bin/python
"""
    Calculate genres scores for movie-book pairs.
    
    Inputs:
    Books6fb: use columns: key_id, genres
    
    Movies_clean3_id, use columns: key_id, title
    Movies genres from
    10681 movies.dat
    /Users/litfeix/Insight/myproject/testmovielens/ml-10M100K/movies.dat
    
    Output:
    genrestable
    item1_id, item2_id, genscore
    
    Sumin Tang, June 22, 2014
"""

import numpy as np
import time
import MySQLdb
import re
from snap_utils import *
from collections import Counter
import scipy.stats as stats


tic = time.time()

# Define Movie and Book Genres:
mgroup = [];                                   bgroup = []
mgroup.append(['action']);                     bgroup.append(['action'])
mgroup.append(['sci-fi', 'fantasy']);          bgroup.append(['fantasy', 'sci-fi-fantasy', 'science-fiction', 'scifi', 'sf', 'sci-fi', 'sci-fi-fantasy'])
mgroup.append(['thriller', 'mystery']);        bgroup.append(['mysteries', 'mystery', 'mystery-thriller', 'thriller', 'thrillers'])
mgroup.append(['war']);                        bgroup.append(['war', 'military'])
mgroup.append(['adventure']);                  bgroup.append(['adventure'])
mgroup.append(['children']);                   bgroup.append(['childrens'])
mgroup.append(['comedy']);                     bgroup.append([ 'comedy', 'humor', 'humour'])
mgroup.append(['crime', 'film-noir']);         bgroup.append(['crime', 'detective'])
mgroup.append(['romance']);                    bgroup.append(['romance'])
mgroup.append(['horror']);                     bgroup.append(['horror'])


# ===== 1. load the book data; a few seconds

booktable = 'Books6fb'
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

cur.execute('SELECT key_id, genres from %s' % booktable)
results = np.array(cur.fetchall())
mybookkeys = np.array(results[:, 0], int)
bookgenres = np.array(results[:, 1], str)
db.close()

# get the genre id for books
mybookgenids = []
for gen in bookgenres: # loop through books
    mygen = [i.strip().lower() for i in gen.split(";")]
    mygenid = []
    for j in range(len(bgroup)):
        grp = [i.strip().lower() for i in bgroup[j]]
        noverlap = len(list(set(mygen).intersection(set(grp))))
        if noverlap>0:
            mygenid.append(j)
    
    mybookgenids.append(mygenid)


MySaver('mybookgenids', mybookgenids)
MySaver('mybookkeys', mybookkeys)


''' check top genres in books
bgall = ""
for mygenre in bookgenres:
    bgall = bgall + mygenre + ' '

genres = [i.strip().lower() for i in bgall.split(";")]
c = Counter(genres)
genid = np.array(c.keys())
ngen = np.array(c.values())
ix = np.where(ngen>=20)[0]
genid[ix]
'''


# ========= 2. Movies: It took 2-3h to match movies (mylist vs movielens) to get genres

# get the movie genres
mgenfile = "/Users/litfeix/Insight/myproject/testmovielens/ml-10M100K/movies.dat"
f = open(mgenfile, 'r')
mid = []
mtitle = []
mgenres = []
for l in f:
    l = l.strip()
    values = l.split("::", 2)
    mid.append(values[0])
    mtitle.append(values[1])
    mgenres.append(values[2])

# get the genre ids for movies
moviegenids = []
for gen in mgenres: # loop through books
    mygen = [i.strip().lower() for i in gen.split("|")]
    mygenid = []
    for j in range(len(mgroup)):
        grp = [i.strip().lower() for i in mgroup[j]]
        noverlap = len(list(set(mygen).intersection(set(grp))))
        if noverlap>0:
            mygenid.append(j)
    
    moviegenids.append(mygenid)


'''   check top genres in movies
    mgall = ""
    for mygenre in mgenres:
    mgall = mgall + mygenre + '|'
    
    genres = [i.strip().lower() for i in mgall.split("|")]
    c = Counter(genres)
    genid = np.array(c.keys())
    ngen = np.array(c.values())
    
    ix = np.where(ngen>=20)[0]
    np.sort(genid[ix])
'''

# load movie id and title, match with movielens genres
movietable = "Movies_clean3_id"
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')
# movie id
cur.execute('SELECT key_id, title from Movies_clean3_id')
results = np.array(cur.fetchall())
mymoviekeys = np.array(results[:, 0], int)
movietitles = np.array(results[:, 1], str)
db.close()

mymoviegenids = []
for (mykey, mytitle) in zip(mymoviekeys, movietitles):
    mygenid = []
    yearA = findyear(mytitle)
    for i in range(len(mtitle)):
        titleB = mtitle[i]
        yearB = findyear(titleB)
        if (TitleOverlap(mytitle, titleB)) & ((yearA == yearB) | (int(yearA)*int(yearB)==0)):
            mygenid = moviegenids[i]
            print mytitle, titleB
            break # quit the current for loop (looping through movielens mtitle)

    mymoviegenids.append(mygenid)

MySaver('mymoviegenids', mymoviegenids)
MySaver('mymoviekeys', mymoviekeys)

print "Time elapsed to get genres for movies: %.2f" % (time.time() - tic)



# ========= 3. Calculate genres scores between movies and books, it takes 1min
# Movies: mymoviekeys, mymoviegenids
# Books: mybookkeyids, mybookgenids

# Load the book and movie genres:
mybookgenids = MyLoader('mybookgenids')
mybookkeys = MyLoader('mybookkeys')

mymoviegenids = MyLoader('mymoviegenids')
mymoviekeys = MyLoader('mymoviekeys')

# define names of the tables
intable = "textsimilarityscores"
outtable = "textsimilarityscores2"

# connect to mysql
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create a new table with columns: item_id1, item_id2, sim_cosine, gen_score
cur.execute('drop table if exists %s' % outtable)
cur.execute('CREATE TABLE %s (item_id1 INT NOT NULL, item_id2 INT NOT NULL, sim_cosine FLOAT NOT NULL, gen_score FLOAT NOT NULL)' % outtable)


# load the intable
cur.execute('SELECT item_id1, item_id2, sim_cosine from %s' % intable)
results = np.array(cur.fetchall())
item_id1 = np.array(results[:, 0], int)
item_id2 = np.array(results[:, 1], int)
sim_cosine = np.array(results[:, 2], float)

    

for (id1, id2, simcos) in zip(item_id1, item_id2, sim_cosine):
    # get movie genres
    ix = np.where(mymoviekeys == id1)[0]
    if len(ix)>0:
        gen1 = mymoviegenids[ix[0]]
    else:
        gen1 = []
    
    # get book genres
    ix = np.where(mybookkeys == id2)[0]
    if len(ix)>0:
        gen2 = mybookgenids[ix[0]]
    else:
        gen2 = []
    
    # jaccard index of genres overlap
    ncom = len(list(set(gen1).intersection(set(gen2))))
    jgen = 0
    if ncom>0:
        nall = len(set(gen1)) + len(set(gen2)) - ncom
        jgen = 1.*ncom/nall

    # check for confliction: 5, 6 (children, comedy) is conflicting with 7, 9 (crime, horror)
    if ((len(list(set(gen1).intersection([5, 6])))>0) & (len(list(set(gen2).intersection([7, 9])))>0)) or  ((len(list(set(gen2).intersection([5, 6])))>0) & (len(list(set(gen1).intersection([7, 9])))>0)):
        jgen = 0

    cur.execute("INSERT INTO %s (item_id1, item_id2, sim_cosine, gen_score) VALUES (%i, %i, %.5f, %.5f)" % (outtable, id1, id2, simcos, jgen))


db.commit()
db.close()

Save2File(outtable) # save to csv file

print "Time elapsed to build the genres-textsimilarity table: %.2f" % (time.time() - tic)



# =========== 4. normalize the sim_cosine and gen_score, calculate a combined score; it took 1min

# define names of the tables
intable = "textsimilarityscores2"
outtable = "textsimilarityscores3"

# connect to mysql
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create a new table with columns: item_id1, item_id2, sim_cosine, gen_score, simgen_score
cur.execute('drop table if exists %s' % outtable)
cur.execute('CREATE TABLE %s (item_id1 INT NOT NULL, item_id2 INT NOT NULL, sim_cosine FLOAT NOT NULL, gen_score FLOAT NOT NULL, simgen_score FLOAT NOT NULL)' % outtable)


# load the intable
cur.execute('SELECT item_id1, item_id2, sim_cosine, gen_score from %s' % intable)
results = np.array(cur.fetchall())
item_id1 = np.array(results[:, 0], int)
item_id2 = np.array(results[:, 1], int)
sim_cosine = np.array(results[:, 2], float)
gen_score = np.array(results[:, 3], float)

simmean = np.mean(sim_cosine)
genmean = np.mean(gen_score)
simsig= np.std(sim_cosine)
gensig = np.std(gen_score)

print simmean, simsig, genmean, gensig


for (id1, id2, simcos, genscore) in zip(item_id1, item_id2, sim_cosine, gen_score):
    simgenscore = (simcos - simmean)/simsig + (genscore-genmean)/gensig # this only takes 1min in total
    ''' this takes 14h to finish
    simpscore = stats.percentileofscore(sim_cosine, simcos)
    genpscore = stats.percentileofscore(gen_score, genscore)
    
    simgenscore = (simpscore + genpscore)/2.
    '''
    cur.execute("INSERT INTO %s (item_id1, item_id2, sim_cosine, gen_score, simgen_score) VALUES (%i, %i, %.5f, %.5f, %.5f)" % (outtable, id1, id2, simcos, genscore, simgenscore))


db.commit()
db.close()

Save2File(outtable) # save to csv file

print "Time elapsed: %.2f" % (time.time() - tic)


    